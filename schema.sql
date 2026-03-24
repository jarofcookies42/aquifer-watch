-- West Texas Water & Data Center Intelligence Dashboard
-- Database Schema v0.2
-- Requires: PostgreSQL 15+, PostGIS, TimescaleDB
-- Safe to re-run: uses IF NOT EXISTS / ON CONFLICT throughout.

-- ============================================================
-- Extensions
-- ============================================================
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- fuzzy text search

-- ============================================================
-- Enums (use DO blocks for idempotency)
-- ============================================================
DO $$ BEGIN
    CREATE TYPE site_status AS ENUM (
        'rumored', 'filing_detected', 'permitted', 'under_construction',
        'operational', 'paused', 'cancelled'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE data_source AS ENUM (
        'twdb_gwdb', 'twdb_recorder', 'tceq_air', 'tceq_water',
        'ercot_gen_gis', 'ercot_large_load', 'county_cad',
        'news', 'manual'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE permit_type AS ENUM (
        'air_quality_nsr', 'air_quality_psd', 'air_quality_ghg',
        'water_rights', 'water_quality', 'wastewater', 'other'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE alert_severity AS ENUM ('info', 'watch', 'critical');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ============================================================
-- Core: Data center sites we're tracking
-- ============================================================
CREATE TABLE IF NOT EXISTS dc_sites (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,           -- e.g. "Galaxy Helios"
    project_code    TEXT UNIQUE,             -- e.g. "HELIOS", "MATADOR"
    operator        TEXT,
    tenant          TEXT,                    -- e.g. "CoreWeave"
    county          TEXT NOT NULL,
    location        GEOMETRY(Point, 4326),   -- lat/lon
    capacity_mw     NUMERIC,                 -- approved or planned MW
    water_demand_gpd NUMERIC,                -- gallons per day estimate
    status          site_status DEFAULT 'rumored',
    notes           TEXT,
    first_detected  DATE,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dc_sites_location ON dc_sites USING GIST(location);
CREATE INDEX IF NOT EXISTS idx_dc_sites_county ON dc_sites(county);
CREATE INDEX IF NOT EXISTS idx_dc_sites_status ON dc_sites(status);

-- Seed all tracked sites (safe to re-run)
INSERT INTO dc_sites (name, project_code, operator, tenant, county, location, capacity_mw, water_demand_gpd, status, first_detected, notes) VALUES
    ('Galaxy Helios', 'HELIOS', 'Galaxy Digital', 'CoreWeave',
     'Dickens', ST_SetSRID(ST_MakePoint(-100.78, 33.77), 4326),
     1600, NULL, 'under_construction', '2022-01-01',
     '$3.5B Phase 2 expansion (Helios 2/3/4). 1.6 GW approved by ERCOT.'),
    ('Fermi Project Matador', 'MATADOR', 'Fermi America', NULL,
     'Carson', ST_SetSRID(ST_MakePoint(-101.58, 35.33), 4326),
     11000, 5000000, 'permitted', '2025-08-01',
     'MOU with Amarillo for 2.5M-10M gal/day (non-binding). Hybrid dry-wet cooling planned but full system not complete until 2034. Using midpoint estimate.'),
    ('Lubbock NE (Prospective)', 'LBK_NE', NULL, NULL,
     'Lubbock', ST_SetSRID(ST_MakePoint(-101.80, 33.58), 4326),
     NULL, NULL, 'rumored', NULL,
     'Calvano Development / Texas Solarworks. 936 acres off NE Loop 289. Zoning rejected Jan 2026, may resubmit.'),
    ('Outlaw Ventures', 'OUTLAW', 'Outlaw Ventures', NULL,
     'Lubbock', ST_SetSRID(ST_MakePoint(-101.85, 33.75), 4326),
     600, NULL, 'filing_detected', '2025-06-01',
     '~20 min north of Lubbock off I-27. 600 MW capacity.'),
    ('TeraWulf / Fluidstack', 'TERAWULF', 'TeraWulf', NULL,
     'Lubbock', ST_SetSRID(ST_MakePoint(-101.82, 33.80), 4326),
     NULL, NULL, 'rumored', '2025-09-01',
     '~22 miles north of Lubbock. Partnership with Fluidstack for GPU hosting.')
ON CONFLICT (project_code) DO NOTHING;

-- ============================================================
-- TWDB: Groundwater wells
-- ============================================================
CREATE TABLE IF NOT EXISTS wells (
    id                  SERIAL PRIMARY KEY,
    state_well_number   TEXT UNIQUE NOT NULL,  -- TWDB identifier
    latitude            NUMERIC NOT NULL,
    longitude           NUMERIC NOT NULL,
    location            GEOMETRY(Point, 4326),
    county              TEXT,
    aquifer_code        TEXT,                  -- e.g. "121OGLL" for Ogallala
    aquifer_name        TEXT,
    well_depth_ft       NUMERIC,
    well_type           TEXT,                  -- monitoring, public supply, etc.
    owner               TEXT,
    driller             TEXT,
    completion_date     DATE,
    twdb_record_url     TEXT,
    raw_json            JSONB,                 -- full API response preserved
    ingested_at         TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_wells_location ON wells USING GIST(location);
CREATE INDEX IF NOT EXISTS idx_wells_aquifer ON wells(aquifer_code);
CREATE INDEX IF NOT EXISTS idx_wells_county ON wells(county);
CREATE INDEX IF NOT EXISTS idx_wells_state_num ON wells(state_well_number);

-- ============================================================
-- TWDB: Water level measurements (time-series)
-- ============================================================
CREATE TABLE IF NOT EXISTS water_levels (
    well_id             INTEGER NOT NULL REFERENCES wells(id),
    measured_at         TIMESTAMPTZ NOT NULL,
    depth_to_water_ft   NUMERIC,               -- depth below land surface
    water_elevation_ft  NUMERIC,               -- elevation above sea level
    measurement_method  TEXT,
    measuring_agency    TEXT,
    source              data_source DEFAULT 'twdb_gwdb',
    ingested_at         TIMESTAMPTZ DEFAULT now()
);

-- Convert to TimescaleDB hypertable (only if not already one)
SELECT create_hypertable('water_levels', 'measured_at', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_wl_well_time ON water_levels(well_id, measured_at DESC);

-- ============================================================
-- TWDB: Water quality samples
-- ============================================================
CREATE TABLE IF NOT EXISTS water_quality (
    id                  SERIAL PRIMARY KEY,
    well_id             INTEGER NOT NULL REFERENCES wells(id),
    sampled_at          TIMESTAMPTZ NOT NULL,
    tds_mg_l            NUMERIC,               -- total dissolved solids
    chloride_mg_l       NUMERIC,
    nitrate_mg_l        NUMERIC,
    fluoride_mg_l       NUMERIC,
    arsenic_ug_l        NUMERIC,
    ph                  NUMERIC,
    specific_conductance NUMERIC,
    collection_entity   TEXT,
    reliability_code    TEXT,
    raw_json            JSONB,
    ingested_at         TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_wq_well_time ON water_quality(well_id, sampled_at DESC);

-- ============================================================
-- ERCOT: Generation interconnection queue
-- ============================================================
CREATE TABLE IF NOT EXISTS ercot_gen_queue (
    id                  SERIAL PRIMARY KEY,
    inr_number          TEXT UNIQUE,           -- ERCOT interconnection request #
    project_name        TEXT,
    fuel_type           TEXT,                  -- Solar, Wind, Gas, Battery, etc.
    capacity_mw         NUMERIC,
    county              TEXT,
    interconnection_bus TEXT,
    status              TEXT,                  -- e.g. "IA Executed", "Under Review"
    projected_cod       DATE,                  -- commercial operation date
    tsp                 TEXT,                  -- transmission service provider
    ercot_region        TEXT,
    gis_report_month    DATE,                  -- which monthly report this came from
    raw_json            JSONB,
    ingested_at         TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ercot_county ON ercot_gen_queue(county);
CREATE INDEX IF NOT EXISTS idx_ercot_fuel ON ercot_gen_queue(fuel_type);
CREATE INDEX IF NOT EXISTS idx_ercot_status ON ercot_gen_queue(status);

-- ============================================================
-- ERCOT: Large load tracking (manually supplemented)
-- ============================================================
CREATE TABLE IF NOT EXISTS ercot_large_loads (
    id                  SERIAL PRIMARY KEY,
    lli_number          TEXT UNIQUE,           -- LLI-### identifier
    entity_name         TEXT,
    county              TEXT,
    location            GEOMETRY(Point, 4326),
    requested_mw        NUMERIC,
    approved_mw         NUMERIC,
    status              TEXT,
    tsp                 TEXT,
    dc_site_id          INTEGER REFERENCES dc_sites(id),
    source_document     TEXT,                  -- URL to TAC report, etc.
    source_date         DATE,
    notes               TEXT,
    ingested_at         TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ll_county ON ercot_large_loads(county);

-- ============================================================
-- TCEQ: Permits
-- ============================================================
CREATE TABLE IF NOT EXISTS tceq_permits (
    id                  SERIAL PRIMARY KEY,
    regulated_entity_rn TEXT,                  -- TCEQ RN number
    customer_cn         TEXT,                  -- TCEQ CN number
    permit_number       TEXT,
    permit_type         permit_type,
    entity_name         TEXT,
    county              TEXT,
    location            GEOMETRY(Point, 4326),
    status              TEXT,                  -- pending, issued, denied, etc.
    application_date    DATE,
    issued_date         DATE,
    description         TEXT,
    dc_site_id          INTEGER REFERENCES dc_sites(id),
    source_url          TEXT,
    raw_json            JSONB,
    ingested_at         TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tceq_county ON tceq_permits(county);
CREATE INDEX IF NOT EXISTS idx_tceq_rn ON tceq_permits(regulated_entity_rn);
CREATE INDEX IF NOT EXISTS idx_tceq_type ON tceq_permits(permit_type);

-- ============================================================
-- County: Property records
-- ============================================================
CREATE TABLE IF NOT EXISTS property_records (
    id                  SERIAL PRIMARY KEY,
    county              TEXT NOT NULL,
    parcel_id           TEXT,
    owner_name          TEXT,
    owner_name_previous TEXT,
    address             TEXT,
    location            GEOMETRY(Point, 4326),
    acreage             NUMERIC,
    appraised_value     NUMERIC,
    appraised_value_prev NUMERIC,
    land_use_code       TEXT,
    deed_date           DATE,
    sale_amount         NUMERIC,
    dc_site_id          INTEGER REFERENCES dc_sites(id),
    source              TEXT,
    raw_json            JSONB,
    ingested_at         TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_prop_county ON property_records(county);
CREATE INDEX IF NOT EXISTS idx_prop_owner ON property_records USING GIN(owner_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_prop_location ON property_records USING GIST(location);

-- ============================================================
-- Intelligence: Alerts & events
-- ============================================================
CREATE TABLE IF NOT EXISTS alerts (
    id                  SERIAL PRIMARY KEY,
    severity            alert_severity NOT NULL,
    title               TEXT NOT NULL,
    body                TEXT,
    source              data_source NOT NULL,
    dc_site_id          INTEGER REFERENCES dc_sites(id),
    triggered_at        TIMESTAMPTZ DEFAULT now(),
    acknowledged_at     TIMESTAMPTZ,
    trigger_rule        TEXT,                  -- which detection rule fired
    source_record_id    INTEGER,               -- FK to source table row
    source_record_table TEXT                   -- which table it came from
);

CREATE INDEX IF NOT EXISTS idx_alerts_time ON alerts(triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_site ON alerts(dc_site_id);
CREATE INDEX IF NOT EXISTS idx_alerts_sev ON alerts(severity);

-- ============================================================
-- Metadata: Ingestion runs
-- ============================================================
CREATE TABLE IF NOT EXISTS ingestion_log (
    id              SERIAL PRIMARY KEY,
    source          data_source NOT NULL,
    started_at      TIMESTAMPTZ DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    records_fetched INTEGER DEFAULT 0,
    records_new     INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    status          TEXT DEFAULT 'running',    -- running, success, failed
    error_message   TEXT,
    parameters      JSONB                      -- query params used
);

-- ============================================================
-- Convenience views (CREATE OR REPLACE for idempotency)
-- ============================================================

-- Wells near tracked data center sites (within 25 miles)
CREATE OR REPLACE VIEW wells_near_sites AS
SELECT
    w.id AS well_id,
    w.state_well_number,
    w.aquifer_code,
    w.well_depth_ft,
    s.id AS site_id,
    s.name AS site_name,
    s.project_code,
    ST_Distance(w.location::geography, s.location::geography) / 1609.34 AS distance_miles
FROM wells w
CROSS JOIN dc_sites s
WHERE ST_DWithin(w.location::geography, s.location::geography, 40234)  -- 25 miles in meters
  AND w.aquifer_code LIKE '121OG%'
ORDER BY s.id, distance_miles;

-- Latest water level per well
CREATE OR REPLACE VIEW latest_water_levels AS
SELECT DISTINCT ON (well_id)
    well_id,
    measured_at,
    depth_to_water_ft,
    water_elevation_ft,
    measurement_method
FROM water_levels
ORDER BY well_id, measured_at DESC;

-- Site intelligence summary
CREATE OR REPLACE VIEW site_dashboard AS
SELECT
    s.id,
    s.name,
    s.project_code,
    s.county,
    s.capacity_mw,
    s.status,
    (SELECT COUNT(*) FROM wells_near_sites wns WHERE wns.site_id = s.id) AS nearby_wells,
    (SELECT COUNT(*) FROM tceq_permits p WHERE p.dc_site_id = s.id) AS permit_count,
    (SELECT COUNT(*) FROM alerts a WHERE a.dc_site_id = s.id AND a.acknowledged_at IS NULL) AS open_alerts
FROM dc_sites s;
