-- West Texas Water & Data Center Intelligence Dashboard
-- Database Schema v0.2
-- Requires: PostgreSQL 15+, PostGIS, TimescaleDB
-- Safe to re-run: uses IF NOT EXISTS / ON CONFLICT throughout.

-- ============================================================
-- Extensions
-- ============================================================
CREATE EXTENSION IF NOT EXISTS postgis;
DO $$ BEGIN
    CREATE EXTENSION IF NOT EXISTS timescaledb;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'TimescaleDB not available, skipping (water_levels will use regular table)';
END $$;
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

-- Convert to TimescaleDB hypertable if available
DO $$ BEGIN
    PERFORM create_hypertable('water_levels', 'measured_at', if_not_exists => TRUE);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'TimescaleDB not available, water_levels remains a regular table';
END $$;

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
-- Phase 5: Water usage & agriculture
-- ============================================================

-- Extend data_source enum
DO $$ BEGIN ALTER TYPE data_source ADD VALUE IF NOT EXISTS 'twdb_wud'; EXCEPTION WHEN others THEN NULL; END $$;
DO $$ BEGIN ALTER TYPE data_source ADD VALUE IF NOT EXISTS 'usda_nass'; EXCEPTION WHEN others THEN NULL; END $$;

-- Water use category
DO $$ BEGIN
    CREATE TYPE water_use_category AS ENUM (
        'municipal', 'irrigation', 'manufacturing', 'mining', 'livestock', 'steam_electric'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Water source type
DO $$ BEGIN
    CREATE TYPE water_source_type AS ENUM ('groundwater', 'surface_water', 'total');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- TWDB Water Use Survey estimates (county × year × category × source_type)
CREATE TABLE IF NOT EXISTS water_usage (
    id              SERIAL PRIMARY KEY,
    county          TEXT NOT NULL,
    county_fips     TEXT,
    year            INTEGER NOT NULL,
    category        water_use_category NOT NULL,
    source_type     water_source_type NOT NULL,
    volume_acre_ft  NUMERIC,             -- acre-feet
    aquifer_name    TEXT,                -- groundwater source name where known
    notes           TEXT,
    raw_json        JSONB,
    ingested_at     TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE(county_fips, year, category, source_type)
);

CREATE INDEX IF NOT EXISTS idx_wu_county   ON water_usage(county);
CREATE INDEX IF NOT EXISTS idx_wu_year     ON water_usage(year);
CREATE INDEX IF NOT EXISTS idx_wu_category ON water_usage(category);
CREATE INDEX IF NOT EXISTS idx_wu_fips     ON water_usage(county_fips);

-- USDA NASS irrigated acreage and crop production (county × year × crop)
CREATE TABLE IF NOT EXISTS agricultural_data (
    id               SERIAL PRIMARY KEY,
    county           TEXT NOT NULL,
    county_fips      TEXT,
    year             INTEGER NOT NULL,
    crop_type        TEXT NOT NULL,      -- e.g. COTTON, WHEAT, CORN, SORGHUM
    acres_irrigated  NUMERIC,            -- irrigated harvested acres
    acres_harvested  NUMERIC,            -- total harvested acres
    production_value NUMERIC,            -- e.g. bushels, bales, cwt
    production_units TEXT,               -- e.g. "BU", "480 LB BALES"
    source           TEXT,               -- SURVEY or CENSUS
    raw_json         JSONB,
    ingested_at      TIMESTAMPTZ DEFAULT now(),
    updated_at       TIMESTAMPTZ DEFAULT now(),
    UNIQUE(county_fips, year, crop_type)
);

CREATE INDEX IF NOT EXISTS idx_ag_county ON agricultural_data(county);
CREATE INDEX IF NOT EXISTS idx_ag_year   ON agricultural_data(year);
CREATE INDEX IF NOT EXISTS idx_ag_crop   ON agricultural_data(crop_type);
CREATE INDEX IF NOT EXISTS idx_ag_fips   ON agricultural_data(county_fips);

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

-- ============================================================
-- Surface Water: Reservoirs
-- ============================================================

-- Extend data_source enum for new ingestion sources (safe to re-run)
DO $$ BEGIN
    ALTER TYPE data_source ADD VALUE 'twdb_wdft';
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TYPE data_source ADD VALUE 'usgs_nwis';
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS reservoirs (
    id                          SERIAL PRIMARY KEY,
    name                        TEXT NOT NULL UNIQUE,
    slug                        TEXT UNIQUE,             -- WDFT URL identifier
    county                      TEXT NOT NULL,
    location                    GEOMETRY(Point, 4326),
    managing_authority          TEXT,
    conservation_storage_acft   NUMERIC,                 -- full pool storage capacity
    dead_pool_acft              NUMERIC,                 -- minimum operable pool
    surface_area_acres          NUMERIC,                 -- at conservation pool
    usgs_site_no                TEXT,                    -- USGS NWIS gauge ID if monitored
    wdft_reservoir_id           TEXT,                    -- Water Data for Texas reservoir ID
    notes                       TEXT,
    created_at                  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_reservoirs_location ON reservoirs USING GIST(location);
CREATE INDEX IF NOT EXISTS idx_reservoirs_county ON reservoirs(county);
CREATE INDEX IF NOT EXISTS idx_reservoirs_slug ON reservoirs(slug);

-- Seed the eight target reservoirs (safe to re-run)
INSERT INTO reservoirs (
    name, slug, county, location, managing_authority,
    conservation_storage_acft, dead_pool_acft, surface_area_acres,
    usgs_site_no, wdft_reservoir_id, notes
) VALUES
    ('Lake Meredith',
     'lake-meredith',
     'Potter/Hutchinson/Moore',
     ST_SetSRID(ST_MakePoint(-101.567, 35.643), 4326),
     'Bureau of Reclamation / Canadian River Municipal Water Authority',
     863000, 14000, 21613,
     '07227500', 'meredith',
     'Federal reservoir on Canadian River. Primary municipal supply for Amarillo region. Storage well below design capacity due to sedimentation and drought.'),

    ('Lake Alan Henry',
     'lake-alan-henry',
     'Garza',
     ST_SetSRID(ST_MakePoint(-101.044, 33.017), 4326),
     'City of Lubbock',
     116600, 0, 2880,
     NULL, 'alan-henry',
     'Lubbock primary backup water supply on Double Mountain Fork Brazos River. Completed 1994.'),

    ('Lake J.B. Thomas',
     'lake-jb-thomas',
     'Scurry/Mitchell/Howard',
     ST_SetSRID(ST_MakePoint(-101.096, 32.558), 4326),
     'Colorado River Municipal Water District',
     204000, 0, 7820,
     NULL, 'jb-thomas',
     'CRMWD supply reservoir on Colorado River. Serves Midland, Odessa, Big Spring.'),

    ('O.H. Ivie Reservoir',
     'o-h-ivie',
     'Concho/Coleman/McCulloch',
     ST_SetSRID(ST_MakePoint(-99.707, 31.553), 4326),
     'Colorado River Municipal Water District',
     554000, 0, 19149,
     NULL, 'o-h-ivie',
     'Largest CRMWD reservoir. Critical supply for Midland, Odessa, Big Spring, San Angelo region.'),

    ('White River Lake',
     'white-river-lake',
     'Crosby',
     ST_SetSRID(ST_MakePoint(-100.837, 33.430), 4326),
     'White River Municipal Water District',
     13756, 0, 1410,
     NULL, 'white-river',
     'Serves several small Panhandle communities. Sensitive to drought — frequently at low levels.'),

    ('Mackenzie Reservoir',
     'mackenzie-reservoir',
     'Briscoe',
     ST_SetSRID(ST_MakePoint(-100.887, 34.270), 4326),
     'Mackenzie Municipal Water Authority',
     46200, 0, 1025,
     NULL, 'mackenzie',
     'Serves Lubbock supplemental supply, Floydada, Lockney, and surrounding communities on Tule Creek.'),

    ('Greenbelt Lake',
     'greenbelt-lake',
     'Donley',
     ST_SetSRID(ST_MakePoint(-100.678, 34.797), 4326),
     'Greenbelt Municipal & Industrial Water Authority',
     37370, 0, 1987,
     NULL, 'greenbelt',
     'Serves Clarendon and surrounding Donley/Hall counties on Salt Fork Red River.'),

    ('Palo Duro Reservoir',
     'palo-duro-reservoir',
     'Hansford',
     ST_SetSRID(ST_MakePoint(-101.224, 36.467), 4326),
     'North Canadian River Municipal Water Authority',
     47070, 0, 2410,
     NULL, 'palo-duro',
     'Northernmost Texas Panhandle reservoir. Serves Spearman, Gruver, and nearby communities.')
ON CONFLICT (name) DO NOTHING;

-- ============================================================
-- Surface Water: Reservoir level time-series (hypertable)
-- ============================================================

CREATE TABLE IF NOT EXISTS reservoir_levels (
    reservoir_id                INTEGER NOT NULL REFERENCES reservoirs(id),
    measured_at                 TIMESTAMPTZ NOT NULL,
    percent_full                NUMERIC,                 -- % of conservation storage
    conservation_storage_acft   NUMERIC,                 -- measured storage (acre-feet)
    water_elevation_ft          NUMERIC,                 -- elevation above NGVD 1929 / NAVD 88
    source                      TEXT NOT NULL,           -- 'twdb_wdft', 'usgs_nwis', 'manual'
    ingested_at                 TIMESTAMPTZ DEFAULT now()
);

-- TimescaleDB hypertable (falls back to regular table if TimescaleDB unavailable)
DO $$ BEGIN
    PERFORM create_hypertable('reservoir_levels', 'measured_at', if_not_exists => TRUE);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'TimescaleDB not available, reservoir_levels remains a regular table';
END $$;

-- Unique constraint: one record per reservoir per day per source
CREATE UNIQUE INDEX IF NOT EXISTS idx_rl_unique ON reservoir_levels(reservoir_id, measured_at, source);
CREATE INDEX IF NOT EXISTS idx_rl_reservoir_time ON reservoir_levels(reservoir_id, measured_at DESC);

-- ============================================================
-- Convenience views for reservoirs
-- ============================================================

-- Latest level per reservoir (most recent measurement)
CREATE OR REPLACE VIEW latest_reservoir_levels AS
SELECT DISTINCT ON (rl.reservoir_id)
    rl.reservoir_id,
    r.name,
    r.slug,
    r.county,
    r.managing_authority,
    r.conservation_storage_acft   AS capacity_acft,
    ST_Y(r.location)              AS lat,
    ST_X(r.location)              AS lon,
    rl.measured_at,
    rl.percent_full,
    rl.conservation_storage_acft  AS current_storage_acft,
    rl.water_elevation_ft,
    rl.source
FROM reservoir_levels rl
JOIN reservoirs r ON r.id = rl.reservoir_id
ORDER BY rl.reservoir_id, rl.measured_at DESC;

-- Reservoirs near tracked data center sites (within 100 miles)
CREATE OR REPLACE VIEW reservoirs_near_sites AS
SELECT
    r.id   AS reservoir_id,
    r.name AS reservoir_name,
    r.slug,
    r.county,
    s.id   AS site_id,
    s.name AS site_name,
    s.project_code,
    ST_Distance(r.location::geography, s.location::geography) / 1609.34 AS distance_miles
FROM reservoirs r
CROSS JOIN dc_sites s
WHERE ST_DWithin(r.location::geography, s.location::geography, 160934)  -- 100 miles in meters
ORDER BY s.id, distance_miles;

-- ============================================================
-- Phase 3: ERCOT Energy Market Tables
-- ============================================================

-- Settlement point prices (15-min real-time market)
CREATE TABLE IF NOT EXISTS ercot_pricing (
    ts              timestamptz    NOT NULL,
    settlement_point text          NOT NULL,
    price_per_mwh   numeric(10,2),
    ingested_at     timestamptz    DEFAULT now(),
    PRIMARY KEY (ts, settlement_point)
);
CREATE INDEX IF NOT EXISTS idx_ercot_pricing_point ON ercot_pricing (settlement_point, ts DESC);

-- Wind/solar generation output (5-min fuel mix)
CREATE TABLE IF NOT EXISTS ercot_generation (
    ts           timestamptz  NOT NULL,
    fuel_type    text         NOT NULL,
    output_mw    numeric(10,2),
    forecast_mw  numeric(10,2),
    ingested_at  timestamptz  DEFAULT now(),
    PRIMARY KEY (ts, fuel_type)
);
CREATE INDEX IF NOT EXISTS idx_ercot_generation_fuel ON ercot_generation (fuel_type, ts DESC);

-- Latest price per settlement point
CREATE OR REPLACE VIEW latest_ercot_pricing AS
SELECT DISTINCT ON (settlement_point)
    settlement_point,
    ts,
    price_per_mwh
FROM ercot_pricing
ORDER BY settlement_point, ts DESC;

-- Daily average pricing (useful for trend charts)
CREATE OR REPLACE VIEW daily_avg_ercot_pricing AS
SELECT
    date_trunc('day', ts)                                   AS day,
    settlement_point,
    AVG(price_per_mwh)                                      AS avg_price,
    MIN(price_per_mwh)                                      AS min_price,
    MAX(price_per_mwh)                                      AS max_price,
    COUNT(*) FILTER (WHERE price_per_mwh < 0)               AS negative_hours
FROM ercot_pricing
GROUP BY date_trunc('day', ts), settlement_point
ORDER BY day DESC;

-- Latest generation snapshot per fuel type
CREATE OR REPLACE VIEW latest_ercot_generation AS
SELECT DISTINCT ON (fuel_type)
    fuel_type,
    ts,
    output_mw,
    forecast_mw
FROM ercot_generation
ORDER BY fuel_type, ts DESC;

-- ============================================================
-- Phase 4: Weather & Drought Data
-- ============================================================

-- Extend data_source enum with new sources
DO $$ BEGIN ALTER TYPE data_source ADD VALUE IF NOT EXISTS 'noaa_nws';        EXCEPTION WHEN OTHERS THEN NULL; END $$;
DO $$ BEGIN ALTER TYPE data_source ADD VALUE IF NOT EXISTS 'drought_monitor';  EXCEPTION WHEN OTHERS THEN NULL; END $$;
DO $$ BEGIN ALTER TYPE data_source ADD VALUE IF NOT EXISTS 'twdb_evaporation'; EXCEPTION WHEN OTHERS THEN NULL; END $$;

-- Weather observations from NOAA National Weather Service
CREATE TABLE IF NOT EXISTS weather_observations (
    id                  SERIAL PRIMARY KEY,
    station_id          TEXT NOT NULL,          -- ICAO station code, e.g. "KLBB"
    station_name        TEXT,
    observed_at         TIMESTAMPTZ NOT NULL,
    temperature_f       NUMERIC,
    dewpoint_f          NUMERIC,
    humidity_pct        NUMERIC,
    wind_speed_mph      NUMERIC,
    wind_direction_deg  INTEGER,
    wind_gust_mph       NUMERIC,
    precip_last_hour_in NUMERIC,
    precip_last_6hr_in  NUMERIC,
    precip_last_24hr_in NUMERIC,
    visibility_miles    NUMERIC,
    pressure_mb         NUMERIC,
    conditions          TEXT,                   -- e.g. "Partly Cloudy"
    raw_json            JSONB,
    ingested_at         TIMESTAMPTZ DEFAULT now(),
    UNIQUE (station_id, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_wx_station_time ON weather_observations(station_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_wx_time ON weather_observations(observed_at DESC);

-- US Drought Monitor weekly county-level drought status
CREATE TABLE IF NOT EXISTS drought_status (
    id              SERIAL PRIMARY KEY,
    county_fips     TEXT NOT NULL,          -- 5-digit FIPS, e.g. "48303"
    county_name     TEXT,
    state_abbr      TEXT DEFAULT 'TX',
    valid_date      DATE NOT NULL,          -- Tuesday release date
    d0_pct          NUMERIC,               -- Abnormally Dry (% of county area)
    d1_pct          NUMERIC,               -- Moderate Drought
    d2_pct          NUMERIC,               -- Severe Drought
    d3_pct          NUMERIC,               -- Extreme Drought
    d4_pct          NUMERIC,               -- Exceptional Drought
    no_drought_pct  NUMERIC,               -- No drought
    worst_category  TEXT,                  -- "None", "D0", "D1", "D2", "D3", "D4"
    ingested_at     TIMESTAMPTZ DEFAULT now(),
    UNIQUE (county_fips, valid_date)
);

CREATE INDEX IF NOT EXISTS idx_drought_county_date ON drought_status(county_fips, valid_date DESC);
CREATE INDEX IF NOT EXISTS idx_drought_date ON drought_status(valid_date DESC);

-- TWDB evaporation data for tracked reservoirs (monthly/annual rates)
CREATE TABLE IF NOT EXISTS reservoir_evaporation (
    id                  SERIAL PRIMARY KEY,
    reservoir_name      TEXT NOT NULL,
    reservoir_id        TEXT,              -- TWDB reservoir identifier
    county              TEXT,
    period_type         TEXT DEFAULT 'monthly',   -- "monthly" or "annual"
    period_start        DATE NOT NULL,
    period_end          DATE,
    evaporation_rate_in NUMERIC,          -- inches
    evaporation_af      NUMERIC,          -- acre-feet
    surface_area_acres  NUMERIC,
    raw_json            JSONB,
    ingested_at         TIMESTAMPTZ DEFAULT now(),
    UNIQUE (reservoir_id, period_start, period_type)
);

CREATE INDEX IF NOT EXISTS idx_evap_reservoir ON reservoir_evaporation(reservoir_name, period_start DESC);

-- ============================================================
-- Phase 4 convenience views
-- ============================================================

-- Latest weather reading per station
CREATE OR REPLACE VIEW latest_weather AS
SELECT DISTINCT ON (station_id)
    station_id,
    station_name,
    observed_at,
    temperature_f,
    dewpoint_f,
    humidity_pct,
    wind_speed_mph,
    wind_direction_deg,
    wind_gust_mph,
    precip_last_24hr_in,
    conditions
FROM weather_observations
ORDER BY station_id, observed_at DESC;

-- Latest drought status per county
CREATE OR REPLACE VIEW latest_drought AS
SELECT DISTINCT ON (county_fips)
    county_fips,
    county_name,
    state_abbr,
    valid_date,
    d0_pct,
    d1_pct,
    d2_pct,
    d3_pct,
    d4_pct,
    no_drought_pct,
    worst_category
FROM drought_status
ORDER BY county_fips, valid_date DESC;

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

-- Water use totals by county and year (pivoted by category, total source only)
CREATE OR REPLACE VIEW water_usage_by_county AS
SELECT
    county,
    county_fips,
    year,
    COALESCE(SUM(CASE WHEN category = 'irrigation'     THEN volume_acre_ft END), 0) AS irrigation_af,
    COALESCE(SUM(CASE WHEN category = 'municipal'      THEN volume_acre_ft END), 0) AS municipal_af,
    COALESCE(SUM(CASE WHEN category = 'manufacturing'  THEN volume_acre_ft END), 0) AS manufacturing_af,
    COALESCE(SUM(CASE WHEN category = 'mining'         THEN volume_acre_ft END), 0) AS mining_af,
    COALESCE(SUM(CASE WHEN category = 'livestock'      THEN volume_acre_ft END), 0) AS livestock_af,
    COALESCE(SUM(CASE WHEN category = 'steam_electric' THEN volume_acre_ft END), 0) AS steam_electric_af,
    COALESCE(SUM(volume_acre_ft), 0) AS total_af
FROM water_usage
WHERE source_type = 'total'
GROUP BY county, county_fips, year;

-- Regional water use totals by year (all target counties combined)
CREATE OR REPLACE VIEW regional_water_usage AS
SELECT
    year,
    COALESCE(SUM(CASE WHEN category = 'irrigation'     THEN volume_acre_ft END), 0) AS irrigation_af,
    COALESCE(SUM(CASE WHEN category = 'municipal'      THEN volume_acre_ft END), 0) AS municipal_af,
    COALESCE(SUM(CASE WHEN category = 'manufacturing'  THEN volume_acre_ft END), 0) AS manufacturing_af,
    COALESCE(SUM(CASE WHEN category = 'mining'         THEN volume_acre_ft END), 0) AS mining_af,
    COALESCE(SUM(CASE WHEN category = 'livestock'      THEN volume_acre_ft END), 0) AS livestock_af,
    COALESCE(SUM(CASE WHEN category = 'steam_electric' THEN volume_acre_ft END), 0) AS steam_electric_af,
    COALESCE(SUM(volume_acre_ft), 0) AS total_af
FROM water_usage
WHERE source_type = 'total'
GROUP BY year
ORDER BY year;

-- Agricultural irrigated acreage summary by county and year
CREATE OR REPLACE VIEW ag_water_demand AS
SELECT
    county,
    county_fips,
    year,
    SUM(acres_irrigated) AS total_irrigated_acres,
    SUM(acres_harvested) AS total_harvested_acres,
    COUNT(DISTINCT crop_type) AS crop_types
FROM agricultural_data
GROUP BY county, county_fips, year;
