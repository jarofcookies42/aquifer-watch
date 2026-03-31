# AquiferWatch — West Texas Water & Data Center Intelligence Dashboard

## What this project is
A public-facing web dashboard that tracks water usage, aquifer levels, and data center development across West Texas. It brings transparency and data-driven decision-making to the intersection of AI infrastructure growth and water resource management.

This is NOT an anti-data center tool. It's pro-transparency, pro-smart-growth. The pitch: "Helping West Texas grow its tech economy without guessing about water." Data center companies should want this to exist.

## Who's building it
A CS grad student based in Lubbock, TX. Strong Python skills. First major web project being deployed. Building fast with Claude's help. Has a personal connection to a Lubbock City Council member who has publicly called for data center transparency.

## Tech stack
- **Backend**: Python, FastAPI (api/main.py serves everything)
- **Database**: PostgreSQL with PostGIS (spatial) + TimescaleDB (time-series), hosted on Supabase
- **Frontend**: Vanilla JS + Leaflet 1.9.4 for maps, custom Canvas 2D charts (no Chart.js/React)
- **Deployment**: Railway (app via Dockerfile) + Supabase (database)
- **Data pipeline**: Python ingestion scripts pulling from public APIs (cron-scheduled in production)
- **Key dependencies**: FastAPI, psycopg2-binary, requests, python-dateutil, click, pydantic

## Data sources

### Actively ingested
1. **TWDB Groundwater Wells** — ArcGIS FeatureServer REST API for Ogallala Aquifer monitoring wells within 30mi of tracked sites. `ingest_twdb.py`
2. **TWDB Water Levels** — Bulk download (GWDBDownload.zip, pipe-delimited WaterLevelsMajor.txt). Real depth-to-water measurements. `ingest_twdb_water_levels.py`
3. **ERCOT Generation Queue** — Via `gridstatus` Python library. Interconnection queue projects in West Texas counties. `ingest_ercot.py`
4. **ERCOT Energy Pricing** — Settlement point prices (SPPs) and wind/solar generation via `gridstatus`. Zones: HB_WEST, LZ_WEST, HB_NORTH, HB_BUSAVG. `ingest_ercot_pricing.py`
5. **NOAA Weather** — NWS REST API (api.weather.gov). Stations: KLBB (Lubbock), KAMA (Amarillo), KCDS (Childress), KBPG (Big Spring). No API key needed. `ingest_weather.py`
6. **US Drought Monitor** — County-level weekly drought status (D0-D4) from USDA/Columbia. `ingest_drought.py`
7. **TWDB Reservoir Levels** — Daily storage from waterdatafortexas.org + USGS NWIS gauges. `ingest_reservoir_levels.py`
8. **TWDB Water Use Survey** — Annual county-level water use by category (irrigation, municipal, manufacturing, etc.) and source type. `ingest_water_usage.py`
9. **USDA NASS Agriculture** — Irrigated acreage and crop production via Quick Stats API. Requires free `NASS_API_KEY`. `ingest_agriculture.py`

### Not yet built
- **TCEQ Permits** (B-tier): GIS shapefiles and Central Registry scraping. No REST API.
- **County CAD** (D-tier): No APIs. Would require web scraping or commercial aggregators.
- **ERCOT Large Load Queue** (C-tier): No public download. Monitor TAC meeting PDFs. SB 6 rulemaking may improve by late 2026.
- **SEC filings**: For public companies like Fermi America (NASDAQ: FRMI).

## Key data center sites being tracked

### Lubbock area
- **Lubbock NE (Zoning Case 3548)**: Calvano Development / Texas Solarworks. 936 acres off NE Loop 289. Zoning rejected, applicant withdrew Jan 2026. May resubmit. Coords: ~33.58°N, -101.80°W
- **Galaxy Helios** (Dickens County): Former bitcoin mining facility. $3.5B Phase 2 expansion (Helios 2/3/4). 1.6 GW approved by ERCOT. Coords: ~33.77°N, -100.78°W
- **Outlaw Ventures**: 600 MW, ~20 min north of Lubbock off I-27
- **TeraWulf / Fluidstack**: ~22 miles north of Lubbock

### Amarillo / Panhandle
- **Fermi America "Project Matador"** (Carson County): 5,769 acres adjacent to Pantex. 99-year TTU System lease. Up to 11 GW / 18M sq ft. Co-founded by Rick Perry. TCEQ air permits approved Feb 2026. MOU with MVM EGI for hybrid dry-wet cooling (non-binding, full system not complete until 2034). Asked Amarillo for 2.5M-10M gallons/day. Coords: ~35.33°N, -101.58°W

## Project structure
```
aquifer-watch/
├── CLAUDE.md                    # This file
├── README.md                    # Setup and deployment instructions
├── Dockerfile                   # Railway deployment (python:3.12-slim + uvicorn)
├── requirements.txt             # Python dependencies
├── schema.sql                   # PostgreSQL + PostGIS + TimescaleDB schema (17 tables)
│
├── api/
│   └── main.py                  # FastAPI app — 31 GET endpoints, serves frontend
│
├── frontend/
│   ├── index.html               # Single-page dashboard (vanilla HTML/CSS)
│   └── app.js                   # All frontend logic, map, charts, view switching
│
├── ingest_twdb.py               # TWDB groundwater well locations (ArcGIS API)
├── ingest_twdb_water_levels.py  # TWDB water level measurements (bulk download)
├── ingest_ercot.py              # ERCOT generation queue (gridstatus)
├── ingest_ercot_pricing.py      # ERCOT settlement prices + wind/solar gen (gridstatus)
├── ingest_weather.py            # NOAA NWS weather observations (api.weather.gov)
├── ingest_drought.py            # US Drought Monitor county status (USDA/Columbia)
├── ingest_reservoir_levels.py   # Reservoir storage levels (TWDB + USGS)
├── ingest_water_usage.py        # TWDB water use survey data (county/year/category)
├── ingest_agriculture.py        # USDA NASS irrigated acreage (Quick Stats API)
├── seed_water_levels.py         # Generates modeled water level data (for demo/dev)
│
├── data/                        # Local data cache / dry-run JSON output
├── tests/
└── venv/                        # Python virtual environment (gitignored)
```

## Database schema (17 tables)
- **dc_sites** — Tracked data center projects (5 sites)
- **wells** — TWDB groundwater monitoring wells
- **water_levels** — Depth-to-water time series (TimescaleDB hypertable)
- **water_quality** — TWDB water quality samples
- **ercot_gen_queue** — ERCOT generation interconnection queue
- **ercot_large_loads** — ERCOT large load tracking
- **tceq_permits** — TCEQ environmental permits
- **property_records** — County property/parcel records
- **alerts** — Intelligence alerts and events
- **ingestion_log** — Metadata for each ingestion run
- **water_usage** — TWDB water use survey (county x year x category)
- **agricultural_data** — USDA NASS irrigated acreage and crop production
- **reservoirs** — Surface water reservoirs (metadata)
- **reservoir_levels** — Reservoir storage time series (TimescaleDB hypertable)
- **weather_observations** — NOAA weather station observations
- **drought_status** — US Drought Monitor county-level status (D0-D4)
- **reservoir_evaporation** — TWDB evaporation data

## API endpoints (31 GET routes in api/main.py)

### Data center sites
- `GET /api/sites` — All tracked sites with coords, capacity, status
- `GET /api/sites/{site_id}` — Single site details with nearby well count

### Wells & aquifer
- `GET /api/wells` — Wells list with optional site proximity filter
- `GET /api/wells/geojson` — Wells as GeoJSON for map rendering
- `GET /api/water-levels` — Annual average depth-to-water trends near a site

### Reservoirs
- `GET /api/reservoirs` — All reservoirs with latest storage level
- `GET /api/reservoirs/{id}` — Single reservoir with nearby DC sites
- `GET /api/reservoirs/{id}/levels` — Time-series storage (daily/monthly/annual)
- `GET /api/reservoir-summary` — Latest snapshot for all reservoirs

### Water usage & impact
- `GET /api/water-usage` — TWDB water use survey with filters
- `GET /api/water-usage/summary` — By-category aggregation for recent 5 years
- `GET /api/water-usage/trends` — Year-over-year regional totals
- `GET /api/water-impact` — Calculator: MW + cooling type -> gallons/day

### Agriculture
- `GET /api/agriculture` — USDA NASS irrigated acreage data
- `GET /api/agriculture/summary` — Regional overview by crop and year

### ERCOT energy
- `GET /api/ercot` — Generation queue with county/fuel filters
- `GET /api/ercot/summary` — Aggregate stats by fuel type
- `GET /api/ercot/geojson` — Projects as GeoJSON

### Energy market
- `GET /api/energy/pricing` — Settlement point prices by zone (raw/hourly/daily)
- `GET /api/energy/generation` — Wind/solar output with forecasts
- `GET /api/energy/summary` — Current prices, generation snapshot

### Weather & drought
- `GET /api/weather/current` — Latest observation per station
- `GET /api/weather/history` — Historical observations (default 72h)
- `GET /api/drought/current` — Latest drought status per county
- `GET /api/drought/history` — Drought time series by FIPS code
- `GET /api/drought/summary` — Regional drought overview

### Dashboard & views
- `GET /api/dashboard` — Header stats (site count, wells, counties, ERCOT capacity)
- `GET /api/views/policy-summary` — Policy & Planning view data
- `GET /api/views/industry-summary` — Industry & Economic view data
- `GET /api/views/public-summary` — Public/Learn view data (with household/farm comparisons)
- `GET /api/compare/sites` — Side-by-side site comparison

## Frontend views (audience switcher)
The dashboard has 4 audience-specific views, switched via tabs in the header:

1. **Full Dashboard** — Complete view with all panels: sites list, aquifer trend chart, ERCOT queue, water impact calculator
2. **Policy & Planning** — Water budget overview, DC pipeline by status, aquifer health metrics, depletion projections
3. **Industry & Economic** — Energy market analysis, water availability scoring by county, site comparison, ERCOT data
4. **Public / Learn** — Simplified facts, city-level data, household/farm equivalents, glossary of terms

Map layers: Sites (color-coded by status), Wells, ERCOT projects, Reservoirs, Drought overlay.

## Coding guidelines
- Write production-quality code with error handling and logging
- Use type hints in Python
- All ingestion scripts support `--dry-run` mode (output to JSON, no DB required)
- Upsert pattern for all ingestion (safe to re-run)
- Handle API pagination (TWDB caps at 1,000 records per request)
- Use environment variables for database connection strings, never hardcode
- Keep dependencies minimal — don't add libraries unless there's a clear reason
- Write docstrings for functions that interact with external APIs
- Git commit messages should be descriptive

## Feature status

### Phase 1 — MVP (DONE)
1. Interactive map showing data center locations (proposed, permitted, operational)
2. Aquifer level time series from nearest TWDB monitoring wells
3. Water impact calculator (MW capacity + cooling type -> estimated gallons/day)
4. Public data center tracker table (who, where, capacity, water commitments)
5. Mobile-friendly layout

### Phase 2 — Surface water reservoirs (DONE)
- Reservoir locations on map with storage levels
- Time-series charts for reservoir percent-full
- Reservoir summary endpoint for dashboard

### Phase 3 — ERCOT energy market data (DONE)
- Settlement point pricing for West Texas zones
- Wind and solar generation tracking
- Energy market summary panel

### Phase 4 — Weather & drought integration (DONE)
- NOAA weather observations from 4 West Texas stations
- US Drought Monitor county-level status (D0-D4)
- Weather and drought panels in dashboard
- Drought overlay on map

### Phase 5 — Water usage & agriculture (DONE)
- TWDB water use survey data by county/category/year
- USDA NASS irrigated acreage for major crops
- Water usage trends and agriculture summary endpoints

### Phase 6 — Multi-audience views (DONE)
- Policy & Planning view (water budget, pipeline, aquifer health)
- Industry & Economic view (energy market, water availability, site comparison)
- Public / Learn view (simplified facts, comparisons, glossary)
- View switcher in header

### Planned
- TCEQ permit scraping
- County CAD property records
- ERCOT large load queue monitoring
- Alerting system for threshold crossings
- Historical trend analysis and projections

## Important context
- The Ogallala Aquifer is being drained faster than it recharges across West Texas
- Texas water planning updates every 5 years — too slow for data center growth
- Data centers are not consistently required to disclose water consumption
- "Closed-loop cooling" claims need scrutiny — Fermi's MOU is non-binding and the full system won't be ready until 2034
- Agricultural users and municipalities are the primary competing water users
- The product serves municipal leaders, water districts, economic development offices, data center developers doing community engagement, and informed residents
