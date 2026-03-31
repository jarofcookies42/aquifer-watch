# AquiferWatch — West Texas Water & Data Center Intelligence

**Live:** https://accomplished-respect-production-29c4.up.railway.app

A public-facing dashboard tracking water usage, aquifer levels, and data center development across West Texas. Brings transparency to the intersection of AI infrastructure growth and Ogallala Aquifer depletion.

## What it does

- Interactive map showing 5 data center sites (proposed through operational), 4,660+ Ogallala wells, ERCOT generation projects, reservoirs, and drought overlay
- Aquifer level trend charts with real TWDB groundwater measurements (72,000+ records)
- Water impact calculator: input MW capacity + cooling type, get estimated daily water consumption
- ERCOT generation queue breakdown by fuel type (solar, wind, battery, gas)
- Energy market data: real-time settlement point pricing and wind/solar generation for West Texas
- Weather conditions from 4 NOAA stations (Lubbock, Amarillo, Childress, Big Spring)
- US Drought Monitor status (D0-D4) for all tracked West Texas counties
- Surface water reservoir storage levels from TWDB and USGS
- TWDB water use survey data and USDA irrigated agriculture trends
- Four audience views: Full Dashboard, Policy & Planning, Industry & Economic, Public/Learn
- Mobile-responsive — works on a phone in a council meeting

## Tech stack

- **Backend**: Python, FastAPI (31 API endpoints)
- **Database**: PostgreSQL + PostGIS + TimescaleDB (Supabase)
- **Frontend**: Vanilla JS + Leaflet maps + custom Canvas charts
- **Deployment**: Railway (app) + Supabase (database)

## Local development

```bash
# 1. Clone and set up
git clone https://github.com/jarofcookies42/aquifer-watch.git
cd aquifer-watch
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 2. Start local PostgreSQL (Docker)
docker run -d --name wtx-db \
  -e POSTGRES_DB=wtx_intel -e POSTGRES_PASSWORD=dev \
  -p 5433:5432 timescale/timescaledb-ha:pg16

# 3. Load schema
docker exec -i wtx-db psql -U postgres -d wtx_intel < schema.sql

# 4. Set database URL
export DATABASE_URL="postgresql://postgres:dev@127.0.0.1:5433/wtx_intel"

# 5. Run core data pipelines
python ingest_twdb.py                    # Ogallala wells (ArcGIS API)
python ingest_twdb_water_levels.py       # Water level measurements (bulk download)
python ingest_ercot.py                   # ERCOT generation queue (gridstatus)

# 6. Run extended data pipelines
python ingest_weather.py                 # NOAA weather observations
python ingest_drought.py                 # US Drought Monitor status
python ingest_ercot_pricing.py           # ERCOT settlement prices + generation
python ingest_reservoir_levels.py        # Reservoir storage levels
python ingest_water_usage.py             # TWDB water use survey data
python ingest_agriculture.py             # USDA NASS irrigated acreage (needs NASS_API_KEY)

# 7. Start the dashboard
uvicorn api.main:app --reload --port 8000
# Open http://localhost:8000
```

All ingestion scripts support `--dry-run` (outputs JSON, no DB needed) and are safe to re-run (upsert pattern).

For agriculture data, get a free API key at https://quickstats.nass.usda.gov/api/ and export it:
```bash
export NASS_API_KEY="your-key-here"
```

## Data sources

| Source | Script | Method | Status |
|--------|--------|--------|--------|
| TWDB Groundwater Wells | `ingest_twdb.py` | ArcGIS REST API | Built |
| TWDB Water Levels | `ingest_twdb_water_levels.py` | Bulk download (pipe-delimited) | Built |
| ERCOT Gen Queue | `ingest_ercot.py` | gridstatus Python lib | Built |
| ERCOT Energy Pricing | `ingest_ercot_pricing.py` | gridstatus Python lib | Built |
| NOAA Weather | `ingest_weather.py` | api.weather.gov REST API | Built |
| US Drought Monitor | `ingest_drought.py` | USDA/Columbia API | Built |
| TWDB Reservoirs | `ingest_reservoir_levels.py` | waterdatafortexas.org + USGS NWIS | Built |
| TWDB Water Use Survey | `ingest_water_usage.py` | TWDB WUD CSV download | Built |
| USDA NASS Agriculture | `ingest_agriculture.py` | Quick Stats API | Built |
| TCEQ Air Permits | — | Central Registry scrape | Planned |
| County CAD | — | Web scrape | Planned |

## Tracked sites

| Code | Name | County | Capacity | Status |
|------|------|--------|----------|--------|
| HELIOS | Galaxy Helios | Dickens | 1.6 GW | Under construction |
| MATADOR | Fermi Project Matador | Carson | 11 GW | Permitted |
| LBK_NE | Lubbock NE (Prospective) | Lubbock | TBD | Rumored |
| OUTLAW | Outlaw Ventures | Lubbock | 600 MW | Filing detected |
| TERAWULF | TeraWulf / Fluidstack | Lubbock | TBD | Rumored |
