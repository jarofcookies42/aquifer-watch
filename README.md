# AquiferWatch — West Texas Water & Data Center Intelligence

**Live:** https://accomplished-respect-production-29c4.up.railway.app

A public-facing dashboard tracking water usage, aquifer levels, and data center development across West Texas. Brings transparency to the intersection of AI infrastructure growth and Ogallala Aquifer depletion.

## What it does

- Interactive map showing 5 data center sites (proposed through operational), 4,660 Ogallala wells, and 64 ERCOT generation queue projects
- Aquifer level trend charts with real TWDB groundwater measurements (72,000+ records)
- Water impact calculator: input MW capacity + cooling type, get estimated daily water consumption
- ERCOT generation queue breakdown by fuel type (solar, wind, battery, gas)
- Mobile-responsive — works on a phone in a council meeting

## Tech stack

- **Backend**: Python, FastAPI
- **Database**: PostgreSQL + PostGIS (Supabase)
- **Frontend**: Vanilla JS + Leaflet maps
- **Data**: TWDB ArcGIS API, TWDB bulk groundwater downloads, ERCOT via gridstatus
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

# 4. Run data pipelines
export DATABASE_URL="postgresql://postgres:dev@127.0.0.1:5433/wtx_intel"
python ingest_twdb.py                          # 4,660 Ogallala wells
python ingest_twdb_water_levels.py             # 72,908 real measurements
python ingest_ercot.py                         # 64 ERCOT generation projects

# 5. Start the dashboard
uvicorn api.main:app --reload --port 8000
# Open http://localhost:8000
```

## Data sources

| Source | Method | Status |
|--------|--------|--------|
| TWDB Groundwater Wells | ArcGIS REST API | Built |
| TWDB Water Levels | Bulk download (pipe-delimited) | Built |
| ERCOT Gen Queue | gridstatus Python lib | Built |
| TCEQ Air Permits | Central Registry scrape | Planned |
| County CAD | Web scrape | Planned |

## Tracked sites

| Code | Name | County | Capacity | Status |
|------|------|--------|----------|--------|
| HELIOS | Galaxy Helios | Dickens | 1.6 GW | Under construction |
| MATADOR | Fermi Project Matador | Carson | 11 GW | Permitted |
| LBK_NE | Lubbock NE (Prospective) | Lubbock | TBD | Rumored |
| OUTLAW | Outlaw Ventures | Lubbock | 600 MW | Filing detected |
| TERAWULF | TeraWulf / Fluidstack | Lubbock | TBD | Rumored |
