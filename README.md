# West Texas Water & Data Center Intelligence Dashboard

## MVP Data Layer

### Quick Start

```bash
# 1. Set up PostgreSQL with PostGIS + TimescaleDB
#    (Docker is easiest for local dev)
docker run -d --name wtx-db \
  -e POSTGRES_DB=wtx_intel \
  -e POSTGRES_PASSWORD=dev \
  -p 5432:5432 \
  timescale/timescaledb-ha:pg16

# 2. Install PostGIS in the database
docker exec -it wtx-db psql -U postgres -d wtx_intel \
  -c "CREATE EXTENSION IF NOT EXISTS postgis;"

# 3. Run schema migration
docker exec -i wtx-db psql -U postgres -d wtx_intel < schema.sql

# 4. Install Python dependencies
pip install requests psycopg2-binary python-dateutil

# 5. Test the TWDB pipeline (dry run — no DB needed)
python ingest_twdb.py --dry-run

# 6. Run for real
export DATABASE_URL="postgresql://postgres:dev@localhost:5432/wtx_intel"
python ingest_twdb.py
```

### Project Structure

```
wtx-intel/
├── schema.sql           # PostgreSQL + PostGIS + TimescaleDB schema
├── ingest_twdb.py       # TWDB Ogallala well data ingestion
├── README.md            # This file
└── (future)
    ├── ingest_ercot.py  # ERCOT generation queue via gridstatus
    ├── scrape_tceq.py   # TCEQ Central Registry + air permits
    ├── scrape_cad.py    # County appraisal district data
    └── alerts.py        # Detection rules + notifications
```

### Data Sources

| Source | Method | Frequency | Status |
|--------|--------|-----------|--------|
| TWDB Groundwater (Ogallala) | ArcGIS REST API | Daily | ✅ Built |
| ERCOT Gen Queue | gridstatus Python lib | Monthly | 🔜 Next |
| ERCOT Large Load | PDF scraping + manual | Monthly | 🔜 Planned |
| TCEQ Air Permits | Central Registry scrape | Weekly | 🔜 Planned |
| TCEQ Water Rights | File download | Monthly | 🔜 Planned |
| County CAD | Web scrape + bulk | Monthly | 🔜 Planned |

### Tracked Sites

| Code | Name | County | Capacity | Status |
|------|------|--------|----------|--------|
| HELIOS | Galaxy Helios | Dickens | 1.6 GW | Under construction |
| MATADOR | Fermi Project Matador | Carson | 11 GW | Permitted |
| LBK_NE | Lubbock NE (Prospective) | Lubbock | TBD | Rumored |

### Key API Endpoints

- **TWDB FeatureServer**: `services.twdb.texas.gov/arcgis/rest/services/Public/TWDB_Groundwater_database/FeatureServer/0/query`
- **TWDB Bulk Download**: `txwaterdatahub.org/dataset/groundwater-database`
- **TCEQ Central Registry**: `www15.tceq.texas.gov/crpub/`
- **ERCOT GIS Reports**: `ercot.com/gridinfo/resource`
- **gridstatus docs**: `opensource.gridstatus.io`
