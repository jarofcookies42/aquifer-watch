# AquiferWatch — West Texas Water & Data Center Intelligence Dashboard

## What this project is
A public-facing web dashboard that tracks water usage, aquifer levels, and data center development across West Texas. It brings transparency and data-driven decision-making to the intersection of AI infrastructure growth and water resource management.

This is NOT an anti-data center tool. It's pro-transparency, pro-smart-growth. The pitch: "Helping West Texas grow its tech economy without guessing about water." Data center companies should want this to exist.

## Who's building it
A CS grad student based in Lubbock, TX. Strong Python skills. First major web project being deployed. Building fast with Claude's help. Has a personal connection to a Lubbock City Council member who has publicly called for data center transparency.

## Tech stack
- **Backend**: Python (FastAPI or Flask), scheduled data ingestion scripts
- **Database**: PostgreSQL with PostGIS (spatial) + TimescaleDB (time-series)
- **Frontend**: React or simple HTML/JS with Leaflet/Mapbox for maps
- **Deployment**: Vercel, Railway, or similar cheap/free tier
- **Data pipeline**: Cron-scheduled Python scripts pulling from public APIs

## Data sources
1. **TWDB Groundwater** (A-tier): ArcGIS FeatureServer REST API for Ogallala Aquifer well data. Bulk nightly downloads also available. `ingest_twdb.py` handles this.
2. **ERCOT Generation Queue** (A-tier): Monthly Excel downloads + `gridstatus` Python library. Tracks power generation co-located with data centers.
3. **ERCOT Large Load Queue** (C-tier): No public downloadable format for individual projects. Monitor TAC meeting PDFs and board presentations. SB 6 rulemaking may improve this by late 2026.
4. **TCEQ Permits** (B-tier): Downloadable GIS shapefiles and Central Registry web database (scrapeable). No REST API.
5. **County CAD** (D-tier): No APIs. Web scraping or commercial aggregators for property records.
6. **SEC filings**: For public companies like Fermi America (NASDAQ: FRMI).

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
├── CLAUDE.md              # This file
├── README.md              # Setup instructions
├── schema.sql             # PostgreSQL + PostGIS + TimescaleDB schema
├── ingest_twdb.py         # TWDB groundwater data ingestion script
├── ingest_ercot.py        # ERCOT generation queue ingestion (to build)
├── ingest_tceq.py         # TCEQ permit scraper (to build)
├── api/                   # FastAPI backend (to build)
│   ├── main.py
│   ├── routes/
│   └── models/
├── frontend/              # Web dashboard (to build)
│   ├── index.html
│   ├── map.js
│   └── charts.js
├── data/                  # Local data cache / dry-run JSON output
├── tests/
└── docker-compose.yml     # PostGIS + TimescaleDB local dev setup
```

## Coding guidelines
- Write production-quality code with error handling and logging
- Use type hints in Python
- All ingestion scripts should support `--dry-run` mode (output to JSON, no DB required)
- Upsert pattern for all ingestion (safe to re-run)
- Handle API pagination (TWDB caps at 1,000 records per request)
- Use environment variables for database connection strings, never hardcode
- Keep dependencies minimal — don't add libraries unless there's a clear reason
- Write docstrings for functions that interact with external APIs
- Git commit messages should be descriptive

## MVP features (Phase 1)
1. Interactive map showing data center locations (proposed, permitted, operational)
2. Aquifer level time series from nearest TWDB monitoring wells
3. Water impact calculator (MW capacity + cooling type → estimated gallons/day)
4. Public data center tracker table (who, where, capacity, water commitments)
5. Mobile-friendly — a council member should be able to pull it up on their phone

## Important context
- The Ogallala Aquifer is being drained faster than it recharges across West Texas
- Texas water planning updates every 5 years — too slow for data center growth
- Data centers are not consistently required to disclose water consumption
- "Closed-loop cooling" claims need scrutiny — Fermi's MOU is non-binding and the full system won't be ready until 2034
- Agricultural users and municipalities are the primary competing water users
- The product serves municipal leaders, water districts, economic development offices, data center developers doing community engagement, and informed residents
