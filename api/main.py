"""
AquiferWatch API
================
FastAPI backend serving data center sites, well data, and dashboard metrics.
"""

import os
from contextlib import asynccontextmanager
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:dev@127.0.0.1:5433/wtx_intel",
)

_pool = None


def get_conn():
    """Get a database connection. Simple approach — no pool for MVP."""
    return psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Verify DB connectivity on startup
    conn = get_conn()
    conn.close()
    yield


app = FastAPI(
    title="AquiferWatch",
    description="West Texas Water & Data Center Intelligence",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

@app.get("/api/sites")
def list_sites():
    """All tracked data center sites with coordinates."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, project_code, operator, tenant, county,
                       capacity_mw, water_demand_gpd, status::text,
                       ST_Y(location) AS lat, ST_X(location) AS lon,
                       notes, first_detected
                FROM dc_sites
                ORDER BY name
            """)
            return cur.fetchall()


@app.get("/api/sites/{site_id}")
def get_site(site_id: int):
    """Single site with nearby well count."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.id, s.name, s.project_code, s.operator, s.tenant,
                       s.county, s.capacity_mw, s.water_demand_gpd,
                       s.status::text, s.notes, s.first_detected,
                       ST_Y(s.location) AS lat, ST_X(s.location) AS lon,
                       (SELECT COUNT(*) FROM wells_near_sites wns
                        WHERE wns.site_id = s.id) AS nearby_wells
                FROM dc_sites s
                WHERE s.id = %s
            """, (site_id,))
            return cur.fetchone()


@app.get("/api/dashboard")
def dashboard():
    """Summary stats for the dashboard header."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM site_dashboard")
            sites = cur.fetchall()

            cur.execute("SELECT COUNT(*) AS total FROM wells")
            well_count = cur.fetchone()["total"]

            cur.execute("""
                SELECT COUNT(DISTINCT county) AS counties
                FROM wells
            """)
            county_count = cur.fetchone()["counties"]

            cur.execute("""
                SELECT COUNT(*) AS projects, COALESCE(SUM(capacity_mw), 0) AS total_mw
                FROM ercot_gen_queue
            """)
            ercot = cur.fetchone()

            return {
                "sites": sites,
                "total_wells": well_count,
                "counties_covered": county_count,
                "ercot_projects": ercot["projects"],
                "ercot_total_mw": float(ercot["total_mw"]),
            }


@app.get("/api/ercot")
def list_ercot(
    county: Optional[str] = Query(None),
    fuel: Optional[str] = Query(None),
):
    """ERCOT generation interconnection queue for West Texas."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            conditions = ["1=1"]
            params: list = []
            if county:
                conditions.append("county = %s")
                params.append(county)
            if fuel:
                conditions.append("fuel_type = %s")
                params.append(fuel)

            cur.execute(f"""
                SELECT inr_number, project_name, fuel_type, capacity_mw,
                       county, status, projected_cod,
                       interconnection_bus, tsp, ercot_region
                FROM ercot_gen_queue
                WHERE {' AND '.join(conditions)}
                ORDER BY capacity_mw DESC NULLS LAST
            """, params)
            return cur.fetchall()


@app.get("/api/ercot/summary")
def ercot_summary():
    """Aggregate ERCOT stats by fuel type for the dashboard."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT fuel_type,
                       COUNT(*) AS project_count,
                       SUM(capacity_mw) AS total_mw
                FROM ercot_gen_queue
                GROUP BY fuel_type
                ORDER BY total_mw DESC
            """)
            by_fuel = cur.fetchall()

            cur.execute("""
                SELECT COUNT(*) AS total_projects,
                       SUM(capacity_mw) AS total_mw
                FROM ercot_gen_queue
            """)
            totals = cur.fetchone()

            return {
                "by_fuel": by_fuel,
                "total_projects": totals["total_projects"],
                "total_mw": float(totals["total_mw"]) if totals["total_mw"] else 0,
            }


@app.get("/api/ercot/geojson")
def ercot_geojson():
    """ERCOT generation projects as GeoJSON, placed at county well centroids."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Use the centroid of wells in each county as a proxy location
            # (ERCOT queue data doesn't include lat/lon)
            cur.execute("""
                SELECT e.inr_number, e.project_name, e.fuel_type,
                       e.capacity_mw, e.county, e.status,
                       AVG(w.latitude) AS lat, AVG(w.longitude) AS lon
                FROM ercot_gen_queue e
                LEFT JOIN wells w ON LOWER(w.county) = LOWER(e.county)
                GROUP BY e.inr_number, e.project_name, e.fuel_type,
                         e.capacity_mw, e.county, e.status
                HAVING AVG(w.latitude) IS NOT NULL
                ORDER BY e.capacity_mw DESC NULLS LAST
            """)
            rows = cur.fetchall()

    # Jitter positions slightly so overlapping county projects don't stack
    import hashlib
    features = []
    for r in rows:
        # Deterministic jitter based on project ID
        h = int(hashlib.md5(r["inr_number"].encode()).hexdigest()[:8], 16)
        jitter_lat = ((h % 1000) - 500) / 50000.0
        jitter_lon = (((h >> 10) % 1000) - 500) / 50000.0

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [
                    float(r["lon"]) + jitter_lon,
                    float(r["lat"]) + jitter_lat,
                ],
            },
            "properties": {
                "inr": r["inr_number"],
                "name": r["project_name"],
                "fuel": r["fuel_type"],
                "mw": float(r["capacity_mw"]) if r["capacity_mw"] else 0,
                "county": r["county"],
                "status": r["status"],
            },
        })

    return {"type": "FeatureCollection", "features": features}


@app.get("/api/wells")
def list_wells(
    site_id: Optional[int] = Query(None, description="Filter wells near a specific site"),
    limit: int = Query(500, le=5000),
    offset: int = Query(0, ge=0),
):
    """Wells, optionally filtered to those near a data center site."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            if site_id is not None:
                cur.execute("""
                    SELECT w.id, w.state_well_number, w.latitude, w.longitude,
                           w.county, w.aquifer_code, w.aquifer_name,
                           w.well_depth_ft, w.well_type, w.owner,
                           wns.distance_miles
                    FROM wells_near_sites wns
                    JOIN wells w ON w.id = wns.well_id
                    WHERE wns.site_id = %s
                    ORDER BY wns.distance_miles
                    LIMIT %s OFFSET %s
                """, (site_id, limit, offset))
            else:
                cur.execute("""
                    SELECT id, state_well_number, latitude, longitude,
                           county, aquifer_code, aquifer_name,
                           well_depth_ft, well_type, owner
                    FROM wells
                    ORDER BY county, state_well_number
                    LIMIT %s OFFSET %s
                """, (limit, offset))
            return cur.fetchall()


@app.get("/api/wells/geojson")
def wells_geojson(
    site_id: Optional[int] = Query(None),
    limit: int = Query(5000, le=10000),
):
    """Wells as GeoJSON for direct map rendering."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            if site_id is not None:
                cur.execute("""
                    SELECT w.id, w.state_well_number, w.latitude, w.longitude,
                           w.county, w.aquifer_name, w.well_depth_ft,
                           w.well_type, wns.distance_miles
                    FROM wells_near_sites wns
                    JOIN wells w ON w.id = wns.well_id
                    WHERE wns.site_id = %s
                    ORDER BY wns.distance_miles
                    LIMIT %s
                """, (site_id, limit))
            else:
                cur.execute("""
                    SELECT id, state_well_number, latitude, longitude,
                           county, aquifer_name, well_depth_ft, well_type
                    FROM wells
                    LIMIT %s
                """, (limit,))

            rows = cur.fetchall()

    features = []
    for r in rows:
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(r["longitude"]), float(r["latitude"])],
            },
            "properties": {
                "id": r["id"],
                "swn": r["state_well_number"],
                "county": r["county"],
                "aquifer": r.get("aquifer_name"),
                "depth_ft": float(r["well_depth_ft"]) if r["well_depth_ft"] else None,
                "type": r.get("well_type"),
                "distance_mi": round(float(r["distance_miles"]), 1) if r.get("distance_miles") else None,
            },
        })

    return {
        "type": "FeatureCollection",
        "features": features,
    }


@app.get("/api/water-levels")
def water_levels(
    site_id: int = Query(..., description="Site ID to get aggregate water levels for"),
    years: int = Query(15, le=30),
):
    """
    Aggregate water level trend for wells near a data center site.
    Returns annual average depth-to-water for charting.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    date_trunc('year', wl.measured_at) AS year,
                    AVG(wl.depth_to_water_ft) AS avg_depth_ft,
                    MIN(wl.depth_to_water_ft) AS min_depth_ft,
                    MAX(wl.depth_to_water_ft) AS max_depth_ft,
                    COUNT(*) AS measurement_count
                FROM water_levels wl
                JOIN wells_near_sites wns ON wns.well_id = wl.well_id
                WHERE wns.site_id = %s
                  AND wl.measured_at >= now() - interval '%s years'
                GROUP BY date_trunc('year', wl.measured_at)
                ORDER BY year
            """, (site_id, years))
            rows = cur.fetchall()

    return [
        {
            "year": row["year"].strftime("%Y"),
            "avg_depth_ft": round(float(row["avg_depth_ft"]), 1),
            "min_depth_ft": round(float(row["min_depth_ft"]), 1),
            "max_depth_ft": round(float(row["max_depth_ft"]), 1),
            "measurements": row["measurement_count"],
        }
        for row in rows
    ]


@app.get("/api/water-impact")
def water_impact(
    capacity_mw: float = Query(..., description="Data center capacity in MW"),
    cooling: str = Query("evaporative", description="Cooling type: evaporative, hybrid, air"),
):
    """
    Estimate daily water usage based on capacity and cooling type.
    Based on DOE/LBNL estimates for data center cooling.
    """
    # Gallons per MWh by cooling type (DOE estimates)
    gpd_per_mw = {
        "evaporative": 7_500,   # ~7,500 gal/day per MW (wet cooling tower)
        "hybrid": 3_000,        # ~3,000 gal/day per MW (hybrid dry-wet)
        "air": 100,             # minimal — only for humidification
    }

    rate = gpd_per_mw.get(cooling, gpd_per_mw["evaporative"])
    daily_gallons = capacity_mw * rate
    annual_gallons = daily_gallons * 365
    acre_feet_year = annual_gallons / 325_851  # 1 acre-foot = 325,851 gallons

    return {
        "capacity_mw": capacity_mw,
        "cooling_type": cooling,
        "gallons_per_day": round(daily_gallons),
        "gallons_per_year": round(annual_gallons),
        "acre_feet_per_year": round(acre_feet_year, 1),
        "note": "Estimates based on DOE/LBNL data center cooling studies. "
                "Actual usage varies by climate, PUE, and cooling system design.",
    }


# ---------------------------------------------------------------------------
# Serve frontend
# ---------------------------------------------------------------------------

FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")

if os.path.isdir(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

    @app.get("/")
    def serve_index():
        return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))
