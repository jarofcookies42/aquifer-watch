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

_raw_url = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:dev@127.0.0.1:5433/wtx_intel",
)
# Railway/Supabase may use postgres:// which psycopg2 doesn't accept
DB_URL = _raw_url.replace("postgres://", "postgresql://", 1)

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
# View Summary Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/views/policy-summary")
def policy_summary():
    """
    Aggregated metrics for the Policy & Planning view.
    Designed for local officials and water district managers.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            # DC pipeline by status
            cur.execute("""
                SELECT status::text, COUNT(*) AS count,
                       COALESCE(SUM(capacity_mw), 0) AS total_mw,
                       COALESCE(SUM(water_demand_gpd), 0) AS total_gpd
                FROM dc_sites
                WHERE status != 'cancelled'
                GROUP BY status
                ORDER BY total_mw DESC
            """)
            pipeline = cur.fetchall()

            # Total active pipeline
            cur.execute("""
                SELECT COUNT(*) AS total_projects,
                       COALESCE(SUM(capacity_mw), 0) AS total_mw,
                       COALESCE(SUM(water_demand_gpd), 0) AS total_gpd
                FROM dc_sites
                WHERE status != 'cancelled'
            """)
            totals = cur.fetchone()

            # Region-wide aquifer trend (last 10 years)
            cur.execute("""
                SELECT date_trunc('year', measured_at) AS yr,
                       AVG(depth_to_water_ft) AS avg_depth
                FROM water_levels
                WHERE measured_at >= now() - interval '10 years'
                GROUP BY yr
                ORDER BY yr
            """)
            aq_trend = cur.fetchall()

            cur.execute("SELECT COUNT(*) AS cnt FROM wells")
            well_count = cur.fetchone()["cnt"]

    # Aquifer depletion rate (ft/yr, positive = deeper = worse)
    depletion_rate_ft_per_yr = None
    time_to_depletion_yrs = None
    if len(aq_trend) >= 2:
        first_depth = float(aq_trend[0]["avg_depth"])
        last_depth = float(aq_trend[-1]["avg_depth"])
        years_span = len(aq_trend) - 1
        depletion_rate_ft_per_yr = round((last_depth - first_depth) / years_span, 2)
        # Average Ogallala saturated thickness in West TX ~100-150 ft, declining
        # Use remaining depth from surface as rough proxy: avg depth implies water table at that depth
        # More meaningful: estimate remaining saturated thickness (~100 ft avg for region)
        remaining_thickness_ft = 100.0
        if depletion_rate_ft_per_yr and depletion_rate_ft_per_yr > 0:
            time_to_depletion_yrs = round(remaining_thickness_ft / depletion_rate_ft_per_yr, 0)

    # Regional context: Ogallala irrigated ag in TX uses ~14M acre-feet/yr (TWDB estimate)
    regional_ag_gpd = 14_000_000 * 325_851 / 365  # gallons per day
    dc_total_gpd = float(totals["total_gpd"]) if totals["total_gpd"] else 0
    dc_pct_of_regional = round(dc_total_gpd / regional_ag_gpd * 100, 3) if regional_ag_gpd > 0 else 0

    return {
        "total_projects": totals["total_projects"],
        "total_capacity_mw": float(totals["total_mw"]),
        "total_water_demand_gpd": dc_total_gpd,
        "total_water_demand_acft_yr": round(dc_total_gpd * 365 / 325_851, 1),
        "dc_pct_of_regional_ag_water": dc_pct_of_regional,
        "pipeline_by_status": [
            {
                "status": r["status"],
                "count": r["count"],
                "total_mw": float(r["total_mw"]),
                "total_gpd": float(r["total_gpd"]),
            }
            for r in pipeline
        ],
        "aquifer_depletion_rate_ft_per_yr": depletion_rate_ft_per_yr,
        "time_to_depletion_yrs": time_to_depletion_yrs,
        "aquifer_trend": [
            {"year": row["yr"].strftime("%Y"), "avg_depth_ft": round(float(row["avg_depth"]), 1)}
            for row in aq_trend
        ],
        "monitoring_wells": well_count,
        "notes": {
            "regional_ag": "Regional ag use ~14M acre-feet/yr based on TWDB regional water plan estimates.",
            "depletion": "Time-to-depletion estimate uses ~100 ft remaining saturated thickness — a rough regional average. Actual thickness varies greatly by location.",
            "reservoirs": "Reservoir status data ingestion planned (Phase 5).",
            "drought": "Drought index data ingestion planned (Phase 5).",
        },
    }


@app.get("/api/views/industry-summary")
def industry_summary():
    """
    Aggregated metrics for the Industry & Economic view.
    Designed for data center developers, site selectors, and energy companies.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            # ERCOT generation mix
            cur.execute("""
                SELECT fuel_type,
                       COUNT(*) AS project_count,
                       COALESCE(SUM(capacity_mw), 0) AS total_mw
                FROM ercot_gen_queue
                GROUP BY fuel_type
                ORDER BY total_mw DESC
            """)
            ercot_by_fuel = cur.fetchall()

            cur.execute("""
                SELECT COALESCE(SUM(capacity_mw), 0) AS total_mw,
                       COUNT(*) AS total_projects
                FROM ercot_gen_queue
            """)
            ercot_totals = cur.fetchone()

            # All active tracked DC sites
            cur.execute("""
                SELECT id, name, project_code, operator, county,
                       status::text, capacity_mw, water_demand_gpd,
                       ST_Y(location) AS lat, ST_X(location) AS lon
                FROM dc_sites
                WHERE status != 'cancelled'
                ORDER BY capacity_mw DESC NULLS LAST
            """)
            sites = cur.fetchall()

            # Aquifer depth by county (water availability proxy)
            cur.execute("""
                SELECT w.county,
                       COUNT(DISTINCT w.id) AS well_count,
                       AVG(wl.depth_to_water_ft) AS avg_depth_ft
                FROM wells w
                LEFT JOIN water_levels wl ON wl.well_id = w.id
                    AND wl.measured_at >= now() - interval '2 years'
                WHERE w.county IS NOT NULL
                GROUP BY w.county
                HAVING COUNT(DISTINCT w.id) >= 3
                ORDER BY COUNT(DISTINCT w.id) DESC
                LIMIT 15
            """)
            county_water = cur.fetchall()

    def water_avail_score(avg_depth) -> int:
        """0–100 water availability score (higher = more available)."""
        if avg_depth is None:
            return 50
        depth = float(avg_depth)
        if depth < 150:
            return 80
        if depth < 250:
            return 60
        if depth < 400:
            return 40
        return 20

    renewable_fuels = {"Solar", "Wind", "Battery"}
    renewable_mw = sum(
        float(r["total_mw"]) for r in ercot_by_fuel if r["fuel_type"] in renewable_fuels
    )
    total_mw = float(ercot_totals["total_mw"]) or 1
    renewable_pct = round(renewable_mw / total_mw * 100, 1)

    return {
        "ercot_total_mw": total_mw,
        "ercot_total_projects": ercot_totals["total_projects"],
        "renewable_mw": renewable_mw,
        "renewable_pct": renewable_pct,
        "ercot_by_fuel": [
            {
                "fuel": r["fuel_type"],
                "mw": float(r["total_mw"]),
                "projects": r["project_count"],
                "pct": round(float(r["total_mw"]) / total_mw * 100, 1),
            }
            for r in ercot_by_fuel
        ],
        "tracked_sites": [
            {
                "id": s["id"],
                "name": s["name"],
                "operator": s["operator"],
                "county": s["county"],
                "status": s["status"],
                "capacity_mw": float(s["capacity_mw"]) if s["capacity_mw"] else None,
                "water_demand_gpd": float(s["water_demand_gpd"]) if s["water_demand_gpd"] else None,
                "lat": float(s["lat"]) if s["lat"] else None,
                "lon": float(s["lon"]) if s["lon"] else None,
            }
            for s in sites
        ],
        "county_water_availability": [
            {
                "county": r["county"],
                "well_count": r["well_count"],
                "avg_depth_ft": round(float(r["avg_depth_ft"]), 1) if r["avg_depth_ft"] else None,
                "avail_score": water_avail_score(r["avg_depth_ft"]),
            }
            for r in county_water
        ],
        "notes": {
            "energy_price": "Live ERCOT settlement point prices not yet ingested. Check ERCOT SCED/DAM data directly for real-time pricing.",
            "water_score": "Water availability score (0–100) is derived from aquifer depth measurements. 80+ = favorable, 40–60 = monitor, <40 = constrained.",
            "negative_prices": "West Texas (LCRA_LCRA hub) frequently sees negative prices during high wind generation — typically overnight. Verify with ERCOT historical data.",
        },
    }


@app.get("/api/views/public-summary")
def public_summary():
    """
    Simplified metrics for the Public / Learn view.
    Plain-language framing for general audiences.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS cnt,
                       COALESCE(SUM(capacity_mw), 0) AS total_mw,
                       COALESCE(SUM(water_demand_gpd), 0) AS total_gpd
                FROM dc_sites
                WHERE status != 'cancelled'
            """)
            totals = cur.fetchone()

            cur.execute("""
                SELECT county, COUNT(*) AS cnt,
                       COALESCE(SUM(capacity_mw), 0) AS mw,
                       COALESCE(SUM(water_demand_gpd), 0) AS gpd
                FROM dc_sites
                WHERE status != 'cancelled' AND county IS NOT NULL
                GROUP BY county
                ORDER BY mw DESC
            """)
            by_county = cur.fetchall()

            cur.execute("""
                SELECT AVG(depth_to_water_ft) AS avg_depth
                FROM water_levels
                WHERE measured_at >= now() - interval '1 year'
            """)
            latest_depth = cur.fetchone()["avg_depth"]

    total_gpd = float(totals["total_gpd"])
    total_mw = float(totals["total_mw"])

    # Household comparison: avg US household uses ~80 gal/person/day, 2.53 people
    household_gpd = 80 * 2.53
    households_equivalent = int(total_gpd / household_gpd) if total_gpd else 0

    # Farm comparison: irrigating wheat in West TX ~1.5 acre-feet/acre/year
    acre_feet_per_yr = total_gpd * 365 / 325_851
    farm_acres_equivalent = int(acre_feet_per_yr / 1.5)

    # Olympic swimming pool comparison: ~660,000 gallons each
    pools_per_day = round(total_gpd / 660_000, 1) if total_gpd else 0

    return {
        "tracked_sites": totals["cnt"],
        "total_capacity_mw": float(total_mw),
        "total_water_demand_gpd": total_gpd,
        "total_water_demand_acft_yr": round(acre_feet_per_yr, 0),
        "by_county": [
            {
                "county": r["county"],
                "sites": r["cnt"],
                "mw": float(r["mw"]),
                "gpd": float(r["gpd"]),
            }
            for r in by_county
        ],
        "comparisons": {
            "households_equivalent": households_equivalent,
            "farm_acres_equivalent": farm_acres_equivalent,
            "olympic_pools_per_day": pools_per_day,
        },
        "avg_aquifer_depth_ft": round(float(latest_depth), 1) if latest_depth else None,
        "context": {
            "ogallala_recharge": "The Ogallala Aquifer recharges less than 1 inch per year in most of West Texas, but is being pumped many feet per year.",
            "planning_gap": "Texas updates its regional water plans every 5 years — but a data center can be proposed and built in 2–3 years.",
            "disclosure_gap": "Texas does not currently require data centers to publicly disclose their actual water consumption.",
            "cooling_impact": "A 1,000 MW data center using evaporative (wet) cooling can consume 7.5 million gallons of water per day.",
        },
    }


@app.get("/api/compare/sites")
def compare_sites():
    """Side-by-side comparison data for all tracked DC sites."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.id, s.name, s.project_code, s.operator, s.tenant,
                       s.county, s.capacity_mw, s.water_demand_gpd,
                       s.status::text, s.notes, s.first_detected,
                       ST_Y(s.location) AS lat, ST_X(s.location) AS lon,
                       (SELECT COUNT(*)
                        FROM wells_near_sites wns
                        WHERE wns.site_id = s.id) AS nearby_wells,
                       (SELECT AVG(wl.depth_to_water_ft)
                        FROM water_levels wl
                        JOIN wells_near_sites wns2 ON wns2.well_id = wl.well_id
                        WHERE wns2.site_id = s.id
                          AND wl.measured_at >= now() - interval '2 years'
                       ) AS nearby_avg_depth_ft
                FROM dc_sites s
                ORDER BY s.capacity_mw DESC NULLS LAST
            """)
            sites = cur.fetchall()

    result = []
    for s in sites:
        cap = float(s["capacity_mw"]) if s["capacity_mw"] else None
        gpd = float(s["water_demand_gpd"]) if s["water_demand_gpd"] else None
        gpd_per_mw = round(gpd / cap, 0) if (gpd and cap) else None
        result.append({
            "id": s["id"],
            "name": s["name"],
            "project_code": s["project_code"],
            "operator": s["operator"],
            "county": s["county"],
            "status": s["status"],
            "capacity_mw": cap,
            "water_demand_gpd": gpd,
            "water_demand_acft_yr": round(gpd * 365 / 325_851, 0) if gpd else None,
            "water_intensity_gpd_per_mw": gpd_per_mw,
            "nearby_monitoring_wells": s["nearby_wells"],
            "avg_aquifer_depth_nearby_ft": round(float(s["nearby_avg_depth_ft"]), 1) if s["nearby_avg_depth_ft"] else None,
            "lat": float(s["lat"]) if s["lat"] else None,
            "lon": float(s["lon"]) if s["lon"] else None,
            "notes": s["notes"],
        })

    return result


# ---------------------------------------------------------------------------
# Serve frontend
# ---------------------------------------------------------------------------

FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")

if os.path.isdir(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

    @app.get("/")
    def serve_index():
        return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))
