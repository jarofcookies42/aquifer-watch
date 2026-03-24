"""
TWDB Groundwater Database Ingestion Pipeline
=============================================
Pulls Ogallala Aquifer monitoring well data from the TWDB ArcGIS FeatureServer
for wells near tracked data center sites in West Texas.

Usage:
    python ingest_twdb.py                    # Full ingest for all sites
    python ingest_twdb.py --site HELIOS      # Single site
    python ingest_twdb.py --dry-run          # Preview without DB writes

Requirements:
    pip install requests psycopg2-binary python-dateutil
"""

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TWDB_FEATURE_SERVER = (
    "https://services.twdb.texas.gov/arcgis/rest/services"
    "/Public/TWDB_Groundwater_database/FeatureServer/0/query"
)

# Search radius around each data center site (miles -> meters)
SEARCH_RADIUS_MILES = 30
SEARCH_RADIUS_METERS = SEARCH_RADIUS_MILES * 1609.34

# ArcGIS paginates at 1000 records; we'll page through
PAGE_SIZE = 1000

# Tracked sites: (project_code, lat, lon)
TRACKED_SITES = {
    "HELIOS": {
        "name": "Galaxy Helios",
        "lat": 33.77,
        "lon": -100.78,
        "county": "Dickens",
    },
    "MATADOR": {
        "name": "Fermi Project Matador",
        "lat": 35.33,
        "lon": -101.58,
        "county": "Carson",
    },
    "LBK_NE": {
        "name": "Lubbock NE (Prospective)",
        "lat": 33.58,
        "lon": -101.80,
        "county": "Lubbock",
    },
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("twdb_ingest")


# ---------------------------------------------------------------------------
# ArcGIS FeatureServer client
# ---------------------------------------------------------------------------

@dataclass
class WellRecord:
    """Parsed well record from TWDB FeatureServer."""
    state_well_number: str
    latitude: float
    longitude: float
    county: str
    aquifer_code: str
    aquifer_name: str
    well_depth_ft: Optional[float]
    well_type: Optional[str]
    owner: Optional[str]
    raw: dict


def _radius_to_degree_delta(radius_miles: float, lat: float) -> tuple[float, float]:
    """Convert a radius in miles to approximate lat/lon deltas for a bounding box."""
    # 1 degree latitude ~ 69 miles everywhere
    lat_delta = radius_miles / 69.0
    # 1 degree longitude shrinks with cos(lat)
    import math
    lon_delta = radius_miles / (69.0 * math.cos(math.radians(lat)))
    return lat_delta, lon_delta


def query_wells_near_point(
    lat: float,
    lon: float,
    radius_m: float = SEARCH_RADIUS_METERS,
    aquifer_code: str = "OGL",
) -> list[WellRecord]:
    """
    Query the TWDB ArcGIS FeatureServer for wells within a bounding box
    around a point, filtered to Ogallala aquifer codes.

    Uses an envelope geometry since the TWDB server doesn't support
    point+distance spatial queries.
    """
    all_records = []
    offset = 0
    radius_miles = radius_m / 1609.34
    lat_delta, lon_delta = _radius_to_degree_delta(radius_miles, lat)

    # TWDB uses AquiferCodeName (not AquiferCode); Ogallala codes start with "121OG"
    where_clause = "AquiferCodeName LIKE '121OG%'"

    while True:
        params = {
            "where": where_clause,
            "geometry": json.dumps({
                "xmin": lon - lon_delta,
                "ymin": lat - lat_delta,
                "xmax": lon + lon_delta,
                "ymax": lat + lat_delta,
                "spatialReference": {"wkid": 4326},
            }),
            "geometryType": "esriGeometryEnvelope",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "*",
            "returnGeometry": "true",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
            "f": "json",
        }

        log.info(
            f"  Querying FeatureServer (offset={offset}, "
            f"radius={SEARCH_RADIUS_MILES}mi, aquifer={aquifer_code})..."
        )

        try:
            resp = requests.get(TWDB_FEATURE_SERVER, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            log.error(f"  HTTP error: {e}")
            break
        except json.JSONDecodeError:
            log.error(f"  Invalid JSON response (status {resp.status_code})")
            break

        if "error" in data:
            log.error(f"  ArcGIS error: {data['error']}")
            break

        features = data.get("features", [])
        if not features:
            break

        for feat in features:
            attrs = feat.get("attributes", {})
            geom = feat.get("geometry", {})

            # Parse aquifer code from AquiferCodeName (e.g. "121OGLL - Ogallala Formation")
            aq_code_name = str(attrs.get("AquiferCodeName", ""))
            aq_code = aq_code_name.split(" - ")[0].strip() if aq_code_name else ""
            aq_name = aq_code_name.split(" - ")[1].strip() if " - " in aq_code_name else aq_code_name

            record = WellRecord(
                state_well_number=str(attrs.get("StateWellNumber", "")).strip(),
                latitude=geom.get("y", 0),
                longitude=geom.get("x", 0),
                county=str(attrs.get("CountyName", "")).strip(),
                aquifer_code=aq_code,
                aquifer_name=aq_name,
                well_depth_ft=attrs.get("WellDepth"),
                well_type=attrs.get("WellType"),
                owner=attrs.get("OwnerName"),
                raw=attrs,
            )

            if record.state_well_number:
                all_records.append(record)

        log.info(f"  Got {len(features)} features (total so far: {len(all_records)})")

        # Check if there are more pages
        if len(features) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(0.5)  # polite rate limit

    return all_records


# ---------------------------------------------------------------------------
# Database operations (PostgreSQL + PostGIS)
# ---------------------------------------------------------------------------

def get_db_connection(conn_string: Optional[str] = None):
    """Get a psycopg2 connection. Falls back to env or defaults."""
    import psycopg2

    if conn_string:
        return psycopg2.connect(conn_string)

    import os
    conn_string = os.environ.get(
        "DATABASE_URL",
        "postgresql://localhost:5432/wtx_intel"
    )
    return psycopg2.connect(conn_string)


def upsert_wells(wells: list[WellRecord], conn) -> tuple[int, int]:
    """
    Insert or update well records. Returns (new_count, updated_count).
    Uses state_well_number as the natural key.
    """
    new_count = 0
    updated_count = 0

    with conn.cursor() as cur:
        for w in wells:
            cur.execute(
                """
                INSERT INTO wells (
                    state_well_number, latitude, longitude, location,
                    county, aquifer_code, aquifer_name,
                    well_depth_ft, well_type, owner, raw_json, updated_at
                ) VALUES (
                    %(swn)s, %(lat)s, %(lon)s,
                    ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326),
                    %(county)s, %(aq_code)s, %(aq_name)s,
                    %(depth)s, %(wtype)s, %(owner)s,
                    %(raw)s, now()
                )
                ON CONFLICT (state_well_number) DO UPDATE SET
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    location = EXCLUDED.location,
                    well_depth_ft = EXCLUDED.well_depth_ft,
                    well_type = EXCLUDED.well_type,
                    owner = EXCLUDED.owner,
                    raw_json = EXCLUDED.raw_json,
                    updated_at = now()
                RETURNING (xmax = 0) AS is_new
                """,
                {
                    "swn": w.state_well_number,
                    "lat": w.latitude,
                    "lon": w.longitude,
                    "county": w.county,
                    "aq_code": w.aquifer_code,
                    "aq_name": w.aquifer_name,
                    "depth": w.well_depth_ft,
                    "wtype": w.well_type,
                    "owner": w.owner,
                    "raw": json.dumps(w.raw),
                },
            )
            row = cur.fetchone()
            if row and row[0]:
                new_count += 1
            else:
                updated_count += 1

    conn.commit()
    return new_count, updated_count


def log_ingestion(
    conn,
    source: str,
    started: datetime,
    fetched: int,
    new: int,
    updated: int,
    status: str,
    error: Optional[str] = None,
    params: Optional[dict] = None,
):
    """Write a row to the ingestion_log table."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion_log
                (source, started_at, finished_at, records_fetched,
                 records_new, records_updated, status, error_message, parameters)
            VALUES (%s, %s, now(), %s, %s, %s, %s, %s, %s)
            """,
            (source, started, fetched, new, updated, status, error,
             json.dumps(params) if params else None),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_ingest(
    site_codes: Optional[list[str]] = None,
    dry_run: bool = False,
    db_url: Optional[str] = None,
    radius_miles: int = SEARCH_RADIUS_MILES,
):
    """Run the TWDB ingestion pipeline."""
    radius_meters = radius_miles * 1609.34
    started = datetime.now(timezone.utc)
    sites = TRACKED_SITES

    if site_codes:
        sites = {k: v for k, v in sites.items() if k in site_codes}

    if not sites:
        log.error("No valid sites selected.")
        return

    log.info(f"=== TWDB Ogallala Well Ingestion ===")
    log.info(f"Sites: {', '.join(sites.keys())}")
    log.info(f"Search radius: {radius_miles} miles")
    log.info(f"Dry run: {dry_run}")
    log.info("")

    total_fetched = 0
    total_new = 0
    total_updated = 0
    all_wells: list[WellRecord] = []

    for code, site in sites.items():
        log.info(f"--- {site['name']} ({code}) ---")
        log.info(f"  Center: ({site['lat']}, {site['lon']})")

        wells = query_wells_near_point(
            lat=site["lat"],
            lon=site["lon"],
            radius_m=radius_meters,
            aquifer_code="OGL",
        )

        log.info(f"  Found {len(wells)} Ogallala wells within {radius_miles}mi")

        if wells:
            # Show some stats
            depths = [w.well_depth_ft for w in wells if w.well_depth_ft]
            counties = set(w.county for w in wells)
            log.info(f"  Counties represented: {', '.join(sorted(counties))}")
            if depths:
                log.info(
                    f"  Well depths: min={min(depths):.0f}ft, "
                    f"max={max(depths):.0f}ft, avg={sum(depths)/len(depths):.0f}ft"
                )

        total_fetched += len(wells)
        all_wells.extend(wells)

    # Deduplicate by state well number (sites may overlap)
    seen = set()
    unique_wells = []
    for w in all_wells:
        if w.state_well_number not in seen:
            seen.add(w.state_well_number)
            unique_wells.append(w)

    log.info("")
    log.info(f"Total unique wells: {len(unique_wells)} (from {total_fetched} queries)")

    if dry_run:
        log.info("DRY RUN — skipping database writes.")
        log.info("")
        log.info("Sample records:")
        for w in unique_wells[:10]:
            log.info(
                f"  {w.state_well_number:15s} | {w.county:12s} | "
                f"depth={w.well_depth_ft or '?':>6} ft | "
                f"({w.latitude:.4f}, {w.longitude:.4f})"
            )
        if len(unique_wells) > 10:
            log.info(f"  ... and {len(unique_wells) - 10} more")

        # Write to a local JSON file for inspection
        output = [
            {
                "state_well_number": w.state_well_number,
                "latitude": w.latitude,
                "longitude": w.longitude,
                "county": w.county,
                "aquifer": w.aquifer_name,
                "depth_ft": w.well_depth_ft,
                "type": w.well_type,
                "owner": w.owner,
            }
            for w in unique_wells
        ]
        outfile = "twdb_wells_preview.json"
        with open(outfile, "w") as f:
            json.dump(output, f, indent=2)
        log.info(f"\nPreview written to {outfile}")
        return

    # Database writes
    log.info("Connecting to database...")
    try:
        conn = get_db_connection(db_url)
        total_new, total_updated = upsert_wells(unique_wells, conn)
        log.info(f"Database: {total_new} new, {total_updated} updated")

        log_ingestion(
            conn,
            source="twdb_gwdb",
            started=started,
            fetched=len(unique_wells),
            new=total_new,
            updated=total_updated,
            status="success",
            params={
                "sites": list(sites.keys()),
                "radius_miles": radius_miles,
                "aquifer": "OGL",
            },
        )
        conn.close()
        log.info("Ingestion complete.")

    except Exception as e:
        log.error(f"Database error: {e}")
        log.info("Tip: Run with --dry-run to test the API without a database.")
        raise


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Ingest TWDB Ogallala Aquifer well data for West Texas DC sites"
    )
    parser.add_argument(
        "--site",
        choices=list(TRACKED_SITES.keys()),
        nargs="+",
        help="Specific site(s) to query (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Query API but don't write to database (saves JSON preview)",
    )
    parser.add_argument(
        "--radius",
        type=int,
        default=SEARCH_RADIUS_MILES,
        help=f"Search radius in miles (default: {SEARCH_RADIUS_MILES})",
    )
    parser.add_argument(
        "--db-url",
        help="PostgreSQL connection string (default: $DATABASE_URL or localhost)",
    )
    args = parser.parse_args()

    run_ingest(
        site_codes=args.site,
        dry_run=args.dry_run,
        db_url=args.db_url,
        radius_miles=args.radius,
    )


if __name__ == "__main__":
    main()
