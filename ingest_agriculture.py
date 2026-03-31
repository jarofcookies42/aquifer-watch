"""
USDA NASS Agricultural Data Ingestion Pipeline
===============================================
Pulls irrigated acreage and crop production data from the USDA National
Agricultural Statistics Service (NASS) Quick Stats API for West Texas counties
in the Ogallala Aquifer region.

Data covers major irrigated crops: cotton, wheat, corn, sorghum, soybeans,
and hay — the primary drivers of groundwater demand in the region.

Usage:
    python ingest_agriculture.py                    # All crops, all target counties
    python ingest_agriculture.py --year 2022        # Census/survey year
    python ingest_agriculture.py --dry-run          # Preview without DB writes
    python ingest_agriculture.py --crop COTTON      # Single crop

Requirements:
    pip install requests psycopg2-binary
    export NASS_API_KEY=your_key_here  # Free key from https://quickstats.nass.usda.gov/api
"""

import argparse
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

NASS_API_BASE = "https://quickstats.nass.usda.gov/api/api_GET/"

# NASS returns up to 50,000 rows per call; we request in chunks by county
NASS_PAGE_LIMIT = 50000

# Target West Texas counties (county name -> ANSI 3-digit county code, state FIPS 48)
# NASS uses 3-digit county_ansi codes within the state
TARGET_COUNTIES: dict[str, str] = {
    "LUBBOCK":     "303",
    "DICKENS":     "125",
    "CARSON":      "065",
    "HOCKLEY":     "219",
    "TERRY":       "445",
    "YOAKUM":      "501",
    "LAMB":        "279",
    "CROSBY":      "107",
    "FLOYD":       "153",
    "HALE":        "189",
    "SWISHER":     "437",
    "CASTRO":      "069",
    "PARMER":      "369",
    "BAILEY":      "011",
    "DEAF SMITH":  "117",
    "RANDALL":     "381",
    "POTTER":      "375",
    "LYNN":        "305",
    "GARZA":       "169",
}

# Map county ANSI code -> 5-digit FIPS (state FIPS 48 + county ANSI)
COUNTY_FIPS: dict[str, str] = {
    ansi: f"48{ansi}" for ansi in TARGET_COUNTIES.values()
}

# Crops to track (NASS commodity_desc values)
TARGET_CROPS = [
    "COTTON",
    "WHEAT",
    "CORN",
    "SORGHUM",
    "SOYBEANS",
    "HAY",
]

# NASS params for irrigated harvested acres
NASS_BASE_PARAMS = {
    "state_fips_code":     "48",   # Texas
    "sector_desc":         "CROPS",
    "statisticcat_desc":   "AREA HARVESTED",
    "prodn_practice_desc": "IRRIGATED",
    "unit_desc":           "ACRES",
    "agg_level_desc":      "COUNTY",
    "format":              "JSON",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("agriculture_ingest")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class AgRecord:
    """One row of USDA NASS irrigated acreage data."""
    county: str
    county_fips: str
    year: int
    crop_type: str             # e.g. "COTTON", "CORN"
    acres_irrigated: Optional[float]
    acres_harvested: Optional[float]
    production_value: Optional[float]
    production_units: Optional[str]
    source: str                # "SURVEY" or "CENSUS"
    raw: dict


# ---------------------------------------------------------------------------
# NASS API client
# ---------------------------------------------------------------------------

def _get_api_key() -> str:
    """Get NASS API key from environment. Raises if not set."""
    key = os.environ.get("NASS_API_KEY", "").strip()
    if not key:
        raise EnvironmentError(
            "NASS_API_KEY environment variable is not set.\n"
            "Get a free key at: https://quickstats.nass.usda.gov/api"
        )
    return key


def _parse_value(val: str) -> Optional[float]:
    """Parse a NASS numeric value string. Returns None for suppressed/invalid data."""
    if not val:
        return None
    cleaned = str(val).strip().replace(",", "")
    # NASS uses special codes for suppressed or unavailable data
    if cleaned in ("(D)", "(Z)", "(L)", "(H)", "(NA)", ""):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def query_nass(
    api_key: str,
    county_ansi: str,
    commodity: str,
    year: Optional[int] = None,
    source_desc: str = "SURVEY",
) -> list[dict]:
    """
    Query the NASS Quick Stats API for irrigated acreage in a county.

    Args:
        api_key: NASS API key.
        county_ansi: 3-digit county ANSI code.
        commodity: Crop commodity (e.g. "COTTON").
        year: Specific year, or None for all available years.
        source_desc: "SURVEY" for annual surveys, "CENSUS" for 5-year census.

    Returns:
        List of raw NASS row dicts.
    """
    params = {
        **NASS_BASE_PARAMS,
        "source_desc":    source_desc,
        "commodity_desc": commodity,
        "county_ansi":    county_ansi,
        "key":            api_key,
    }
    if year:
        params["year"] = str(year)

    try:
        resp = requests.get(NASS_API_BASE, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        log.error(f"    HTTP error querying NASS (county={county_ansi}, crop={commodity}): {e}")
        return []
    except json.JSONDecodeError:
        log.error(f"    Invalid JSON from NASS (county={county_ansi}, crop={commodity})")
        return []

    if "error" in data:
        # NASS returns {"error": ["no data"]} for empty results — not a failure
        errors = data["error"]
        if any("no data" in str(e).lower() for e in errors):
            return []
        log.warning(f"    NASS API error: {data['error']}")
        return []

    return data.get("data", [])


def fetch_county_crop_data(
    api_key: str,
    county_name: str,
    county_ansi: str,
    commodity: str,
    year: Optional[int] = None,
) -> list[AgRecord]:
    """
    Fetch irrigated acreage for one county × crop combination from NASS.

    Queries both SURVEY and CENSUS sources to maximize coverage.
    """
    county_fips = COUNTY_FIPS.get(county_ansi, f"48{county_ansi}")
    records: list[AgRecord] = []
    seen_keys: set[tuple] = set()

    for source in ("SURVEY", "CENSUS"):
        rows = query_nass(api_key, county_ansi, commodity, year=year, source_desc=source)

        for row in rows:
            year_val = int(row.get("year", 0))
            if year_val < 1970:
                continue

            # Deduplicate across sources: prefer SURVEY over CENSUS for same year
            key = (county_fips, year_val, commodity)
            if key in seen_keys and source == "CENSUS":
                continue
            seen_keys.add(key)

            acres = _parse_value(row.get("Value", ""))

            records.append(AgRecord(
                county=county_name.title(),
                county_fips=county_fips,
                year=year_val,
                crop_type=commodity,
                acres_irrigated=acres,
                acres_harvested=acres,   # irrigated harvested = irrigated area
                production_value=None,   # fetched separately if needed
                production_units=row.get("unit_desc"),
                source=source,
                raw=dict(row),
            ))

        if rows:
            log.debug(
                f"    {source}: {len(rows)} rows for {county_name}/{commodity}"
            )
        time.sleep(0.2)  # polite rate limiting

    return records


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

def get_db_connection(conn_string: Optional[str] = None):
    """Get a psycopg2 connection from env or explicit URL."""
    import psycopg2

    if not conn_string:
        conn_string = os.environ.get(
            "DATABASE_URL",
            "postgresql://localhost:5432/wtx_intel",
        )
    conn_string = conn_string.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(conn_string)


def upsert_agricultural_data(records: list[AgRecord], conn) -> tuple[int, int]:
    """
    Insert or update agricultural_data rows.

    Uses (county_fips, year, crop_type) as the natural key.
    Returns (new_count, updated_count).
    """
    new_count = 0
    updated_count = 0

    with conn.cursor() as cur:
        for r in records:
            cur.execute(
                """
                INSERT INTO agricultural_data (
                    county, county_fips, year, crop_type,
                    acres_irrigated, acres_harvested,
                    production_value, production_units,
                    source, raw_json, updated_at
                ) VALUES (
                    %(county)s, %(fips)s, %(year)s, %(crop)s,
                    %(irr)s, %(harv)s,
                    %(prod_val)s, %(prod_units)s,
                    %(source)s, %(raw)s, now()
                )
                ON CONFLICT (county_fips, year, crop_type) DO UPDATE SET
                    county           = EXCLUDED.county,
                    acres_irrigated  = EXCLUDED.acres_irrigated,
                    acres_harvested  = EXCLUDED.acres_harvested,
                    production_value = EXCLUDED.production_value,
                    production_units = EXCLUDED.production_units,
                    source           = EXCLUDED.source,
                    raw_json         = EXCLUDED.raw_json,
                    updated_at       = now()
                RETURNING (xmax = 0) AS is_new
                """,
                {
                    "county":      r.county,
                    "fips":        r.county_fips,
                    "year":        r.year,
                    "crop":        r.crop_type,
                    "irr":         r.acres_irrigated,
                    "harv":        r.acres_harvested,
                    "prod_val":    r.production_value,
                    "prod_units":  r.production_units,
                    "source":      r.source,
                    "raw":         json.dumps(r.raw),
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
) -> None:
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
    year: Optional[int] = None,
    crops: Optional[list[str]] = None,
    dry_run: bool = False,
    db_url: Optional[str] = None,
) -> None:
    """Run the USDA NASS agriculture ingestion pipeline."""
    started = datetime.now(timezone.utc)

    try:
        api_key = _get_api_key()
    except EnvironmentError as e:
        log.error(str(e))
        raise SystemExit(1)

    target_crops = [c.upper() for c in crops] if crops else TARGET_CROPS

    log.info("=== USDA NASS Agriculture Ingestion ===")
    log.info(f"Counties: {len(TARGET_COUNTIES)}")
    log.info(f"Crops: {', '.join(target_crops)}")
    log.info(f"Year filter: {year or 'all available'}")
    log.info(f"Dry run: {dry_run}")
    log.info("")

    all_records: list[AgRecord] = []
    total_combos = len(TARGET_COUNTIES) * len(target_crops)
    combo_count = 0

    for county_name, county_ansi in TARGET_COUNTIES.items():
        for commodity in target_crops:
            combo_count += 1
            log.info(
                f"  [{combo_count}/{total_combos}] "
                f"{county_name.title()} / {commodity}"
            )

            records = fetch_county_crop_data(
                api_key=api_key,
                county_name=county_name,
                county_ansi=county_ansi,
                commodity=commodity,
                year=year,
            )

            if records:
                years_found = sorted(set(r.year for r in records))
                log.info(
                    f"    → {len(records)} rows, "
                    f"years {min(years_found)}–{max(years_found)}"
                )
            else:
                log.info("    → no data")

            all_records.extend(records)

    log.info("")
    log.info(f"Total records fetched: {len(all_records)}")

    if not all_records:
        log.warning("No records found.")
        return

    if dry_run:
        log.info("DRY RUN — skipping database writes.")
        log.info("\nSample records (first 10):")
        for r in all_records[:10]:
            log.info(
                f"  {r.county:15s} {r.year} | {r.crop_type:10s} | "
                f"{r.acres_irrigated:>10,.0f} irrigated acres ({r.source})"
                if r.acres_irrigated is not None else
                f"  {r.county:15s} {r.year} | {r.crop_type:10s} | None ({r.source})"
            )
        if len(all_records) > 10:
            log.info(f"  ... and {len(all_records) - 10} more")

        outfile = "agriculture_preview.json"
        output = [
            {
                "county": r.county,
                "fips": r.county_fips,
                "year": r.year,
                "crop": r.crop_type,
                "acres_irrigated": r.acres_irrigated,
                "source": r.source,
            }
            for r in all_records
        ]
        with open(outfile, "w") as f:
            json.dump(output, f, indent=2)
        log.info(f"\nPreview written to {outfile}")
        return

    # Database writes
    log.info("Connecting to database...")
    try:
        conn = get_db_connection(db_url)
        new_count, updated_count = upsert_agricultural_data(all_records, conn)
        log.info(f"Database: {new_count} new, {updated_count} updated")

        log_ingestion(
            conn,
            source="usda_nass",
            started=started,
            fetched=len(all_records),
            new=new_count,
            updated=updated_count,
            status="success",
            params={
                "year": year,
                "counties": len(TARGET_COUNTIES),
                "crops": target_crops,
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

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest USDA NASS irrigated acreage data for West Texas counties",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Data source:
  USDA NASS Quick Stats API
  https://quickstats.nass.usda.gov/api

Requires NASS_API_KEY environment variable (free registration).

Target crops: COTTON, WHEAT, CORN, SORGHUM, SOYBEANS, HAY
Target counties: 19 West Texas/High Plains counties in the Ogallala region
        """,
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Specific survey/census year (default: all available)",
    )
    parser.add_argument(
        "--crop",
        dest="crops",
        nargs="+",
        metavar="COMMODITY",
        help=f"Crop(s) to ingest (default: all). Choices: {', '.join(TARGET_CROPS)}",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch from API but don't write to database (saves JSON preview)",
    )
    parser.add_argument(
        "--db-url",
        help="PostgreSQL connection string (default: $DATABASE_URL or localhost)",
    )
    args = parser.parse_args()

    run_ingest(
        year=args.year,
        crops=args.crops,
        dry_run=args.dry_run,
        db_url=args.db_url,
    )


if __name__ == "__main__":
    main()
