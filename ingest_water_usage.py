"""
TWDB Water Use Survey Ingestion Pipeline
=========================================
Downloads historical water use estimates from the Texas Water Development Board
(TWDB) Water Use Database (WUD) for target West Texas counties.

TWDB publishes annual county-level water use estimates by category and source
(groundwater vs surface water) going back to 1974. Data URL:
  https://www3.twdb.texas.gov/apps/reports/WUD/

Usage:
    python ingest_water_usage.py                        # All years, all target counties
    python ingest_water_usage.py --year 2020            # Single year
    python ingest_water_usage.py --csv-file data.csv    # Use a pre-downloaded CSV
    python ingest_water_usage.py --dry-run              # Preview without DB writes

Requirements:
    pip install requests psycopg2-binary python-dateutil
"""

import argparse
import csv
import io
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# TWDB publishes historical water use estimates as downloadable CSV/Excel.
# The primary URL attempts to download their county-level estimates.
# Ref: https://www.twdb.texas.gov/waterplanning/waterusesurvey/estimates/index.asp
TWDB_WUD_BASE = "https://www3.twdb.texas.gov/apps/reports/WUD"

# The bulk historical estimates file (all years, all counties)
TWDB_WUD_HIST_URL = f"{TWDB_WUD_BASE}/WUDHistoricalCountyEstimates.csv"

# Fallback: per-year URL pattern (TWDB sometimes publishes year-specific files)
TWDB_WUD_YEAR_URL = f"{TWDB_WUD_BASE}/hist_wuest_{{year}}.csv"

# Target West Texas counties: name -> 5-digit FIPS
TARGET_COUNTIES: dict[str, str] = {
    "Lubbock":     "48303",
    "Dickens":     "48125",
    "Carson":      "48065",
    "Hockley":     "48219",
    "Terry":       "48445",
    "Yoakum":      "48501",
    "Lamb":        "48279",
    "Crosby":      "48107",
    "Floyd":       "48153",
    "Hale":        "48189",
    "Swisher":     "48437",
    "Castro":      "48069",
    "Parmer":      "48369",
    "Bailey":      "48011",
    "Deaf Smith":  "48117",
    "Randall":     "48381",
    "Potter":      "48375",
    "Lynn":        "48305",
    "Garza":       "48169",
}

# TWDB water use category codes -> normalized names
CATEGORY_MAP: dict[str, str] = {
    "M":   "municipal",
    "MUN": "municipal",
    "MUNICIPAL": "municipal",
    "I":   "irrigation",
    "IRR": "irrigation",
    "IRRIGATION": "irrigation",
    "P":   "manufacturing",
    "MFG": "manufacturing",
    "MANUFACTURING": "manufacturing",
    "MN":  "mining",
    "MIN": "mining",
    "MINING": "mining",
    "L":   "livestock",
    "LIV": "livestock",
    "LIVESTOCK": "livestock",
    "SE":  "steam_electric",
    "STE": "steam_electric",
    "STEAM": "steam_electric",
    "STEAM ELECTRIC": "steam_electric",
    "STEAM_ELECTRIC": "steam_electric",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("water_usage_ingest")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class WaterUseRecord:
    """One row of TWDB water use survey data."""
    county: str
    county_fips: str
    year: int
    category: str          # normalized: municipal, irrigation, etc.
    source_type: str       # groundwater, surface_water, total
    volume_acre_ft: Optional[float]
    aquifer_name: Optional[str]
    raw: dict


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------

def _normalize_col(name: str) -> str:
    """Normalize column header to lowercase with underscores."""
    return name.strip().lower().replace(" ", "_").replace("-", "_")


def _parse_float(val) -> Optional[float]:
    """Parse a numeric string, returning None on blank/invalid values."""
    if val is None:
        return None
    s = str(val).strip().replace(",", "")
    if not s or s in ("-", "N/A", "NA", "null", "NULL"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _normalize_category(raw_cat: str) -> Optional[str]:
    """Map TWDB category codes/names to our enum values."""
    key = raw_cat.strip().upper()
    return CATEGORY_MAP.get(key)


def parse_twdb_csv(content: str, target_fips: Optional[set[str]] = None) -> list[WaterUseRecord]:
    """
    Parse TWDB water use CSV content into WaterUseRecord objects.

    TWDB publishes two CSV formats depending on the report type:
    - Wide format: county, year, category, gw_af, sw_af, total_af
    - Long format: county, year, category, source_type, volume_af

    This function handles both by inspecting the column headers.

    Args:
        content: Raw CSV string from TWDB.
        target_fips: If provided, only include rows matching these FIPS codes.

    Returns:
        List of WaterUseRecord objects.
    """
    records: list[WaterUseRecord] = []
    reader = csv.DictReader(io.StringIO(content))

    if reader.fieldnames is None:
        log.warning("CSV has no headers — cannot parse.")
        return records

    cols = {_normalize_col(f): f for f in reader.fieldnames}
    log.debug(f"CSV columns (normalized): {list(cols.keys())}")

    # Build a FIPS lookup from county name for rows that lack FIPS column
    fips_by_name = {v.upper(): v_fips for v, v_fips in TARGET_COUNTIES.items()}

    # Detect wide vs long format
    has_gw = any(k for k in cols if "ground" in k)
    has_sw = any(k for k in cols if "surface" in k)
    has_total = any(k for k in cols if "total" in k)
    wide_format = has_gw or has_sw or has_total

    for row in reader:
        # Normalize row keys
        norm_row = {_normalize_col(k): v for k, v in row.items()}

        # County
        county_raw = (
            norm_row.get("county") or norm_row.get("countyname") or
            norm_row.get("county_name") or norm_row.get("name") or ""
        ).strip().title()

        # FIPS
        fips_raw = (
            norm_row.get("fips") or norm_row.get("county_fips") or
            norm_row.get("fips_code") or ""
        ).strip().zfill(5) if (
            norm_row.get("fips") or norm_row.get("county_fips") or norm_row.get("fips_code")
        ) else ""

        if not fips_raw:
            # Attempt to derive FIPS from county name
            fips_raw = fips_by_name.get(county_raw.upper(), "")

        if target_fips and fips_raw and fips_raw not in target_fips:
            continue

        # Year
        year_raw = norm_row.get("year") or norm_row.get("reportyear") or ""
        try:
            year = int(str(year_raw).strip())
        except (ValueError, AttributeError):
            continue

        # Category
        cat_raw = (
            norm_row.get("category") or norm_row.get("usecategory") or
            norm_row.get("use_category") or norm_row.get("categorycode") or
            norm_row.get("usetype") or ""
        ).strip()
        category = _normalize_category(cat_raw)
        if not category:
            continue

        # Aquifer (optional)
        aquifer = (
            norm_row.get("aquifer") or norm_row.get("aquifername") or
            norm_row.get("aquifer_name") or None
        )
        if aquifer:
            aquifer = str(aquifer).strip() or None

        raw_dict = dict(row)

        if wide_format:
            # Wide: separate columns for GW, SW, Total
            gw_col  = next((cols[k] for k in cols if "ground" in k), None)
            sw_col  = next((cols[k] for k in cols if "surface" in k), None)
            tot_col = next((cols[k] for k in cols if "total" in k and "acre" in k), None) or \
                      next((cols[k] for k in cols if "total" in k), None)

            sources = []
            if gw_col:
                sources.append(("groundwater", row.get(gw_col)))
            if sw_col:
                sources.append(("surface_water", row.get(sw_col)))
            if tot_col:
                sources.append(("total", row.get(tot_col)))

            for src_type, vol_raw in sources:
                vol = _parse_float(vol_raw)
                records.append(WaterUseRecord(
                    county=county_raw,
                    county_fips=fips_raw,
                    year=year,
                    category=category,
                    source_type=src_type,
                    volume_acre_ft=vol,
                    aquifer_name=aquifer,
                    raw=raw_dict,
                ))
        else:
            # Long: one row per source type
            src_raw = (
                norm_row.get("source_type") or norm_row.get("source") or
                norm_row.get("watertype") or ""
            ).strip().lower()

            if "ground" in src_raw:
                src_type = "groundwater"
            elif "surface" in src_raw:
                src_type = "surface_water"
            elif "total" in src_raw:
                src_type = "total"
            else:
                src_type = "total"

            vol_raw = (
                norm_row.get("volume_acre_ft") or norm_row.get("acre_feet") or
                norm_row.get("acrefeet") or norm_row.get("af") or
                norm_row.get("value") or ""
            )
            vol = _parse_float(vol_raw)

            records.append(WaterUseRecord(
                county=county_raw,
                county_fips=fips_raw,
                year=year,
                category=category,
                source_type=src_type,
                volume_acre_ft=vol,
                aquifer_name=aquifer,
                raw=raw_dict,
            ))

    return records


# ---------------------------------------------------------------------------
# TWDB download
# ---------------------------------------------------------------------------

def download_twdb_wud(year: Optional[int] = None) -> Optional[str]:
    """
    Attempt to download TWDB water use data.

    Tries the historical bulk file first, then year-specific URL patterns.
    Returns raw CSV string on success, None on failure.
    """
    urls_to_try = []

    if year:
        # Try year-specific URL patterns first
        urls_to_try += [
            TWDB_WUD_YEAR_URL.format(year=year),
            f"{TWDB_WUD_BASE}/hist_wuest_{year}_sde.csv",
            f"{TWDB_WUD_BASE}/WUDHistoricalEstimates_{year}.csv",
        ]

    # Always try the bulk historical file
    urls_to_try.append(TWDB_WUD_HIST_URL)

    for url in urls_to_try:
        log.info(f"  Trying: {url}")
        try:
            resp = requests.get(url, timeout=60)
            if resp.status_code == 200 and len(resp.text) > 500:
                log.info(f"  Downloaded {len(resp.text):,} bytes from {url}")
                return resp.text
            else:
                log.debug(f"  HTTP {resp.status_code} — skipping")
        except requests.RequestException as e:
            log.debug(f"  Request failed: {e}")

    log.warning(
        "Could not download TWDB water use data automatically.\n"
        "  Download manually from:\n"
        "    https://www3.twdb.texas.gov/apps/reports/WUD/\n"
        "  Then run: python ingest_water_usage.py --csv-file <path>"
    )
    return None


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


def upsert_water_usage(records: list[WaterUseRecord], conn) -> tuple[int, int]:
    """
    Insert or update water_usage rows.

    Uses (county_fips, year, category, source_type) as the natural key.
    Returns (new_count, updated_count).
    """
    new_count = 0
    updated_count = 0

    with conn.cursor() as cur:
        for r in records:
            cur.execute(
                """
                INSERT INTO water_usage (
                    county, county_fips, year, category, source_type,
                    volume_acre_ft, aquifer_name, raw_json, updated_at
                ) VALUES (
                    %(county)s, %(fips)s, %(year)s,
                    %(category)s::water_use_category,
                    %(source_type)s::water_source_type,
                    %(volume)s, %(aquifer)s, %(raw)s, now()
                )
                ON CONFLICT (county_fips, year, category, source_type)
                DO UPDATE SET
                    county         = EXCLUDED.county,
                    volume_acre_ft = EXCLUDED.volume_acre_ft,
                    aquifer_name   = EXCLUDED.aquifer_name,
                    raw_json       = EXCLUDED.raw_json,
                    updated_at     = now()
                RETURNING (xmax = 0) AS is_new
                """,
                {
                    "county":      r.county,
                    "fips":        r.county_fips or None,
                    "year":        r.year,
                    "category":    r.category,
                    "source_type": r.source_type,
                    "volume":      r.volume_acre_ft,
                    "aquifer":     r.aquifer_name,
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
    csv_file: Optional[str] = None,
    dry_run: bool = False,
    db_url: Optional[str] = None,
) -> None:
    """Run the TWDB water use ingestion pipeline."""
    started = datetime.now(timezone.utc)
    target_fips = set(TARGET_COUNTIES.values())

    log.info("=== TWDB Water Use Survey Ingestion ===")
    log.info(f"Target counties: {len(TARGET_COUNTIES)}")
    log.info(f"Year filter: {year or 'all'}")
    log.info(f"Dry run: {dry_run}")
    log.info("")

    # Get CSV content
    if csv_file:
        log.info(f"Reading from file: {csv_file}")
        with open(csv_file, encoding="utf-8-sig", errors="replace") as f:
            content = f.read()
    else:
        content = download_twdb_wud(year)
        if not content:
            log.error("No data available. Use --csv-file to provide data manually.")
            sys.exit(1)

    # Parse
    log.info("Parsing CSV...")
    records = parse_twdb_csv(content, target_fips=target_fips)

    # Filter by year if requested
    if year:
        records = [r for r in records if r.year == year]

    log.info(f"Parsed {len(records)} records for target counties")
    if not records:
        log.warning("No records found — check county names or FIPS codes in the CSV.")
        return

    # Summary
    counties_seen = sorted(set(r.county for r in records))
    years_seen = sorted(set(r.year for r in records))
    log.info(f"Counties: {', '.join(counties_seen)}")
    log.info(f"Years: {min(years_seen)}–{max(years_seen)} ({len(years_seen)} years)")

    if dry_run:
        log.info("\nDRY RUN — skipping database writes.")
        log.info("\nSample records (first 10):")
        for r in records[:10]:
            log.info(
                f"  {r.county:15s} {r.year} | {r.category:15s} | "
                f"{r.source_type:12s} | {r.volume_acre_ft:>12,.0f} af"
                if r.volume_acre_ft is not None else
                f"  {r.county:15s} {r.year} | {r.category:15s} | {r.source_type:12s} | None"
            )
        if len(records) > 10:
            log.info(f"  ... and {len(records) - 10} more")

        outfile = "water_usage_preview.json"
        output = [
            {
                "county": r.county,
                "fips": r.county_fips,
                "year": r.year,
                "category": r.category,
                "source_type": r.source_type,
                "volume_acre_ft": r.volume_acre_ft,
            }
            for r in records
        ]
        with open(outfile, "w") as f:
            json.dump(output, f, indent=2)
        log.info(f"\nPreview written to {outfile}")
        return

    # Database writes
    log.info("Connecting to database...")
    try:
        conn = get_db_connection(db_url)
        new_count, updated_count = upsert_water_usage(records, conn)
        log.info(f"Database: {new_count} new, {updated_count} updated")

        log_ingestion(
            conn,
            source="twdb_wud",
            started=started,
            fetched=len(records),
            new=new_count,
            updated=updated_count,
            status="success",
            params={
                "year": year,
                "counties": len(counties_seen),
                "years_range": f"{min(years_seen)}-{max(years_seen)}",
                "csv_file": csv_file,
            },
        )
        conn.close()
        log.info("Ingestion complete.")

    except Exception as e:
        log.error(f"Database error: {e}")
        log.info("Tip: Run with --dry-run to test parsing without a database.")
        raise


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest TWDB water use survey estimates for West Texas counties",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Data source:
  TWDB Historical Water Use Estimates
  https://www.twdb.texas.gov/waterplanning/waterusesurvey/estimates/index.asp

If automatic download fails, download the CSV manually from the TWDB WUD
web portal and pass it via --csv-file.
        """,
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Filter to a specific year (default: all available years)",
    )
    parser.add_argument(
        "--csv-file",
        metavar="PATH",
        help="Path to a pre-downloaded TWDB water use CSV file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse data but don't write to database (saves JSON preview)",
    )
    parser.add_argument(
        "--db-url",
        help="PostgreSQL connection string (default: $DATABASE_URL or localhost)",
    )
    args = parser.parse_args()

    run_ingest(
        year=args.year,
        csv_file=args.csv_file,
        dry_run=args.dry_run,
        db_url=args.db_url,
    )


if __name__ == "__main__":
    main()
