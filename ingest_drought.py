"""
US Drought Monitor County-Level Ingestion
=========================================
Pulls weekly drought status for West Texas counties from the US Drought
Monitor API (US Drought Monitor — National Drought Mitigation Center,
University of Nebraska-Lincoln, and USDA).

Data is released each Thursday covering the period through the previous Tuesday.
No authentication required.

Usage:
    python ingest_drought.py                    # Latest week for all counties
    python ingest_drought.py --weeks 12         # Last 12 weeks
    python ingest_drought.py --fips 48303       # Single county (Lubbock)
    python ingest_drought.py --dry-run          # Preview without DB writes

API endpoint:
    https://usdm.climate.columbia.edu/api/webservice/statisticsdata/percentofarea
    /{fips}/{startDate}/{endDate}/county/

Requirements:
    pip install requests psycopg2-binary
"""

import argparse
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

USDM_BASES = [
    "https://usdm.climate.columbia.edu/api/webservice/statisticsdata/percentofarea",
    "https://droughtmonitor.unl.edu/api/webservice/statisticsdata/percentofarea",
]

# Target counties: name → 5-digit FIPS code
# Covers all counties within ~50 miles of tracked data center sites, matching
# the county set used by ingest_ercot.py.
TARGET_COUNTIES: dict[str, str] = {
    # Panhandle / Fermi Matador (Carson Co.) area
    "Armstrong":  "48011",
    "Carson":     "48065",
    "Donley":     "48129",
    "Gray":       "48179",
    "Randall":    "48381",
    "Potter":     "48375",
    "Oldham":     "48359",
    "Deaf Smith": "48117",
    "Moore":      "48341",
    "Hutchinson": "48233",
    "Roberts":    "48393",
    "Hartley":    "48205",
    "Dallam":     "48111",
    "Sherman":    "48421",
    "Hansford":   "48195",
    "Ochiltree":  "48357",
    "Lipscomb":   "48295",
    # South Plains / Lubbock area
    "Lubbock":    "48303",
    "Hale":       "48189",
    "Floyd":      "48151",
    "Crosby":     "48107",
    "Dickens":    "48125",
    "Motley":     "48345",
    "Garza":      "48169",
    "Lynn":       "48305",
    "Hockley":    "48219",
    "Lamb":       "48279",
    "Terry":      "48445",
    "Swisher":    "48467",
    "Briscoe":    "48059",
    "King":       "48269",
    "Kent":       "48263",
    "Stonewall":  "48433",
}

# Drought category column names in the USDM API response
DROUGHT_CATEGORIES = ["None", "D0", "D1", "D2", "D3", "D4"]

DEFAULT_WEEKS = 4

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("drought_ingest")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class DroughtRecord:
    """Parsed county drought record from USDM API."""
    county_fips: str
    county_name: str
    state_abbr: str
    valid_date: date
    no_drought_pct: Optional[float]
    d0_pct: Optional[float]
    d1_pct: Optional[float]
    d2_pct: Optional[float]
    d3_pct: Optional[float]
    d4_pct: Optional[float]
    worst_category: str


def _worst_category(rec: "DroughtRecord") -> str:
    """Return the label for the worst drought category that has any coverage."""
    for category, pct in [
        ("D4", rec.d4_pct),
        ("D3", rec.d3_pct),
        ("D2", rec.d2_pct),
        ("D1", rec.d1_pct),
        ("D0", rec.d0_pct),
    ]:
        if pct and pct > 0:
            return category
    return "None"


# ---------------------------------------------------------------------------
# USDM API client
# ---------------------------------------------------------------------------

_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": "AquiferWatch/1.0 (West Texas Water Monitor)",
    "Accept": "application/json",
})


def fetch_drought_for_county(
    fips: str,
    county_name: str,
    start: date,
    end: date,
) -> list[DroughtRecord]:
    """
    Fetch weekly drought status for a single county between start and end dates.

    The USDM API returns one row per weekly map release (Tuesdays).
    Dates are formatted as YYYYMMDD.

    Args:
        fips: 5-digit county FIPS code (e.g. "48303")
        county_name: Human-readable county name for logging
        start: Start date (inclusive)
        end: End date (inclusive)

    Returns:
        List of DroughtRecord, one per weekly release in the range.
    """
    data = None
    for base_url in USDM_BASES:
        url = (
            f"{base_url}/{fips}"
            f"/{start.strftime('%Y%m%d')}"
            f"/{end.strftime('%Y%m%d')}"
            "/county/"
        )

        try:
            resp = _SESSION.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if data:
                break  # Success — stop trying other endpoints
        except requests.HTTPError as e:
            log.warning(f"  HTTP {e.response.status_code} for {county_name} ({fips}): {url}")
        except requests.RequestException as e:
            log.warning(f"  Request error for {county_name} ({fips}): {e}")
        except json.JSONDecodeError:
            log.warning(f"  Invalid JSON for {county_name} ({fips})")

    if not data:
        return []

    records: list[DroughtRecord] = []
    for row in data:
        # Parse the map date — format is "YYYYMMDD" or "YYYY-MM-DD" depending on API version
        raw_date = str(row.get("MapDate", "")).replace("-", "")
        if len(raw_date) != 8:
            continue
        try:
            valid_date = date(int(raw_date[:4]), int(raw_date[4:6]), int(raw_date[6:8]))
        except ValueError:
            continue

        def _pct(key: str) -> Optional[float]:
            v = row.get(key)
            return float(v) if v is not None else None

        rec = DroughtRecord(
            county_fips=fips,
            county_name=county_name,
            state_abbr=str(row.get("State", "TX")),
            valid_date=valid_date,
            no_drought_pct=_pct("None"),
            d0_pct=_pct("D0"),
            d1_pct=_pct("D1"),
            d2_pct=_pct("D2"),
            d3_pct=_pct("D3"),
            d4_pct=_pct("D4"),
            worst_category="",  # filled below
        )
        rec.worst_category = _worst_category(rec)
        records.append(rec)

    return records


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

def get_db_connection(db_url: Optional[str] = None):
    """Return a psycopg2 connection, falling back to DATABASE_URL env var."""
    import psycopg2

    url = db_url or os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:dev@127.0.0.1:5433/wtx_intel",
    )
    url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url)


def upsert_drought(records: list[DroughtRecord], conn) -> tuple[int, int]:
    """
    Insert or update drought status records.
    Returns (new_count, updated_count). Conflict key: (county_fips, valid_date).
    """
    new_count = 0
    updated_count = 0

    with conn.cursor() as cur:
        for rec in records:
            cur.execute(
                """
                INSERT INTO drought_status (
                    county_fips, county_name, state_abbr, valid_date,
                    d0_pct, d1_pct, d2_pct, d3_pct, d4_pct,
                    no_drought_pct, worst_category
                ) VALUES (
                    %(fips)s, %(name)s, %(state)s, %(valid_date)s,
                    %(d0)s, %(d1)s, %(d2)s, %(d3)s, %(d4)s,
                    %(none)s, %(worst)s
                )
                ON CONFLICT (county_fips, valid_date) DO UPDATE SET
                    county_name    = EXCLUDED.county_name,
                    d0_pct         = EXCLUDED.d0_pct,
                    d1_pct         = EXCLUDED.d1_pct,
                    d2_pct         = EXCLUDED.d2_pct,
                    d3_pct         = EXCLUDED.d3_pct,
                    d4_pct         = EXCLUDED.d4_pct,
                    no_drought_pct = EXCLUDED.no_drought_pct,
                    worst_category = EXCLUDED.worst_category
                RETURNING (xmax = 0) AS is_new
                """,
                {
                    "fips":       rec.county_fips,
                    "name":       rec.county_name,
                    "state":      rec.state_abbr,
                    "valid_date": rec.valid_date,
                    "d0":         rec.d0_pct,
                    "d1":         rec.d1_pct,
                    "d2":         rec.d2_pct,
                    "d3":         rec.d3_pct,
                    "d4":         rec.d4_pct,
                    "none":       rec.no_drought_pct,
                    "worst":      rec.worst_category,
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
    fips_filter: Optional[list[str]] = None,
    weeks: int = DEFAULT_WEEKS,
    dry_run: bool = False,
    db_url: Optional[str] = None,
) -> None:
    """Run the US Drought Monitor ingestion pipeline."""
    started = datetime.now(timezone.utc)
    counties = TARGET_COUNTIES

    if fips_filter:
        # Allow filtering by FIPS code or county name
        fips_set = set(fips_filter)
        counties = {
            name: fips for name, fips in counties.items()
            if fips in fips_set or name in fips_filter
        }

    if not counties:
        log.error("No valid counties selected.")
        return

    end_date = date.today()
    start_date = end_date - timedelta(weeks=weeks)

    log.info("=== US Drought Monitor Ingestion ===")
    log.info(f"Counties: {len(counties)}")
    log.info(f"Date range: {start_date} → {end_date} ({weeks} weeks)")
    log.info(f"Dry run: {dry_run}")
    log.info("")

    all_records: list[DroughtRecord] = []
    error_counties: list[str] = []

    for county_name, fips in counties.items():
        log.info(f"  {county_name} County ({fips})...")
        records = fetch_drought_for_county(fips, county_name, start_date, end_date)

        if not records:
            log.warning(f"    No data returned for {county_name}")
            error_counties.append(county_name)
        else:
            log.info(f"    {len(records)} weekly records")
            all_records.extend(records)

        time.sleep(0.3)  # polite rate limit

    log.info("")
    log.info(f"Total records: {len(all_records)} ({len(error_counties)} counties had no data)")

    if dry_run:
        log.info("DRY RUN — skipping database writes.")
        log.info("")

        # Show the latest status per county
        latest: dict[str, DroughtRecord] = {}
        for rec in all_records:
            if rec.county_fips not in latest or rec.valid_date > latest[rec.county_fips].valid_date:
                latest[rec.county_fips] = rec

        log.info("Latest drought status per county:")
        for fips, rec in sorted(latest.items(), key=lambda x: x[1].county_name):
            d_pct = (rec.d1_pct or 0) + (rec.d2_pct or 0) + (rec.d3_pct or 0) + (rec.d4_pct or 0)
            log.info(
                f"  {rec.county_name:15s} ({rec.valid_date})  "
                f"worst={rec.worst_category}  D1+={d_pct:.1f}%"
            )

        output = [
            {
                "county_fips": r.county_fips,
                "county_name": r.county_name,
                "valid_date": r.valid_date.isoformat(),
                "worst_category": r.worst_category,
                "d0_pct": r.d0_pct,
                "d1_pct": r.d1_pct,
                "d2_pct": r.d2_pct,
                "d3_pct": r.d3_pct,
                "d4_pct": r.d4_pct,
                "no_drought_pct": r.no_drought_pct,
            }
            for r in all_records
        ]
        outfile = "drought_preview.json"
        with open(outfile, "w") as f:
            json.dump(output, f, indent=2)
        log.info(f"\nPreview written to {outfile}")
        return

    log.info("Connecting to database...")
    try:
        conn = get_db_connection(db_url)
        total_new, total_updated = upsert_drought(all_records, conn)
        log.info(f"Database: {total_new} new, {total_updated} updated")

        log_ingestion(
            conn,
            source="drought_monitor",
            started=started,
            fetched=len(all_records),
            new=total_new,
            updated=total_updated,
            status="success",
            params={
                "counties": len(counties),
                "weeks": weeks,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
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
        description="Ingest US Drought Monitor weekly status for West Texas counties"
    )
    parser.add_argument(
        "--fips",
        nargs="+",
        metavar="FIPS",
        help="5-digit county FIPS code(s) to query (default: all tracked counties)",
    )
    parser.add_argument(
        "--weeks",
        type=int,
        default=DEFAULT_WEEKS,
        help=f"Number of weeks of history to fetch (default: {DEFAULT_WEEKS})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Query API but don't write to database (saves JSON preview)",
    )
    parser.add_argument(
        "--db-url",
        help="PostgreSQL connection string (default: $DATABASE_URL or localhost)",
    )
    args = parser.parse_args()

    run_ingest(
        fips_filter=args.fips,
        weeks=args.weeks,
        dry_run=args.dry_run,
        db_url=args.db_url,
    )


if __name__ == "__main__":
    main()
