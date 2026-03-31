"""
Reservoir Level Ingestion
=========================
Fetches daily storage data for West Texas surface-water reservoirs from two
public sources:

  1. TWDB Water Data for Texas (waterdatafortexas.org) — primary source.
     Per-reservoir CSV endpoint:
       https://www.waterdatafortexas.org/reservoirs/individual/{slug}/storage.csv
     Columns expected: date, storage_acft, percent_full, elevation_ft
     (column names may vary; script tries common alternatives)

  2. USGS National Water Information System (NWIS) — used for reservoirs that
     have USGS gauges (currently Lake Meredith, site 07227500).
     Daily values endpoint:
       https://waterservices.usgs.gov/nwis/dv/
     Parameters: 72943 (reservoir storage, acre-ft), 62614 (elevation, ft)

Usage:
    python ingest_reservoir_levels.py              # Last 30 days, all reservoirs
    python ingest_reservoir_levels.py --dry-run    # Preview, no DB writes
    python ingest_reservoir_levels.py --days-back 365    # 1-year backfill
    python ingest_reservoir_levels.py --reservoir lake-alan-henry  # Single target
    python ingest_reservoir_levels.py --dry-run --days-back 7

Requirements:
    pip install requests psycopg2-binary
"""

import argparse
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from typing import Optional

import psycopg2
import psycopg2.extras
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("reservoir_ingest")

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:dev@127.0.0.1:5433/wtx_intel",
).replace("postgres://", "postgresql://", 1)

WDFT_BASE = "https://www.waterdatafortexas.org"
USGS_DV_URL = "https://waterservices.usgs.gov/nwis/dv/"

# Request timeout (seconds)
HTTP_TIMEOUT = 30

# ---------------------------------------------------------------------------
# Reservoir configuration
# Each entry matches a row in the `reservoirs` seed table.
# ---------------------------------------------------------------------------

RESERVOIR_CONFIG: list[dict] = [
    {
        "slug": "lake-meredith",
        "wdft_id": "meredith",
        "usgs_site_no": "07227500",  # LAKE MEREDITH NR SANFORD TX
        "conservation_storage_acft": 863000,
    },
    {
        "slug": "lake-alan-henry",
        "wdft_id": "alan-henry",
        "usgs_site_no": None,
        "conservation_storage_acft": 116600,
    },
    {
        "slug": "lake-jb-thomas",
        "wdft_id": "jb-thomas",
        "usgs_site_no": None,
        "conservation_storage_acft": 204000,
    },
    {
        "slug": "o-h-ivie",
        "wdft_id": "o-h-ivie",
        "usgs_site_no": None,
        "conservation_storage_acft": 554000,
    },
    {
        "slug": "white-river-lake",
        "wdft_id": "white-river",
        "usgs_site_no": None,
        "conservation_storage_acft": 13756,
    },
    {
        "slug": "mackenzie-reservoir",
        "wdft_id": "mackenzie",
        "usgs_site_no": None,
        "conservation_storage_acft": 46200,
    },
    {
        "slug": "greenbelt-lake",
        "wdft_id": "greenbelt",
        "usgs_site_no": None,
        "conservation_storage_acft": 37370,
    },
    {
        "slug": "palo-duro-reservoir",
        "wdft_id": "palo-duro",
        "usgs_site_no": None,
        "conservation_storage_acft": 47070,
    },
]


# ---------------------------------------------------------------------------
# WDFT fetcher
# ---------------------------------------------------------------------------

def fetch_wdft_levels(
    wdft_id: str,
    slug: str,
    start_date: date,
    end_date: date,
    conservation_storage_acft: Optional[float],
) -> list[dict]:
    """
    Fetch daily storage data from TWDB Water Data for Texas CSV endpoint.

    WDFT serves a CSV file for each reservoir with columns that include the
    date, storage in acre-feet, percent-of-conservation-capacity, and
    optionally a water-surface elevation.  Column headers vary slightly by
    reservoir so the parser tries several common names.

    Args:
        wdft_id: Reservoir identifier used in the WDFT URL path.
        slug: Human-readable slug (used for logging only).
        start_date: First date of the requested range.
        end_date: Last date of the requested range.
        conservation_storage_acft: Design conservation capacity; used to
            calculate percent_full when the API does not provide it directly.

    Returns:
        List of dicts with keys: measured_at, percent_full,
        conservation_storage_acft, water_elevation_ft, source.
    """
    # WDFT CSV URL — the query-string parameters filter by date range.
    url = (
        f"{WDFT_BASE}/reservoirs/individual/{wdft_id}/storage.csv"
        f"?start={start_date.isoformat()}&end={end_date.isoformat()}"
    )
    log.info(f"  WDFT fetch: {slug} → {url}")

    try:
        resp = requests.get(url, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.warning(f"  WDFT request failed for {slug}: {exc}")
        return []

    return _parse_wdft_csv(resp.text, slug, conservation_storage_acft)


def _parse_wdft_csv(
    text: str,
    slug: str,
    conservation_storage_acft: Optional[float],
) -> list[dict]:
    """
    Parse WDFT CSV response into normalized level dicts.

    The CSV has a header row followed by daily data rows.  This function
    tries several common column-name variations to handle differences across
    reservoirs.

    Returns list of level dicts (empty on parse failure).
    """
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if len(lines) < 2:
        log.warning(f"  WDFT: empty or single-line response for {slug}")
        return []

    # Parse header — lowercase and normalise whitespace
    headers = [h.strip().lower().replace(" ", "_") for h in lines[0].split(",")]

    # Column name aliases (try each in order, use first match)
    DATE_COLS = ("date", "measurement_date", "record_date")
    STORAGE_COLS = ("storage_acft", "storage", "conservation_storage", "acre_feet", "acft")
    PCT_COLS = ("percent_full", "percent_conservation", "pct_full", "pct_conservation", "%_full")
    ELEV_COLS = ("elevation_ft", "water_surface_elevation", "elevation", "elev_ft", "wse")

    def find_col(aliases):
        for a in aliases:
            if a in headers:
                return headers.index(a)
        return None

    date_idx = find_col(DATE_COLS)
    storage_idx = find_col(STORAGE_COLS)
    pct_idx = find_col(PCT_COLS)
    elev_idx = find_col(ELEV_COLS)

    if date_idx is None:
        log.warning(f"  WDFT: could not find date column in headers {headers[:6]} for {slug}")
        return []

    records = []
    for row_text in lines[1:]:
        cols = row_text.split(",")
        if len(cols) <= date_idx:
            continue

        # Parse date
        date_str = cols[date_idx].strip().strip('"')
        if not date_str:
            continue
        try:
            measured_at = datetime.strptime(date_str, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            try:
                measured_at = datetime.strptime(date_str, "%m/%d/%Y").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                continue

        # Parse storage
        storage = None
        if storage_idx is not None and len(cols) > storage_idx:
            raw = cols[storage_idx].strip().strip('"').replace(",", "")
            try:
                storage = float(raw) if raw else None
            except ValueError:
                pass

        # Parse percent full
        pct = None
        if pct_idx is not None and len(cols) > pct_idx:
            raw = cols[pct_idx].strip().strip('"').replace("%", "")
            try:
                pct = float(raw) if raw else None
            except ValueError:
                pass

        # Derive percent full from storage if not in data
        if pct is None and storage is not None and conservation_storage_acft:
            pct = round(storage / conservation_storage_acft * 100, 2)

        # Parse elevation
        elev = None
        if elev_idx is not None and len(cols) > elev_idx:
            raw = cols[elev_idx].strip().strip('"')
            try:
                elev = float(raw) if raw else None
            except ValueError:
                pass

        if storage is None and pct is None:
            continue  # skip rows with no usable data

        records.append({
            "measured_at": measured_at,
            "percent_full": pct,
            "conservation_storage_acft": storage,
            "water_elevation_ft": elev,
            "source": "twdb_wdft",
        })

    log.info(f"  WDFT: parsed {len(records)} records for {slug}")
    return records


# ---------------------------------------------------------------------------
# USGS NWIS fetcher
# ---------------------------------------------------------------------------

def fetch_usgs_levels(
    site_no: str,
    slug: str,
    start_date: date,
    end_date: date,
    conservation_storage_acft: Optional[float],
) -> list[dict]:
    """
    Fetch daily reservoir storage and elevation from USGS NWIS.

    Uses the USGS Water Services daily-values endpoint.  Requests two
    parameter codes:
      72943 — Reservoir storage, acre-feet
      62614 — Water surface elevation, feet above NGVD 1929

    Args:
        site_no: USGS NWIS site number (e.g. "07227500").
        slug: Reservoir slug used for logging.
        start_date: Start of the requested date range.
        end_date: End of the requested date range.
        conservation_storage_acft: Design capacity; used to compute
            percent_full when not directly provided by USGS.

    Returns:
        List of normalised level dicts.
    """
    params = {
        "format": "json",
        "sites": site_no,
        "parameterCd": "72943,62614",
        "startDT": start_date.isoformat(),
        "endDT": end_date.isoformat(),
        "siteStatus": "all",
    }
    log.info(f"  USGS fetch: {slug} (site {site_no})")

    try:
        resp = requests.get(USGS_DV_URL, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        log.warning(f"  USGS request failed for {slug}: {exc}")
        return []
    except ValueError as exc:
        log.warning(f"  USGS JSON parse failed for {slug}: {exc}")
        return []

    return _parse_usgs_response(data, slug, conservation_storage_acft)


def _parse_usgs_response(
    data: dict,
    slug: str,
    conservation_storage_acft: Optional[float],
) -> list[dict]:
    """
    Parse USGS NWIS WaterML2 JSON response into normalised level dicts.

    The USGS daily-values JSON structure nests time-series under
    ``value.timeSeries``.  Each time-series has a ``variable.variableCode``
    list identifying the parameter and a ``values[0].value`` list of
    date/value pairs.

    Returns list of level dicts (empty on parse failure).
    """
    try:
        time_series = data["value"]["timeSeries"]
    except (KeyError, TypeError):
        log.warning(f"  USGS: unexpected response structure for {slug}")
        return []

    # Index time-series by parameter code
    storage_by_date: dict[str, Optional[float]] = {}
    elev_by_date: dict[str, Optional[float]] = {}

    for ts in time_series:
        try:
            param_code = ts["variable"]["variableCode"][0]["value"]
            values = ts["values"][0]["value"]
        except (KeyError, IndexError):
            continue

        target_map = None
        if param_code == "72943":
            target_map = storage_by_date
        elif param_code == "62614":
            target_map = elev_by_date
        else:
            continue

        no_data_val = ts.get("variable", {}).get("noDataValue")

        for v in values:
            raw_date = v.get("dateTime", "")[:10]  # YYYY-MM-DD
            raw_val = v.get("value")
            if not raw_date or raw_val is None:
                continue
            try:
                fval = float(raw_val)
            except ValueError:
                continue
            # Exclude no-data sentinel
            if no_data_val is not None and fval == float(no_data_val):
                continue
            target_map[raw_date] = fval

    all_dates = set(storage_by_date) | set(elev_by_date)
    if not all_dates:
        log.info(f"  USGS: no data returned for {slug}")
        return []

    records = []
    for date_str in sorted(all_dates):
        storage = storage_by_date.get(date_str)
        elev = elev_by_date.get(date_str)

        pct = None
        if storage is not None and conservation_storage_acft:
            pct = round(storage / conservation_storage_acft * 100, 2)

        try:
            measured_at = datetime.strptime(date_str, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            continue

        records.append({
            "measured_at": measured_at,
            "percent_full": pct,
            "conservation_storage_acft": storage,
            "water_elevation_ft": elev,
            "source": "usgs_nwis",
        })

    log.info(f"  USGS: parsed {len(records)} records for {slug}")
    return records


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_reservoir_id_map(conn) -> dict[str, int]:
    """Return a mapping of slug → reservoir.id for all seeded reservoirs."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, slug FROM reservoirs WHERE slug IS NOT NULL")
        return {row[1]: row[0] for row in cur.fetchall()}


def upsert_levels(conn, reservoir_id: int, levels: list[dict]) -> int:
    """
    Upsert reservoir level records into the database.

    Uses ON CONFLICT (reservoir_id, measured_at, source) DO UPDATE so the
    script is safe to re-run — existing rows are overwritten with fresh data.

    Args:
        conn: Active psycopg2 connection.
        reservoir_id: Primary key of the reservoir row.
        levels: List of normalised level dicts from a fetcher.

    Returns:
        Number of rows processed.
    """
    if not levels:
        return 0

    with conn.cursor() as cur:
        args = ",".join(
            cur.mogrify(
                "(%s, %s, %s, %s, %s, %s)",
                (
                    reservoir_id,
                    r["measured_at"],
                    r["percent_full"],
                    r["conservation_storage_acft"],
                    r["water_elevation_ft"],
                    r["source"],
                ),
            ).decode()
            for r in levels
        )
        cur.execute(
            f"""
            INSERT INTO reservoir_levels
                (reservoir_id, measured_at, percent_full,
                 conservation_storage_acft, water_elevation_ft, source)
            VALUES {args}
            ON CONFLICT (reservoir_id, measured_at, source)
            DO UPDATE SET
                percent_full              = EXCLUDED.percent_full,
                conservation_storage_acft = EXCLUDED.conservation_storage_acft,
                water_elevation_ft        = EXCLUDED.water_elevation_ft,
                ingested_at               = now()
            """
        )
    return len(levels)


def log_ingestion(
    conn,
    started: datetime,
    records_fetched: int,
    records_new: int,
    status: str = "success",
    error_message: Optional[str] = None,
) -> None:
    """Write a row to ingestion_log summarising this run."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion_log
                (source, started_at, finished_at, records_fetched,
                 records_new, records_updated, status, error_message)
            VALUES ('twdb_wdft', %s, now(), %s, %s, %s, %s, %s)
            """,
            (started, records_fetched, records_new, 0, status, error_message),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Main ingest runner
# ---------------------------------------------------------------------------

def run_ingest(
    dry_run: bool = False,
    days_back: int = 30,
    target_slug: Optional[str] = None,
) -> None:
    """
    Fetch and store reservoir levels for all configured reservoirs (or one).

    For each reservoir the script attempts:
      1. WDFT CSV endpoint (primary)
      2. USGS NWIS daily-values (if the reservoir has a usgs_site_no)

    When both sources return data for the same date, USGS records take
    precedence (they are inserted with source='usgs_nwis' and have their own
    unique-index slot).

    Args:
        dry_run: If True, fetches data but does not write to the database.
        days_back: How many calendar days of history to request.
        target_slug: If set, only process the reservoir with this slug.
    """
    started = datetime.now(timezone.utc)
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)

    log.info(
        f"Reservoir level ingest | range: {start_date} → {end_date} "
        f"| dry_run={dry_run}"
    )

    targets = RESERVOIR_CONFIG
    if target_slug:
        targets = [r for r in RESERVOIR_CONFIG if r["slug"] == target_slug]
        if not targets:
            log.error(f"Unknown reservoir slug: {target_slug}")
            return

    if dry_run:
        conn = None
        reservoir_id_map: dict[str, int] = {}
    else:
        conn = psycopg2.connect(DB_URL)
        reservoir_id_map = get_reservoir_id_map(conn)
        log.info(f"Reservoirs in database: {len(reservoir_id_map)}")

    total_fetched = 0
    total_inserted = 0
    dry_run_output: list[dict] = []

    for cfg in targets:
        slug = cfg["slug"]
        wdft_id = cfg["wdft_id"]
        usgs_site_no = cfg.get("usgs_site_no")
        capacity = cfg.get("conservation_storage_acft")

        log.info(f"Processing: {slug}")

        all_levels: list[dict] = []

        # -- WDFT (primary) --
        wdft_levels = fetch_wdft_levels(wdft_id, slug, start_date, end_date, capacity)
        all_levels.extend(wdft_levels)

        # -- USGS (supplemental, if gauged) --
        if usgs_site_no:
            usgs_levels = fetch_usgs_levels(
                usgs_site_no, slug, start_date, end_date, capacity
            )
            all_levels.extend(usgs_levels)

        if not all_levels:
            log.warning(f"  No data retrieved for {slug} — skipping")
            continue

        total_fetched += len(all_levels)

        if dry_run:
            sample = all_levels[:3]
            log.info(f"  DRY RUN sample for {slug}:")
            for r in sample:
                log.info(
                    f"    {r['measured_at'].strftime('%Y-%m-%d')} | "
                    f"storage: {r['conservation_storage_acft'] or '?':>8} ac-ft | "
                    f"pct_full: {r['percent_full'] or '?':>5}% | "
                    f"source: {r['source']}"
                )
            if len(all_levels) > 3:
                log.info(f"    ... and {len(all_levels) - 3} more")
            dry_run_output.extend(all_levels)
            continue

        reservoir_id = reservoir_id_map.get(slug)
        if reservoir_id is None:
            log.warning(f"  Reservoir slug '{slug}' not found in database — run schema.sql first")
            continue

        n = upsert_levels(conn, reservoir_id, all_levels)
        conn.commit()
        total_inserted += n
        log.info(f"  Upserted {n} records for {slug}")

    if dry_run:
        log.info(
            f"DRY RUN complete — {total_fetched} records across {len(targets)} reservoirs. "
            "No database writes."
        )
        if total_fetched > 0:
            log.info("Sample JSON (first record):")
            sample = dry_run_output[0]
            sample_serialisable = {
                k: v.isoformat() if isinstance(v, datetime) else v
                for k, v in sample.items()
            }
            log.info(json.dumps(sample_serialisable, indent=2))
        return

    log_ingestion(conn, started, total_fetched, total_inserted)
    conn.close()
    log.info(
        f"Done — fetched {total_fetched} records, "
        f"upserted {total_inserted} across {len(targets)} reservoirs."
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest reservoir level data from TWDB WDFT and USGS NWIS"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data and preview results without writing to the database",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=30,
        metavar="N",
        help="Number of calendar days of history to fetch (default: 30)",
    )
    parser.add_argument(
        "--reservoir",
        metavar="SLUG",
        help="Only process one reservoir by slug (e.g. lake-alan-henry)",
    )
    args = parser.parse_args()
    run_ingest(dry_run=args.dry_run, days_back=args.days_back, target_slug=args.reservoir)


if __name__ == "__main__":
    main()
