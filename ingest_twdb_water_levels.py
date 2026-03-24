"""
TWDB Bulk Water Level Ingestion
===============================
Reads the WaterLevelsMajor.txt pipe-delimited file from the TWDB bulk
download and loads real measurements for wells already in our database.

Downloads the zip if not present, extracts water levels, filters to
wells we track, and upserts into the water_levels table.

Usage:
    python ingest_twdb_water_levels.py              # Full ingest
    python ingest_twdb_water_levels.py --dry-run     # Preview counts
    python ingest_twdb_water_levels.py --clear-seed  # Remove modeled data first

Requirements:
    pip install requests psycopg2-binary
"""

import argparse
import logging
import os
import zipfile
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("twdb_wl")

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:dev@127.0.0.1:5433/wtx_intel",
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
ZIP_PATH = os.path.join(DATA_DIR, "GWDBDownload.zip")
BULK_URL = "https://www.twdb.texas.gov/groundwater/data/GWDBDownload.zip"
WL_FILE = "GWDBDownload/WaterLevelsMajor.txt"


def ensure_download() -> str:
    """Download the TWDB bulk zip if not already present."""
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(ZIP_PATH):
        log.info(f"Using cached download: {os.path.getsize(ZIP_PATH)/1024/1024:.1f} MB")
        return ZIP_PATH

    log.info(f"Downloading TWDB bulk data (~77 MB)...")
    resp = requests.get(BULK_URL, stream=True, timeout=300)
    resp.raise_for_status()
    with open(ZIP_PATH, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
    log.info(f"Downloaded: {os.path.getsize(ZIP_PATH)/1024/1024:.1f} MB")
    return ZIP_PATH


def get_our_well_ids(conn) -> dict[str, int]:
    """Get a mapping of state_well_number -> well_id for wells in our DB."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, state_well_number FROM wells")
        return {row[1]: row[0] for row in cur.fetchall()}


def parse_water_levels(zip_path: str, well_map: dict[str, int]):
    """
    Stream-parse the bulk water level file and yield records
    only for wells in our database.
    """
    z = zipfile.ZipFile(zip_path)
    with z.open(WL_FILE) as f:
        header_line = f.readline().decode("latin-1").strip()
        headers = header_line.split("|")

        # Find column indices
        swn_idx = headers.index("StateWellNumber")
        date_idx = headers.index("MeasurementDate")
        depth_idx = headers.index("DepthFromLSD")
        elev_idx = headers.index("WaterElevation")
        method_idx = headers.index("MeasurementMethod") if "MeasurementMethod" in headers else None
        agency_idx = headers.index("MeasuringAgency") if "MeasuringAgency" in headers else None

        matched = 0
        skipped = 0

        for line_bytes in f:
            line = line_bytes.decode("latin-1").strip()
            if not line:
                continue

            fields = line.split("|")
            if len(fields) <= max(swn_idx, date_idx, depth_idx):
                continue

            swn = fields[swn_idx].strip()
            if swn not in well_map:
                skipped += 1
                continue

            # Parse date
            date_str = fields[date_idx].strip()
            if not date_str:
                continue
            try:
                measured_at = datetime.strptime(date_str, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                continue

            # Parse depth to water
            depth_raw = fields[depth_idx].strip()
            depth_ft = float(depth_raw) if depth_raw else None

            # Parse water elevation
            elev_raw = fields[elev_idx].strip()
            water_elev = float(elev_raw) if elev_raw else None

            if depth_ft is None and water_elev is None:
                continue

            method = fields[method_idx].strip() if method_idx and len(fields) > method_idx else None
            agency = fields[agency_idx].strip() if agency_idx and len(fields) > agency_idx else None

            matched += 1
            yield {
                "well_id": well_map[swn],
                "measured_at": measured_at,
                "depth_to_water_ft": depth_ft,
                "water_elevation_ft": water_elev,
                "measurement_method": method,
                "measuring_agency": agency or "TWDB",
            }

    log.info(f"Parsed: {matched} matched records, {skipped} skipped (not in our wells)")


def run_ingest(dry_run: bool = False, clear_seed: bool = False):
    """Run the water level ingestion from TWDB bulk data."""
    started = datetime.now(timezone.utc)

    zip_path = ensure_download()

    conn = psycopg2.connect(DB_URL)
    well_map = get_our_well_ids(conn)
    log.info(f"Wells in database: {len(well_map)}")

    if clear_seed and not dry_run:
        log.info("Clearing modeled/seed water level data...")
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM water_levels WHERE measurement_method = 'Modeled'"
            )
            deleted = cur.rowcount
            conn.commit()
            log.info(f"Deleted {deleted} modeled records")

    log.info("Parsing bulk water level file...")
    records = list(parse_water_levels(zip_path, well_map))
    log.info(f"Total records to load: {len(records)}")

    if dry_run:
        log.info("DRY RUN — skipping database writes.")
        # Show sample
        for r in records[:10]:
            log.info(
                f"  Well {r['well_id']:>5d} | {r['measured_at'].strftime('%Y-%m-%d')} | "
                f"DTW: {r['depth_to_water_ft'] or '?':>7} ft | "
                f"Elev: {r['water_elevation_ft'] or '?':>7} ft"
            )
        if len(records) > 10:
            log.info(f"  ... and {len(records) - 10} more")

        # Stats
        wells_with_data = len(set(r["well_id"] for r in records))
        log.info(f"Wells with measurements: {wells_with_data} / {len(well_map)}")
        return

    # Batch insert
    log.info("Inserting water levels...")
    batch_size = 1000
    inserted = 0

    with conn.cursor() as cur:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            args_str = ",".join(
                cur.mogrify(
                    "(%s,%s,%s,%s,%s,%s)",
                    (
                        r["well_id"],
                        r["measured_at"],
                        r["depth_to_water_ft"],
                        r["water_elevation_ft"],
                        r["measurement_method"],
                        r["measuring_agency"],
                    ),
                ).decode()
                for r in batch
            )
            cur.execute(
                f"""
                INSERT INTO water_levels
                    (well_id, measured_at, depth_to_water_ft,
                     water_elevation_ft, measurement_method, measuring_agency)
                VALUES {args_str}
                ON CONFLICT DO NOTHING
                """
            )
            inserted += len(batch)

            if (i + batch_size) % 10000 == 0 or i + batch_size >= len(records):
                log.info(f"  Inserted {inserted}/{len(records)} records...")

        conn.commit()

    # Log ingestion
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion_log
                (source, started_at, finished_at, records_fetched,
                 records_new, status)
            VALUES ('twdb_gwdb', %s, now(), %s, %s, 'success')
            """,
            (started, len(records), inserted),
        )
        conn.commit()

    conn.close()

    wells_with_data = len(set(r["well_id"] for r in records))
    log.info(f"Done. {inserted} records for {wells_with_data} wells.")


def main():
    parser = argparse.ArgumentParser(
        description="Ingest real TWDB water level measurements from bulk download"
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument(
        "--clear-seed",
        action="store_true",
        help="Remove modeled/seed data before loading real data",
    )
    args = parser.parse_args()
    run_ingest(dry_run=args.dry_run, clear_seed=args.clear_seed)


if __name__ == "__main__":
    main()
