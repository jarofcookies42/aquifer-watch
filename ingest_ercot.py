"""
ERCOT Generation Interconnection Queue Ingestion
=================================================
Pulls the ERCOT GIS (Generation Interconnection Status) report via the
gridstatus Python library and loads projects in West Texas counties into
the database.

Usage:
    python ingest_ercot.py                # Full ingest
    python ingest_ercot.py --dry-run      # Preview without DB writes
    python ingest_ercot.py --all-counties # Include all TX counties, not just West TX

Requirements:
    pip install gridstatus psycopg2-binary
"""

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import gridstatus
import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ercot_ingest")

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:dev@127.0.0.1:5433/wtx_intel",
)

# West Texas counties we care about (within ~50mi of tracked sites)
WEST_TX_COUNTIES = {
    # Panhandle / Carson area (Fermi Matador)
    "Carson", "Potter", "Randall", "Armstrong", "Moore", "Hutchinson",
    "Roberts", "Gray", "Donley", "Deaf Smith", "Oldham", "Hartley",
    "Dallam", "Sherman", "Hansford", "Ochiltree", "Lipscomb",
    # South Plains / Lubbock area
    "Lubbock", "Hale", "Floyd", "Crosby", "Dickens", "Motley",
    "Garza", "Lynn", "Hockley", "Lamb", "Terry", "Swisher",
    "Briscoe", "King", "Kent", "Stonewall",
}


def fetch_queue() -> list[dict]:
    """
    Fetch the ERCOT interconnection queue via gridstatus and filter
    to West Texas counties.
    """
    log.info("Fetching ERCOT interconnection queue via gridstatus...")
    ercot = gridstatus.Ercot()
    df = ercot.get_interconnection_queue()
    log.info(f"Total ERCOT queue: {len(df)} projects")

    # Filter to West TX
    df["County_clean"] = df["County"].str.strip()
    wtx = df[df["County_clean"].isin(WEST_TX_COUNTIES)].copy()
    log.info(f"West Texas projects: {len(wtx)}")

    records = []
    for _, row in wtx.iterrows():
        # Map gridstatus fuel types
        fuel = str(row.get("Fuel", "")).strip()
        if fuel == "Other":
            tech = str(row.get("Technology", "")).strip()
            if "battery" in tech.lower() or "storage" in tech.lower() or "bess" in tech.lower():
                fuel = "Battery"
            elif "gas" in tech.lower():
                fuel = "Gas"

        # Parse dates safely
        def parse_date(val):
            if val is None or (hasattr(val, 'year') is False and str(val).strip() in ('', 'NaT', 'nan')):
                return None
            try:
                import pandas as pd
                if pd.isna(val):
                    return None
                if hasattr(val, 'strftime'):
                    return val.strftime("%Y-%m-%d")
                return str(val)[:10]
            except Exception:
                return None

        record = {
            "inr_number": str(row.get("Queue ID", "")).strip(),
            "project_name": str(row.get("Project Name", "")).strip() or None,
            "fuel_type": fuel,
            "capacity_mw": float(row["Capacity (MW)"]) if row.get("Capacity (MW)") else None,
            "county": str(row["County_clean"]),
            "interconnection_bus": str(row.get("Interconnection Location", "")).strip() or None,
            "status": str(row.get("Status", "")).strip() or None,
            "projected_cod": parse_date(row.get("Proposed Completion Date")),
            "tsp": str(row.get("Transmission Owner", "")).strip() or None,
            "ercot_region": str(row.get("CDR Reporting Zone", "")).strip() or None,
            "gis_report_month": datetime.now(timezone.utc).strftime("%Y-%m-01"),
            "entity": str(row.get("Interconnecting Entity", "")).strip() or None,
            "raw": {
                k: (v.isoformat() if hasattr(v, 'isoformat') else str(v))
                for k, v in row.to_dict().items()
                if str(v) not in ('nan', 'NaT', '')
            },
        }
        records.append(record)

    return records


def upsert_ercot(records: list[dict], conn) -> tuple[int, int]:
    """Insert or update ERCOT generation queue records."""
    new_count = 0
    updated_count = 0

    with conn.cursor() as cur:
        for r in records:
            cur.execute(
                """
                INSERT INTO ercot_gen_queue (
                    inr_number, project_name, fuel_type, capacity_mw,
                    county, interconnection_bus, status, projected_cod,
                    tsp, ercot_region, gis_report_month, raw_json, updated_at
                ) VALUES (
                    %(inr_number)s, %(project_name)s, %(fuel_type)s, %(capacity_mw)s,
                    %(county)s, %(interconnection_bus)s, %(status)s,
                    %(projected_cod)s, %(tsp)s, %(ercot_region)s,
                    %(gis_report_month)s, %(raw)s, now()
                )
                ON CONFLICT (inr_number) DO UPDATE SET
                    project_name = EXCLUDED.project_name,
                    fuel_type = EXCLUDED.fuel_type,
                    capacity_mw = EXCLUDED.capacity_mw,
                    status = EXCLUDED.status,
                    projected_cod = EXCLUDED.projected_cod,
                    gis_report_month = EXCLUDED.gis_report_month,
                    raw_json = EXCLUDED.raw_json,
                    updated_at = now()
                RETURNING (xmax = 0) AS is_new
                """,
                {
                    **r,
                    "raw": json.dumps(r["raw"]),
                    "gis_report_month": r["gis_report_month"],
                },
            )
            row = cur.fetchone()
            if row and row[0]:
                new_count += 1
            else:
                updated_count += 1

    conn.commit()
    return new_count, updated_count


def run_ingest(dry_run: bool = False):
    """Run the ERCOT ingestion pipeline."""
    started = datetime.now(timezone.utc)
    records = fetch_queue()

    if not records:
        log.warning("No West Texas projects found.")
        return

    # Summary by fuel type
    from collections import Counter
    fuel_counts = Counter(r["fuel_type"] for r in records)
    total_mw = sum(r["capacity_mw"] or 0 for r in records)
    log.info(f"Fuel breakdown: {dict(fuel_counts)}")
    log.info(f"Total capacity: {total_mw:,.0f} MW")

    if dry_run:
        log.info("DRY RUN — skipping database writes.")
        log.info("")
        log.info("Projects:")
        for r in sorted(records, key=lambda x: x["capacity_mw"] or 0, reverse=True):
            log.info(
                f"  {r['inr_number']:>6s} | {r['county']:>12s} | "
                f"{r['fuel_type']:>8s} | {r['capacity_mw'] or 0:>8.0f} MW | "
                f"{r['status']:>12s} | {(r['entity'] or '')[:35]}"
            )
        return

    log.info("Connecting to database...")
    conn = psycopg2.connect(DB_URL)
    new, updated = upsert_ercot(records, conn)
    log.info(f"Database: {new} new, {updated} updated")

    # Log ingestion
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion_log
                (source, started_at, finished_at, records_fetched,
                 records_new, records_updated, status)
            VALUES ('ercot_gen_gis', %s, now(), %s, %s, %s, 'success')
            """,
            (started, len(records), new, updated),
        )
    conn.commit()
    conn.close()
    log.info("Ingestion complete.")


def main():
    parser = argparse.ArgumentParser(
        description="Ingest ERCOT generation interconnection queue for West Texas"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but don't write to database",
    )
    args = parser.parse_args()
    run_ingest(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
