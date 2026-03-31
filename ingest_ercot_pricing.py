"""
ERCOT Energy Market Data Ingestion
===================================
Pulls settlement point prices (SPPs) and wind/solar generation data
from ERCOT via the gridstatus Python library.

Settlement points tracked:
  HB_WEST    — West Hub (primary price signal for West Texas data centers)
  LZ_WEST    — West Load Zone (regional demand price)
  HB_NORTH   — North Hub (comparison reference)
  HB_BUSAVG  — Bus average (system-wide reference)

Usage:
    python ingest_ercot_pricing.py                   # Latest data only
    python ingest_ercot_pricing.py --dry-run          # Preview without DB writes
    python ingest_ercot_pricing.py --days-back 7      # Backfill last 7 days
    python ingest_ercot_pricing.py --pricing-only     # Skip generation fetch
    python ingest_ercot_pricing.py --generation-only  # Skip pricing fetch

Requirements:
    pip install gridstatus psycopg2-binary pandas
"""

import argparse
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ercot_energy_ingest")

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:dev@127.0.0.1:5433/wtx_intel",
).replace("postgres://", "postgresql://", 1)

# Settlement points relevant to West Texas data center energy costs
TARGET_SETTLEMENT_POINTS = {"HB_WEST", "LZ_WEST", "HB_NORTH", "HB_BUSAVG"}


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_spp(
    start: Optional[datetime],
    end: Optional[datetime],
) -> list[dict]:
    """
    Fetch ERCOT settlement point prices for target nodes via gridstatus.

    Returns a list of dicts with keys: ts, settlement_point, price_per_mwh.
    """
    try:
        import gridstatus
        import pandas as pd
    except ImportError as e:
        log.error(f"Missing dependency: {e}. Run: pip install gridstatus pandas")
        return []

    log.info("Fetching ERCOT settlement point prices via gridstatus...")
    ercot = gridstatus.Ercot()

    try:
        if start and end:
            df = ercot.get_spp(start=start, end=end, verbose=False)
        else:
            df = ercot.get_spp(date="latest", verbose=False)
    except Exception as e:
        log.error(f"gridstatus get_spp failed: {e}")
        return []

    if df is None or len(df) == 0:
        log.warning("SPP fetch returned no rows.")
        return []

    log.info(f"Raw SPP rows: {len(df)}")

    # Normalize column names — gridstatus column names may vary by version
    df.columns = [c.strip() for c in df.columns]
    cols = {c.lower(): c for c in df.columns}

    time_col = next(
        (cols[k] for k in ("time", "interval_start", "timestamp") if k in cols),
        None,
    )
    loc_col = next(
        (cols[k] for k in ("location", "settlement point", "settlement_point") if k in cols),
        None,
    )
    price_col = next(
        (cols[k] for k in ("spp", "price", "lmp") if k in cols),
        None,
    )

    if not all([time_col, loc_col, price_col]):
        log.error(
            f"Cannot map SPP columns. Available: {list(df.columns)}. "
            f"Expected time/location/price columns."
        )
        return []

    # Filter to target settlement points
    df_filtered = df[df[loc_col].isin(TARGET_SETTLEMENT_POINTS)].copy()
    log.info(
        f"Filtered to target settlement points: {len(df_filtered)} rows "
        f"(from {df[loc_col].nunique()} unique points)"
    )

    records = []
    for _, row in df_filtered.iterrows():
        try:
            ts = row[time_col]
            if pd.isna(ts):
                continue
            # Ensure timezone-aware UTC
            if not hasattr(ts, "tzinfo") or ts.tzinfo is None:
                ts = ts.tz_localize("US/Central")
            ts_utc = ts.astimezone(timezone.utc)

            price = row[price_col]
            if pd.isna(price):
                continue

            records.append({
                "ts": ts_utc.isoformat(),
                "settlement_point": str(row[loc_col]).strip(),
                "price_per_mwh": float(price),
            })
        except Exception as e:
            log.debug(f"Skipping SPP row due to parse error: {e}")

    log.info(f"Parsed {len(records)} SPP records")
    return records


def fetch_generation(
    start: Optional[datetime],
    end: Optional[datetime],
) -> list[dict]:
    """
    Fetch ERCOT wind and solar generation from the fuel mix endpoint.

    Returns a list of dicts with keys: ts, fuel_type, output_mw, forecast_mw.
    """
    try:
        import gridstatus
        import pandas as pd
    except ImportError as e:
        log.error(f"Missing dependency: {e}. Run: pip install gridstatus pandas")
        return []

    log.info("Fetching ERCOT fuel mix (wind/solar generation) via gridstatus...")
    ercot = gridstatus.Ercot()

    try:
        if start and end:
            df = ercot.get_fuel_mix(start=start, end=end, verbose=False)
        else:
            df = ercot.get_fuel_mix(date="latest", verbose=False)
    except Exception as e:
        log.error(f"gridstatus get_fuel_mix failed: {e}")
        return []

    if df is None or len(df) == 0:
        log.warning("Fuel mix fetch returned no rows.")
        return []

    log.info(f"Raw fuel mix rows: {len(df)}")

    df.columns = [c.strip() for c in df.columns]
    cols = {c.lower(): c for c in df.columns}

    time_col = next(
        (cols[k] for k in ("time", "interval_start", "timestamp") if k in cols),
        None,
    )
    if not time_col:
        log.error(f"No time column found in fuel mix. Available: {list(df.columns)}")
        return []

    # Map fuel types to their column names (case-insensitive)
    fuel_map: dict[str, Optional[str]] = {
        "Wind": cols.get("wind"),
        "Solar": cols.get("solar"),
    }
    available_fuels = {k: v for k, v in fuel_map.items() if v is not None}
    if not available_fuels:
        log.error(f"No Wind/Solar columns found. Available: {list(df.columns)}")
        return []

    import pandas as pd
    records = []
    for _, row in df.iterrows():
        try:
            ts = row[time_col]
            if pd.isna(ts):
                continue
            if not hasattr(ts, "tzinfo") or ts.tzinfo is None:
                ts = ts.tz_localize("US/Central")
            ts_utc = ts.astimezone(timezone.utc)

            for fuel_type, col in available_fuels.items():
                val = row[col]
                output_mw = float(val) if not pd.isna(val) else None
                records.append({
                    "ts": ts_utc.isoformat(),
                    "fuel_type": fuel_type,
                    "output_mw": output_mw,
                    "forecast_mw": None,
                })
        except Exception as e:
            log.debug(f"Skipping fuel mix row due to parse error: {e}")

    log.info(f"Parsed {len(records)} generation records")
    return records


# ---------------------------------------------------------------------------
# Database writes
# ---------------------------------------------------------------------------

def upsert_pricing(records: list[dict], conn) -> tuple[int, int]:
    """Upsert ERCOT settlement point price records. Safe to re-run."""
    new_count = 0
    updated_count = 0

    with conn.cursor() as cur:
        for r in records:
            cur.execute(
                """
                INSERT INTO ercot_pricing (ts, settlement_point, price_per_mwh)
                VALUES (%(ts)s, %(settlement_point)s, %(price_per_mwh)s)
                ON CONFLICT (ts, settlement_point) DO UPDATE SET
                    price_per_mwh = EXCLUDED.price_per_mwh,
                    ingested_at   = now()
                RETURNING (xmax = 0) AS is_new
                """,
                r,
            )
            row = cur.fetchone()
            if row and row[0]:
                new_count += 1
            else:
                updated_count += 1

    conn.commit()
    return new_count, updated_count


def upsert_generation(records: list[dict], conn) -> tuple[int, int]:
    """Upsert ERCOT wind/solar generation records. Safe to re-run."""
    new_count = 0
    updated_count = 0

    with conn.cursor() as cur:
        for r in records:
            cur.execute(
                """
                INSERT INTO ercot_generation (ts, fuel_type, output_mw, forecast_mw)
                VALUES (%(ts)s, %(fuel_type)s, %(output_mw)s, %(forecast_mw)s)
                ON CONFLICT (ts, fuel_type) DO UPDATE SET
                    output_mw   = EXCLUDED.output_mw,
                    forecast_mw = EXCLUDED.forecast_mw,
                    ingested_at = now()
                RETURNING (xmax = 0) AS is_new
                """,
                r,
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
    started: datetime,
    total_fetched: int,
    total_new: int,
    total_updated: int,
    days_back: int,
) -> None:
    """Write a summary record to the ingestion_log table."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion_log
                (source, started_at, finished_at, records_fetched,
                 records_new, records_updated, status, parameters)
            VALUES ('ercot_pricing', %s, now(), %s, %s, %s, 'success', %s)
            """,
            (
                started,
                total_fetched,
                total_new,
                total_updated,
                json.dumps({"days_back": days_back, "type": "energy_market"}),
            ),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_ingest(
    dry_run: bool = False,
    days_back: int = 0,
    pricing_only: bool = False,
    generation_only: bool = False,
) -> None:
    """Run the ERCOT energy market ingestion pipeline."""
    started = datetime.now(timezone.utc)

    # Determine time range
    start: Optional[datetime] = None
    end: Optional[datetime] = None
    if days_back > 0:
        end = started
        start = started - timedelta(days=days_back)
        log.info(f"Backfilling {days_back} days: {start.date()} → {end.date()}")
    else:
        log.info("Fetching latest data only (use --days-back N for historical backfill)")

    # Fetch
    pricing_records: list[dict] = []
    generation_records: list[dict] = []

    if not generation_only:
        pricing_records = fetch_spp(start, end)

    if not pricing_only:
        generation_records = fetch_generation(start, end)

    if dry_run:
        log.info("DRY RUN — skipping database writes.")
        if pricing_records:
            log.info(f"Would upsert {len(pricing_records)} pricing records:")
            for r in pricing_records[:8]:
                log.info(
                    f"  {r['settlement_point']:>12s} @ {r['ts']} "
                    f"= ${r['price_per_mwh']:>8.2f}/MWh"
                )
        else:
            log.info("No pricing records to upsert.")

        if generation_records:
            log.info(f"Would upsert {len(generation_records)} generation records:")
            for r in generation_records[:8]:
                log.info(
                    f"  {r['fuel_type']:>6s} @ {r['ts']} "
                    f"= {(r['output_mw'] or 0):>10,.0f} MW"
                )
        else:
            log.info("No generation records to upsert.")
        return

    if not pricing_records and not generation_records:
        log.warning("No records fetched. Check gridstatus connectivity.")
        return

    log.info("Connecting to database...")
    conn = psycopg2.connect(DB_URL)

    pricing_new = pricing_updated = 0
    gen_new = gen_updated = 0

    if pricing_records:
        pricing_new, pricing_updated = upsert_pricing(pricing_records, conn)
        log.info(f"Pricing: {pricing_new} new, {pricing_updated} updated")

    if generation_records:
        gen_new, gen_updated = upsert_generation(generation_records, conn)
        log.info(f"Generation: {gen_new} new, {gen_updated} updated")

    log_ingestion(
        conn,
        started=started,
        total_fetched=len(pricing_records) + len(generation_records),
        total_new=pricing_new + gen_new,
        total_updated=pricing_updated + gen_updated,
        days_back=days_back,
    )
    conn.close()
    log.info("Ingestion complete.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest ERCOT settlement point prices and wind/solar generation"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but don't write to the database",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=0,
        metavar="N",
        help="Backfill N days of history (default: 0 = latest interval only)",
    )
    parser.add_argument(
        "--pricing-only",
        action="store_true",
        help="Only fetch settlement point prices, skip generation",
    )
    parser.add_argument(
        "--generation-only",
        action="store_true",
        help="Only fetch wind/solar generation, skip pricing",
    )
    args = parser.parse_args()
    run_ingest(
        dry_run=args.dry_run,
        days_back=args.days_back,
        pricing_only=args.pricing_only,
        generation_only=args.generation_only,
    )


if __name__ == "__main__":
    main()
