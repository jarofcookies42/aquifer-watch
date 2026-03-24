"""
Seed water level measurements for Ogallala wells.
=================================================
Generates historically plausible water level trends based on documented
Ogallala Aquifer decline rates from TWDB reports.

Key facts used:
- Average Ogallala decline: ~1-2 ft/year across the Texas Panhandle
- Seasonal variation: ~5-10 ft (lower in summer irrigation, recovers in winter)
- Some areas declining faster (3+ ft/year near heavy irrigation)

This is MODELED DATA for dashboard development. The frontend labels it
as "Estimated from TWDB regional trends" until real per-well measurements
are integrated from TWDB bulk downloads.

Usage:
    python seed_water_levels.py                  # Seed all wells
    python seed_water_levels.py --site-id 1      # Wells near site 1
    python seed_water_levels.py --dry-run         # Preview only
"""

import argparse
import logging
import math
import os
import random
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("seed_wl")

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:dev@127.0.0.1:5433/wtx_intel",
)

# Years of historical data to generate
YEARS_BACK = 15
# Measurements per year (roughly quarterly + some extras)
MEASUREMENTS_PER_YEAR = 4


def generate_water_levels(
    well_id: int,
    well_depth_ft: float,
    latitude: float,
    start_year: int = 2010,
    end_year: int = 2026,
) -> list[dict]:
    """
    Generate plausible water level time series for one well.

    Uses well depth as a proxy for initial depth-to-water, with
    documented regional decline rates and seasonal patterns.
    """
    # Initial depth to water: roughly 40-70% of well depth
    random.seed(well_id)  # reproducible per well
    initial_dtw = well_depth_ft * random.uniform(0.35, 0.65)

    # Annual decline rate: 0.5-3.0 ft/year (varies by location)
    # Higher latitudes (Panhandle) tend to decline faster
    base_decline = 1.0 + (latitude - 33.0) * 0.3 + random.gauss(0, 0.4)
    annual_decline = max(0.3, min(base_decline, 3.5))

    # Seasonal amplitude (summer drawdown)
    seasonal_amp = random.uniform(3.0, 8.0)

    measurements = []
    for year in range(start_year, end_year + 1):
        # 4 measurements per year: Jan, Apr, Jul, Oct
        for quarter, month in enumerate([1, 4, 7, 10]):
            years_elapsed = year - start_year + month / 12.0

            # Trend: steady decline
            trend = initial_dtw + annual_decline * years_elapsed

            # Seasonal: deeper in summer (irrigation), shallower in winter
            seasonal = seasonal_amp * math.sin(2 * math.pi * (month - 1) / 12.0)

            # Random noise
            noise = random.gauss(0, 1.5)

            depth_to_water = trend + seasonal + noise

            # Estimate water elevation (rough: surface elevation - depth to water)
            # Use a simple estimate based on typical West TX elevations
            surface_elev = 2800 + (latitude - 33.0) * 400 + random.gauss(0, 50)
            water_elev = surface_elev - depth_to_water

            dt = datetime(year, month, 15, 12, 0, 0, tzinfo=timezone.utc)

            measurements.append({
                "well_id": well_id,
                "measured_at": dt,
                "depth_to_water_ft": round(depth_to_water, 2),
                "water_elevation_ft": round(water_elev, 2),
                "measurement_method": "Modeled",
                "measuring_agency": "AquiferWatch (estimated from TWDB regional trends)",
            })

    return measurements


def seed(site_id: int | None = None, dry_run: bool = False, max_wells: int = 200):
    """Seed water levels for wells, optionally filtered by site proximity."""
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)

    with conn.cursor() as cur:
        if site_id is not None:
            cur.execute("""
                SELECT w.id, w.well_depth_ft, w.latitude
                FROM wells_near_sites wns
                JOIN wells w ON w.id = wns.well_id
                WHERE wns.site_id = %s AND w.well_depth_ft IS NOT NULL
                ORDER BY wns.distance_miles
                LIMIT %s
            """, (site_id, max_wells))
        else:
            # Sample wells across all sites
            cur.execute("""
                SELECT DISTINCT ON (w.id) w.id, w.well_depth_ft, w.latitude
                FROM wells_near_sites wns
                JOIN wells w ON w.id = wns.well_id
                WHERE w.well_depth_ft IS NOT NULL
                ORDER BY w.id
                LIMIT %s
            """, (max_wells,))

        wells = cur.fetchall()

    log.info(f"Generating water levels for {len(wells)} wells...")

    total_records = 0
    for i, well in enumerate(wells):
        measurements = generate_water_levels(
            well_id=well["id"],
            well_depth_ft=float(well["well_depth_ft"]),
            latitude=float(well["latitude"]),
        )
        total_records += len(measurements)

        if not dry_run:
            with conn.cursor() as cur:
                for m in measurements:
                    cur.execute("""
                        INSERT INTO water_levels
                            (well_id, measured_at, depth_to_water_ft,
                             water_elevation_ft, measurement_method, measuring_agency)
                        VALUES (%(well_id)s, %(measured_at)s, %(depth_to_water_ft)s,
                                %(water_elevation_ft)s, %(measurement_method)s,
                                %(measuring_agency)s)
                        ON CONFLICT DO NOTHING
                    """, m)
            conn.commit()

        if (i + 1) % 50 == 0:
            log.info(f"  Processed {i + 1}/{len(wells)} wells...")

    if dry_run:
        log.info(f"DRY RUN: Would insert {total_records} water level records for {len(wells)} wells")
        # Show a sample
        sample = generate_water_levels(wells[0]["id"], float(wells[0]["well_depth_ft"]), float(wells[0]["latitude"]))
        log.info("Sample for first well:")
        for m in sample[:6]:
            log.info(f"  {m['measured_at'].strftime('%Y-%m')} | DTW: {m['depth_to_water_ft']:.1f} ft")
    else:
        log.info(f"Inserted {total_records} water level records for {len(wells)} wells.")

    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Seed water level data for Ogallala wells")
    parser.add_argument("--site-id", type=int, help="Only seed wells near this site ID")
    parser.add_argument("--max-wells", type=int, default=200, help="Max wells to seed (default: 200)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    seed(site_id=args.site_id, dry_run=args.dry_run, max_wells=args.max_wells)


if __name__ == "__main__":
    main()
