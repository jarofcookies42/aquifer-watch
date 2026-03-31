"""
NOAA National Weather Service Observation Ingestion
====================================================
Pulls current and recent weather observations for stations near tracked
West Texas data center sites from the NWS REST API (api.weather.gov).
No API key required — only a descriptive User-Agent header.

Usage:
    python ingest_weather.py                    # Ingest all stations
    python ingest_weather.py --station KLBB     # Single station
    python ingest_weather.py --dry-run          # Preview without DB writes
    python ingest_weather.py --hours 48         # How many hours of history

Requirements:
    pip install requests psycopg2-binary
"""

import argparse
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

NWS_BASE = "https://api.weather.gov"

# Identify ourselves to the NWS API — required by their terms of service.
# https://www.weather.gov/documentation/services-web-api
NWS_USER_AGENT = "(AquiferWatch West Texas Monitor, contact@aquifer-watch.example.com)"

# ASOS/AWOS stations near tracked West Texas counties
# KLBB → Lubbock County (Galaxy Helios, Outlaw, TeraWulf nearby)
# KAMA → Potter/Carson Counties (Fermi Matador area)
# KCDS → Childress County (eastern panhandle reference)
# KBPG → Howard County (southern reference, Big Spring)
NWS_STATIONS: dict[str, dict] = {
    "KLBB": {
        "name": "Lubbock Preston Smith International Airport",
        "county": "Lubbock",
        "county_fips": "48303",
    },
    "KAMA": {
        "name": "Amarillo Rick Husband International Airport",
        "county": "Potter",
        "county_fips": "48375",
    },
    "KCDS": {
        "name": "Childress Municipal Airport",
        "county": "Childress",
        "county_fips": "48075",
    },
    "KBPG": {
        "name": "Big Spring McMahon-Wrinkle Airport",
        "county": "Howard",
        "county_fips": "48227",
    },
}

# Default history window
DEFAULT_HOURS = 72

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("weather_ingest")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class WeatherObservation:
    """Parsed observation record from NWS API."""
    station_id: str
    station_name: str
    observed_at: datetime
    temperature_f: Optional[float]
    dewpoint_f: Optional[float]
    humidity_pct: Optional[float]
    wind_speed_mph: Optional[float]
    wind_direction_deg: Optional[int]
    wind_gust_mph: Optional[float]
    precip_last_hour_in: Optional[float]
    precip_last_6hr_in: Optional[float]
    precip_last_24hr_in: Optional[float]
    visibility_miles: Optional[float]
    pressure_mb: Optional[float]
    conditions: Optional[str]
    raw: dict


# ---------------------------------------------------------------------------
# Unit conversion helpers
# ---------------------------------------------------------------------------

def _c_to_f(celsius: Optional[float]) -> Optional[float]:
    """Celsius to Fahrenheit."""
    return round((celsius * 9 / 5) + 32, 1) if celsius is not None else None


def _kmh_to_mph(kmh: Optional[float]) -> Optional[float]:
    """km/h to mph."""
    return round(kmh / 1.60934, 1) if kmh is not None else None


def _m_to_miles(meters: Optional[float]) -> Optional[float]:
    """Metres to miles."""
    return round(meters / 1609.34, 1) if meters is not None else None


def _pa_to_mb(pascals: Optional[float]) -> Optional[float]:
    """Pascals to millibars."""
    return round(pascals / 100, 1) if pascals is not None else None


def _mm_to_in(mm: Optional[float]) -> Optional[float]:
    """Millimetres to inches."""
    return round(mm / 25.4, 3) if mm is not None else None


def _nws_value(prop: dict) -> Optional[float]:
    """Extract a numeric value from an NWS quantity object {"value": ..., "unitCode": ...}."""
    v = prop.get("value") if isinstance(prop, dict) else None
    return float(v) if v is not None else None


# ---------------------------------------------------------------------------
# NWS API client
# ---------------------------------------------------------------------------

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": NWS_USER_AGENT, "Accept": "application/geo+json"})


def _get(url: str, params: Optional[dict] = None, retries: int = 3) -> dict:
    """GET with retry on transient failures."""
    for attempt in range(1, retries + 1):
        try:
            resp = _SESSION.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            log.warning(f"  HTTP {e.response.status_code} on attempt {attempt}: {url}")
            if e.response.status_code in (429, 500, 502, 503) and attempt < retries:
                time.sleep(5 * attempt)
            else:
                raise
        except requests.RequestException as e:
            log.warning(f"  Request error on attempt {attempt}: {e}")
            if attempt < retries:
                time.sleep(5 * attempt)
            else:
                raise
    return {}


def fetch_observations(
    station_id: str,
    start: datetime,
    end: Optional[datetime] = None,
) -> list[WeatherObservation]:
    """
    Fetch hourly observations for a NWS station between start and end times.

    Args:
        station_id: ICAO station identifier (e.g. "KLBB")
        start: Earliest observation time (UTC)
        end: Latest observation time (UTC, defaults to now)

    Returns:
        List of WeatherObservation records, newest-first.
    """
    url = f"{NWS_BASE}/stations/{station_id}/observations"
    params: dict = {"start": start.isoformat()}
    if end:
        params["end"] = end.isoformat()

    log.info(f"  Fetching {station_id} observations since {start.strftime('%Y-%m-%d %H:%M UTC')}...")

    try:
        data = _get(url, params=params)
    except requests.RequestException as e:
        log.error(f"  Failed to fetch {station_id}: {e}")
        return []

    features = data.get("features", [])
    if not features:
        log.info(f"  No observations returned for {station_id}")
        return []

    station_name = NWS_STATIONS.get(station_id, {}).get("name", station_id)
    records: list[WeatherObservation] = []

    for feat in features:
        props = feat.get("properties", {})

        # Timestamp
        ts_str = props.get("timestamp")
        if not ts_str:
            continue
        try:
            observed_at = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except ValueError:
            log.debug(f"  Skipping record with bad timestamp: {ts_str}")
            continue

        # The NWS API often returns the station's @id URL in rawMessage; extract name if present
        sname = props.get("station", "").split("/")[-1] or station_name

        # Build observation
        obs = WeatherObservation(
            station_id=station_id,
            station_name=sname if sname != station_id else station_name,
            observed_at=observed_at,
            temperature_f=_c_to_f(_nws_value(props.get("temperature", {}))),
            dewpoint_f=_c_to_f(_nws_value(props.get("dewpoint", {}))),
            humidity_pct=_nws_value(props.get("relativeHumidity", {})),
            wind_speed_mph=_kmh_to_mph(_nws_value(props.get("windSpeed", {}))),
            wind_direction_deg=int(d) if (d := _nws_value(props.get("windDirection", {}))) is not None else None,
            wind_gust_mph=_kmh_to_mph(_nws_value(props.get("windGust", {}))),
            precip_last_hour_in=_mm_to_in(_nws_value(props.get("precipitationLastHour", {}))),
            precip_last_6hr_in=_mm_to_in(_nws_value(props.get("precipitationLast3Hours", {}))),
            precip_last_24hr_in=_mm_to_in(_nws_value(props.get("precipitationLast6Hours", {}))),
            visibility_miles=_m_to_miles(_nws_value(props.get("visibility", {}))),
            pressure_mb=_pa_to_mb(_nws_value(props.get("barometricPressure", {}))),
            conditions=props.get("textDescription") or None,
            raw=props,
        )
        records.append(obs)

    log.info(f"  Got {len(records)} observations for {station_id}")
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


def upsert_observations(
    observations: list[WeatherObservation],
    conn,
) -> tuple[int, int]:
    """
    Insert or update weather observations. Returns (new_count, updated_count).
    Conflict key: (station_id, observed_at).
    """
    new_count = 0
    updated_count = 0

    with conn.cursor() as cur:
        for obs in observations:
            cur.execute(
                """
                INSERT INTO weather_observations (
                    station_id, station_name, observed_at,
                    temperature_f, dewpoint_f, humidity_pct,
                    wind_speed_mph, wind_direction_deg, wind_gust_mph,
                    precip_last_hour_in, precip_last_6hr_in, precip_last_24hr_in,
                    visibility_miles, pressure_mb, conditions, raw_json
                ) VALUES (
                    %(station_id)s, %(station_name)s, %(observed_at)s,
                    %(temp)s, %(dewpoint)s, %(humidity)s,
                    %(wind_spd)s, %(wind_dir)s, %(wind_gust)s,
                    %(precip_1h)s, %(precip_6h)s, %(precip_24h)s,
                    %(visibility)s, %(pressure)s, %(conditions)s, %(raw)s
                )
                ON CONFLICT (station_id, observed_at) DO UPDATE SET
                    temperature_f       = EXCLUDED.temperature_f,
                    dewpoint_f          = EXCLUDED.dewpoint_f,
                    humidity_pct        = EXCLUDED.humidity_pct,
                    wind_speed_mph      = EXCLUDED.wind_speed_mph,
                    wind_direction_deg  = EXCLUDED.wind_direction_deg,
                    wind_gust_mph       = EXCLUDED.wind_gust_mph,
                    precip_last_hour_in = EXCLUDED.precip_last_hour_in,
                    precip_last_6hr_in  = EXCLUDED.precip_last_6hr_in,
                    precip_last_24hr_in = EXCLUDED.precip_last_24hr_in,
                    visibility_miles    = EXCLUDED.visibility_miles,
                    pressure_mb         = EXCLUDED.pressure_mb,
                    conditions          = EXCLUDED.conditions,
                    raw_json            = EXCLUDED.raw_json
                RETURNING (xmax = 0) AS is_new
                """,
                {
                    "station_id":   obs.station_id,
                    "station_name": obs.station_name,
                    "observed_at":  obs.observed_at,
                    "temp":         obs.temperature_f,
                    "dewpoint":     obs.dewpoint_f,
                    "humidity":     obs.humidity_pct,
                    "wind_spd":     obs.wind_speed_mph,
                    "wind_dir":     obs.wind_direction_deg,
                    "wind_gust":    obs.wind_gust_mph,
                    "precip_1h":    obs.precip_last_hour_in,
                    "precip_6h":    obs.precip_last_6hr_in,
                    "precip_24h":   obs.precip_last_24hr_in,
                    "visibility":   obs.visibility_miles,
                    "pressure":     obs.pressure_mb,
                    "conditions":   obs.conditions,
                    "raw":          json.dumps(obs.raw),
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
    station_ids: Optional[list[str]] = None,
    hours: int = DEFAULT_HOURS,
    dry_run: bool = False,
    db_url: Optional[str] = None,
) -> None:
    """Run the weather observation ingestion pipeline."""
    started = datetime.now(timezone.utc)
    stations = NWS_STATIONS

    if station_ids:
        stations = {k: v for k, v in stations.items() if k in station_ids}

    if not stations:
        log.error("No valid stations selected.")
        return

    log.info("=== NOAA NWS Weather Observation Ingestion ===")
    log.info(f"Stations: {', '.join(stations.keys())}")
    log.info(f"History window: {hours} hours")
    log.info(f"Dry run: {dry_run}")
    log.info("")

    start_time = datetime.now(timezone.utc) - timedelta(hours=hours)
    all_obs: list[WeatherObservation] = []

    for station_id in stations:
        log.info(f"--- {station_id}: {stations[station_id]['name']} ---")
        obs = fetch_observations(station_id, start=start_time)
        all_obs.extend(obs)
        time.sleep(0.5)  # polite rate limit

    log.info("")
    log.info(f"Total observations collected: {len(all_obs)}")

    if dry_run:
        log.info("DRY RUN — skipping database writes.")
        log.info("")
        log.info("Sample records:")
        for obs in all_obs[:10]:
            log.info(
                f"  {obs.station_id}  {obs.observed_at.strftime('%Y-%m-%d %H:%M')} UTC  "
                f"T={obs.temperature_f}°F  RH={obs.humidity_pct}%  "
                f"Wind={obs.wind_speed_mph}mph  {obs.conditions or '-'}"
            )
        if len(all_obs) > 10:
            log.info(f"  ... and {len(all_obs) - 10} more")

        output = [
            {
                "station_id": obs.station_id,
                "observed_at": obs.observed_at.isoformat(),
                "temperature_f": obs.temperature_f,
                "humidity_pct": obs.humidity_pct,
                "wind_speed_mph": obs.wind_speed_mph,
                "conditions": obs.conditions,
            }
            for obs in all_obs
        ]
        outfile = "weather_preview.json"
        with open(outfile, "w") as f:
            json.dump(output, f, indent=2, default=str)
        log.info(f"\nPreview written to {outfile}")
        return

    log.info("Connecting to database...")
    try:
        conn = get_db_connection(db_url)
        total_new, total_updated = upsert_observations(all_obs, conn)
        log.info(f"Database: {total_new} new, {total_updated} updated")

        log_ingestion(
            conn,
            source="noaa_nws",
            started=started,
            fetched=len(all_obs),
            new=total_new,
            updated=total_updated,
            status="success",
            params={"stations": list(stations.keys()), "hours": hours},
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
        description="Ingest NOAA NWS weather observations for West Texas stations"
    )
    parser.add_argument(
        "--station",
        choices=list(NWS_STATIONS.keys()),
        nargs="+",
        help="Specific station(s) to query (default: all)",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=DEFAULT_HOURS,
        help=f"Hours of history to fetch (default: {DEFAULT_HOURS})",
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
        station_ids=args.station,
        hours=args.hours,
        dry_run=args.dry_run,
        db_url=args.db_url,
    )


if __name__ == "__main__":
    main()
