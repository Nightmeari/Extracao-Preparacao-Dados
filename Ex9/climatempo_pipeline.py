"""
climatempo_pipeline.py
======================
Concurrent weather data extraction pipeline for climatempo.com.br
Fetches weather from internal JSON endpoints by brute-forcing locale IDs.

Author : Production-ready example
Python : 3.9+
Deps   : requests, pandas, tqdm  (pip install requests pandas tqdm)
"""

from __future__ import annotations

import csv
import logging
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from typing import Optional

import pandas as pd
import requests
from tqdm import tqdm

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────

BASE_URL = "https://www.climatempo.com.br/json/myclimatempo/user/weatherNow"

DEFAULT_CONFIG = {
    "id_start": 1,
    "id_end": 5000,          # upper bound of locale IDs to probe
    "max_workers": 40,       # concurrent threads
    "timeout": 8,            # HTTP request timeout (seconds)
    "retry_attempts": 2,     # retries on transient errors
    "retry_backoff": 0.3,    # base backoff (seconds) between retries
    "delay_between_requests": 0.05,  # polite per-thread delay (seconds)
    "state_filter": None,    # e.g. ["SP", "RJ", "MG", "ES"] or None for all
    "output_csv": "climatempo_weatherlatlong.csv",
    "log_level": logging.INFO,
}

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    level=DEFAULT_CONFIG["log_level"],
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# DATA MODEL
# ──────────────────────────────────────────────

@dataclass
class WeatherRecord:
    locale_id: int
    city: str
    state: str
    temperature: Optional[float]
    feels_like: Optional[float]
    latitude: Optional[float]
    longitude: Optional[float]

    def to_dict(self) -> dict:
        return asdict(self)


# ──────────────────────────────────────────────
# 1. FETCHER — raw HTTP call with retry logic
# ──────────────────────────────────────────────

def fetch_weather_json(
    locale_id: int,
    session: requests.Session,
    timeout: int = 8,
    retry_attempts: int = 2,
    retry_backoff: float = 0.3,
    delay: float = 0.05,
    
) -> Optional[dict]:
    """
    Fetch the raw JSON payload for a given locale ID.

    Returns the parsed dict on success, or None on any failure.
    Uses exponential-ish backoff on transient errors (5xx / connection issues).
    """
    url = BASE_URL
    params = {"idlocale": locale_id}

    for attempt in range(1, retry_attempts + 1):
        try:
            time.sleep(delay + random.uniform(0, 0.02))  # jitter to avoid bursts
            response = session.get(url, params=params, timeout=timeout)

            # 404 / 204 → locale doesn't exist, no retry needed
            if response.status_code in (404, 204):
                return None

            # Server errors → worth retrying
            if response.status_code >= 500:
                raise requests.HTTPError(f"Server error {response.status_code}")

            response.raise_for_status()
            return response.json()

        except (requests.JSONDecodeError, ValueError):
            logger.debug("Locale %d: malformed JSON", locale_id)
            return None  # no point retrying a parse failure

        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
            if attempt < retry_attempts:
                sleep_time = retry_backoff * (2 ** (attempt - 1))
                logger.debug(
                    "Locale %d: transient error (%s) — retry %d/%d in %.1fs",
                    locale_id, exc, attempt, retry_attempts, sleep_time,
                )
                time.sleep(sleep_time)
            else:
                logger.debug("Locale %d: giving up after %d attempts", locale_id, retry_attempts)
                return None

    return None


# ──────────────────────────────────────────────
# 2. VALIDATOR — determine if response is useful
# ──────────────────────────────────────────────

def is_valid_response(data: dict) -> bool:
    try:
        weather = data["data"]["getWeatherNow"][0]["data"][0]["weather"]
        locale  = data["data"]["getWeatherNow"][0]["data"][0]["locale"]

        return (
            weather.get("temperature") is not None
            and locale.get("city") is not None
        )
    except (KeyError, IndexError, TypeError):
        return False


def _dig(obj: dict, *keys):
    """Safely traverse nested dicts; returns None if any key is missing."""
    cur = obj
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


# ──────────────────────────────────────────────
# 3. EXTRACTOR — pull fields from valid payload
# ──────────────────────────────────────────────

def extract_fields(locale_id: int, data: dict) -> Optional[WeatherRecord]:
    try:
        root = data["data"]["getWeatherNow"][0]["data"][0]

        locale = root["locale"]
        weather = root["weather"]
        temp = float(weather.get("temperature"))
        feels = float(weather.get("sensation"))
        latitude = locale.get("latitude")
        longitude = locale.get("longitude")

        # 👇 FILTRO DE SANIDADE
        if temp < -10 or temp > 60:
            return None

        # opcional (esse é o que resolve seu caso)
        if temp < 5:
            return None

        return WeatherRecord(
            locale_id=locale_id,
            city=locale.get("city", "").strip(),
            state=locale.get("uf", "").strip().upper(),
            temperature=temp,
            feels_like=feels,
            latitude=latitude,        
            longitude=longitude,
        )

    except (KeyError, IndexError, TypeError, ValueError):
        return None

def _to_float(value) -> Optional[float]:
    """Convert a raw value to float, returning None on failure."""
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "."))
    except (ValueError, TypeError):
        return None


# ──────────────────────────────────────────────
# 4. WORKER — orchestrate one locale ID end-to-end
# ──────────────────────────────────────────────

def process_locale(
    locale_id: int,
    session: requests.Session,
    config: dict,
    state_filter: Optional[list[str]],
) -> Optional[WeatherRecord]:
    """Single-threaded unit of work: fetch → validate → extract → filter."""
    data = fetch_weather_json(
        locale_id,
        session,
        timeout=config["timeout"],
        retry_attempts=config["retry_attempts"],
        retry_backoff=config["retry_backoff"],
        delay=config["delay_between_requests"],
    )

    if not data or not is_valid_response(data):
        return None

    record = extract_fields(locale_id, data)

    if record is None:
        return None

    if state_filter and record.state not in state_filter:
        return None

    return record


# ──────────────────────────────────────────────
# 5. PARALLEL EXECUTION ENGINE
# ──────────────────────────────────────────────

def run_pipeline(config: dict) -> pd.DataFrame:
    """
    Drive the full pipeline:
      - Create a shared requests.Session (connection pooling)
      - Submit all locale IDs to the thread pool
      - Collect valid WeatherRecords with a live progress bar
      - Return a cleaned DataFrame
    """
    id_range = range(config["id_start"], config["id_end"] + 1)
    state_filter = config.get("state_filter")
    results: list[WeatherRecord] = []

    # Shared session: reuses TCP connections across threads (significant speedup)
    session = requests.Session()

    from requests.adapters import HTTPAdapter

    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (compatible; WeatherBot/1.0; +https://github.com/example)"
        ),
        "Accept": "application/json",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Referer": "https://www.climatempo.com.br/",
    })

    logger.info(
        "Starting pipeline: IDs %d–%d | workers=%d | filter=%s",
        config["id_start"], config["id_end"],
        config["max_workers"],
        state_filter or "ALL",
    )

    start_time = time.perf_counter()

    with ThreadPoolExecutor(max_workers=config["max_workers"]) as executor:
        futures = {
            executor.submit(process_locale, lid, session, config, state_filter): lid
            for lid in id_range
        }

        with tqdm(
            total=len(id_range),
            desc="Probing locales",
            unit="id",
            dynamic_ncols=True,
            colour="cyan",
        ) as pbar:
            for future in as_completed(futures):
                pbar.update(1)
                try:
                    record = future.result()
                    if record:
                        results.append(record)
                        pbar.set_postfix(found=len(results), refresh=False)
                except Exception as exc:
                    lid = futures[future]
                    logger.debug("Locale %d raised unexpected exception: %s", lid, exc)

    elapsed = time.perf_counter() - start_time
    logger.info(
        "Pipeline finished in %.1fs — %d valid cities found out of %d IDs probed.",
        elapsed, len(results), len(id_range),
    )

    return _build_dataframe(results)


# ──────────────────────────────────────────────
# 6 & 7. DATAFRAME BUILDER + CLEANER
# ──────────────────────────────────────────────

def _build_dataframe(records: list[WeatherRecord]) -> pd.DataFrame:
    """Convert records to a cleaned DataFrame with proper dtypes."""
    if not records:
        logger.warning("No valid records collected — returning empty DataFrame.")
        return pd.DataFrame(
            columns=["locale_id", "city", "state", "temperature", "feels_like"]
        )

    df = pd.DataFrame([r.to_dict() for r in records])

    # ── Numeric coercion (belt-and-suspenders after _to_float) ──
    df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce")
    df["feels_like"]  = pd.to_numeric(df["feels_like"],  errors="coerce")

    # ── Drop rows with no temperature at all (unusable) ──
    before = len(df)
    df.dropna(subset=["temperature"], inplace=True)
    dropped = before - len(df)
    if dropped:
        logger.info("Dropped %d rows with missing temperature.", dropped)

    # ── Normalise city names ──
    df["city"]  = df["city"].str.strip().str.title()
    df["state"] = df["state"].str.strip().str.upper()

    # ── Reset index ──
    df.reset_index(drop=True, inplace=True)

    return df


# ──────────────────────────────────────────────
# ANALYTICS — compute hottest / coldest cities
# ──────────────────────────────────────────────

def print_summary(df: pd.DataFrame) -> None:
    """Print a formatted summary of extreme temperatures."""
    if df.empty:
        print("\n⚠️  No data to summarise.")
        return

    sep = "─" * 55

    def extreme(col: str, func: str) -> pd.Series:
        method = df[col].idxmax if func == "max" else df[col].idxmin
        idx = method()
        return df.loc[idx]

    hottest_temp   = extreme("temperature", "max")
    coldest_temp   = extreme("temperature", "min")
    hottest_feels  = extreme("feels_like",  "max") if df["feels_like"].notna().any() else None
    coldest_feels  = extreme("feels_like",  "min") if df["feels_like"].notna().any() else None

    print(f"\n{'🌡️  WEATHER EXTREMES':^55}")
    print(sep)

    def row(label, rec, col):
        val = rec[col]
        val_str = f"{val:.1f}°C" if pd.notna(val) else "N/A"
        print(f"  {label:<28} {rec['city']}, {rec['state']}  ({val_str})")

    row("🔥 Hottest (actual temp):",  hottest_temp,  "temperature")
    row("❄️  Coldest (actual temp):", coldest_temp,  "temperature")
    if hottest_feels is not None:
        row("🔥 Hottest (feels like):", hottest_feels, "feels_like")
    if coldest_feels is not None:
        row("❄️  Coldest (feels like):", coldest_feels, "feels_like")

    print(sep)
    print(f"\n  Total cities: {len(df)}")
    print(f"  Avg temperature:  {df['temperature'].mean():.1f}°C")
    if df['feels_like'].notna().any():
        print(f"  Avg feels like:   {df['feels_like'].mean():.1f}°C")

    # Per-state breakdown
    if len(df["state"].unique()) > 1:
        print(f"\n{'📍 CITIES PER STATE':}")
        state_counts = df.groupby("state").size().sort_values(ascending=False)
        for state, count in state_counts.items():
            print(f"    {state:>4}: {count} cities")


# ──────────────────────────────────────────────
# SAVE TO CSV
# ──────────────────────────────────────────────

def save_csv(df: pd.DataFrame, path: str) -> None:
    """Persist the DataFrame to a CSV file with consistent formatting."""
    df.to_csv(
        path,
        index=False,
        encoding="utf-8-sig",       # BOM for Excel compat
        quoting=csv.QUOTE_NONNUMERIC,
        float_format="%.1f",
    )
    logger.info("Dataset saved → %s  (%d rows)", path, len(df))


# ──────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────

def main(config: dict | None = None) -> pd.DataFrame:
    """
    Public entry point.  Pass a partial config dict to override defaults.
    Returns the final DataFrame so callers can do their own analysis.
    """
    cfg = {**DEFAULT_CONFIG, **(config or {})}

    total_start = time.perf_counter()

    df = run_pipeline(cfg)
    print_summary(df)

    if not df.empty:
        save_csv(df, cfg["output_csv"])

    total_elapsed = time.perf_counter() - total_start
    print(f"\n⏱  Total execution time: {total_elapsed:.1f}s\n")

    return df


# ──────────────────────────────────────────────
# CLI usage
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Climatempo weather data pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--start",   type=int,   default=DEFAULT_CONFIG["id_start"],  help="First locale ID to probe")
    parser.add_argument("--end",     type=int,   default=DEFAULT_CONFIG["id_end"],    help="Last locale ID to probe")
    parser.add_argument("--workers", type=int,   default=DEFAULT_CONFIG["max_workers"], help="Thread pool size")
    parser.add_argument("--timeout", type=int,   default=DEFAULT_CONFIG["timeout"],   help="HTTP timeout (seconds)")
    parser.add_argument("--states",  nargs="*",  default=None,                        help="State filter e.g. --states SP RJ MG")
    parser.add_argument("--output",  type=str,   default=DEFAULT_CONFIG["output_csv"], help="Output CSV path")
    parser.add_argument("--delay",   type=float, default=DEFAULT_CONFIG["delay_between_requests"], help="Per-request delay")
    parser.add_argument("--retries", type=int,   default=DEFAULT_CONFIG["retry_attempts"], help="Retry attempts")
    parser.add_argument("--verbose", action="store_true", help="Enable DEBUG logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    override = {
        "id_start": args.start,
        "id_end": args.end,
        "max_workers": args.workers,
        "timeout": args.timeout,
        "state_filter": [s.upper() for s in args.states] if args.states else None,
        "output_csv": args.output,
        "delay_between_requests": args.delay,
        "retry_attempts": args.retries,
    }

    main(override)