#!/usr/bin/env python3
"""
Shared NSE delivery-volume helper for the Quantex scanners.

Fetches, caches, and provides delivery% data from NSE's daily bhavcopy.
Used by pocket_pivot_scanner.py and vol_expansion_scanner.py to filter
out false-positive breakouts caused by intraday algo/arbitrage volume
that lacks real institutional accumulation.

Usage:
    from delivery_data import get_delivery_pct, get_avg_delivery_pct

    dp = get_delivery_pct("RELIANCE")           # today (or last trading day)
    dp = get_delivery_pct("RELIANCE", "2026-07-08")
    avg = get_avg_delivery_pct("RELIANCE", 10)  # 10-day rolling avg

If bhavcopy is unavailable (network error, weekend, holiday), functions
return None — scanners should treat None as "data missing" and fall
back to their existing logic (don't kill signals due to missing data).

Local cache: quantex_logs/bhavcopy_cache/YYYY-MM-DD.csv
"""

import os
import io
import time
import logging
import warnings
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

warnings.filterwarnings("ignore")
logging.getLogger("urllib3").setLevel(logging.CRITICAL)

CACHE_DIR = Path(__file__).parent / "quantex_logs" / "bhavcopy_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# NSE endpoints (with fallback)
BHAVCOPY_URLS = [
    "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv",
    "https://archives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv",
]

# Headers to avoid 403 Forbidden from NSE
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/csv,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}


# ──────────────────────────────────────────────────────────────────────────────
# TRADING DAY UTILITIES
# ──────────────────────────────────────────────────────────────────────────────

_NSE_HOLIDAYS_2026 = {
    "2026-01-26", "2026-02-19", "2026-03-06", "2026-03-31", "2026-04-03",
    "2026-04-10", "2026-04-14", "2026-05-01", "2026-08-15", "2026-08-25",
    "2026-10-02", "2026-10-19", "2026-11-05", "2026-11-25", "2026-12-25",
}


def is_trading_day(d: date) -> bool:
    if d.weekday() >= 5:  # Sat/Sun
        return False
    if d.isoformat() in _NSE_HOLIDAYS_2026:
        return False
    return True


def last_trading_day(anchor: Optional[date] = None) -> date:
    """Return the most recent trading day at or before anchor (default today)."""
    d = anchor or date.today()
    for _ in range(10):
        if is_trading_day(d):
            return d
        d -= timedelta(days=1)
    return d


# ──────────────────────────────────────────────────────────────────────────────
# CACHE + DOWNLOAD
# ──────────────────────────────────────────────────────────────────────────────

def _cache_path(d: date) -> Path:
    return CACHE_DIR / f"{d.isoformat()}.csv"


def _download_bhavcopy(d: date) -> Optional[pd.DataFrame]:
    """Download NSE sec_bhavdata_full for a specific trading day."""
    ddmmyyyy = d.strftime("%d%m%Y")

    for url_template in BHAVCOPY_URLS:
        url = url_template.format(ddmmyyyy=ddmmyyyy)
        try:
            r = requests.get(url, headers=NSE_HEADERS, timeout=15)
            if r.status_code == 200 and len(r.content) > 1000:
                df = pd.read_csv(io.BytesIO(r.content))
                # Normalize column names (NSE has trailing spaces sometimes)
                df.columns = [c.strip() for c in df.columns]
                return df
        except Exception:
            continue
    return None


def get_bhavcopy(d: Optional[date] = None) -> Optional[pd.DataFrame]:
    """
    Return bhavcopy DataFrame for the given trading day (default: last one).
    Uses local cache if available; otherwise downloads and caches.

    Columns of interest:
      SYMBOL, SERIES, DATE1, CLOSE_PRICE, TTL_TRD_QNTY, DELIV_QTY, DELIV_PER
    """
    d = d or last_trading_day()
    cache = _cache_path(d)

    if cache.exists() and cache.stat().st_size > 500:
        try:
            df = pd.read_csv(cache)
            df.columns = [c.strip() for c in df.columns]
            return df
        except Exception:
            cache.unlink(missing_ok=True)

    df = _download_bhavcopy(d)
    if df is None:
        return None

    try:
        df.to_csv(cache, index=False)
    except Exception:
        pass
    return df


# ──────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ──────────────────────────────────────────────────────────────────────────────

def get_delivery_pct(symbol: str, when: Optional[str | date] = None) -> Optional[float]:
    """
    Return delivery% for a symbol on a specific trading day.

    Args:
        symbol: NSE tradingsymbol (e.g., "RELIANCE")
        when: date or ISO string ("YYYY-MM-DD"). Default: last trading day.

    Returns:
        float delivery %  (e.g., 47.5)
        None if data unavailable or symbol not found
    """
    if isinstance(when, str):
        try:
            when = datetime.strptime(when, "%Y-%m-%d").date()
        except Exception:
            when = None
    d = when or last_trading_day()

    df = get_bhavcopy(d)
    if df is None:
        return None

    # Match on SYMBOL + SERIES == EQ (equity segment only)
    if "SYMBOL" not in df.columns or "DELIV_PER" not in df.columns:
        return None

    row = df[(df["SYMBOL"] == symbol) & (df["SERIES"].str.strip() == "EQ")]
    if row.empty:
        return None

    try:
        val = row.iloc[0]["DELIV_PER"]
        if isinstance(val, str):
            val = val.strip()
            if val in ("-", "", "NA"):
                return None
        return float(val)
    except (ValueError, TypeError):
        return None


def get_avg_delivery_pct(symbol: str, days: int = 10,
                         anchor: Optional[date] = None) -> Optional[float]:
    """
    Rolling average delivery% over the last N trading days.
    Skips days with missing data. Returns None if fewer than half the requested
    days have data (unreliable average).
    """
    anchor = anchor or last_trading_day()
    d = anchor
    vals = []
    fetched = 0
    max_fetches = days * 2  # allow for weekends/holidays gap

    while len(vals) < days and fetched < max_fetches:
        if is_trading_day(d):
            v = get_delivery_pct(symbol, d)
            if v is not None:
                vals.append(v)
            fetched += 1
        d -= timedelta(days=1)

    if len(vals) < max(3, days // 2):
        return None

    return sum(vals) / len(vals)


def prefetch_bhavcopy(days_back: int = 15) -> int:
    """
    Pre-download bhavcopies for the last N trading days.
    Call once at scanner startup to warm cache. Returns count of successful pulls.
    """
    d = last_trading_day()
    ok = 0
    for _ in range(days_back * 2):
        if is_trading_day(d):
            df = get_bhavcopy(d)
            if df is not None:
                ok += 1
                if ok >= days_back:
                    break
        d -= timedelta(days=1)
    return ok


# ──────────────────────────────────────────────────────────────────────────────
# CLI DIAGNOSTIC
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    sym = sys.argv[1] if len(sys.argv) > 1 else "RELIANCE"
    print(f"Symbol: {sym}")
    print(f"Last trading day: {last_trading_day()}")

    today_pct = get_delivery_pct(sym)
    print(f"Today's delivery %: {today_pct}")

    avg10 = get_avg_delivery_pct(sym, 10)
    print(f"10-day avg delivery %: {avg10:.1f}" if avg10 else "10-day avg: N/A")

    avg30 = get_avg_delivery_pct(sym, 30)
    print(f"30-day avg delivery %: {avg30:.1f}" if avg30 else "30-day avg: N/A")
