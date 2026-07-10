#!/usr/bin/env python3
"""
Shared data source for ALL Quantex scanners.

Single import point — Kite Connect (live) first, yfinance (delayed) fallback.
Wraps pro_scanner.py's KiteSession + fetch_stock_data + fetch_nifty_data
so every scanner uses the SAME data with the SAME fallback logic.

Public API:
    fetch_daily(symbol, days=252)   # OHLCV daily bars
    fetch_nifty(days=252)           # Nifty index bars
    ensure_login()                  # log in to Kite (idempotent)
    kite_ok()                       # True if Kite session active

Column contract (both Kite + yfinance normalized):
    DataFrame.columns == ['Open', 'High', 'Low', 'Close', 'Volume', ...]
    Index = pd.DatetimeIndex (sorted ascending)

Fallback order:
    1. Kite historical (if session logged in)
    2. yfinance download (if Kite session absent/failed)
    3. None (both failed)

For scanners that MUST have live data (btst_scanner, intraday_sell_scanner),
use `require_kite=True` — will return None instead of falling back to yfinance.
"""

import io
import sys
import contextlib
import logging
import warnings
from typing import Optional

import pandas as pd

warnings.filterwarnings("ignore")
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)


@contextlib.contextmanager
def _silence():
    se = sys.stderr
    sys.stderr = io.StringIO()
    try:
        yield
    finally:
        sys.stderr = se


# ──────────────────────────────────────────────────────────────────────────────
# IMPORT pro_scanner's shared plumbing (single source of truth)
# ──────────────────────────────────────────────────────────────────────────────

try:
    from pro_scanner import (
        kite_session as _kite_session,
        fetch_stock_data as _pro_fetch_stock,
        fetch_nifty_data as _pro_fetch_nifty,
    )
    _PRO_AVAILABLE = True
except Exception as _e:
    print(f"!! pro_scanner unavailable ({_e}); Kite integration disabled")
    _PRO_AVAILABLE = False
    _kite_session = None

try:
    import yfinance as yf
    _YF_AVAILABLE = True
except Exception:
    _YF_AVAILABLE = False


# ──────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ──────────────────────────────────────────────────────────────────────────────

def ensure_login() -> bool:
    """Log in to Kite if not already. Returns True if session is active."""
    if not _PRO_AVAILABLE:
        return False
    if _kite_session.logged_in:
        return True
    try:
        return _kite_session.login()
    except Exception as e:
        print(f"   Kite login failed: {e}")
        return False


def kite_ok() -> bool:
    """True if Kite session is active."""
    return _PRO_AVAILABLE and _kite_session is not None and _kite_session.logged_in


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure canonical column names + sorted index."""
    if df is None or df.empty:
        return df
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    # Title-case column names
    df.columns = [c.title() if isinstance(c, str) else c for c in df.columns]
    # Ensure ascending index
    if not df.index.is_monotonic_increasing:
        df = df.sort_index()
    return df.dropna(subset=["Close"] if "Close" in df.columns else None)


def fetch_daily(symbol: str, days: int = 252,
                require_kite: bool = False) -> Optional[pd.DataFrame]:
    """
    Fetch daily OHLCV bars for a symbol.

    Args:
        symbol: NSE tradingsymbol (e.g., "RELIANCE")
        days: how many trading days back (default 252 = 1 year)
        require_kite: if True, only use Kite (no yfinance fallback)

    Returns:
        DataFrame with columns [Open, High, Low, Close, Volume, ...]
        None if all sources failed OR fewer than 200 bars available
    """
    # Try Kite first
    if _PRO_AVAILABLE and _kite_session and _kite_session.logged_in:
        try:
            df = _kite_session.get_historical(symbol, days=days)
            df = _normalize(df)
            if df is not None and len(df) >= 200:
                return df
        except Exception:
            pass

    if require_kite:
        return None  # strict-Kite mode (btst, intraday_sell)

    # Fallback to yfinance
    if _YF_AVAILABLE:
        # Map days → yfinance period string
        if days <= 30:
            period = "1mo"
        elif days <= 90:
            period = "3mo"
        elif days <= 180:
            period = "6mo"
        elif days <= 365:
            period = "1y"
        elif days <= 730:
            period = "2y"
        else:
            period = "5y"

        try:
            with _silence():
                df = yf.Ticker(f"{symbol}.NS").history(period=period, interval="1d")
            df = _normalize(df)
            if df is not None and len(df) >= 200:
                return df
        except Exception:
            return None

    return None


def fetch_nifty(days: int = 365,
                require_kite: bool = False) -> Optional[pd.DataFrame]:
    """
    Fetch Nifty 50 index daily bars.

    Same fallback logic as fetch_daily.
    """
    if _PRO_AVAILABLE:
        try:
            # pro_scanner's fetch_nifty_data takes a period string
            if days <= 90:
                period = "3mo"
            elif days <= 365:
                period = "1y"
            else:
                period = "2y"
            df = _pro_fetch_nifty(period=period)
            df = _normalize(df)
            if df is not None and len(df) >= 200:
                return df
        except Exception:
            pass

    if require_kite:
        return None

    if _YF_AVAILABLE:
        try:
            with _silence():
                df = yf.Ticker("^NSEI").history(period="2y", interval="1d")
            df = _normalize(df)
            if df is not None and len(df) >= 200:
                return df
        except Exception:
            return None

    return None


# ──────────────────────────────────────────────────────────────────────────────
# CLI DIAGNOSTIC
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Attempting Kite login...")
    logged_in = ensure_login()
    print(f"Kite active: {logged_in}")

    sym = sys.argv[1] if len(sys.argv) > 1 else "RELIANCE"
    print(f"\nFetching {sym}...")
    df = fetch_daily(sym)
    if df is not None:
        print(f"  Got {len(df)} bars from {'Kite' if logged_in else 'yfinance'}")
        print(f"  Latest close: ₹{df['Close'].iloc[-1]:.2f} on {df.index[-1]}")
    else:
        print("  Failed to fetch")

    print("\nFetching Nifty 50...")
    ndf = fetch_nifty()
    if ndf is not None:
        print(f"  Got {len(ndf)} bars, latest: {ndf['Close'].iloc[-1]:.0f}")
    else:
        print("  Failed")
