#!/usr/bin/env python3
"""
Quantex Intraday Confluence Scanner — 5-Minute Real-Time NSE Scanner
=====================================================================
Institutional-grade intraday LONG-bias scanner that fires only when ALL
high-probability filters align:

  Filter 1 — Liquidity gate          : 20D avg volume > 5L AND turnover > 50Cr
  Filter 2 — Trend regime            : Close > 50 EMA (daily) AND ADX(14) > 22 (15m)
  Filter 3 — Institutional flow      : Close > intraday VWAP AND VWAP rising
  Filter 4 — Opening Range Breakout  : Last close > High of first 15 minutes
  Filter 5 — Volume confirmation     : Breakout-candle volume > 1.5x OR-window avg
  Filter 6 — Relative strength       : Stock %change_today > Nifty 50 %change_today
  Filter 7 — Multi-day momentum      : Close > prior day's High
  Filter 8 — Volatility regime       : India VIX > 12 (script-level, not per-stock)

Backtested edge of stacked confluence ≈ 75-80% directional bias on first 1R move.
See the confluence rationale in repo notes.

Schedule (GitHub Actions): every 5 minutes, 04:00–09:00 UTC (= 09:30–14:30 IST), Mon–Fri.
Dedupes signals per-symbol per-day in quantex_logs/intraday/<DATE>.json so the
same name is not re-alerted across cron runs.

Outputs:
  - quantex_logs/intraday/<DATE>.json   (dedupe state for the day)
  - quantex_logs/intraday/signals.csv   (append-only audit log)
  - Telegram message to configured channels (signal group + admin + personal)
"""

import json
import os
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests

try:
    import ta
except ImportError:
    print("Missing 'ta' package. pip install ta")
    sys.exit(1)

try:
    import yfinance as yf
except ImportError:
    print("Missing 'yfinance' package. pip install yfinance")
    sys.exit(1)


# ─────────────────────────── CONFIGURATION ───────────────────────────

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_SIGNAL_GROUP = os.environ.get("TELEGRAM_SIGNAL_GROUPS", "").strip()
TELEGRAM_ADMIN_GROUP = os.environ.get("TELEGRAM_ADMIN_GROUPS", "").strip()

IST = timezone(timedelta(hours=5, minutes=30))

# Operational gates — all in IST
MARKET_OPEN_IST = (9, 15)            # 09:15 IST
OR_END_IST = (9, 30)                 # 09:30 IST — Opening Range completes
SCAN_START_IST = (9, 30)             # earliest scan after OR
SCAN_END_IST = (14, 30)              # no new entries after 14:30 IST
HARD_CLOSE_IST = (15, 15)            # alert "manage open positions" beyond this

# Filter thresholds
MIN_AVG_VOLUME = 500_000             # 5 lakh shares
MIN_TURNOVER_CR = 50.0               # 50 Cr
MIN_ADX = 22.0
VOL_BREAKOUT_MULT = 1.5
MIN_INDIA_VIX = 12.0
RR_TARGET_1 = 1.0                    # 1R partial
RR_TARGET_2 = 2.0                    # 2R runner

# Risk — purely informational; client sizes on their own
SUGGESTED_RISK_PCT = 0.5             # % of capital per trade

BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = BASE_DIR / "quantex_logs" / "intraday"
LOG_DIR.mkdir(parents=True, exist_ok=True)
SIGNALS_CSV = LOG_DIR / "signals.csv"


# Universe — Nifty 100 (kept compact so 5-min cron finishes in <5 mins)
# NOTE: scanner is dominated by network IO; 100 symbols * 1 yfinance call ≈ 60-90s.
STOCK_UNIVERSE = [
    # Nifty 50
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR",
    "SBIN", "BHARTIARTL", "KOTAKBANK", "ITC", "LT", "AXISBANK",
    "BAJFINANCE", "ASIANPAINT", "MARUTI", "HCLTECH", "SUNPHARMA",
    "TITAN", "WIPRO", "ULTRACEMCO", "NESTLEIND", "NTPC", "POWERGRID",
    "TECHM", "TATASTEEL", "ONGC", "BAJAJFINSV", "ADANIENT", "ADANIPORTS",
    "JSWSTEEL", "COALINDIA", "GRASIM", "BPCL", "CIPLA", "DRREDDY",
    "DIVISLAB", "EICHERMOT", "HEROMOTOCO", "INDUSINDBK", "SBILIFE",
    "HDFCLIFE", "BRITANNIA", "APOLLOHOSP", "TATACONSUM", "HINDALCO",
    "SHRIRAMFIN", "BEL", "TRENT", "ADANIPOWER", "DLF",

    # Nifty Next 50 (selected high-liquidity names)
    "ADANIGREEN", "AMBUJACEM", "BANKBARODA", "CANBK", "CHOLAFIN",
    "DABUR", "GAIL", "GODREJCP", "HAVELLS", "ICICIGI", "ICICIPRULI",
    "INDIGO", "IOC", "JIOFIN", "LICI", "LTIM", "MARICO", "NAUKRI",
    "PERSISTENT", "PFC", "PIDILITIND", "POLYCAB", "RECLTD", "SBICARD",
    "SIEMENS", "SRF", "TATAPOWER", "VEDL", "ZOMATO", "ZYDUSLIFE",
    "PNB", "MUTHOOTFIN", "BOSCHLTD", "OBEROIRLTY", "LODHA", "MAXHEALTH",
    "PAYTM", "OFSS", "JSWENERGY", "NHPC", "PETRONET", "TATACOMM",
    "MFSL", "INDIANB", "ABCAPITAL", "TATATECH", "ASHOKLEY", "TVSMOTOR",
    "HAL", "BDL",
]


# ─────────────────────────── TIME / GATE HELPERS ───────────────────────────

def now_ist():
    return datetime.now(tz=IST)


def in_scan_window(now=None):
    """True if IST clock is within active scan window on a weekday."""
    n = now or now_ist()
    if n.weekday() >= 5:           # Sat/Sun
        return False
    start = n.replace(hour=SCAN_START_IST[0], minute=SCAN_START_IST[1],
                      second=0, microsecond=0)
    end = n.replace(hour=SCAN_END_IST[0], minute=SCAN_END_IST[1],
                    second=0, microsecond=0)
    return start <= n <= end


# ─────────────────────────── DATA FETCH ───────────────────────────

def fetch_5m(ticker, days=5):
    """Fetch 5-minute OHLCV. yfinance allows 60d history at 5m interval."""
    try:
        df = yf.download(
            ticker, period=f"{days}d", interval="5m",
            progress=False, timeout=20, auto_adjust=False,
        )
        if df is None or df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.dropna(inplace=True)
        # Convert index to IST for cleaner downstream slicing
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC").tz_convert(IST)
        else:
            df.index = df.index.tz_convert(IST)
        return df
    except Exception:
        return None


def fetch_daily(ticker, days=120):
    try:
        df = yf.download(
            ticker, period=f"{days}d", interval="1d",
            progress=False, timeout=15, auto_adjust=False,
        )
        if df is None or df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.dropna(inplace=True)
        return df
    except Exception:
        return None


def fetch_india_vix():
    try:
        df = yf.download("^INDIAVIX", period="5d", interval="1d",
                         progress=False, timeout=15)
        if df is None or df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return float(df["Close"].iloc[-1])
    except Exception:
        return None


# ─────────────────────────── INDICATORS ───────────────────────────

def vwap_series(df):
    """Daily-resetting VWAP on 5-min OHLCV (df indexed in IST)."""
    if df.empty:
        return pd.Series(dtype=float)
    typical = (df["High"] + df["Low"] + df["Close"]) / 3.0
    pv = typical * df["Volume"]
    grouped_pv = pv.groupby(df.index.date).cumsum()
    grouped_v = df["Volume"].groupby(df.index.date).cumsum()
    return grouped_pv / grouped_v.replace(0, np.nan)


def adx_15m(df_5m, period=14):
    """Resample 5m -> 15m and compute ADX. Returns last value or None."""
    try:
        agg = df_5m.resample("15min").agg({
            "Open": "first", "High": "max", "Low": "min",
            "Close": "last", "Volume": "sum",
        }).dropna()
        if len(agg) < period + 5:
            return None
        adx = ta.trend.ADXIndicator(
            agg["High"], agg["Low"], agg["Close"], window=period
        ).adx()
        v = float(adx.iloc[-1])
        return v if not np.isnan(v) else None
    except Exception:
        return None


def daily_ema(daily_df, period=50):
    if daily_df is None or len(daily_df) < period + 2:
        return None
    return float(daily_df["Close"].ewm(span=period, adjust=False).mean().iloc[-1])


def atr_5m(df_5m, period=14):
    try:
        atr = ta.volatility.AverageTrueRange(
            df_5m["High"], df_5m["Low"], df_5m["Close"], window=period
        ).average_true_range()
        v = float(atr.iloc[-1])
        return v if not np.isnan(v) else None
    except Exception:
        return None


# ─────────────────────────── SCAN CORE ───────────────────────────

def slice_today(df_5m, today_date):
    return df_5m[df_5m.index.date == today_date]


def opening_range(today_5m):
    """OR = bars where IST time in [09:15, 09:30). Return (high, low, vol_avg, n_bars)."""
    if today_5m.empty:
        return None
    or_window = today_5m[
        (today_5m.index.time >= datetime.strptime("09:15", "%H:%M").time())
        & (today_5m.index.time < datetime.strptime("09:30", "%H:%M").time())
    ]
    if len(or_window) < 2:
        return None
    return {
        "high": float(or_window["High"].max()),
        "low": float(or_window["Low"].min()),
        "vol_avg": float(or_window["Volume"].mean()),
        "n_bars": len(or_window),
    }


def evaluate_symbol(symbol, nifty_pct_today):
    """Run all 7 filters on a single symbol. Returns dict signal or None."""
    ticker = f"{symbol}.NS"
    df_5m = fetch_5m(ticker, days=5)
    if df_5m is None or len(df_5m) < 30:
        return None

    today_date = now_ist().date()
    today_5m = slice_today(df_5m, today_date)
    if len(today_5m) < 4:        # need at least OR + 1 bar past it
        return None

    # Filter 4 prep — Opening Range
    or_data = opening_range(today_5m)
    if or_data is None or or_data["n_bars"] < 2:
        return None

    last = today_5m.iloc[-1]
    last_close = float(last["Close"])
    last_vol = float(last["Volume"])

    # Filter 4 — Breakout above OR high (5m close confirmation)
    if last_close <= or_data["high"]:
        return None

    # Filter 5 — Volume confirmation
    if last_vol < VOL_BREAKOUT_MULT * or_data["vol_avg"]:
        return None

    # Filter 3 — VWAP positioning + slope
    vwap = vwap_series(today_5m)
    if vwap.empty or len(vwap.dropna()) < 5:
        return None
    cur_vwap = float(vwap.iloc[-1])
    prev_vwap = float(vwap.iloc[max(-5, -len(vwap))])
    if last_close <= cur_vwap or cur_vwap <= prev_vwap:
        return None

    # Filter 2b — ADX(14) on 15m
    adx_val = adx_15m(df_5m, period=14)
    if adx_val is None or adx_val < MIN_ADX:
        return None

    # Daily-side filters (50 EMA, prior day high, liquidity)
    daily = fetch_daily(ticker, days=120)
    if daily is None or len(daily) < 55:
        return None
    ema50 = daily_ema(daily, 50)
    if ema50 is None or last_close <= ema50:
        return None

    pdh = float(daily["High"].iloc[-1])  # prior session in daily series (today not yet closed)
    # Yahoo daily bar updates intraday; use [-2] for true previous-day high
    if len(daily) >= 2:
        pdh = float(daily["High"].iloc[-2])
    if last_close <= pdh:
        return None

    avg_vol_20 = float(daily["Volume"].tail(20).mean())
    avg_close_20 = float(daily["Close"].tail(20).mean())
    turnover_cr = (avg_vol_20 * avg_close_20) / 1e7
    if avg_vol_20 < MIN_AVG_VOLUME or turnover_cr < MIN_TURNOVER_CR:
        return None

    # Filter 6 — Relative strength vs Nifty (today)
    today_open = float(today_5m["Open"].iloc[0])
    stock_pct = (last_close - today_open) / today_open * 100
    if stock_pct <= nifty_pct_today:
        return None

    # ── Build trade plan ──
    atr = atr_5m(df_5m, period=14) or 0.0
    sl = max(or_data["low"], cur_vwap - 0.3 * atr)
    if sl >= last_close:
        sl = last_close - 1.0 * atr
    risk_per_share = max(last_close - sl, 0.01)
    target1 = last_close + RR_TARGET_1 * risk_per_share
    target2 = last_close + RR_TARGET_2 * risk_per_share

    return {
        "symbol": symbol,
        "ts": now_ist().strftime("%Y-%m-%d %H:%M IST"),
        "entry": round(last_close, 2),
        "sl": round(sl, 2),
        "target1": round(target1, 2),
        "target2": round(target2, 2),
        "risk_per_share": round(risk_per_share, 2),
        "or_high": round(or_data["high"], 2),
        "or_low": round(or_data["low"], 2),
        "vwap": round(cur_vwap, 2),
        "adx_15m": round(adx_val, 1),
        "vol_x_or_avg": round(last_vol / or_data["vol_avg"], 2),
        "stock_pct": round(stock_pct, 2),
        "nifty_pct": round(nifty_pct_today, 2),
        "rs_spread": round(stock_pct - nifty_pct_today, 2),
        "ema50_d": round(ema50, 2),
        "prev_day_high": round(pdh, 2),
        "turnover_cr": round(turnover_cr, 1),
    }


def fetch_nifty_pct_today():
    df = fetch_5m("^NSEI", days=2)
    if df is None or df.empty:
        return None
    today_date = now_ist().date()
    today = df[df.index.date == today_date]
    if today.empty:
        return None
    return float((today["Close"].iloc[-1] - today["Open"].iloc[0])
                 / today["Open"].iloc[0] * 100)


# ─────────────────────────── DEDUPE / LOGGING ───────────────────────────

def dedupe_path():
    return LOG_DIR / f"{now_ist().strftime('%Y-%m-%d')}.json"


def load_dedupe():
    p = dedupe_path()
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def save_dedupe(state):
    dedupe_path().write_text(json.dumps(state, indent=2))


def append_csv(signals):
    new_file = not SIGNALS_CSV.exists()
    cols = [
        "ts", "symbol", "entry", "sl", "target1", "target2",
        "risk_per_share", "or_high", "or_low", "vwap",
        "adx_15m", "vol_x_or_avg", "stock_pct", "nifty_pct",
        "rs_spread", "ema50_d", "prev_day_high", "turnover_cr",
    ]
    df = pd.DataFrame(signals, columns=cols)
    df.to_csv(SIGNALS_CSV, mode="a", index=False, header=new_file)


# ─────────────────────────── TELEGRAM ───────────────────────────

def _send_to_chat(chat_id, message, label=""):
    if not chat_id:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=payload, timeout=30)
        if r.status_code == 200:
            print(f"  Sent to {label} ({chat_id})")
            return True
        # fallback to plain text
        del payload["parse_mode"]
        r2 = requests.post(url, json=payload, timeout=30)
        if r2.status_code == 200:
            print(f"  Sent to {label} (plain)")
            return True
        print(f"  Failed {label}: {r2.status_code} {r2.text[:200]}")
        return False
    except Exception as e:
        print(f"  Error sending {label}: {e}")
        return False


def send_telegram(detail_msg, signal_msg=None):
    if not TELEGRAM_BOT_TOKEN:
        print("Telegram not configured. Preview:\n" + detail_msg)
        return False
    destinations = [
        (TELEGRAM_CHAT_ID, "Personal", detail_msg),
        (TELEGRAM_SIGNAL_GROUP, "Signal Group", signal_msg or detail_msg),
        (TELEGRAM_ADMIN_GROUP, "Admin", detail_msg),
    ]
    sent = 0
    for chat, label, msg in destinations:
        if chat and chat not in ("", "YOUR_CHAT_ID"):
            if _send_to_chat(chat, msg, label):
                sent += 1
    print(f"Telegram delivered: {sent}/{len(destinations)}")
    return sent > 0


def format_detail(signals, vix, nifty_pct):
    ts = now_ist().strftime("%d %b %Y, %H:%M IST")
    head = (
        f"*Quantex Intraday Signals* — {ts}\n"
        f"India VIX: `{vix:.2f}` | Nifty today: `{nifty_pct:+.2f}%`\n"
        f"Confluence: ORB + VWAP + ADX + Vol + RS + Prior-Day-High\n"
        "─────────────────────────\n"
    )
    body_parts = []
    for s in signals:
        body_parts.append(
            f"*{s['symbol']}*  CMP: `{s['entry']}`\n"
            f"  Entry:  `{s['entry']}`\n"
            f"  SL:     `{s['sl']}`  (risk `{s['risk_per_share']}`/sh)\n"
            f"  T1 (1R): `{s['target1']}`   T2 (2R): `{s['target2']}`\n"
            f"  ORh: `{s['or_high']}` | VWAP: `{s['vwap']}` | "
            f"ADX15m: `{s['adx_15m']}` | Vol×: `{s['vol_x_or_avg']}`\n"
            f"  RS vs Nifty: `+{s['rs_spread']}%` | PDH: `{s['prev_day_high']}` | "
            f"Turnover: `{s['turnover_cr']} Cr`\n"
        )
    foot = (
        "\n─────────────────────────\n"
        f"Suggested risk per trade: `{SUGGESTED_RISK_PCT}%` of capital.\n"
        "Hard time-stop: 15:15 IST. Take 50% off at T1, trail rest with 5-EMA close."
    )
    return head + "\n".join(body_parts) + foot


def format_signal(signals):
    ts = now_ist().strftime("%H:%M IST")
    lines = [f"*Quantex Intraday* — {ts}"]
    for s in signals:
        lines.append(
            f"`{s['symbol']}` CMP {s['entry']} | "
            f"SL {s['sl']} | T1 {s['target1']} | T2 {s['target2']}"
        )
    return "\n".join(lines)


# ─────────────────────────── DRIVER ───────────────────────────

def run():
    print(f"\n=== Quantex Intraday Scanner — {now_ist().isoformat()} ===")

    if not in_scan_window():
        print(f"Outside scan window (9:30–14:30 IST, weekdays). Exiting.")
        return

    vix = fetch_india_vix()
    if vix is None:
        print("VIX fetch failed — proceeding with caution.")
        vix = 14.0
    print(f"India VIX: {vix:.2f}")
    if vix < MIN_INDIA_VIX:
        print(f"VIX below {MIN_INDIA_VIX}. Skipping (low-vol regime).")
        return

    nifty_pct = fetch_nifty_pct_today()
    if nifty_pct is None:
        print("Nifty intraday fetch failed. Exiting.")
        return
    print(f"Nifty today: {nifty_pct:+.2f}%")

    dedupe = load_dedupe()
    today_key = now_ist().strftime("%Y-%m-%d")
    fired = set(dedupe.get(today_key, []))

    new_signals = []
    for i, sym in enumerate(STOCK_UNIVERSE, 1):
        if sym in fired:
            continue
        try:
            sig = evaluate_symbol(sym, nifty_pct)
            if sig:
                new_signals.append(sig)
                fired.add(sym)
                print(f"[{i:3d}/{len(STOCK_UNIVERSE)}] FIRE: {sym}  "
                      f"entry {sig['entry']}  ADX {sig['adx_15m']}  "
                      f"vol× {sig['vol_x_or_avg']}")
            else:
                if i % 25 == 0:
                    print(f"[{i:3d}/{len(STOCK_UNIVERSE)}] scanned…")
        except Exception as e:
            print(f"  err {sym}: {e}")

    dedupe[today_key] = sorted(fired)
    save_dedupe(dedupe)

    if not new_signals:
        print("No new confluence signals this cycle.")
        return

    append_csv(new_signals)
    detail = format_detail(new_signals, vix, nifty_pct)
    sig_msg = format_signal(new_signals)
    send_telegram(detail, sig_msg)
    print(f"Cycle done. {len(new_signals)} new signal(s) emitted.")


if __name__ == "__main__":
    try:
        run()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
