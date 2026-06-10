#!/usr/bin/env python3
"""
Quantex Intraday SELL Scanner (F&O universe)
=============================================

Runs at 11:30 AM IST Mon-Fri. Surfaces high-conviction intraday SHORT setups
in F&O-listed stocks where ALL these align (anti-buy mirror):

    1. Stock is in confirmed Stage 3 chop or Stage 4 downtrend
    2. Today's close in BOTTOM 10% of day's range (institutional dumping)
    3. RED body ≤ -1.5% (real breakdown candle)
    4. Today's close BELOW intraday VWAP (the killer intraday short filter)
    5. Today's close < yesterday's low (broke prior swing support)
    6. Volume ≥ 1.5× of 20-day avg AND down-day volume signature
    7. Stock UNDERperformed Nifty today (negative RS)
    8. No false-signal patterns (oversold, hammer, support cluster, etc.)

Architecture: Kite-only (no yfinance fallback). MIS intraday trade plan.
Hard square-off by 3:15 PM IST.
"""

import os
import sys
import json
import time
import io
import contextlib
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np

# Silence yfinance/urllib3/peewee noise
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("peewee").setLevel(logging.CRITICAL)


@contextlib.contextmanager
def _silence_stdio():
    se = sys.stderr
    sys.stderr = io.StringIO()
    try:
        yield
    finally:
        sys.stderr = se


import ta
import requests


# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_ADMIN_GROUPS = os.environ.get("TELEGRAM_ADMIN_GROUPS", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

KITE_API_KEY = os.environ.get("KITE_API_KEY", "").strip()
KITE_API_SECRET = os.environ.get("KITE_API_SECRET", "").strip()
ZERODHA_USER_ID = os.environ.get("ZERODHA_USER_ID", "").strip()
ZERODHA_PASSWORD = os.environ.get("ZERODHA_PASSWORD", "").strip()
ZERODHA_TOTP_KEY = os.environ.get("ZERODHA_TOTP_KEY", "").strip()

MAX_SIGNALS_PER_DAY = 5
SCORE_THRESHOLD = 65

LOG_DIR = Path(__file__).parent / "quantex_logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
PICKS_JSON = LOG_DIR / "intraday_sell_picks.json"
RECS_JSON = LOG_DIR / "recommendations.json"

SCANNER_TYPE = "intraday_sell"

# Layer 1 — Universe filters
TURNOVER_MIN_CR = 50.0
PRICE_MIN = 100.0
PRICE_MAX = 15000.0

# Layer 2 — Trend (must be on SELL side)
SLOPE_DAYS = 5
ADX_MIN = 20

# Layer 3 — Today's price action (the breakdown signature)
CLOSE_POS_MAX = 0.10           # Close in BOTTOM 10% of range
BODY_PCT_MAX = -1.5            # Red body ≤ -1.5%
RANGE_VS_ATR_MIN = 1.0

# Layer 4 — Volume signature (anti-Pocket-Pivot)
VOL_MULT_MIN = 1.5
UP_VOL_LOOKBACK = 10

# Layer 5 — Momentum + Relative Strength
RSI_MIN = 30                   # Avoid oversold (mean-reversion risk)
RSI_MAX = 60
PCT_FROM_52W_LOW_MAX = 15.0    # Within 15% of 52w low (room to fall)

# Layer 7 — False-signal killers
RED_STREAK_MAX = 5
GAP_DOWN_AT_OPEN_MAX_PCT = -4.0
DOJI_BODY_RATIO_MAX = 0.10
HAMMER_LOWER_WICK_RATIO = 2.0

# Intraday trade plan
STOP_MAX_PCT = 2.0            # +2% above entry (tight intraday stop)
T1_PCT = -2.0                 # -2% from entry (typical intraday breakdown)
T2_PCT = -4.0                 # -4% (continuation runner)
HARD_CLOSE_TIME = "15:15"     # MIS square-off cutoff


# ──────────────────────────────────────────────────────────────────────────────
# KITE INTEGRATION (mandatory — no yfinance fallback)
# ──────────────────────────────────────────────────────────────────────────────

try:
    from pro_scanner import KiteSession  # type: ignore
    _KITE_CLASS_AVAILABLE = True
except Exception as _kite_err:
    _KITE_CLASS_AVAILABLE = False
    KiteSession = None

_kite = None


def _kite_credentials_present():
    return all([KITE_API_KEY, KITE_API_SECRET, ZERODHA_USER_ID, ZERODHA_PASSWORD, ZERODHA_TOTP_KEY])


def _init_kite():
    global _kite
    if _kite is not None:
        return _kite if _kite.logged_in else None
    if not _KITE_CLASS_AVAILABLE or not _kite_credentials_present():
        _kite = type('NullKite', (), {'logged_in': False})()
        return None
    _kite = KiteSession()
    if _kite.login():
        return _kite
    return None


def get_fno_universe(kite):
    """Get F&O underlying stock symbols from Kite's NFO instrument list."""
    if not kite or not kite.nfo_futures:
        return []
    fno_symbols = sorted(set(
        f["name"] for f in kite.nfo_futures
        if f.get("instrument_type") == "FUT" and f.get("name")
    ))
    return fno_symbols


# ──────────────────────────────────────────────────────────────────────────────
# DATA HELPERS — historical + intraday OHLC + VWAP
# ──────────────────────────────────────────────────────────────────────────────

def fetch_daily(symbol):
    """400 calendar days of daily bars from Kite. ~250 trading days."""
    kite = _init_kite()
    if kite is None:
        return None
    try:
        df = kite.get_historical(symbol, days=400)
        if df is None or df.empty or len(df) < 200:
            return None
        return df
    except Exception:
        return None


def fetch_intraday_ohlc_and_vwap(symbol):
    """Get today's intraday OHLC + VWAP via Kite minute-level bars.
    Returns (today_open, today_high, today_low, today_close, today_volume, today_vwap) or None.
    """
    kite = _init_kite()
    if kite is None:
        return None
    token = kite.instrument_map.get(symbol) if kite.instrument_map else None
    if not token:
        return None
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        bars = kite.kite.historical_data(token, today, today, interval="5minute")
        if not bars or len(bars) == 0:
            return None
        df = pd.DataFrame(bars)
        df.columns = [c.lower() for c in df.columns]
        if "volume" not in df or "close" not in df:
            return None
        today_open = float(df["open"].iloc[0])
        today_high = float(df["high"].max())
        today_low = float(df["low"].min())
        today_close = float(df["close"].iloc[-1])
        today_volume = float(df["volume"].sum())
        # VWAP = sum(typical_price × vol) / sum(vol)
        tp = (df["high"] + df["low"] + df["close"]) / 3.0
        vol_sum = float(df["volume"].sum())
        vwap = float((tp * df["volume"]).sum() / vol_sum) if vol_sum > 0 else today_close
        return {
            "open": today_open, "high": today_high, "low": today_low,
            "close": today_close, "volume": today_volume, "vwap": vwap,
            "bars_count": len(df),
        }
    except Exception:
        return None


def fetch_nifty():
    """Nifty 50 daily + today's intraday close (Kite-only)."""
    kite = _init_kite()
    if kite is None:
        return None
    token = kite.instrument_map.get("NIFTY 50") if kite.instrument_map else None
    if not token:
        for key in (kite.instrument_map or {}):
            if "NIFTY" in key and "BANK" not in key and "50" in key:
                token = kite.instrument_map[key]
                break
    if not token:
        return None
    try:
        from_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
        to_date = datetime.now().strftime("%Y-%m-%d")
        hist = kite.kite.historical_data(token, from_date, to_date, interval="day")
        if not hist:
            return None
        df = pd.DataFrame(hist)
        df.set_index("date", inplace=True)
        df.index = pd.to_datetime(df.index)
        df.rename(columns={"open": "Open", "high": "High", "low": "Low",
                           "close": "Close", "volume": "Volume"}, inplace=True)
        # Refresh today's Nifty with live quote
        try:
            q = kite.kite.quote(["NSE:NIFTY 50"])
            nq = q.get("NSE:NIFTY 50") if isinstance(q, dict) else None
            if nq and "last_price" in nq:
                df.at[df.index[-1], "Close"] = float(nq["last_price"])
        except Exception:
            pass
        return df
    except Exception:
        return None


def annotate(df):
    out = df.copy()
    out["EMA20"] = out["Close"].ewm(span=20, adjust=False).mean()
    out["DMA50"] = out["Close"].rolling(50).mean()
    out["DMA200"] = out["Close"].rolling(200).mean()
    out["RSI"] = ta.momentum.RSIIndicator(out["Close"], window=14).rsi()
    out["ATR14"] = ta.volatility.AverageTrueRange(
        out["High"], out["Low"], out["Close"], window=14
    ).average_true_range()
    try:
        out["ADX"] = ta.trend.ADXIndicator(
            out["High"], out["Low"], out["Close"], window=14
        ).adx()
    except Exception:
        out["ADX"] = 25
    out["Vol20"] = out["Volume"].rolling(20).mean()
    return out


# ──────────────────────────────────────────────────────────────────────────────
# RELATIVE STRENGTH (negative for shorts)
# ──────────────────────────────────────────────────────────────────────────────

def stock_vs_nifty_today(df, nifty_df):
    """Stock's underperformance vs Nifty today. Negative = stock LAGGED."""
    try:
        s_today = (float(df['Close'].iloc[-1]) / float(df['Close'].iloc[-2]) - 1) * 100
        n_today = (float(nifty_df['Close'].iloc[-1]) / float(nifty_df['Close'].iloc[-2]) - 1) * 100
        return s_today - n_today, s_today, n_today
    except Exception:
        return 0, 0, 0


# ──────────────────────────────────────────────────────────────────────────────
# PATTERN DETECTION — INTRADAY SELL
# ──────────────────────────────────────────────────────────────────────────────

def detect_intraday_sell(symbol, df_daily, intraday, nifty_df):
    """Return dict if symbol qualifies for an intraday SHORT entry, else None.

    df_daily = annotated historical daily bars (today's row will be replaced
               with intraday aggregates).
    intraday = dict from fetch_intraday_ohlc_and_vwap(symbol).
    nifty_df = Nifty 50 historical with today's close refreshed.
    """
    if df_daily is None or len(df_daily) < 200:
        return None
    if intraday is None:
        return None

    # Replace today's row in daily df with intraday aggregates
    df = df_daily.copy()
    idx = df.index[-1]
    df.at[idx, 'Open'] = intraday['open']
    df.at[idx, 'High'] = intraday['high']
    df.at[idx, 'Low'] = intraday['low']
    df.at[idx, 'Close'] = intraday['close']
    df.at[idx, 'Volume'] = intraday['volume']

    last = df.iloc[-1]
    prev = df.iloc[-2]
    close = float(last['Close'])
    open_ = float(last['Open'])
    high = float(last['High'])
    low = float(last['Low'])
    vol = float(last['Volume'])
    prev_close = float(prev['Close'])
    prev_low = float(prev['Low'])
    vwap = float(intraday['vwap'])

    # ── LAYER 1: UNIVERSE FILTERS ──
    if not (PRICE_MIN <= close <= PRICE_MAX):
        return None
    avg_turnover_cr = float((df["Close"].tail(20) * df["Volume"].tail(20)).mean()) / 1e7
    if avg_turnover_cr < TURNOVER_MIN_CR:
        return None

    # ── LAYER 2: TREND HEALTH (must be SELL side) ──
    dma50 = float(last['DMA50']) if not pd.isna(last['DMA50']) else None
    dma200 = float(last['DMA200']) if not pd.isna(last['DMA200']) else None
    ema20 = float(last['EMA20']) if not pd.isna(last['EMA20']) else None
    rsi = float(last['RSI']) if not pd.isna(last['RSI']) else None
    atr = float(last['ATR14']) if not pd.isna(last['ATR14']) else None
    vol20 = float(last['Vol20']) if not pd.isna(last['Vol20']) else None
    adx = float(last['ADX']) if not pd.isna(last['ADX']) else 20

    if not all([dma50, dma200, ema20, rsi, atr, vol20]):
        return None

    # Stage 3 (close < 50-DMA, 50-DMA > 200-DMA) OR Stage 4 (close < 50-DMA < 200-DMA)
    # NEVER short Stage 2 (close > 50 > 200)
    if close >= dma50:
        return None  # Not in breakdown stage
    dma50_prev = float(df['DMA50'].iloc[-1 - SLOPE_DAYS]) if len(df) > SLOPE_DAYS else dma50
    slope_50_pct = (dma50 - dma50_prev) / dma50_prev * 100 if dma50_prev > 0 else 0
    if slope_50_pct > 0.5:
        return None  # 50-DMA still rising — not yet broken
    if close > ema20:
        return None  # short-term momentum still bullish
    if adx < ADX_MIN:
        return None  # not trending

    is_stage_4 = close < dma50 < dma200
    is_stage_3 = close < dma50 and dma50 >= dma200

    # ── LAYER 3: TODAY'S BREAKDOWN SIGNATURE ──
    today_range = high - low
    if today_range <= 0:
        return None
    close_pos = (close - low) / today_range
    if close_pos > CLOSE_POS_MAX:
        return None  # close not in bottom 10%
    body_pct = (close - open_) / open_ * 100 if open_ > 0 else 0
    if body_pct > BODY_PCT_MAX:
        return None  # not a real red body
    if today_range / atr < RANGE_VS_ATR_MIN:
        return None
    if close >= prev_low:
        return None  # didn't break yesterday's low

    # ── LAYER 4: VOLUME SIGNATURE (anti-Pocket-Pivot) ──
    vol_mult = vol / vol20 if vol20 > 0 else 0
    if vol_mult < VOL_MULT_MIN:
        return None
    # Today DOWN-vol must exceed MAX UP-vol of prior 10 days
    last_10 = df.iloc[-(UP_VOL_LOOKBACK + 1):-1]
    up_vols = last_10.loc[last_10['Close'] > last_10['Open'], 'Volume']
    if len(up_vols) > 0:
        max_up_vol_10 = float(up_vols.max())
        anti_pp_signature = vol > max_up_vol_10
    else:
        max_up_vol_10 = float(last_10['Volume'].mean())
        anti_pp_signature = vol > max_up_vol_10
    last_30_vols = sorted(df['Volume'].tail(30).values, reverse=True)
    vol_rank_top25 = vol >= last_30_vols[min(len(last_30_vols) - 1, len(last_30_vols) // 4)]

    # ── LAYER 4b: VWAP REJECTION ──
    if close >= vwap:
        return None  # close above VWAP = bulls still in control

    # ── LAYER 5: MOMENTUM + RS NEGATIVE ──
    if not (RSI_MIN <= rsi <= RSI_MAX):
        return None  # too oversold (bounce risk) or too strong
    move_5d = (close / float(df['Close'].iloc[-6]) - 1) * 100 if len(df) >= 6 else 0
    if move_5d >= 0:
        return None  # 5-day return not declining
    rs_today_diff, s_today_pct, n_today_pct = stock_vs_nifty_today(df, nifty_df)
    if rs_today_diff >= 0:
        return None  # stock outperformed Nifty — not weak enough
    last_252 = df.tail(252) if len(df) >= 252 else df
    low_52w = float(last_252['Low'].min())
    pct_from_52w_low = (close - low_52w) / low_52w * 100
    # Stock should be within striking distance of 52w low (room to fall)
    if pct_from_52w_low > PCT_FROM_52W_LOW_MAX * 4:  # allow up to 60% above 52w low
        pass  # don't reject — Stage 3 setups can be far from 52w low

    # ── LAYER 7: FALSE-SIGNAL KILLERS ──
    # 5+ consecutive red days?
    last_7 = df.tail(7)
    red_streak = 0
    for _, row in reversed(list(last_7.iterrows())):
        if float(row['Close']) < float(row['Open']):
            red_streak += 1
        else:
            break
    if red_streak >= RED_STREAK_MAX:
        return None  # too oversold, reversal due
    # Gap-down at open > 4%?
    gap_at_open_pct = (open_ - prev_close) / prev_close * 100 if prev_close > 0 else 0
    if gap_at_open_pct < GAP_DOWN_AT_OPEN_MAX_PCT:
        return None  # don't chase a gap-down
    # Doji
    body = abs(close - open_)
    if body / today_range < DOJI_BODY_RATIO_MAX:
        return None  # indecision
    # Hammer (long lower wick = bullish reversal candidate)
    lower_wick = min(open_, close) - low
    if body > 0 and lower_wick > HAMMER_LOWER_WICK_RATIO * body:
        return None
    # Friday afternoon shorts are risky (weekend gap-up)
    is_friday = datetime.now().weekday() == 4

    # Multi-touch SUPPORT check — don't short at the floor
    # Find horizontal support touched 3+ times in last 60d
    sub60 = df.tail(60)
    lows = sub60['Low'].values
    tolerance = close * 0.015  # 1.5% band
    near_support = False
    for i, lv in enumerate(lows):
        if abs(close - lv) < tolerance:
            # count touches within tolerance band
            touches = sum(1 for x in lows if abs(x - lv) < tolerance)
            if touches >= 3:
                near_support = True
                break
    if near_support:
        return None  # at multi-touch support = high bounce risk

    # ── LAYER 6: CATALYSTS (score bonuses) ──
    catalysts = []
    if pct_from_52w_low < 5:
        catalysts.append(("52w low breakdown", 8))
    elif pct_from_52w_low < 10:
        catalysts.append(("Near 52w low", 4))
    if anti_pp_signature:
        catalysts.append(("Anti-Pocket-Pivot volume signature", 10))
    if vol_mult >= 3.0:
        catalysts.append(("Heavy distribution day (3×+)", 5))
    # Failed-breakout reversal: made new 5-day high but closed near low
    high_5d_excl_today = float(df.tail(6)['High'].iloc[:-1].max())
    if high > high_5d_excl_today * 1.001 and close_pos < 0.20:
        catalysts.append(("Failed-breakout reversal", 7))
    # Death cross
    dma50_2d = float(df['DMA50'].iloc[-2]) if len(df) >= 2 and not pd.isna(df['DMA50'].iloc[-2]) else dma50
    dma200_2d = float(df['DMA200'].iloc[-2]) if len(df) >= 2 and not pd.isna(df['DMA200'].iloc[-2]) else dma200
    if dma50_2d > dma200_2d and dma50 < dma200:
        catalysts.append(("Death Cross today", 10))
    if rs_today_diff <= -3.0:
        catalysts.append(("Strong RS underperformance (-3%+)", 4))

    # ── INTRADAY TRADE PLAN ──
    entry = round(close, 2)
    # Stop: tighter of today's high or +2%
    today_high_stop = high * 1.005
    pct_stop = close * (1 + STOP_MAX_PCT / 100)
    sl = round(min(today_high_stop, pct_stop), 2)
    risk = sl - entry
    if risk <= 0:
        return None
    t1 = round(close * (1 + T1_PCT / 100), 2)
    t2 = round(close * (1 + T2_PCT / 100), 2)
    rr = (entry - t1) / risk if risk > 0 else 0

    return {
        "close": close, "open": open_, "high": high, "low": low, "volume": vol,
        "vwap": vwap, "vwap_distance_pct": (close - vwap) / vwap * 100,
        "dma50": dma50, "dma200": dma200, "ema20": ema20,
        "rsi": rsi, "atr": atr, "atr_pct": atr / close * 100,
        "adx": adx, "slope_50_pct": slope_50_pct,
        "is_stage_3": is_stage_3, "is_stage_4": is_stage_4,
        "today_range": today_range, "close_position": close_pos,
        "body_pct": body_pct, "range_vs_atr": today_range / atr,
        "vol_mult": vol_mult, "max_up_vol_10": max_up_vol_10,
        "anti_pp_signature": anti_pp_signature, "vol_rank_top25": vol_rank_top25,
        "low_52w": low_52w, "pct_from_52w_low": pct_from_52w_low,
        "avg_turnover_cr": avg_turnover_cr,
        "move_5d": move_5d,
        "s_today_pct": s_today_pct, "n_today_pct": n_today_pct,
        "rs_today_diff": rs_today_diff,
        "gap_at_open_pct": gap_at_open_pct,
        "red_streak": red_streak,
        "is_friday": is_friday,
        "catalysts": catalysts,
        "entry": entry, "sl": sl, "t1": t1, "t2": t2, "rr": rr,
        "risk_pct": (sl / entry - 1) * 100,
        "t1_pct": T1_PCT, "t2_pct": T2_PCT,
        "hard_close_time": HARD_CLOSE_TIME,
    }


def score_intraday_sell(p):
    """0-100 composite score."""
    s = 0
    # Volume signature (25)
    vs = 0
    vs += min(15, (p["vol_mult"] - VOL_MULT_MIN) * 6 + 5)
    if p["anti_pp_signature"]: vs += 6
    if p["vol_rank_top25"]: vs += 4
    s += min(vs, 25)
    # Close weakness (20) — close near low + red body
    cw = 0
    cw += min(10, (CLOSE_POS_MAX - p["close_position"]) * 100 + 5)
    cw += min(10, abs(p["body_pct"] - BODY_PCT_MAX) * 1.5 + 3)
    s += min(cw, 20)
    # RS NEGATIVE vs Nifty (15)
    rs = 0
    rs += min(10, abs(p["rs_today_diff"]) * 2 + 3) if p["rs_today_diff"] < 0 else 0
    rs += 5 if p["move_5d"] < -3 else (3 if p["move_5d"] < 0 else 0)
    s += min(rs, 15)
    # Trend breakdown (15)
    tb = 0
    tb += min(6, abs(p["slope_50_pct"]) * 4) if p["slope_50_pct"] <= 0 else 0
    tb += 5 if p["is_stage_4"] else (3 if p["is_stage_3"] else 0)
    tb += min(4, (p["adx"] - 20) * 0.3 + 1) if p["adx"] >= 20 else 0
    s += min(tb, 15)
    # Range expansion (10)
    s += min(10, (p["range_vs_atr"] - RANGE_VS_ATR_MIN) * 7 + 4)
    # Liquidity (5)
    s += min(5, p["avg_turnover_cr"] / 100)
    # Catalyst bonus (10)
    catalyst_total = sum(b for _, b in p["catalysts"])
    s += min(10, catalyst_total)
    # Friday penalty (weekend gap-up risk)
    if p["is_friday"]:
        s -= 5
    return round(max(0, min(100, s)), 1)


# ──────────────────────────────────────────────────────────────────────────────
# OUTPUT FORMATTING
# ──────────────────────────────────────────────────────────────────────────────

def format_message(picks, scan_time):
    now = scan_time.strftime("%d %b %Y, %I:%M %p IST")
    msg = f"📉 *QUANTEX INTRADAY SELL — {now}*\n"
    msg += f"#IntradayShort #FnO #ScoreCutoff{SCORE_THRESHOLD}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "_F&O stocks in Stage 3/4 with VWAP rejection + heavy distribution._\n"
    msg += f"_MIS short — HARD SQUARE-OFF BY {HARD_CLOSE_TIME} IST._\n\n"

    if not picks:
        msg += "⚠️ No intraday short signals fired today (score ≥65 threshold).\n"
        msg += "Either market is broadly bullish, or no F&O stock printed the\n"
        msg += "VWAP rejection + volume + RS-negative combo.\n"
        return msg

    msg += f"*📋 {len(picks)} signals (max {MAX_SIGNALS_PER_DAY}/day):*\n\n"

    for i, p in enumerate(picks, 1):
        d = p["pattern"]
        stage_label = "Stage 4 DOWN" if d["is_stage_4"] else "Stage 3 chop"
        msg += (
            f"*{i}. {p['symbol']}* — Score *{p['score']}/100*  ({stage_label})\n"
            f"   💸 SHORT @ ₹{d['entry']:.2f}  |  VWAP ₹{d['vwap']:.2f} ({d['vwap_distance_pct']:+.2f}%)\n"
            f"   🛑 SL: ₹{d['sl']:.2f} (+{d['risk_pct']:.1f}%)\n"
            f"   🎯 T1: ₹{d['t1']:.2f} ({d['t1_pct']:.1f}%) → cover 50%\n"
            f"   🎯 T2: ₹{d['t2']:.2f} ({d['t2_pct']:.1f}%) → cover rest\n"
            f"   ⚖️  R:R 1:{d['rr']:.1f}  |  ⏱  HARD COVER {d['hard_close_time']}\n"
            f"   📊 *Why it fires:*\n"
            f"     • Close in BOTTOM {d['close_position']*100:.0f}% of range, RED body {d['body_pct']:+.1f}%\n"
            f"     • Vol *{d['vol_mult']:.1f}× of 20d avg*"
            + (" (anti-Pocket-Pivot)" if d['anti_pp_signature'] else "") + "\n"
            f"     • Close BELOW VWAP by {abs(d['vwap_distance_pct']):.2f}%  ← intraday breakdown\n"
            f"     • RS NEGATIVE: stock {d['s_today_pct']:+.1f}% vs Nifty {d['n_today_pct']:+.1f}%\n"
            f"     • {stage_label}, RSI {d['rsi']:.0f}, ADX {d['adx']:.0f}\n"
        )
        if d["catalysts"]:
            cat_str = ", ".join(name for name, _ in d["catalysts"])
            msg += f"     • ⚡ Catalysts: {cat_str}\n"
        msg += f"     • Turnover ₹{d['avg_turnover_cr']:.0f} Cr/day\n\n"

    msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"⚠️ *MIS shorts MUST be squared off by {HARD_CLOSE_TIME} IST.*\n"
    msg += "_Verify F&O ban list before short. Tight stops mandatory._\n"
    msg += "🤖 _Quantex Intraday Sell Scanner — Kite live data_"
    return msg


# ──────────────────────────────────────────────────────────────────────────────
# TELEGRAM
# ──────────────────────────────────────────────────────────────────────────────

def send_to_chat(chat_id, message):
    if not (TELEGRAM_BOT_TOKEN and chat_id):
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": chat_id, "text": message,
            "parse_mode": "Markdown", "disable_web_page_preview": True,
        }, timeout=30)
        return r.status_code == 200
    except Exception:
        return False


def send_telegram(message):
    sent = 0
    for chat_id in [TELEGRAM_CHAT_ID, TELEGRAM_ADMIN_GROUPS]:
        if not chat_id:
            continue
        for gid in chat_id.split(","):
            gid = gid.strip()
            if gid and send_to_chat(gid, message):
                sent += 1
    return sent


# ──────────────────────────────────────────────────────────────────────────────
# RECOMMENDATIONS DUAL-WRITE
# ──────────────────────────────────────────────────────────────────────────────

def append_to_recommendations(picks, scan_time):
    scan_date = scan_time.strftime("%Y-%m-%d")
    record = {
        "scan_date": scan_date,
        "scan_time": scan_time.isoformat(),
        "scanner_type": SCANNER_TYPE,
        "total_qualified": len(picks),
        "top_10": [
            {
                "symbol": p["symbol"],
                "scanner_type": SCANNER_TYPE,
                "trade_direction": "SHORT",
                "score": p["score"],
                "entry": p["pattern"]["entry"],
                "sl": p["pattern"]["sl"],
                "target1": p["pattern"]["t1"],
                "target2": p["pattern"]["t2"],
                "hold_period": "Intraday MIS (square-off 15:15)",
                "cmp": p["pattern"]["close"],
                "signals": [
                    f"Close bottom {p['pattern']['close_position']*100:.0f}%",
                    f"Red {p['pattern']['body_pct']:.1f}%",
                    f"Vol {p['pattern']['vol_mult']:.1f}x",
                    f"VWAP {p['pattern']['vwap_distance_pct']:.1f}%",
                    f"RS {p['pattern']['rs_today_diff']:.1f}% vs Nifty",
                ] + [name for name, _ in p["pattern"]["catalysts"]],
            }
            for p in picks
        ],
    }
    existing = []
    if RECS_JSON.exists():
        try:
            existing = json.loads(RECS_JSON.read_text())
        except Exception:
            existing = []
    if not isinstance(existing, list):
        existing = []
    existing = [
        r for r in existing
        if not (r.get("scan_date") == scan_date and r.get("scanner_type") == SCANNER_TYPE)
    ]
    existing.append(record)
    RECS_JSON.write_text(json.dumps(existing, indent=2, default=str))


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}\n  QUANTEX INTRADAY SELL SCANNER — {datetime.now()}\n{'='*60}")
    print(f">> Score threshold: {SCORE_THRESHOLD}  |  Max signals: {MAX_SIGNALS_PER_DAY}")

    # Kite is mandatory — strict mode
    kite = _init_kite()
    if kite is None:
        if not _KITE_CLASS_AVAILABLE:
            print(f"❌ ABORT: KiteSession class unavailable (pro_scanner import failed)")
        elif not _kite_credentials_present():
            missing = [k for k, v in {
                "KITE_API_KEY": KITE_API_KEY, "KITE_API_SECRET": KITE_API_SECRET,
                "ZERODHA_USER_ID": ZERODHA_USER_ID, "ZERODHA_PASSWORD": ZERODHA_PASSWORD,
                "ZERODHA_TOTP_KEY": ZERODHA_TOTP_KEY,
            }.items() if not v]
            print(f"❌ ABORT: Kite credentials missing: {', '.join(missing)}")
        else:
            print(f"❌ ABORT: Kite login failed")
        print(f"   Intraday Sell scanner runs Kite-only. Set required secrets and retry.")
        sys.exit(1)
    print(f">> Data source: Kite Connect (historical + 5min intraday + live VWAP) ✅")

    # F&O universe
    fno_symbols = get_fno_universe(kite)
    print(f">> F&O universe: {len(fno_symbols)} stocks\n")
    if not fno_symbols:
        print("❌ ABORT: F&O universe empty (NFO instruments not loaded)")
        sys.exit(1)

    # Fetch Nifty for RS calc
    nifty_df = fetch_nifty()
    if nifty_df is None:
        print("!! WARNING: Couldn't fetch Nifty data — RS calc will be 0")
        nifty_df = pd.DataFrame()

    candidates = []
    start = time.time()
    for idx, sym in enumerate(fno_symbols, 1):
        if idx % 25 == 0:
            print(f"   ... {idx}/{len(fno_symbols)} ({time.time()-start:.0f}s)")
        df_daily = fetch_daily(sym)
        if df_daily is None:
            continue
        df_daily = annotate(df_daily)
        intraday = fetch_intraday_ohlc_and_vwap(sym)
        if intraday is None:
            continue
        p = detect_intraday_sell(sym, df_daily, intraday, nifty_df)
        if p is None:
            continue
        s = score_intraday_sell(p)
        if s >= SCORE_THRESHOLD:
            candidates.append({"symbol": sym, "score": s, "pattern": p})

    candidates.sort(key=lambda x: x["score"], reverse=True)
    picks = candidates[:MAX_SIGNALS_PER_DAY]

    elapsed = time.time() - start
    print(f"\n>> Scan complete in {elapsed:.0f}s — {len(candidates)} qualified, top {len(picks)} fired\n")
    for p in picks:
        d = p["pattern"]
        stage = "S4" if d["is_stage_4"] else "S3"
        print(f"   {p['symbol']:13s} {stage}  score {p['score']:5.1f}  "
              f"close-pos {d['close_position']*100:.0f}%  vol {d['vol_mult']:.1f}×  "
              f"VWAP {d['vwap_distance_pct']:+.1f}%  RS {d['rs_today_diff']:+.1f}%")

    scan_time = datetime.now()

    PICKS_JSON.write_text(json.dumps([
        {"symbol": p["symbol"], "score": p["score"],
         "scan_time": scan_time.isoformat(),
         **p["pattern"]} for p in picks
    ], indent=2, default=str))
    print(f"\n>> Saved {PICKS_JSON}")

    if picks:
        append_to_recommendations(picks, scan_time)
        print(f">> Appended to {RECS_JSON} as scanner_type={SCANNER_TYPE}")

    msg = format_message(picks, scan_time)
    print(f"\n--- TELEGRAM PREVIEW ---\n{msg}\n")
    if TELEGRAM_BOT_TOKEN:
        send_telegram(msg)


if __name__ == "__main__":
    main()
