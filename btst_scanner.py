#!/usr/bin/env python3
"""
Quantex BTST (Buy Today, Sell Tomorrow) Scanner
================================================

Runs at 3:20 PM IST Mon-Fri. Surfaces high-conviction overnight setups where:
    1. Stock is in confirmed Stage 2 uptrend
    2. Today's close is in top 10% of day's range (institutions buying into close)
    3. Today's volume is ≥1.5× of 20-day average
    4. Today's close > yesterday's high (broke prior swing structure)
    5. Stock outperformed Nifty today (relative strength positive)
    6. No false-signal patterns (5+ green days streak, doji, exhaustion gap, etc.)

8-layer filter design:
    L1: Universe filters (liquidity, price band)
    L2: Trend health (Stage 2, slope, ADX, EMA20)
    L3: Today's price action (close pos, body, range, vs yesterday high)
    L4: Volume signature (vs 20d avg, vs MAX-DOWN-10 — pocket pivot lite)
    L5: Momentum + Relative Strength vs Nifty
    L6: Catalyst layer (52w high break, Pocket Pivot fires, sector strength) — score booster
    L7: False-signal killers (5+ green streak, gap > 4%, doji, etc.)
    L8: Composite scoring 0-100 — fire only at ≥65

Output:
    • Max 5 signals/day, ranked by score
    • Admin + personal Telegram only (NOT signal group)
    • Position sizing on 5% of portfolio assumption
    • R-multiple targets + Realistic T1/T2 (ATR + resistance + round-#)
    • Dual-writes to recommendations.json as scanner_type=btst for tracker
"""

import os
import sys
import json
import time
import io
import contextlib
import logging
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import numpy as np

# Silence yfinance noise (delisted/404 warnings flooding GitHub Actions logs)
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

import yfinance as yf
import ta
import requests


# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_ADMIN_GROUPS = os.environ.get("TELEGRAM_ADMIN_GROUPS", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

MAX_SIGNALS_PER_DAY = 5
SCORE_THRESHOLD = 65          # Fire signals at score ≥65

LOG_DIR = Path(__file__).parent / "quantex_logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
PICKS_JSON = LOG_DIR / "btst_picks.json"
RECS_JSON = LOG_DIR / "recommendations.json"

SCANNER_TYPE = "btst"

# Layer 1 — Universe filters
TURNOVER_MIN_CR = 30.0        # ₹30 Cr/day minimum
PRICE_MIN = 100.0
PRICE_MAX = 15000.0

# Layer 2 — Trend health
SLOPE_DAYS = 5
ADX_MIN = 20

# Layer 3 — Today's price action
CLOSE_POS_MIN = 0.90          # Top 10% of day's range
BODY_PCT_MIN = 1.5            # Green body ≥ 1.5%
RANGE_VS_ATR_MIN = 1.0        # Range ≥ ATR

# Layer 4 — Volume signature
VOL_MULT_MIN = 1.5            # vs 20d avg
DOWN_VOL_LOOKBACK = 10

# Layer 5 — Momentum + RS
RSI_MIN = 50
RSI_MAX = 70
PCT_FROM_52W_HIGH_MAX = 25.0

# Layer 7 — False-signal killers
GREEN_STREAK_MAX = 5          # Reject if 5+ consecutive green days
GAP_UP_AT_OPEN_MAX_PCT = 4.0  # Reject if gapped up >4% then bought more (exhaustion)
DOJI_BODY_RATIO_MAX = 0.10    # Body ≤ 10% of range = doji
SHOOTING_STAR_WICK_RATIO = 2.0  # Upper wick ≥ 2× body = shooting star

# Trade plan
STOP_MAX_PCT = 2.0            # BTST stop: tight, max -2%
T1_PCT = 2.5                  # +2.5% — typical BTST profit
T2_PCT = 5.0                  # +5% — runner extension


# ──────────────────────────────────────────────────────────────────────────────
# UNIVERSE — merge pro_scanner.STOCK_UNIVERSE + nifty500_symbols.json
# ──────────────────────────────────────────────────────────────────────────────

try:
    from pro_scanner import STOCK_UNIVERSE  # type: ignore
except Exception:
    print("!! Couldn't import STOCK_UNIVERSE; using empty list")
    STOCK_UNIVERSE = []

try:
    n500_path = LOG_DIR / "nifty500_symbols.json"
    if n500_path.exists():
        with open(n500_path) as f:
            _n500 = json.load(f)
        UNIVERSE = sorted(set(STOCK_UNIVERSE) | set(_n500))
    else:
        UNIVERSE = STOCK_UNIVERSE
except Exception:
    UNIVERSE = STOCK_UNIVERSE

# Smart-target helpers from pro_scanner
try:
    from pro_scanner import (  # type: ignore
        find_resistance_zones,
        round_number_adjust,
        atr_projected_move,
    )
    _SMART_TARGETS_AVAILABLE = True
except Exception as e:
    print(f"!! Smart-target helpers unavailable ({e}); Realistic T1/T2 fall back to ATR-only")
    _SMART_TARGETS_AVAILABLE = False


# ──────────────────────────────────────────────────────────────────────────────
# DATA HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def fetch_daily(symbol, period="1y"):
    try:
        with _silence_stdio():
            df = yf.Ticker(f"{symbol}.NS").history(period=period, interval="1d")
        if df is None or df.empty or len(df) < 200:
            return None
        df.columns = [c.title() for c in df.columns]
        return df.dropna(subset=["Close"])
    except Exception:
        return None


def fetch_nifty(period="1y"):
    """Nifty 50 index for relative strength calculation."""
    try:
        with _silence_stdio():
            df = yf.Ticker("^NSEI").history(period=period, interval="1d")
        if df is None or df.empty or len(df) < 20:
            return None
        df.columns = [c.title() for c in df.columns]
        return df.dropna(subset=["Close"])
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
        out["ADX"] = 25  # neutral fallback
    out["Vol20"] = out["Volume"].rolling(20).mean()
    out["Range"] = out["High"] - out["Low"]
    return out


# ──────────────────────────────────────────────────────────────────────────────
# REALISTIC TARGETS (same math as pro_scanner / pocket pivot)
# ──────────────────────────────────────────────────────────────────────────────

def compute_realistic_targets(df, cmp, atr_val, est_days_t1, est_days_t2):
    """BTST timeframe → est_days_t1 typically 1, est_days_t2 typically 3."""
    if _SMART_TARGETS_AVAILABLE:
        atr_cap_t1 = atr_projected_move(df, cmp, holding_days=max(1, est_days_t1))
        atr_cap_t2 = atr_projected_move(df, cmp, holding_days=max(3, est_days_t2))
    else:
        atr_cap_t1 = round(cmp + atr_val * (max(1, est_days_t1) ** 0.5) * 1.2, 2)
        atr_cap_t2 = round(cmp + atr_val * (max(3, est_days_t2) ** 0.5) * 1.2, 2)

    resistances = []
    if _SMART_TARGETS_AVAILABLE:
        try:
            resistances = find_resistance_zones(df, cmp, lookback_days=60) or []
        except Exception:
            resistances = []

    rt1_basis = "ATR cap (1-day)"
    if resistances:
        nearest = resistances[0]
        if nearest["price"] <= atr_cap_t1:
            rt1 = nearest["price"]
            rt1_basis = f"Resistance ₹{nearest['price']:.0f} [{nearest['touches']}× touches]"
        else:
            rt1 = atr_cap_t1
            rt1_basis = f"ATR cap (1-day) — resistance ₹{nearest['price']:.0f} too far"
    else:
        rt1 = atr_cap_t1

    if _SMART_TARGETS_AVAILABLE:
        rt1_adj = round_number_adjust(rt1, cmp)
        if rt1_adj != rt1:
            rt1_basis += " + round-# adj"
        rt1 = rt1_adj

    atr_cap_t2_adj = atr_cap_t2 * 1.15
    rt2_basis = "ATR cap (3-day ×1.15)"
    higher = [r for r in resistances if r["price"] > rt1 * 1.005]
    if higher:
        nxt = higher[0]
        if nxt["price"] <= atr_cap_t2_adj:
            rt2 = nxt["price"]
            rt2_basis = f"Resistance ₹{nxt['price']:.0f} [{nxt['touches']}× touches]"
        else:
            rt2 = atr_cap_t2_adj
            rt2_basis = f"ATR cap (×1.15) — next R ₹{nxt['price']:.0f} too far"
    else:
        rt2 = atr_cap_t2_adj

    if _SMART_TARGETS_AVAILABLE:
        rt2_adj = round_number_adjust(rt2, cmp)
        if rt2_adj != rt2:
            rt2_basis += " + round-# adj"
        rt2 = rt2_adj

    if rt2 <= rt1 * 1.015:
        rt2 = round(rt1 * 1.025, 2)
        rt2_basis = "Forced ≥2.5% above realistic T1"

    return {
        "realistic_t1": round(rt1, 2),
        "realistic_t2": round(rt2, 2),
        "realistic_t1_basis": rt1_basis,
        "realistic_t2_basis": rt2_basis,
    }


# ──────────────────────────────────────────────────────────────────────────────
# RELATIVE STRENGTH HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def stock_vs_nifty_today(df, nifty_df):
    """Return stock's outperformance % vs Nifty today (positive = stock won)."""
    try:
        s_today = (float(df['Close'].iloc[-1]) / float(df['Close'].iloc[-2]) - 1) * 100
        n_today = (float(nifty_df['Close'].iloc[-1]) / float(nifty_df['Close'].iloc[-2]) - 1) * 100
        return s_today - n_today, s_today, n_today
    except Exception:
        return 0, 0, 0


def rs_5d(df, nifty_df):
    """5-day return outperformance."""
    try:
        s5 = (float(df['Close'].iloc[-1]) / float(df['Close'].iloc[-6]) - 1) * 100
        n5 = (float(nifty_df['Close'].iloc[-1]) / float(nifty_df['Close'].iloc[-6]) - 1) * 100
        return s5 - n5
    except Exception:
        return 0


# ──────────────────────────────────────────────────────────────────────────────
# PATTERN DETECTION — BTST
# ──────────────────────────────────────────────────────────────────────────────

def detect_btst(df, nifty_df):
    """Return dict if today's bar qualifies for a BTST entry, else None."""
    if len(df) < 200:
        return None

    last = df.iloc[-1]
    prev = df.iloc[-2]
    close = float(last["Close"])
    open_ = float(last["Open"])
    high = float(last["High"])
    low = float(last["Low"])
    vol = float(last["Volume"])
    prev_close = float(prev["Close"])
    prev_high = float(prev["High"])

    # ── LAYER 1: UNIVERSE FILTERS ──
    if not (PRICE_MIN <= close <= PRICE_MAX):
        return None
    avg_turnover_cr = float((df["Close"].tail(20) * df["Volume"].tail(20)).mean()) / 1e7
    if avg_turnover_cr < TURNOVER_MIN_CR:
        return None

    # ── LAYER 2: TREND HEALTH ──
    dma50 = float(last["DMA50"]) if not pd.isna(last["DMA50"]) else None
    dma200 = float(last["DMA200"]) if not pd.isna(last["DMA200"]) else None
    ema20 = float(last["EMA20"]) if not pd.isna(last["EMA20"]) else None
    rsi = float(last["RSI"]) if not pd.isna(last["RSI"]) else None
    atr = float(last["ATR14"]) if not pd.isna(last["ATR14"]) else None
    vol20 = float(last["Vol20"]) if not pd.isna(last["Vol20"]) else None
    adx = float(last["ADX"]) if not pd.isna(last["ADX"]) else 20

    if not all([dma50, dma200, ema20, rsi, atr, vol20]):
        return None
    if not (close > dma50 > dma200):
        return None
    dma50_prev = float(df["DMA50"].iloc[-1 - SLOPE_DAYS]) if len(df) > SLOPE_DAYS else dma50
    slope_50_pct = (dma50 - dma50_prev) / dma50_prev * 100 if dma50_prev > 0 else 0
    if slope_50_pct <= 0:
        return None
    if close < ema20:
        return None
    if adx < ADX_MIN:
        return None

    # ── LAYER 3: TODAY'S PRICE ACTION ──
    today_range = high - low
    if today_range <= 0:
        return None
    close_pos = (close - low) / today_range
    if close_pos < CLOSE_POS_MIN:
        return None
    body_pct = (close - open_) / open_ * 100 if open_ > 0 else 0
    if body_pct < BODY_PCT_MIN:
        return None
    if today_range / atr < RANGE_VS_ATR_MIN:
        return None
    if close <= prev_high:
        return None

    # New 5-day or 20-day high (bonus structural confirmation)
    high_5d = float(df.tail(6)["High"].iloc[:-1].max())  # excludes today
    high_20d = float(df.tail(21)["High"].iloc[:-1].max())
    new_5d_high = close > high_5d
    new_20d_high = close > high_20d

    # ── LAYER 4: VOLUME SIGNATURE ──
    vol_mult = vol / vol20 if vol20 > 0 else 0
    if vol_mult < VOL_MULT_MIN:
        return None
    # Pocket pivot lite: today UP-vol > max DOWN-vol of prior 10 days
    last_10 = df.iloc[-(DOWN_VOL_LOOKBACK + 1):-1]
    down_vols = last_10.loc[last_10["Close"] < last_10["Open"], "Volume"]
    if len(down_vols) > 0:
        max_down_vol_10 = float(down_vols.max())
        pp_signature = vol > max_down_vol_10
    else:
        max_down_vol_10 = float(last_10["Volume"].mean())
        pp_signature = vol > max_down_vol_10
    # Today's volume in top 25% of last 30 days
    last_30_vols = sorted(df["Volume"].tail(30).values, reverse=True)
    vol_rank_top25 = vol >= last_30_vols[min(len(last_30_vols) - 1, len(last_30_vols) // 4)]

    # ── LAYER 5: MOMENTUM + RELATIVE STRENGTH ──
    if not (RSI_MIN <= rsi <= RSI_MAX):
        return None
    move_5d = (close / float(df["Close"].iloc[-6]) - 1) * 100 if len(df) >= 6 else 0
    if move_5d <= 0:
        return None
    rs_today_diff, s_today_pct, n_today_pct = stock_vs_nifty_today(df, nifty_df)
    if rs_today_diff <= 0:
        return None
    rs_5d_diff = rs_5d(df, nifty_df)
    last_252 = df.tail(252) if len(df) >= 252 else df
    high_52w = float(last_252["High"].max())
    pct_from_52w = (high_52w - close) / high_52w * 100
    if pct_from_52w > PCT_FROM_52W_HIGH_MAX:
        return None

    # ── LAYER 7: FALSE-SIGNAL KILLERS ──
    # 5+ consecutive green days?
    last_7 = df.tail(7)
    green_streak = 0
    for _, row in reversed(list(last_7.iterrows())):
        if float(row["Close"]) > float(row["Open"]):
            green_streak += 1
        else:
            break
    if green_streak >= GREEN_STREAK_MAX:
        return None
    # Gap-up at open + bought more (exhaustion gap risk)
    gap_at_open_pct = (open_ - prev_close) / prev_close * 100 if prev_close > 0 else 0
    if gap_at_open_pct > GAP_UP_AT_OPEN_MAX_PCT:
        return None
    # Doji
    body = abs(close - open_)
    if body / today_range < DOJI_BODY_RATIO_MAX:
        return None
    # Shooting star (long upper wick, indecision)
    upper_wick = high - max(close, open_)
    if body > 0 and upper_wick > SHOOTING_STAR_WICK_RATIO * body:
        return None
    # Friday signal flag (don't reject, just flag for scoring)
    is_friday = datetime.now().weekday() == 4

    # ── LAYER 6: CATALYST DETECTION (score bonuses) ──
    catalysts = []
    if pct_from_52w < 1.0:
        catalysts.append(("52w high breakout", 7))
    elif pct_from_52w < 3.0:
        catalysts.append(("Near 52w high (pre-breakout zone)", 4))
    if pp_signature and close > open_:
        catalysts.append(("Pocket Pivot volume signature", 10))
    if new_20d_high:
        catalysts.append(("New 20-day high", 5))
    elif new_5d_high:
        catalysts.append(("New 5-day high", 2))
    if vol_mult >= 3.0:
        catalysts.append(("Heavy volume surge (3×+)", 5))
    if rs_5d_diff >= 5.0:
        catalysts.append(("Strong 5-day RS vs Nifty", 4))

    # ── TRADE PLAN ──
    entry = round(close, 2)
    # Stop: tighter of today's low (with buffer) or -2%
    today_low_stop = low * 0.995
    pct_stop = close * (1 - STOP_MAX_PCT / 100)
    sl = round(max(today_low_stop, pct_stop), 2)
    risk = entry - sl
    if risk <= 0:
        return None
    t1 = round(close * (1 + T1_PCT / 100), 2)
    t2 = round(close * (1 + T2_PCT / 100), 2)
    rr = (t1 - entry) / risk if risk > 0 else 0

    # BTST hold ≈ 1 session for T1, 2-3 for T2
    est_days_t1 = 1
    est_days_t2 = 3
    realistic = compute_realistic_targets(df, close, atr, est_days_t1, est_days_t2)

    return {
        "close": close, "open": open_, "high": high, "low": low, "volume": vol,
        "dma50": dma50, "dma200": dma200, "ema20": ema20,
        "rsi": rsi, "atr": atr, "atr_pct": atr / close * 100,
        "adx": adx, "slope_50_pct": slope_50_pct,
        "today_range": today_range, "close_position": close_pos,
        "body_pct": body_pct, "range_vs_atr": today_range / atr,
        "vol_mult": vol_mult, "max_down_vol_10": max_down_vol_10,
        "pp_signature": pp_signature, "vol_rank_top25": vol_rank_top25,
        "new_5d_high": new_5d_high, "new_20d_high": new_20d_high,
        "high_52w": high_52w, "pct_from_52w": pct_from_52w,
        "avg_turnover_cr": avg_turnover_cr,
        "move_5d": move_5d,
        "s_today_pct": s_today_pct, "n_today_pct": n_today_pct,
        "rs_today_diff": rs_today_diff, "rs_5d_diff": rs_5d_diff,
        "gap_at_open_pct": gap_at_open_pct,
        "green_streak": green_streak,
        "is_friday": is_friday,
        "catalysts": catalysts,
        "entry": entry, "sl": sl, "t1": t1, "t2": t2, "rr": rr,
        "risk_pct": (1 - sl / entry) * 100,
        "t1_pct": T1_PCT, "t2_pct": T2_PCT,
        "est_days_t1": est_days_t1, "est_days_t2": est_days_t2,
        **realistic,
    }


def score_btst(p):
    """0-100 composite score per the design spec."""
    s = 0
    # Volume signature (25)
    vs = 0
    vs += min(15, (p["vol_mult"] - VOL_MULT_MIN) * 6 + 5)
    if p["pp_signature"]: vs += 6
    if p["vol_rank_top25"]: vs += 4
    s += min(vs, 25)
    # Close strength (20)
    cs = 0
    cs += min(10, (p["close_position"] - CLOSE_POS_MIN) * 100 + 5)
    cs += min(7, (p["body_pct"] - BODY_PCT_MIN) * 1.5 + 3)
    cs += 3 if p["close"] > p["high_52w"] * 0.99 else 0
    s += min(cs, 20)
    # Last-hour momentum proxy (15) — using close-in-top-X% as best available daily proxy
    lh = 0
    lh += min(10, (p["close_position"] - 0.85) * 50) if p["close_position"] >= 0.85 else 0
    lh += min(5, (p["range_vs_atr"] - 1.0) * 5 + 2) if p["range_vs_atr"] >= 1.0 else 0
    s += min(lh, 15)
    # RS vs Nifty (15)
    rs = 0
    rs += min(8, p["rs_today_diff"] * 2 + 3) if p["rs_today_diff"] > 0 else 0
    rs += min(7, p["rs_5d_diff"] * 0.7 + 3) if p["rs_5d_diff"] > 0 else 0
    s += min(rs, 15)
    # Trend health (10)
    th = 0
    th += min(4, p["slope_50_pct"] * 2)
    th += min(3, (p["adx"] - 20) * 0.2 + 1) if p["adx"] >= 20 else 0
    th += 3 if p["close"] > p["dma50"] * 1.05 else 0
    s += min(th, 10)
    # Liquidity (5)
    s += min(5, p["avg_turnover_cr"] / 50)
    # Catalyst bonus (10)
    catalyst_total = sum(b for _, b in p["catalysts"])
    s += min(10, catalyst_total)
    # Friday penalty (weekend gap-down risk)
    if p["is_friday"]:
        s -= 5
    return round(max(0, min(100, s)), 1)


# ──────────────────────────────────────────────────────────────────────────────
# OUTPUT FORMATTING
# ──────────────────────────────────────────────────────────────────────────────

def format_message(picks, scan_time):
    now = scan_time.strftime("%d %b %Y, %I:%M %p IST")
    msg = f"🌅 *QUANTEX BTST SCAN — {now}*\n"
    msg += f"#BTST #OvernightSetups #ScoreCutoff{SCORE_THRESHOLD}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "_Stage 2 stocks with strong-close + heavy volume + RS vs Nifty,_\n"
    msg += "_no exhaustion signals. Hold overnight, exit by tomorrow 11 AM._\n\n"

    if not picks:
        msg += "⚠️ No BTST signals fired today (score ≥65 threshold).\n"
        msg += "Either no Stage 2 stock printed the volume+close+RS combo, or\n"
        msg += "all candidates tripped a false-signal filter.\n"
        return msg

    msg += f"*📋 {len(picks)} signals (max {MAX_SIGNALS_PER_DAY}/day, score ≥{SCORE_THRESHOLD}):*\n\n"

    for i, p in enumerate(picks, 1):
        d = p["pattern"]
        rt1 = d.get("realistic_t1"); rt2 = d.get("realistic_t2")
        msg += (
            f"*{i}. {p['symbol']}* — Score *{p['score']}/100*\n"
            f"   💰 Entry: ₹{d['entry']:.2f}\n"
            f"   🛑 SL: ₹{d['sl']:.2f} (-{d['risk_pct']:.1f}%)\n"
            f"   🎯 T1: ₹{d['t1']:.2f} (+{d['t1_pct']:.1f}%) → exit half\n"
            f"   🎯 T2: ₹{d['t2']:.2f} (+{d['t2_pct']:.1f}%) → exit by 11 AM\n"
            f"   ⚖️  R:R 1:{d['rr']:.1f}\n"
        )
        if rt1 is not None:
            msg += (
                f"   🧠 Realistic T1: ₹{rt1:.2f} (+{(rt1/d['entry']-1)*100:.1f}%) _{d.get('realistic_t1_basis','')}_\n"
                f"   🧠 Realistic T2: ₹{rt2:.2f} (+{(rt2/d['entry']-1)*100:.1f}%) _{d.get('realistic_t2_basis','')}_\n"
            )
        msg += (
            f"   📊 *Why it fires:*\n"
            f"     • Close in top {d['close_position']*100:.0f}% of range, green +{d['body_pct']:.1f}%\n"
            f"     • Vol *{d['vol_mult']:.1f}× of 20d avg*"
            + (f" (pocket-pivot signature)" if d['pp_signature'] else "")
            + (", new 20d high" if d['new_20d_high'] else (", new 5d high" if d['new_5d_high'] else "")) + "\n"
            f"     • RS vs Nifty: stock +{d['s_today_pct']:.1f}% vs Nifty {d['n_today_pct']:+.1f}%\n"
            f"     • Stage 2 UP, RSI {d['rsi']:.0f}, ADX {d['adx']:.0f}, "
            f"{d['pct_from_52w']:.1f}% off 52w high\n"
        )
        if d["catalysts"]:
            cat_str = ", ".join(name for name, _ in d["catalysts"])
            msg += f"     • ⚡ Catalysts: {cat_str}\n"
        msg += f"     • Avg turnover ₹{d['avg_turnover_cr']:.0f} Cr/day\n\n"

    msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "⚠️ *Risk discipline:* exit by tomorrow 11 AM IST regardless.\n"
    msg += "_Verify F&O ban list + results calendar before entry._\n"
    msg += "🤖 _Quantex BTST Scanner — overnight momentum strategy_"
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
    """Admin + personal only — NOT signal group until validated."""
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
# RECOMMENDATIONS DUAL-WRITE (for performance tracker)
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
                "score": p["score"],
                "entry": p["pattern"]["entry"],
                "sl": p["pattern"]["sl"],
                "target1": p["pattern"]["t1"],
                "target2": p["pattern"]["t2"],
                "hold_period": "1 session (BTST)",
                "cmp": p["pattern"]["close"],
                "signals": [
                    f"Close top {p['pattern']['close_position']*100:.0f}%",
                    f"Green +{p['pattern']['body_pct']:.1f}%",
                    f"Vol {p['pattern']['vol_mult']:.1f}x",
                    f"RS +{p['pattern']['rs_today_diff']:.1f}% vs Nifty",
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
    print(f"\n{'='*60}\n  QUANTEX BTST SCANNER — {datetime.now()}\n{'='*60}")
    print(f">> Universe: {len(UNIVERSE)} symbols")
    print(f">> Score threshold: {SCORE_THRESHOLD}  |  Max signals: {MAX_SIGNALS_PER_DAY}\n")

    nifty_df = fetch_nifty()
    if nifty_df is None:
        print("!! WARNING: Couldn't fetch Nifty data — RS calc will be 0")
        nifty_df = pd.DataFrame()

    candidates = []
    start = time.time()
    for idx, sym in enumerate(UNIVERSE, 1):
        if idx % 50 == 0:
            print(f"   ... {idx}/{len(UNIVERSE)} ({time.time()-start:.0f}s)")
        df = fetch_daily(sym)
        if df is None:
            continue
        df = annotate(df)
        p = detect_btst(df, nifty_df)
        if p is None:
            continue
        s = score_btst(p)
        if s >= SCORE_THRESHOLD:
            candidates.append({"symbol": sym, "score": s, "pattern": p})

    candidates.sort(key=lambda x: x["score"], reverse=True)
    picks = candidates[:MAX_SIGNALS_PER_DAY]

    elapsed = time.time() - start
    print(f"\n>> Scan complete in {elapsed:.0f}s — {len(candidates)} qualified, top {len(picks)} fired\n")
    for p in picks:
        d = p["pattern"]
        print(f"   {p['symbol']:13s} score {p['score']:5.1f}  "
              f"close-pos {d['close_position']*100:.0f}%  vol {d['vol_mult']:.1f}×  "
              f"RS +{d['rs_today_diff']:.1f}%  entry ₹{d['entry']:8.2f}  R:R 1:{d['rr']:.1f}")

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
