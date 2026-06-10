#!/usr/bin/env python3
"""
Quantex Volume Expansion Breakout Scanner
==========================================

Catches stocks emerging FROM Stage 1 base INTO Stage 2 uptrend on heavy
institutional volume. This is the pattern that Pocket Pivot, BTST, and
Tight Coil miss BY DESIGN — those scanners require confirmed Stage 2
(close > 50-DMA > 200-DMA). This scanner catches the *transition itself*.

Pattern signature:
    • Stock above 200-DMA but 50-DMA still below 200-DMA (i.e. post-base,
      pre-Golden-Cross)
    • Today's body ≥ +5% (powerful green candle)
    • Today's volume ≥ 3× of 20-day average
    • Today's range ≥ 1.5× ATR (real volatility expansion)
    • Close in upper half of day's range (no late-day fade)
    • Was previously beaten down (52w drawdown ≥ 25%) — true recovery, not
      a momentum extension

Why this matters:
    • Same-day +10% breakout with 6×+ volume from a long base typically
      kicks off a multi-month Stage 2 advance
    • Catches the *first* entry into a trend, not the 3rd retest
    • Complements Pocket Pivot (which buys later in the same trend)

Reference: Stockbee "Vol Expansion Breakout" + IBD "Power Earnings Gap"
when paired with a base.
"""

import os
import sys
import json
import time
import io
import contextlib
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

# Silence yfinance noise
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

LOG_DIR = Path(__file__).parent / "quantex_logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
PICKS_JSON = LOG_DIR / "vol_expansion_picks.json"
RECS_JSON = LOG_DIR / "recommendations.json"

SCANNER_TYPE = "vol_expansion"
MAX_SIGNALS_PER_DAY = 7
SCORE_THRESHOLD = 65

# Pattern thresholds
BODY_PCT_MIN = 5.0            # ≥+5% green body
VOL_MULT_MIN = 3.0            # ≥3× 20-day avg
RANGE_VS_ATR_MIN = 1.5        # ≥1.5× ATR (real expansion)
CLOSE_POS_MIN = 0.5           # Close in upper half (looser than BTST's 90%)
DRAWDOWN_52W_MIN = 25.0       # ≥25% off 52w high (was beaten down)
PCT_FROM_52W_HIGH_MAX = 50.0  # but not >50% off (don't catch ongoing waterfall)
TURNOVER_MIN_CR = 30.0
STOP_MAX_PCT = 10.0           # wider stop for transition entries (volatility)


# ──────────────────────────────────────────────────────────────────────────────
# UNIVERSE + SMART-TARGET HELPERS
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

try:
    from pro_scanner import (  # type: ignore
        find_resistance_zones,
        round_number_adjust,
        atr_projected_move,
    )
    _SMART_TARGETS_AVAILABLE = True
except Exception as e:
    print(f"!! Smart-target helpers unavailable ({e}); fallback to ATR-only")
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
# REALISTIC TARGETS (same math as pro_scanner / pocket pivot)
# ──────────────────────────────────────────────────────────────────────────────

def compute_realistic_targets(df, cmp, atr_val, est_days_t1, est_days_t2):
    if _SMART_TARGETS_AVAILABLE:
        atr_cap_t1 = atr_projected_move(df, cmp, holding_days=max(3, est_days_t1))
        atr_cap_t2 = atr_projected_move(df, cmp, holding_days=max(10, est_days_t2))
    else:
        atr_cap_t1 = round(cmp + atr_val * (max(3, est_days_t1) ** 0.5) * 1.2, 2)
        atr_cap_t2 = round(cmp + atr_val * (max(10, est_days_t2) ** 0.5) * 1.2, 2)

    resistances = []
    if _SMART_TARGETS_AVAILABLE:
        try:
            resistances = find_resistance_zones(df, cmp, lookback_days=180) or []
        except Exception:
            resistances = []

    rt1_basis = "ATR cap"
    if resistances:
        nearest = resistances[0]
        if nearest["price"] <= atr_cap_t1:
            rt1 = nearest["price"]
            rt1_basis = f"Resistance ₹{nearest['price']:.0f} [{nearest['touches']}× touches]"
        else:
            rt1 = atr_cap_t1
            rt1_basis = f"ATR cap — nearest R ₹{nearest['price']:.0f} too far"
    else:
        rt1 = atr_cap_t1

    if _SMART_TARGETS_AVAILABLE:
        rt1_adj = round_number_adjust(rt1, cmp)
        if rt1_adj != rt1:
            rt1_basis += " + round-# adj"
        rt1 = rt1_adj

    atr_cap_t2_adj = atr_cap_t2 * 1.15
    rt2_basis = "ATR cap (×1.15)"
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

    if rt2 <= rt1 * 1.03:
        rt2 = round(rt1 * 1.05, 2)
        rt2_basis = "Forced ≥5% above realistic T1"

    return {
        "realistic_t1": round(rt1, 2),
        "realistic_t2": round(rt2, 2),
        "realistic_t1_basis": rt1_basis,
        "realistic_t2_basis": rt2_basis,
    }


# ──────────────────────────────────────────────────────────────────────────────
# PATTERN DETECTION
# ──────────────────────────────────────────────────────────────────────────────

def detect_vol_expansion(df):
    """Return dict if today's bar qualifies for a Vol Expansion breakout."""
    if len(df) < 200:
        return None

    last = df.iloc[-1]
    close = float(last["Close"])
    open_ = float(last["Open"])
    high = float(last["High"])
    low = float(last["Low"])
    vol = float(last["Volume"])
    prev_close = float(df["Close"].iloc[-2])

    dma50 = float(last["DMA50"]) if not pd.isna(last["DMA50"]) else None
    dma200 = float(last["DMA200"]) if not pd.isna(last["DMA200"]) else None
    rsi = float(last["RSI"]) if not pd.isna(last["RSI"]) else None
    atr = float(last["ATR14"]) if not pd.isna(last["ATR14"]) else None
    adx = float(last["ADX"]) if not pd.isna(last["ADX"]) else 0
    vol20 = float(last["Vol20"]) if not pd.isna(last["Vol20"]) else None
    ema20 = float(last["EMA20"]) if not pd.isna(last["EMA20"]) else None

    if not all([dma50, dma200, rsi, atr, vol20, ema20]):
        return None

    # ── 1. STAGE 1→2 TRANSITION (the key filter) ──
    # Must be ABOVE 200-DMA (no falling-knife buys)
    if close < dma200:
        return None
    # 50-DMA may still be below 200-DMA (Stage 1 → 2 transition) OR
    # may be just crossing above (early Stage 2) — both valid
    # We want STILL EMERGING, not deep in Stage 2 (which Pocket Pivot catches)
    # So we accept either Golden Cross-pending OR just-crossed (within 10% of cross)
    if dma50 > dma200 * 1.10:
        return None  # already deep in Stage 2 → Pocket Pivot territory
    # Must be above EMA20 (short-term momentum aligned)
    if close < ema20:
        return None

    # ── 2. POWERFUL GREEN BODY ──
    body_pct = (close - open_) / open_ * 100 if open_ > 0 else 0
    if body_pct < BODY_PCT_MIN:
        return None

    # ── 3. VOLUME EXPANSION (the signature) ──
    vol_mult = vol / vol20 if vol20 > 0 else 0
    if vol_mult < VOL_MULT_MIN:
        return None

    # ── 4. RANGE EXPANSION ──
    today_range = high - low
    if today_range <= 0:
        return None
    range_vs_atr = today_range / atr if atr > 0 else 0
    if range_vs_atr < RANGE_VS_ATR_MIN:
        return None

    # ── 5. CLOSING STRENGTH (no late-day fade) ──
    close_pos = (close - low) / today_range
    if close_pos < CLOSE_POS_MIN:
        return None
    if close <= open_:
        return None  # must be green

    # ── 6. 52w CONTEXT — was beaten down, now recovering ──
    last_252 = df.tail(252) if len(df) >= 252 else df
    high_52w = float(last_252["High"].max())
    low_52w = float(last_252["Low"].min())
    drawdown_52w = (high_52w - low_52w) / high_52w * 100.0
    if drawdown_52w < DRAWDOWN_52W_MIN:
        return None  # never had a real base, just a momentum stock
    pct_from_52w_high = (high_52w - close) / high_52w * 100.0
    if pct_from_52w_high > PCT_FROM_52W_HIGH_MAX:
        return None  # still too deep — not yet emerged

    # ── 7. RECOVERY ANGLE — must have moved off the low ──
    pct_off_52w_low = (close - low_52w) / low_52w * 100.0
    if pct_off_52w_low < 15:
        return None  # not yet recovering, may be dead cat

    # ── 8. LIQUIDITY ──
    avg_turnover_cr = float((df["Close"].tail(20) * df["Volume"].tail(20)).mean()) / 1e7
    if avg_turnover_cr < TURNOVER_MIN_CR:
        return None

    # ── 9. NOT IN F&O BAN / OVERHEATED RSI ──
    # We allow RSI up to 90 here (vs BTST's 70) because expansion days often
    # spike RSI temporarily — that's the whole point of the pattern
    if rsi > 92:
        return None  # extreme — likely pump-and-dump

    # ── TRADE PLAN ──
    entry = round(close, 2)
    # Stop: today's low - small buffer OR 10% absolute (whichever tighter)
    today_low_stop = low * 0.985
    pct_stop = close * (1 - STOP_MAX_PCT / 100)
    sl = round(max(today_low_stop, pct_stop), 2)
    risk = entry - sl
    if risk <= 0:
        return None
    t1 = round(close + risk * 2.0, 2)   # 2R
    t2 = round(close + risk * 4.0, 2)   # 4R
    rr = (t1 - close) / risk if risk > 0 else 0

    # Hold period: Stage 1→2 transitions typically take 5-30 days to confirm,
    # full Stage 2 advances run 20-90 days.
    recent_velocity = float(df["Close"].diff().abs().tail(10).mean())
    if recent_velocity <= 0:
        recent_velocity = atr * 0.6
    days_to_t1 = (t1 - close) / recent_velocity * 1.4
    days_to_t2 = (t2 - close) / recent_velocity * 1.4
    est_days_t1 = int(max(5, min(25, round(days_to_t1))))
    est_days_t2 = int(max(15, min(75, round(days_to_t2))))
    hold_period = f"{est_days_t1}-{est_days_t2} sessions"

    realistic = compute_realistic_targets(df, close, atr, est_days_t1, est_days_t2)

    # Distance to Golden Cross (informational)
    gc_distance_pct = (dma200 - dma50) / dma200 * 100.0 if dma50 < dma200 else 0
    near_golden_cross = -2 <= gc_distance_pct <= 8   # 50-DMA within striking distance

    return {
        "close": close, "open": open_, "high": high, "low": low,
        "volume": vol, "vol20": vol20, "vol_mult": vol_mult,
        "dma50": dma50, "dma200": dma200, "ema20": ema20,
        "rsi": rsi, "adx": adx, "atr": atr,
        "today_range": today_range, "range_vs_atr": range_vs_atr,
        "body_pct": body_pct, "close_position": close_pos,
        "high_52w": high_52w, "low_52w": low_52w,
        "drawdown_52w": drawdown_52w,
        "pct_from_52w_high": pct_from_52w_high,
        "pct_off_52w_low": pct_off_52w_low,
        "avg_turnover_cr": avg_turnover_cr,
        "gc_distance_pct": gc_distance_pct,
        "near_golden_cross": near_golden_cross,
        "entry": entry, "sl": sl, "t1": t1, "t2": t2, "rr": rr,
        "risk_pct": (1 - sl / entry) * 100,
        "t1_pct": (t1 / entry - 1) * 100,
        "t2_pct": (t2 / entry - 1) * 100,
        "est_days_t1": est_days_t1, "est_days_t2": est_days_t2,
        "hold_period": hold_period,
        **realistic,
    }


def score_vol_expansion(p):
    """0-100 composite score."""
    s = 0
    # Body size — biggest weight (this IS the pattern)
    s += min(25, (p["body_pct"] - BODY_PCT_MIN) * 2 + 12)
    # Volume signature
    s += min(25, (p["vol_mult"] - VOL_MULT_MIN) * 4 + 12)
    # Range expansion
    s += min(15, (p["range_vs_atr"] - RANGE_VS_ATR_MIN) * 6 + 5)
    # 52w recovery angle — moderate recovery is best (15-50% off low)
    angle = p["pct_off_52w_low"]
    if 20 <= angle <= 60:
        s += 12
    elif 15 <= angle < 20 or 60 < angle <= 100:
        s += 8
    else:
        s += 4
    # Near Golden Cross bonus
    if p["near_golden_cross"]:
        s += 8
    # Closing strength
    s += min(7, (p["close_position"] - CLOSE_POS_MIN) * 14)
    # Liquidity bonus
    s += min(5, p["avg_turnover_cr"] / 100)
    # RSI sweet spot for this pattern (60-80 is ideal — momentum present)
    rsi = p["rsi"]
    if 60 <= rsi <= 80:
        s += 3
    elif 50 <= rsi < 60:
        s += 2
    return round(min(100, s), 1)


# ──────────────────────────────────────────────────────────────────────────────
# OUTPUT FORMATTING
# ──────────────────────────────────────────────────────────────────────────────

def format_message(picks, scan_time):
    now = scan_time.strftime("%d %b %Y, %I:%M %p IST")
    msg = f"🚀 *QUANTEX VOL EXPANSION — {now}*\n"
    msg += f"#VolExpansion #Stage1to2 #ScoreCutoff{SCORE_THRESHOLD}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "_Stocks emerging from Stage 1 base on +5%+ body + 3×+ volume +_\n"
    msg += "_range expansion. The FIRST entry into a new Stage 2 trend._\n\n"

    if not picks:
        msg += "⚠️ No vol expansion breakouts fired today.\n"
        msg += "Either no stock printed the body+volume+range combo,\n"
        msg += "or candidates were already deep in Stage 2 (use Pocket Pivot for those).\n"
        return msg

    msg += f"*📋 {len(picks)} signals (max {MAX_SIGNALS_PER_DAY}/day, score ≥{SCORE_THRESHOLD}):*\n\n"

    for i, p in enumerate(picks, 1):
        d = p["pattern"]
        rt1 = d.get("realistic_t1"); rt2 = d.get("realistic_t2")
        gc_tag = " ⚡ near Golden Cross" if d["near_golden_cross"] else ""
        msg += (
            f"*{i}. {p['symbol']}* — Score *{p['score']}/100*{gc_tag}\n"
            f"   💰 Entry: ₹{d['entry']:.2f}\n"
            f"   🛑 SL: ₹{d['sl']:.2f} ({-d['risk_pct']:.1f}%)\n"
            f"   🎯 T1: ₹{d['t1']:.2f} (+{d['t1_pct']:.1f}%)  2R\n"
            f"   🎯 T2: ₹{d['t2']:.2f} (+{d['t2_pct']:.1f}%)  4R\n"
            f"   ⚖️  R:R 1:{d['rr']:.1f}  |  ⏱  Hold {d['hold_period']}\n"
        )
        if rt1 is not None:
            msg += (
                f"   🧠 Realistic T1: ₹{rt1:.2f} (+{(rt1/d['entry']-1)*100:.1f}%) _{d.get('realistic_t1_basis','')}_\n"
                f"   🧠 Realistic T2: ₹{rt2:.2f} (+{(rt2/d['entry']-1)*100:.1f}%) _{d.get('realistic_t2_basis','')}_\n"
            )
        msg += (
            f"   📊 *Pattern:*\n"
            f"     • Body *+{d['body_pct']:.1f}%* | Vol *{d['vol_mult']:.1f}× of 20d* | Range *{d['range_vs_atr']:.1f}× ATR*\n"
            f"     • Above 200-DMA ✓ | 50-DMA gap {d['gc_distance_pct']:+.1f}% from 200-DMA\n"
            f"     • {d['pct_from_52w_high']:.0f}% off 52w high | {d['pct_off_52w_low']:.0f}% off 52w low\n"
            f"     • RSI {d['rsi']:.0f} | ADX {d['adx']:.0f} | Turnover ₹{d['avg_turnover_cr']:.0f} Cr/day\n\n"
        )

    msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "⚠️ _Wider stops vs Pocket Pivot — transition entries are volatile._\n"
    msg += "_Hold longer (5-25 sessions for T1) — Stage 2 takes time to confirm._\n"
    msg += "🤖 _Quantex Vol Expansion Scanner — Stage 1→2 transition catcher_"
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
                "score": p["score"],
                "entry": p["pattern"]["entry"],
                "sl": p["pattern"]["sl"],
                "target1": p["pattern"]["t1"],
                "target2": p["pattern"]["t2"],
                "hold_period": p["pattern"]["hold_period"],
                "cmp": p["pattern"]["close"],
                "signals": [
                    f"Body +{p['pattern']['body_pct']:.1f}%",
                    f"Vol {p['pattern']['vol_mult']:.1f}x",
                    f"Range {p['pattern']['range_vs_atr']:.1f}xATR",
                    f"{p['pattern']['pct_from_52w_high']:.0f}% off 52wH",
                ] + (["Near Golden Cross"] if p["pattern"]["near_golden_cross"] else []),
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
    print(f"\n{'='*60}\n  QUANTEX VOL EXPANSION SCANNER — {datetime.now()}\n{'='*60}")
    print(f">> Universe: {len(UNIVERSE)} symbols")
    print(f">> Score threshold: {SCORE_THRESHOLD}  |  Max signals: {MAX_SIGNALS_PER_DAY}\n")

    candidates = []
    start = time.time()
    for idx, sym in enumerate(UNIVERSE, 1):
        if idx % 50 == 0:
            print(f"   ... {idx}/{len(UNIVERSE)} ({time.time()-start:.0f}s)")
        df = fetch_daily(sym)
        if df is None:
            continue
        df = annotate(df)
        p = detect_vol_expansion(df)
        if p is None:
            continue
        s = score_vol_expansion(p)
        if s >= SCORE_THRESHOLD:
            candidates.append({"symbol": sym, "score": s, "pattern": p})

    candidates.sort(key=lambda x: x["score"], reverse=True)
    picks = candidates[:MAX_SIGNALS_PER_DAY]

    elapsed = time.time() - start
    print(f"\n>> Scan complete in {elapsed:.0f}s — {len(candidates)} qualified, top {len(picks)} fired\n")
    for p in picks:
        d = p["pattern"]
        gc = "⚡GC" if d["near_golden_cross"] else "   "
        print(f"   {p['symbol']:13s} {gc} score {p['score']:5.1f}  body +{d['body_pct']:5.1f}%  "
              f"vol {d['vol_mult']:.1f}×  range {d['range_vs_atr']:.1f}×ATR  "
              f"entry ₹{d['entry']:7.2f}  R:R 1:{d['rr']:.1f}")

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
