#!/usr/bin/env python3
"""
Quantex Chart Pattern Analyser
==============================

For any given stock, this:
    1. Reads the chart structure (Stage, swing pattern, volume bias, named patterns)
    2. Identifies WHY it's a buy candidate (selection rationale)
    3. Computes Entry / SL / T1 / T2 / R:R / hold period
    4. Generates an annotated candlestick chart (last ~120 sessions):
         • 20-EMA / 50-DMA / 200-DMA overlays
         • Swing high & low markers
         • Resistance / support zones as horizontal bands
         • Entry zone (green box), Stop (red), T1/T2 (green dashed)
         • Pattern label + setup-type annotation
         • Volume panel below, color-coded by candle direction
    5. Sends chart + caption to Telegram (admin chat + personal)

CLI:
    python chart_pattern_analyser.py THERMAX CGPOWER NATCOPHARM
    python chart_pattern_analyser.py --no-telegram THERMAX     # local preview only

Output:
    quantex_logs/charts/<symbol>_<date>.png
"""

import os
import sys
import io
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mplfinance as mpf
import requests

# Reuse existing helpers — single source of truth for fetch/annotate/scanners/targets
from pocket_pivot_scanner import (
    fetch_daily, annotate, detect_pocket_pivot, score_pivot,
)
# Tight coil scanner is optional — if not present, just skip that signal
try:
    from tight_coil_breakout_scanner import detect_tight_coil_breakout, score_coil
    _TC_AVAILABLE = True
except Exception:
    _TC_AVAILABLE = False
    def detect_tight_coil_breakout(df): return None
    def score_coil(p): return 0
from pro_scanner import (
    find_swing_points, find_resistance_zones, atr_projected_move,
    round_number_adjust,
)


# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_ADMIN_GROUPS = os.environ.get("TELEGRAM_ADMIN_GROUPS", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

CHART_DIR = Path(__file__).parent / "quantex_logs" / "charts"
CHART_DIR.mkdir(parents=True, exist_ok=True)

CHART_LOOKBACK = 120  # ~6 months of trading days
STOP_MAX_PCT = 8.0    # absolute stop cap


# ──────────────────────────────────────────────────────────────────────────────
# CHART PATTERN LIBRARY — 20+ classical patterns
# ──────────────────────────────────────────────────────────────────────────────

def detect_chart_patterns(df, close, dma50, dma200, ema20, atr, rsi, slope_50,
                          sh_pts, sl_pts, hh, hl, h52, l52, pct_from_h52):
    """Detect classical chart patterns. Returns ordered list — most actionable first.

    Continuation patterns (bullish):
      • Cup-with-handle, Cup-without-handle
      • Bull flag / pole-and-flag, Pennant
      • Ascending triangle, Symmetrical triangle
      • Rectangle / Darvas box, Falling wedge
      • VCP / tight base near pivot, Pullback to 50-DMA
      • High Tight Flag, Channel up
      • Inverse head & shoulders

    Reversal patterns (bullish from downtrend):
      • Double bottom, Triple bottom
      • Rounding bottom / saucer
      • Bullish engulfing at support

    Reversal patterns (bearish — flagged as caution):
      • Double top, Triple top, Head & shoulders
      • Rising wedge, Descending triangle
      • Channel down, Bearish engulfing at resistance

    Special structures:
      • 52w high breakout, HH+HL clean uptrend
      • Stage 1 base near 200-DMA
    """
    patterns = []

    range_20d_pct = (float(df.tail(20)["High"].max()) - float(df.tail(20)["Low"].min())) / close * 100
    range_10d_pct = (float(df.tail(10)["High"].max()) - float(df.tail(10)["Low"].min())) / close * 100
    pullback_30d = (float(df.tail(30)["High"].max()) - close) / float(df.tail(30)["High"].max()) * 100
    gain_60d = (close / float(df.tail(60)["Close"].iloc[0]) - 1) * 100 if len(df) >= 60 else 0
    gain_30d = (close / float(df.tail(30)["Close"].iloc[0]) - 1) * 100 if len(df) >= 30 else 0
    sh_prices = [s["price"] for s in sh_pts[-5:]]
    sl_prices = [s["price"] for s in sl_pts[-5:]]
    last = df.iloc[-1]; prev = df.iloc[-2] if len(df) >= 2 else last

    # ═══ CONTINUATION PATTERNS ═══

    # 1. CUP-WITH-HANDLE — deep U then tight pullback near old high
    if len(df) >= 80 and len(sh_pts) >= 1:
        cup_window = df.tail(80)
        cup_high_idx = cup_window['High'].values.argmax()
        cup_high = float(cup_window['High'].iloc[cup_high_idx])
        post_high = cup_window.iloc[cup_high_idx:]
        if len(post_high) >= 15:
            cup_low = float(post_high['Low'].min())
            cup_depth = (cup_high - cup_low) / cup_high * 100
            recovery_pct = (close - cup_low) / (cup_high - cup_low) if cup_high > cup_low else 0
            handle_pullback = (max(close, cup_high * 0.97) - close) / max(close, cup_high*0.97) * 100
            if 12 <= cup_depth <= 35 and recovery_pct >= 0.85 and 1 <= handle_pullback <= 8:
                patterns.append(f"Cup-with-handle (depth {cup_depth:.0f}%, {recovery_pct*100:.0f}% recovered)")
            elif 12 <= cup_depth <= 35 and recovery_pct >= 0.92:
                patterns.append(f"Cup-without-handle (depth {cup_depth:.0f}%, full recovery)")

    # 2. BULL FLAG / POLE-AND-FLAG
    if len(df) >= 30:
        pole = df.iloc[-30:-10]
        flag = df.iloc[-10:]
        pole_gain = (float(pole['Close'].iloc[-1]) / float(pole['Close'].iloc[0]) - 1) * 100
        flag_range = (float(flag['High'].max()) - float(flag['Low'].min())) / close * 100
        if pole_gain >= 15 and flag_range <= 8:
            patterns.append(f"Bull flag (pole +{pole_gain:.0f}%, flag {flag_range:.1f}%)")

    # 3. HIGH TIGHT FLAG — explosive 90%+ run then tight 3-5wk consolidation
    if len(df) >= 60:
        early = df.iloc[-60:-25]
        late = df.iloc[-25:]
        early_gain = (float(early['Close'].iloc[-1]) / float(early['Close'].iloc[0]) - 1) * 100
        late_range = (float(late['High'].max()) - float(late['Low'].min())) / close * 100
        if early_gain >= 90 and late_range <= 25:
            patterns.append(f"High Tight Flag (pole +{early_gain:.0f}% in 35d, flag {late_range:.0f}%)")

    # 4. PENNANT — small symmetrical triangle after sharp move
    if len(df) >= 25 and len(sh_pts) >= 2 and len(sl_pts) >= 2:
        recent_sh = sh_pts[-2:]; recent_sl = sl_pts[-2:]
        if (len(recent_sh) == 2 and len(recent_sl) == 2 and
            recent_sh[0]["price"] > recent_sh[1]["price"] and
            recent_sl[0]["price"] < recent_sl[1]["price"]):
            prev_run = (float(df.tail(30)['Close'].iloc[10]) / float(df.tail(30)['Close'].iloc[0]) - 1) * 100
            if abs(prev_run) >= 10 and range_10d_pct <= 7:
                direction = "bullish" if prev_run > 0 else "bearish"
                patterns.append(f"Pennant ({direction} — converging after {abs(prev_run):.0f}% move)")

    # 5. ASCENDING TRIANGLE — flat top resistance + rising lows
    if len(sh_pts) >= 2 and len(sl_pts) >= 2:
        recent_sh = sh_pts[-3:] if len(sh_pts) >= 3 else sh_pts[-2:]
        recent_sl = sl_pts[-3:] if len(sl_pts) >= 3 else sl_pts[-2:]
        sh_spread = (max(s["price"] for s in recent_sh) - min(s["price"] for s in recent_sh)) / close * 100
        if sh_spread < 2.5 and len(recent_sl) >= 2 and recent_sl[-1]["price"] > recent_sl[0]["price"]:
            patterns.append(f"Ascending triangle (flat top ₹{recent_sh[-1]['price']:.0f}, rising lows)")

    # 6. SYMMETRICAL TRIANGLE — converging
    if len(sh_pts) >= 2 and len(sl_pts) >= 2:
        if (sh_pts[-1]["price"] < sh_pts[-2]["price"] and
            sl_pts[-1]["price"] > sl_pts[-2]["price"] and
            range_20d_pct < 10):
            patterns.append("Symmetrical triangle (converging — breakout imminent)")

    # 7. DESCENDING TRIANGLE — flat bottom support + lower highs (bearish)
    if len(sh_pts) >= 2 and len(sl_pts) >= 2:
        recent_sl = sl_pts[-3:] if len(sl_pts) >= 3 else sl_pts[-2:]
        sl_spread = (max(s["price"] for s in recent_sl) - min(s["price"] for s in recent_sl)) / close * 100
        if sl_spread < 2.5 and sh_pts[-1]["price"] < sh_pts[-2]["price"]:
            patterns.append(f"⚠️ Descending triangle (flat bottom ₹{recent_sl[-1]['price']:.0f}, lower highs)")

    # 8. RECTANGLE / DARVAS BOX
    if len(sh_pts) >= 2 and len(sl_pts) >= 2 and len(df) >= 30:
        last_30 = df.tail(30)
        box_high = float(last_30['High'].max()); box_low = float(last_30['Low'].min())
        box_range_pct = (box_high - box_low) / close * 100
        sh_30 = [s for s in sh_pts if s["index"] >= len(df) - 30]
        sl_30 = [s for s in sl_pts if s["index"] >= len(df) - 30]
        if (len(sh_30) >= 2 and len(sl_30) >= 2 and box_range_pct < 12 and
            max(s["price"] for s in sh_30) - min(s["price"] for s in sh_30) < close * 0.025 and
            max(s["price"] for s in sl_30) - min(s["price"] for s in sl_30) < close * 0.025):
            patterns.append(f"Darvas box (₹{box_low:.0f}-₹{box_high:.0f}, range {box_range_pct:.1f}%)")

    # 9. FALLING WEDGE (bullish reversal/continuation)
    if len(sh_pts) >= 2 and len(sl_pts) >= 2:
        if (sh_pts[-1]["price"] < sh_pts[-2]["price"] and
            sl_pts[-1]["price"] < sl_pts[-2]["price"]):
            sh_drop = (sh_pts[-2]["price"] - sh_pts[-1]["price"]) / sh_pts[-2]["price"]
            sl_drop = (sl_pts[-2]["price"] - sl_pts[-1]["price"]) / sl_pts[-2]["price"]
            if sh_drop > sl_drop * 1.3:  # highs falling faster than lows = converging downward
                patterns.append("Falling wedge (bullish — converging downward, breakout up likely)")

    # 10. RISING WEDGE (bearish)
    if len(sh_pts) >= 2 and len(sl_pts) >= 2:
        if (sh_pts[-1]["price"] > sh_pts[-2]["price"] and
            sl_pts[-1]["price"] > sl_pts[-2]["price"]):
            sh_rise = (sh_pts[-1]["price"] - sh_pts[-2]["price"]) / sh_pts[-2]["price"]
            sl_rise = (sl_pts[-1]["price"] - sl_pts[-2]["price"]) / sl_pts[-2]["price"]
            if sl_rise > sh_rise * 1.3 and pct_from_h52 < 5:
                patterns.append("⚠️ Rising wedge (bearish — lows rising faster than highs)")

    # 11. CHANNEL UP / CHANNEL DOWN
    if len(sh_pts) >= 3 and len(sl_pts) >= 3:
        sh3 = sh_pts[-3:]; sl3 = sl_pts[-3:]
        sh_trend = sh3[-1]["price"] > sh3[0]["price"]
        sl_trend = sl3[-1]["price"] > sl3[0]["price"]
        if sh_trend and sl_trend and hh >= 2 and hl >= 2:
            patterns.append("Channel up (parallel HHs + HLs — measured uptrend)")
        elif not sh_trend and not sl_trend:
            patterns.append("⚠️ Channel down (parallel LHs + LLs — measured downtrend)")

    # 12. VCP / TIGHT BASE NEAR PIVOT (Minervini)
    if range_20d_pct <= 8 and pullback_30d <= 5 and pct_from_h52 < 5:
        patterns.append(f"VCP / Tight base near pivot (range {range_20d_pct:.1f}%, {pct_from_h52:.1f}% from 52wH)")

    # 13. PULLBACK TO 50-DMA — re-launch zone
    dist_50dma = (close - dma50) / dma50 * 100
    if 0 < dist_50dma < 4 and slope_50 > 0 and pullback_30d > 8:
        patterns.append("Pullback to 50-DMA in rising trend — re-launch zone")

    # ═══ REVERSAL PATTERNS ═══

    # 14. DOUBLE BOTTOM (W) — two lows at similar price, rallying out
    if len(sl_pts) >= 2:
        last_two = sl_pts[-2:]
        diff = abs(last_two[0]["price"] - last_two[1]["price"]) / last_two[0]["price"] * 100
        gap = last_two[1]["index"] - last_two[0]["index"]
        if diff < 3 and gap >= 8 and close > min(last_two[0]["price"], last_two[1]["price"]) * 1.05:
            patterns.append(f"Double bottom at ~₹{last_two[1]['price']:.0f} (W-shape)")

    # 15. TRIPLE BOTTOM
    if len(sl_pts) >= 3:
        last_three = sl_pts[-3:]
        prices = [s["price"] for s in last_three]
        spread = (max(prices) - min(prices)) / min(prices) * 100
        if spread < 3 and last_three[-1]["index"] - last_three[0]["index"] >= 15:
            patterns.append(f"Triple bottom at ~₹{prices[-1]:.0f}")

    # 16. INVERSE HEAD & SHOULDERS (IH&S) — bullish reversal
    if len(sl_pts) >= 3:
        ls, head, rs = sl_pts[-3], sl_pts[-2], sl_pts[-1]
        if (head["price"] < ls["price"] and head["price"] < rs["price"] and
            abs(ls["price"] - rs["price"]) / ls["price"] < 0.05):
            patterns.append(f"Inverse Head & Shoulders (head ₹{head['price']:.0f}, neckline forming)")

    # 17. HEAD & SHOULDERS TOP (bearish)
    if len(sh_pts) >= 3:
        ls, head, rs = sh_pts[-3], sh_pts[-2], sh_pts[-1]
        if (head["price"] > ls["price"] and head["price"] > rs["price"] and
            abs(ls["price"] - rs["price"]) / ls["price"] < 0.05 and
            rs["price"] < head["price"] * 0.97):
            patterns.append(f"⚠️ Head & Shoulders top (head ₹{head['price']:.0f}, distribution risk)")

    # 18. ROUNDING BOTTOM / SAUCER
    if len(df) >= 80:
        window = df.tail(80)
        mid = len(window) // 2
        early_avg = float(window['Close'].iloc[:mid].mean())
        late_avg = float(window['Close'].iloc[mid:].mean())
        low_idx = window['Low'].values.argmin()
        # Trough should be roughly in the middle of the window
        if mid * 0.25 < low_idx < mid * 1.75 and late_avg > early_avg * 1.03 and close > window['Low'].min() * 1.15:
            patterns.append("Rounding bottom / saucer (slow accumulation)")

    # 19. DOUBLE TOP (M) — bearish
    if len(sh_pts) >= 2:
        last_two = sh_pts[-2:]
        diff = abs(last_two[0]["price"] - last_two[1]["price"]) / last_two[0]["price"] * 100
        gap = last_two[1]["index"] - last_two[0]["index"]
        if diff < 2 and gap >= 10 and pct_from_h52 < 5:
            patterns.append(f"⚠️ Double top at ~₹{last_two[1]['price']:.0f} (M-shape)")

    # 20. TRIPLE TOP
    if len(sh_pts) >= 3:
        last_three = sh_pts[-3:]
        prices = [s["price"] for s in last_three]
        spread = (max(prices) - min(prices)) / max(prices) * 100
        if spread < 2 and last_three[-1]["index"] - last_three[0]["index"] >= 15:
            patterns.append(f"⚠️ Triple top at ~₹{prices[-1]:.0f}")

    # ═══ CANDLESTICK PATTERNS (recent 2 bars) ═══

    # 21. BULLISH ENGULFING (last 2 bars at support)
    if len(df) >= 2:
        prev_red = float(prev["Close"]) < float(prev["Open"])
        today_green = float(last["Close"]) > float(last["Open"])
        engulfs = (float(last["Close"]) > float(prev["Open"]) and
                   float(last["Open"]) < float(prev["Close"]))
        near_support = sl_prices and abs(close - max(p for p in sl_prices if p < close * 1.05)) / close < 0.03 if any(p < close * 1.05 for p in sl_prices) else False
        if prev_red and today_green and engulfs:
            patterns.append("Bullish engulfing candle (today swallows yesterday's red)")

    # 22. BEARISH ENGULFING
    if len(df) >= 2:
        prev_green = float(prev["Close"]) > float(prev["Open"])
        today_red = float(last["Close"]) < float(last["Open"])
        engulfs_down = (float(last["Open"]) > float(prev["Close"]) and
                       float(last["Close"]) < float(prev["Open"]))
        if prev_green and today_red and engulfs_down and pct_from_h52 < 5:
            patterns.append("⚠️ Bearish engulfing candle (rejection at high)")

    # 23. HAMMER (bullish at oversold)
    if len(df) >= 1:
        body = abs(float(last["Close"]) - float(last["Open"]))
        lower_wick = min(float(last["Open"]), float(last["Close"])) - float(last["Low"])
        upper_wick = float(last["High"]) - max(float(last["Open"]), float(last["Close"]))
        if body > 0 and lower_wick > 2 * body and upper_wick < body * 0.5 and rsi < 45:
            patterns.append("Hammer candle at oversold zone (potential reversal)")

    # ═══ SPECIAL STRUCTURES ═══

    # 24. 52-WEEK HIGH BREAKOUT (today is making/at the high)
    if pct_from_h52 < 1.0:
        patterns.append("At/near 52-week high — new-high breakout candidate")
    elif pct_from_h52 < 3.0:
        patterns.append(f"Within 3% of 52w high — pre-breakout zone")

    # 25. CLEAN HH+HL UPTREND
    if hh >= 2 and hl >= 2 and pct_from_h52 < 12:
        patterns.append(f"Clean HH+HL uptrend ({hh} HHs + {hl} HLs)")

    # 26. ⚠️ LH+LL DOWNTREND (caution flag)
    if hh == 0 and hl == 0 and len(sh_prices) >= 2 and len(sl_prices) >= 2:
        if all(sh_prices[i] < sh_prices[i-1] for i in range(1, len(sh_prices))) and \
           all(sl_prices[i] < sl_prices[i-1] for i in range(1, len(sl_prices))):
            patterns.append("⚠️ LH+LL downtrend structure")

    # 27. STAGE 1 BASE near 200-DMA
    if dma50 < dma200 and abs(close - dma200) / dma200 < 0.05:
        patterns.append("Stage 1 base — testing 200-DMA from below")

    # 28. 200-DMA RECLAIM (close just crossed above)
    prev_close = float(df.iloc[-2]["Close"]) if len(df) >= 2 else close
    prev_dma200 = float(df["DMA200"].iloc[-2]) if len(df) >= 2 and not pd.isna(df["DMA200"].iloc[-2]) else dma200
    if prev_close < prev_dma200 and close > dma200:
        patterns.append("⚡ 200-DMA reclaim today (Stage 1 → Stage 2 transition)")

    # 29. GOLDEN CROSS (50-DMA just crossed above 200-DMA)
    if len(df) >= 3:
        dma50_2d = float(df["DMA50"].iloc[-2])
        dma200_2d = float(df["DMA200"].iloc[-2])
        if not (pd.isna(dma50_2d) or pd.isna(dma200_2d)):
            if dma50_2d < dma200_2d and dma50 > dma200:
                patterns.append("⚡ Golden Cross today (50-DMA crossed above 200-DMA)")

    # 30. DEATH CROSS (bearish)
    if len(df) >= 3:
        dma50_2d = float(df["DMA50"].iloc[-2])
        dma200_2d = float(df["DMA200"].iloc[-2])
        if not (pd.isna(dma50_2d) or pd.isna(dma200_2d)):
            if dma50_2d > dma200_2d and dma50 < dma200:
                patterns.append("⚠️ Death Cross today (50-DMA crossed below 200-DMA)")

    return patterns if patterns else ["No clean named pattern — choppy / undefined structure"]


# ──────────────────────────────────────────────────────────────────────────────
# ANALYSIS
# ──────────────────────────────────────────────────────────────────────────────

def analyse(symbol):
    """Return a dict with all the data we need to draw + caption."""
    df = fetch_daily(symbol)
    if df is None or len(df) < 200:
        return None
    df = annotate(df)
    df["Vol50"] = df["Volume"].rolling(50).mean()

    last = df.iloc[-1]
    c = float(last["Close"])
    o = float(last["Open"])
    h = float(last["High"])
    l = float(last["Low"])
    vol = float(last["Volume"])
    dma50 = float(last["DMA50"])
    dma200 = float(last["DMA200"])
    ema20 = float(last["EMA20"])
    rsi = float(last["RSI"])
    atr = float(last["ATR14"])
    vol50 = float(last["Vol50"]) if not pd.isna(last["Vol50"]) else 0

    # ── Trend slopes ──
    dma50_prev = float(df["DMA50"].iloc[-6]) if len(df) > 6 else dma50
    slope_50 = (dma50 - dma50_prev) / dma50_prev * 100 if dma50_prev > 0 else 0
    dma200_prev = float(df["DMA200"].iloc[-21]) if len(df) > 21 else dma200
    slope_200 = (dma200 - dma200_prev) / dma200_prev * 100 if dma200_prev > 0 else 0

    # ── 52w extremes ──
    last_252 = df.tail(252) if len(df) >= 252 else df
    h52 = float(last_252["High"].max())
    l52 = float(last_252["Low"].min())
    pct_from_h52 = (h52 - c) / h52 * 100

    # ── Stage ──
    if c > dma50 > dma200 and slope_50 > 0:
        stage = "Stage 2 — UP"
    elif c > dma50 > dma200 and slope_50 <= 0:
        stage = "Stage 2 — UP (slope flattening)"
    elif dma50 > dma200 and c < dma50:
        stage = "Stage 3 — TOP"
    elif dma50 < dma200 and c < dma50:
        stage = "Stage 4 — DOWN"
    else:
        stage = "Stage 1 — BASE"

    # ── Swing structure ──
    sub = df.tail(120).reset_index()
    sh_pts, sl_pts = find_swing_points(sub, lookback=5)
    sh_p = [s["price"] for s in sh_pts[-4:]]
    sl_p = [s["price"] for s in sl_pts[-4:]]
    hh = sum(1 for i in range(1, len(sh_p)) if sh_p[i] > sh_p[i - 1])
    hl = sum(1 for i in range(1, len(sl_p)) if sl_p[i] > sl_p[i - 1])
    if hh >= max(1, len(sh_p) - 2) and hl >= max(1, len(sl_p) - 2):
        swing_struct = "Higher highs + higher lows — clean uptrend"
    elif hh == 0 and len(sh_p) >= 2:
        swing_struct = "Lower highs — distribution risk"
    else:
        swing_struct = "Mixed / consolidating"

    # ── Volume bias ──
    last_20 = df.tail(20)
    gv = float(last_20[last_20["Close"] > last_20["Open"]]["Volume"].mean()) if any(last_20["Close"] > last_20["Open"]) else 0
    rv = float(last_20[last_20["Close"] < last_20["Open"]]["Volume"].mean()) if any(last_20["Close"] < last_20["Open"]) else 0
    vol_bias = gv / rv if rv > 0 else 1.0
    if vol_bias > 1.1:
        vol_label = f"Accumulation ({vol_bias:.2f}×)"
    elif vol_bias < 0.9:
        vol_label = f"Distribution ({vol_bias:.2f}×)"
    else:
        vol_label = f"Neutral ({vol_bias:.2f}×)"

    # ── Setup detection — comprehensive chart pattern library ──
    setups = detect_chart_patterns(
        df=df, close=c, dma50=dma50, dma200=dma200, ema20=ema20,
        atr=atr, rsi=rsi, slope_50=slope_50,
        sh_pts=sh_pts, sl_pts=sl_pts, hh=hh, hl=hl,
        h52=h52, l52=l52, pct_from_h52=pct_from_h52,
    )

    # Scanners firing today
    pp = detect_pocket_pivot(df)
    tc = detect_tight_coil_breakout(df)
    scanner_fires = []
    if pp:
        scanner_fires.append(f"Pocket Pivot ({score_pivot(pp):.0f}/100)")
    if tc:
        scanner_fires.append(f"Tight Coil Breakout ({score_coil(tc):.0f}/100)")

    # ── Entry / SL / T1 / T2 ──
    # Entry = current close
    entry = round(c, 2)
    # SL: nearest swing low below, capped at -8%
    sl_below = sorted([s["price"] for s in sl_pts if s["price"] < c], reverse=True)
    if sl_below:
        struct_sl = sl_below[0] * 0.99  # 1% buffer below swing low
    else:
        struct_sl = c * 0.93
    pct_sl = c * (1 - STOP_MAX_PCT / 100)
    sl = round(max(struct_sl, pct_sl), 2)  # tighter of the two
    risk = entry - sl

    # T1: nearest resistance OR 2R, whichever closer
    resistances = find_resistance_zones(df, c, lookback_days=120) or []
    t1_r = entry + risk * 2.0   # 2R target
    if resistances:
        nearest_res = resistances[0]["price"]
        # Use 2R but cap by resistance + ATR projection
        atr_cap_t1 = atr_projected_move(df, c, holding_days=15)
        t1_smart = min(t1_r, max(nearest_res, atr_cap_t1))
        t1 = round(t1_smart, 2)
        t1_basis = f"2R / nearest resistance ₹{nearest_res:.0f}" if t1_smart == nearest_res else "2R target"
    else:
        t1 = round(t1_r, 2)
        t1_basis = "2R target"
    # Apply round-number adjustment
    t1_adj = round_number_adjust(t1, c)
    t1 = round(t1_adj, 2)

    # T2: 4R or next resistance
    t2_r = entry + risk * 4.0
    if len(resistances) > 1:
        next_res = resistances[1]["price"]
        atr_cap_t2 = atr_projected_move(df, c, holding_days=30) * 1.15
        t2 = round(min(t2_r, max(next_res, atr_cap_t2)), 2)
        t2_basis = f"4R / next resistance ₹{next_res:.0f}"
    else:
        t2 = round(t2_r, 2)
        t2_basis = "4R target"
    t2 = round(round_number_adjust(t2, c), 2)
    if t2 <= t1 * 1.03:
        t2 = round(t1 * 1.05, 2)

    rr = (t1 - entry) / risk if risk > 0 else 0

    # Hold period
    recent_velocity = float(df["Close"].diff().abs().tail(10).mean())
    if recent_velocity <= 0:
        recent_velocity = atr * 0.6
    days_to_t1 = (t1 - entry) / recent_velocity * 1.4
    days_to_t2 = (t2 - entry) / recent_velocity * 1.4
    est_days_t1 = int(max(5, min(30, round(days_to_t1))))
    est_days_t2 = int(max(15, min(60, round(days_to_t2))))

    # ── Why selected (rationale narrative) ──
    rationale = []
    if "Stage 2" in stage:
        rationale.append(f"Confirmed {stage} (50-DMA rising +{slope_50:.1f}%/wk)")
    if vol_bias > 1.1:
        rationale.append(f"Accumulation volume bias {vol_bias:.2f}× (green-day vol > red-day vol)")
    if hh >= 2 and hl >= 2:
        rationale.append(f"Clean swing structure: {hh} HHs + {hl} HLs")
    if pct_from_h52 < 5:
        rationale.append(f"Just {pct_from_h52:.1f}% off 52w high — relative strength leader")
    if 50 <= rsi <= 70:
        rationale.append(f"RSI {rsi:.0f} in sweet spot (50-70 range)")
    # Show top 2 bullish patterns + warn if bearish are present
    bullish_setups = [s for s in setups if not s.startswith("⚠️")]
    bearish_setups = [s for s in setups if s.startswith("⚠️")]
    if bullish_setups:
        rationale.append(f"Primary pattern: *{bullish_setups[0]}*")
        if len(bullish_setups) > 1:
            rationale.append(f"Also detected: {bullish_setups[1]}")
    if bearish_setups:
        rationale.append(f"⚠️ Caution flag: {bearish_setups[0].lstrip('⚠️ ')}")
    if scanner_fires:
        rationale.append(f"Scanner firing TODAY: {', '.join(scanner_fires)}")
    if not rationale:
        rationale.append("Multi-factor chart-quality score qualified")

    return {
        "symbol": symbol,
        "df": df,
        "close": c, "open": o, "high": h, "low": l,
        "rsi": rsi, "atr": atr, "vol": vol,
        "dma50": dma50, "dma200": dma200, "ema20": ema20,
        "slope_50": slope_50, "slope_200": slope_200,
        "stage": stage,
        "swing_struct": swing_struct,
        "swing_highs": sh_pts, "swing_lows": sl_pts,
        "vol_bias": vol_bias, "vol_label": vol_label,
        "h52": h52, "l52": l52, "pct_from_h52": pct_from_h52,
        "setups": setups,
        "scanner_fires": scanner_fires,
        "resistances": resistances,
        "support_below": sl_below[:3],
        "entry": entry, "sl": sl, "t1": t1, "t2": t2,
        "t1_basis": t1_basis, "t2_basis": t2_basis,
        "risk_pct": (1 - sl / entry) * 100,
        "t1_pct": (t1 / entry - 1) * 100,
        "t2_pct": (t2 / entry - 1) * 100,
        "rr": rr,
        "est_days_t1": est_days_t1, "est_days_t2": est_days_t2,
        "hold_period": f"{est_days_t1}-{est_days_t2} sessions",
        "rationale": rationale,
    }


# ──────────────────────────────────────────────────────────────────────────────
# CHART RENDERING
# ──────────────────────────────────────────────────────────────────────────────

def draw_chart(a, output_path):
    """Generate an annotated candlestick chart and save to PNG.

    Design rules:
      • All annotations stay INSIDE the chart axes (no floating boxes outside)
      • Y-axis range is set explicitly to include Entry/SL/T1/T2 + breathing room
      • Trade-plan labels sit on the right edge of the chart, anchored to axes coords
      • Less clutter: only 2 most recent swing markers per direction, 1 resistance line
    """
    df = a["df"].tail(CHART_LOOKBACK).copy()
    df.index.name = "Date"

    # ── Y-axis range: include all trade-plan levels with buffer ──
    price_min = min(float(df["Low"].min()), a["sl"]) * 0.985
    price_max = max(float(df["High"].max()), a["t2"]) * 1.02

    # ── MA overlays ──
    addplots = [
        mpf.make_addplot(df["EMA20"],  color="#9ca3af", width=1.0, linestyle="--"),
        mpf.make_addplot(df["DMA50"],  color="#2563eb", width=1.6),
        mpf.make_addplot(df["DMA200"], color="#dc2626", width=1.6),
    ]

    # ── Market style ──
    mc = mpf.make_marketcolors(
        up="#16a34a", down="#dc2626", edge="inherit",
        wick={"up": "#16a34a", "down": "#dc2626"},
        volume={"up": "#16a34a", "down": "#dc2626"},
    )
    style = mpf.make_mpf_style(
        marketcolors=mc, gridstyle="--", gridcolor="#f3f4f6",
        facecolor="white", figcolor="white",
        rc={"font.size": 10, "axes.labelsize": 10, "axes.titlesize": 12,
            "xtick.labelsize": 9, "ytick.labelsize": 9},
    )

    fig, axlist = mpf.plot(
        df, type="candle", style=style, addplot=addplots, volume=True,
        figsize=(16, 10), returnfig=True,
        panel_ratios=(4, 1), tight_layout=False, show_nontrading=False,
        ylim=(price_min, price_max),
        update_width_config=dict(candle_linewidth=0.9, candle_width=0.6),
    )
    ax_main = axlist[0]
    ax_vol = axlist[2]
    fig.subplots_adjust(top=0.92, bottom=0.08, left=0.06, right=0.93)

    xmax_data = len(df) - 1

    # ── Trade-plan horizontal lines (full-width) ──
    plan = [
        ("Entry", a["entry"], 0.0,            "#16a34a", "-",  2.0),
        ("SL",    a["sl"],    -a["risk_pct"], "#dc2626", "-",  1.6),
        ("T1",    a["t1"],    a["t1_pct"],    "#15803d", "--", 1.4),
        ("T2",    a["t2"],    a["t2_pct"],    "#15803d", ":",  1.2),
    ]
    for label, px, pct, color, ls, lw in plan:
        ax_main.axhline(px, color=color, linestyle=ls, linewidth=lw, alpha=0.85, zorder=3)
        # Label INSIDE the axes, right edge — axes-coords for x, data-coords for y
        pct_str = f"{pct:+.1f}%" if label != "Entry" else "entry"
        text = f"{label} ₹{px:,.0f}  {pct_str}"
        ax_main.annotate(
            text,
            xy=(1.0, px), xycoords=("axes fraction", "data"),
            xytext=(-8, 0), textcoords="offset points",
            ha="right", va="center", fontsize=10, fontweight="bold",
            color=color,
            bbox=dict(boxstyle="round,pad=0.35", facecolor="white",
                      edgecolor=color, linewidth=1.2, alpha=0.95),
            zorder=10,
        )

    # ── Swing markers (3 most recent each direction, smaller, less obtrusive) ──
    df_idx_to_pos = {ts: i for i, ts in enumerate(df.index)}
    sub = a["df"].tail(CHART_LOOKBACK).reset_index()
    sh_pts, sl_pts = find_swing_points(sub, lookback=5)
    for s in sh_pts[-3:]:
        try:
            ts = sub.iloc[s["index"]]["Date"]
            if ts in df_idx_to_pos:
                pos = df_idx_to_pos[ts]
                ax_main.scatter([pos], [s["price"]], marker="v", s=55, color="#dc2626",
                                zorder=5, edgecolors="white", linewidths=1.2)
        except Exception:
            pass
    for s in sl_pts[-3:]:
        try:
            ts = sub.iloc[s["index"]]["Date"]
            if ts in df_idx_to_pos:
                pos = df_idx_to_pos[ts]
                ax_main.scatter([pos], [s["price"]], marker="^", s=55, color="#16a34a",
                                zorder=5, edgecolors="white", linewidths=1.2)
        except Exception:
            pass

    # ── Single most-important resistance zone (subtle, inside axes) ──
    if a["resistances"]:
        r = a["resistances"][0]
        if price_min < r["price"] < price_max:
            ax_main.axhline(r["price"], color="#f59e0b", linestyle=":", linewidth=0.9, alpha=0.6, zorder=2)
            ax_main.annotate(
                f"R ₹{r['price']:,.0f} ({r['touches']}× touches)",
                xy=(0.0, r["price"]), xycoords=("axes fraction", "data"),
                xytext=(8, 0), textcoords="offset points",
                ha="left", va="center", fontsize=8.5, color="#92400e",
                bbox=dict(boxstyle="round,pad=0.25", facecolor="#fef3c7",
                          edgecolor="#f59e0b", linewidth=0.6, alpha=0.85),
                zorder=4,
            )

    # ── Title block — symbol, price, stage, RSI ──
    fig.suptitle(
        f"{a['symbol']}    ₹{a['close']:,.2f}    {a['stage']}    RSI {a['rsi']:.0f}",
        fontsize=15, fontweight="bold", y=0.97, color="#111827",
    )

    # ── Setup banner (top-left, inside chart) — shows primary + count ──
    bullish_count = sum(1 for p in (a["setups"] or []) if not p.startswith("⚠️"))
    bearish_count = sum(1 for p in (a["setups"] or []) if p.startswith("⚠️"))
    primary = a["setups"][0] if a["setups"] else a["swing_struct"]
    multi_suffix = ""
    if bullish_count + bearish_count > 1:
        parts = []
        if bullish_count: parts.append(f"+{bullish_count - (1 if not primary.startswith('⚠️') else 0)} bullish")
        if bearish_count: parts.append(f"+{bearish_count - (1 if primary.startswith('⚠️') else 0)} caution")
        parts = [p for p in parts if not p.startswith("+0")]
        if parts: multi_suffix = "    (" + ", ".join(parts) + ")"
    setup_text = f"PATTERN:  {primary}{multi_suffix}"
    if a["scanner_fires"]:
        setup_text += f"   ⚡ {a['scanner_fires'][0]}"
    banner_color = "#7f1d1d" if primary.startswith("⚠️") else "#1e3a8a"
    banner_bg = "#fee2e2" if primary.startswith("⚠️") else "#dbeafe"
    ax_main.text(
        0.012, 0.972, setup_text,
        transform=ax_main.transAxes, fontsize=10.5, fontweight="bold",
        color=banner_color, va="top", ha="left",
        bbox=dict(boxstyle="round,pad=0.45", facecolor=banner_bg,
                  edgecolor=banner_color, linewidth=0.8, alpha=0.92),
        zorder=12,
    )

    # ── Diagnostics line (bottom-left, inside main chart) ──
    diag = (f"R:R 1:{a['rr']:.1f}    Hold {a['hold_period']}    "
            f"Vol bias: {a['vol_label']}    52wH: ₹{a['h52']:,.0f} ({a['pct_from_h52']:.1f}% away)")
    ax_main.text(
        0.012, 0.025, diag,
        transform=ax_main.transAxes, fontsize=9, color="#374151",
        va="bottom", ha="left",
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white",
                  edgecolor="#9ca3af", linewidth=0.6, alpha=0.88),
        zorder=12,
    )

    # ── MA legend (top-right, inside axes) ──
    legend_items = [
        ("EMA20", "#9ca3af", "--"),
        ("50-DMA", "#2563eb", "-"),
        ("200-DMA", "#dc2626", "-"),
    ]
    y_pos = 0.972
    for name, color, ls in legend_items:
        ax_main.text(
            0.988, y_pos, f"━ {name}" if ls == "-" else f"╌ {name}",
            transform=ax_main.transAxes, fontsize=9, color=color,
            fontweight="bold", va="top", ha="right",
            zorder=12,
        )
        y_pos -= 0.028

    # ── Volume label on Y-axis of volume panel ──
    ax_vol.set_ylabel("Volume", fontsize=9, color="#6b7280")

    fig.savefig(output_path, dpi=160, bbox_inches="tight", facecolor="white", pad_inches=0.15)
    plt.close(fig)
    return output_path


# ──────────────────────────────────────────────────────────────────────────────
# TELEGRAM CAPTION + SENDER
# ──────────────────────────────────────────────────────────────────────────────

def format_caption(a):
    """Build a Markdown caption for Telegram."""
    # Split detected patterns into bullish vs bearish (warnings)
    bullish = [p for p in a["setups"] if not p.startswith("⚠️")]
    bearish = [p for p in a["setups"] if p.startswith("⚠️")]

    lines = [
        f"📊 *QUANTEX CHART ANALYSIS — {a['symbol']}*",
        f"_{datetime.now().strftime('%d %b %Y, %I:%M %p IST')}_",
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"💰 *Current Price*: ₹{a['close']:,.2f}",
        f"📈 *Trend*: {a['stage']}  |  RSI {a['rsi']:.0f}",
        f"📊 *Volume*: {a['vol_label']}",
        f"📍 *52w high*: ₹{a['h52']:,.0f}  ({a['pct_from_h52']:.1f}% away)",
        "",
        f"*🎯 CHART PATTERNS DETECTED ({len(a['setups'])}):*",
    ]
    if bullish:
        lines.append("  *Bullish / continuation:*")
        for p in bullish:
            lines.append(f"    ✅ {p}")
    if bearish:
        lines.append("  *Caution flags:*")
        for p in bearish:
            lines.append(f"    {p}")
    if not bullish and not bearish:
        lines.append(f"    • {a['setups'][0] if a['setups'] else 'None'}")

    lines += [
        "",
        "*🧠 WHY THIS STOCK:*",
    ]
    for r in a["rationale"]:
        lines.append(f"  • {r}")
    lines += [
        "",
        "*📋 TRADE PLAN:*",
        f"  Entry:  ₹{a['entry']:,.2f}",
        f"  🛑 SL:  ₹{a['sl']:,.2f}  ({-a['risk_pct']:.1f}%)",
        f"  🎯 T1:  ₹{a['t1']:,.2f}  (+{a['t1_pct']:.1f}%)  _{a['t1_basis']}_",
        f"  🎯 T2:  ₹{a['t2']:,.2f}  (+{a['t2_pct']:.1f}%)  _{a['t2_basis']}_",
        f"  ⚖️  R:R 1:{a['rr']:.1f}",
        f"  ⏱  Hold: {a['hold_period']}",
    ]
    if a["scanner_fires"]:
        lines += ["", f"*⚡ Active scanner signals:* {', '.join(a['scanner_fires'])}"]
    if a["swing_struct"]:
        lines += ["", f"_Swing structure: {a['swing_struct']}_"]
    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        "⚠️ _Educational only. Not investment advice._",
        "🤖 _Quantex Chart Pattern Analyser_",
    ]
    return "\n".join(lines)


def send_photo_to_chat(chat_id, image_path, caption):
    if not (TELEGRAM_BOT_TOKEN and chat_id):
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(image_path, "rb") as f:
            r = requests.post(url, data={
                "chat_id": chat_id, "caption": caption,
                "parse_mode": "Markdown",
            }, files={"photo": f}, timeout=60)
        return r.status_code == 200
    except Exception as e:
        print(f"   Telegram error: {e}")
        return False


def send_telegram(image_path, caption):
    """Admin + personal only."""
    sent = 0
    for chat_id in [TELEGRAM_CHAT_ID, TELEGRAM_ADMIN_GROUPS]:
        if not chat_id:
            continue
        for gid in chat_id.split(","):
            gid = gid.strip()
            if gid and send_photo_to_chat(gid, image_path, caption):
                sent += 1
    return sent


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    args = [a for a in sys.argv[1:] if a]
    if not args:
        print("Usage: python chart_pattern_analyser.py [--no-telegram] SYM1 SYM2 ...")
        sys.exit(1)

    send_to_tg = True
    if "--no-telegram" in args:
        send_to_tg = False
        args = [a for a in args if a != "--no-telegram"]

    today = datetime.now().strftime("%Y-%m-%d")

    for sym in args:
        print(f"\n— {sym} —")
        a = analyse(sym)
        if a is None:
            print(f"   ⚠️  No data for {sym}")
            continue
        out = CHART_DIR / f"{sym}_{today}.png"
        draw_chart(a, out)
        print(f"   📊 Chart saved: {out}")
        caption = format_caption(a)
        print(f"\n--- TELEGRAM CAPTION PREVIEW ---\n{caption}\n")
        if send_to_tg and TELEGRAM_BOT_TOKEN:
            n = send_telegram(out, caption)
            print(f"   ✉️  Sent to {n} chat(s)")
        else:
            print(f"   (Telegram skipped — {'no token' if not TELEGRAM_BOT_TOKEN else '--no-telegram flag'})")


if __name__ == "__main__":
    main()
