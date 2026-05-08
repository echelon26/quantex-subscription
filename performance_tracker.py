#!/usr/bin/env python3
"""
Quantex Performance Tracker
============================
Tracks scanner recommendations against actual market outcomes.

For each past recommendation:
  1. Fetches price history from scan date to today
  2. Checks day-by-day: did T1 hit? T2 hit? SL triggered? Or still active?
  3. Records outcome: WIN (T1 hit), BIG WIN (T2 hit), LOSS (SL hit), ACTIVE (still open)
  4. Calculates: win rate, avg return, avg holding days, best/worst picks
  5. Sends weekly scorecard to Telegram

Reads: quantex_logs/recommendations.json
Writes: quantex_logs/performance.json
Sends: Telegram summary to admin + signal groups
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import requests
import yfinance as yf

# ─────────────────────────── CONFIGURATION ───────────────────────────

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_ADMIN_GROUP = os.environ.get("TELEGRAM_ADMIN_GROUPS", "").strip()

_signal_groups_raw = os.environ.get("TELEGRAM_SIGNAL_GROUPS", "").strip()
TELEGRAM_SIGNAL_GROUPS = [g.strip() for g in _signal_groups_raw.split(",") if g.strip()]

SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = SCRIPT_DIR / "quantex_logs"
RECOMMENDATIONS_FILE = LOG_DIR / "recommendations.json"
PERFORMANCE_FILE = LOG_DIR / "performance.json"

# How many days back to track (max)
MAX_TRACK_DAYS = 30

# After this many trading days, force-close as EXPIRED
MAX_HOLD_DAYS = 20


# ─────────────────────────── HELPERS ───────────────────────────

def fetch_history(symbol, start_date, end_date=None):
    """Fetch daily OHLC from scan date to today."""
    try:
        ticker = f"{symbol}.NS"
        df = yf.Ticker(ticker).history(start=start_date, end=end_date or datetime.now().strftime("%Y-%m-%d"))
        if df is None or df.empty:
            # Try BSE
            ticker = f"{symbol}.BO"
            df = yf.Ticker(ticker).history(start=start_date, end=end_date)
        if df is not None and not df.empty:
            if hasattr(df.columns, 'get_level_values'):
                df.columns = df.columns.get_level_values(0)
            return df
    except Exception as e:
        print(f"   Error fetching {symbol}: {e}")
    return None


def evaluate_trade(entry, sl, t1, t2, df_after_entry):
    """
    Walk through price bars day by day after entry.
    Check if SL or T1/T2 was hit first.

    BUG FIXES (2026-05-08):
      • Fix #1 (lock-in T1): once T1 was hit on any day, a subsequent SL touch
        resolves as T1_HIT (exit at T1 price), NEVER as SL_HIT. A disciplined
        trader exits at T1 / trails SL to breakeven — they don't passively let
        a 1R+ winner become a stop-out.
      • Fix #2 (same-day whipsaw): if today's bar has BOTH high >= T1 AND
        low <= SL, prefer T1_HIT (you'd have set GTT at T1 and tagged out).

    Returns dict:
      outcome: "T1_HIT" | "T2_HIT" | "SL_HIT" | "ACTIVE" | "EXPIRED" | "NO_DATA"
      exit_price, exit_date, days_held, return_pct, max_gain_pct,
      max_drawdown_pct,
      period_to_t1_days, period_to_t2_days, period_to_sl_days,
      period_total_days   ← explicit duration fields for CSV/Excel pivots
    """
    if df_after_entry is None or df_after_entry.empty:
        return {"outcome": "NO_DATA", "days_held": 0, "return_pct": 0}

    highs = df_after_entry["High"].values
    lows = df_after_entry["Low"].values
    closes = df_after_entry["Close"].values
    dates = df_after_entry.index

    max_high = entry
    min_low = entry
    t1_hit = False
    t1_info = {}

    def _build(outcome, exit_price, day_date, days_held, max_high, min_low,
               t1_info, period_t2=None, period_sl=None):
        """Common return-builder so we never forget to attach the period fields."""
        period_t1 = t1_info.get("t1_hit_day") if t1_info else None
        return {
            "outcome": outcome,
            "exit_price": round(exit_price, 2),
            "exit_date": day_date,
            "days_held": days_held,
            "return_pct": round(((exit_price - entry) / entry) * 100, 2),
            "max_gain_pct": round(((max_high - entry) / entry) * 100, 2),
            "max_drawdown_pct": round(((min_low - entry) / entry) * 100, 2),
            "period_to_t1_days": period_t1,
            "period_to_t2_days": period_t2,
            "period_to_sl_days": period_sl,
            "period_total_days": days_held,
            **t1_info,
        }

    for i in range(len(df_after_entry)):
        day_high = float(highs[i])
        day_low = float(lows[i])
        day_close = float(closes[i])
        day_date = dates[i].strftime("%Y-%m-%d") if hasattr(dates[i], 'strftime') else str(dates[i])[:10]

        max_high = max(max_high, day_high)
        min_low = min(min_low, day_low)

        # ── Detect what got tagged TODAY before deciding the outcome ──
        sl_tagged_today = day_low <= sl
        t1_tagged_today = day_high >= t1
        t2_tagged_today = day_high >= t2

        # Record T1 hit (sticky — cannot be un-set on later days)
        if not t1_hit and t1_tagged_today:
            t1_hit = True
            t1_info = {"t1_hit_date": day_date, "t1_hit_day": i + 1}

        # ── EXIT LOGIC (priority order matters) ──
        # Priority 1: T2_HIT (extended winner) — always wins, settle here.
        if t2_tagged_today:
            return _build("T2_HIT", t2, day_date, i + 1, max_high, min_low,
                          t1_info, period_t2=i + 1)

        # Priority 2: SL touch — outcome depends on whether T1 was already hit.
        if sl_tagged_today:
            if t1_hit:
                # FIX #1 + #2: T1 was already a winner (either prior day, or
                # this same day's high also hit T1). Trader would have exited
                # at T1 with profit. Mark as T1_HIT.
                return _build("T1_HIT", t1, day_date, i + 1, max_high, min_low,
                              t1_info, period_sl=None)
            else:
                return _build("SL_HIT", sl, day_date, i + 1, max_high, min_low,
                              t1_info={}, period_sl=i + 1)

        # Priority 3: MAX_HOLD reached — close at last close as T1_HIT or EXPIRED.
        if i + 1 >= MAX_HOLD_DAYS:
            outcome = "T1_HIT" if t1_hit else "EXPIRED"
            return _build(outcome, day_close, day_date, i + 1, max_high, min_low,
                          t1_info)

    # ── Still within hold window — trade is open OR T1 hit but no exit yet ──
    last_close = float(closes[-1])
    last_date = dates[-1].strftime("%Y-%m-%d") if hasattr(dates[-1], 'strftime') else str(dates[-1])[:10]
    outcome = "T1_HIT" if t1_hit else "ACTIVE"
    return _build(outcome, last_close, last_date, len(df_after_entry),
                  max_high, min_low, t1_info)


def load_recommendations():
    """Load all past recommendations from JSON."""
    if not RECOMMENDATIONS_FILE.exists():
        print("No recommendations file found.")
        return []
    with open(RECOMMENDATIONS_FILE) as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


def load_existing_performance():
    """Load existing performance data to avoid re-evaluating closed trades."""
    if not PERFORMANCE_FILE.exists():
        return {}
    try:
        with open(PERFORMANCE_FILE) as f:
            data = json.load(f)
        # Key: "symbol_scandate" → result
        return {p["key"]: p for p in data.get("trades", [])}
    except Exception:
        return {}


def run_tracker():
    """Main tracker: evaluate all recommendations."""
    print("=" * 60)
    print("  QUANTEX PERFORMANCE TRACKER")
    print(f"  Date: {datetime.now().strftime('%d %b %Y, %I:%M %p IST')}")
    print("=" * 60)

    recs = load_recommendations()
    if not recs:
        print("No recommendations to track.")
        return

    existing = load_existing_performance()
    cutoff_date = (datetime.now() - timedelta(days=MAX_TRACK_DAYS)).strftime("%Y-%m-%d")

    all_trades = []
    new_evaluations = 0
    seen_keys = set()  # Deduplicate: one entry per symbol per scan_date

    today_str = datetime.now().strftime("%Y-%m-%d")

    for scan in recs:
        scan_date = scan.get("scan_date", "")
        if not scan_date or scan_date < cutoff_date:
            continue

        stocks = scan.get("top_10", scan.get("top_stocks", []))
        regime = scan.get("market_regime", {})

        for stock in stocks:
            symbol = stock["symbol"]
            key = f"{symbol}_{scan_date}"

            # Deduplicate: skip if we already processed this symbol+date
            if key in seen_keys:
                continue
            seen_keys.add(key)

            entry = stock.get("entry", 0)
            sl = stock.get("sl", 0)
            t1 = stock.get("target1", 0)
            t2 = stock.get("target2", 0)
            score = stock.get("score", 0)

            if entry == 0 or sl == 0 or t1 == 0:
                continue

            # Skip today's scans — need at least 1 full trading day
            if scan_date == today_str:
                all_trades.append({
                    "key": key, "symbol": symbol, "scan_date": scan_date,
                    "score": score, "entry": entry, "sl": sl,
                    "target1": t1, "target2": t2,
                    "outcome": "ACTIVE", "days_held": 0,
                    "return_pct": 0, "exit_price": entry,
                    "exit_date": scan_date,
                    "regime": regime.get("label", ""),
                    "hold_period_est": stock.get("hold_period", ""),
                    "target_method": stock.get("target_method", ""),
                })
                continue

            # Skip already closed trades (SL_HIT, T2_HIT, EXPIRED)
            if key in existing and existing[key].get("outcome") in ("SL_HIT", "T2_HIT", "EXPIRED"):
                all_trades.append(existing[key])
                continue

            # Fetch price data from day AFTER scan (trade starts next day)
            next_day = (datetime.strptime(scan_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            print(f"  Tracking: {symbol} (scan: {scan_date}, entry: ₹{entry:.2f})...")

            df = fetch_history(symbol, next_day)
            if df is None or df.empty:
                # No data after scan date — mark as active
                all_trades.append({
                    "key": key, "symbol": symbol, "scan_date": scan_date,
                    "score": score, "entry": entry, "sl": sl,
                    "target1": t1, "target2": t2,
                    "outcome": "ACTIVE", "days_held": 0,
                    "return_pct": 0, "exit_price": entry,
                    "exit_date": scan_date,
                    "regime": regime.get("label", ""),
                })
                continue

            result = evaluate_trade(entry, sl, t1, t2, df)
            new_evaluations += 1

            trade = {
                "key": key,
                "symbol": symbol,
                "scan_date": scan_date,
                "score": score,
                "entry": entry,
                "sl": sl,
                "target1": t1,
                "target2": t2,
                "regime": regime.get("label", ""),
                "hold_period_est": stock.get("hold_period", ""),
                "target_method": stock.get("target_method", ""),
                **result,
            }
            all_trades.append(trade)

    print(f"\n>> Evaluated {new_evaluations} new trades, {len(all_trades)} total tracked")

    # ── Calculate Statistics ──
    stats = calculate_stats(all_trades)

    # ── Save Performance Data ──
    save_performance(all_trades, stats)

    # ── Generate & Send Report ──
    report = format_performance_report(all_trades, stats)
    print("\n" + report)
    send_telegram(report)

    return stats


def calculate_stats(trades):
    """Calculate aggregate performance statistics."""
    if not trades:
        return {}

    closed = [t for t in trades if t["outcome"] in ("T1_HIT", "T2_HIT", "SL_HIT", "EXPIRED")]
    active = [t for t in trades if t["outcome"] == "ACTIVE"]
    wins = [t for t in closed if t["outcome"] in ("T1_HIT", "T2_HIT")]
    losses = [t for t in closed if t["outcome"] == "SL_HIT"]
    expired = [t for t in closed if t["outcome"] == "EXPIRED"]

    total_closed = len(closed)
    win_count = len(wins)
    loss_count = len(losses)

    win_rate = (win_count / total_closed * 100) if total_closed > 0 else 0

    # Average returns
    all_returns = [t["return_pct"] for t in closed if "return_pct" in t]
    avg_return = np.mean(all_returns) if all_returns else 0
    total_return = sum(all_returns)

    win_returns = [t["return_pct"] for t in wins if "return_pct" in t]
    loss_returns = [t["return_pct"] for t in losses if "return_pct" in t]
    avg_win = np.mean(win_returns) if win_returns else 0
    avg_loss = np.mean(loss_returns) if loss_returns else 0

    # Average holding days
    all_days = [t["days_held"] for t in closed if "days_held" in t]
    avg_days = np.mean(all_days) if all_days else 0
    win_days = [t["days_held"] for t in wins if "days_held" in t]
    avg_win_days = np.mean(win_days) if win_days else 0

    # Best and worst trades
    best = max(closed, key=lambda t: t.get("return_pct", 0)) if closed else None
    worst = min(closed, key=lambda t: t.get("return_pct", 0)) if closed else None

    # Profit factor: total gains / total losses
    total_gains = sum(t["return_pct"] for t in wins if "return_pct" in t)
    total_losses = abs(sum(t["return_pct"] for t in losses if "return_pct" in t))
    profit_factor = total_gains / total_losses if total_losses > 0 else float('inf')

    # T1 vs T2 breakdown
    t1_hits = len([t for t in wins if t["outcome"] == "T1_HIT"])
    t2_hits = len([t for t in wins if t["outcome"] == "T2_HIT"])

    # Score-based analysis
    high_score_trades = [t for t in closed if t.get("score", 0) >= 75]
    high_score_wins = [t for t in high_score_trades if t["outcome"] in ("T1_HIT", "T2_HIT")]
    high_score_wr = (len(high_score_wins) / len(high_score_trades) * 100) if high_score_trades else 0

    return {
        "total_tracked": len(trades),
        "total_closed": total_closed,
        "active": len(active),
        "wins": win_count,
        "losses": loss_count,
        "expired": len(expired),
        "win_rate": round(win_rate, 1),
        "avg_return": round(avg_return, 2),
        "total_return": round(total_return, 2),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "avg_days_held": round(avg_days, 1),
        "avg_win_days": round(avg_win_days, 1),
        "profit_factor": round(profit_factor, 2),
        "t1_hits": t1_hits,
        "t2_hits": t2_hits,
        "high_score_win_rate": round(high_score_wr, 1),
        "best_trade": {
            "symbol": best["symbol"],
            "return_pct": best["return_pct"],
            "scan_date": best["scan_date"],
        } if best else None,
        "worst_trade": {
            "symbol": worst["symbol"],
            "return_pct": worst["return_pct"],
            "scan_date": worst["scan_date"],
        } if worst else None,
    }


def format_performance_report(trades, stats):
    """Format Telegram performance report."""
    now = datetime.now().strftime("%d %b %Y")

    msg = f"📊 *QUANTEX SCANNER — PERFORMANCE REPORT*\n"
    msg += f"_{now} | Last {MAX_TRACK_DAYS} Days_\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    if not stats or stats.get("total_closed", 0) == 0:
        msg += "No closed trades to report yet.\n"
        active = [t for t in trades if t.get("outcome") == "ACTIVE"]
        if active:
            msg += f"\n📌 *Active Trades:* {len(active)}\n"
            for t in active:
                pnl_emoji = "🟢" if t.get("return_pct", 0) >= 0 else "🔴"
                msg += f"   {pnl_emoji} {t['symbol']} — Entry: ₹{t['entry']:.0f} | Now: {t['return_pct']:+.1f}%\n"
        return msg

    # ── Overall Scorecard ──
    msg += f"*Overall Win Rate: {stats['win_rate']}%*\n"
    msg += f"Closed: {stats['total_closed']} | Wins: {stats['wins']} | Losses: {stats['losses']}"
    if stats['expired'] > 0:
        msg += f" | Expired: {stats['expired']}"
    msg += "\n"
    msg += f"T1 Hits: {stats['t1_hits']} | T2 Hits: {stats['t2_hits']}\n\n"

    msg += f"💰 *Returns*\n"
    msg += f"   Avg Win: +{stats['avg_win']:.1f}% | Avg Loss: {stats['avg_loss']:.1f}%\n"
    msg += f"   Avg Return: {stats['avg_return']:+.1f}% | Total: {stats['total_return']:+.1f}%\n"
    msg += f"   Profit Factor: {stats['profit_factor']:.2f}\n\n"

    msg += f"⏱ *Timing*\n"
    msg += f"   Avg Hold: {stats['avg_days_held']:.0f} days | Avg Win Hold: {stats['avg_win_days']:.0f} days\n\n"

    if stats.get('high_score_win_rate') and stats['total_closed'] >= 5:
        msg += f"⭐ *High Score (75+) Win Rate: {stats['high_score_win_rate']}%*\n\n"

    # ── Best & Worst ──
    if stats.get("best_trade"):
        b = stats["best_trade"]
        msg += f"🏆 Best: {b['symbol']} +{b['return_pct']:.1f}% ({b['scan_date']})\n"
    if stats.get("worst_trade"):
        w = stats["worst_trade"]
        msg += f"💀 Worst: {w['symbol']} {w['return_pct']:.1f}% ({w['scan_date']})\n"

    # ── Recent Closed Trades ──
    closed = [t for t in trades if t["outcome"] in ("T1_HIT", "T2_HIT", "SL_HIT", "EXPIRED")]
    closed.sort(key=lambda x: x.get("exit_date", ""), reverse=True)

    if closed:
        msg += f"\n*Recent Trades:*\n"
        for t in closed[:10]:
            if t["outcome"] == "SL_HIT":
                emoji = "🔴 SL"
            elif t["outcome"] == "T2_HIT":
                emoji = "🟢 T2"
            elif t["outcome"] == "T1_HIT":
                emoji = "🟢 T1"
            else:
                emoji = "⚪ EXP"
            msg += f"   {emoji} {t['symbol']} — {t['return_pct']:+.1f}% in {t['days_held']}d (score: {t.get('score', '?')})\n"

    # ── Active Trades ──
    active = [t for t in trades if t.get("outcome") == "ACTIVE"]
    if active:
        msg += f"\n📌 *Active Trades ({len(active)}):*\n"
        for t in active:
            pnl_emoji = "🟢" if t.get("return_pct", 0) >= 0 else "🔴"
            msg += f"   {pnl_emoji} {t['symbol']} — Entry: ₹{t['entry']:.0f} | P&L: {t['return_pct']:+.1f}% | Day {t['days_held']}\n"

    msg += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🤖 _Quantex Performance Tracker v1_"

    return msg


def save_performance(trades, stats):
    """Save performance data to JSON + CSV (Excel-friendly)."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Stamp every trade with last_evaluated_date and a default scanner_type
    # so CSV pivots / Excel filters work cleanly.
    now_iso = datetime.now().isoformat()
    for t in trades:
        t.setdefault("scanner_type", "pro")
        t["last_evaluated_date"] = now_iso

    data = {
        "last_updated": now_iso,
        "stats": stats,
        "trades": trades,
    }

    # ── 1. JSON (canonical) ──
    with open(PERFORMANCE_FILE, "w") as f:
        json.dump(data, f, indent=2, default=str)

    # ── 2. CSV mirror — opens directly in Excel; one row per pick ──
    import csv as _csv
    csv_path = LOG_DIR / "performance.csv"
    if trades:
        # Stable column order: most useful for sort/filter at the front.
        cols = [
            "scan_date", "symbol", "scanner_type", "score",
            "entry", "sl", "target1", "target2",
            "outcome",
            "period_to_t1_days", "period_to_t2_days",
            "period_to_sl_days", "period_total_days",
            "exit_date", "exit_price", "return_pct",
            "max_gain_pct", "max_drawdown_pct",
            "t1_hit_date", "t1_hit_day",
            "regime", "hold_period_est", "target_method",
            "last_evaluated_date", "key",
        ]
        # Include any extra fields not in the canonical list at the end.
        extras = sorted({k for t in trades for k in t.keys()} - set(cols))
        cols = cols + extras
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = _csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            writer.writeheader()
            for t in trades:
                writer.writerow({c: t.get(c, "") for c in cols})

    print(f">> Performance data saved to {PERFORMANCE_FILE}")
    print(f">> CSV mirror saved to {csv_path} ({len(trades)} rows)")


def send_telegram(message):
    """Send report to Telegram."""
    if not TELEGRAM_BOT_TOKEN:
        print("   Telegram not configured — skipping send.")
        return

    # NOTE: Performance tracker output goes to personal + admin only.
    # The subscriber signal group is reserved for the 8 AM pre-market report
    # and the 10 AM Pro Scanner; daily P&L recaps are admin-internal.
    destinations = [
        (TELEGRAM_CHAT_ID, "Personal Chat"),
        (TELEGRAM_ADMIN_GROUP, "Admin Group"),
    ]

    for chat_id, label in destinations:
        if not chat_id or chat_id in ("", "YOUR_CHAT_ID"):
            continue
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            resp = requests.post(url, json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }, timeout=15)
            if resp.status_code == 200:
                print(f"   ✅ Sent to {label}")
            else:
                print(f"   ❌ Failed {label}: {resp.text[:100]}")
        except Exception as e:
            print(f"   ❌ Error sending to {label}: {e}")


if __name__ == "__main__":
    run_tracker()
