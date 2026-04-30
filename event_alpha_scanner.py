#!/usr/bin/env python3
"""
Quantex Event Alpha Scanner — Post-Earnings Momentum Detector
==============================================================
Scans Nifty 500 stocks for recent earnings results and analyzes:
  1. Actual vs Estimated EPS (Beat / Miss / In-line)
  2. Actual vs Estimated Revenue (Surge / Decline / Flat)
  3. Post-earnings price action (Gap Up/Down, Momentum)
  4. Volume spike after results (institutional reaction)

Output: quantex_logs/event_alpha.csv  (Date, Symbol, Signals, CMP)
This CSV is consumed by premarket_report.py for the "Earnings Spotlight" section.

Schedule: Run daily at 7:00 PM IST (after market close + results announcements)
Trigger: repository_dispatch [event-alpha] or workflow_dispatch
"""

import csv
import json
import os
import warnings
from datetime import datetime, timedelta, date
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import yfinance as yf

warnings.filterwarnings("ignore")

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_ADMIN_GROUPS = os.environ.get("TELEGRAM_ADMIN_GROUPS", "").strip()

BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = BASE_DIR / "quantex_logs"
LOG_DIR.mkdir(exist_ok=True)

TODAY = date.today()
TODAY_STR = TODAY.strftime("%Y-%m-%d")

# ═══════════════════════════════════════════════════════════════
# STOCK UNIVERSE — Nifty 500 (same as premarket_report.py)
# ═══════════════════════════════════════════════════════════════
def fetch_nifty500_symbols():
    """Fetch Nifty 500 symbols from NSE, fallback to cached/hardcoded list."""
    cache_file = LOG_DIR / "nifty500_symbols.json"

    nse_urls = [
        "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv",
        "https://archives.nseindia.com/content/indices/ind_nifty500list.csv",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/csv,text/html,*/*",
    }
    for url in nse_urls:
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.ok and len(resp.text) > 500:
                import io
                csv_df = pd.read_csv(io.StringIO(resp.text))
                sym_col = [c for c in csv_df.columns if 'symbol' in c.lower()]
                if sym_col:
                    symbols = csv_df[sym_col[0]].dropna().str.strip().tolist()
                    symbols = [s for s in symbols if s and len(s) > 0]
                    if len(symbols) >= 400:
                        cache_file.write_text(json.dumps(symbols))
                        print(f"   Fetched {len(symbols)} Nifty 500 symbols from NSE")
                        return symbols
        except Exception:
            pass

    # Fallback: use cached file
    if cache_file.exists():
        try:
            symbols = json.loads(cache_file.read_text())
            if len(symbols) >= 400:
                print(f"   Using cached Nifty 500 list ({len(symbols)} symbols)")
                return symbols
        except Exception:
            pass

    # Final fallback: top ~200 high-impact stocks
    print("   NSE fetch failed, using hardcoded fallback")
    return [
        "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR",
        "SBIN", "BHARTIARTL", "KOTAKBANK", "ITC", "LT", "AXISBANK",
        "BAJFINANCE", "ASIANPAINT", "MARUTI", "HCLTECH", "SUNPHARMA",
        "TITAN", "WIPRO", "ULTRACEMCO", "NESTLEIND", "NTPC", "POWERGRID",
        "M&M", "TECHM", "TATASTEEL", "ONGC", "ADANIENT", "ADANIPORTS",
        "JSWSTEEL", "COALINDIA", "CIPLA", "DRREDDY", "DIVISLAB",
        "EICHERMOT", "HEROMOTOCO", "INDUSINDBK", "BRITANNIA", "APOLLOHOSP",
        "HINDALCO", "BAJAJ-AUTO", "BEL", "HAL", "TATAPOWER", "TRENT",
        "VEDL", "DLF", "HAVELLS", "POLYCAB", "PFC", "RECLTD",
        "SBILIFE", "HDFCLIFE", "BAJAJFINSV", "GRASIM", "BPCL",
        "TATACONSUM", "SHRIRAMFIN", "PIDILITIND", "SIEMENS", "ABB",
        "GODREJCP", "DABUR", "MARICO", "COLPAL", "BIOCON",
        "AUROPHARMA", "TORNTPHARM", "LUPIN", "ALKEM", "IPCALAB",
        "MUTHOOTFIN", "CHOLAFIN", "MAXHEALTH", "PERSISTENT", "COFORGE",
        "MPHASIS", "LTTS", "KPITTECH", "TATAELXSI", "DIXON",
        "KAYNES", "CROMPTON", "VOLTAS", "CUMMINSIND", "THERMAX",
        "SRF", "DEEPAKNTR", "FLUOROCHEM", "PIIND", "NAVINFLUOR",
        "SAIL", "NMDC", "NATIONALUM", "HINDCOPPER", "HINDZINC",
        "GODREJPROP", "PRESTIGE", "OBEROIRLTY", "LODHA", "BRIGADE",
        "BANKBARODA", "CANBK", "PNB", "UNIONBANK", "INDIANB",
        "IRCTC", "IRFC", "RVNL", "IRCON", "RAILTEL",
        "NHPC", "SJVN", "TATAPOWER", "SUZLON", "ADANIGREEN",
        "MAZDOCK", "COCHINSHIP", "GRSE", "BDL", "DATAPATTNS",
        "ZYDUSLIFE", "GLENMARK", "GRANULES", "NATCOPHARM", "LAURUSLABS",
        "MCX", "CDSL", "BSE", "IEX", "ANGELONE",
        "DMART", "JUBLFOOD", "DEVYANI", "PVRINOX", "SUNTV",
        "TATACHEM", "UPL", "TATACOMM", "ZEEL", "IDEA",

        # Extended watchlist (mirrors pro_scanner.py / swing_scanner.py).
        # Catches earnings events on mid/small-caps outside Nifty 500.
        "MTARTECH", "PARASD", "APOLLOMICRO", "HBLENGINE",
        "NTPCGREEN", "WAAREEENER", "ACMESOLAR",
        "SYRMA", "PGEL", "CYIENTDLM",
        "ANUPAMRAS", "TATVA",
        "PRAJIND", "TRITURBINE", "ELECON",
        "SOBHA", "KOLTEPATIL",
        "360ONE", "VEDANTFASH", "KEC",
    ]


# ═══════════════════════════════════════════════════════════════
# EARNINGS DETECTION & ANALYSIS
# ═══════════════════════════════════════════════════════════════

def analyze_earnings(symbol, lookback_days=3):
    """
    Check if a stock reported earnings in the last N days.
    If yes, analyze the post-earnings reaction and generate signals.

    Returns dict with Date, Symbol, Signals, CMP or None if no recent earnings.
    """
    ticker = f"{symbol}.NS"
    try:
        tk = yf.Ticker(ticker)

        # ── Step 1: Check if earnings were reported recently ──
        # Method A: Check earnings_dates (most reliable)
        earnings_date = None
        try:
            ed = tk.earnings_dates
            if ed is not None and len(ed) > 0:
                # earnings_dates index is datetime, filter for recent past dates
                for dt_idx in ed.index:
                    ed_date = dt_idx.date() if hasattr(dt_idx, 'date') else pd.to_datetime(dt_idx).date()
                    days_ago = (TODAY - ed_date).days
                    if 0 <= days_ago <= lookback_days:
                        earnings_date = ed_date
                        # Get actual vs estimated EPS from the earnings_dates df
                        row = ed.loc[dt_idx]
                        actual_eps = row.get("Reported EPS", None)
                        estimated_eps = row.get("EPS Estimate", None)
                        break
        except Exception:
            actual_eps = None
            estimated_eps = None

        # Method B: Check calendar if Method A didn't find anything
        if earnings_date is None:
            try:
                cal = tk.calendar
                if cal is not None:
                    ed_val = None
                    if isinstance(cal, dict):
                        ed_list = cal.get("Earnings Date")
                        if ed_list:
                            ed_val = ed_list[0] if isinstance(ed_list, list) else ed_list
                    elif hasattr(cal, 'iloc'):
                        for col in cal.columns:
                            try:
                                ed_val = pd.to_datetime(cal[col].iloc[0]).date()
                                break
                            except Exception:
                                pass
                    if ed_val:
                        ed_parsed = pd.to_datetime(ed_val).date() if not isinstance(ed_val, date) else ed_val
                        days_ago = (TODAY - ed_parsed).days
                        if 0 <= days_ago <= lookback_days:
                            earnings_date = ed_parsed
            except Exception:
                pass

        if earnings_date is None:
            return None

        # ── Step 2: Fetch price data around earnings ──
        start_date = earnings_date - timedelta(days=10)
        end_date = TODAY + timedelta(days=1)
        df = yf.download(ticker, start=str(start_date), end=str(end_date), progress=False)
        if df is None or len(df) < 3:
            return None

        df = df.reset_index()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        cmp = float(df["Close"].iloc[-1])

        # Find the pre-earnings close (last close before earnings date)
        df["DateOnly"] = pd.to_datetime(df["Date"]).dt.date
        pre_earnings = df[df["DateOnly"] < earnings_date]
        post_earnings = df[df["DateOnly"] >= earnings_date]

        if len(pre_earnings) == 0 or len(post_earnings) == 0:
            return None

        pre_close = float(pre_earnings["Close"].iloc[-1])
        post_open = float(post_earnings["Open"].iloc[0])
        post_close = float(post_earnings["Close"].iloc[0])

        # Volume comparison
        avg_vol = float(pre_earnings["Volume"].iloc[-5:].mean()) if len(pre_earnings) >= 5 else float(pre_earnings["Volume"].mean())
        post_vol = float(post_earnings["Volume"].iloc[0])
        vol_ratio = post_vol / avg_vol if avg_vol > 0 else 1

        # ── Step 3: Generate signals ──
        signals = []

        # a) EPS analysis
        if actual_eps is not None and estimated_eps is not None:
            try:
                actual_eps = float(actual_eps)
                estimated_eps = float(estimated_eps)
                if estimated_eps != 0:
                    eps_surprise = ((actual_eps - estimated_eps) / abs(estimated_eps)) * 100
                    if eps_surprise > 10:
                        signals.append(f"EPS Beat +{eps_surprise:.0f}%")
                    elif eps_surprise > 0:
                        signals.append(f"EPS Beat +{eps_surprise:.0f}%")
                    elif eps_surprise < -10:
                        signals.append(f"EPS Miss {eps_surprise:.0f}%")
                    elif eps_surprise < 0:
                        signals.append(f"EPS Miss {eps_surprise:.0f}%")
                    else:
                        signals.append("EPS In-Line")
                elif actual_eps > 0:
                    signals.append(f"EPS: {actual_eps:.2f}")
            except (ValueError, TypeError):
                pass

        # b) Price gap analysis (post-earnings reaction)
        gap_pct = ((post_open - pre_close) / pre_close) * 100
        day_chg_pct = ((post_close - pre_close) / pre_close) * 100

        if gap_pct > 5:
            signals.append(f"Gap Up +{gap_pct:.1f}%")
        elif gap_pct > 2:
            signals.append(f"Gap Up +{gap_pct:.1f}%")
        elif gap_pct < -5:
            signals.append(f"Gap Down {gap_pct:.1f}%")
        elif gap_pct < -2:
            signals.append(f"Gap Down {gap_pct:.1f}%")

        # c) Post-earnings day close vs open (did buyers/sellers step in?)
        if day_chg_pct > 5:
            signals.append(f"Surge +{day_chg_pct:.1f}%")
        elif day_chg_pct > 2:
            signals.append(f"Rally +{day_chg_pct:.1f}%")
        elif day_chg_pct < -5:
            signals.append(f"Decline {day_chg_pct:.1f}%")
        elif day_chg_pct < -2:
            signals.append(f"Selloff {day_chg_pct:.1f}%")
        else:
            signals.append(f"Flat {day_chg_pct:+.1f}%")

        # d) Volume reaction
        if vol_ratio >= 3.0:
            signals.append(f"Vol Explosion {vol_ratio:.1f}x")
        elif vol_ratio >= 2.0:
            signals.append(f"Vol Surge {vol_ratio:.1f}x")
        elif vol_ratio >= 1.5:
            signals.append(f"High Vol {vol_ratio:.1f}x")

        # e) Multi-day momentum (if we have 2+ post-earnings days)
        if len(post_earnings) >= 2:
            total_move = ((cmp - pre_close) / pre_close) * 100
            if total_move > 8:
                signals.append(f"Strong Momentum +{total_move:.1f}%")
            elif total_move < -8:
                signals.append(f"Heavy Selling {total_move:.1f}%")

        # f) Overall verdict
        if day_chg_pct > 3 and vol_ratio >= 1.5:
            signals.insert(0, "Bullish Reaction")
        elif day_chg_pct < -3 and vol_ratio >= 1.5:
            signals.insert(0, "Bearish Reaction")
        elif abs(day_chg_pct) <= 1 and vol_ratio < 1.3:
            signals.insert(0, "Muted Reaction")

        if not signals:
            signals.append("Results Declared")

        return {
            "Date": str(earnings_date),
            "Symbol": symbol,
            "Signals": " | ".join(signals),
            "CMP": round(cmp, 2),
        }

    except Exception as e:
        return None


# ═══════════════════════════════════════════════════════════════
# TELEGRAM NOTIFICATION
# ═══════════════════════════════════════════════════════════════

def send_telegram(message, chat_id):
    """Send message to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=30)
        return resp.ok
    except Exception:
        return False


def format_telegram_summary(results):
    """Format a Telegram summary of today's earnings results."""
    if not results:
        return None

    msg = f"<b>Quantex Event Alpha Scanner</b>\n"
    msg += f"{TODAY_STR}\n\n"
    msg += f"<b>{len(results)} stocks reported earnings recently:</b>\n\n"

    for r in results[:15]:  # Max 15 in message
        # Determine emoji based on signals
        sig = r["Signals"]
        if "Bullish" in sig or "Surge" in sig or "Beat" in sig:
            emoji = "🟢"
        elif "Bearish" in sig or "Decline" in sig or "Miss" in sig:
            emoji = "🔴"
        else:
            emoji = "🟡"

        msg += f"{emoji} <b>{r['Symbol']}</b> — {r['Date']}\n"
        msg += f"   CMP: {r['CMP']:,.2f} | {sig[:80]}\n\n"

    msg += f"<i>Powered by Quantex Scanner Bot</i>"
    return msg


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def run_event_alpha():
    """Main event alpha scanner execution."""
    print(f"{'='*60}")
    print(f"QUANTEX EVENT ALPHA SCANNER — {TODAY_STR}")
    print(f"{'='*60}\n")

    # Step 1: Load stock universe
    print(">> Loading stock universe...")
    symbols = fetch_nifty500_symbols()
    print(f"   Scanning {len(symbols)} stocks for recent earnings...\n")

    # Step 2: Scan each stock for recent earnings
    results = []
    total = len(symbols)
    for idx, symbol in enumerate(symbols, 1):
        if idx % 50 == 0 or idx == total:
            print(f"   Scanning... {idx}/{total} ({symbol})")

        result = analyze_earnings(symbol, lookback_days=3)
        if result:
            results.append(result)
            print(f"   >> Found: {result['Symbol']} — {result['Signals'][:60]}")

    print(f"\n>> Found {len(results)} stocks with recent earnings results")

    # Step 3: Load existing CSV and merge (keep last 30 days of data)
    csv_path = LOG_DIR / "event_alpha.csv"
    existing_rows = []
    if csv_path.exists():
        try:
            existing_df = pd.read_csv(csv_path)
            cutoff = (TODAY - timedelta(days=30)).strftime("%Y-%m-%d")
            existing_df["Date"] = pd.to_datetime(existing_df["Date"]).dt.strftime("%Y-%m-%d")
            existing_df = existing_df[existing_df["Date"] >= cutoff]
            existing_rows = existing_df.to_dict("records")
            print(f"   Loaded {len(existing_rows)} existing records (last 30 days)")
        except Exception as e:
            print(f"   Error loading existing CSV: {e}")

    # Merge: new results replace existing entries for same Symbol+Date
    existing_keys = set()
    for r in results:
        existing_keys.add(f"{r['Symbol']}_{r['Date']}")

    # Keep old rows that aren't being replaced by new scan
    merged = list(results)
    for old in existing_rows:
        key = f"{old['Symbol']}_{old['Date']}"
        if key not in existing_keys:
            merged.append(old)

    # Sort by date descending, then symbol
    merged.sort(key=lambda x: (x["Date"], x["Symbol"]), reverse=True)

    # Step 4: Save CSV
    try:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["Date", "Symbol", "Signals", "CMP"])
            writer.writeheader()
            writer.writerows(merged)
        print(f"\n>> CSV saved: {csv_path} ({len(merged)} total records)")
    except Exception as e:
        print(f"!! CSV save error: {e}")

    # Step 5: Save JSON log
    json_path = LOG_DIR / "event_alpha.json"
    try:
        log_entry = {
            "scan_date": TODAY_STR,
            "scan_time": datetime.now().isoformat(),
            "stocks_scanned": len(symbols),
            "results_found": len(results),
            "results": results,
        }
        existing_log = []
        if json_path.exists():
            try:
                existing_log = json.loads(json_path.read_text())
            except Exception:
                pass
        existing_log.append(log_entry)
        existing_log = existing_log[-90:]  # Keep last 90 days
        json_path.write_text(json.dumps(existing_log, indent=2))
        print(f">> JSON log saved: {json_path}")
    except Exception as e:
        print(f"!! JSON log error: {e}")

    # Step 6: Send Telegram summary
    if results:
        summary = format_telegram_summary(results)
        if summary:
            if TELEGRAM_CHAT_ID:
                print("\n>> Sending summary to personal chat...")
                send_telegram(summary, TELEGRAM_CHAT_ID)
            if TELEGRAM_ADMIN_GROUPS:
                for gid in TELEGRAM_ADMIN_GROUPS.split(","):
                    gid = gid.strip()
                    if gid:
                        print(f">> Sending summary to admin group {gid}...")
                        send_telegram(summary, gid)
    else:
        print("\n>> No recent earnings found. No Telegram message sent.")

    print(f"\n>> Event Alpha Scanner complete!")
    return results


if __name__ == "__main__":
    run_event_alpha()
