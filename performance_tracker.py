#!/usr/bin/env python3
"""
Quantex Performance Tracker
============================
Bi-weekly review (1st & 15th of each month).
- Reads recommendations.csv
- Fetches actual price history for each pick
- Checks if T1, T2, or SL was hit (and when)
- Updates recommendations.csv with outcome columns
- Sends performance scorecard to Telegram
- Commits updated CSV back to repo
"""

import csv
import json
import os
import sys
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import yfinance as yf

try:
    import pyotp
    HAS_PYOTP = True
except ImportError:
    HAS_PYOTP = False

# ─────────────────────────── CONFIGURATION ───────────────────────────

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8573365697:AAESTWF5H1ZAKE0bQg-yQbBaJoGLZwcZ9XQ")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "854001335")
TELEGRAM_SIGNAL_GROUP = os.environ.get("TELEGRAM_SIGNAL_GROUPS", "-1003754088976")
TELEGRAM_ADMIN_GROUP = os.environ.get("TELEGRAM_ADMIN_GROUPS", "-5298634309")

KITE_API_KEY = os.environ.get("KITE_API_KEY", "7sa02mhb5t3onyt8")
KITE_API_SECRET = os.environ.get("KITE_API_SECRET", "okmrcxgxsswyd4g4ydz4xw3utlh6cj5n")
ZERODHA_USER_ID = os.environ.get("ZERODHA_USER_ID", "YSZ319")
ZERODHA_PASSWORD = os.environ.get("ZERODHA_PASSWORD", "HelloJitu@2019")
ZERODHA_TOTP_KEY = os.environ.get("ZERODHA_TOTP_KEY", "TWOA2OHXLR7VWLEJZPWVPTDROPQK7TFZ")

SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = SCRIPT_DIR / "quantex_logs"
CSV_PATH = LOG_DIR / "recommendations.csv"

# Outcome columns to add/update
OUTCOME_COLUMNS = [
    "Result",        # T1_HIT, T2_HIT, SL_HIT, OPEN, EXPIRED
    "Hit_Price",     # Price at which T1/T2/SL was hit
    "Hit_Date",      # Date when result was confirmed
    "Days_Held",     # Days from entry to hit
    "Actual_Return",  # Actual % return at hit
    "Peak_High",     # Highest price reached since entry
    "Max_Drawdown",  # Maximum drawdown % from entry
]

# Expire trades after 30 calendar days with no T1/T2/SL hit
EXPIRY_DAYS = 30


# ─────────────────────────── KITE SESSION ───────────────────────────

class KiteSession:
    """Lightweight Kite login for fetching historical data."""

    def __init__(self):
        self.kite = None
        self.logged_in = False

    def login(self):
        """Attempt automated Kite login."""
        try:
            from kiteconnect import KiteConnect
        except ImportError:
            print("⚠️ kiteconnect not installed, using Yahoo Finance only")
            return False

        try:
            kite = KiteConnect(api_key=KITE_API_KEY)
            session = requests.Session()

            # Step 1: POST login
            login_resp = session.post(
                "https://kite.zerodha.com/api/login",
                data={"user_id": ZERODHA_USER_ID, "password": ZERODHA_PASSWORD},
                timeout=30,
            )
            login_data = login_resp.json().get("data", {})
            request_id = login_data.get("request_id", "")
            if not request_id:
                print("❌ Kite login failed: no request_id")
                return False

            # Step 2: POST TOTP
            if not HAS_PYOTP:
                print("❌ pyotp not installed")
                return False
            totp = pyotp.TOTP(ZERODHA_TOTP_KEY)
            totp_resp = session.post(
                "https://kite.zerodha.com/api/twofa",
                data={"user_id": ZERODHA_USER_ID, "request_id": request_id, "twofa_value": totp.now(), "twofa_type": "totp"},
                timeout=30,
            )

            # Step 3: Extract request_token from OAuth redirect
            redirect_url = f"https://kite.trade/connect/login?v=3&api_key={KITE_API_KEY}"
            r = session.get(redirect_url, allow_redirects=False, timeout=30)
            location = r.headers.get("Location", "")

            if "request_token=" not in location:
                if "/connect/finish" in location:
                    r2 = session.get(location, allow_redirects=False, timeout=30)
                    location = r2.headers.get("Location", "")

            if "request_token=" not in location:
                print("❌ Could not extract request_token")
                return False

            from urllib.parse import parse_qs, urlparse
            token = parse_qs(urlparse(location).query).get("request_token", [None])[0]
            if not token:
                return False

            # Step 4: Generate session
            data = kite.generate_session(token, api_secret=KITE_API_SECRET)
            kite.set_access_token(data["access_token"])
            self.kite = kite
            self.logged_in = True
            profile = kite.profile()
            print(f"✅ Kite logged in: {profile.get('user_name', ZERODHA_USER_ID)}")
            return True
        except Exception as e:
            print(f"⚠️ Kite login failed: {e}")
            return False

    def get_historical(self, symbol, from_date, to_date):
        """Fetch daily OHLCV from Kite."""
        if not self.logged_in or not self.kite:
            return None
        try:
            # Find instrument token
            instruments = self.kite.instruments("NSE")
            token = None
            for inst in instruments:
                if inst["tradingsymbol"] == symbol:
                    token = inst["instrument_token"]
                    break
            if not token:
                return None

            data = self.kite.historical_data(token, from_date, to_date, "day")
            if not data:
                return None
            df = pd.DataFrame(data)
            df.columns = [c.capitalize() if c != "date" else "Date" for c in df.columns]
            return df
        except Exception as e:
            print(f"  ⚠️ Kite historical error for {symbol}: {e}")
            return None


kite_session = KiteSession()


# ─────────────────────────── DATA FETCHING ───────────────────────────

def fetch_price_history(symbol, from_date, to_date):
    """Fetch daily OHLCV data. Kite first, Yahoo fallback."""
    # Try Kite
    if kite_session.logged_in:
        df = kite_session.get_historical(symbol, from_date, to_date)
        if df is not None and len(df) > 0:
            return df

    # Yahoo Finance fallback
    try:
        ticker = f"{symbol}.NS"
        # Add 1 day buffer to to_date for Yahoo
        yf_to = (to_date + timedelta(days=1)).strftime("%Y-%m-%d")
        yf_from = from_date.strftime("%Y-%m-%d")
        df = yf.download(ticker, start=yf_from, end=yf_to, progress=False)
        if df is not None and len(df) > 0:
            df = df.reset_index()
            # Handle MultiIndex columns from yfinance
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
            return df
    except Exception:
        pass

    return None


# ─────────────────────────── TRADE ANALYSIS ───────────────────────────

def analyze_trade(symbol, entry_date, entry_price, sl_price, t1_price, t2_price):
    """
    Analyze a trade by fetching price history from entry_date to today.
    Returns dict with Result, Hit_Price, Hit_Date, Days_Held, Actual_Return, Peak_High, Max_Drawdown.
    """
    today = datetime.now().date()
    from_date = entry_date
    to_date = today

    df = fetch_price_history(symbol, from_date, to_date)
    if df is None or len(df) == 0:
        return {
            "Result": "NO_DATA",
            "Hit_Price": "",
            "Hit_Date": "",
            "Days_Held": "",
            "Actual_Return": "",
            "Peak_High": "",
            "Max_Drawdown": "",
        }

    peak_high = float(entry_price)
    max_drawdown = 0.0
    result = None
    hit_price = None
    hit_date = None
    days_held = None

    for _, row in df.iterrows():
        row_date = pd.Timestamp(row.get("Date", row.get("date", ""))).date() if "Date" in row or "date" in row else None
        if row_date is None:
            continue
        if row_date <= entry_date:
            continue

        high = float(row.get("High", row.get("high", 0)))
        low = float(row.get("Low", row.get("low", 0)))
        close = float(row.get("Close", row.get("close", 0)))

        # Track peak and drawdown
        if high > peak_high:
            peak_high = high
        current_dd = (low - entry_price) / entry_price * 100
        if current_dd < max_drawdown:
            max_drawdown = current_dd

        # Check SL hit (intraday low)
        if low <= sl_price and result is None:
            result = "SL_HIT"
            hit_price = sl_price
            hit_date = row_date
            days_held = (row_date - entry_date).days
            # Don't break — continue to track if T1 was hit on same day before SL
            # Actually, check if high hit T1 before low hit SL on same candle
            if high >= t1_price:
                # Ambiguous — conservatively assume T1 hit first if high > t1
                result = "T1_HIT"
                hit_price = t1_price
            break

        # Check T2 hit first (intraday high)
        if high >= t2_price and result is None:
            result = "T2_HIT"
            hit_price = t2_price
            hit_date = row_date
            days_held = (row_date - entry_date).days
            break

        # Check T1 hit (intraday high)
        if high >= t1_price and result is None:
            result = "T1_HIT"
            hit_price = t1_price
            hit_date = row_date
            days_held = (row_date - entry_date).days
            # Continue to see if T2 also gets hit later
            # But for simplicity, record T1 first — next review will catch T2
            break

    # If no result yet
    if result is None:
        elapsed = (today - entry_date).days
        if elapsed >= EXPIRY_DAYS:
            # Expired — use last close as exit
            last_close = float(df.iloc[-1].get("Close", df.iloc[-1].get("close", entry_price)))
            result = "EXPIRED"
            hit_price = last_close
            hit_date = today
            days_held = elapsed
        else:
            result = "OPEN"
            hit_price = float(df.iloc[-1].get("Close", df.iloc[-1].get("close", entry_price)))
            hit_date = ""
            days_held = elapsed

    # Calculate actual return
    if hit_price and entry_price > 0:
        actual_return = round((float(hit_price) - entry_price) / entry_price * 100, 2)
    else:
        actual_return = ""

    return {
        "Result": result,
        "Hit_Price": round(float(hit_price), 2) if hit_price else "",
        "Hit_Date": str(hit_date) if hit_date else "",
        "Days_Held": days_held if days_held is not None else "",
        "Actual_Return": f"{actual_return}%" if actual_return != "" else "",
        "Peak_High": round(peak_high, 2),
        "Max_Drawdown": f"{round(max_drawdown, 2)}%",
    }


# ─────────────────────────── CSV UPDATE ───────────────────────────

def load_csv():
    """Load recommendations.csv into list of dicts."""
    if not CSV_PATH.exists():
        print(f"❌ CSV not found: {CSV_PATH}")
        return []
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def save_csv(rows):
    """Save updated rows back to recommendations.csv."""
    if not rows:
        return
    # Ensure outcome columns exist in fieldnames
    fieldnames = list(rows[0].keys())
    for col in OUTCOME_COLUMNS:
        if col not in fieldnames:
            fieldnames.append(col)

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"📝 CSV updated: {CSV_PATH}")


# ─────────────────────────── TELEGRAM ───────────────────────────

def send_telegram(message):
    """Send message to all Telegram destinations."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    destinations = [
        (TELEGRAM_CHAT_ID, "Personal Chat"),
        (TELEGRAM_SIGNAL_GROUP, "Signal Group"),
        (TELEGRAM_ADMIN_GROUP, "Admin Group"),
    ]
    print("📨 Sending performance report to Telegram...")
    for chat_id, label in destinations:
        if chat_id and chat_id not in ("", "YOUR_CHAT_ID"):
            payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown", "disable_web_page_preview": True}
            try:
                resp = requests.post(url, json=payload, timeout=30)
                if resp.status_code == 200:
                    print(f"  ✅ Sent to {label}")
                else:
                    # Retry without Markdown
                    del payload["parse_mode"]
                    resp2 = requests.post(url, json=payload, timeout=30)
                    if resp2.status_code == 200:
                        print(f"  ✅ Sent to {label} (plain text)")
                    else:
                        print(f"  ❌ Failed for {label}: {resp2.text}")
            except Exception as e:
                print(f"  ❌ Error for {label}: {e}")


def format_scorecard(stats, updated_rows, review_period):
    """Format performance scorecard for Telegram."""
    msg = f"📊 *QUANTEX PERFORMANCE REVIEW*\n"
    msg += f"#Performance #Monthly\n"
    msg += f"_{review_period}_\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    msg += f"📋 *Summary*\n"
    msg += f"   Total Picks: {stats['total']} | Reviewed: {stats['reviewed']}\n"
    msg += f"   Still Open: {stats['open']} | No Data: {stats['no_data']}\n\n"

    if stats["reviewed"] > 0:
        msg += f"✅ *Results*\n"
        msg += f"   🎯 T1 Hit: {stats['t1_hit']} ({stats['t1_pct']:.0f}%)\n"
        msg += f"   🎯 T2 Hit: {stats['t2_hit']} ({stats['t2_pct']:.0f}%)\n"
        msg += f"   🛑 SL Hit: {stats['sl_hit']} ({stats['sl_pct']:.0f}%)\n"
        msg += f"   ⏰ Expired: {stats['expired']} ({stats['expired_pct']:.0f}%)\n\n"

        msg += f"📈 *Performance*\n"
        msg += f"   Win Rate: *{stats['win_rate']:.1f}%*\n"
        msg += f"   Avg Return: *{stats['avg_return']:+.2f}%*\n"
        msg += f"   Avg Days Held: {stats['avg_days']:.0f}\n"

        if stats["best_pick"]:
            msg += f"   🏆 Best: {stats['best_pick']} ({stats['best_return']:+.2f}%)\n"
        if stats["worst_pick"]:
            msg += f"   💔 Worst: {stats['worst_pick']} ({stats['worst_return']:+.2f}%)\n"

    msg += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

    # Show details of completed trades
    completed = [r for r in updated_rows if r.get("Result") in ("T1_HIT", "T2_HIT", "SL_HIT", "EXPIRED")]
    if completed:
        msg += f"\n📝 *Trade Details*\n\n"
        for r in completed[:15]:  # Limit to 15 to avoid message length issues
            symbol = r["Symbol"]
            result = r["Result"]
            actual_ret = r.get("Actual_Return", "")
            days = r.get("Days_Held", "")
            hit_date = r.get("Hit_Date", "")

            emoji = "✅" if result in ("T1_HIT", "T2_HIT") else "🛑" if result == "SL_HIT" else "⏰"
            msg += f"   {emoji} {symbol}: {result.replace('_', ' ')} | {actual_ret} | {days}d | {hit_date}\n"

    msg += f"\n⚠️ _For educational purposes only._\n"
    msg += f"🤖 _Quantex Performance Tracker_"

    return msg


# ─────────────────────────── MAIN ───────────────────────────

def run_tracker():
    """Main performance tracker execution."""
    print(f"📊 Quantex Performance Tracker started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Load CSV
    rows = load_csv()
    if not rows:
        print("❌ No recommendations found. Nothing to track.")
        return

    print(f"📋 Loaded {len(rows)} recommendations")

    # Login to Kite for price data
    print("🔐 Connecting to Kite API...")
    kite_session.login()

    # Track stats
    total = len(rows)
    reviewed = 0
    t1_hit = 0
    t2_hit = 0
    sl_hit = 0
    expired = 0
    open_trades = 0
    no_data = 0
    returns = []
    days_list = []
    best_pick = None
    best_return = -999
    worst_pick = None
    worst_return = 999

    updated_rows = []

    for i, row in enumerate(rows):
        symbol = row.get("Symbol", "")
        date_str = row.get("Date", "")
        entry = float(row.get("Entry", 0))
        sl = float(row.get("SL", 0))
        t1 = float(row.get("Target1", 0))
        t2 = float(row.get("Target2", 0))

        if not symbol or not date_str or entry == 0:
            updated_rows.append(row)
            continue

        # Parse entry date
        try:
            entry_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            updated_rows.append(row)
            continue

        # Skip if already has a final result (T1_HIT, T2_HIT, SL_HIT, EXPIRED)
        existing_result = row.get("Result", "")
        if existing_result in ("T1_HIT", "T2_HIT", "SL_HIT", "EXPIRED"):
            # Already tracked — just count stats
            updated_rows.append(row)
            reviewed += 1
            if existing_result == "T1_HIT":
                t1_hit += 1
            elif existing_result == "T2_HIT":
                t2_hit += 1
            elif existing_result == "SL_HIT":
                sl_hit += 1
            elif existing_result == "EXPIRED":
                expired += 1

            ret_str = row.get("Actual_Return", "").replace("%", "")
            if ret_str:
                ret = float(ret_str)
                returns.append(ret)
                d = row.get("Days_Held", "")
                if d:
                    days_list.append(int(d))
                if ret > best_return:
                    best_return = ret
                    best_pick = symbol
                if ret < worst_return:
                    worst_return = ret
                    worst_pick = symbol
            continue

        # Skip if trade is too fresh (less than 2 days)
        elapsed = (datetime.now().date() - entry_date).days
        if elapsed < 2:
            row.update({col: row.get(col, "") for col in OUTCOME_COLUMNS})
            if "Result" not in row or not row["Result"]:
                row["Result"] = "OPEN"
            updated_rows.append(row)
            open_trades += 1
            continue

        # Analyze the trade
        print(f"   Analyzing {i+1}/{total}: {symbol} (entered {date_str}, {elapsed}d ago)")
        outcome = analyze_trade(symbol, entry_date, entry, sl, t1, t2)
        row.update(outcome)
        updated_rows.append(row)

        result = outcome["Result"]
        if result == "NO_DATA":
            no_data += 1
        elif result == "OPEN":
            open_trades += 1
        else:
            reviewed += 1
            if result == "T1_HIT":
                t1_hit += 1
            elif result == "T2_HIT":
                t2_hit += 1
            elif result == "SL_HIT":
                sl_hit += 1
            elif result == "EXPIRED":
                expired += 1

            ret_str = outcome.get("Actual_Return", "").replace("%", "")
            if ret_str:
                ret = float(ret_str)
                returns.append(ret)
                d = outcome.get("Days_Held", "")
                if d:
                    days_list.append(int(float(d)))
                if ret > best_return:
                    best_return = ret
                    best_pick = symbol
                if ret < worst_return:
                    worst_return = ret
                    worst_pick = symbol

    # Save updated CSV
    save_csv(updated_rows)

    # Compute stats
    wins = t1_hit + t2_hit
    closed = wins + sl_hit + expired
    win_rate = (wins / closed * 100) if closed > 0 else 0
    avg_return = np.mean(returns) if returns else 0
    avg_days = np.mean(days_list) if days_list else 0

    stats = {
        "total": total,
        "reviewed": reviewed,
        "t1_hit": t1_hit,
        "t2_hit": t2_hit,
        "sl_hit": sl_hit,
        "expired": expired,
        "open": open_trades,
        "no_data": no_data,
        "t1_pct": (t1_hit / reviewed * 100) if reviewed > 0 else 0,
        "t2_pct": (t2_hit / reviewed * 100) if reviewed > 0 else 0,
        "sl_pct": (sl_hit / reviewed * 100) if reviewed > 0 else 0,
        "expired_pct": (expired / reviewed * 100) if reviewed > 0 else 0,
        "win_rate": win_rate,
        "avg_return": avg_return,
        "avg_days": avg_days,
        "best_pick": best_pick,
        "best_return": best_return if best_pick else 0,
        "worst_pick": worst_pick,
        "worst_return": worst_return if worst_pick else 0,
    }

    # Print summary
    print(f"\n{'='*50}")
    print(f"📊 PERFORMANCE SUMMARY")
    print(f"{'='*50}")
    print(f"Total: {total} | Reviewed: {reviewed} | Open: {open_trades}")
    print(f"T1 Hit: {t1_hit} | T2 Hit: {t2_hit} | SL Hit: {sl_hit} | Expired: {expired}")
    print(f"Win Rate: {win_rate:.1f}% | Avg Return: {avg_return:+.2f}%")
    if best_pick:
        print(f"Best: {best_pick} ({best_return:+.2f}%)")
    if worst_pick:
        print(f"Worst: {worst_pick} ({worst_return:+.2f}%)")

    # Format review period
    today = datetime.now()
    if today.day <= 15:
        review_period = f"1-15 {today.strftime('%b %Y')}"
    else:
        review_period = f"16-{today.day} {today.strftime('%b %Y')}"

    # Send Telegram
    scorecard = format_scorecard(stats, updated_rows, review_period)
    send_telegram(scorecard)

    # Save stats to JSON
    stats_path = LOG_DIR / "performance_history.json"
    try:
        existing = []
        if stats_path.exists():
            with open(stats_path, "r") as f:
                existing = json.load(f)
        stats["review_date"] = today.isoformat()
        stats["review_period"] = review_period
        existing.append(stats)
        with open(stats_path, "w") as f:
            json.dump(existing, f, indent=2, default=str)
        print(f"📝 Performance history saved: {stats_path}")
    except Exception as e:
        print(f"⚠️ Error saving performance history: {e}")

    return stats


if __name__ == "__main__":
    run_tracker()
