#!/usr/bin/env python3
"""
Quantex Pro Scanner — Multi-Factor Confluence Swing Scanner for NSE/BSE
========================================================================
Upgraded 7-layer version with:
  - ADX trend-strength gate (Layer 1)
  - Multi-timeframe (weekly) confirmation (Layer 7)
  - Dynamic market-regime filter (Nifty 50-DMA + India VIX)

Scores stocks out of 100 across 7 layers:
  Layer 1: Trend Alignment (20 pts) — EMA stack (10) + Supertrend (4) + ADX (6)
  Layer 2: Momentum (18 pts) — RSI power zone (9) + MACD crossover (9)
  Layer 3: Volume & Delivery (17 pts) — Volume surge (9) + Delivery proxy (8)
  Layer 4: Price Action & Breakout (13 pts) — 20D breakout (8) + Candle patterns (5)
  Layer 5: Relative Strength (12 pts) — Stock vs Nifty RS (8) + Sector momentum (4)
  Layer 6: OI / Smart Money (10 pts) — Long buildup / short covering + F&O activity
  Layer 7: Multi-Timeframe (10 pts) — Weekly EMA stack (5) + RSI > 50 (3) + MACD (2)

Global market regime filter (Nifty vs 50DMA + India VIX) dynamically sets
MIN_SCORE_THRESHOLD: 60 in bullish, 68 in neutral, 75 in bearish regime.

Outputs top 10 stocks/ETFs with Entry, SL, Target1, Target2, Score, and Hold period.
Sends results to Telegram.
"""

import csv
import json
import math
import os
import sys
import traceback
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import ta
import yfinance as yf

try:
    import pyotp
    HAS_PYOTP = True
except ImportError:
    HAS_PYOTP = False

# ─────────────────────────── CONFIGURATION ───────────────────────────

# Telegram — reads from GitHub Secrets, no hardcoded defaults
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_SIGNAL_GROUP = os.environ.get("TELEGRAM_SIGNAL_GROUPS", "").strip()
TELEGRAM_ADMIN_GROUP = os.environ.get("TELEGRAM_ADMIN_GROUPS", "").strip()

# ── ZERODHA KITE API (for live quotes & OI data) ──
KITE_API_KEY = os.environ.get("KITE_API_KEY", "").strip()
KITE_API_SECRET = os.environ.get("KITE_API_SECRET", "").strip()
ZERODHA_USER_ID = os.environ.get("ZERODHA_USER_ID", "").strip()
ZERODHA_PASSWORD = os.environ.get("ZERODHA_PASSWORD", "").strip()
ZERODHA_TOTP_KEY = os.environ.get("ZERODHA_TOTP_KEY", "").strip()

MIN_SCORE_THRESHOLD = 60  # Minimum score to qualify

# ── PRICE FILTERS ──
STOCK_PRICE_MIN = 100
STOCK_PRICE_MAX = 8000
ETF_PRICE_MIN = 10

ETF_SYMBOLS = {
    # Gold ETFs
    "GOLDBEES", "SETFGOLD", "HDFCGOLD", "AXISGOLD", "BSLGOLDETF",
    "LICMFGOLD", "QGOLDHALF", "GOLDETF", "GOLDCASE", "MOGSEC",
    "EGOLD", "TATAGOLD",
    # Silver ETFs
    "SILVERBEES", "SBISILVER", "HDFCSILVER", "SILVERCASE", "MOSILVER",
    # Index ETFs
    "NIFTYBEES", "BANKBEES", "JUNIORBEES",
}

# ──────────────────────────────────────────────────────────────────────────────
# STOCK UNIVERSE — Nifty 50 + Nifty Next 50 + Nifty Midcap 100 + Nifty 500
# Total: ~325 unique stocks
# ──────────────────────────────────────────────────────────────────────────────
STOCK_UNIVERSE = [
    # ── NIFTY 50 ──
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR",
    "SBIN", "BHARTIARTL", "KOTAKBANK", "ITC", "LT", "AXISBANK",
    "BAJFINANCE", "ASIANPAINT", "MARUTI", "HCLTECH", "SUNPHARMA",
    "TATAMOTOR", "TITAN", "WIPRO", "ULTRACEMCO", "NESTLEIND",
    "NTPC", "POWERGRID", "M&M", "TECHM", "TATASTEEL", "ONGC",
    "BAJAJFINSV", "ADANIENT", "ADANIPORTS", "JSWSTEEL", "COALINDIA",
    "GRASIM", "BPCL", "CIPLA", "DRREDDY", "DIVISLAB", "EICHERMOT",
    "HEROMOTOCO", "INDUSINDBK", "SBILIFE", "HDFCLIFE", "BRITANNIA",
    "APOLLOHOSP", "TATACONSUM", "HINDALCO", "BAJAJ-AUTO", "SHRIRAMFIN", "BEL",

    # ── NIFTY NEXT 50 ──
    "ADANIGREEN", "ADANIPOWER", "AMBUJACEM", "ATGL", "AWL", "BANKBARODA",
    "BOSCHLTD", "CANBK", "CHOLAFIN", "COLPAL", "CONCOR", "DABUR", "DLF",
    "GAIL", "GODREJCP", "HAVELLS", "ICICIGI", "ICICIPRULI", "IDEA", "INDIGO",
    "INDUSTOWER", "IOC", "IRCTC", "JIOFIN", "JSWENERGY", "LICI", "LODHA",
    "LTIM", "MARICO", "MAXHEALTH", "MUTHOOTFIN", "NAUKRI", "NHPC", "OBEROIRLTY",
    "OFSS", "PAYTM", "PERSISTENT", "PETRONET", "PIDILITIND", "PFC", "PIIND",
    "PNB", "POLYCAB", "RECLTD", "SBICARD", "SIEMENS", "SJVN", "SRF",
    "TATAPOWER", "TRENT", "VEDL", "ZOMATO", "ZYDUSLIFE",

    # ── DEFENCE SECTOR ──
    "HAL", "BDL", "MAZDOCK", "COCHINSHIP", "GRSE", "DATAPATTNS",
    "BEML", "MIDHANI", "SOLARINDS", "PARAS",
    "ASTRAMICRO", "ZENTEC", "AVANTEL", "IDEAFORGE", "GARUDA",
    "DCXINDIA", "GANDHAR", "PREMEXPLN", "JNKINDIA", "CEIGALL",

    # ── AUTO & AUTO ANCILLARY ──
    "TVSMOTOR", "ASHOKLEY", "BALKRISIND", "ENDURANCE", "EXIDEIND",
    "MOTHERSON", "SCHAEFFLER", "TIMKEN", "SONACOMS", "BOSCHLTD",
    "MRF", "APOLLOTYRE", "BHARATFORG", "CRAFTSMAN",
    "AMARAJABAT", "SUNDRMFAST", "SUPRAJIT", "UNOMINDA", "LUMAXTECH",
    "OLECTRA", "GREAVES",

    # ── ENERGY / OIL & GAS ──
    "HINDPETRO", "MRPL", "GSPL", "GUJGASLTD", "IGL",
    "ADANIENSOL", "GPPL",

    # ── POWER & RENEWABLES ──
    "TORNTPOWER", "CESC", "JSWENERGY", "SUZLON", "SWSOLAR",
    "IREDA", "HUDCO", "NLCINDIA", "INOXWIND", "WAABORIG",
    "KPIGREEN", "JSWHLDGS",

    # ── GOLD ETFs ──
    "GOLDBEES", "SETFGOLD", "HDFCGOLD", "AXISGOLD", "BSLGOLDETF",
    "LICMFGOLD", "QGOLDHALF", "GOLDETF", "GOLDCASE", "MOGSEC",
    "EGOLD", "TATAGOLD",

    # ── SILVER ETFs ──
    "SILVERBEES", "SBISILVER", "HDFCSILVER", "SILVERCASE", "MOSILVER",

    # ── INDEX / SECTOR ETFs ──
    "NIFTYBEES", "BANKBEES", "JUNIORBEES",

    # ── GOLD & SILVER PLAYS (stocks) ──
    "GOLDIAM", "RAJESHEXPO",

    # ── PSU BANKS ──
    "UNIONBANK", "INDIANB", "CENTRALBK", "MAHABANK", "UCOBANK",
    "IOB", "BANKINDIA", "IDBI", "CANFINHOME",

    # ── RAILWAY & INFRA PSU ──
    "IRFC", "RVNL", "IRCON", "RAILTEL", "RITES", "CGPOWER",

    # ── IT & TECH ──
    "LTTS", "COFORGE", "MPHASIS", "TATAELXSI", "KPITTECH", "CYIENT",
    "HAPPSTMNDS", "MASTEK", "LATENTVIEW", "SONATSOFTW", "ECLERX",
    "INTELLECT", "TANLA", "TATATECH", "ZENSARTECH", "SASKEN",
    "NETWEB", "ROUTE",

    # ── PHARMA & HEALTHCARE ──
    "AUROPHARMA", "BIOCON", "TORNTPHARM", "LUPIN", "ALKEM", "IPCALAB",
    "LAURUSLABS", "METROPOLIS", "FORTIS", "SYNGENE", "NATCOPHARM",
    "GRANULES", "GLENMARK", "AJANTPHARM", "SUVENPHAR", "JBCHEPHARM",
    "GLAXO", "SANOFI", "ABBOT",

    # ── METALS & MINING ──
    "SAIL", "NMDC", "NATIONALUM", "HINDCOPPER", "HINDZINC",
    "JSL", "WELCORP", "RATNAMANI", "APLAPOLLO",

    # ── CHEMICALS ──
    "DEEPAKNTR", "ATUL", "CLEAN", "FLUOROCHEM", "PIIND",
    "NAVINFLUOR", "SUMICHEM", "FINEORG", "CHAMBLFERT", "GNFC",
    "DCMSHRIRAM", "VINATIORGA", "GALAXYSURF",

    # ── FMCG & CONSUMER ──
    "PAGEIND", "DMART", "DEVYANI", "JUBLFOOD", "UBL", "RADICO",
    "BATAINDIA", "RELAXO", "EMAMILTD", "HATSUN", "PATANJALI",
    "KALYANKJIL", "PVRINOX", "SUNTV",

    # ── REALTY ──
    "GODREJPROP", "PRESTIGE", "BRIGADE", "PHOENIXLTD", "CHALET",

    # ── FINANCE / NBFC / INSURANCE ──
    "MANAPPURAM", "IIFL", "ABCAPITAL", "LICHSGFIN", "MFSL",
    "SUNDARMFIN", "POONAWALLA", "STARHEALTH", "POLICYBZR",
    "ANGELONE", "MCX", "CDSL", "BSE", "IEX", "CRISIL",
    "MOTILALOFS", "HDFCAMC", "JMFINANCIL",

    # ── ELECTRICALS / ELECTRONICS / CAPITAL GOODS ──
    "CROMPTON", "DIXON", "KAYNES", "VOLTAS", "BERGEPAINT",
    "ABB", "CUMMINSIND", "THERMAX", "KIRLOSENG", "KEI", "VGUARD",
    "BLUESTARCO", "KAJARIACER", "KANSAINER", "CENTURYPLY",
    "SUPREMEIND", "GRINDWELL", "CARBORUNIV", "LAXMIMACH",

    # ── TELECOM & MEDIA ──
    "BHARTIARTL", "INDUSTOWER", "IDEA", "TATACOMM", "ZEEL",

    # ── MISCELLANEOUS NIFTY 500 ──
    "3MINDIA", "AARTIIND", "AAVAS", "ACC", "ABSLAMC", "APARINDS",
    "ASTRAL", "BASF", "BECTORFOOD", "CASTROLIND", "CUB",
    "DELTACORP", "EQUITASBNK", "FEDERALBNK", "IDFCFIRSTB", "BANDHANBNK",
    "AUBANK", "RBLBANK", "FSL", "GILLETTE", "GMRAIRPORT",
    "HONAUT", "ICRA", "IIFLWAM", "INDIAMART", "INDIANHOTELS",
    "JKCEMENT", "RAMCOCEM", "HEIDELBERG", "KRBL", "POLYMED",
    "PRSMJOHNSN", "QUESS", "RAJESHEXPO", "TATACHEM",
    "TIINDIA", "TRIDENT", "UNITDSPR", "UPL", "WHIRLPOOL",
    "YESBANK",
]

# Remove duplicates while preserving order
seen = set()
STOCK_UNIVERSE = [s for s in STOCK_UNIVERSE if not (s in seen or seen.add(s))]

# ──────────────────────────────────────────────────────────────────────────────
# SECTOR MAP — 18 sectors for momentum ranking
# ──────────────────────────────────────────────────────────────────────────────
SECTOR_MAP = {
    "IT": ["TCS", "INFY", "HCLTECH", "WIPRO", "TECHM", "LTIM", "LTTS",
           "PERSISTENT", "COFORGE", "MPHASIS", "TATAELXSI", "KPITTECH",
           "CYIENT", "HAPPSTMNDS", "MASTEK", "ECLERX", "TATATECH",
           "ZENSARTECH", "SASKEN", "NETWEB", "ROUTE"],

    "BANKING": ["HDFCBANK", "ICICIBANK", "SBIN", "KOTAKBANK", "AXISBANK",
                "BANKBARODA", "CANBK", "PNB", "INDUSINDBK", "FEDERALBNK",
                "IDFCFIRSTB", "BANDHANBNK", "AUBANK", "RBLBANK",
                "UNIONBANK", "INDIANB", "CENTRALBK", "MAHABANK", "UCOBANK",
                "IOB", "BANKINDIA", "IDBI", "YESBANK"],

    "FINANCE": ["BAJFINANCE", "BAJAJFINSV", "SBILIFE", "HDFCLIFE", "CHOLAFIN",
                "MUTHOOTFIN", "MANAPPURAM", "SBICARD", "SHRIRAMFIN", "ABCAPITAL",
                "LICI", "RECLTD", "PFC", "IRFC", "JIOFIN", "LICHSGFIN",
                "MFSL", "SUNDARMFIN", "POONAWALLA", "STARHEALTH", "POLICYBZR",
                "ANGELONE", "MCX", "CDSL", "BSE", "IEX", "CRISIL",
                "MOTILALOFS", "HDFCAMC", "CANFINHOME", "IIFL"],

    "PHARMA": ["SUNPHARMA", "CIPLA", "DRREDDY", "DIVISLAB", "AUROPHARMA",
               "BIOCON", "TORNTPHARM", "LUPIN", "ALKEM", "IPCALAB", "LAURUSLABS",
               "METROPOLIS", "FORTIS", "SYNGENE", "NATCOPHARM", "GRANULES",
               "GLENMARK", "AJANTPHARM", "SUVENPHAR", "JBCHEPHARM",
               "GLAXO", "SANOFI", "ABBOT", "ZYDUSLIFE"],

    "DEFENCE": ["HAL", "BEL", "BDL", "MAZDOCK", "COCHINSHIP", "GRSE",
                "DATAPATTNS", "BEML", "MIDHANI", "SOLARINDS", "PARAS",
                "ASTRAMICRO", "ZENTEC", "AVANTEL", "IDEAFORGE", "GARUDA",
                "DCXINDIA", "GANDHAR", "PREMEXPLN", "JNKINDIA", "CEIGALL"],

    "AUTO": ["TATAMOTOR", "MARUTI", "M&M", "BAJAJ-AUTO", "EICHERMOT",
             "HEROMOTOCO", "TVSMOTOR", "ASHOKLEY", "BALKRISIND", "ENDURANCE",
             "EXIDEIND", "SCHAEFFLER", "TIMKEN", "SONACOMS", "BOSCHLTD", "MRF",
             "MOTHERSON", "BHARATFORG", "AMARAJABAT", "SUNDRMFAST", "SUPRAJIT",
             "UNOMINDA", "LUMAXTECH", "GREAVES", "APOLLOTYRE", "CRAFTSMAN"],

    "METALS": ["TATASTEEL", "JSWSTEEL", "HINDALCO", "VEDL", "SAIL", "NMDC",
               "NATIONALUM", "HINDCOPPER", "HINDZINC", "JSL", "WELCORP",
               "RATNAMANI", "APLAPOLLO"],

    "ENERGY": ["RELIANCE", "ONGC", "BPCL", "IOC", "GAIL", "PETRONET", "IGL",
               "HINDPETRO", "MRPL", "GSPL", "GUJGASLTD", "GPPL",
               "COALINDIA", "ATGL"],

    "POWER": ["NTPC", "POWERGRID", "TATAPOWER", "ADANIGREEN", "NHPC", "SJVN",
              "ADANIPOWER", "TORNTPOWER", "CESC", "JSWENERGY", "SUZLON",
              "OLECTRA", "SWSOLAR", "IREDA", "HUDCO", "NLCINDIA",
              "INOXWIND", "KPIGREEN"],

    "GOLD_ETF": ["GOLDBEES", "SETFGOLD", "HDFCGOLD", "AXISGOLD", "BSLGOLDETF",
                 "LICMFGOLD", "QGOLDHALF", "GOLDETF", "GOLDCASE", "MOGSEC",
                 "EGOLD", "TATAGOLD", "GOLDIAM"],

    "SILVER_ETF": ["SILVERBEES", "SBISILVER", "HDFCSILVER", "SILVERCASE", "MOSILVER"],

    "INDEX_ETF": ["NIFTYBEES", "BANKBEES", "JUNIORBEES"],

    "FMCG": ["HINDUNILVR", "ITC", "NESTLEIND", "BRITANNIA", "TATACONSUM",
             "GODREJCP", "DABUR", "MARICO", "COLPAL", "EMAMILTD", "HATSUN",
             "PATANJALI", "BATAINDIA", "RELAXO", "UBL", "RADICO"],

    "REALTY": ["DLF", "GODREJPROP", "OBEROIRLTY", "PRESTIGE", "LODHA",
              "BRIGADE", "PHOENIXLTD", "CHALET"],

    "INFRA": ["LT", "ADANIENT", "ADANIPORTS", "CONCOR", "SIEMENS", "ABB",
              "BHEL", "CGPOWER", "CUMMINSIND", "THERMAX", "KEC", "KIRLOSENG",
              "LAXMIMACH"],

    "RAILWAY": ["IRCTC", "IRFC", "RVNL", "IRCON", "RAILTEL", "RITES", "CONCOR"],

    "CONSUMER": ["TITAN", "TRENT", "DMART", "ZOMATO", "NAUKRI", "INDIANHOTELS",
                 "APOLLOHOSP", "MAXHEALTH", "PAGEIND", "KALYANKJIL",
                 "DEVYANI", "JUBLFOOD", "PVRINOX", "SUNTV"],

    "CHEMICALS": ["PIDILITIND", "DEEPAKNTR", "SRF", "FLUOROCHEM", "ATUL", "CLEAN",
                  "PIIND", "NAVINFLUOR", "SUMICHEM", "FINEORG", "CHAMBLFERT",
                  "GNFC", "DCMSHRIRAM", "VINATIORGA", "GALAXYSURF"],

    "ELECTRICALS": ["HAVELLS", "CROMPTON", "DIXON", "KAYNES", "POLYCAB", "VOLTAS",
                    "BERGEPAINT", "ASIANPAINT", "KEI", "VGUARD", "BLUESTARCO",
                    "KAJARIACER", "SUPREMEIND", "GRINDWELL", "CARBORUNIV"],
}

# Reverse mapping: stock -> sector
STOCK_SECTOR = {}
for sector, stocks in SECTOR_MAP.items():
    for s in stocks:
        STOCK_SECTOR[s] = sector


# ─────────────────────────── KITE API HELPERS ───────────────────────────

class KiteSession:
    """
    Manages Kite Connect session for live market data.
    Automated login: credentials + TOTP -> request_token -> access_token.
    Used for: live LTP, historical OHLCV, quotes with volume, OI data.
    NOT used for: placing orders, modifying positions, or any trading actions.
    """

    def __init__(self):
        self.kite = None
        self.logged_in = False
        self.instrument_map = {}
        self.nfo_futures = []

    def login(self):
        """Full automated Kite login: login -> TOTP -> request_token -> session."""
        try:
            from kiteconnect import KiteConnect
            import pyotp
            import urllib.parse

            session = requests.Session()

            r1 = session.post("https://kite.zerodha.com/api/login",
                              data={"user_id": ZERODHA_USER_ID, "password": ZERODHA_PASSWORD},
                              timeout=15)
            if r1.json().get("status") != "success":
                print("   Kite login failed (credentials)")
                return False
            request_id = r1.json()["data"]["request_id"]

            totp = pyotp.TOTP(ZERODHA_TOTP_KEY)
            r2 = session.post("https://kite.zerodha.com/api/twofa", data={
                "user_id": ZERODHA_USER_ID, "request_id": request_id,
                "twofa_value": totp.now(), "twofa_type": "totp",
            }, timeout=15)
            if r2.json().get("status") != "success":
                print("   Kite TOTP failed")
                return False

            r3 = session.get(
                f"https://kite.zerodha.com/connect/login?v=3&api_key={KITE_API_KEY}",
                allow_redirects=False, timeout=15)
            location = r3.headers.get("Location", "")
            if "connect/finish" in location:
                r3b = session.get(location, allow_redirects=False, timeout=15)
                location = r3b.headers.get("Location", location)

            parsed = urllib.parse.urlparse(location)
            params = urllib.parse.parse_qs(parsed.query)
            request_token = params.get("request_token", [None])[0]
            if not request_token:
                print("   Kite: could not get request_token")
                return False

            self.kite = KiteConnect(api_key=KITE_API_KEY)
            data = self.kite.generate_session(request_token, api_secret=KITE_API_SECRET)
            self.kite.set_access_token(data["access_token"])
            self.logged_in = True

            profile = self.kite.profile()
            print(f"   Kite logged in: {profile['user_name']} ({profile['user_id']})")

            self._load_instruments()
            return True

        except ImportError:
            print("   kiteconnect not installed -- falling back to Yahoo Finance")
            return False
        except Exception as e:
            print(f"   Kite login failed ({e}) -- falling back to Yahoo Finance")
            return False

    def _load_instruments(self):
        """Load NSE instrument tokens + NFO futures for OI data."""
        try:
            nse_instruments = self.kite.instruments("NSE")
            self.instrument_map = {i["tradingsymbol"]: i["instrument_token"] for i in nse_instruments}
            print(f"   Loaded {len(self.instrument_map)} NSE instruments")

            from datetime import date
            nfo_instruments = self.kite.instruments("NFO")
            today = date.today()
            self.nfo_futures = [
                i for i in nfo_instruments
                if i["instrument_type"] == "FUT" and i["expiry"] >= today
            ]
            print(f"   Loaded {len(self.nfo_futures)} active NFO futures")
        except Exception as e:
            print(f"   Instrument load failed: {e}")

    def get_token(self, symbol):
        return self.instrument_map.get(symbol)

    def get_historical(self, symbol, days=90):
        if not self.logged_in:
            return None
        token = self.get_token(symbol)
        if not token:
            return None
        try:
            from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            to_date = datetime.now().strftime("%Y-%m-%d")
            hist = self.kite.historical_data(token, from_date, to_date, interval="day")
            if not hist:
                return None
            df = pd.DataFrame(hist)
            df.set_index("date", inplace=True)
            df.index = pd.to_datetime(df.index)
            df.rename(columns={"open": "Open", "high": "High", "low": "Low",
                               "close": "Close", "volume": "Volume"}, inplace=True)
            return df
        except Exception:
            return None

    def get_ltp_bulk(self, symbols):
        if not self.logged_in:
            return {}
        try:
            instruments = [f"NSE:{s}" for s in symbols if self.get_token(s)]
            result = {}
            for i in range(0, len(instruments), 200):
                batch = instruments[i:i+200]
                ltps = self.kite.ltp(batch)
                for inst, data in ltps.items():
                    sym = inst.replace("NSE:", "")
                    result[sym] = data["last_price"]
            return result
        except Exception:
            return {}

    def get_quote(self, symbol):
        if not self.logged_in:
            return None
        try:
            q = self.kite.quote([f"NSE:{symbol}"])
            return q.get(f"NSE:{symbol}")
        except Exception:
            return None

    def get_oi_data(self, symbol):
        if not self.logged_in or not self.nfo_futures:
            return None
        try:
            matching = [f for f in self.nfo_futures if f["name"] == symbol]
            if not matching:
                return None
            matching.sort(key=lambda x: x["expiry"])
            nearest = matching[0]
            fut_symbol = f"NFO:{nearest['tradingsymbol']}"
            q = self.kite.quote([fut_symbol])
            data = q.get(fut_symbol, {})
            return {
                "oi": data.get("oi", 0),
                "oi_day_high": data.get("oi_day_high", 0),
                "oi_day_low": data.get("oi_day_low", 0),
                "volume": data.get("volume", 0),
                "last_price": data.get("last_price", 0),
            }
        except Exception:
            return None


# Global Kite session instance
kite_session = KiteSession()


# ─────────────────────────── HELPER FUNCTIONS ───────────────────────────

def fetch_stock_data(symbol, period="3mo", interval="1d"):
    """Fetch OHLCV data. Kite API first, Yahoo Finance fallback."""
    if kite_session.logged_in:
        df = kite_session.get_historical(symbol, days=90)
        if df is not None and len(df) >= 20:
            return df

    ticker = f"{symbol}.NS"
    try:
        df = yf.download(ticker, period=period, interval=interval, progress=False, timeout=15)
        if df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.dropna(inplace=True)
        return df
    except Exception:
        return None


def fetch_nifty_data(period="3mo"):
    """Fetch Nifty 50 index data. Kite first, Yahoo fallback."""
    if kite_session.logged_in:
        token = kite_session.instrument_map.get("NIFTY 50")
        if not token:
            for key in kite_session.instrument_map:
                if "NIFTY" in key and "BANK" not in key and "50" in key:
                    token = kite_session.instrument_map[key]
                    break
        if token:
            try:
                from_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
                to_date = datetime.now().strftime("%Y-%m-%d")
                hist = kite_session.kite.historical_data(token, from_date, to_date, interval="day")
                if hist:
                    df = pd.DataFrame(hist)
                    df.set_index("date", inplace=True)
                    df.rename(columns={"open": "Open", "high": "High", "low": "Low",
                                       "close": "Close", "volume": "Volume"}, inplace=True)
                    if len(df) >= 20:
                        return df
            except Exception:
                pass

    try:
        df = yf.download("^NSEI", period=period, interval="1d", progress=False, timeout=15)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df
    except Exception:
        return None


def compute_supertrend(df, atr_period=10, multiplier=3):
    """Compute Supertrend indicator."""
    hl2 = (df["High"] + df["Low"]) / 2
    atr = ta.volatility.AverageTrueRange(df["High"], df["Low"], df["Close"], window=atr_period).average_true_range()

    upper_band = hl2 + (multiplier * atr)
    lower_band = hl2 - (multiplier * atr)

    supertrend = pd.Series(index=df.index, dtype=float)
    direction = pd.Series(index=df.index, dtype=float)

    supertrend.iloc[0] = upper_band.iloc[0]
    direction.iloc[0] = -1

    for i in range(1, len(df)):
        if df["Close"].iloc[i] > upper_band.iloc[i - 1]:
            direction.iloc[i] = 1
        elif df["Close"].iloc[i] < lower_band.iloc[i - 1]:
            direction.iloc[i] = -1
        else:
            direction.iloc[i] = direction.iloc[i - 1]

        if direction.iloc[i] == 1:
            supertrend.iloc[i] = lower_band.iloc[i]
        else:
            supertrend.iloc[i] = upper_band.iloc[i]

    return supertrend, direction


def compute_adx(df, period=14):
    """Compute ADX -- trend strength meter. ADX > 25 = strong trend."""
    try:
        adx_ind = ta.trend.ADXIndicator(df["High"], df["Low"], df["Close"], window=period)
        return adx_ind.adx()
    except Exception:
        return None


def resample_to_weekly(df):
    """Resample daily OHLCV to weekly bars."""
    try:
        weekly = df.resample("W").agg({
            "Open": "first", "High": "max", "Low": "min",
            "Close": "last", "Volume": "sum",
        }).dropna()
        return weekly
    except Exception:
        return None


def check_weekly_trend(df):
    """Multi-timeframe confirmation: weekly EMA stack + RSI + MACD. Returns 0-10 score."""
    empty = {"ema_bullish": False, "rsi_above_50": False,
             "macd_bullish": False, "rsi": 0, "score": 0}
    weekly = resample_to_weekly(df)
    if weekly is None or len(weekly) < 20:
        return empty

    wclose = weekly["Close"]
    try:
        w_ema5 = ta.trend.EMAIndicator(wclose, window=5).ema_indicator()
        w_ema10 = ta.trend.EMAIndicator(wclose, window=10).ema_indicator()
        w_ema20 = ta.trend.EMAIndicator(wclose, window=20).ema_indicator()
        w_rsi = ta.momentum.RSIIndicator(wclose, window=14).rsi()
        w_macd = ta.trend.MACD(wclose, window_slow=26, window_fast=12, window_sign=9)
    except Exception:
        return empty

    wc = float(wclose.iloc[-1])
    ema_bullish = (wc > float(w_ema5.iloc[-1]) > float(w_ema10.iloc[-1]) > float(w_ema20.iloc[-1]))
    rsi_val = float(w_rsi.iloc[-1])
    rsi_above_50 = rsi_val > 50
    macd_bullish = float(w_macd.macd().iloc[-1]) > float(w_macd.macd_signal().iloc[-1])

    score = 0
    if ema_bullish:
        score += 5
    if rsi_above_50:
        score += 3
    if macd_bullish:
        score += 2

    return {
        "ema_bullish": ema_bullish,
        "rsi_above_50": rsi_above_50,
        "macd_bullish": macd_bullish,
        "rsi": rsi_val,
        "score": score,
    }


def fetch_india_vix():
    """Fetch latest India VIX close."""
    try:
        df = yf.download("^INDIAVIX", period="1mo", interval="1d", progress=False, timeout=15)
        if df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return float(df["Close"].iloc[-1])
    except Exception:
        return None


def compute_market_regime(nifty_df, vix_val):
    """
    Determine market regime from Nifty-vs-50DMA and India VIX.
    BULLISH : Nifty > 50DMA AND VIX <= 18 -> min_score 60
    BEARISH : Nifty < 50DMA AND VIX > 18  -> min_score 75
    NEUTRAL : mixed                        -> min_score 68
    """
    if nifty_df is None or len(nifty_df) < 50:
        return {"regime": "neutral", "min_score": 65,
                "label": "UNKNOWN (limited data)",
                "nifty_above_50dma": False, "vix": vix_val}

    nclose = nifty_df["Close"]
    try:
        nifty_ema50 = ta.trend.EMAIndicator(nclose, window=50).ema_indicator()
        nifty_above_50dma = float(nclose.iloc[-1]) > float(nifty_ema50.iloc[-1])
    except Exception:
        nifty_above_50dma = False

    vix_high = vix_val is not None and vix_val > 18

    if nifty_above_50dma and not vix_high:
        regime, min_score, label = "bullish", 60, "BULLISH (risk-on)"
    elif (not nifty_above_50dma) and vix_high:
        regime, min_score, label = "bearish", 75, "BEARISH (risk-off)"
    else:
        regime, min_score, label = "neutral", 68, "NEUTRAL (mixed)"

    return {
        "regime": regime,
        "min_score": min_score,
        "label": label,
        "nifty_above_50dma": nifty_above_50dma,
        "vix": vix_val,
    }


def detect_candlestick_patterns(df):
    """Detect bullish candlestick patterns on the last candle."""
    if len(df) < 3:
        return False, ""

    o, h, l, c = df["Open"].iloc[-1], df["High"].iloc[-1], df["Low"].iloc[-1], df["Close"].iloc[-1]
    po, ph, pl, pc = df["Open"].iloc[-2], df["High"].iloc[-2], df["Low"].iloc[-2], df["Close"].iloc[-2]
    body = abs(c - o)
    prev_body = abs(pc - po)
    candle_range = h - l if (h - l) > 0 else 0.01

    patterns = []

    # Bullish Engulfing
    if pc < po and c > o and c > po and o < pc:
        patterns.append("Bullish Engulfing")

    # Hammer
    lower_shadow = min(o, c) - l
    upper_shadow = h - max(o, c)
    if lower_shadow > 2 * body and upper_shadow < body * 0.5 and c > o:
        patterns.append("Hammer")

    # Morning Star (3-candle)
    if len(df) >= 3:
        ppo, ppc = df["Open"].iloc[-3], df["Close"].iloc[-3]
        if ppc < ppo and abs(pc - po) < abs(ppc - ppo) * 0.3 and c > o and c > (ppo + ppc) / 2:
            patterns.append("Morning Star")

    # Bullish Marubozu
    if c > o and upper_shadow < body * 0.1 and lower_shadow < body * 0.1:
        patterns.append("Marubozu")

    if patterns:
        return True, ", ".join(patterns)
    return False, ""


# ─────────────────────────── SCORING ENGINE ───────────────────────────

def score_stock(symbol, df, nifty_df, sector_performance):
    """Score a stock across all 7 layers. Returns dict with score breakdown."""
    result = {
        "symbol": symbol,
        "score": 0,
        "breakdown": {},
        "signals": [],
        "entry": 0,
        "sl": 0,
        "target1": 0,
        "target2": 0,
        "hold_period": "",
        "cmp": 0,
    }

    if df is None or len(df) < 50:
        return None

    close = df["Close"]
    cmp = close.iloc[-1]
    result["cmp"] = round(float(cmp), 2)

    # ──── LAYER 1: TREND ALIGNMENT (20 pts) ────
    # EMA stack (10) + Supertrend (4) + ADX (6)
    layer1 = 0
    ema9 = ta.trend.EMAIndicator(close, window=9).ema_indicator()
    ema21 = ta.trend.EMAIndicator(close, window=21).ema_indicator()
    ema50 = ta.trend.EMAIndicator(close, window=50).ema_indicator()

    e9, e21, e50 = ema9.iloc[-1], ema21.iloc[-1], ema50.iloc[-1]

    # EMA stack scoring (10 pts max)
    ema_score = 0
    if cmp > e9:
        ema_score += 3
    if e9 > e21:
        ema_score += 3
    if e21 > e50:
        ema_score += 2
    if cmp > e50:
        ema_score += 2
    layer1 += ema_score

    if ema_score >= 8:
        result["signals"].append("EMA Stacked Bullish")

    # Supertrend (4 pts)
    try:
        st, st_dir = compute_supertrend(df)
        if st_dir.iloc[-1] == 1:
            layer1 += 4
            if any(st_dir.iloc[-4:-1] == -1):
                result["signals"].append("Supertrend FLIP Bullish")
            else:
                result["signals"].append("Supertrend Bullish")
    except Exception:
        pass

    # ADX trend strength (6 pts)
    try:
        adx_series = compute_adx(df, period=14)
        if adx_series is not None and not pd.isna(adx_series.iloc[-1]):
            adx_val = float(adx_series.iloc[-1])
            if adx_val >= 25:
                layer1 += 6
                result["signals"].append(f"ADX {adx_val:.0f} (Strong Trend)")
            elif adx_val >= 20:
                layer1 += 4
                result["signals"].append(f"ADX {adx_val:.0f} (Trending)")
            elif adx_val >= 15:
                layer1 += 2
            if len(adx_series) >= 4 and adx_val > float(adx_series.iloc[-4]):
                result["signals"].append("ADX Rising")
    except Exception:
        pass

    result["breakdown"]["Trend"] = layer1

    # ──── LAYER 2: MOMENTUM (18 pts) ────
    layer2 = 0
    rsi = ta.momentum.RSIIndicator(close, window=14).rsi()
    rsi_val = rsi.iloc[-1]

    if 50 <= rsi_val <= 70:
        layer2 += 9
        result["signals"].append(f"RSI {rsi_val:.0f} (Power Zone)")
    elif 45 <= rsi_val < 50:
        layer2 += 4
    elif 70 < rsi_val <= 75:
        layer2 += 4

    macd = ta.trend.MACD(close, window_slow=26, window_fast=12, window_sign=9)
    macd_line = macd.macd()
    signal_line = macd.macd_signal()
    macd_hist = macd.macd_diff()

    if macd_line.iloc[-1] > signal_line.iloc[-1]:
        if any(macd_line.iloc[-4:-1] < signal_line.iloc[-4:-1]):
            layer2 += 9
            result["signals"].append("MACD Fresh Crossover")
        else:
            layer2 += 6
            result["signals"].append("MACD Bullish")
    elif macd_hist.iloc[-1] > macd_hist.iloc[-2]:
        layer2 += 2

    result["breakdown"]["Momentum"] = layer2

    # ──── LAYER 3: VOLUME & DELIVERY (17 pts) ────
    layer3 = 0
    vol = df["Volume"]
    vol_sma20 = vol.rolling(20).mean()

    if vol_sma20.iloc[-1] > 0:
        vol_ratio = vol.iloc[-1] / vol_sma20.iloc[-1]
        if vol_ratio >= 2.0:
            layer3 += 9
            result["signals"].append(f"Vol Surge {vol_ratio:.1f}x")
        elif vol_ratio >= 1.5:
            layer3 += 7
            result["signals"].append(f"Vol Above Avg {vol_ratio:.1f}x")
        elif vol_ratio >= 1.2:
            layer3 += 4

    close_to_high_ratio = (cmp - df["Low"].iloc[-1]) / (df["High"].iloc[-1] - df["Low"].iloc[-1] + 0.01)
    if close_to_high_ratio > 0.75 and vol.iloc[-1] > vol_sma20.iloc[-1]:
        layer3 += 8
        result["signals"].append("Strong Delivery (Close near High)")
    elif close_to_high_ratio > 0.6:
        layer3 += 4

    result["breakdown"]["Volume"] = layer3

    # ──── LAYER 4: PRICE ACTION & BREAKOUT (13 pts) ────
    layer4 = 0

    high_20 = df["High"].rolling(20).max()
    if cmp >= high_20.iloc[-2]:
        layer4 += 8
        result["signals"].append("20-Day Breakout")
    elif cmp >= high_20.iloc[-2] * 0.98:
        layer4 += 4
        result["signals"].append("Near Breakout")

    has_pattern, pattern_name = detect_candlestick_patterns(df)
    if has_pattern:
        layer4 += 5
        result["signals"].append(pattern_name)

    result["breakdown"]["Price Action"] = layer4

    # ──── LAYER 5: RELATIVE STRENGTH (12 pts) ────
    layer5 = 0

    if nifty_df is not None and len(nifty_df) >= 20:
        nifty_close = nifty_df["Close"]
        stock_ret_10 = (cmp / close.iloc[-10] - 1) * 100 if len(close) >= 10 else 0
        stock_ret_20 = (cmp / close.iloc[-20] - 1) * 100 if len(close) >= 20 else 0
        nifty_ret_10 = (nifty_close.iloc[-1] / nifty_close.iloc[-10] - 1) * 100
        nifty_ret_20 = (nifty_close.iloc[-1] / nifty_close.iloc[-20] - 1) * 100

        rs_10 = stock_ret_10 - float(nifty_ret_10)
        rs_20 = stock_ret_20 - float(nifty_ret_20)

        if rs_10 > 2 and rs_20 > 3:
            layer5 += 8
            result["signals"].append("Strong RS vs Nifty")
        elif rs_10 > 0 and rs_20 > 0:
            layer5 += 5
            result["signals"].append("Outperforming Nifty")
        elif rs_10 > 0:
            layer5 += 2

    sector = STOCK_SECTOR.get(symbol, "")
    if sector and sector in sector_performance:
        sector_rank = sector_performance[sector]
        if sector_rank <= 3:
            layer5 += 4
            result["signals"].append(f"Sector Top 3: {sector}")
        elif sector_rank <= 5:
            layer5 += 2

    result["breakdown"]["Relative Strength"] = layer5

    # ──── LAYER 6: OI / SMART MONEY (10 pts) ────
    layer6 = 0
    oi_used = False

    if kite_session.logged_in:
        oi_data = kite_session.get_oi_data(symbol)
        if oi_data and oi_data["oi"] > 0:
            oi_used = True
            oi = oi_data["oi"]
            oi_high = oi_data["oi_day_high"]
            oi_low = oi_data["oi_day_low"]
            fut_vol = oi_data["volume"]

            if oi >= oi_high * 0.9 and cmp > close.iloc[-2]:
                layer6 += 5
                result["signals"].append(f"Long Buildup (OI: {oi:,})")
            elif oi <= oi_low * 1.1 and cmp > close.iloc[-2]:
                layer6 += 4
                result["signals"].append("Short Covering")

            if fut_vol > 0:
                cash_vol = float(vol.iloc[-1]) if vol.iloc[-1] > 0 else 1
                fut_cash_ratio = fut_vol / cash_vol
                if fut_cash_ratio > 0.3:
                    layer6 += 3
                    result["signals"].append("High F&O Activity")
                elif fut_cash_ratio > 0.15:
                    layer6 += 2

            avg5 = close.iloc[-5:].mean()
            if cmp > avg5:
                layer6 += 2

    if not oi_used:
        last5_closes = close.iloc[-5:]
        last5_vols = vol.iloc[-5:]
        bullish_days = sum(1 for i in range(1, len(last5_closes)) if last5_closes.iloc[i] > last5_closes.iloc[i-1])
        vol_rising = sum(1 for i in range(1, len(last5_vols)) if last5_vols.iloc[i] > last5_vols.iloc[i-1])

        if bullish_days >= 3 and vol_rising >= 3:
            layer6 += 7
            result["signals"].append("Institutional Accumulation")
        elif bullish_days >= 3:
            layer6 += 4
            result["signals"].append("Consistent Buying")

        avg5 = close.iloc[-5:].mean()
        if cmp > avg5:
            layer6 += 3

    result["breakdown"]["Smart Money"] = layer6

    # ──── LAYER 7: MULTI-TIMEFRAME CONFIRMATION (10 pts) ────
    weekly = check_weekly_trend(df)
    layer7 = weekly["score"]

    if weekly["ema_bullish"]:
        result["signals"].append("Weekly EMA Stacked")
    if weekly["macd_bullish"] and weekly["rsi_above_50"]:
        result["signals"].append(f"Weekly MTF OK (RSI {weekly['rsi']:.0f})")

    result["breakdown"]["Multi-Timeframe"] = layer7

    # ──── TOTAL SCORE ────
    total = layer1 + layer2 + layer3 + layer4 + layer5 + layer6 + layer7
    result["score"] = total

    # ──── ENTRY, SL, TARGETS ────
    atr = ta.volatility.AverageTrueRange(df["High"], df["Low"], df["Close"], window=14).average_true_range()
    atr_val = float(atr.iloc[-1])

    result["entry"] = round(float(cmp), 2)

    swing_low = float(df["Low"].iloc[-5:].min())
    atr_sl = float(cmp) - (1.5 * atr_val)
    sl = max(swing_low, atr_sl)
    sl = max(sl, float(cmp) * 0.95)
    sl = min(sl, float(cmp) * 0.99)
    result["sl"] = round(sl, 2)

    risk = float(cmp) - sl
    result["target1"] = round(float(cmp) + (2 * risk), 2)

    next_resistance = float(df["High"].iloc[-20:].max())
    t2_rr = float(cmp) + (3 * risk)
    result["target2"] = round(max(t2_rr, next_resistance), 2)

    pct_to_t1 = (result["target1"] - float(cmp)) / float(cmp) * 100
    if pct_to_t1 < 3:
        result["hold_period"] = "1-3 days"
    elif pct_to_t1 < 6:
        result["hold_period"] = "3-7 days"
    else:
        result["hold_period"] = "1-2 weeks"

    return result


def compute_sector_performance(nifty_df):
    """Compute sector performance ranking."""
    sector_returns = {}
    for sector, stocks in SECTOR_MAP.items():
        returns = []
        for sym in stocks[:3]:
            try:
                df = fetch_stock_data(sym, period="1mo", interval="1d")
                if df is not None and len(df) >= 5:
                    ret = (float(df["Close"].iloc[-1]) / float(df["Close"].iloc[-5]) - 1) * 100
                    returns.append(ret)
            except Exception:
                continue
        if returns:
            sector_returns[sector] = np.mean(returns)

    sorted_sectors = sorted(sector_returns.items(), key=lambda x: x[1], reverse=True)
    sector_ranks = {}
    for rank, (sector, ret) in enumerate(sorted_sectors, 1):
        sector_ranks[sector] = rank

    return sector_ranks


def star_rating(score):
    if score >= 85:
        return "⭐⭐⭐⭐⭐"
    elif score >= 75:
        return "⭐⭐⭐⭐"
    elif score >= 65:
        return "⭐⭐⭐"
    elif score >= 55:
        return "⭐⭐"
    else:
        return "⭐"


def format_telegram_message(top_stocks, scan_time, regime=None):
    """Format detailed results for Personal Chat & Admin Group (top 10)."""
    now = scan_time.strftime("%d %b %Y, %I:%M %p IST")

    msg = f"🟢 *QUANTEX PRO SCANNER — {now}*\n"
    msg += f"#QuantexPro #SwingScanner #Daily\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

    if regime:
        vix_str = f"{regime['vix']:.2f}" if regime.get('vix') is not None else "N/A"
        msg += f"*Regime:* {regime['label']}  |  *VIX:* {vix_str}  |  *Threshold:* {regime['min_score']}\n"

    msg += f"_7-Layer Confluence Scanner (Top 10)_\n\n"

    if not top_stocks:
        msg += "⚠️ No stocks met the minimum score threshold today.\n"
        msg += "Market conditions may not be favorable for new swing entries.\n"
        return msg

    for i, stock in enumerate(top_stocks, 1):
        score = stock["score"]
        stars = star_rating(score)
        risk_pct = abs(stock["entry"] - stock["sl"]) / stock["entry"] * 100
        reward_pct = abs(stock["target1"] - stock["entry"]) / stock["entry"] * 100
        rr_ratio = reward_pct / risk_pct if risk_pct > 0 else 0

        msg += f"*{i}. {stock['symbol']}* — Score: *{score}/100* {stars}\n"
        msg += f"   💰 Entry: ₹{stock['entry']:.2f}\n"
        msg += f"   🛑 SL: ₹{stock['sl']:.2f} ({risk_pct:.1f}%)\n"
        msg += f"   🎯 T1: ₹{stock['target1']:.2f} ({reward_pct:.1f}%)\n"
        msg += f"   🎯 T2: ₹{stock['target2']:.2f}\n"
        msg += f"   ⏱ Hold: {stock['hold_period']} | R:R = 1:{rr_ratio:.1f}\n"

        signals = stock["signals"][:4]
        msg += f"   📊 _{', '.join(signals)}_\n"

        bd = stock["breakdown"]
        msg += f"   📈 Tr:{bd.get('Trend',0)} Mo:{bd.get('Momentum',0)} "
        msg += f"Vo:{bd.get('Volume',0)} PA:{bd.get('Price Action',0)} "
        msg += f"RS:{bd.get('Relative Strength',0)} SM:{bd.get('Smart Money',0)} "
        msg += f"MTF:{bd.get('Multi-Timeframe',0)}\n\n"

    msg += f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"⚠️ _Disclaimer: For educational purposes only. Do your own research before trading. Past patterns don't guarantee future results._\n"
    msg += f"🤖 _Powered by Quantex Pro Scanner_"

    return msg


def format_signal_group_message(top_stocks, scan_time, regime=None):
    """Format compact results for Signal Group (top 5 only)."""
    now = scan_time.strftime("%d %b %Y, %I:%M %p IST")

    msg = f"🟢 *QUANTEX PRO SCANNER* — {now}\n"
    msg += f"#QuantexPro #SwingScanner #Daily\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

    if regime:
        vix_str = f"{regime['vix']:.2f}" if regime.get('vix') is not None else "N/A"
        msg += f"*Regime:* {regime['label']}  |  *VIX:* {vix_str}\n"

    msg += f"_7-Layer Confluence Scanner (Top 5)_\n\n"

    if not top_stocks:
        msg += "⚠️ No stocks met the minimum score threshold today.\n"
        msg += "Market conditions may not be favorable for new swing entries.\n"
        return msg

    for i, stock in enumerate(top_stocks[:5], 1):
        score = stock["score"]
        stars = star_rating(score)
        risk_pct = abs(stock["entry"] - stock["sl"]) / stock["entry"] * 100
        reward_pct = abs(stock["target1"] - stock["entry"]) / stock["entry"] * 100
        rr_ratio = reward_pct / risk_pct if risk_pct > 0 else 0

        msg += f"*{i}. {stock['symbol']}* — Score: *{score}/100* {stars}\n"
        msg += f"   💰 Entry: ₹{stock['entry']:.2f}\n"
        msg += f"   🛑 SL: ₹{stock['sl']:.2f} ({risk_pct:.1f}%)\n"
        msg += f"   🎯 T1: ₹{stock['target1']:.2f} ({reward_pct:.1f}%)\n"
        msg += f"   🎯 T2: ₹{stock['target2']:.2f}\n"
        msg += f"   ⏱ Hold: {stock['hold_period']} | R:R = 1:{rr_ratio:.1f}\n\n"

    msg += f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"⚠️ _Disclaimer: For educational purposes only. Do your own research before trading. Past patterns don't guarantee future results._\n"
    msg += f"🤖 _Powered by Quantex Pro Scanner_"

    return msg


def _send_to_chat(chat_id, message, label=""):
    """Send a message to a single Telegram chat/group."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=payload, timeout=30)
        if resp.status_code == 200:
            print(f"  Sent to {label} ({chat_id})")
            return True
        else:
            print(f"  Markdown failed for {label}, retrying plain text...")
            del payload["parse_mode"]
            resp2 = requests.post(url, json=payload, timeout=30)
            if resp2.status_code == 200:
                print(f"  Sent to {label} (plain text)")
                return True
            else:
                print(f"  Failed for {label}: {resp2.status_code} -- {resp2.text}")
                return False
    except Exception as e:
        print(f"  Error sending to {label}: {e}")
        return False


def send_telegram(detail_message, signal_message=None):
    """Send messages to all configured Telegram destinations."""
    if not TELEGRAM_BOT_TOKEN:
        print("   Telegram not configured.")
        print("\n--- MESSAGE PREVIEW ---")
        print(detail_message)
        return False

    destinations = [
        (TELEGRAM_CHAT_ID, "Personal Chat", detail_message),
        (TELEGRAM_SIGNAL_GROUP, "Signal Group", signal_message or detail_message),
        (TELEGRAM_ADMIN_GROUP, "Admin Group", detail_message),
    ]

    print(">> Sending to Telegram...")
    success_count = 0
    for chat_id, label, msg in destinations:
        if chat_id and chat_id not in ("", "YOUR_CHAT_ID"):
            if _send_to_chat(chat_id, msg, label):
                success_count += 1

    print(f">> Delivered to {success_count}/{len(destinations)} destinations")
    return success_count > 0


# ─────────────────────────── RESULT LOGGING ───────────────────────────

SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = SCRIPT_DIR / "quantex_logs"


def save_scan_results(top_stocks, all_qualified_count, sector_rankings, scan_time, regime=None):
    """Save scan results to CSV, JSON (recommendations.json), and daily text file."""
    LOG_DIR.mkdir(exist_ok=True)
    daily_dir = LOG_DIR / "daily"
    daily_dir.mkdir(exist_ok=True)

    date_str = scan_time.strftime("%Y-%m-%d")
    time_str = scan_time.strftime("%I:%M %p IST")
    regime_label = regime["regime"] if regime else ""
    vix_val = regime.get("vix") if regime else None
    vix_str = f"{vix_val:.2f}" if vix_val is not None else ""
    min_score = regime.get("min_score", MIN_SCORE_THRESHOLD) if regime else MIN_SCORE_THRESHOLD

    # ── 1. CSV Log ──
    csv_path = LOG_DIR / "recommendations.csv"
    csv_exists = csv_path.exists()
    try:
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not csv_exists:
                writer.writerow([
                    "Date", "Time", "Regime", "VIX", "MinScore", "Rank", "Symbol", "Score",
                    "Trend", "Momentum", "Volume", "PriceAction", "RelStrength",
                    "SmartMoney", "MultiTimeframe",
                    "Entry", "SL", "Target1", "Target2", "RR_Ratio",
                    "Hold_Period", "Signals"
                ])
            for i, s in enumerate(top_stocks, 1):
                bd = s.get("breakdown", {})
                risk = abs(s["entry"] - s["sl"])
                reward = abs(s["target1"] - s["entry"])
                rr = f"1:{reward/risk:.1f}" if risk > 0 else "N/A"
                writer.writerow([
                    date_str, time_str, regime_label, vix_str, min_score,
                    i, s["symbol"], s["score"],
                    bd.get("Trend", 0), bd.get("Momentum", 0), bd.get("Volume", 0),
                    bd.get("Price Action", 0), bd.get("Relative Strength", 0),
                    bd.get("Smart Money", 0), bd.get("Multi-Timeframe", 0),
                    s["entry"], s["sl"], s["target1"], s["target2"], rr,
                    s["hold_period"], " | ".join(s.get("signals", []))
                ])
        print(f">> CSV log updated: {csv_path}")
    except Exception as e:
        print(f"   CSV log error: {e}")

    # ── 2. JSON Log (recommendations.json — consumed by premarket_report.py) ──
    json_path = LOG_DIR / "recommendations.json"
    try:
        existing = []
        if json_path.exists():
            with open(json_path, "r", encoding="utf-8") as f:
                existing = json.load(f)

        existing.append({
            "scan_date": date_str,
            "scan_time": scan_time.isoformat(),
            "market_regime": regime,
            "min_score_threshold": min_score,
            "total_qualified": all_qualified_count,
            "sector_rankings": sector_rankings,
            "top_10": top_stocks,
        })

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2, default=str)
        print(f">> JSON log updated: {json_path}")
    except Exception as e:
        print(f"   JSON log error: {e}")

    # ── 3. Daily Text File ──
    txt_path = daily_dir / f"{date_str}.txt"
    try:
        with open(txt_path, "a", encoding="utf-8") as f:
            f.write(f"{'='*60}\n")
            f.write(f"QUANTEX PRO SCANNER — {date_str} at {time_str}\n")
            if regime:
                f.write(f"Regime: {regime['label']} | VIX: {vix_str} | Min Score: {min_score}\n")
            f.write(f"Qualified: {all_qualified_count} stocks | Top 10 below\n")
            f.write(f"{'='*60}\n\n")
            for i, s in enumerate(top_stocks, 1):
                bd = s.get("breakdown", {})
                risk = abs(s["entry"] - s["sl"])
                reward = abs(s["target1"] - s["entry"])
                rr = f"1:{reward/risk:.1f}" if risk > 0 else "N/A"
                f.write(f"{i}. {s['symbol']} -- Score: {s['score']}/100\n")
                f.write(f"   Entry: {s['entry']:.2f} | SL: {s['sl']:.2f} | T1: {s['target1']:.2f} | T2: {s['target2']:.2f} | RR: {rr}\n")
                f.write(f"   Tr:{bd.get('Trend',0)} Mo:{bd.get('Momentum',0)} Vo:{bd.get('Volume',0)} PA:{bd.get('Price Action',0)} RS:{bd.get('Relative Strength',0)} SM:{bd.get('Smart Money',0)} MTF:{bd.get('Multi-Timeframe',0)}\n")
                f.write(f"   Hold: {s['hold_period']} | Signals: {', '.join(s.get('signals', []))}\n\n")
            f.write(f"\n{'─'*60}\n\n")
        print(f">> Daily log saved: {txt_path}")
    except Exception as e:
        print(f"   Daily log error: {e}")


# ─────────────────────────── MAIN SCANNER ───────────────────────────

def run_scanner():
    """Main scanner execution."""
    scan_time = datetime.now()
    print(f">> Quantex Pro Scanner started at {scan_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f">> Scanning {len(STOCK_UNIVERSE)} stocks + ETFs across {len(SECTOR_MAP)} sectors...\n")

    # Step 0: Login to Kite for live data
    print(">> Connecting to Kite API...")
    kite_ok = kite_session.login()
    if kite_ok:
        print(f"   Data source: Kite API (LIVE) + OI data enabled")
    else:
        print(f"   Data source: Yahoo Finance (DELAYED, no OI)")
    print()

    # Step 1: Fetch Nifty data + India VIX -> Market Regime
    print(">> Fetching Nifty 50 benchmark data...")
    nifty_df = fetch_nifty_data()

    print(">> Fetching India VIX...")
    vix_val = fetch_india_vix()

    regime = compute_market_regime(nifty_df, vix_val)
    dynamic_min_score = regime["min_score"]
    vix_str = f"{vix_val:.2f}" if vix_val is not None else "N/A"
    print(f"   Market Regime: {regime['label']}")
    print(f"   India VIX: {vix_str} | Nifty > 50DMA: {regime['nifty_above_50dma']}")
    print(f"   Dynamic MIN_SCORE_THRESHOLD: {dynamic_min_score} (base: {MIN_SCORE_THRESHOLD})\n")

    # Step 2: Compute sector performance
    print(">> Computing sector momentum rankings...")
    sector_perf = compute_sector_performance(nifty_df)
    print(f"   Top sectors: {sorted(sector_perf.items(), key=lambda x: x[1])[:5]}\n")

    # Step 3: Score all stocks
    results = []
    total = len(STOCK_UNIVERSE)
    for idx, symbol in enumerate(STOCK_UNIVERSE, 1):
        if idx % 20 == 0 or idx == total:
            print(f"   Scanning... {idx}/{total} ({symbol})")

        df = fetch_stock_data(symbol)
        if df is None or len(df) < 50:
            continue

        cmp = float(df["Close"].iloc[-1])
        if symbol in ETF_SYMBOLS:
            if cmp < ETF_PRICE_MIN:
                continue
        else:
            if cmp < STOCK_PRICE_MIN or cmp > STOCK_PRICE_MAX:
                continue

        scored = score_stock(symbol, df, nifty_df, sector_perf)
        if scored and scored["score"] >= dynamic_min_score:
            results.append(scored)

    # Step 4: Rank and pick top 10
    results.sort(key=lambda x: x["score"], reverse=True)
    top10 = results[:10]

    print(f"\n>> Scan complete! {len(results)} stocks scored >= {dynamic_min_score} [{regime['regime'].upper()}]")
    print(f">> Top {len(top10)} picks:\n")

    for i, s in enumerate(top10, 1):
        print(f"   {i}. {s['symbol']} -- Score: {s['score']}/100 -- Entry: Rs.{s['entry']}")

    # Step 5: Send to Telegram
    detail_message = format_telegram_message(top10, scan_time, regime)
    signal_message = format_signal_group_message(top10, scan_time, regime)
    send_telegram(detail_message, signal_message)

    # Step 6: Save results
    save_scan_results(top10, len(results), sector_perf, scan_time, regime)

    # Step 7: Print results JSON
    output = {
        "scan_time": scan_time.isoformat(),
        "market_regime": regime,
        "min_score_threshold": dynamic_min_score,
        "top_stocks": top10,
        "total_qualified": len(results),
        "sector_rankings": sector_perf,
    }
    print("\n>> Full results JSON:")
    print(json.dumps(output, indent=2, default=str))

    return output


if __name__ == "__main__":
    run_scanner()
