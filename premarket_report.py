#!/usr/bin/env python3
"""
Quantex Pre-Market Report — Daily Morning PDF Report at 8:00 AM IST
====================================================================
Generates a professional PDF report and sends it via Telegram covering:
1. Fear & Greed Index (computed from market indicators)
2. Global Markets — GIFT Nifty, US, Asia, Europe
3. Indian Market — Nifty, Sensex, Bank Nifty, VIX, USD/INR
4. Top Gainers & Losers (Nifty 500 sample)
5. Volume Shockers
6. 52-Week Highs
7. Sectoral Indices — 1D and 7D change
8. Motivational Quote
"""

import os, json, warnings, random, math
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf
import requests

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm, cm
from reportlab.lib.colors import HexColor, white, black
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, KeepTogether
)
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

warnings.filterwarnings("ignore")

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_SIGNAL_GROUPS = os.environ.get("TELEGRAM_SIGNAL_GROUPS", "").strip()
TELEGRAM_ADMIN_GROUPS = os.environ.get("TELEGRAM_ADMIN_GROUPS", "").strip()

BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = BASE_DIR / "quantex_logs"
LOG_DIR.mkdir(exist_ok=True)

TODAY = datetime.now().strftime("%a, %d %b %Y")
TODAY_SHORT = datetime.now().strftime("%Y-%m-%d")
TIME_NOW = datetime.now().strftime("%I:%M %p")

# Color palette
DARK_BG = HexColor("#1a1a2e")
DARK_CARD = HexColor("#16213e")
ACCENT_BLUE = HexColor("#0f3460")
ACCENT_CYAN = HexColor("#00b4d8")
GREEN = HexColor("#00c853")
RED = HexColor("#ff1744")
GOLD = HexColor("#ffd700")
LIGHT_TEXT = HexColor("#e0e0e0")
MID_TEXT = HexColor("#9e9e9e")
HEADER_BG = HexColor("#0d47a1")
ROW_ALT = HexColor("#1e2a4a")
ROW_DARK = HexColor("#152238")

# Motivational quotes
QUOTES = [
    ('"The stock market is a device for transferring money from the impatient to the patient."', "Warren Buffett"),
    ('"In investing, what is comfortable is rarely profitable."', "Robert Arnott"),
    ('"Know what you own, and know why you own it."', "Peter Lynch"),
    ('"Risk comes from not knowing what you are doing."', "Warren Buffett"),
    ('"The best investment you can make is in yourself."', "Warren Buffett"),
    ('"Bull markets are born on pessimism, grow on skepticism, mature on optimism, and die on euphoria."', "John Templeton"),
    ('"The market is never wrong; opinions often are."', "Jesse Livermore"),
    ('"Wide diversification is only required when investors do not understand what they are doing."', "Warren Buffett"),
]


# ═══════════════════════════════════════════════════════════════
# DATA FETCHING
# ═══════════════════════════════════════════════════════════════

def fetch_quote(ticker, period="5d"):
    """Fetch recent data for a ticker."""
    try:
        df = yf.download(ticker, period=period, progress=False)
        if df is not None and len(df) > 0:
            df = df.reset_index()
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            for col in ["Open", "High", "Low", "Close", "Volume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            return df
    except:
        pass
    return None


def get_change(df):
    """Get latest close, change, change%."""
    if df is None or len(df) < 2:
        return None, None, None
    close = float(df["Close"].iloc[-1])
    prev = float(df["Close"].iloc[-2])
    if np.isnan(close) or np.isnan(prev) or prev == 0:
        return None, None, None
    chg = close - prev
    chg_pct = (chg / prev) * 100
    return round(close, 2), round(chg, 2), round(chg_pct, 2)


# ── 1. GLOBAL MARKETS ──
print(">> Fetching global markets...")

GLOBAL_INDICES = {
    "Hang Seng": ("^HSI", "HK"),
    "Nikkei 225": ("^N225", "Japan"),
    "KOSPI": ("^KS200", "Korea"),
    "ASX 200": ("^AXJO", "Australia"),
    "DAX": ("^GDAXI", "Germany"),
    "FTSE 100": ("^FTSE", "UK"),
    "CAC 40": ("^FCHI", "France"),
    "Dow Jones": ("^DJI", "US"),
    "Nasdaq": ("^IXIC", "US"),
    "S&P 500": ("^GSPC", "US"),
}

INDIAN_INDICES = {
    "Sensex": "^BSESN",
    "Nifty 50": "^NSEI",
    "Bank Nifty": "^NSEBANK",
    "India VIX": "^INDIAVIX",
    "USD/INR": "USDINR=X",
}

GIFT_NIFTY_TICKERS = ["0Q0I.L", "^NSEI"]

global_data = {}
for name, (ticker, country) in GLOBAL_INDICES.items():
    df = fetch_quote(ticker)
    close, chg, pct = get_change(df)
    global_data[name] = {"close": close, "chg": chg, "pct": pct, "country": country}

indian_data = {}
for name, ticker in INDIAN_INDICES.items():
    df = fetch_quote(ticker)
    close, chg, pct = get_change(df)
    indian_data[name] = {"close": close, "chg": chg, "pct": pct}

gift_close, gift_chg, gift_pct = None, None, None
for gt in GIFT_NIFTY_TICKERS:
    gift_df = fetch_quote(gt)
    gift_close, gift_chg, gift_pct = get_change(gift_df)
    if gift_close is not None:
        break


# ── 2. FEAR & GREED INDEX ──
print(">> Computing Fear & Greed Index...")

def compute_fear_greed():
    scores = []
    nifty_df = yf.download("^NSEI", period="3mo", progress=False)
    if nifty_df is not None and len(nifty_df) > 50:
        nifty_df = nifty_df.reset_index()
        if isinstance(nifty_df.columns, pd.MultiIndex):
            nifty_df.columns = nifty_df.columns.get_level_values(0)
        nifty_df["Close"] = pd.to_numeric(nifty_df["Close"], errors="coerce")
        import ta
        ema50 = ta.trend.EMAIndicator(nifty_df["Close"], 50).ema_indicator()
        cmp = float(nifty_df["Close"].iloc[-1])
        e50 = float(ema50.iloc[-1])
        dist = (cmp - e50) / e50 * 100
        s = max(0, min(100, (dist + 5) * 10))
        scores.append(s)

    vix_data = indian_data.get("India VIX", {})
    vix = vix_data.get("close")
    if vix:
        s = max(0, min(100, (30 - vix) / 20 * 100))
        scores.append(s)

    # Market breadth: batch download Nifty 50 stocks for EMA check
    breadth_syms = ["RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR",
        "SBIN", "BHARTIARTL", "KOTAKBANK", "ITC", "LT", "AXISBANK", "BAJFINANCE",
        "ASIANPAINT", "MARUTI", "HCLTECH", "SUNPHARMA", "TITAN", "WIPRO",
        "ULTRACEMCO", "NESTLEIND", "NTPC", "POWERGRID", "TECHM", "TATASTEEL",
        "ONGC", "ADANIENT", "ADANIPORTS", "JSWSTEEL", "COALINDIA", "CIPLA",
        "DRREDDY", "DIVISLAB", "EICHERMOT", "HEROMOTOCO", "INDUSINDBK",
        "BRITANNIA", "APOLLOHOSP", "HINDALCO", "BAJAJ-AUTO", "BEL",
        "HAL", "TATAPOWER", "TRENT", "VEDL", "DLF", "HAVELLS", "POLYCAB",
        "PFC", "RECLTD"]
    breadth_tickers = [f"{s}.NS" for s in breadth_syms]
    above_ema = 0
    total_checked = 0
    try:
        bd = yf.download(breadth_tickers, period="3mo", progress=False, group_by="ticker", threads=True)
        if bd is not None and len(bd) > 50:
            for ticker in breadth_tickers:
                try:
                    d = bd[ticker].dropna(subset=["Close"]) if ticker in bd.columns.get_level_values(0) else None
                    if d is not None and len(d) > 50:
                        close_series = pd.to_numeric(d["Close"], errors="coerce")
                        ema = ta.trend.EMAIndicator(close_series, 50).ema_indicator()
                        if float(close_series.iloc[-1]) > float(ema.iloc[-1]):
                            above_ema += 1
                        total_checked += 1
                except:
                    pass
    except:
        pass
    if total_checked > 0:
        scores.append((above_ema / total_checked) * 100)

    if nifty_df is not None and len(nifty_df) > 20:
        rsi = ta.momentum.RSIIndicator(nifty_df["Close"], 14).rsi()
        rsi_val = float(rsi.iloc[-1])
        s = max(0, min(100, (rsi_val - 30) / 40 * 100))
        scores.append(s)

    fg = round(sum(scores) / len(scores), 1) if scores else 50.0

    if fg < 25: label = "Extreme Fear"
    elif fg < 40: label = "Fear"
    elif fg < 60: label = "Neutral"
    elif fg < 75: label = "Greed"
    else: label = "Extreme Greed"

    return fg, label

fg_score, fg_label = compute_fear_greed()


# ── 3. STOCKS DATA ──
print(">> Fetching Nifty 500 stock list...")

def fetch_nifty500_symbols():
    """Fetch full Nifty 500 symbols from NSE, fallback to cached/hardcoded list."""
    cache_file = LOG_DIR / "nifty500_symbols.json"

    # Try fetching from NSE
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
                # NSE CSV has 'Symbol' column
                sym_col = [c for c in csv_df.columns if 'symbol' in c.lower()]
                if sym_col:
                    symbols = csv_df[sym_col[0]].dropna().str.strip().tolist()
                    symbols = [s for s in symbols if s and len(s) > 0]
                    if len(symbols) >= 400:
                        # Cache for future use
                        cache_file.write_text(json.dumps(symbols))
                        print(f"   Fetched {len(symbols)} Nifty 500 symbols from NSE")
                        return symbols
        except Exception as e:
            pass

    # Fallback: use cached file
    if cache_file.exists():
        try:
            symbols = json.loads(cache_file.read_text())
            if len(symbols) >= 400:
                print(f"   Using cached Nifty 500 list ({len(symbols)} symbols)")
                return symbols
        except:
            pass

    # Final fallback: full Nifty 500 hardcoded list (updated Apr 2026)
    print("   NSE fetch failed, using hardcoded Nifty 500 fallback")
    return [
        "360ONE", "3MINDIA", "ABB", "ACC", "ACMESOLAR", "AIAENG", "APLAPOLLO", "AUBANK", "AWL", "AADHARHFC",
        "AARTIIND", "AAVAS", "ABBOTINDIA", "ACE", "ACUTAAS", "ADANIENSOL", "ADANIENT", "ADANIGREEN", "ADANIPORTS", "ADANIPOWER",
        "ATGL", "ABCAPITAL", "ABFRL", "ABLBL", "ABREL", "ABSLAMC", "CPPLUS", "AEGISLOG", "AEGISVOPAK", "AFCONS",
        "AFFLE", "AJANTPHARM", "ALKEM", "ABDL", "ARE&M", "AMBER", "AMBUJACEM", "ANANDRATHI", "ANANTRAJ", "ANGELONE",
        "ANTHEM", "ANURAS", "APARINDS", "APOLLOHOSP", "APOLLOTYRE", "APTUS", "ASAHIINDIA", "ASHOKLEY", "ASIANPAINT", "ASTERDM",
        "ASTRAL", "ATHERENERG", "ATUL", "AUROPHARMA", "AIIL", "DMART", "AXISBANK", "BEML", "BLS", "BSE",
        "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BAJAJHLDNG", "BAJAJHFL", "BALKRISIND", "BALRAMCHIN", "BANDHANBNK", "BANKBARODA", "BANKINDIA",
        "MAHABANK", "BATAINDIA", "BAYERCROP", "BELRISE", "BERGEPAINT", "BDL", "BEL", "BHARATFORG", "BHEL", "BPCL",
        "BHARTIARTL", "BHARTIHEXA", "BIKAJI", "GROWW", "BIOCON", "BSOFT", "BLUEDART", "BLUEJET", "BLUESTARCO", "BBTC",
        "BOSCHLTD", "FIRSTCRY", "BRIGADE", "BRITANNIA", "MAPMYINDIA", "CCL", "CESC", "CGPOWER", "CRISIL", "CANFINHOME",
        "CANBK", "CANHLIFE", "CAPLIPOINT", "CGCL", "CARBORUNIV", "CARTRADE", "CASTROLIND", "CEATLTD", "CEMPRO", "CENTRALBK",
        "CDSL", "CHALET", "CHAMBLFERT", "CHENNPETRO", "CHOICEIN", "CHOLAHLDNG", "CHOLAFIN", "CIPLA", "CUB", "CLEAN",
        "COALINDIA", "COCHINSHIP", "COFORGE", "COHANCE", "COLPAL", "CAMS", "CONCORDBIO", "CONCOR", "COROMANDEL", "CRAFTSMAN",
        "CREDITACC", "CROMPTON", "CUMMINSIND", "CYIENT", "DCMSHRIRAM", "DLF", "DOMS", "DABUR", "DALBHARAT", "DATAPATTNS",
        "DEEPAKFERT", "DEEPAKNTR", "DELHIVERY", "DEVYANI", "DIVISLAB", "DIXON", "LALPATHLAB", "DRREDDY", "EIDPARRY", "EIHOTEL",
        "EICHERMOT", "ELECON", "ELGIEQUIP", "EMAMILTD", "EMCURE", "EMMVEE", "ENDURANCE", "ENGINERSIN", "ERIS", "ESCORTS",
        "ETERNAL", "EXIDEIND", "NYKAA", "FEDERALBNK", "FACT", "FINCABLES", "FSL", "FIVESTAR", "FORCEMOT", "FORTIS",
        "GAIL", "GVT&D", "GMRAIRPORT", "GABRIEL", "GALLANTT", "GRSE", "GICRE", "GILLETTE", "GLAND", "GLAXO",
        "GLENMARK", "MEDANTA", "GODIGIT", "GPIL", "GODFRYPHLP", "GODREJCP", "GODREJIND", "GODREJPROP", "GRANULES", "GRAPHITE",
        "GRASIM", "GRAVITA", "GESHIP", "FLUOROCHEM", "GMDCLTD", "GSPL", "HEG", "HBLENGINE", "HCLTECH", "HDBFS",
        "HDFCAMC", "HDFCBANK", "HDFCLIFE", "HFCL", "HAVELLS", "HEROMOTOCO", "HEXT", "HSCL", "HINDALCO", "HAL",
        "HINDCOPPER", "HINDPETRO", "HINDUNILVR", "HINDZINC", "POWERINDIA", "HOMEFIRST", "HONASA", "HONAUT", "HUDCO", "HYUNDAI",
        "ICICIBANK", "ICICIGI", "ICICIAMC", "ICICIPRULI", "IDBI", "IDFCFIRSTB", "IFCI", "IIFL", "IRB", "IRCON",
        "ITCHOTELS", "ITC", "ITI", "INDGN", "INDIACEM", "INDIAMART", "INDIANB", "IEX", "INDHOTEL", "IOC",
        "IOB", "IRCTC", "IRFC", "IREDA", "IGL", "INDUSTOWER", "INDUSINDBK", "NAUKRI", "INFY", "INOXWIND",
        "INTELLECT", "INDIGO", "IGIL", "IKS", "IPCALAB", "JBCHEPHARM", "JKCEMENT", "JBMA", "JKTYRE", "JMFINANCIL",
        "JSWCEMENT", "JSWDULUX", "JSWENERGY", "JSWINFRA", "JSWSTEEL", "JAINREC", "JPPOWER", "J&KBANK", "JINDALSAW", "JSL",
        "JINDALSTEL", "JIOFIN", "JUBLFOOD", "JUBLINGREA", "JUBLPHARMA", "JWL", "JYOTICNC", "KPRMILL", "KEI", "KPITTECH",
        "KAJARIACER", "KPIL", "KALYANKJIL", "KARURVYSYA", "KAYNES", "KEC", "KFINTECH", "KIRLOSENG", "KOTAKBANK", "KIMS",
        "LTF", "LTTS", "LGEINDIA", "LICHSGFIN", "LTFOODS", "LTM", "LT", "LATENTVIEW", "LAURUSLABS", "THELEELA",
        "LEMONTREE", "LENSKART", "LICI", "LINDEINDIA", "LLOYDSME", "LODHA", "LUPIN", "MMTC", "MRF", "MGL",
        "M&MFIN", "M&M", "MANAPPURAM", "MRPL", "MANKIND", "MARICO", "MARUTI", "MFSL", "MAXHEALTH", "MAZDOCK",
        "MEESHO", "MINDACORP", "MSUMI", "MOTILALOFS", "MPHASIS", "MCX", "MUTHOOTFIN", "NATCOPHARM", "NBCC", "NCC",
        "NHPC", "NLCINDIA", "NMDC", "NSLNISP", "NTPCGREEN", "NTPC", "NH", "NATIONALUM", "NAVA", "NAVINFLUOR",
        "NESTLEIND", "NETWEB", "NEULANDLAB", "NEWGEN", "NAM-INDIA", "NIVABUPA", "NUVAMA", "NUVOCO", "OBEROIRLTY", "ONGC",
        "OIL", "OLAELEC", "OLECTRA", "PAYTM", "ONESOURCE", "OFSS", "POLICYBZR", "PCBL", "PGEL", "PIIND",
        "PNBHOUSING", "PTCIL", "PVRINOX", "PAGEIND", "PARADEEP", "PATANJALI", "PERSISTENT", "PETRONET", "PFIZER", "PHOENIXLTD",
        "PWL", "PIDILITIND", "PINELABS", "PIRAMALFIN", "PPLPHARMA", "POLYMED", "POLYCAB", "POONAWALLA", "PFC", "POWERGRID",
        "PREMIERENE", "PRESTIGE", "PNB", "RRKABEL", "RBLBANK", "RECLTD", "RHIM", "RITES", "RADICO", "RVNL",
        "RAILTEL", "RAINBOW", "RKFORGE", "REDINGTON", "RELIANCE", "RPOWER", "SBFC", "SBICARD", "SBILIFE", "SJVN",
        "SRF", "SAGILITY", "SAILIFE", "SAMMAANCAP", "MOTHERSON", "SAPPHIRE", "SARDAEN", "SAREGAMA", "SCHAEFFLER", "SCHNEIDER",
        "SCI", "SHREECEM", "SHRIRAMFIN", "SHYAMMETL", "ENRIN", "SIEMENS", "SIGNATURE", "SOBHA", "SOLARINDS", "SONACOMS",
        "SONATSOFTW", "STARHEALTH", "SBIN", "SAIL", "SUMICHEM", "SUNPHARMA", "SUNTV", "SUNDARMFIN", "SUPREMEIND", "SPLPETRO",
        "SUZLON", "SWANCORP", "SWIGGY", "SYNGENE", "SYRMA", "TBOTEK", "TVSMOTOR", "TATACAP", "TATACHEM", "TATACOMM",
        "TCS", "TATACONSUM", "TATAELXSI", "TATAINVEST", "TMCV", "TMPV", "TATAPOWER", "TATASTEEL", "TATATECH", "TTML",
        "TECHM", "TECHNOE", "TEGA", "TEJASNET", "TENNIND", "NIACL", "RAMCOCEM", "THERMAX", "TIMKEN", "TITAGARH",
        "TITAN", "TORNTPHARM", "TORNTPOWER", "TARIL", "TRAVELFOOD", "TRENT", "TRIDENT", "TRITURBINE", "TIINDIA", "UCOBANK",
        "UNOMINDA", "UPL", "UTIAMC", "ULTRACEMCO", "UNIONBANK", "UBL", "UNITDSPR", "URBANCO", "USHAMART", "VTL",
        "VBL", "VEDL", "VIJAYA", "VMM", "IDEA", "VOLTAS", "WAAREEENER", "WELCORP", "WELSPUNLIV", "WHIRLPOOL",
        "WIPRO", "WOCKPHARMA", "YESBANK", "ZFCVINDIA", "ZEEL", "ZENTEC", "ZENSARTECH", "ZYDUSLIFE", "ZYDUSWELL", "ECLERX",
    ]

NIFTY500 = fetch_nifty500_symbols()

print(f">> Fetching stock data for {len(NIFTY500)} stocks...")

# Batch download using yfinance for speed (download all at once)
tickers_ns = [f"{s}.NS" for s in NIFTY500]
batch_size = 50  # download in batches to avoid timeouts
stock_data = []

for batch_start in range(0, len(tickers_ns), batch_size):
    batch = tickers_ns[batch_start:batch_start + batch_size]
    batch_num = batch_start // batch_size + 1
    total_batches = (len(tickers_ns) + batch_size - 1) // batch_size
    print(f"   Batch {batch_num}/{total_batches} ({len(batch)} stocks)...")

    try:
        batch_df = yf.download(batch, period="1y", progress=False, group_by="ticker", threads=True)
        if batch_df is None or len(batch_df) == 0:
            continue

        for ticker in batch:
            sym = ticker.replace(".NS", "")
            try:
                if len(batch) == 1:
                    df = batch_df.copy()
                else:
                    df = batch_df[ticker].copy() if ticker in batch_df.columns.get_level_values(0) else None
                if df is None or len(df) < 10:
                    continue

                df = df.dropna(subset=["Close"])
                if len(df) < 10:
                    continue

                close = float(df["Close"].iloc[-1])
                prev_close = float(df["Close"].iloc[-2])
                if np.isnan(close) or np.isnan(prev_close) or prev_close == 0:
                    continue
                chg_pct = (close - prev_close) / prev_close * 100

                vol_today = float(df["Volume"].iloc[-1])
                vol_avg = float(df["Volume"].iloc[-6:-1].mean())
                vol_ratio = vol_today / vol_avg if vol_avg > 0 else 1

                high_52w = float(df["High"].max())
                dist_from_52w = (high_52w - close) / high_52w * 100

                stock_data.append({
                    "symbol": sym, "close": round(close, 2), "chg_pct": round(chg_pct, 2),
                    "volume": vol_today, "vol_ratio": round(vol_ratio, 2),
                    "high_52w": round(high_52w, 2), "dist_52w": round(dist_from_52w, 2),
                })
            except:
                pass
    except:
        pass

print(f"   Processed {len(stock_data)} stocks successfully")

stock_df = pd.DataFrame(stock_data)
top_gainers = stock_df.nlargest(5, "chg_pct") if len(stock_df) > 0 else pd.DataFrame()
top_losers = stock_df.nsmallest(5, "chg_pct") if len(stock_df) > 0 else pd.DataFrame()
vol_shockers = stock_df[stock_df["vol_ratio"] >= 1.5].nlargest(5, "vol_ratio") if len(stock_df) > 0 else pd.DataFrame()
week52_highs = stock_df[stock_df["dist_52w"] <= 2].nlargest(5, "chg_pct") if len(stock_df) > 0 else pd.DataFrame()


# ── 4. SECTORAL INDICES ──
print(">> Fetching sectoral indices...")

SECTOR_INDICES = {
    "Nifty Bank": "^NSEBANK",
    "Nifty IT": "^CNXIT",
    "Nifty Pharma": "^CNXPHARMA",
    "Nifty Auto": "^CNXAUTO",
    "Nifty Metal": "^CNXMETAL",
    "Nifty Realty": "^CNXREALTY",
    "Nifty Energy": "^CNXENERGY",
    "Nifty FMCG": "^CNXFMCG",
    "Nifty PSU Bank": "^CNXPSUBANK",
    "Nifty Fin Service": "^CNXFIN",
    "Nifty Media": "^CNXMEDIA",
    "Nifty Infra": "^CNXINFRA",
    "Nifty PSE": "^CNXPSE",
    "Nifty MNC": "^CNXMNC",
    "Nifty Defence": "^CNXDEFN",
}

sector_data = []
for name, ticker in SECTOR_INDICES.items():
    try:
        df = yf.download(ticker, period="1mo", progress=False)
        if df is not None and len(df) >= 7:
            df = df.reset_index()
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
            close = float(df["Close"].iloc[-1])
            prev = float(df["Close"].iloc[-2])
            chg_1d = (close - prev) / prev * 100
            prev_7d = float(df["Close"].iloc[-6]) if len(df) >= 7 else prev
            chg_7d = (close - prev_7d) / prev_7d * 100
            sector_data.append({"name": name, "close": close, "chg_1d": round(chg_1d, 2), "chg_7d": round(chg_7d, 2)})
    except:
        pass

sector_df = pd.DataFrame(sector_data)


# ── 5. EARNINGS DATA ──
print(">> Loading earnings data...")

from datetime import date

today_date = datetime.now().date()
week_start = today_date - timedelta(days=today_date.weekday())  # Monday
week_end = week_start + timedelta(days=4)  # Friday

# a) Recent results from event_alpha.csv (last 2 days)
recent_results = []
try:
    ea_path = BASE_DIR / "quantex_logs" / "event_alpha.csv"
    if ea_path.exists():
        ea_df = pd.read_csv(ea_path)
        ea_df["Date"] = pd.to_datetime(ea_df["Date"]).dt.date
        cutoff = today_date - timedelta(days=3)
        recent_ea = ea_df[ea_df["Date"] >= cutoff].copy()
        # Deduplicate by symbol, keep latest
        recent_ea = recent_ea.sort_values("Date", ascending=False).drop_duplicates("Symbol", keep="first")
        for _, row in recent_ea.head(10).iterrows():
            recent_results.append({
                "symbol": row["Symbol"],
                "date": str(row["Date"]),
                "signal": row.get("Signals", ""),
                "price": row.get("CMP", 0),
            })
        print(f"   Found {len(recent_results)} recent earnings results")
except Exception as e:
    print(f"   Event alpha load error: {e}")

# b) Upcoming earnings in next 3 days via yfinance
upcoming_earnings = []
next_7d = today_date + timedelta(days=3)
try:
    # Check ALL tracked stocks for earnings dates in next 7 days
    print("   Scanning earnings calendar for next 3 days...")
    for idx, sym in enumerate(NIFTY500):
        if (idx + 1) % 50 == 0:
            print(f"      {idx+1}/{len(NIFTY500)}...")
        try:
            tk = yf.Ticker(f"{sym}.NS")
            cal = tk.calendar
            if cal is not None:
                ed_val = None
                est_eps = None
                est_rev = None
                if isinstance(cal, dict):
                    ed_list = cal.get("Earnings Date")
                    if ed_list:
                        ed_val = ed_list[0] if isinstance(ed_list, list) else ed_list
                    est_eps = cal.get("Earnings Average")
                    est_rev = cal.get("Revenue Average")
                elif hasattr(cal, 'iloc'):
                    for col in cal.columns:
                        try:
                            ed_val = pd.to_datetime(cal[col].iloc[0]).date()
                            break
                        except:
                            pass

                if ed_val:
                    ed = pd.to_datetime(ed_val).date() if not isinstance(ed_val, date) else ed_val
                    if today_date <= ed <= next_7d:
                        # Get current price from stock_data if available
                        price = 0
                        sd = [s for s in stock_data if s["symbol"] == sym]
                        if sd:
                            price = sd[0]["close"]

                        rev_str = ""
                        if est_rev and est_rev > 0:
                            if est_rev >= 1e12:
                                rev_str = f"{est_rev/1e10:,.0f} Cr"
                            elif est_rev >= 1e9:
                                rev_str = f"{est_rev/1e9:,.1f}B"
                            else:
                                rev_str = f"{est_rev/1e7:,.0f} Cr"

                        upcoming_earnings.append({
                            "symbol": sym,
                            "date": str(ed),
                            "day": ed.strftime("%a"),
                            "price": price,
                            "est_eps": round(est_eps, 2) if est_eps else None,
                            "est_rev": rev_str,
                        })
        except:
            pass

    # Deduplicate and sort by date
    seen = set()
    unique_upcoming = []
    for e in upcoming_earnings:
        if e["symbol"] not in seen:
            seen.add(e["symbol"])
            unique_upcoming.append(e)
    upcoming_earnings = sorted(unique_upcoming, key=lambda x: x["date"])
    print(f"   Found {len(upcoming_earnings)} stocks with earnings in next 3 days")
except Exception as e:
    print(f"   Earnings calendar error: {e}")


# ── 6. FOCUS STOCKS FROM EVENING SCANNERS ──
print(">> Loading focus stocks from evening scanners...")

focus_stocks = []
focus_source = ""

# a) Load swing scanner recommendations (evening run)
try:
    rec_path = BASE_DIR / "quantex_logs" / "recommendations.json"
    if rec_path.exists():
        with open(rec_path) as f:
            rec_data = json.load(f)
        # Get the latest scan
        if isinstance(rec_data, list) and len(rec_data) > 0:
            latest_scan = rec_data[-1]
            scan_date = latest_scan.get("scan_date", "")
            focus_source = f"Swing Scanner ({scan_date})"
            top_recs = latest_scan.get("top_10", [])[:10]
            for r in top_recs:
                focus_stocks.append({
                    "symbol": r["symbol"],
                    "score": r["score"],
                    "entry": r.get("entry", 0),
                    "sl": r.get("sl", 0),
                    "target": r.get("target1", 0),
                    "signals": ", ".join(r.get("signals", [])[:3]),
                    "source": "Swing",
                })
            print(f"   Loaded {len(focus_stocks)} from swing scanner ({scan_date})")
except Exception as e:
    print(f"   Swing scanner load error: {e}")

# b) Load pullback picks
try:
    pb_path = BASE_DIR / "quantex_logs" / "pullback_latest.json"
    if pb_path.exists():
        with open(pb_path) as f:
            pb_data = json.load(f)
        pb_date = pb_data.get("scan_date", "")
        for p in pb_data.get("picks", []):
            # Add only if not already in focus list
            if p["symbol"] not in [f["symbol"] for f in focus_stocks]:
                focus_stocks.append({
                    "symbol": p["symbol"],
                    "score": p["score"],
                    "entry": p.get("entry", 0),
                    "sl": p.get("sl", 0),
                    "target": p.get("target_3", p.get("target_5", 0)),
                    "signals": " | ".join(p.get("signals", [])[:3]),
                    "source": "Pullback",
                })
        if not focus_source:
            focus_source = f"Pullback Scanner ({pb_date})"
        else:
            focus_source += f" + Pullback ({pb_date})"
        print(f"   Added pullback picks, total focus: {len(focus_stocks)}")
except Exception as e:
    print(f"   Pullback load error: {e}")

# Sort by score and take top 10
focus_stocks = sorted(focus_stocks, key=lambda x: x["score"], reverse=True)[:10]




# ═══════════════════════════════════════════════════════════════
# 7. BUILD MAGAZINE-STYLE PDF REPORT
# ═══════════════════════════════════════════════════════════════

print(">> Building magazine-style PDF report...")

PDF_PATH = LOG_DIR / f"PreMarket_Report_{TODAY_SHORT}.pdf"

from reportlab.graphics.shapes import Drawing, Rect, Line, String, Group
from reportlab.graphics import renderPDF
from reportlab.platypus import Flowable
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# ── Register DejaVuSans for ₹ symbol support ──
import glob as _glob
_dv_paths = _glob.glob("/usr/share/fonts/**/DejaVuSans.ttf", recursive=True)
_dv_bold_paths = _glob.glob("/usr/share/fonts/**/DejaVuSans-Bold.ttf", recursive=True)
_dv_oblique_paths = _glob.glob("/usr/share/fonts/**/DejaVuSans-Oblique.ttf", recursive=True)
FONT = "Helvetica"
FONT_B = "Helvetica-Bold"
FONT_I = "Helvetica-Oblique"
MONO = "Courier"
MONO_B = "Courier-Bold"
RUPEE = "Rs."  # fallback
try:
    if _dv_paths:
        pdfmetrics.registerFont(TTFont("DejaVu", _dv_paths[0]))
        FONT = "DejaVu"
        RUPEE = "\u20B9"
    if _dv_bold_paths:
        pdfmetrics.registerFont(TTFont("DejaVuBd", _dv_bold_paths[0]))
        FONT_B = "DejaVuBd"
    if _dv_oblique_paths:
        pdfmetrics.registerFont(TTFont("DejaVuIt", _dv_oblique_paths[0]))
        FONT_I = "DejaVuIt"
    # Also try monospace
    _dvm_paths = _glob.glob("/usr/share/fonts/**/DejaVuSansMono.ttf", recursive=True)
    _dvm_bold = _glob.glob("/usr/share/fonts/**/DejaVuSansMono-Bold.ttf", recursive=True)
    if _dvm_paths:
        pdfmetrics.registerFont(TTFont("DejaVuMono", _dvm_paths[0]))
        MONO = "DejaVuMono"
    if _dvm_bold:
        pdfmetrics.registerFont(TTFont("DejaVuMonoBd", _dvm_bold[0]))
        MONO_B = "DejaVuMonoBd"
    print(f"   Fonts registered: {FONT}, {FONT_B}, {MONO}")
except Exception as e:
    print(f"   Font registration warning: {e}")

# ── Magazine Color Palette (light theme) ──
C_DARK   = HexColor("#1e293b")
C_TEXT   = HexColor("#111827")
C_MUTED  = HexColor("#6b7280")
C_LIGHT  = HexColor("#9ca3af")
C_GREEN  = HexColor("#059669")
C_RED    = HexColor("#dc2626")
C_BLUE   = HexColor("#2563eb")
C_TEAL   = HexColor("#0d9488")
C_BORDER = HexColor("#e5e7eb")
C_BG_CARD = HexColor("#f9fafb")
C_AMBER  = HexColor("#d97706")
C_WHITE  = white
C_SCORE_GREEN = HexColor("#16a34a")
C_SCORE_AMBER = HexColor("#ca8a04")

page_w = A4[0] - 30*mm  # usable width (15mm margins each side)

# ── Helpers ──
def spaced(text):
    """Letter-space uppercase text, preserving word gaps."""
    words = text.upper().split()
    spaced_words = [' '.join(w) for w in words]
    return '   '.join(spaced_words)  # triple space between words

def fmt_num(val, decimals=2):
    if val is None: return "—"
    try:
        if abs(val) >= 1000:
            return f"{val:,.{decimals}f}"
        return f"{val:.{decimals}f}"
    except:
        return str(val)

def fmt_chg(val):
    if val is None: return "—"
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:.2f}%"

def chg_color(val):
    if val is None: return C_MUTED
    return C_GREEN if val >= 0 else C_RED

def rupee(val):
    """Format price with rupee symbol."""
    return f"{RUPEE}{fmt_num(val)}"

# ── Styles ──
styles = getSampleStyleSheet()

def ps(name, **kw):
    """Shorthand ParagraphStyle builder."""
    defaults = {"fontName": FONT, "fontSize": 8, "leading": 11, "textColor": C_TEXT}
    defaults.update(kw)
    return ParagraphStyle(name, **defaults)

s_body = ps("Body", fontSize=8.5, leading=12)
s_body_bold = ps("BodyBold", fontName=FONT_B, fontSize=8.5, leading=12)
s_small = ps("Small", fontSize=7, leading=9, textColor=C_MUTED)


# ═══════ CUSTOM FLOWABLES ═══════

class SectionHeader(Flowable):
    """Numbered badge + title + horizontal rule + right subtitle."""
    def __init__(self, num, title, subtitle="", width=None):
        Flowable.__init__(self)
        self.num = num; self.title = title; self.subtitle = subtitle
        self.width = width or page_w; self.height = 10*mm

    def draw(self):
        c = self.canv; w = self.width
        bw = 7*mm; bh = 7*mm
        c.setFillColor(C_DARK)
        c.roundRect(0, 1.5*mm, bw, bh, 1.2*mm, fill=1, stroke=0)
        c.setFillColor(C_WHITE)
        c.setFont(FONT_B, 8)
        c.drawCentredString(bw/2, 3.8*mm, f"{self.num:02d}")
        c.setFillColor(C_DARK)
        c.setFont(FONT_B, 14)
        c.drawString(bw + 3*mm, 3*mm, self.title)
        tw = c.stringWidth(self.title, FONT_B, 14)
        lx = bw + 3*mm + tw + 3*mm
        c.setStrokeColor(C_BORDER); c.setLineWidth(0.5)
        c.line(lx, 6*mm, w - 2*mm, 6*mm)
        if self.subtitle:
            c.setFillColor(C_MUTED); c.setFont(FONT, 7)
            st = spaced(self.subtitle)
            c.drawRightString(w - 2*mm, 3.5*mm, st)


class FearGreedBar(Flowable):
    """Horizontal gradient bar with circular marker."""
    def __init__(self, score, width=None, height=8*mm):
        Flowable.__init__(self)
        self.score = score; self.width = width or 70*mm; self.height = height

    def draw(self):
        c = self.canv; w = self.width
        bar_h = 3*mm; bar_y = 2.5*mm
        segs = [(HexColor("#dc2626"),0,.25),(HexColor("#f59e0b"),.25,.45),
                (HexColor("#22c55e"),.45,.55),(HexColor("#f59e0b"),.55,.75),
                (HexColor("#dc2626"),.75,1.0)]
        for color, s, e in segs:
            c.setFillColor(color); c.rect(w*s, bar_y, w*(e-s), bar_h, fill=1, stroke=0)
        mx = (self.score/100)*w
        c.setFillColor(C_DARK); c.circle(mx, bar_y+bar_h/2, 2.5*mm, fill=1, stroke=0)
        c.setFillColor(C_WHITE); c.circle(mx, bar_y+bar_h/2, 1.5*mm, fill=1, stroke=0)
        c.setFillColor(C_MUTED); c.setFont(FONT, 5.5)
        c.drawString(0, -1*mm, spaced("Extreme Fear"))
        c.drawCentredString(w/2, -1*mm, spaced("Neutral"))
        eg = spaced("Extreme Greed")
        c.drawRightString(w, -1*mm, eg)


class SectorBar(Flowable):
    """Bidirectional bar for sectoral % change."""
    def __init__(self, pct, max_pct=1.5, width=50*mm, height=4*mm):
        Flowable.__init__(self)
        self.pct = pct; self.max_pct = max_pct; self.width = width; self.height = height

    def draw(self):
        c = self.canv; w = self.width; bar_h = 3*mm; mid = w/2
        c.setFillColor(HexColor("#f3f4f6")); c.rect(0, 0.5*mm, w, bar_h, fill=1, stroke=0)
        bl = min((abs(self.pct)/self.max_pct)*(w/2), w/2)
        if self.pct >= 0:
            c.setFillColor(C_GREEN); c.rect(mid, 0.5*mm, bl, bar_h, fill=1, stroke=0)
        else:
            c.setFillColor(C_RED); c.rect(mid-bl, 0.5*mm, bl, bar_h, fill=1, stroke=0)
        c.setStrokeColor(C_MUTED); c.setLineWidth(0.3); c.line(mid, 0, mid, bar_h+1*mm)


class GapBar(Flowable):
    """Progress bar for 52-week gap."""
    def __init__(self, gap_pct, width=28*mm, height=3*mm):
        Flowable.__init__(self)
        self.gap_pct = gap_pct; self.width = width; self.height = height

    def draw(self):
        c = self.canv
        c.setFillColor(HexColor("#e5e7eb")); c.rect(0, 0, self.width, 2.5*mm, fill=1, stroke=0)
        fp = max(0, min(1, 1-self.gap_pct/2))
        c.setFillColor(C_TEAL); c.rect(0, 0, self.width*fp, 2.5*mm, fill=1, stroke=0)


# ═══════ PAGE TEMPLATES ═══════

def page1_template(canvas_obj, doc):
    c = canvas_obj; c.saveState(); w, h = A4
    c.setFillColor(C_BLUE); c.rect(0, h-3*mm, w, 3*mm, fill=1, stroke=0)
    c.setFillColor(C_MUTED); c.setFont(FONT, 6.5)
    c.drawString(15*mm, h-10*mm, spaced("Vol. I · No. 001 · Pre-Market Intelligence"))
    c.setFillColor(C_DARK); c.setFont(FONT_B, 28)
    c.drawString(15*mm, h-24*mm, "Quant")
    qw = c.stringWidth("Quant", FONT_B, 28)
    c.setFillColor(C_TEAL); c.setFont(FONT_B, 28)
    c.drawString(15*mm+qw, h-24*mm, "ex")
    c.setFillColor(C_DARK); c.setFont(FONT, 10)
    day_name = datetime.now().strftime("%A")
    c.drawRightString(w-15*mm, h-13*mm, f"{day_name} Edition")
    c.setFillColor(C_MUTED); c.setFont(FONT, 7.5)
    c.drawRightString(w-15*mm, h-19*mm, f"{datetime.now().strftime('%d %B %Y').upper()} \u00b7 {TIME_NOW} IST")
    c.drawRightString(w-15*mm, h-25*mm, "FOR EDUCATIONAL PURPOSE ONLY")
    c.setFont(FONT, 7); c.drawRightString(w-15*mm, 8*mm, "PAGE 01")
    c.restoreState()

def later_page_template(canvas_obj, doc):
    c = canvas_obj; c.saveState(); w, h = A4
    pn = doc.page
    parts = {2: "II", 3: "III", 4: "IV", 5: "V"}
    pl = parts.get(pn, str(pn))
    c.setFillColor(C_MUTED); c.setFont(FONT, 6.5)
    c.drawString(15*mm, h-10*mm, spaced(f"Pre-Market Intelligence · Part {pl}"))
    c.setFillColor(C_DARK); c.setFont(FONT_B, 11)
    brand = "Quant"
    bw = c.stringWidth(brand, FONT_B, 11)
    cx = w/2 - bw/2 - 2
    c.drawString(cx, h-10*mm, brand)
    c.setFillColor(C_TEAL); c.setFont(FONT_B, 11)
    c.drawString(cx+bw, h-10*mm, "ex")
    c.setFillColor(C_MUTED); c.setFont(FONT, 6.5)
    c.drawRightString(w-15*mm, h-10*mm, f"{datetime.now().strftime('%d %B %Y').upper()}")
    c.setFont(FONT, 7); c.drawRightString(w-15*mm, 8*mm, f"PAGE {pn:02d}")
    c.restoreState()


# ═══════ BUILD STORY ═══════

story = []

# F&G tip
if fg_score < 25: fg_tip = "Opportunity zone \u2014 look for quality pullbacks"
elif fg_score < 40: fg_tip = "Cautious optimism \u2014 wait for confirmation"
elif fg_score < 60: fg_tip = "Market balanced \u2014 be selective with entries"
elif fg_score < 75: fg_tip = "Getting heated \u2014 tighten stop-losses"
else: fg_tip = "Euphoria zone \u2014 avoid fresh positions"

# Gift Nifty commentary
if gift_pct is not None:
    if abs(gift_pct) < 0.1:
        gift_comment = "A flat-to-marginally-positive open indicated. Global weakness in Asia &amp; Europe could weigh on sentiment."
    elif gift_pct > 0.5:
        gift_comment = "A gap-up open indicated. Positive global cues supporting bullish sentiment."
    elif gift_pct > 0:
        gift_comment = "A mildly positive open indicated. Watch for follow-through above key resistance levels."
    elif gift_pct > -0.5:
        gift_comment = "A marginally negative open indicated. Support levels will be tested early in the session."
    else:
        gift_comment = "A gap-down open indicated. Global sell-off weighing heavily on sentiment."
else:
    gift_comment = ""

story.append(Spacer(1, 22*mm))

# ═══════ HERO: F&G + GIFT NIFTY ═══════

hero_left_w = 82*mm
hero_right_w = 82*mm

fg_label_color = C_TEAL if "Neutral" in fg_label else (C_GREEN if "Greed" in fg_label else C_RED)

fg_card = [
    [Paragraph(f'<font name="{FONT}" size="6" color="#{C_MUTED.hexval()[2:]}">{spaced("Market Sentiment")}</font>', s_small)],
    [Paragraph(f'<font name="{FONT_B}" size="11" color="#{C_DARK.hexval()[2:]}">Fear &amp; Greed Index</font>', ps("fgt", fontName=FONT_B, fontSize=11, leading=14, textColor=C_DARK))],
    [Spacer(1, 2*mm)],
    [Paragraph(f'<font name="{MONO_B}" size="28" color="#{C_DARK.hexval()[2:]}">{fg_score}</font>', ps("fgs", fontName=MONO_B, fontSize=28, leading=32, textColor=C_DARK))],
    [Paragraph(f'<font name="{FONT_B}" size="9" color="#{fg_label_color.hexval()[2:]}">{fg_label.upper()}</font>', ps("fgl", fontName=FONT_B, fontSize=9, leading=12, textColor=fg_label_color))],
    [Spacer(1, 1*mm)],
    [Paragraph(f'<font name="{FONT}" size="7.5" color="#{C_MUTED.hexval()[2:]}">{fg_tip}</font>', s_small)],
    [Spacer(1, 2*mm)],
    [FearGreedBar(fg_score, width=72*mm)],
]
fg_t = Table(fg_card, colWidths=[76*mm])
fg_t.setStyle(TableStyle([
    ('BACKGROUND', (0,0), (-1,-1), C_BG_CARD),
    ('BOX', (0,0), (-1,-1), 0.5, C_BORDER),
    ('TOPPADDING', (0,0), (-1,0), 4*mm), ('BOTTOMPADDING', (0,-1), (-1,-1), 5*mm),
    ('TOPPADDING', (0,1), (-1,-2), 1*mm), ('BOTTOMPADDING', (0,0), (-1,-2), 0),
    ('LEFTPADDING', (0,0), (-1,-1), 4*mm), ('RIGHTPADDING', (0,0), (-1,-1), 4*mm),
    ('ROUNDEDCORNERS', [2*mm,2*mm,2*mm,2*mm]),
]))

gift_sign = "+" if (gift_chg or 0) >= 0 else ""
gift_arrow = "\u25B2" if (gift_pct or 0) >= 0 else "\u25BC"
gc = chg_color(gift_pct)

gn_card = [
    [Paragraph(f'<font name="{FONT}" size="6" color="#{C_MUTED.hexval()[2:]}">{spaced("Pre-Market Indicator · Gift Nifty")}</font>', s_small)],
    [Paragraph(f'<font name="{FONT_B}" size="11" color="#{C_DARK.hexval()[2:]}">Opening Bell Preview</font>', ps("gnt", fontName=FONT_B, fontSize=11, leading=14, textColor=C_DARK))],
    [Spacer(1, 2*mm)],
    [Paragraph(f'<font name="{MONO_B}" size="24" color="#{C_DARK.hexval()[2:]}">{fmt_num(gift_close)}</font>', ps("gnp", fontName=MONO_B, fontSize=24, leading=28, textColor=C_DARK))],
    [Paragraph(f'<font name="{FONT_B}" size="9" color="#{gc.hexval()[2:]}">{gift_arrow} {gift_sign}{fmt_num(gift_chg)}    {fmt_chg(gift_pct)}</font>', ps("gnc", fontName=FONT_B, fontSize=9, leading=12, textColor=gc))],
    [Spacer(1, 2*mm)],
    [Paragraph(f'<font name="{FONT}" size="7.5" color="#{C_MUTED.hexval()[2:]}">{gift_comment}</font>', s_small)],
]
gn_t = Table(gn_card, colWidths=[76*mm])
gn_t.setStyle(TableStyle([
    ('BACKGROUND', (0,0), (-1,-1), C_WHITE),
    ('BOX', (0,0), (-1,-1), 0.5, C_BORDER),
    ('TOPPADDING', (0,0), (-1,0), 4*mm), ('BOTTOMPADDING', (0,-1), (-1,-1), 4*mm),
    ('TOPPADDING', (0,1), (-1,-2), 1*mm), ('BOTTOMPADDING', (0,0), (-1,-2), 0),
    ('LEFTPADDING', (0,0), (-1,-1), 4*mm), ('RIGHTPADDING', (0,0), (-1,-1), 4*mm),
    ('ROUNDEDCORNERS', [2*mm,2*mm,2*mm,2*mm]),
]))

hero = Table([[fg_t, gn_t]], colWidths=[hero_left_w, hero_right_w], hAlign='LEFT')
hero.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),0),('RIGHTPADDING',(0,0),(-1,-1),0)]))
story.append(hero)
story.append(Spacer(1, 5*mm))


# ═══════ 01 GLOBAL MARKETS ═══════
story.append(SectionHeader(1, "Global Markets", "Previous Session Close"))
story.append(Spacer(1, 3*mm))

asia = ["Hang Seng","Nikkei 225","KOSPI","ASX 200"]
europe = ["DAX","FTSE 100","CAC 40"]
us = ["Dow Jones","Nasdaq","S&P 500"]
cc_map = {"HK":"HK","Japan":"JP","Korea":"KR","Australia":"AU","Germany":"DE","UK":"UK","France":"FR","US":"US"}

def global_col(label, count, names):
    rows = [[
        Paragraph(f'<font name="{FONT}" size="7" color="#{C_MUTED.hexval()[2:]}">{spaced(label)}</font>', s_small),
        Paragraph(f'<font name="{FONT}" size="7" color="#{C_MUTED.hexval()[2:]}">{count:02d}</font>', ps("gc2", fontSize=7, textColor=C_MUTED, alignment=TA_RIGHT)),
    ]]
    for n in names:
        d = global_data.get(n, {})
        cc = cc_map.get(d.get("country",""), "")
        p = d.get("pct"); pc = chg_color(p)
        rows.append([
            Paragraph(f'<font name="{FONT}" size="7.5">{n}</font> <font name="{FONT}" size="6" color="#{C_LIGHT.hexval()[2:]}">{cc}</font>', s_body),
            Paragraph(f'<font name="{MONO}" size="7.5">{fmt_num(d.get("close"))}</font>  <font name="{FONT_B}" size="7.5" color="#{pc.hexval()[2:]}">{fmt_chg(p)}</font>', ps("gv", fontName=MONO, fontSize=7.5, leading=11, textColor=C_TEXT, alignment=TA_RIGHT)),
        ])
    t = Table(rows, colWidths=[26*mm, 29*mm])
    t.setStyle(TableStyle([
        ('TOPPADDING',(0,0),(-1,-1),1.5*mm), ('BOTTOMPADDING',(0,0),(-1,-1),1.5*mm),
        ('LINEBELOW',(0,0),(-1,0),0.5,C_BORDER), ('LINEBELOW',(0,1),(-1,-1),0.3,HexColor("#f3f4f6")),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
    ]))
    return t

g1 = global_col("Asia Pac", len(asia), asia)
g2 = global_col("Europe", len(europe), europe)
g3 = global_col("US Markets", len(us), us)

gt = Table([[g1, g2, g3]], colWidths=[57*mm, 55*mm, 55*mm], hAlign='LEFT')
gt.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),0),('RIGHTPADDING',(0,0),(1,0),2*mm)]))
story.append(gt)
story.append(Spacer(1, 5*mm))


# ═══════ 02 INDIAN MARKET ═══════
story.append(SectionHeader(2, "Indian Market \u00b7 Previous Close", "Benchmark Snapshot"))
story.append(Spacer(1, 3*mm))

bench = ["Sensex","Nifty 50","Bank Nifty","India VIX","USD/INR"]
bcells = []
cw = page_w / 5
for name in bench:
    d = indian_data.get(name, {})
    cl = d.get("close"); ch = d.get("chg"); pt = d.get("pct")
    cc2 = chg_color(pt)
    sign = "+" if (ch or 0) >= 0 else ""
    warn = " \u26A0" if name == "India VIX" and (cl or 0) > 15 else ""
    cd = [
        [Paragraph(f'<font name="{FONT}" size="6" color="#{C_MUTED.hexval()[2:]}">{spaced(name)}{warn}</font>', s_small)],
        [Paragraph(f'<font name="{MONO_B}" size="15">{fmt_num(cl)}</font>', ps("bp", fontName=MONO_B, fontSize=15, leading=19, textColor=C_DARK))],
        [Paragraph(f'<font name="{FONT}" size="7" color="#{cc2.hexval()[2:]}">{sign}{fmt_num(ch)} \u00b7 {fmt_chg(pt)}</font>', ps("bc", fontSize=7, leading=10, textColor=cc2))],
    ]
    ct = Table(cd, colWidths=[cw - 2*mm])
    ct.setStyle(TableStyle([('TOPPADDING',(0,0),(-1,-1),0),('BOTTOMPADDING',(0,0),(-1,-1),0.5*mm),('LEFTPADDING',(0,0),(-1,-1),0)]))
    bcells.append(ct)

bt = Table([bcells], colWidths=[cw]*5, hAlign='LEFT')
bt.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),0),('RIGHTPADDING',(0,0),(-1,-1),0)]))
story.append(bt)
story.append(Spacer(1, 5*mm))


# ═══════ 03 TOP MOVERS ═══════
story.append(SectionHeader(3, "Top Movers", "Leaders & Laggards"))
story.append(Spacer(1, 3*mm))

g_list = top_gainers.to_dict("records") if len(top_gainers) > 0 else []
l_list = top_losers.to_dict("records") if len(top_losers) > 0 else []

def mover_col(title, arrow, label, items, is_gain=True):
    color = C_GREEN if is_gain else C_RED
    rows = []
    # Header row as single merged paragraph
    hdr_text = f'<font name="{FONT_B}" size="9" color="#{C_DARK.hexval()[2:]}">Top {title}</font>'
    lbl_text = f'<font name="{FONT}" size="6" color="#{C_MUTED.hexval()[2:]}">{arrow} {label.upper()}</font>'
    rows.append([
        Paragraph(hdr_text, ps(f"mh{title}", fontName=FONT_B, fontSize=9, leading=12, textColor=C_DARK)),
        '', '',
        Paragraph(lbl_text, ps(f"ml{title}", fontSize=6, textColor=C_MUTED, alignment=TA_RIGHT)),
    ])
    for i, item in enumerate(items[:5]):
        sym = item.get("symbol",""); cl = item.get("close",0); pt = item.get("chg_pct",0)
        rows.append([
            Paragraph(f'<font name="{FONT}" size="7" color="#{C_MUTED.hexval()[2:]}">{i+1:02d}</font>', s_small),
            Paragraph(f'<font name="{FONT_B}" size="8">{sym}</font>', ps(f"ms{i}{title}", fontName=FONT_B, fontSize=8)),
            Paragraph(f'<font name="{MONO}" size="8">{rupee(cl)}</font>', ps(f"mp{i}{title}", fontName=MONO, fontSize=8, alignment=TA_RIGHT)),
            Paragraph(f'<font name="{FONT_B}" size="8" color="#{color.hexval()[2:]}">{fmt_chg(pt)}</font>', ps(f"mc{i}{title}", fontName=FONT_B, fontSize=8, textColor=color, alignment=TA_RIGHT)),
        ])
    t = Table(rows, colWidths=[8*mm, 25*mm, 20*mm, 19*mm])
    t.setStyle(TableStyle([
        ('SPAN',(0,0),(1,0)),
        ('TOPPADDING',(0,0),(-1,-1),1.8*mm), ('BOTTOMPADDING',(0,0),(-1,-1),1.8*mm),
        ('LINEBELOW',(0,0),(-1,0),0.5,C_BORDER), ('LINEBELOW',(0,1),(-1,-1),0.3,HexColor("#f3f4f6")),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
    ]))
    return t

gc1 = mover_col("Gainers", "\u25B2", "Advancing", g_list, True)
lc1 = mover_col("Losers", "\u25BC", "Declining", l_list, False)

mt = Table([[gc1, lc1]], colWidths=[80*mm, 80*mm], hAlign='LEFT')
mt.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),0),('RIGHTPADDING',(0,0),(0,0),4*mm)]))
story.append(mt)
story.append(Spacer(1, 5*mm))


# ═══════ 04 VOLUME SHOCKERS ═══════
vs_list = vol_shockers.to_dict("records") if len(vol_shockers) > 0 else []
vs_cells = []
card_w = page_w / 5 - 2*mm
for v in vs_list[:5]:
    sym = v.get("symbol",""); ratio = v.get("vol_ratio",0); cl = v.get("close",0); pt = v.get("chg_pct",0)
    pc = chg_color(pt)
    cd = [
        [Paragraph(f'<font name="{FONT_B}" size="7.5">{sym}</font>', ps("vs1", fontName=FONT_B, fontSize=7.5, textColor=C_DARK))],
        [Paragraph(f'<font name="{MONO_B}" size="18">{ratio:.1f}</font><font name="{MONO}" size="9">\u00d7</font>', ps("vr1", fontName=MONO_B, fontSize=18, leading=22, textColor=C_DARK))],
        [Paragraph(f'<font name="{MONO}" size="7" color="#{C_MUTED.hexval()[2:]}">{rupee(cl)}</font>', ps("vp1", fontName=MONO, fontSize=7, textColor=C_MUTED))],
        [Paragraph(f'<font name="{FONT_B}" size="7.5" color="#{pc.hexval()[2:]}">{fmt_chg(pt)}</font>', ps("vc1", fontName=FONT_B, fontSize=7.5, textColor=pc))],
    ]
    ct = Table(cd, colWidths=[card_w])
    ct.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,-1),C_BG_CARD), ('BOX',(0,0),(-1,-1),0.5,C_BORDER),
        ('TOPPADDING',(0,0),(0,0),3*mm), ('BOTTOMPADDING',(0,-1),(-1,-1),3*mm),
        ('TOPPADDING',(0,1),(-1,-2),1*mm), ('BOTTOMPADDING',(0,0),(-1,-2),1*mm),
        ('LEFTPADDING',(0,0),(-1,-1),3*mm), ('RIGHTPADDING',(0,0),(-1,-1),3*mm),
        ('ROUNDEDCORNERS',[2*mm,2*mm,2*mm,2*mm]),
    ]))
    vs_cells.append(ct)

if vs_cells:
    while len(vs_cells) < 5: vs_cells.append(Paragraph('', s_body))
    vt = Table([vs_cells], colWidths=[page_w/5]*5, hAlign='LEFT')
    vt.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),1*mm),('RIGHTPADDING',(0,0),(-1,-1),1*mm)]))
    # Wrap section header + cards in KeepTogether so they don't split across pages
    story.append(KeepTogether([
        SectionHeader(4, "Volume Shockers", "Above 1.5\u00d7 Average"),
        Spacer(1, 3*mm),
        vt,
    ]))

story.append(Spacer(1, 3*mm))

# ═══════ PAGE BREAK ═══════
story.append(PageBreak())
story.append(Spacer(1, 8*mm))


# ═══════ 05 NEAR 52-WEEK HIGHS ═══════
story.append(SectionHeader(5, "Near 52-Week Highs", "Within 2% of Peak"))
story.append(Spacer(1, 3*mm))

w52_list = week52_highs.to_dict("records") if len(week52_highs) > 0 else []
w52_cells = []
for w in w52_list[:5]:
    sym = w.get("symbol",""); cl = w.get("close",0); hi = w.get("high_52w",0); gap = w.get("dist_52w",0)
    cd = [
        [Paragraph(f'<font name="{FONT_B}" size="7.5">{sym}</font>', ps("ws1", fontName=FONT_B, fontSize=7.5, textColor=C_DARK))],
        [Paragraph(f'<font name="{MONO_B}" size="11">{rupee(cl)}</font>', ps("wp1", fontName=MONO_B, fontSize=11, leading=14, textColor=C_DARK))],
        [Paragraph(f'<font name="{MONO}" size="7" color="#{C_MUTED.hexval()[2:]}">52W {rupee(hi)}</font>', ps("wh1", fontName=MONO, fontSize=7, textColor=C_MUTED))],
        [GapBar(gap, width=card_w - 6*mm)],
        [Paragraph(f'<font name="{FONT}" size="7" color="#{C_TEAL.hexval()[2:]}">{gap:.1f}% gap</font>', ps("wg1", fontSize=7, textColor=C_TEAL))],
    ]
    ct = Table(cd, colWidths=[card_w])
    ct.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,-1),C_BG_CARD), ('BOX',(0,0),(-1,-1),0.5,C_BORDER),
        ('TOPPADDING',(0,0),(0,0),3*mm), ('BOTTOMPADDING',(0,-1),(-1,-1),3*mm),
        ('TOPPADDING',(0,1),(-1,-2),1*mm), ('BOTTOMPADDING',(0,0),(-1,-2),1*mm),
        ('LEFTPADDING',(0,0),(-1,-1),3*mm), ('RIGHTPADDING',(0,0),(-1,-1),3*mm),
        ('ROUNDEDCORNERS',[2*mm,2*mm,2*mm,2*mm]),
    ]))
    w52_cells.append(ct)

if w52_cells:
    while len(w52_cells) < 5: w52_cells.append(Paragraph('', s_body))
    wt = Table([w52_cells], colWidths=[page_w/5]*5, hAlign='LEFT')
    wt.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),1*mm),('RIGHTPADDING',(0,0),(-1,-1),1*mm)]))
    story.append(wt)
story.append(Spacer(1, 6*mm))


# ═══════ 06 SECTORAL PERFORMANCE ═══════
story.append(SectionHeader(6, "Sectoral Performance", "1D \u00b7 7D Returns"))
story.append(Spacer(1, 3*mm))

if len(sector_df) > 0:
    sd = sector_df.sort_values("chg_1d", ascending=False)
    mx = max(abs(sd["chg_1d"].max()), abs(sd["chg_1d"].min()), 0.5)
    all_s = sd.to_dict("records")
    mid = (len(all_s)+1)//2; ls = all_s[:mid]; rs = all_s[mid:]

    def sec_col(sectors):
        rows = []
        for s in sectors:
            p1 = s["chg_1d"]; p7 = s["chg_7d"]; c1 = chg_color(p1); c7 = chg_color(p7)
            rows.append([
                Paragraph(f'<font name="{FONT}" size="7.5">{s["name"]}</font>', ps("sn1", fontSize=7.5)),
                SectorBar(p1, max_pct=mx, width=32*mm),
                Paragraph(f'<font name="{FONT_B}" size="7.5" color="#{c1.hexval()[2:]}">{fmt_chg(p1)}</font>', ps("s1a", fontName=FONT_B, fontSize=7.5, textColor=c1, alignment=TA_RIGHT)),
                Paragraph(f'<font name="{FONT}" size="7" color="#{c7.hexval()[2:]}">{fmt_chg(p7)}</font>', ps("s7a", fontSize=7, textColor=c7, alignment=TA_RIGHT)),
            ])
        t = Table(rows, colWidths=[22*mm, 24*mm, 16*mm, 16*mm])
        t.setStyle(TableStyle([
            ('TOPPADDING',(0,0),(-1,-1),1.5*mm), ('BOTTOMPADDING',(0,0),(-1,-1),1.5*mm),
            ('VALIGN',(0,0),(-1,-1),'MIDDLE'), ('LINEBELOW',(0,0),(-1,-2),0.2,HexColor("#f3f4f6")),
        ]))
        return t

    st = Table([[sec_col(ls), sec_col(rs)]], colWidths=[84*mm, 84*mm], hAlign='LEFT')
    st.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),0),('RIGHTPADDING',(0,0),(0,0),0)]))
    story.append(st)
story.append(Spacer(1, 6*mm))


# ═══════ 07 EARNINGS SPOTLIGHT ═══════
story.append(SectionHeader(7, "Earnings Spotlight", "Recent & Upcoming"))
story.append(Spacer(1, 3*mm))

def recent_col():
    rows = []
    dt = recent_results[0]["date"] if recent_results else ""
    try: dt = datetime.strptime(dt,"%Y-%m-%d").strftime("%d %b")
    except: pass
    rows.append([
        Paragraph(f'<font name="{FONT_B}" size="8.5">Recent Beats \u00b7 {dt}</font>', ps("rh1", fontName=FONT_B, fontSize=8.5, textColor=C_DARK)),
        '',
        Paragraph(f'<font name="{FONT}" size="6" color="#{C_MUTED.hexval()[2:]}">{len(recent_results):02d} {spaced("Results")}</font>', ps("rc1", fontSize=6, textColor=C_MUTED, alignment=TA_RIGHT)),
    ])
    for r in recent_results[:10]:
        sig = r.get("signal","")
        rows.append([
            Paragraph(f'<font name="{FONT_B}" size="6" color="#ffffff">&nbsp;BEAT&nbsp;</font>', ps("bb1", fontName=FONT_B, fontSize=6, textColor=C_WHITE, backColor=C_GREEN)),
            Paragraph(f'<font name="{FONT_B}" size="7">{r["symbol"]}</font> <font name="{FONT}" size="6" color="#{C_MUTED.hexval()[2:]}">{sig[:25]}</font>', ps("rs1", fontName=FONT_B, fontSize=7)),
            Paragraph(f'<font name="{MONO}" size="7">{rupee(r.get("price",0))}</font>', ps("rp1", fontName=MONO, fontSize=7, alignment=TA_RIGHT)),
        ])
    t = Table(rows, colWidths=[13*mm, 42*mm, 22*mm])
    t.setStyle(TableStyle([
        ('SPAN',(0,0),(1,0)),
        ('TOPPADDING',(0,0),(-1,-1),1.5*mm), ('BOTTOMPADDING',(0,0),(-1,-1),1.5*mm),
        ('LINEBELOW',(0,0),(-1,0),0.5,C_BORDER), ('LINEBELOW',(0,1),(-1,-1),0.2,HexColor("#f3f4f6")),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
    ]))
    return t

def upcoming_col():
    rows = []
    rows.append([
        Paragraph(f'<font name="{FONT_B}" size="8.5">Upcoming \u00b7 Next 3 Days</font>', ps("uh1", fontName=FONT_B, fontSize=8.5, textColor=C_DARK)),
        '',
        Paragraph(f'<font name="{FONT}" size="6" color="#{C_MUTED.hexval()[2:]}">{len(upcoming_earnings):02d} {spaced("Results")}</font>', ps("uc1", fontSize=6, textColor=C_MUTED, alignment=TA_RIGHT)),
    ])
    tomorrow = (datetime.now() + timedelta(days=1)).date()
    for e in upcoming_earnings[:10]:
        ed = datetime.strptime(e["date"],"%Y-%m-%d").date()
        dl = e["day"].upper()[:3]
        dc = C_TEAL if ed == tomorrow else C_MUTED
        eps_s = f"EPS {e['est_eps']}" if e.get("est_eps") else ""
        rev_s = f"Rev {e['est_rev']}" if e.get("est_rev") else ""
        det = f"{eps_s} \u00b7 {rev_s}" if eps_s and rev_s else eps_s or rev_s
        rows.append([
            Paragraph(f'<font name="{FONT_B}" size="6" color="#{dc.hexval()[2:]}">&nbsp;{dl}&nbsp;</font>', ps("db1", fontName=FONT_B, fontSize=6, textColor=dc)),
            Paragraph(f'<font name="{FONT_B}" size="7">{e["symbol"]}</font> <font name="{FONT}" size="6" color="#{C_MUTED.hexval()[2:]}">{det}</font>', ps("us1", fontName=FONT_B, fontSize=7)),
            Paragraph(f'<font name="{MONO}" size="7">{rupee(e.get("price",0))}</font>', ps("up1", fontName=MONO, fontSize=7, alignment=TA_RIGHT)),
        ])
    t = Table(rows, colWidths=[11*mm, 44*mm, 22*mm])
    t.setStyle(TableStyle([
        ('SPAN',(0,0),(1,0)),
        ('TOPPADDING',(0,0),(-1,-1),1.5*mm), ('BOTTOMPADDING',(0,0),(-1,-1),1.5*mm),
        ('LINEBELOW',(0,0),(-1,0),0.5,C_BORDER), ('LINEBELOW',(0,1),(-1,-1),0.2,HexColor("#f3f4f6")),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
    ]))
    return t

if recent_results or upcoming_earnings:
    ecols = []
    ecols.append(recent_col() if recent_results else Paragraph('', s_body))
    ecols.append(upcoming_col() if upcoming_earnings else Paragraph('', s_body))
    et = Table([ecols], colWidths=[82*mm, 82*mm], hAlign='LEFT')
    et.setStyle(TableStyle([
        ('VALIGN',(0,0),(-1,-1),'TOP'), ('LEFTPADDING',(0,0),(-1,-1),3*mm),
        ('RIGHTPADDING',(0,0),(-1,-1),3*mm), ('TOPPADDING',(0,0),(-1,-1),2*mm),
        ('BOTTOMPADDING',(0,0),(-1,-1),2*mm),
        ('BOX',(0,0),(-1,-1),0.5,C_BORDER), ('BACKGROUND',(0,0),(-1,-1),C_BG_CARD),
        ('ROUNDEDCORNERS',[2*mm,2*mm,2*mm,2*mm]),
    ]))
    story.append(et)

story.append(PageBreak())
story.append(Spacer(1, 8*mm))


# ═══════ 08 THE FOCUS TEN ═══════
story.append(SectionHeader(8, "The Focus Ten", "Highest Conviction Picks"))
story.append(Spacer(1, 1*mm))
story.append(Paragraph(f'<font name="{FONT_I}" size="7" color="#6b7280">Ranked by proprietary conviction score \u00b7 Price levels &amp; targets as of close</font>', s_small))
story.append(Spacer(1, 3*mm))

if focus_stocks:
    frows = []
    hdr = [
        Paragraph(f'<font name="{FONT}" size="6" color="#{C_MUTED.hexval()[2:]}">#</font>', s_small),
        Paragraph(f'<font name="{FONT}" size="6" color="#{C_MUTED.hexval()[2:]}">{spaced("Symbol")}</font>', s_small),
        Paragraph(f'<font name="{FONT}" size="6" color="#{C_MUTED.hexval()[2:]}">{spaced("Score")}</font>', ps("fh3", fontSize=6, textColor=C_MUTED, alignment=TA_CENTER)),
        Paragraph(f'<font name="{FONT}" size="6" color="#{C_MUTED.hexval()[2:]}">{spaced("CMP")}</font>', ps("fh4", fontSize=6, textColor=C_MUTED, alignment=TA_RIGHT)),
        Paragraph(f'<font name="{FONT}" size="6" color="#{C_MUTED.hexval()[2:]}">{spaced("Entry")}</font>', ps("fh5", fontSize=6, textColor=C_MUTED, alignment=TA_RIGHT)),
        Paragraph(f'<font name="{FONT}" size="6" color="#{C_MUTED.hexval()[2:]}">{spaced("Holding")}</font>', ps("fh6", fontSize=6, textColor=C_MUTED, alignment=TA_CENTER)),
        Paragraph(f'<font name="{FONT}" size="6" color="#{C_MUTED.hexval()[2:]}">{spaced("Target")}</font>', ps("fh7", fontSize=6, textColor=C_MUTED, alignment=TA_RIGHT)),
    ]
    frows.append(hdr)

    for i, fs in enumerate(focus_stocks[:10]):
        sc = fs.get("score",0); en = fs.get("entry",0); tg = fs.get("target",0)
        hold = "2\u20135 Days" if fs.get("source") == "Pullback" else "5\u201310 Days"
        gp = ((tg-en)/en*100) if en > 0 else 0
        if sc >= 90: scc = C_SCORE_GREEN
        elif sc >= 80: scc = C_TEAL
        else: scc = C_SCORE_AMBER

        frows.append([
            Paragraph(f'<font name="{FONT_B}" size="9">{i+1:02d}</font>', ps(f"fn{i}", fontName=FONT_B, fontSize=9, leading=14, textColor=C_DARK)),
            Paragraph(f'<font name="{FONT_B}" size="9">{fs["symbol"]}</font>', ps(f"fs{i}", fontName=FONT_B, fontSize=9, leading=14, textColor=C_DARK)),
            Paragraph(f'<font name="{FONT_B}" size="8" color="#{scc.hexval()[2:]}">{sc}</font>', ps(f"fsc{i}", fontName=FONT_B, fontSize=8, leading=14, textColor=scc, alignment=TA_CENTER)),
            Paragraph(f'<font name="{MONO}" size="8">{rupee(en)}</font>', ps(f"fcm{i}", fontName=MONO, fontSize=8, leading=14, alignment=TA_RIGHT)),
            Paragraph(f'<font name="{MONO}" size="8">{rupee(en)}</font>', ps(f"fen{i}", fontName=MONO, fontSize=8, leading=14, alignment=TA_RIGHT)),
            Paragraph(f'<font name="{FONT}" size="7.5" color="#{C_TEAL.hexval()[2:]}">{hold}</font>', ps(f"fho{i}", fontSize=7.5, leading=14, textColor=C_TEAL, alignment=TA_CENTER)),
            Paragraph(f'<font name="{MONO_B}" size="8">{rupee(tg)}</font><br/><font name="{FONT}" size="6" color="#{C_GREEN.hexval()[2:]}">+{gp:.1f}%</font>', ps(f"fta{i}", fontName=MONO_B, fontSize=8, leading=11, textColor=C_DARK, alignment=TA_RIGHT)),
        ])

    ft = Table(frows, colWidths=[10*mm, 30*mm, 14*mm, 22*mm, 22*mm, 22*mm, 30*mm])
    ft.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,0),HexColor("#f8fafc")), ('LINEBELOW',(0,0),(-1,0),0.8,C_BORDER),
        ('TOPPADDING',(0,0),(-1,-1),2.5*mm), ('BOTTOMPADDING',(0,0),(-1,-1),2.5*mm),
        ('LINEBELOW',(0,1),(-1,-1),0.3,HexColor("#f3f4f6")),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('LEFTPADDING',(0,0),(-1,-1),2*mm), ('RIGHTPADDING',(0,0),(-1,-1),2*mm),
        ('BOX',(0,0),(-1,-1),0.5,C_BORDER), ('ROUNDEDCORNERS',[2*mm,2*mm,2*mm,2*mm]),
    ]))
    story.append(ft)
    story.append(Spacer(1, 5*mm))

    # Key Technical Signals
    story.append(Paragraph(f'<font name="{FONT_B}" size="9">Key Technical Signals \u2014 Top 3</font>', ps("kts1", fontName=FONT_B, fontSize=9, textColor=C_DARK)))
    story.append(Spacer(1, 1*mm))
    story.append(Paragraph(f'<font name="{FONT}" size="6" color="#{C_MUTED.hexval()[2:]}">{spaced("Indicator Confluence")}</font>', ps("ic1", fontSize=6, textColor=C_MUTED, alignment=TA_RIGHT)))
    story.append(Spacer(1, 2*mm))

    sig_rows = []
    for fs in focus_stocks[:3]:
        sigs = fs.get("signals","")
        parts = [s.strip() for s in sigs.replace("|",",").split(",") if s.strip()]
        badges = ""
        for sp in parts[:3]:
            badges += f'<font name="{FONT_B}" size="6" color="#{C_TEAL.hexval()[2:]}">&nbsp;{sp.upper()[:20]}&nbsp;</font>  '
        desc = ""
        sl = sigs.lower()
        if "perfect ema" in sl or "ema stack" in sl:
            desc = "Clean trend structure intact"
        elif "supertrend" in sl:
            desc = "Momentum strong but monitor for exhaustion"
        elif "rising" in sl:
            desc = "Uptrend with rising moving averages"
        else:
            desc = sigs[:60]
        sig_rows.append([
            Paragraph(f'<font name="{FONT_B}" size="8">{fs["symbol"]}</font>', ps("ss1", fontName=FONT_B, fontSize=8, textColor=C_DARK)),
            Paragraph(badges, ps("sb1", fontName=FONT_B, fontSize=6, textColor=C_TEAL)),
            Paragraph(f'<font name="{FONT}" size="7" color="#{C_MUTED.hexval()[2:]}">{desc}</font>', ps("sd1", fontSize=7, textColor=C_MUTED)),
        ])

    if sig_rows:
        srt = Table(sig_rows, colWidths=[22*mm, 60*mm, 68*mm])
        srt.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,-1),C_BG_CARD), ('BOX',(0,0),(-1,-1),0.5,C_BORDER),
            ('TOPPADDING',(0,0),(-1,-1),2*mm), ('BOTTOMPADDING',(0,0),(-1,-1),2*mm),
            ('LEFTPADDING',(0,0),(-1,-1),3*mm), ('LINEBELOW',(0,0),(-1,-2),0.3,HexColor("#f3f4f6")),
            ('VALIGN',(0,0),(-1,-1),'MIDDLE'), ('ROUNDEDCORNERS',[2*mm,2*mm,2*mm,2*mm]),
        ]))
        story.append(srt)

story.append(Spacer(1, 5*mm))


# ═══════ QUOTE ═══════
quote_text, quote_author = random.choice(QUOTES)
q = quote_text.strip('"').strip('\u201c').strip('\u201d')

qdata = [
    [Paragraph(f'<font name="{FONT}" size="16" color="#{C_TEAL.hexval()[2:]}">\u275D</font>', ps("qq1", fontSize=16, textColor=C_TEAL)),
     Paragraph(f'<font name="{FONT_I}" size="9">{q}</font>', ps("qt1", fontName=FONT_I, fontSize=9, leading=14, textColor=C_DARK))],
    ['', Paragraph(f'<font name="{FONT}" size="7.5">\u2014 {spaced(quote_author.upper())}</font>', ps("qa1", fontSize=7.5, textColor=C_TEAL))],
]
qt2 = Table(qdata, colWidths=[10*mm, page_w-20*mm])
qt2.setStyle(TableStyle([
    ('VALIGN',(0,0),(-1,-1),'TOP'), ('TOPPADDING',(0,0),(-1,-1),3*mm), ('BOTTOMPADDING',(0,0),(-1,-1),2*mm),
    ('LEFTPADDING',(0,0),(-1,-1),3*mm),
    ('BACKGROUND',(0,0),(-1,-1),HexColor("#f0fdfa")), ('BOX',(0,0),(-1,-1),0.5,C_TEAL),
    ('ROUNDEDCORNERS',[2*mm,2*mm,2*mm,2*mm]),
]))
story.append(qt2)
story.append(Spacer(1, 5*mm))


# ═══════ DISCLAIMER + BRANDING (side by side) ═══════
disc_para = Paragraph(
    f'<font name="{FONT_B}" size="7" color="#{C_RED.hexval()[2:]}">Disclaimer:</font> '
    f'<font name="{FONT}" size="6" color="#{C_MUTED.hexval()[2:]}">This report is for educational purposes only and does not constitute investment advice. '
    f'Data is derived from publicly available sources and may contain errors. Always conduct your own research before making '
    f'trading or investment decisions. Past performance does not guarantee future results.</font>',
    ps("disc1", fontSize=6, leading=8, textColor=C_MUTED)
)
brand_para = Paragraph(
    f'<font name="{FONT_B}" size="10">Quant</font><font name="{FONT_B}" size="10" color="#{C_TEAL.hexval()[2:]}">ex</font><br/>'
    f'<font name="{FONT}" size="7" color="#{C_MUTED.hexval()[2:]}">#PreMarket</font>',
    ps("br1", fontName=FONT_B, fontSize=10, textColor=C_DARK, alignment=TA_RIGHT)
)
footer_t = Table([[disc_para, brand_para]], colWidths=[page_w - 30*mm, 30*mm])
footer_t.setStyle(TableStyle([
    ('VALIGN',(0,0),(-1,-1),'TOP'),
    ('LEFTPADDING',(0,0),(-1,-1),0), ('RIGHTPADDING',(0,0),(-1,-1),0),
]))
story.append(footer_t)


# ═══════ BUILD PDF ═══════

# BUILD
doc = SimpleDocTemplate(str(PDF_PATH), pagesize=A4,
    leftMargin=15*mm, rightMargin=15*mm, topMargin=8*mm, bottomMargin=16*mm)
doc.build(story, onFirstPage=page1_template, onLaterPages=later_page_template)
print(f">> PDF saved: {PDF_PATH}")

# 6. SEND PDF VIA TELEGRAM
# ═══════════════════════════════════════════════════════════════

def send_telegram_document(file_path, chat_id, caption=""):
    """Send a PDF document to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    try:
        with open(file_path, "rb") as f:
            resp = requests.post(url, data={
                "chat_id": chat_id,
                "caption": caption,
                "parse_mode": "HTML",
            }, files={"document": (os.path.basename(file_path), f, "application/pdf")}, timeout=60)
        if resp.ok:
            print(f"   Sent to {chat_id}")
            return True
        else:
            print(f"   Error: {resp.text[:200]}")
            return False
    except Exception as e:
        print(f"   Telegram error: {e}")
        return False


caption = (
    f"<b>Quantex Pre-Market Report</b>\n"
    f"{TODAY}\n\n"
    f"Fear & Greed: <b>{fg_score}/100 ({fg_label})</b>\n"
    f"Nifty: <b>{indian_data.get('Nifty 50', {}).get('close', 'N/A')}</b>  |  "
    f"Sensex: <b>{indian_data.get('Sensex', {}).get('close', 'N/A')}</b>\n"
    f"VIX: <b>{indian_data.get('India VIX', {}).get('close', 'N/A')}</b>"
)

sent = False

if TELEGRAM_CHAT_ID:
    print(f"\n>> Sending PDF to personal chat...")
    send_telegram_document(PDF_PATH, TELEGRAM_CHAT_ID, caption)
    sent = True

if TELEGRAM_SIGNAL_GROUPS:
    for gid in TELEGRAM_SIGNAL_GROUPS.split(","):
        gid = gid.strip()
        if gid:
            print(f">> Sending PDF to signal group {gid}...")
            send_telegram_document(PDF_PATH, gid, caption)
            sent = True

if TELEGRAM_ADMIN_GROUPS:
    for gid in TELEGRAM_ADMIN_GROUPS.split(","):
        gid = gid.strip()
        if gid:
            print(f">> Sending PDF to admin group {gid}...")
            send_telegram_document(PDF_PATH, gid, caption)
            sent = True

if not sent:
    print("!! No Telegram credentials found. PDF saved locally only.")


# ═══════════════════════════════════════════════════════════════
# 7. SAVE LOG
# ═══════════════════════════════════════════════════════════════

log_data = {
    "date": TODAY,
    "fear_greed": fg_score,
    "fear_greed_label": fg_label,
    "gift_nifty": gift_close,
    "nifty": indian_data.get("Nifty 50", {}).get("close"),
    "sensex": indian_data.get("Sensex", {}).get("close"),
    "bank_nifty": indian_data.get("Bank Nifty", {}).get("close"),
    "vix": indian_data.get("India VIX", {}).get("close"),
    "top_gainers": [r["symbol"] for r in g_list[:5]],
    "top_losers": [r["symbol"] for r in l_list[:5]],
    "pdf_path": str(PDF_PATH),
}

log_file = LOG_DIR / "premarket_pulse.json"
try:
    history = json.loads(log_file.read_text()) if log_file.exists() else []
    history.append(log_data)
    history = history[-90:]
    log_file.write_text(json.dumps(history, indent=2))
    print(f">> Log saved: {log_file}")
except Exception as e:
    print(f"!! Log save error: {e}")

print(f"\n>> Pre-Market Pulse complete!")
