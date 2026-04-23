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
# ── 1b. COMMODITIES, CRYPTO, CURRENCIES ──
print(">> Fetching commodities, crypto & currencies...")
COMMODITIES = {
    "Gold (Spot)": "GC=F",
    "Silver (Spot)": "SI=F",
    "Brent Crude": "BZ=F",
    "WTI Crude": "CL=F",
}
CRYPTO = {
    "Bitcoin": "BTC-USD",
    "Ethereum": "ETH-USD",
    "BNB": "BNB-USD",
    "Solana": "SOL-USD",
    "XRP": "XRP-USD",
    "Dogecoin": "DOGE-USD",
    "Cardano": "ADA-USD",
    "Avalanche": "AVAX-USD",
    "Polkadot": "DOT-USD",
    "Polygon": "POL-USD",
}
CURRENCIES = {
    "EUR/USD": "EURUSD=X",
    "GBP/USD": "GBPUSD=X",
    "USD/JPY": "JPY=X",
}
commodity_data = {}
for name, ticker in COMMODITIES.items():
    df = fetch_quote(ticker)
    close, chg, pct = get_change(df)
    commodity_data[name] = {"close": close, "chg": chg, "pct": pct}

crypto_data = {}
for name, ticker in CRYPTO.items():
    df = fetch_quote(ticker)
    close, chg, pct = get_change(df)
    crypto_data[name] = {"close": close, "chg": chg, "pct": pct}

currency_data = {}
for name, ticker in CURRENCIES.items():
    df = fetch_quote(ticker)
    close, chg, pct = get_change(df)
    currency_data[name] = {"close": close, "chg": chg, "pct": pct}
# Add USD/INR from indian_data
currency_data["USD/INR"] = indian_data.get("USD/INR", {})

# ── 1c. NIFTY/BANKNIFTY PIVOT LEVELS ──
print(">> Computing Nifty & BankNifty pivot levels...")
pivot_data = {}
for idx_name, idx_ticker in [("Nifty 50", "^NSEI"), ("Bank Nifty", "^NSEBANK")]:
    try:
        pdf_df = yf.download(idx_ticker, period="5d", progress=False)
        if pdf_df is not None and len(pdf_df) >= 2:
            if isinstance(pdf_df.columns, pd.MultiIndex):
                pdf_df.columns = pdf_df.columns.get_level_values(0)
            # Use previous day's OHLC for pivot calculation
            prev = pdf_df.iloc[-2]
            h = float(prev["High"])
            l = float(prev["Low"])
            c = float(prev["Close"])
            pp = round((h + l + c) / 3, 2)

            # Classic Pivots
            classic = {
                "PP": pp,
                "R1": round(2 * pp - l, 2),
                "R2": round(pp + (h - l), 2),
                "R3": round(h + 2 * (pp - l), 2),
                "S1": round(2 * pp - h, 2),
                "S2": round(pp - (h - l), 2),
                "S3": round(l - 2 * (h - pp), 2),
            }

            # Fibonacci Pivots
            diff = h - l
            fib = {
                "PP": pp,
                "R1": round(pp + 0.382 * diff, 2),
                "R2": round(pp + 0.618 * diff, 2),
                "R3": round(pp + 1.000 * diff, 2),
                "S1": round(pp - 0.382 * diff, 2),
                "S2": round(pp - 0.618 * diff, 2),
                "S3": round(pp - 1.000 * diff, 2),
            }

            pivot_data[idx_name] = {"classic": classic, "fibonacci": fib}
            print(f"   {idx_name}: PP={pp:.0f} | R1={classic['R1']:.0f} R2={classic['R2']:.0f} | S1={classic['S1']:.0f} S2={classic['S2']:.0f}")
    except Exception as e:
        print(f"   {idx_name} pivot error: {e}")

# ── 1d. FEAR & GREED WEEKLY CHANGE ──
# Load previous F&G from saved JSON for weekly comparison
fg_prev = None
try:
    fg_log = LOG_DIR / "premarket_pulse.json"
    if fg_log.exists():
        with open(fg_log) as f:
            fg_hist = json.load(f)
        # Get F&G from ~7 days ago
        if isinstance(fg_hist, dict) and "fear_greed" in fg_hist:
            fg_prev = fg_hist.get("fear_greed_prev")  # stored from previous run
        elif isinstance(fg_hist, list):
            for entry in reversed(fg_hist):
                entry_date = entry.get("date", "")
                if entry_date and entry_date < (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d"):
                    fg_prev = entry.get("fear_greed")
                    break
except Exception:
    pass

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
# ── 5. FII/DII DATA (Last 7 trading days) ──
print(">> Fetching FII/DII activity data (last 7 days)...")
fii_dii_data = {}       # Latest day (for summary)
fii_dii_history = []    # Last 7 days date-wise
try:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://www.nseindia.com/reports-indices",
    }

    nse_session = requests.Session()
    nse_session.get("https://www.nseindia.com", headers=headers, timeout=10)

    # Try NSE API for latest data
    resp = nse_session.get("https://www.nseindia.com/api/fiidiiTradeReact", headers=headers, timeout=10)

    if resp.status_code == 200:
        fii_dii_raw = resp.json()
        for item in fii_dii_raw:
            cat = item.get("category", "")
            if "FII" in cat or "FPI" in cat:
                fii_dii_data["fii_buy"] = float(item.get("buyValue", 0))
                fii_dii_data["fii_sell"] = float(item.get("sellValue", 0))
                fii_dii_data["fii_net"] = float(item.get("netValue", 0))
                fii_dii_data["fii_date"] = item.get("date", "")
            elif "DII" in cat:
                fii_dii_data["dii_buy"] = float(item.get("buyValue", 0))
                fii_dii_data["dii_sell"] = float(item.get("sellValue", 0))
                fii_dii_data["dii_net"] = float(item.get("netValue", 0))
                fii_dii_data["dii_date"] = item.get("date", "")
        if fii_dii_data:
            print(f"   Latest: FII Net: ₹{fii_dii_data.get('fii_net', 0):,.0f} Cr | DII Net: ₹{fii_dii_data.get('dii_net', 0):,.0f} Cr")
    else:
        print(f"   NSE latest API returned status {resp.status_code}")

    # Try NSE historical API for last 7 days
    try:
        from_date = (datetime.now() - timedelta(days=12)).strftime("%d-%b-%Y")
        to_date = datetime.now().strftime("%d-%b-%Y")
        hist_url = f"https://www.nseindia.com/api/fiidiiTradeReact?from={from_date}&to={to_date}"
        resp_hist = nse_session.get(hist_url, headers=headers, timeout=10)

        if resp_hist.status_code == 200:
            hist_raw = resp_hist.json()
            # Group by date
            date_map = {}
            for item in hist_raw:
                dt = item.get("date", "")
                cat = item.get("category", "")
                if not dt:
                    continue
                if dt not in date_map:
                    date_map[dt] = {"date": dt}
                if "FII" in cat or "FPI" in cat:
                    date_map[dt]["fii_buy"] = float(item.get("buyValue", 0))
                    date_map[dt]["fii_sell"] = float(item.get("sellValue", 0))
                    date_map[dt]["fii_net"] = float(item.get("netValue", 0))
                elif "DII" in cat:
                    date_map[dt]["dii_buy"] = float(item.get("buyValue", 0))
                    date_map[dt]["dii_sell"] = float(item.get("sellValue", 0))
                    date_map[dt]["dii_net"] = float(item.get("netValue", 0))

            # Sort by date descending, take last 7
            sorted_dates = sorted(date_map.values(),
                key=lambda x: datetime.strptime(x["date"], "%d-%b-%Y") if "-" in x["date"] else datetime.min,
                reverse=True)
            fii_dii_history = sorted_dates[:7]
            print(f"   Loaded {len(fii_dii_history)} days of FII/DII history")
        else:
            print(f"   NSE history API returned status {resp_hist.status_code}")
    except Exception as e:
        print(f"   FII/DII history fetch error: {e}")

except Exception as e:
    print(f"   FII/DII fetch error: {e}")

if not fii_dii_data:
    print("   FII/DII data not available — section will be skipped")

# ── 5b. INDEX OI BUILDUP DATA ──
print(">> Fetching Index OI buildup data...")
oi_buildup_data = []
try:
    # OI Buildup Logic:
    #   Price ↑ + OI ↑ = Long Buildup (Bullish)
    #   Price ↓ + OI ↑ = Short Buildup (Bearish)
    #   Price ↑ + OI ↓ = Short Covering (Mildly Bullish)
    #   Price ↓ + OI ↓ = Long Unwinding (Mildly Bearish)

    oi_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://www.nseindia.com",
    }

    # Create NSE session with cookies
    oi_session = requests.Session()
    oi_session.get("https://www.nseindia.com", headers=oi_headers, timeout=10)

    # Fetch index futures OI data from NSE
    indices_to_track = [
        {"name": "NIFTY 50", "symbol": "NIFTY", "nse_sym": "NIFTY"},
        {"name": "BANK NIFTY", "symbol": "BANKNIFTY", "nse_sym": "BANKNIFTY"},
        {"name": "NIFTY FIN SVC", "symbol": "FINNIFTY", "nse_sym": "FINNIFTY"},
    ]

    for idx in indices_to_track:
        try:
            # Method 1: NSE derivatives API for OI data
            url = f"https://www.nseindia.com/api/derivativesAnalysis?index={idx['nse_sym']}"
            resp = oi_session.get(url, headers=oi_headers, timeout=10)

            if resp.status_code == 200:
                data = resp.json()
                fut_data = data.get("futuresAnalysis", {})

                if fut_data:
                    price_chg = float(fut_data.get("underlyingValue", 0)) - float(fut_data.get("previousClose", 0))
                    price_chg_pct = (price_chg / float(fut_data.get("previousClose", 1))) * 100
                    oi_val = float(fut_data.get("openInterest", 0))
                    oi_chg = float(fut_data.get("changeinOpenInterest", 0))
                    oi_chg_pct = (oi_chg / (oi_val - oi_chg)) * 100 if (oi_val - oi_chg) > 0 else 0

                    # Determine buildup type
                    if price_chg > 0 and oi_chg > 0:
                        buildup = "LONG BUILDUP"
                        buildup_color = "#1a7f37"
                        buildup_emoji = "🟢"
                    elif price_chg < 0 and oi_chg > 0:
                        buildup = "SHORT BUILDUP"
                        buildup_color = "#cf222e"
                        buildup_emoji = "🔴"
                    elif price_chg > 0 and oi_chg < 0:
                        buildup = "SHORT COVERING"
                        buildup_color = "#bf8700"
                        buildup_emoji = "🟡"
                    elif price_chg < 0 and oi_chg < 0:
                        buildup = "LONG UNWINDING"
                        buildup_color = "#bc4c00"
                        buildup_emoji = "🟠"
                    else:
                        buildup = "NEUTRAL"
                        buildup_color = "#8c959f"
                        buildup_emoji = "⚪"

                    oi_buildup_data.append({
                        "name": idx["name"],
                        "close": float(fut_data.get("underlyingValue", 0)),
                        "price_chg": round(price_chg, 2),
                        "price_chg_pct": round(price_chg_pct, 2),
                        "oi": oi_val,
                        "oi_chg": oi_chg,
                        "oi_chg_pct": round(oi_chg_pct, 2),
                        "buildup": buildup,
                        "buildup_color": buildup_color,
                        "buildup_emoji": buildup_emoji,
                    })
                    print(f"   {idx['name']}: {buildup} (Price: {price_chg:+.0f}, OI Chg: {oi_chg:+,.0f})")
                    continue

            # Method 2: Fallback — use yfinance futures for basic OI
            # (limited, may not have OI for Indian indices)
            print(f"   {idx['name']}: NSE API failed (status {resp.status_code}), trying yfinance...")

        except Exception as e:
            print(f"   {idx['name']} OI error: {e}")

    if not oi_buildup_data:
        # Method 3: Compute from spot price changes as proxy
        print("   NSE OI API not available — computing from price action proxy...")
        for idx in indices_to_track[:2]:  # Nifty and BankNifty only
            try:
                ticker_map = {"NIFTY": "^NSEI", "BANKNIFTY": "^NSEBANK"}
                yf_sym = ticker_map.get(idx["symbol"])
                if not yf_sym:
                    continue
                df_idx = yf.download(yf_sym, period="5d", progress=False)
                if df_idx is not None and len(df_idx) >= 2:
                    if isinstance(df_idx.columns, pd.MultiIndex):
                        df_idx.columns = df_idx.columns.get_level_values(0)
                    close_now = float(df_idx["Close"].iloc[-1])
                    close_prev = float(df_idx["Close"].iloc[-2])
                    vol_now = float(df_idx["Volume"].iloc[-1])
                    vol_prev = float(df_idx["Volume"].iloc[-2])
                    price_chg = close_now - close_prev
                    price_chg_pct = (price_chg / close_prev) * 100
                    vol_chg = vol_now - vol_prev

                    # Use volume as OI proxy
                    if price_chg > 0 and vol_chg > 0:
                        buildup = "LONG BUILDUP"
                        buildup_color = "#1a7f37"
                    elif price_chg < 0 and vol_chg > 0:
                        buildup = "SHORT BUILDUP"
                        buildup_color = "#cf222e"
                    elif price_chg > 0 and vol_chg < 0:
                        buildup = "SHORT COVERING"
                        buildup_color = "#bf8700"
                    else:
                        buildup = "LONG UNWINDING"
                        buildup_color = "#bc4c00"

                    oi_buildup_data.append({
                        "name": idx["name"],
                        "close": round(close_now, 2),
                        "price_chg": round(price_chg, 2),
                        "price_chg_pct": round(price_chg_pct, 2),
                        "oi": 0,
                        "oi_chg": 0,
                        "oi_chg_pct": 0,
                        "buildup": buildup,
                        "buildup_color": buildup_color,
                        "vol_proxy": True,
                    })
                    print(f"   {idx['name']}: {buildup} (from volume proxy)")
            except Exception as e:
                print(f"   {idx['name']} proxy error: {e}")

    if oi_buildup_data:
        print(f"   Loaded OI buildup for {len(oi_buildup_data)} indices")
    else:
        print("   OI buildup data not available — section will be skipped")

except Exception as e:
    print(f"   OI buildup fetch error: {e}")

# ── 6. EARNINGS DATA ──
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
# ── 6. FOCUS STOCKS FROM PRO SCANNER ──
print(">> Loading focus stocks from Pro Scanner...")
focus_stocks = []
focus_source = ""
try:
    rec_path = BASE_DIR / "quantex_logs" / "recommendations.json"
    if rec_path.exists():
        with open(rec_path) as f:
            rec_data = json.load(f)
        # Get the latest scan
        if isinstance(rec_data, list) and len(rec_data) > 0:
            latest_scan = rec_data[-1]
            scan_date = latest_scan.get("scan_date", "")
            regime = latest_scan.get("market_regime", {})
            regime_label = regime.get("label", "") if regime else ""
            focus_source = f"Pro Scanner ({scan_date})"
            top_recs = latest_scan.get("top_10", [])[:10]
            for r in top_recs:
                focus_stocks.append({
                    "symbol": r["symbol"],
                    "score": r["score"],
                    "entry": r.get("entry", 0),
                    "sl": r.get("sl", 0),
                    "target": r.get("target1", 0),
                    "signals": ", ".join(r.get("signals", [])[:3]),
                    "source": "Pro",
                })
            print(f"   Loaded {len(focus_stocks)} from Pro Scanner ({scan_date})")
            if regime_label:
                print(f"   Market Regime: {regime_label}")
except Exception as e:
    print(f"   Pro Scanner load error: {e}")
# Sort by score and take top 10
focus_stocks = sorted(focus_stocks, key=lambda x: x["score"], reverse=True)[:10]
# ═══════════════════════════════════════════════════════════════
# 7. BUILD PREMIUM PDF REPORT
# ═══════════════════════════════════════════════════════════════
print(">> Building premium PDF report...")
PDF_PATH = LOG_DIR / f"Pre-Market Report-{TODAY_SHORT}.pdf"
from reportlab.graphics.shapes import Drawing, Rect, Line, Circle, Wedge, String, Group
from reportlab.graphics import renderPDF
from reportlab.platypus import Flowable
# ── Premium Light Color Palette ──
C_BG = HexColor("#f8f9fb")
C_CARD = HexColor("#ffffff")
C_BORDER = HexColor("#d0d7de")
C_ACCENT = HexColor("#0969da")
C_GOLD = HexColor("#bf8700")
C_GREEN = HexColor("#1a7f37")
C_RED = HexColor("#cf222e")
C_ORANGE = HexColor("#bc4c00")
C_PURPLE = HexColor("#8250df")
C_CYAN = HexColor("#0e8a7a")
C_TEXT = HexColor("#1f2328")
C_TEXT_DIM = HexColor("#656d76")
C_TEXT_MUTED = HexColor("#8c959f")
C_HEADER_ACCENT = HexColor("#ddf4ff")
C_ROW1 = HexColor("#ffffff")
C_ROW2 = HexColor("#f6f8fa")
# ═══════ CUSTOM FLOWABLES ═══════
class GaugeFlowable(Flowable):
    """Semicircle gauge for Fear & Greed Index with definition legend and weekly change."""
    def __init__(self, score, label, tip, width=170*mm, height=115*mm, prev_score=None):
        Flowable.__init__(self)
        self.score = score
        self.label = label
        self.tip = tip
        self.width = width
        self.height = height
        self.prev_score = prev_score
    def draw(self):
        c = self.canv
        w, h = self.width, self.height
        cx = w / 2
        gauge_cy = h - 50 * mm  # Center of gauge arc

        # Card background
        c.setFillColor(C_CARD)
        c.setStrokeColor(C_BORDER)
        c.setLineWidth(0.5)
        c.roundRect(0, 0, w, h, 8, fill=1, stroke=1)

        # ── GAUGE ARC ──
        radius = 32 * mm
        c.setLineWidth(12)
        c.setStrokeColor(HexColor("#e1e4e8"))
        c.arc(cx - radius, gauge_cy - radius, cx + radius, gauge_cy + radius, 0, 180)

        # Gradient arc segments with labels
        arc_segments = [
            ("#1a7f37", "EXTREME FEAR"),
            ("#57ab5a", "FEAR"),
            ("#bf8700", "GREED"),
            ("#cf222e", "EXTREME GREED"),
        ]
        for i, (col, _) in enumerate(arc_segments):
            c.setStrokeColor(HexColor(col))
            c.setLineWidth(12)
            start = 180 - (i * 45)
            c.arc(cx - radius, gauge_cy - radius, cx + radius, gauge_cy + radius, start, -45)

        # Scale marks: 0, 30, 50, 70, 100
        scale_marks = [
            (0, "0"), (30, "30"), (50, "50"), (70, "70"), (100, "100")
        ]
        for val, txt in scale_marks:
            angle = math.radians(180 - (val / 100 * 180))
            mx = cx + (radius + 6 * mm) * math.cos(angle)
            my = gauge_cy + (radius + 6 * mm) * math.sin(angle)
            c.setFillColor(C_TEXT_DIM)
            c.setFont("Helvetica-Bold", 7)
            c.drawCentredString(mx, my - 2, txt)

        # Needle
        angle_deg = 180 - (self.score / 100 * 180)
        angle_rad = math.radians(angle_deg)
        needle_len = radius - 5 * mm
        nx = cx + needle_len * math.cos(angle_rad)
        ny = gauge_cy + needle_len * math.sin(angle_rad)
        c.setStrokeColor(HexColor("#1f2328"))
        c.setLineWidth(2.5)
        c.line(cx, gauge_cy, nx, ny)

        # Center dot
        c.setFillColor(C_CARD)
        c.setStrokeColor(HexColor("#1f2328"))
        c.setLineWidth(1.5)
        c.circle(cx, gauge_cy, 3.5 * mm, fill=1, stroke=1)

        # Score number below gauge
        c.setFillColor(HexColor("#1f2328"))
        c.setFont("Helvetica-Bold", 22)
        c.drawCentredString(cx, gauge_cy - 14 * mm, str(self.score))

        # Label
        if self.score < 30: lc = "#1a7f37"
        elif self.score < 50: lc = "#57ab5a"
        elif self.score < 70: lc = "#bf8700"
        else: lc = "#cf222e"
        c.setFillColor(HexColor(lc))
        c.setFont("Helvetica-Bold", 13)
        c.drawCentredString(cx, gauge_cy - 20 * mm, self.label)

        # ── DEFINITION LEGEND (left side) ──
        def_x = 10 * mm
        def_y = 28 * mm
        c.setFont("Helvetica-Bold", 8)
        c.setFillColor(HexColor("#656d76"))
        c.drawString(def_x, def_y + 2 * mm, "Definition")

        definitions = [
            ("#1a7f37", "Extreme Fear (<30):", "Good time to open positions"),
            ("#57ab5a", "Fear (30-50):", "Wait for market direction"),
            ("#bf8700", "Greed (50-70):", "Be cautious with new positions"),
            ("#cf222e", "Extreme Greed (>70):", "Avoid opening positions"),
        ]
        for i, (dot_col, zone, advice) in enumerate(definitions):
            y_pos = def_y - (i + 1) * 4.5 * mm
            # Colored dot
            c.setFillColor(HexColor(dot_col))
            c.circle(def_x + 2 * mm, y_pos + 1.5, 1.8 * mm, fill=1, stroke=0)
            # Zone text
            c.setFillColor(HexColor("#1f2328"))
            c.setFont("Helvetica-Bold", 7.5)
            c.drawString(def_x + 6 * mm, y_pos, zone)
            # Advice text
            c.setFillColor(HexColor("#656d76"))
            c.setFont("Helvetica", 7)
            c.drawString(def_x + 38 * mm, y_pos, advice)

        # ── WEEKLY CHANGE BOX (right side) ──
        if self.prev_score is not None:
            box_x = w - 55 * mm
            box_y = 8 * mm
            box_w = 48 * mm
            box_h = 22 * mm

            # Box border
            c.setStrokeColor(C_BORDER)
            c.setLineWidth(0.5)
            c.setFillColor(HexColor("#f6f8fa"))
            c.roundRect(box_x, box_y, box_w, box_h, 4, fill=1, stroke=1)

            # Title
            c.setFillColor(HexColor("#656d76"))
            c.setFont("Helvetica-Bold", 8)
            c.drawCentredString(box_x + box_w / 2, box_y + box_h - 5 * mm, "Weekly Change")

            # Previous → Current
            c.setFillColor(HexColor("#1f2328"))
            c.setFont("Helvetica-Bold", 12)
            c.drawString(box_x + 4 * mm, box_y + 4 * mm, f"{self.prev_score:.1f}")

            c.setFillColor(HexColor("#656d76"))
            c.setFont("Helvetica", 12)
            arrow_x = box_x + 18 * mm
            c.drawCentredString(arrow_x, box_y + 4 * mm, "→")

            chg_color = "#1a7f37" if self.score >= self.prev_score else "#cf222e"
            c.setFillColor(HexColor(chg_color))
            c.setFont("Helvetica-Bold", 12)
            c.drawString(box_x + 24 * mm, box_y + 4 * mm, f"{self.score:.1f}")
class HBarFlowable(Flowable):
    """Horizontal bar chart for sectoral performance."""
    def __init__(self, data, width=170*mm, row_h=14):
        Flowable.__init__(self)
        self.data = data  # list of (name, close, chg_1d, chg_7d)
        self.width = width
        self.row_h = row_h
        self.height = (len(data) + 1) * row_h + 8
    def draw(self):
        c = self.canv
        w = self.width
        rh = self.row_h
        max_val = max(abs(d[2]) for d in self.data) if self.data else 1
        if max_val == 0: max_val = 1
        c.setFillColor(C_CARD)
        c.setStrokeColor(C_BORDER)
        c.setLineWidth(0.5)
        c.roundRect(0, 0, w, self.height, 8, fill=1, stroke=1)
        # Header
        y = self.height - rh - 2
        c.setFillColor(C_HEADER_ACCENT)
        c.rect(1, y, w - 2, rh, fill=1, stroke=0)
        c.setFillColor(C_ACCENT)
        c.setFont("Helvetica-Bold", 8)
        c.drawString(8, y + 4, "Sector")
        c.drawRightString(w * 0.32, y + 4, "Close")
        c.drawCentredString(w * 0.58, y + 4, "1-Day Change")
        c.drawRightString(w - 8, y + 4, "7-Day")
        bar_center = w * 0.58
        bar_max_w = w * 0.16
        for i, (name, close_val, v1d, v7d) in enumerate(self.data):
            y = self.height - (i + 2) * rh - 2
            c.setFillColor(C_ROW2 if i % 2 == 0 else C_ROW1)
            c.rect(1, y, w - 2, rh, fill=1, stroke=0)
            c.setFillColor(C_TEXT)
            c.setFont("Helvetica-Bold", 7.5)
            c.drawString(8, y + 4, name)
            # Close price
            c.setFillColor(C_TEXT)
            c.setFont("Helvetica", 7)
            c.drawRightString(w * 0.32, y + 4, f"{close_val:,.2f}")
            bar_w = (abs(v1d) / max_val) * bar_max_w
            bar_col = C_GREEN if v1d >= 0 else C_RED
            c.setFillColor(bar_col)
            if v1d >= 0:
                c.roundRect(bar_center + 2, y + 3, max(bar_w, 2), rh - 6, 2, fill=1, stroke=0)
            else:
                c.roundRect(bar_center - 2 - bar_w, y + 3, max(bar_w, 2), rh - 6, 2, fill=1, stroke=0)
            c.setFillColor(bar_col)
            c.setFont("Helvetica-Bold", 7)
            sign = "+" if v1d >= 0 else ""
            if v1d >= 0:
                c.drawString(bar_center + bar_w + 5, y + 4, f"{sign}{v1d:.2f}%")
            else:
                c.drawRightString(bar_center - bar_w - 5, y + 4, f"{sign}{v1d:.2f}%")
            col7 = C_GREEN if v7d >= 0 else C_RED
            c.setFillColor(col7)
            c.setFont("Helvetica-Bold", 7)
            s7 = "+" if v7d >= 0 else ""
            c.drawRightString(w - 8, y + 4, f"{s7}{v7d:.2f}%")
            c.setStrokeColor(HexColor("#d0d7de"))
            c.setLineWidth(0.3)
            c.line(1, y, w - 1, y)
class QuoteFlowable(Flowable):
    """Styled motivational quote card."""
    def __init__(self, text, author, width=170*mm):
        Flowable.__init__(self)
        self.text = text
        self.author = author
        self.width = width
        self.height = 30 * mm
    def draw(self):
        c = self.canv
        w, h = self.width, self.height
        # Light gradient card
        for i in range(20):
            frac = i / 20
            r = int((0.93 + frac * 0.04) * 255)
            g = int((0.95 + frac * 0.02) * 255)
            b = int((0.98 + frac * 0.01) * 255)
            c.setFillColor(HexColor(f"#{min(r,255):02x}{min(g,255):02x}{min(b,255):02x}"))
            y = h * (1 - (i + 1) / 20)
            c.rect(0, y, w, h / 20 + 1, fill=1, stroke=0)
        c.setStrokeColor(C_ACCENT)
        c.setLineWidth(0.8)
        c.roundRect(0, 0, w, h, 8, fill=0, stroke=1)
        # Big quote mark
        c.setFillColor(HexColor("#0969da30"))
        c.setFont("Helvetica-Bold", 36)
        c.drawString(10, h - 16 * mm, '"')
        # Quote text (word-wrapped)
        c.setFillColor(HexColor("#1f2328"))
        c.setFont("Helvetica-Oblique", 9)
        words = self.text.replace('"', '').split()
        lines, line = [], ""
        for wd in words:
            test = line + " " + wd if line else wd
            if c.stringWidth(test, "Helvetica-Oblique", 9) < w - 40:
                line = test
            else:
                lines.append(line)
                line = wd
        if line: lines.append(line)
        y_start = h - 9 * mm
        for j, ln in enumerate(lines[:3]):
            c.drawCentredString(w / 2, y_start - j * 11, ln)
        c.setFillColor(C_GOLD)
        c.setFont("Helvetica-Bold", 8.5)
        c.drawCentredString(w / 2, 4 * mm, f"- {self.author}")
# ═══════ STYLES ═══════
styles = getSampleStyleSheet()
s_section = ParagraphStyle("Sec", parent=styles["Heading2"],
    fontSize=12, textColor=C_ACCENT, spaceAfter=5, spaceBefore=14,
    fontName="Helvetica-Bold")
s_subsec = ParagraphStyle("SS", parent=styles["Normal"],
    fontSize=9, textColor=C_GOLD, spaceAfter=3, spaceBefore=8,
    fontName="Helvetica-Bold")
s_cell = ParagraphStyle("C", parent=styles["Normal"],
    fontSize=8, textColor=C_TEXT, fontName="Helvetica", leading=11)
s_cell_r = ParagraphStyle("CR", parent=s_cell, alignment=TA_RIGHT)
s_cell_c = ParagraphStyle("CC", parent=s_cell, alignment=TA_CENTER)
s_cell_b = ParagraphStyle("CB", parent=s_cell, fontName="Helvetica-Bold")
s_hdr = ParagraphStyle("H", parent=s_cell, textColor=C_ACCENT, fontName="Helvetica-Bold")
s_hdr_r = ParagraphStyle("HR", parent=s_hdr, alignment=TA_RIGHT)
s_hdr_c = ParagraphStyle("HC", parent=s_hdr, alignment=TA_CENTER)
s_note = ParagraphStyle("Nt", parent=s_cell, fontSize=7, textColor=C_TEXT_DIM)
s_footer = ParagraphStyle("Ft", parent=s_cell, fontSize=7, textColor=C_TEXT_MUTED, alignment=TA_CENTER)
def p_chg(v):
    if v is None: return Paragraph('<font color="#8c959f">-</font>', s_cell_r)
    c = "#1a7f37" if v >= 0 else "#cf222e"
    return Paragraph(f'<font color="{c}"><b>{"+" if v>=0 else ""}{v:.2f}%</b></font>', s_cell_r)
def p_val(v):
    if v is None: return Paragraph('<font color="#8c959f">-</font>', s_cell_r)
    c = "#1a7f37" if v >= 0 else "#cf222e"
    return Paragraph(f'<font color="{c}"><b>{"+" if v>=0 else ""}{v:,.2f}</b></font>', s_cell_r)
def p_price(v):
    if not v: return Paragraph('<font color="#8c959f">-</font>', s_cell_r)
    return Paragraph(f'<font color="#1f2328"><b>{v:,.2f}</b></font>', s_cell_r)
def p_bold(t): return Paragraph(f'<b>{t}</b>', s_cell_b)
def p_txt(t, st=None): return Paragraph(t, st or s_cell)
def premium_table(rows_data, col_w, pw):
    widths = [pw * w for w in col_w]
    t = Table(rows_data, colWidths=widths)
    n = len(rows_data)
    cmds = [
        ("FONTNAME", (0,0), (-1,-1), "Helvetica"),
        ("FONTSIZE", (0,0), (-1,-1), 8),
        ("TOPPADDING", (0,0), (-1,-1), 5),
        ("BOTTOMPADDING", (0,0), (-1,-1), 5),
        ("LEFTPADDING", (0,0), (-1,-1), 8),
        ("RIGHTPADDING", (0,0), (-1,-1), 8),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("BACKGROUND", (0,0), (-1,0), C_HEADER_ACCENT),
        ("TEXTCOLOR", (0,0), (-1,0), C_ACCENT),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("LINEBELOW", (0,0), (-1,0), 1.5, C_ACCENT),
        ("BOX", (0,0), (-1,-1), 0.5, C_BORDER),
        ("LINEBELOW", (0,0), (-1,-2), 0.3, HexColor("#d0d7de")),
    ]
    for i in range(1, n):
        cmds.append(("BACKGROUND", (0,i), (-1,i), C_ROW1 if i%2==1 else C_ROW2))
    t.setStyle(TableStyle(cmds))
    return t
def draw_page_bg(canvas_obj, doc):
    canvas_obj.saveState()
    # Light background
    canvas_obj.setFillColor(HexColor("#f8f9fb"))
    canvas_obj.rect(0, 0, A4[0], A4[1], fill=1, stroke=0)
    # Top accent line
    canvas_obj.setStrokeColor(C_ACCENT)
    canvas_obj.setLineWidth(3)
    canvas_obj.line(0, A4[1]-1.5, A4[0], A4[1]-1.5)
    # Footer
    canvas_obj.setFont("Helvetica", 7)
    canvas_obj.setFillColor(C_TEXT_MUTED)
    canvas_obj.drawCentredString(A4[0]/2, 10*mm,
        f"Quantex Pre-Market Report  |  {TODAY}  |  {TIME_NOW} IST  |  For Educational Purpose Only")
    canvas_obj.restoreState()
# ═══════ BUILD STORY ═══════
story = []
page_w = A4[0] - 30 * mm
# HEADER
story.append(Spacer(1, 2*mm))
story.append(Paragraph("QUANTEX",
    ParagraphStyle("T", parent=styles["Title"], fontSize=26, textColor=HexColor("#1f2328"),
        alignment=TA_CENTER, spaceAfter=1, spaceBefore=2, fontName="Helvetica-Bold",
        leading=30)))
story.append(Paragraph("PRE-MARKET REPORT",
    ParagraphStyle("S", parent=styles["Normal"], fontSize=11, textColor=C_ACCENT,
        alignment=TA_CENTER, spaceAfter=4, spaceBefore=0, fontName="Helvetica-Bold",
        leading=14)))
story.append(Paragraph(f'{TODAY}  |  {TIME_NOW} IST',
    ParagraphStyle("D", parent=styles["Normal"], fontSize=9, textColor=C_TEXT_DIM,
        alignment=TA_CENTER, spaceAfter=6)))
# Divider
dt = Table([[""]], colWidths=[page_w])
dt.setStyle(TableStyle([("LINEBELOW",(0,0),(-1,0),1.5,C_ACCENT),
    ("TOPPADDING",(0,0),(-1,-1),0),("BOTTOMPADDING",(0,0),(-1,-1),0)]))
story.append(dt)
story.append(Spacer(1, 2*mm))
# F&G GAUGE
story.append(Paragraph("FEAR & GREED INDEX", s_section))
if fg_score < 25: fg_tip = "Opportunity zone - look for quality pullbacks"
elif fg_score < 40: fg_tip = "Cautious optimism - wait for confirmation"
elif fg_score < 60: fg_tip = "Market balanced - be selective with entries"
elif fg_score < 75: fg_tip = "Getting heated - tighten stop-losses"
else: fg_tip = "Euphoria zone - avoid fresh positions"
story.append(GaugeFlowable(fg_score, fg_label, fg_tip, width=page_w, height=115*mm, prev_score=fg_prev))
story.append(Spacer(1, 2*mm))
# GIFT NIFTY
if gift_close is not None:
    story.append(Paragraph("GIFT NIFTY  <font color='#656d76' size='8'>(Pre-Market Indicator)</font>", s_section))
    s_gift_price = ParagraphStyle("GP", parent=s_cell, fontSize=18, fontName="Helvetica-Bold",
        textColor=HexColor("#1f2328"), leading=22)
    s_gift_chg = ParagraphStyle("GC", parent=s_cell_r, fontSize=11, fontName="Helvetica-Bold", leading=22)
    chg_c = "#1a7f37" if (gift_chg or 0) >= 0 else "#cf222e"
    chg_sign = "+" if (gift_chg or 0) >= 0 else ""
    pct_sign = "+" if (gift_pct or 0) >= 0 else ""
    gn = Table([[
        Paragraph(f'{gift_close:,.2f}', s_gift_price),
        Paragraph(f'<font color="{chg_c}">{chg_sign}{gift_chg:,.2f}</font>', s_gift_chg),
        Paragraph(f'<font color="{chg_c}">{pct_sign}{gift_pct:.2f}%</font>', s_gift_chg),
    ]], colWidths=[page_w*0.45, page_w*0.27, page_w*0.28])
    gn.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),C_CARD),
        ("BOX",(0,0),(-1,-1),0.5,C_BORDER),("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("TOPPADDING",(0,0),(-1,-1),12),("BOTTOMPADDING",(0,0),(-1,-1),12),
        ("LEFTPADDING",(0,0),(-1,-1),12),("RIGHTPADDING",(0,0),(-1,-1),12)]))
    story.append(gn)
    story.append(Spacer(1, 2*mm))
# MARKET TABLES
def build_mkt(title, names, show_c=True):
    story.append(Paragraph(title, s_section))
    if show_c:
        h = [p_txt("Index",s_hdr),p_txt("Country",s_hdr_c),p_txt("Close",s_hdr_r),p_txt("Change",s_hdr_r),p_txt("Chg%",s_hdr_r)]
    else:
        h = [p_txt("Index",s_hdr),p_txt("Close",s_hdr_r),p_txt("Change",s_hdr_r),p_txt("Chg%",s_hdr_r)]
    rows = [h]
    for n in names:
        d = global_data.get(n, indian_data.get(n, {}))
        if d.get("close") is None: continue
        if show_c:
            rows.append([p_bold(n),p_txt(d.get("country",""),s_cell_c),p_price(d["close"]),p_val(d["chg"]),p_chg(d["pct"])])
        else:
            rows.append([p_bold(n),p_price(d["close"]),p_val(d["chg"]),p_chg(d["pct"])])
    if len(rows) > 1:
        cw = [0.26,0.14,0.22,0.19,0.19] if show_c else [0.30,0.26,0.22,0.22]
        story.append(premium_table(rows, cw, page_w))
build_mkt("ASIAN MARKETS", ["Hang Seng","Nikkei 225","KOSPI","ASX 200"])
build_mkt("EUROPEAN MARKETS", ["DAX","FTSE 100","CAC 40"])
build_mkt("US MARKETS", ["Dow Jones","Nasdaq","S&P 500"])
story.append(Paragraph("INDIAN MARKET  <font color='#656d76' size='8'>(Prev Close)</font>", s_section))
rows = [[p_txt("Index",s_hdr),p_txt("Close",s_hdr_r),p_txt("Change",s_hdr_r),p_txt("Chg%",s_hdr_r)]]
for n in ["Sensex","Nifty 50","Bank Nifty","India VIX","USD/INR"]:
    d = indian_data.get(n, {})
    if d.get("close") is None: continue
    rows.append([p_bold(n),p_price(d["close"]),p_val(d["chg"]),p_chg(d["pct"])])
if len(rows) > 1: story.append(premium_table(rows, [0.30,0.26,0.22,0.22], page_w))

# COMMODITIES
story.append(Paragraph("COMMODITIES  <font color='#656d76' size='8'>(Global Spot)</font>", s_section))
c_rows = [[p_txt("Commodity", s_hdr), p_txt("Price", s_hdr_r), p_txt("Change", s_hdr_r), p_txt("Chg%", s_hdr_r)]]
for name in ["Gold (Spot)", "Silver (Spot)", "Brent Crude", "WTI Crude"]:
    d = commodity_data.get(name, {})
    if d.get("close") is None: continue
    c_rows.append([p_bold(name), p_price(d["close"]), p_val(d["chg"]), p_chg(d["pct"])])
if len(c_rows) > 1: story.append(premium_table(c_rows, [0.30, 0.26, 0.22, 0.22], page_w))

# CRYPTO
story.append(Paragraph("CRYPTO", s_section))
cr_rows = [[p_txt("Crypto", s_hdr), p_txt("Price ($)", s_hdr_r), p_txt("Change", s_hdr_r), p_txt("Chg%", s_hdr_r)]]
for name in ["Bitcoin", "Ethereum", "BNB", "Solana", "XRP", "Dogecoin", "Cardano", "Avalanche", "Polkadot", "Polygon"]:
    d = crypto_data.get(name, {})
    if d.get("close") is None: continue
    cr_rows.append([p_bold(name), p_price(d["close"]), p_val(d["chg"]), p_chg(d["pct"])])
if len(cr_rows) > 1: story.append(premium_table(cr_rows, [0.30, 0.26, 0.22, 0.22], page_w))

# CURRENCIES
story.append(Paragraph("CURRENCIES", s_section))
fx_rows = [[p_txt("Pair", s_hdr), p_txt("Rate", s_hdr_r), p_txt("Change", s_hdr_r), p_txt("Chg%", s_hdr_r)]]
for name in ["USD/INR", "EUR/USD", "GBP/USD", "USD/JPY"]:
    d = currency_data.get(name, {})
    if d.get("close") is None: continue
    fx_rows.append([p_bold(name), p_price(d["close"]), p_val(d["chg"]), p_chg(d["pct"])])
if len(fx_rows) > 1: story.append(premium_table(fx_rows, [0.30, 0.26, 0.22, 0.22], page_w))

# GAINERS & LOSERS
story.append(Paragraph("TOP GAINERS & LOSERS", s_section))
g_list = top_gainers.to_dict("records") if len(top_gainers) > 0 else []
l_list = top_losers.to_dict("records") if len(top_losers) > 0 else []
g_rows = [[p_txt("Gainer",s_hdr),p_txt("Price",s_hdr_r),p_txt("Chg%",s_hdr_r)]]
for g in g_list: g_rows.append([p_bold(g["symbol"]),p_price(g["close"]),p_chg(g["chg_pct"])])
l_rows = [[p_txt("Loser",s_hdr),p_txt("Price",s_hdr_r),p_txt("Chg%",s_hdr_r)]]
for l in l_list: l_rows.append([p_bold(l["symbol"]),p_price(l["close"]),p_chg(l["chg_pct"])])
half = page_w * 0.48
if g_rows and l_rows:
    g_t = premium_table(g_rows, [0.42,0.33,0.25], half)
    l_t = premium_table(l_rows, [0.42,0.33,0.25], half)
    combo = Table([[g_t,"",l_t]], colWidths=[half, page_w*0.04, half])
    combo.setStyle(TableStyle([("VALIGN",(0,0),(-1,-1),"TOP"),
        ("TOPPADDING",(0,0),(-1,-1),0),("BOTTOMPADDING",(0,0),(-1,-1),0),
        ("LEFTPADDING",(0,0),(-1,-1),0),("RIGHTPADDING",(0,0),(-1,-1),0)]))
    story.append(combo)
# VOLUME SHOCKERS
if len(vol_shockers) > 0:
    story.append(Paragraph("VOLUME SHOCKERS  <font color='#656d76' size='8'>(>1.5x avg)</font>", s_section))
    rows = [[p_txt("Symbol",s_hdr),p_txt("Price",s_hdr_r),p_txt("Vol Ratio",s_hdr_r),p_txt("Chg%",s_hdr_r)]]
    for _, row in vol_shockers.iterrows():
        rows.append([p_bold(row["symbol"]),p_price(row["close"]),
            Paragraph(f'<font color="#bf8700"><b>{row["vol_ratio"]:.1f}x</b></font>',s_cell_r),p_chg(row["chg_pct"])])
    story.append(premium_table(rows, [0.30,0.25,0.22,0.23], page_w))
# 52-WEEK HIGHS
if len(week52_highs) > 0:
    story.append(Paragraph("NEAR 52-WEEK HIGHS  <font color='#656d76' size='8'>(within 2%)</font>", s_section))
    rows = [[p_txt("Symbol",s_hdr),p_txt("Price",s_hdr_r),p_txt("52W High",s_hdr_r),p_txt("Gap",s_hdr_r)]]
    for _, row in week52_highs.iterrows():
        rows.append([p_bold(row["symbol"]),p_price(row["close"]),p_price(row["high_52w"]),
            Paragraph(f'<font color="#bf8700"><b>{row["dist_52w"]:.1f}%</b></font>',s_cell_r)])
    story.append(premium_table(rows, [0.28,0.24,0.24,0.24], page_w))
# SECTORAL PERFORMANCE
if len(sector_df) > 0:
    sector_1d = sector_df.sort_values("chg_1d", ascending=False)
    story.append(Paragraph("SECTORAL PERFORMANCE", s_section))
    story.append(HBarFlowable([(r["name"],r["close"],r["chg_1d"],r["chg_7d"]) for _,r in sector_1d.iterrows()], width=page_w))
    story.append(Spacer(1, 2*mm))
# FII/DII ACTIVITY — Previous Day
if fii_dii_data and fii_dii_data.get("fii_net") is not None:
    story.append(Paragraph("FII / DII ACTIVITY  <font color='#656d76' size='8'>(Previous Day — Cash Segment)</font>", s_section))

    fii_net = fii_dii_data.get("fii_net", 0)
    dii_net = fii_dii_data.get("dii_net", 0)
    fii_buy = fii_dii_data.get("fii_buy", 0)
    fii_sell = fii_dii_data.get("fii_sell", 0)
    dii_buy = fii_dii_data.get("dii_buy", 0)
    dii_sell = fii_dii_data.get("dii_sell", 0)

    fii_color = "#1a7f37" if fii_net >= 0 else "#cf222e"
    dii_color = "#1a7f37" if dii_net >= 0 else "#cf222e"
    total_net = fii_net + dii_net
    total_color = "#1a7f37" if total_net >= 0 else "#cf222e"

    rows = [
        [p_txt("", s_hdr_c), p_txt("Buy (₹ Cr)", s_hdr_r), p_txt("Sell (₹ Cr)", s_hdr_r), p_txt("Net (₹ Cr)", s_hdr_r), p_txt("Activity", s_hdr_c)],
        [
            Paragraph('<b>FII / FPI</b>', s_cell),
            p_txt(f'{fii_buy:,.0f}', s_cell_r),
            p_txt(f'{fii_sell:,.0f}', s_cell_r),
            Paragraph(f'<font color="{fii_color}"><b>{fii_net:+,.0f}</b></font>', s_cell_r),
            Paragraph(f'<font color="{fii_color}"><b>{"NET BUYERS" if fii_net >= 0 else "NET SELLERS"}</b></font>', s_cell_c),
        ],
        [
            Paragraph('<b>DII</b>', s_cell),
            p_txt(f'{dii_buy:,.0f}', s_cell_r),
            p_txt(f'{dii_sell:,.0f}', s_cell_r),
            Paragraph(f'<font color="{dii_color}"><b>{dii_net:+,.0f}</b></font>', s_cell_r),
            Paragraph(f'<font color="{dii_color}"><b>{"NET BUYERS" if dii_net >= 0 else "NET SELLERS"}</b></font>', s_cell_c),
        ],
        [
            Paragraph('<b>TOTAL</b>', s_cell),
            p_txt(f'{fii_buy + dii_buy:,.0f}', s_cell_r),
            p_txt(f'{fii_sell + dii_sell:,.0f}', s_cell_r),
            Paragraph(f'<font color="{total_color}"><b>{total_net:+,.0f}</b></font>', s_cell_r),
            Paragraph(f'<font color="{total_color}"><b>{"BULLISH" if total_net >= 0 else "BEARISH"}</b></font>', s_cell_c),
        ],
    ]
    story.append(premium_table(rows, [0.18, 0.20, 0.20, 0.22, 0.20], page_w))

    # Insight based on latest day
    fii_net = fii_dii_data.get("fii_net", 0)
    dii_net = fii_dii_data.get("dii_net", 0)
    if abs(fii_net) > 0:
        if fii_net > 1000:
            insight = "Strong FII buying — bullish signal for markets"
        elif fii_net > 0:
            insight = "Mild FII buying — cautiously positive"
        elif fii_net > -1000:
            insight = "Mild FII selling — watch for support levels"
        else:
            insight = "Heavy FII selling — expect selling pressure"

        if dii_net > 0 and fii_net < 0:
            insight += ". DII absorbing FII selling — supportive"
        elif dii_net > 0 and fii_net > 0:
            insight += ". Both FII + DII buying — strong bullish"
        elif dii_net < 0 and fii_net < 0:
            insight += ". Both selling — cautious approach advised"

        story.append(Paragraph(f'<font color="#8c959f" size="6.5">💡 {insight}</font>', s_note))
    story.append(Spacer(1, 2*mm))

# PIVOT LEVELS (Nifty & BankNifty)
for idx_name in ["Nifty 50", "Bank Nifty"]:
    if idx_name in pivot_data:
        pv = pivot_data[idx_name]
        story.append(Paragraph(f"{idx_name.upper()} PIVOT LEVELS  <font color='#656d76' size='8'>(Today)</font>", s_section))

        # Header row
        pv_rows = [[
            p_txt("Type", s_hdr),
            Paragraph('<font color="#cf222e"><b>R3</b></font>', s_hdr_c),
            Paragraph('<font color="#cf222e"><b>R2</b></font>', s_hdr_c),
            Paragraph('<font color="#cf222e"><b>R1</b></font>', s_hdr_c),
            Paragraph('<font color="#0969da"><b>PP</b></font>', s_hdr_c),
            Paragraph('<font color="#1a7f37"><b>S1</b></font>', s_hdr_c),
            Paragraph('<font color="#1a7f37"><b>S2</b></font>', s_hdr_c),
            Paragraph('<font color="#1a7f37"><b>S3</b></font>', s_hdr_c),
        ]]

        for ptype, pvals in [("Classic", pv["classic"]), ("Fibonacci", pv["fibonacci"])]:
            pv_rows.append([
                Paragraph(f'<b>{ptype}</b>', s_cell),
                Paragraph(f'<font color="#cf222e"><b>{pvals["R3"]:,.0f}</b></font>', s_cell_c),
                Paragraph(f'<font color="#cf222e"><b>{pvals["R2"]:,.0f}</b></font>', s_cell_c),
                Paragraph(f'<font color="#cf222e"><b>{pvals["R1"]:,.0f}</b></font>', s_cell_c),
                Paragraph(f'<font color="#0969da"><b>{pvals["PP"]:,.0f}</b></font>', s_cell_c),
                Paragraph(f'<font color="#1a7f37"><b>{pvals["S1"]:,.0f}</b></font>', s_cell_c),
                Paragraph(f'<font color="#1a7f37"><b>{pvals["S2"]:,.0f}</b></font>', s_cell_c),
                Paragraph(f'<font color="#1a7f37"><b>{pvals["S3"]:,.0f}</b></font>', s_cell_c),
            ])

        story.append(premium_table(pv_rows, [0.14, 0.12, 0.12, 0.12, 0.12, 0.12, 0.12, 0.14], page_w))
        story.append(Spacer(1, 2*mm))

# EARNINGS SPOTLIGHT
if len(recent_results) > 0 or len(upcoming_earnings) > 0:
    story.append(Paragraph("EARNINGS SPOTLIGHT", s_section))
    if len(recent_results) > 0:
        story.append(Paragraph("Recent Results (Last 2 Days)", s_subsec))
        rows = [[p_txt("Symbol",s_hdr),p_txt("Date",s_hdr_c),p_txt("Result",s_hdr),p_txt("Price",s_hdr_r)]]
        for r in recent_results:
            sig = r.get("signal","")
            if "Surge" in sig or "Beat" in sig: badge = '<font color="#1a7f37"><b>BEAT</b></font>'
            elif "Miss" in sig or "Decline" in sig: badge = '<font color="#cf222e"><b>MISS</b></font>'
            else: badge = '<font color="#bf8700"><b>MIXED</b></font>'
            rows.append([p_bold(r["symbol"]),p_txt(r["date"],s_cell_c),
                Paragraph(f'{badge} <font color="#656d76" size="7">{sig[:38]}</font>',s_cell),p_price(r.get("price",0))])
        story.append(premium_table(rows, [0.17,0.14,0.47,0.22], page_w))
        story.append(Spacer(1, 3*mm))
# UPCOMING RESULTS
if len(upcoming_earnings) > 0:
    story.append(Paragraph(f"UPCOMING RESULTS  <font color='#656d76' size='8'>(Next 3 Days - {len(upcoming_earnings)} Stocks)</font>", s_section))
    rows = [[p_txt("Symbol",s_hdr),p_txt("Date",s_hdr_c),p_txt("Day",s_hdr_c),p_txt("CMP",s_hdr_r),p_txt("Est. EPS",s_hdr_r),p_txt("Est. Rev",s_hdr_r)]]
    for e in upcoming_earnings:
        ed = datetime.strptime(e["date"],"%Y-%m-%d").date()
        da = (ed - today_date).days
        if da == 0: dh = '<font color="#bf8700"><b>TODAY</b></font>'
        elif da == 1: dh = '<font color="#bc4c00"><b>TOMORROW</b></font>'
        else: dh = e["date"]
        eps = f'{e["est_eps"]}' if e.get("est_eps") else '<font color="#8c959f">-</font>'
        rev = f'{e["est_rev"]}' if e.get("est_rev") else '<font color="#8c959f">-</font>'
        rows.append([p_bold(e["symbol"]),p_txt(dh,s_cell_c),p_txt(f'<b>{e["day"]}</b>',s_cell_c),
            p_price(e.get("price",0)) if e.get("price") else p_txt("-",s_cell_r),p_txt(eps,s_cell_r),p_txt(rev,s_cell_r)])
    story.append(premium_table(rows, [0.17,0.16,0.10,0.18,0.15,0.24], page_w))
    story.append(Paragraph('<font color="#8c959f" size="6.5">Estimates via Yahoo Finance. Plan entries/exits around result dates.</font>',s_note))
# TOP 10 FOCUS STOCKS
if len(focus_stocks) > 0:
    story.append(Paragraph(f"TOP TRADE IDEAS FOR TODAY  <font color='#656d76' size='8'>({len(focus_stocks)} Stocks)</font>", s_section))
    story.append(Paragraph(f'<font color="#656d76" size="7.5">Ranked by conviction score  |  {focus_source}</font>',s_note))
    story.append(Spacer(1, 2*mm))
    rows = [[p_txt("#",s_hdr_c),p_txt("Symbol",s_hdr),p_txt("Score",s_hdr_c),p_txt("CMP",s_hdr_r),p_txt("Entry",s_hdr_r),p_txt("Holding",s_hdr_c),p_txt("Target",s_hdr_r)]]
    for i, fs in enumerate(focus_stocks):
        sc = fs["score"]
        sc_c = "#1a7f37" if sc >= 85 else "#bf8700" if sc >= 75 else "#bc4c00"
        # Holding period: use hold_period from scanner if available, else default
        holding = "5-10 Days"
        rows.append([p_txt(f'<b>{i+1}</b>',s_cell_c),p_bold(fs["symbol"]),
            Paragraph(f'<font color="{sc_c}"><b>{sc}</b></font>',s_cell_c),
            p_price(fs.get("cmp", fs.get("entry",0))),
            p_price(fs.get("entry",0)),
            Paragraph(f'<font color="#0969da"><b>{holding}</b></font>',s_cell_c),
            Paragraph(f'<font color="#1a7f37">{fs.get("target",0):,.2f}</font>',s_cell_r)])
    story.append(premium_table(rows, [0.06,0.16,0.10,0.17,0.17,0.17,0.17], page_w))
    story.append(Spacer(1, 3*mm))
    story.append(Paragraph("Key Signals (Top 3)", s_subsec))
    for fs in focus_stocks[:3]:
        story.append(Paragraph(
            f'<font color="#1f2328"><b>{fs["symbol"]}</b></font>  <font color="#656d76" size="7">{fs.get("signals","")}</font>',
            ParagraphStyle("sig", parent=s_cell, spaceBefore=2, spaceAfter=1, leftIndent=6)))
# QUOTE
story.append(Spacer(1, 5*mm))
quote_text, quote_author = random.choice(QUOTES)
story.append(QuoteFlowable(quote_text, quote_author, width=page_w))
story.append(Spacer(1, 4*mm))
# Educational purpose disclaimer
story.append(Spacer(1, 3*mm))
story.append(Paragraph(
    '<font color="#cf222e" size="7"><b>Disclaimer:</b></font> '
    '<font color="#656d76" size="6.5">This report is for <b>educational purpose only</b>. '
    'It is not investment advice. The data shown is derived from publicly available sources and may contain errors. '
    'Always do your own research (DYOR) before making any trading or investment decisions. '
    'Past performance does not guarantee future results.</font>',
    ParagraphStyle("disc", parent=s_cell, fontSize=6.5, textColor=C_TEXT_DIM, spaceBefore=2, spaceAfter=2, alignment=TA_CENTER)))
story.append(Spacer(1, 2*mm))
story.append(Paragraph("Powered by Quantex  |  #PreMarket #Quantex", s_footer))
# BUILD
doc = SimpleDocTemplate(str(PDF_PATH), pagesize=A4,
    leftMargin=15*mm, rightMargin=15*mm, topMargin=8*mm, bottomMargin=16*mm)
doc.build(story, onFirstPage=draw_page_bg, onLaterPages=draw_page_bg)
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
    "fear_greed_prev": fg_prev,
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
