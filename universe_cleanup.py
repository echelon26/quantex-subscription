#!/usr/bin/env python3
"""
Quantex Universe Cleanup
========================

Reads the merged scanner universe (pro_scanner.STOCK_UNIVERSE + nifty500_symbols.json),
tests every ticker silently against yfinance, and produces a CLEAN universe by:

  1. Applying known rename map (Zomato→Eternal, IIFLWealth→360ONE, etc.)
  2. Removing tickers that no longer resolve on yfinance (delisted/merged)
  3. Writing updated nifty500_symbols.json + a clean_universe.json
  4. Producing a report of fixes vs removals

Run this monthly (or whenever logs show too many "possibly delisted" warnings).

Usage:
    python universe_cleanup.py              # dry run — report only
    python universe_cleanup.py --apply      # actually update files
    python universe_cleanup.py --apply --include-pro-scanner   # also patch pro_scanner.py STOCK_UNIVERSE
"""

import os
import sys
import json
import io
import contextlib
import logging
from pathlib import Path
from datetime import datetime

# Silence yfinance noise before importing
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)


@contextlib.contextmanager
def silence_stdio():
    """Suppress stderr + stdout for the duration of yfinance fetches."""
    se, so = sys.stderr, sys.stdout
    sys.stderr = io.StringIO()
    sys.stdout = io.StringIO()
    try:
        yield
    finally:
        sys.stderr = se
        sys.stdout = so


import yfinance as yf


# ──────────────────────────────────────────────────────────────────────────────
# KNOWN RENAME MAP — corporate actions that broke tickers
# Source: NSE corporate announcements 2024-2026
# ──────────────────────────────────────────────────────────────────────────────

RENAME_MAP = {
    # Zomato Ltd renamed to Eternal Ltd (Sep 2024)
    "ZOMATO": "ETERNAL",
    # IIFL Wealth Management renamed to 360 ONE WAM
    "IIFLWAM": "360ONE",
    # Amara Raja Batteries → Amara Raja Energy & Mobility
    "AMARAJABAT": "ARE&M",
    # Tata Motors original ticker change
    "TATAMOTOR": "TATAMOTORS",
    # Abbott India ticker format
    "ABBOT": "ABBOTINDIA",
    # Indian Hotels ticker format
    "INDIANHOTELS": "INDHOTEL",
    # L&T Infotech merged with Mindtree → LTIMindtree
    "LTIM": "LTIMINDTREE",
    # Greaves Cotton — short form to long form
    "GREAVES": "GREAVESCOT",
    # Lakshmi Machine Works
    "LAXMIMACH": "LMW",
    # Suven Pharma
    "SUVENPHAR": "SUVENPHA",
    # Anupam Rasayan short form
    "ANUPAMRAS": "ANURAS",
    # Apollo Micro Systems
    "APOLLOMICRO": "APOLLO",
    # Vedant Fashions
    "VEDANTFASH": "MANYAVAR",
    # Waaree Renewables
    "WAABORIG": "WAAREERTL",
    # Paras Defence — keep as PARAS
    "PARASD": "PARAS",
    # JSW Holdings — sometimes listed as JSWHL
    "JSWHLDGS": "JSWHL",
}

# Confirmed dead tickers (no known successor, just remove)
KNOWN_DEAD = set()


# ──────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ──────────────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).parent
NIFTY500_PATH = ROOT / "quantex_logs" / "nifty500_symbols.json"


def load_pro_universe():
    try:
        sys.path.insert(0, str(ROOT))
        from pro_scanner import STOCK_UNIVERSE  # type: ignore
        return list(STOCK_UNIVERSE)
    except Exception as e:
        print(f"  ! Couldn't import STOCK_UNIVERSE: {e}")
        return []


def load_nifty500():
    if NIFTY500_PATH.exists():
        try:
            return json.loads(NIFTY500_PATH.read_text())
        except Exception:
            pass
    return []


# ──────────────────────────────────────────────────────────────────────────────
# YFINANCE PROBE
# ──────────────────────────────────────────────────────────────────────────────

def probe(symbol):
    """Quick check: does symbol.NS resolve? Returns True if has data, False otherwise."""
    try:
        with silence_stdio():
            t = yf.Ticker(f"{symbol}.NS")
            df = t.history(period="5d", interval="1d")
        if df is None or df.empty:
            return False
        return True
    except Exception:
        return False


def cleanup(symbols):
    """Apply renames + probe + classify.

    Returns dict with:
      ok:        symbols that resolve as-is
      renamed:   {old → new} that resolved after rename
      dead:      symbols that don't resolve and have no rename
    """
    ok = []
    renamed = {}
    dead = []

    for i, sym in enumerate(symbols, 1):
        if i % 50 == 0:
            print(f"  ... {i}/{len(symbols)}")

        # Step 1: apply known rename map without probing
        if sym in RENAME_MAP:
            new_sym = RENAME_MAP[sym]
            # Confirm the new symbol actually works
            if probe(new_sym):
                renamed[sym] = new_sym
                continue
            else:
                # Even the mapped name doesn't work — flag as dead
                dead.append(sym)
                continue

        if sym in KNOWN_DEAD:
            dead.append(sym)
            continue

        # Step 2: probe current symbol
        if probe(sym):
            ok.append(sym)
        else:
            dead.append(sym)

    return {"ok": ok, "renamed": renamed, "dead": dead}


# ──────────────────────────────────────────────────────────────────────────────
# REPORT + APPLY
# ──────────────────────────────────────────────────────────────────────────────

def report(label, result):
    print(f"\n=== {label} ===")
    print(f"  OK:      {len(result['ok'])}")
    print(f"  Renamed: {len(result['renamed'])}")
    print(f"  Dead:    {len(result['dead'])}")
    if result['renamed']:
        print("  --- renames applied ---")
        for old, new in sorted(result['renamed'].items()):
            print(f"    {old:14s} → {new}")
    if result['dead']:
        print("  --- dead (will be removed) ---")
        for d in sorted(result['dead']):
            print(f"    {d}")


def write_clean_files(pro_result, n500_result, apply_pro_scanner=False):
    # New nifty500: ok + renamed values
    new_n500 = sorted(set(n500_result['ok']) | set(n500_result['renamed'].values()))
    NIFTY500_PATH.write_text(json.dumps(new_n500, indent=2))
    print(f"\n✅ Wrote {NIFTY500_PATH} ({len(new_n500)} symbols)")

    # Merged clean universe (for any scanner that wants to load directly)
    merged_pro_ok = set(pro_result['ok']) | set(pro_result['renamed'].values())
    merged = sorted(merged_pro_ok | set(new_n500))
    merged_path = ROOT / "quantex_logs" / "clean_universe.json"
    merged_path.write_text(json.dumps(merged, indent=2))
    print(f"✅ Wrote {merged_path} (merged {len(merged)} symbols)")

    if apply_pro_scanner:
        patch_pro_scanner(pro_result)


def patch_pro_scanner(pro_result):
    """Update STOCK_UNIVERSE list inside pro_scanner.py with cleaned symbols."""
    pro_path = ROOT / "pro_scanner.py"
    if not pro_path.exists():
        print(f"  ! pro_scanner.py not found, skipping in-place patch")
        return

    new_universe = sorted(set(pro_result['ok']) | set(pro_result['renamed'].values()))
    content = pro_path.read_text()

    # Find the STOCK_UNIVERSE = [...] block
    import re
    pattern = r"STOCK_UNIVERSE\s*=\s*\[[^\]]*\]"
    new_block = "STOCK_UNIVERSE = " + json.dumps(new_universe, indent=4).replace('"', '"')
    new_content, n = re.subn(pattern, new_block, content, count=1, flags=re.DOTALL)
    if n == 0:
        print(f"  ! Couldn't find STOCK_UNIVERSE assignment in pro_scanner.py — skipping")
        return
    pro_path.write_text(new_content)
    print(f"✅ Patched STOCK_UNIVERSE in pro_scanner.py ({len(new_universe)} symbols)")


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    apply = "--apply" in sys.argv
    include_pro = "--include-pro-scanner" in sys.argv

    print("=" * 60)
    print(f"  QUANTEX UNIVERSE CLEANUP — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Mode: {'APPLY' if apply else 'DRY RUN (report only)'}")
    print("=" * 60)

    pro = load_pro_universe()
    n500 = load_nifty500()
    print(f"\nLoaded:")
    print(f"  pro_scanner.STOCK_UNIVERSE: {len(pro)} symbols")
    print(f"  nifty500_symbols.json:      {len(n500)} symbols")
    merged_before = sorted(set(pro) | set(n500))
    print(f"  Merged unique:              {len(merged_before)} symbols")

    # Run on merged set (faster than running both lists separately for the same symbols)
    print(f"\nProbing all symbols against yfinance (silenced output)...")
    merged_result = cleanup(merged_before)
    report("MERGED UNIVERSE", merged_result)

    # Map merged-result back to pro and n500 separately
    def split(symbols, merged_res):
        ok, renamed, dead = [], {}, []
        for s in symbols:
            if s in merged_res['renamed']:
                renamed[s] = merged_res['renamed'][s]
            elif s in merged_res['dead']:
                dead.append(s)
            elif s in merged_res['ok']:
                ok.append(s)
        return {"ok": ok, "renamed": renamed, "dead": dead}

    pro_result = split(pro, merged_result)
    n500_result = split(n500, merged_result)

    print(f"\n📊 SUMMARY:")
    print(f"  pro_scanner: {len(pro)} → {len(pro_result['ok']) + len(pro_result['renamed'])} clean "
          f"({len(pro_result['renamed'])} renamed, {len(pro_result['dead'])} dead)")
    print(f"  nifty500:    {len(n500)} → {len(n500_result['ok']) + len(n500_result['renamed'])} clean "
          f"({len(n500_result['renamed'])} renamed, {len(n500_result['dead'])} dead)")

    if apply:
        print(f"\n--- APPLYING CHANGES ---")
        write_clean_files(pro_result, n500_result, apply_pro_scanner=include_pro)
    else:
        print(f"\n💡 Re-run with --apply to write changes.")
        print(f"💡 Add --include-pro-scanner to also patch pro_scanner.py")


if __name__ == "__main__":
    main()
