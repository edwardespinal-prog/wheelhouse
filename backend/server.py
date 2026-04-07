#!/usr/bin/env python3
"""
WheelHouse — Portfolio Command Center
A self-contained Python server using standard library http.server.
External deps: yfinance, requests, beautifulsoup4, pandas, openpyxl

Memory Budget (Render Starter — 512 MB RAM)
────────────────────────────────────────────
Python process + libs (yfinance, pandas):  ~60-80 MB
Store dict (all JSON data in memory):      ~200 KB
Intraday log (48-hr, 5-min interval):      ~50 KB
price_cache (5-min TTL, ~60 tickers):      ~5 KB
option_mark_cache (persists to disk):      ~10 KB
_hist_cache (LRU cap: 100 entries):        ~2 MB max
_heatmap_cache (5-min TTL, single obj):    ~50 KB
_insider_portfolio_cache (30-min TTL):     ~100 KB
Dark pool cache (10 days in memory):       ~3 MB max (45 days on disk, lazy load)
WheelOptions chain_cache (LRU cap: 20):    ~160 KB max
ThreadPoolExecutor (max 20 workers):       ~10 MB max concurrent
ETF flow cache (90 days memory):           ~45 KB (365 on disk)
BTC flow cache (90 days memory):           ~45 KB (365 on disk)
BMNR NAV cache (5-min TTL):               ~2 KB
BMNR NAV history (365 entries on disk):    ~15 KB
Lido APR cache (1-hr TTL):                ~1 KB
────────────────────────────────────────────
Estimated peak:                            ~160-210 MB (well under 512 MB)

Cache caps added 2026-04-06:
  _HIST_CACHE_MAX = 100 entries (LRU eviction by timestamp)
  WheelOptionsProvider._CHAIN_CACHE_MAX = 20 entries (LRU eviction)
  FINRADarkPoolProvider._MEM_CACHE_MAX_DAYS = 10 (lazy disk load)
  ThreadPoolExecutor max_workers = 20 (was unbounded ThreadingMixIn)
  Explicit `del df` after yf.download() extraction
"""

import json
import os
import sys
import time
import threading
from collections import defaultdict
from http.server import HTTPServer, SimpleHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, parse_qs
from datetime import datetime, date, timedelta
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

# ── Try importing optional packages ──────────────────────────────────────
try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False
    print("⚠ yfinance not installed. Run: pip install yfinance")

try:
    import requests
    from bs4 import BeautifulSoup
    HAS_SCRAPING = True
except ImportError:
    HAS_SCRAPING = False
    print("⚠ requests/beautifulsoup4 not installed. Run: pip install requests beautifulsoup4")

try:
    # Add backend dir to path so data_providers can be found when running from project root
    sys.path.insert(0, str(Path(__file__).parent))
    from data_providers import get_darkpool_provider, get_insider_provider, get_options_flow_provider, get_card_valuation_provider, get_news_provider, get_wheel_provider, get_etf_flow_provider, get_bmnr_nav_provider, get_staking_provider
    HAS_PROVIDERS = True
except ImportError as e:
    HAS_PROVIDERS = False
    print(f"⚠ data_providers not available: {e}")

# ── Config ───────────────────────────────────────────────────────────────
PORT = int(os.environ.get("PORT", 8000))
BASE_DIR = Path(__file__).parent.parent
BACKEND_DIR = Path(__file__).parent
DATA_DIR = Path("/data") if os.path.isdir("/data") else BACKEND_DIR / "data"  # Render persistent disk
FRONTEND_DIR = BASE_DIR / "frontend"
EXCEL_FILE = None  # Legacy: Excel import not used in WheelHouse

# ── Portfolio Constants ──────────────────────────────────────────────────
JAN1_NET_WORTH = 0  # Set this to your Jan 1 portfolio value for YTD benchmark

# ── Price Cache ──────────────────────────────────────────────────────────
price_cache = {}
CACHE_TTL = 300  # 5 minutes
option_mark_cache = {}  # Persists last known good mark for options across weekends
_mark_cache_warm = False  # Set True ONLY after refresh_all_prices() successfully prices options (NOT on disk load)
_mark_cache_warm_ts = 0   # Timestamp when cache became warm (for post-warmup grace period)

# ── Risk Data Cache ─────────────────────────────────────────────────────
risk_info_cache = {}  # {ticker: {"beta": ..., "sector": ..., "ts": ...}}
RISK_CACHE_TTL = 600  # 10 minutes

# ── Heatmap Cache ──────────────────────────────────────────────────────
_heatmap_cache = {"data": None, "ts": 0}
HEATMAP_TTL = 300  # 5 minutes

# ── Insider Portfolio Cache ────────────────────────────────────────────
_insider_portfolio_cache = {"data": None, "ts": 0}
INSIDER_PORTFOLIO_TTL = 1800  # 30 minutes

# ── Data Store (in-memory, persisted to JSON) ────────────────────────────
store = {
    "equities": [],
    "options": [],
    "crypto": [],
    "cc_income": [],
    "tax_ledger": [],
    "cards": [],
    "cash_flows": [],
    "watchlist": [],
    "sales": [],
    "box_purchases": [],
    "summary": {},
    "cash_balance": 0,
    "option_mark_history": {},
    "_cc_cash_backfilled": False,
    "_ebay_sales_seeded": False,
    "_migration_dedup_fix": False,
    "_intraday_log_cleared": False,
    "_beringer_reverted": False,
    "_cards_origin_migration": False,
    "bmnr_eth_holdings": 4803000,
    "bmnr_eth_staked": 3330000,
}


def ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def save_store():
    ensure_data_dir()
    for key, data in store.items():
        path = DATA_DIR / f"{key}.json"
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
    # Persist option mark cache
    if option_mark_cache:
        mark_path = DATA_DIR / "option_marks.json"
        with open(mark_path, "w") as f:
            json.dump(option_mark_cache, f, indent=2)


def load_store():
    ensure_data_dir()
    for key in store:
        path = DATA_DIR / f"{key}.json"
        if path.exists():
            with open(path) as f:
                store[key] = json.load(f)
    # Load persisted option mark cache (for after-hours/weekend fallback lookups only)
    # NOTE: This does NOT set _mark_cache_warm — disk marks are for price lookups,
    # not an indication that store option values are current.
    # Only refresh_all_prices() should set _mark_cache_warm after computing fresh values.
    mark_path = DATA_DIR / "option_marks.json"
    if mark_path.exists():
        try:
            with open(mark_path) as f:
                marks = json.load(f)
            if marks:
                option_mark_cache.update(marks)
                print(f"  Loaded {len(option_mark_cache)} cached option marks from disk")
        except (json.JSONDecodeError, IOError):
            pass


def load_snapshots():
    path = DATA_DIR / "snapshots.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return []



# save_snapshot() removed — replaced by position-history reconstruction (Option C)
# Options values now logged via save_options_value() after each refresh


def load_from_excel():
    return  # Disabled in WheelHouse
    if not EXCEL_FILE.exists():
        print(f"⚠ Excel file not found: {EXCEL_FILE}")
        return

    print(f"Loading data from {EXCEL_FILE}...")

    # Equities
    df = pd.read_excel(EXCEL_FILE, sheet_name="Equities", header=None)
    equities = []
    for i in range(1, len(df)):
        row = df.iloc[i]
        if pd.isna(row[0]):
            continue
        equities.append({
            "id": i,
            "ticker": str(row[0]),
            "quantity": float(row[1]) if pd.notna(row[1]) else 0,
            "market_value": float(row[2]) if pd.notna(row[2]) else 0,
            "live_price": float(row[3]) if pd.notna(row[3]) else 0,
            "avg_cost": float(row[4]) if pd.notna(row[4]) else 0,
            "total_invested": float(row[5]) if pd.notna(row[5]) else 0,
            "pl_dollar": float(row[6]) if pd.notna(row[6]) else 0,
            "pl_percent": float(row[7]) if isinstance(row[7], (int, float)) and pd.notna(row[7]) else 0,
            "target_buy_price": float(row[9]) if pd.notna(row[9]) else None,
            "cc_income": float(row[10]) if pd.notna(row[10]) else 0,
            "cc_yield": float(row[11]) if isinstance(row[11], (int, float)) and pd.notna(row[11]) else 0,
        })
    store["equities"] = equities

    # Options
    df = pd.read_excel(EXCEL_FILE, sheet_name="Options", header=None)
    options = []
    opt_id = 1
    for i in range(6, len(df)):
        row = df.iloc[i]
        if pd.isna(row[0]):
            continue
        options.append({
            "id": opt_id,
            "position_name": str(row[0]),
            "occ_long": str(row[1]) if pd.notna(row[1]) else "",
            "occ_short": str(row[2]) if pd.notna(row[2]) else "",
            "contracts": int(row[3]) if pd.notna(row[3]) else 0,
            "avg_cost": float(row[4]) if pd.notna(row[4]) else 0,
            "live_price_long": float(row[5]) if pd.notna(row[5]) else 0,
            "live_price_short": float(row[6]) if pd.notna(row[6]) else 0,
            "net_spread": float(row[7]) if pd.notna(row[7]) else 0,
            "pl_dollar": float(row[8]) if pd.notna(row[8]) else 0,
            "total_value": float(row[9]) if pd.notna(row[9]) else 0,
            "total_invested": float(row[10]) if pd.notna(row[10]) else 0,
        })
        opt_id += 1
    store["options"] = options

    # Crypto
    df = pd.read_excel(EXCEL_FILE, sheet_name="Crypto", header=None)
    crypto = []
    for i in range(1, len(df)):
        row = df.iloc[i]
        if pd.isna(row[0]):
            continue
        crypto.append({
            "id": i,
            "asset": str(row[0]),
            "quantity": float(row[1]) if pd.notna(row[1]) else 0,
            "avg_price": float(row[2]) if pd.notna(row[2]) else 0,
            "current_price": float(row[3]) if pd.notna(row[3]) else 0,
            "market_value": float(row[4]) if pd.notna(row[4]) else 0,
            "pl_dollar": float(row[5]) if pd.notna(row[5]) else 0,
            "pl_percent": float(row[6]) if pd.notna(row[6]) else 0,
        })
    store["crypto"] = crypto

    # CC Income
    df = pd.read_excel(EXCEL_FILE, sheet_name="CC Income", header=None)
    cc_income = []
    cc_id = 1
    for i in range(2, len(df)):
        row = df.iloc[i]
        if pd.isna(row[0]):
            continue
        cc_income.append({
            "id": cc_id,
            "ticker": str(row[0]),
            "date_opened": str(row[1])[:10] if pd.notna(row[1]) else "",
            "contract_details": str(row[2]) if pd.notna(row[2]) else "",
            "premium_collected": float(row[3]) if pd.notna(row[3]) else 0,
            "date_closed": str(row[4])[:10] if pd.notna(row[4]) else "",
            "cost_to_close": float(row[5]) if pd.notna(row[5]) else 0,
            "net_profit": float(row[6]) if pd.notna(row[6]) else 0,
        })
        cc_id += 1
    store["cc_income"] = cc_income

    # Tax Ledger
    df = pd.read_excel(EXCEL_FILE, sheet_name="Tax Ledger", header=None)
    tax_ledger = []
    tx_id = 1
    for i in range(4, len(df)):
        row = df.iloc[i]
        if pd.isna(row[0]):
            continue
        tax_ledger.append({
            "id": tx_id,
            "date_acquired": str(row[0])[:10] if pd.notna(row[0]) else "",
            "asset_type": str(row[1]) if pd.notna(row[1]) else "",
            "ticker": str(row[2]) if pd.notna(row[2]) else "",
            "quantity": float(row[3]) if pd.notna(row[3]) else 0,
            "purchase_price": float(row[4]) if pd.notna(row[4]) else 0,
            "cost_basis": float(row[5]) if pd.notna(row[5]) else 0,
            "premium_received": float(row[6]) if pd.notna(row[6]) else 0,
            "current_price": float(row[7]) if pd.notna(row[7]) else 0,
            "unrealized_pl": float(row[8]) if pd.notna(row[8]) else 0,
            "days_held": int(row[9]) if pd.notna(row[9]) else 0,
            "tax_status": str(row[10]) if pd.notna(row[10]) else "",
            "date_closed": str(row[11])[:10] if pd.notna(row[11]) else "",
            "sell_price": float(row[12]) if pd.notna(row[12]) else 0,
            "realized_pl": float(row[13]) if pd.notna(row[13]) else 0,
            "tax_reserve": str(row[14]) if pd.notna(row[14]) else "0",
        })
        tx_id += 1
    store["tax_ledger"] = tax_ledger

    # Cards
    df = pd.read_excel(EXCEL_FILE, sheet_name="Alternative Assets", header=None)
    cards = []
    card_id = 1
    for i in range(1, len(df)):
        row = df.iloc[i]
        if pd.isna(row[0]):
            continue
        cards.append({
            "id": card_id,
            "category": str(row[0]) if pd.notna(row[0]) else "",
            "player": str(row[1]) if pd.notna(row[1]) else "",
            "year": int(row[2]) if pd.notna(row[2]) else 0,
            "brand": str(row[3]) if pd.notna(row[3]) else "",
            "card_name": str(row[4]) if pd.notna(row[4]) else "",
            "serial": str(row[5]) if pd.notna(row[5]) else "",
            "condition": str(row[6]) if pd.notna(row[6]) else "Raw",
            "est_value": float(row[7]) if pd.notna(row[7]) else 0,
        })
        card_id += 1
    store["cards"] = cards

    # Summary
    df = pd.read_excel(EXCEL_FILE, sheet_name="Market Net Worth", header=None)
    store["summary"] = {
        "total_invested": float(df.iloc[1][4]) if pd.notna(df.iloc[1][4]) else 0,
        "market_value": float(df.iloc[2][4]) if pd.notna(df.iloc[2][4]) else 0,
        "total_pl_dollar": float(df.iloc[3][4]) if pd.notna(df.iloc[3][4]) else 0,
        "total_pl_percent": float(df.iloc[4][4]) if pd.notna(df.iloc[4][4]) else 0,
        "cc_income": float(df.iloc[5][1]) if pd.notna(df.iloc[5][1]) else 0,
        "equities_invested": float(df.iloc[1][1]) if pd.notna(df.iloc[1][1]) else 0,
        "equities_value": float(df.iloc[2][1]) if pd.notna(df.iloc[2][1]) else 0,
        "options_invested": float(df.iloc[1][2]) if pd.notna(df.iloc[1][2]) else 0,
        "options_value": float(df.iloc[2][2]) if pd.notna(df.iloc[2][2]) else 0,
        "crypto_invested": float(df.iloc[1][3]) if pd.notna(df.iloc[1][3]) else 0,
        "crypto_value": float(df.iloc[2][3]) if pd.notna(df.iloc[2][3]) else 0,
    }

    save_store()
    print(f"✓ Loaded: {len(equities)} equities, {len(options)} options, {len(crypto)} crypto, {len(cc_income)} CC income, {len(tax_ledger)} tax entries, {len(cards)} cards")


# ── Yahoo Finance ────────────────────────────────────────────────────────
def get_live_price(ticker):
    now = time.time()
    if ticker in price_cache and (now - price_cache[ticker]["time"]) < CACHE_TTL:
        return price_cache[ticker]["price"]

    if not HAS_YFINANCE:
        return None

    try:
        t = yf.Ticker(ticker)
        info = t.info
        # Prefer after-hours/pre-market prices when available
        price = info.get("postMarketPrice") or info.get("preMarketPrice") or info.get("currentPrice") or info.get("regularMarketPrice") or t.fast_info.get("lastPrice")
        if price:
            price_cache[ticker] = {"price": float(price), "time": now}
            return float(price)
    except Exception as e:
        print(f"Price fetch error for {ticker}: {e}")
    return None


def _is_market_open():
    """Check if US equity market is currently open (Mon-Fri 9:30-16:00 ET)."""
    from datetime import timezone
    try:
        import zoneinfo
        et = zoneinfo.ZoneInfo("America/New_York")
    except ImportError:
        # Fallback: assume UTC-4 for ET
        et = timezone(timedelta(hours=-4))
    now_et = datetime.now(et)
    weekday = now_et.weekday()  # 0=Mon, 6=Sun
    if weekday >= 5:  # Weekend
        return False
    market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now_et <= market_close


def get_option_price(occ_symbol):
    """Fetch live option price using yfinance. OCC symbol like SOFI260918C00030000.
    Uses mark (mid bid/ask) during market hours. Caches last good mark for weekends."""
    if not HAS_YFINANCE or not occ_symbol:
        return None
    if occ_symbol in BAD_OCC_SYMBOLS:
        return None

    market_open = _is_market_open()

    try:
        t = yf.Ticker(occ_symbol)
        info = t.info
        bid = info.get("bid", 0) or 0
        ask = info.get("ask", 0) or 0

        # During market hours: use live mark (mid bid/ask)
        if bid > 0 and ask > 0:
            mark = float((bid + ask) / 2)
            option_mark_cache[occ_symbol] = mark  # cache for after-hours/weekend
            return mark

        # Market closed: use cached mark from last live session if available
        if not market_open and occ_symbol in option_mark_cache:
            return option_mark_cache[occ_symbol]

        # Fallback: regularMarketPrice or lastPrice
        price = info.get("regularMarketPrice") or info.get("lastPrice") or t.fast_info.get("lastPrice")
        if price:
            price = float(price)
            # Only cache as mark if we don't already have a better one
            if occ_symbol not in option_mark_cache:
                option_mark_cache[occ_symbol] = price
            return price
    except Exception as e:
        # If fetch fails, return cached mark if available
        if occ_symbol in option_mark_cache:
            return option_mark_cache[occ_symbol]
        print(f"  Option price error for {occ_symbol}: {e}")
    return None


def _batch_fetch_prices(tickers):
    """Batch-fetch prices for multiple tickers using yf.download().
    Returns dict of {ticker: price}. Falls back to individual .info calls
    for after-hours/pre-market pricing when market is closed."""
    if not HAS_YFINANCE or not tickers:
        return {}

    prices = {}
    market_open = _is_market_open()

    # Step 1: Batch download for all tickers at once (~2-3 sec total vs ~2 sec each)
    try:
        print(f"  Batch downloading {len(tickers)} tickers...")
        df = yf.download(tickers, period="2d", group_by="ticker", auto_adjust=True, progress=False)
        if df is not None and not df.empty:
            for ticker in tickers:
                try:
                    if len(tickers) == 1:
                        # Single ticker: columns are just OHLCV, not multi-level
                        close_series = df["Close"]
                    else:
                        close_series = df[ticker]["Close"]
                    close_series = close_series.dropna()
                    if not close_series.empty:
                        prices[ticker] = float(close_series.iloc[-1])
                except (KeyError, IndexError):
                    pass
    except Exception as e:
        print(f"  Batch download error: {e}")
    finally:
        # Explicit cleanup — free the DataFrame memory immediately
        try:
            del df
        except NameError:
            pass

    # Step 2: Outside market hours, fetch after-hours prices individually
    # Only for tickers where we need postMarketPrice/preMarketPrice
    if not market_open:
        print(f"  Market closed — fetching after-hours prices individually...")
        for ticker in tickers:
            try:
                info = yf.Ticker(ticker).info
                ah_price = info.get("postMarketPrice") or info.get("preMarketPrice")
                if ah_price:
                    prices[ticker] = float(ah_price)
                elif ticker not in prices:
                    # Batch didn't get it either, try currentPrice
                    cp = info.get("currentPrice") or info.get("regularMarketPrice")
                    if cp:
                        prices[ticker] = float(cp)
            except Exception as e:
                print(f"    After-hours fetch error for {ticker}: {e}")

    # Update price_cache for all fetched prices
    now = time.time()
    for ticker, price in prices.items():
        price_cache[ticker] = {"price": price, "time": now}

    return prices


def refresh_all_prices():
    results = {}
    crypto_map = {"ETH": "ETH-USD", "BTC": "BTC-USD"}

    # Equities + Crypto: batch fetch all tickers at once
    print("  Refreshing equity + crypto prices (batch)...")
    eq_tickers = [e["ticker"] for e in store["equities"] if e["ticker"] not in ("OPENZ", "OPENL", "OPENW")]
    cr_tickers = [crypto_map.get(c["asset"], f"{c['asset']}-USD") for c in store["crypto"]]
    all_tickers = eq_tickers + cr_tickers
    batch_prices = _batch_fetch_prices(all_tickers)

    # Apply equity prices
    for e in store["equities"]:
        ticker = e["ticker"]
        if ticker in ("OPENZ", "OPENL", "OPENW"):
            continue
        price = batch_prices.get(ticker)
        if price:
            e["live_price"] = price
            e["market_value"] = price * e["quantity"]
            e["pl_dollar"] = e["market_value"] - (e.get("total_invested", 0) or 0)
            if (e.get("total_invested", 0) or 0) > 0:
                e["pl_percent"] = e["pl_dollar"] / (e.get("total_invested", 0) or 1)
            results[ticker] = price
            print(f"    {ticker}: ${price:.2f}")

    # Options (individual calls — need bid/ask from .info)
    print("  Refreshing option prices...")
    _options_freshly_priced = 0  # Track how many options got fresh marks this refresh
    for o in store["options"]:
        long_sym = o.get("occ_long", "")
        short_sym = o.get("occ_short", "")
        updated = False

        if long_sym and long_sym not in ("NaN", "nan", ""):
            price = get_option_price(long_sym)
            if price:
                o["live_price_long"] = price
                updated = True

        if short_sym and short_sym not in ("NaN", "nan", ""):
            price = get_option_price(short_sym)
            if price:
                o["live_price_short"] = price
                updated = True

        if updated:
            _options_freshly_priced += 1
            long_p = o.get("live_price_long", 0) or 0
            short_p = o.get("live_price_short", 0) or 0
            is_cc = long_sym and not re.match(r'^[A-Z]+\d{6}[CP]\d{8}$', long_sym) and short_sym and re.match(r'^[A-Z]+\d{6}[CP]\d{8}$', short_sym)
            if is_cc:
                # Covered call: P/L = total premium - closed roll costs - live cost to close
                cc_ticker = long_sym.upper()
                premium = 0
                closed_roll_costs = 0
                for cc in store["cc_income"]:
                    if cc.get("ticker", "").upper() == cc_ticker and not cc.get("date_closed"):
                        premium = float(cc.get("premium_collected", 0) or 0)
                        closed_roll_costs = float(cc.get("cost_to_close", 0) or 0)
                        break
                live_cost = short_p * (o["contracts"] or 0) * 100
                total_cost = closed_roll_costs + live_cost
                o["cc_premium_collected"] = premium
                o["cc_cost_to_close"] = round(total_cost, 2)
                o["cc_closed_roll_costs"] = round(closed_roll_costs, 2)
                o["net_spread"] = round((premium - total_cost) / max((o["contracts"] or 1) * 100, 1), 4)
                o["total_value"] = round(premium - total_cost, 2)
                o["pl_dollar"] = o["total_value"]
            else:
                o["net_spread"] = long_p - short_p if short_p > 0 else long_p
                o["total_value"] = o["net_spread"] * (o["contracts"] or 0) * 100
                o["pl_dollar"] = o["total_value"] - (o.get("total_invested", 0) or 0)
            results[o["position_name"]] = {"long": long_p, "short": short_p}
            print(f"    {o['position_name']}: long=${long_p:.2f} short=${short_p:.2f}")

    # Save daily option marks for interval P/L history (keyed by position id)
    today_str = date.today().isoformat()
    mark_hist = store.get("option_mark_history", {})
    for o in store["options"]:
        pos_id = str(o.get("id", ""))
        pos_name = o.get("position_name", "")
        if not pos_id or not pos_name:
            continue
        long_p = o.get("live_price_long", 0) or 0
        short_p = o.get("live_price_short", 0) or 0
        is_cc = o.get("cc_premium_collected") is not None
        if is_cc:
            mark = round((o.get("cc_premium_collected", 0) or 0) - (short_p * (o.get("contracts", 0) or 0) * 100), 2)
        else:
            mark = round(long_p - short_p if short_p > 0 else long_p, 4)
        if mark <= 0:
            continue
        if pos_id not in mark_hist:
            mark_hist[pos_id] = {"position_name": pos_name, "marks": []}
        else:
            mark_hist[pos_id]["position_name"] = pos_name
        # Update today's entry or append
        entries = mark_hist[pos_id]["marks"]
        if entries and entries[-1].get("date") == today_str:
            entries[-1]["mark"] = mark
        else:
            entries.append({"date": today_str, "mark": mark})
        # Keep 90 days
        if len(entries) > 90:
            mark_hist[pos_id]["marks"] = entries[-90:]
    store["option_mark_history"] = mark_hist

    # Apply crypto prices from batch
    print("  Applying crypto prices from batch...")
    for c in store["crypto"]:
        yf_ticker = crypto_map.get(c["asset"], f"{c['asset']}-USD")
        price = batch_prices.get(yf_ticker)
        if price:
            c["current_price"] = price
            c["market_value"] = price * c["quantity"]
            c["pl_dollar"] = c["market_value"] - (c["avg_price"] * c["quantity"])
            c["pl_percent"] = c["pl_dollar"] / (c["avg_price"] * c["quantity"]) if c["avg_price"] > 0 else 0
            results[c["asset"]] = price
            print(f"    {c['asset']}: ${price:.2f}")

    # Recalculate summary
    eq_val = sum(e["market_value"] for e in store["equities"])
    eq_inv = sum((e.get("total_invested", 0) or 0) for e in store["equities"])
    cr_val = sum(c["market_value"] for c in store["crypto"])
    cr_inv = sum(c["avg_price"] * c["quantity"] for c in store["crypto"])
    # Non-CC options use total_value; CC options excluded (premium in cash, liability subtracted)
    opt_val = sum((o.get("total_value", 0) or 0) for o in store["options"] if o.get("cc_premium_collected") is None)
    cc_liability = sum(
        float(o.get("cc_cost_to_close", 0) or 0) - float(o.get("cc_closed_roll_costs", 0) or 0)
        for o in store["options"] if o.get("cc_premium_collected") is not None
    )
    opt_val -= cc_liability
    opt_inv = sum(0 if o.get("cc_premium_collected") is not None else (o.get("total_invested", 0) or 0) for o in store["options"])
    card_val = sum(c["est_value"] for c in store["cards"])

    # Realized CC income from closed covered calls
    realized_cc = sum(
        float(e.get("net_profit", 0) or 0)
        for e in store["cc_income"]
        if e.get("date_closed")
    )

    total_inv = eq_inv + opt_inv + cr_inv
    total_val = eq_val + opt_val + cr_val
    cash_bal = float(store.get("cash_balance", 0) or 0)
    net_worth = total_val + cash_bal
    store["summary"].update({
        "equities_value": eq_val,
        "equities_invested": eq_inv,
        "options_value": opt_val,
        "options_invested": opt_inv,
        "crypto_value": cr_val,
        "crypto_invested": cr_inv,
        "card_value": card_val,
        "market_value": total_val,
        "total_invested": total_inv,
        "total_pl_dollar": total_val - total_inv,
        "total_pl_percent": (total_val - total_inv) / total_inv if total_inv > 0 else 0,
        "realized_cc_income": round(realized_cc, 2),
        "cash_balance": round(cash_bal, 2),
        "net_worth": net_worth,
    })

    save_store()

    # Mark cache as warm after first successful refresh
    # Used by save_options_value() to avoid logging garbage values
    global _mark_cache_warm, _mark_cache_warm_ts
    if not _mark_cache_warm:
        _mark_cache_warm = True
        _mark_cache_warm_ts = time.time()
        print(f"  Mark cache now warm — {_options_freshly_priced} options priced")

    save_options_value()
    save_intraday_value()
    return results


# ── OpenInsider Scraping ─────────────────────────────────────────────────
def scrape_insider(ticker):
    if not HAS_SCRAPING:
        return {"error": "requests/beautifulsoup4 not installed"}
    try:
        url = f"http://openinsider.com/screener?s={ticker}&o=&pl=&ph=&st=0&lt=0&lk=&pp=&sp=&lp=&session_id=&ession_id="
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        resp = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(resp.text, "lxml")
        table = soup.find("table", {"class": "tinytable"})
        if not table:
            return {"ticker": ticker, "trades": []}

        rows = table.find_all("tr")[1:]  # skip header
        trades = []
        for row in rows[:20]:  # limit 20 most recent
            cols = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cols) >= 12:
                trades.append({
                    "filing_date": cols[1],
                    "trade_date": cols[2],
                    "ticker": cols[3],
                    "insider_name": cols[4],
                    "title": cols[5],
                    "trade_type": cols[6],
                    "price": cols[7],
                    "qty": cols[8],
                    "owned": cols[9],
                    "delta_own": cols[10],
                    "value": cols[11],
                })
        return {"ticker": ticker, "trades": trades}
    except Exception as e:
        return {"ticker": ticker, "trades": [], "error": str(e)}


def scrape_insider_portfolio():
    results = {}
    tickers = list(set(e["ticker"] for e in store["equities"] if e["ticker"] not in ("OPENZ", "OPENL", "OPENW")))
    for t in tickers:
        results[t] = scrape_insider(t)
        time.sleep(0.5)  # rate limit
    return results


# ── eBay Card Valuation ──────────────────────────────────────────────────
def search_ebay_card(card):
    if not HAS_SCRAPING:
        return {"error": "requests/beautifulsoup4 not installed"}
    try:
        query = f"{card['player']} {card['year']} {card['brand']} {card['card_name']}"
        url = f"https://www.ebay.com/sch/i.html?_nkw={requests.utils.quote(query)}&LH_Complete=1&LH_Sold=1&_sop=13"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        resp = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(resp.text, "lxml")

        items = soup.select(".s-item")
        prices = []
        for item in items[:15]:
            price_el = item.select_one(".s-item__price")
            if price_el:
                price_text = price_el.get_text(strip=True).replace("$", "").replace(",", "")
                try:
                    if "to" in price_text:
                        parts = price_text.split("to")
                        prices.append((float(parts[0]) + float(parts[1])) / 2)
                    else:
                        prices.append(float(price_text))
                except ValueError:
                    pass

        if prices:
            prices.sort()
            avg = sum(prices) / len(prices)
            median = prices[len(prices) // 2]
            return {
                "card_id": card["id"],
                "query": query,
                "num_results": len(prices),
                "average": round(avg, 2),
                "median": round(median, 2),
                "low": round(min(prices), 2),
                "high": round(max(prices), 2),
                "recent_sales": [round(p, 2) for p in prices[:5]],
            }
        return {"card_id": card["id"], "query": query, "num_results": 0, "message": "No sold listings found"}
    except Exception as e:
        return {"card_id": card["id"], "error": str(e)}


# ── Tax Ledger Helpers ───────────────────────────────────────────────────
def get_tax_summary():
    total_realized = 0
    total_reserve = 0
    short_term_pl = 0
    long_term_pl = 0
    open_positions = 0
    closed_positions = 0

    for entry in store["tax_ledger"]:
        rpl = entry.get("realized_pl", 0)
        if isinstance(rpl, (int, float)):
            total_realized += rpl
        tr = entry.get("tax_reserve", "0")
        if isinstance(tr, str):
            try:
                tr = float(tr) if tr not in ("Open", "0", "") else 0
            except ValueError:
                tr = 0
        total_reserve += tr

        if entry.get("date_closed"):
            closed_positions += 1
            status = entry.get("tax_status", "")
            if "Short" in status:
                short_term_pl += (rpl if isinstance(rpl, (int, float)) else 0)
            else:
                long_term_pl += (rpl if isinstance(rpl, (int, float)) else 0)
        else:
            open_positions += 1

    return {
        "total_realized_pl": round(total_realized, 2),
        "total_tax_reserve": round(total_reserve, 2),
        "short_term_pl": round(short_term_pl, 2),
        "long_term_pl": round(long_term_pl, 2),
        "open_positions": open_positions,
        "closed_positions": closed_positions,
    }


def close_tax_ledger_entry(ticker, date_closed, sell_price, realized_pl, days_held, tax_status, cost_to_close=None):
    """Find the open tax ledger entry for this ticker and update it with close details.
    Returns the updated entry or None if not found."""
    ticker_upper = ticker.upper()
    # Try exact match first, then root ticker match
    for tl in store["tax_ledger"]:
        tl_ticker = (tl.get("ticker") or "").upper()
        if tl_ticker == ticker_upper and not tl.get("date_closed"):
            tl["date_closed"] = date_closed
            tl["sell_price"] = sell_price
            tl["realized_pl"] = round(realized_pl, 2)
            tl["days_held"] = days_held
            tl["tax_status"] = tax_status
            tl["tax_reserve"] = str(round(max(realized_pl, 0) * 0.3, 2))
            if cost_to_close is not None:
                tl["cost_to_close"] = cost_to_close
            save_store()
            return tl
    return None


def compute_cash_flow():
    events = []
    # Track CC income ticker+date combos to avoid double-counting with tax_ledger
    cc_income_keys = set()       # for premium collected (open)
    cc_income_close_keys = set()  # for cost to close
    cc_income_open_keys = set()   # (ticker, date_opened) for CCs that have a close in cc_income
    for entry in store["cc_income"]:
        key = (entry.get("ticker", ""), entry.get("date_opened", ""), float(entry.get("premium_collected", 0) or 0))
        cc_income_keys.add(key)
        if entry.get("date_opened") and float(entry.get("premium_collected", 0) or 0) > 0:
            events.append({
                "date": entry["date_opened"],
                "type": "cc_premium",
                "direction": "in",
                "ticker": entry.get("ticker", ""),
                "asset_type": "Covered Calls",
                "amount": round(float(entry["premium_collected"]), 2),
                "description": f"CC premium {entry.get('ticker','')} {entry.get('contract_details','')}"
            })
        if entry.get("date_closed") and float(entry.get("cost_to_close", 0) or 0) > 0:
            close_key = (entry.get("ticker", ""), entry.get("date_closed", ""), float(entry["cost_to_close"]))
            cc_income_close_keys.add(close_key)
            cc_income_open_keys.add((entry.get("ticker", ""), entry.get("date_opened", "")))
            events.append({
                "date": entry["date_closed"],
                "type": "cc_close",
                "direction": "out",
                "ticker": entry.get("ticker", ""),
                "asset_type": "Covered Calls",
                "amount": round(float(entry["cost_to_close"]), 2),
                "description": f"Closed CC {entry.get('ticker','')} {entry.get('contract_details','')}"
            })

    for entry in store["tax_ledger"]:
        asset = entry.get("asset_type", "")
        ticker = entry.get("ticker", "")
        qty = float(entry.get("quantity", 0) or 0)
        cost = float(entry.get("cost_basis", 0) or 0)
        sell = float(entry.get("sell_price", 0) or 0)
        prem = float(entry.get("premium_received", 0) or 0)

        # Buy event (capital out) — skip CC with 0 cost_basis
        if entry.get("date_acquired") and cost > 0:
            if asset == "Equities" or asset == "Equity":
                desc = f"Bought {int(qty)} {ticker} @ ${entry.get('purchase_price', 0):.2f}"
            else:
                desc = f"Bought {ticker} (cost ${cost:.2f})"
            events.append({
                "date": entry["date_acquired"],
                "type": "buy",
                "direction": "out",
                "ticker": ticker.split()[0] if " " in ticker else ticker,
                "asset_type": asset,
                "amount": round(cost, 2),
                "description": desc
            })

        # Sell / close event (capital in)
        if entry.get("date_closed"):
            base = ticker.split()[0] if " " in ticker else ticker
            if asset in ("Equities", "Equity") and (sell > 0 or cost > 0):
                # Derive total proceeds from realized_pl + cost_basis (works regardless of sell_price format)
                rpl = float(entry.get("realized_pl", 0) or 0)
                total_proceeds = rpl + cost if (rpl != 0 or cost > 0) else sell
                per_share = total_proceeds / qty if qty else sell
                events.append({
                    "date": entry["date_closed"], "type": "sell", "direction": "in",
                    "ticker": base, "asset_type": asset,
                    "amount": round(total_proceeds, 2),
                    "description": f"Sold {int(qty)} {ticker} @ ${per_share:.2f}"
                })
            elif asset in ("Call Spreads",) and sell > 0:
                # Spreads: sell_price is total proceeds
                events.append({
                    "date": entry["date_closed"], "type": "sell", "direction": "in",
                    "ticker": base, "asset_type": asset,
                    "amount": round(sell, 2),
                    "description": f"Closed {ticker} (proceeds ${sell:.2f})"
                })
            elif asset in ("Calls", "Puts", "Option") and prem > 0:
                # Options sold to close: premium_received is total cash received
                events.append({
                    "date": entry["date_closed"], "type": "sell", "direction": "in",
                    "ticker": base, "asset_type": asset,
                    "amount": round(prem, 2),
                    "description": f"Sold {int(qty)} {ticker} (${prem:.2f})"
                })
            elif asset in ("Calls", "Puts", "Option") and sell > 0:
                # Fallback: some options may use sell_price
                events.append({
                    "date": entry["date_closed"], "type": "sell", "direction": "in",
                    "ticker": base, "asset_type": asset,
                    "amount": round(sell, 2),
                    "description": f"Closed {ticker} (proceeds ${sell:.2f})"
                })
            elif asset == "Covered Calls" and sell > 0:
                # CC bought to close: sell_price is cost to close (cash OUT)
                # Skip if same CC trade already tracked via cc_income (match on ticker+open date)
                open_key = (base, entry.get("date_acquired", ""))
                if open_key not in cc_income_open_keys:
                    events.append({
                        "date": entry["date_closed"], "type": "cc_close", "direction": "out",
                        "ticker": base, "asset_type": asset,
                        "amount": round(sell, 2),
                        "description": f"Closed CC {ticker} (cost ${sell:.2f})"
                    })

        # CC premium from tax_ledger — only if not already tracked via cc_income
        if asset == "Covered Calls" and prem > 0 and entry.get("date_acquired"):
            base_ticker = ticker.split()[0] if " " in ticker else ticker
            key = (base_ticker, entry["date_acquired"], prem)
            if key not in cc_income_keys:
                events.append({
                    "date": entry["date_acquired"],
                    "type": "cc_premium",
                    "direction": "in",
                    "ticker": base_ticker,
                    "asset_type": "Covered Calls",
                    "amount": round(prem, 2),
                    "description": f"CC premium {ticker}"
                })

    # Manual cash deposits/withdrawals
    for entry in store["cash_flows"]:
        if not entry.get("date"):
            continue
        cf_type = entry.get("type", "deposit")
        amt = float(entry.get("amount", 0) or 0)
        if amt <= 0:
            continue
        events.append({
            "date": entry["date"],
            "type": cf_type,
            "direction": "in" if cf_type in ("deposit", "dividend") else "out",
            "ticker": "",
            "asset_type": "Dividend" if cf_type == "dividend" else "Cash",
            "amount": round(amt, 2),
            "description": entry.get("description", "") or (f"Cash {'deposit' if cf_type == 'deposit' else 'dividend' if cf_type == 'dividend' else 'withdrawal'}")
        })

    events.sort(key=lambda e: e["date"], reverse=True)

    # Build rollups
    daily = defaultdict(lambda: {"deployed": 0, "received": 0})
    for e in events:
        d = e["date"]
        if e["direction"] == "out":
            daily[d]["deployed"] += e["amount"]
        else:
            daily[d]["received"] += e["amount"]

    daily_list = []
    for d in sorted(daily.keys()):
        r = daily[d]
        daily_list.append({
            "date": d,
            "deployed": round(r["deployed"], 2),
            "received": round(r["received"], 2),
            "net": round(r["received"] - r["deployed"], 2)
        })
    # Add cumulative
    cum_dep, cum_rec = 0, 0
    for row in daily_list:
        cum_dep += row["deployed"]
        cum_rec += row["received"]
        row["cum_deployed"] = round(cum_dep, 2)
        row["cum_received"] = round(cum_rec, 2)
        row["cum_net"] = round(cum_rec - cum_dep, 2)

    # Weekly rollup
    weekly = defaultdict(lambda: {"deployed": 0, "received": 0})
    for row in daily_list:
        dt = datetime.strptime(row["date"], "%Y-%m-%d").date()
        iso = dt.isocalendar()
        week_key = f"{iso[0]}-W{iso[1]:02d}"
        weekly[week_key]["deployed"] += row["deployed"]
        weekly[week_key]["received"] += row["received"]
    weekly_list = []
    cum_dep, cum_rec = 0, 0
    for w in sorted(weekly.keys()):
        r = weekly[w]
        cum_dep += r["deployed"]
        cum_rec += r["received"]
        weekly_list.append({
            "period": w, "deployed": round(r["deployed"], 2),
            "received": round(r["received"], 2), "net": round(r["received"] - r["deployed"], 2),
            "cum_deployed": round(cum_dep, 2), "cum_received": round(cum_rec, 2), "cum_net": round(cum_rec - cum_dep, 2)
        })

    # Monthly rollup
    monthly = defaultdict(lambda: {"deployed": 0, "received": 0})
    for row in daily_list:
        month_key = row["date"][:7]
        monthly[month_key]["deployed"] += row["deployed"]
        monthly[month_key]["received"] += row["received"]
    monthly_list = []
    cum_dep, cum_rec = 0, 0
    for m in sorted(monthly.keys()):
        r = monthly[m]
        cum_dep += r["deployed"]
        cum_rec += r["received"]
        monthly_list.append({
            "period": m, "deployed": round(r["deployed"], 2),
            "received": round(r["received"], 2), "net": round(r["received"] - r["deployed"], 2),
            "cum_deployed": round(cum_dep, 2), "cum_received": round(cum_rec, 2), "cum_net": round(cum_rec - cum_dep, 2)
        })

    total_dep = sum(e["amount"] for e in events if e["direction"] == "out")
    total_rec = sum(e["amount"] for e in events if e["direction"] == "in")

    return {
        "events": events,
        "daily": list(reversed(daily_list)),
        "weekly": list(reversed(weekly_list)),
        "monthly": list(reversed(monthly_list)),
        "cash_entries": store["cash_flows"],
        "totals": {
            "total_deployed": round(total_dep, 2),
            "total_received": round(total_rec, 2),
            "net_flow": round(total_rec - total_dep, 2),
            "transaction_count": len(events)
        }
    }


# ── Risk Dashboard ──────────────────────────────────────────────────────
import re

def parse_occ_symbol(occ):
    """Parse OCC symbol like SOFI260918C00030000 into components."""
    if not occ or len(occ) < 10:
        return None
    match = re.match(r'^([A-Z]+\d?)(\d{6})([CP])(\d{8})$', occ)
    if not match:
        return None
    ticker, date_str, cp_type, strike_raw = match.groups()
    try:
        expiry = datetime.strptime(date_str, '%y%m%d').date()
    except ValueError:
        return None
    strike = int(strike_raw) / 1000
    return {
        "underlying": ticker,
        "expiration": str(expiry),
        "type": "call" if cp_type == "C" else "put",
        "strike": strike
    }


def get_risk_info(ticker):
    """Get beta and sector for a ticker via yfinance, with caching."""
    now = time.time()
    if ticker in risk_info_cache:
        cached = risk_info_cache[ticker]
        if now - cached.get("ts", 0) < RISK_CACHE_TTL:
            return cached

    info = {"ticker": ticker, "beta": 1.0, "sector": "Unknown", "industry": "Unknown", "ts": now}
    if HAS_YFINANCE:
        try:
            t = yf.Ticker(ticker)
            yf_info = t.info or {}
            info["beta"] = yf_info.get("beta") or 1.0
            info["sector"] = yf_info.get("sector") or "Unknown"
            info["industry"] = yf_info.get("industry") or "Unknown"
        except Exception:
            pass
    risk_info_cache[ticker] = info
    return info


def compute_risk_dashboard():
    """Compute all portfolio risk metrics."""
    equities = store["equities"]
    options = store["options"]
    crypto = store["crypto"]

    # Skip warrants
    skip = {"OPENZ", "OPENL", "OPENW"}
    eq_positions = [e for e in equities if e["ticker"] not in skip]

    # Total portfolio value (cards excluded)
    eq_val = sum(e.get("market_value", 0) or 0 for e in eq_positions)
    opt_val = sum(o.get("total_value", 0) or 0 for o in options)
    cr_val = sum(c.get("market_value", 0) or 0 for c in crypto)
    total_val = eq_val + opt_val + cr_val
    if total_val == 0:
        total_val = 1  # avoid division by zero

    # ── Concentration Analysis (equities + crypto) ──
    positions = []
    for e in eq_positions:
        mv = e.get("market_value", 0) or 0
        pct = mv / total_val
        positions.append({
            "ticker": e["ticker"], "market_value": round(mv, 2),
            "pct": round(pct * 100, 2), "alert": pct > 0.15, "asset_class": "equity"
        })
    for c in crypto:
        mv = c.get("market_value", 0) or 0
        pct = mv / total_val
        positions.append({
            "ticker": c.get("asset", "CRYPTO"), "market_value": round(mv, 2),
            "pct": round(pct * 100, 2), "alert": pct > 0.15, "asset_class": "crypto"
        })
    positions.sort(key=lambda x: x["market_value"], reverse=True)
    top5_pct = round(sum(p["pct"] for p in positions[:5]), 2)
    hhi = round(sum(p["pct"] ** 2 for p in positions), 0)

    conc_alerts = []
    for p in positions:
        if p["alert"]:
            conc_alerts.append(f"{p['ticker']} is {p['pct']}% of portfolio (>15% threshold)")
    if top5_pct > 80:
        conc_alerts.append(f"Top 5 positions = {top5_pct}% of portfolio")

    # ── Beta Exposure (equities + crypto) ──
    CRYPTO_BETAS = {"BTC": 1.2, "ETH": 1.5, "SOL": 2.0}  # vs S&P 500 approximations
    beta_positions = []
    for p in positions:
        if p["asset_class"] == "crypto":
            beta = CRYPTO_BETAS.get(p["ticker"], 1.5)
        elif p["ticker"] == "BMNU":
            # BMNU = 2x leveraged BMNR — beta is 2x BMNR's beta
            bmnr_info = get_risk_info("BMNR")
            beta = (bmnr_info["beta"] or 1.0) * 2.0
        else:
            info = get_risk_info(p["ticker"])
            beta = info["beta"]
        weight = p["pct"] / 100
        beta_positions.append({
            "ticker": p["ticker"], "beta": round(beta, 2),
            "weight": round(weight, 3), "weighted_beta": round(beta * weight, 3)
        })
    portfolio_beta = round(sum(b["weighted_beta"] for b in beta_positions), 2)
    dollar_beta = round(total_val * portfolio_beta, 0)

    # ── Sector Exposure (equities + crypto) ──
    sector_map = {}
    for p in positions:
        if p["asset_class"] == "crypto":
            sector = "Cryptocurrency"
        elif p["ticker"] == "BMNU":
            # BMNU = 2x leveraged BMNR — use BMNR's sector
            bmnr_info = get_risk_info("BMNR")
            sector = bmnr_info["sector"]
        else:
            info = get_risk_info(p["ticker"])
            sector = info["sector"]
        if sector not in sector_map:
            sector_map[sector] = {"market_value": 0, "tickers": []}
        sector_map[sector]["market_value"] += p["market_value"]
        sector_map[sector]["tickers"].append(p["ticker"])

    sectors = []
    sector_alerts = []
    for sector, data in sorted(sector_map.items(), key=lambda x: x[1]["market_value"], reverse=True):
        pct = round(data["market_value"] / total_val * 100, 2)
        sectors.append({"sector": sector, "market_value": round(data["market_value"], 2), "pct": pct, "tickers": data["tickers"]})
        if pct > 30:
            sector_alerts.append(f"{sector} is {pct}% of portfolio (>30% threshold)")

    # ── Options At-Risk ──
    today = date.today()
    opt_positions = []
    expiring_soon = []
    def _opt_invested(o):
        avg = float(o.get("avg_cost", 0) or 0)
        contracts = float(o.get("contracts", 0) or 0)
        return avg * contracts * 100 if avg > 0 else (o.get("total_invested", 0) or 0)

    total_opt_invested = sum(_opt_invested(o) for o in options)

    for o in options:
        pos = {
            "name": o.get("position_name", ""),
            "invested": round(_opt_invested(o), 2),
            "current_value": round(o.get("total_value", 0) or 0, 2),
            "alerts": []
        }

        # Parse long leg
        parsed = parse_occ_symbol(o.get("occ_long", ""))
        parsed_short = parse_occ_symbol(o.get("occ_short", ""))

        if parsed:
            pos["underlying"] = parsed["underlying"]
            pos["expiration"] = parsed["expiration"]
            pos["strike"] = parsed["strike"]
            pos["type"] = parsed["type"]

            try:
                exp_date = datetime.strptime(parsed["expiration"], "%Y-%m-%d").date()
                days_to_exp = (exp_date - today).days
                pos["days_to_expiry"] = days_to_exp
                if days_to_exp <= 30:
                    pos["alerts"].append(f"{parsed['underlying']} {parsed['type'].upper()} ${parsed['strike']}: Expires in {days_to_exp} days")
                    pos["recommendations"] = pos.get("recommendations", [])
                    if days_to_exp <= 7:
                        pos["recommendations"].extend([
                            f"Roll {parsed['underlying']} to a later expiration to maintain exposure",
                            f"Close position to lock in {'gains' if (pos['current_value'] > pos['invested']) else 'remaining value'}"
                        ])
                    else:
                        pos["recommendations"].extend([
                            f"Monitor {parsed['underlying']} price action — set a stop-loss or take-profit target",
                            f"Consider rolling to a later expiration if thesis is still intact"
                        ])
                    expiring_soon.append(pos)
            except ValueError:
                pass

            # Get underlying price for moneyness
            underlying_price = 0
            for e in equities:
                if e["ticker"].upper() == parsed["underlying"].upper():
                    underlying_price = e.get("live_price", 0) or e.get("market_value", 0) / max(e.get("quantity", 1), 1)
                    break
            if underlying_price > 0:
                pos["underlying_price"] = round(underlying_price, 2)
                distance = (parsed["strike"] - underlying_price) / underlying_price
                if parsed["type"] == "call":
                    pos["moneyness"] = "ITM" if underlying_price > parsed["strike"] else "OTM"
                else:
                    pos["moneyness"] = "ITM" if underlying_price < parsed["strike"] else "OTM"
                pos["distance_pct"] = round(distance * 100, 1)
                if pos["moneyness"] == "OTM" and abs(distance) > 0.25:
                    pos["alerts"].append(f"{parsed['underlying']} ${parsed['strike']} {parsed['type'].upper()}: Deep OTM ({abs(pos['distance_pct'])}% away)")
                    pos["recommendations"] = pos.get("recommendations", [])
                    pos["recommendations"].extend([
                        f"Cut losses on {parsed['underlying']} — position is {abs(pos['distance_pct'])}% OTM with declining time value",
                        f"Roll down to a closer strike to increase probability of profit"
                    ])

        elif parsed_short and not parsed:
            # Covered call — long leg is just ticker
            pos["underlying"] = o.get("occ_long", "")
            ps = parsed_short
            pos["expiration"] = ps["expiration"]
            pos["strike"] = ps["strike"]
            pos["type"] = "covered_call"
            try:
                exp_date = datetime.strptime(ps["expiration"], "%Y-%m-%d").date()
                days_to_exp = (exp_date - today).days
                pos["days_to_expiry"] = days_to_exp
                if days_to_exp <= 30:
                    pos["alerts"].append(f"{pos['underlying']} CC ${ps['strike']}: Expires in {days_to_exp} days")
                    pos["recommendations"] = [
                        f"Let {pos['underlying']} CC expire if OTM to keep full premium",
                        f"Roll to next month if you want to continue collecting premium"
                    ]
                    expiring_soon.append(pos)
            except ValueError:
                pass

        # Max loss
        if parsed_short and parsed:
            # Spread — max loss is what you paid
            pos["max_loss"] = pos["invested"]
        else:
            pos["max_loss"] = pos["invested"]

        # CC P/L uses pl_dollar directly (premium - costs); regular options use value - invested
        if o.get("cc_premium_collected") is not None:
            pos["pl"] = round(o.get("pl_dollar", 0) or 0, 2)
        else:
            pos["pl"] = round(pos["current_value"] - pos["invested"], 2)
        opt_positions.append(pos)

    # ── Underlying Exposure (equity + options + crypto combined) ──
    exposure_map = {}
    for e in eq_positions:
        t = e["ticker"]
        if t not in exposure_map:
            exposure_map[t] = {"equity_value": 0, "options_value": 0, "crypto_value": 0}
        exposure_map[t]["equity_value"] += e.get("market_value", 0) or 0

    for c in crypto:
        t = c.get("asset", "CRYPTO")
        if t not in exposure_map:
            exposure_map[t] = {"equity_value": 0, "options_value": 0, "crypto_value": 0}
        exposure_map[t]["crypto_value"] += c.get("market_value", 0) or 0

    for o in options:
        parsed = parse_occ_symbol(o.get("occ_long", ""))
        if not parsed:
            parsed = parse_occ_symbol(o.get("occ_short", ""))
        if not parsed:
            # Covered call with ticker as long
            t = o.get("occ_long", "").upper()
        else:
            t = parsed["underlying"]
        if t:
            if t not in exposure_map:
                exposure_map[t] = {"equity_value": 0, "options_value": 0, "crypto_value": 0}
            exposure_map[t]["options_value"] += o.get("total_value", 0) or 0

    underlying_exposure = []
    for t, vals in sorted(exposure_map.items(), key=lambda x: x[1]["equity_value"] + x[1]["options_value"] + x[1]["crypto_value"], reverse=True):
        total_exp = vals["equity_value"] + vals["options_value"] + vals["crypto_value"]
        underlying_exposure.append({
            "ticker": t,
            "equity_value": round(vals["equity_value"], 2),
            "options_value": round(vals["options_value"], 2),
            "crypto_value": round(vals["crypto_value"], 2),
            "total_exposure": round(total_exp, 2),
            "pct": round(total_exp / total_val * 100, 2)
        })

    # ── All alerts ──
    all_alerts = conc_alerts + sector_alerts
    for op in opt_positions:
        all_alerts.extend(op["alerts"])

    # Risk score
    alert_count = len(all_alerts)
    if alert_count >= 6:
        risk_score = "HIGH"
    elif alert_count >= 3:
        risk_score = "MODERATE"
    elif alert_count >= 1:
        risk_score = "LOW"
    else:
        risk_score = "MINIMAL"

    return {
        "concentration": {
            "positions": positions,
            "top5_pct": top5_pct,
            "hhi": hhi,
            "alerts": conc_alerts
        },
        "beta": {
            "positions": beta_positions,
            "portfolio_beta": portfolio_beta,
            "dollar_beta_exposure": dollar_beta,
        },
        "sectors": {
            "breakdown": sectors,
            "alerts": sector_alerts
        },
        "options_risk": {
            "total_invested": round(total_opt_invested, 2),
            "total_value": round(opt_val, 2),
            "pct_of_portfolio": round(opt_val / total_val * 100, 2),
            "positions": opt_positions,
            "expiring_soon": expiring_soon,
        },
        "underlying_exposure": underlying_exposure,
        "total_portfolio_value": round(total_val, 2),
        "risk_score": risk_score,
        "alerts": all_alerts,
    }


# ── Watchlist Alerts ─────────────────────────────────────────────────────
def compute_watchlist_alerts():
    """Check watchlist tickers against live prices, dark pool, and insiders."""
    alerts = []
    for item in store["watchlist"]:
        if not item.get("alerts_enabled", True):
            continue
        ticker = item.get("ticker", "").upper()
        target = float(item.get("target_buy", 0) or 0)
        if not ticker:
            continue

        # Get live price
        price = get_live_price(ticker) if HAS_YFINANCE else 0
        if price and target > 0 and price <= target:
            alerts.append({
                "ticker": ticker,
                "type": "price_target",
                "message": f"{ticker} hit target! Current ${price:.2f} <= target ${target:.2f}",
                "priority": "high",
            })

        # Check dark pool if available
        if HAS_PROVIDERS:
            try:
                dp = get_darkpool_provider()
                latest = dp.get_latest(ticker)
                if latest and latest.get("short_ratio", 0) > 50:
                    alerts.append({
                        "ticker": ticker,
                        "type": "dark_pool",
                        "message": f"{ticker} dark pool short ratio {latest['short_ratio']}% (elevated >50%)",
                        "priority": "medium",
                    })
            except Exception:
                pass

            try:
                ip = get_insider_provider()
                trades = ip.get_recent_trades(ticker, days=14)
                buys = [t for t in trades if t.get("is_purchase")]
                if buys:
                    alerts.append({
                        "ticker": ticker,
                        "type": "insider_buy",
                        "message": f"{ticker} insider buying: {buys[0].get('insider_name','')} — {buys[0].get('value','')} ({buys[0].get('trade_date','')})",
                        "priority": "high",
                    })
            except Exception:
                pass

    # ETH ETF Flow alerts + BMNR NAV alerts
    if HAS_PROVIDERS:
        try:
            efp = get_etf_flow_provider()
            streak = efp.get_streak_info()
            # 1. Consecutive inflow streak
            if streak["streak"] >= 3 and streak["direction"] == "inflow":
                alerts.append({"ticker": "ETH-ETF", "type": "etf_flow_streak",
                    "message": f"ETH ETFs: {streak['streak']} consecutive inflow days (${streak['total']:+.0f}M)", "priority": "medium"})
            # 2. Consecutive outflow streak
            elif streak["streak"] >= 3 and streak["direction"] == "outflow":
                alerts.append({"ticker": "ETH-ETF", "type": "etf_flow_streak",
                    "message": f"ETH ETFs: {abs(streak['streak'])} consecutive outflow days (${streak['total']:+.0f}M)", "priority": "medium"})
            # 3. Large single-day flow spike
            yesterday = efp.get_yesterday_flow()
            avg30 = efp.get_30d_avg()
            if avg30 > 0 and abs(yesterday.get("total", 0)) > 2 * avg30:
                alerts.append({"ticker": "ETH-ETF", "type": "etf_flow_spike",
                    "message": f"ETH ETF unusual flow: ${yesterday['total']:+.0f}M (2x+ avg of ${avg30:.0f}M)", "priority": "high"})
            # 4. Weekly flow regime change
            regime = efp.get_weekly_regime_change()
            if regime:
                alerts.append({"ticker": "ETH-ETF", "type": "etf_regime_change",
                    "message": regime["message"], "priority": "medium"})
        except Exception as e:
            print(f"  ETF flow alert error: {e}")

        try:
            nav_data = get_bmnr_nav_provider().get_nav()
            pct = nav_data.get("premium_discount_pct", 0)
            # 5. BMNR NAV discount opportunity
            if pct < -15:
                alerts.append({"ticker": "BMNR", "type": "nav_discount",
                    "message": f"BMNR at {pct:+.1f}% discount to NAV — historically cheap", "priority": "high"})
            # 6. BMNR NAV premium warning
            elif pct > 25:
                alerts.append({"ticker": "BMNR", "type": "nav_premium",
                    "message": f"BMNR at {pct:+.1f}% premium to NAV — historically expensive", "priority": "high"})
        except Exception as e:
            print(f"  BMNR NAV alert error: {e}")

    return alerts


# ── HTTP Request Handler ────────────────────────────────────────────────
BAD_OCC_SYMBOLS = set()  # Add OCC symbols that cause Yahoo 404 loops
_fg_cache = {"data": {"equity": None, "crypto": None}, "ts": 0}

def get_fear_greed():
    """Fetch equity (CNN) and crypto (Alternative.me) Fear & Greed indices. Cached 15 min."""
    import time as _t
    # Return cache if fresh (15 minutes)
    if _fg_cache["data"] and (_fg_cache["data"]["equity"] or _fg_cache["data"]["crypto"]) and (_t.time() - _fg_cache["ts"]) < 900:
        return _fg_cache["data"]

    result = {"equity": None, "crypto": None}

    # Crypto Fear & Greed (Alternative.me)
    try:
        import urllib.request
        req = urllib.request.Request("https://api.alternative.me/fng/?limit=30",
                                     headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=5)
        data = json.loads(resp.read())
        entries = data.get("data", [])
        if entries:
            result["crypto"] = {
                "value": int(entries[0]["value"]),
                "label": entries[0]["value_classification"],
                "timestamp": entries[0].get("timestamp", ""),
                "previous_close": int(entries[1]["value"]) if len(entries) > 1 else 0,
                "week_ago": int(entries[7]["value"]) if len(entries) > 7 else 0,
                "month_ago": int(entries[29]["value"]) if len(entries) > 29 else 0,
            }
    except Exception as e:
        print(f"  Crypto F&G error: {e}")

    # Equity Fear & Greed (CNN via fear-greed package) - with 8s timeout
    try:
        import concurrent.futures
        def _fetch_equity():
            import fear_greed
            fg = fear_greed.get()
            history = fg.get("history", {})
            return {
                "value": round(fg.get("score", 0)),
                "label": fg.get("rating", ""),
                "previous_close": round(fg.get("score", 0)),
                "week_ago": round(history.get("1w", 0)),
                "month_ago": round(history.get("1m", 0)),
                "year_ago": round(history.get("1y", 0)),
            }
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(_fetch_equity)
            result["equity"] = future.result(timeout=8)
    except concurrent.futures.TimeoutError:
        print("  Equity F&G timeout (8s) - returning cached or None")
        if _fg_cache["data"] and _fg_cache["data"].get("equity"):
            result["equity"] = _fg_cache["data"]["equity"]
    except ImportError:
        print("  fear-greed package not installed")
    except Exception as e:
        print(f"  Equity F&G error: {e}")
        if _fg_cache["data"] and _fg_cache["data"].get("equity"):
            result["equity"] = _fg_cache["data"]["equity"]

    _fg_cache["data"] = result
    _fg_cache["ts"] = _t.time()
    return result


# ── Benchmark (SPY / QQQ YTD returns) ─────────────────────────────────
_bench_cache = {"data": None, "ts": 0}
_FUTURES_MAP = {"SPY": "ES=F", "QQQ": "NQ=F"}

def _get_benchmark_price(ticker):
    """Get best available price for benchmark ETF, including overnight futures proxy."""
    from datetime import timezone as tz
    try:
        import zoneinfo
        et = zoneinfo.ZoneInfo("America/New_York")
    except ImportError:
        et = tz(timedelta(hours=-4))
    now_et = datetime.now(et)
    hour = now_et.hour
    is_overnight = hour >= 20 or hour < 4
    is_weekend = now_et.weekday() >= 5

    # Standard after-hours waterfall (works 4 AM – 8 PM ET)
    tk = yf.Ticker(ticker)
    info = tk.info
    price = (info.get("postMarketPrice") or info.get("preMarketPrice")
             or info.get("currentPrice") or info.get("regularMarketPrice"))
    regular_close = info.get("regularMarketPrice") or tk.fast_info.get("lastPrice")

    # During overnight or weekends, use futures proxy for live pricing
    if (is_overnight or is_weekend) and ticker in _FUTURES_MAP:
        try:
            ft = yf.Ticker(_FUTURES_MAP[ticker])
            ft_info = ft.info
            futures_price = ft_info.get("regularMarketPrice")
            if futures_price and regular_close and regular_close > 0:
                # Convert futures to ETF-equivalent using ratio from last close
                # ES=F / SPY ≈ 10x, NQ=F / QQQ ≈ ~40x — compute dynamically
                ft_regular = ft_info.get("previousClose") or ft_info.get("regularMarketPreviousClose")
                if ft_regular and ft_regular > 0:
                    ratio = ft_regular / regular_close
                    implied_price = futures_price / ratio
                    return round(implied_price, 2), True  # True = futures-implied
        except Exception as e:
            print(f"  Futures proxy error for {ticker}: {e}")

    if price:
        return round(float(price), 2), False
    if regular_close:
        return round(float(regular_close), 2), False
    return None, False

def get_benchmarks():
    """Fetch SPY and QQQ YTD returns. Cached 5 min."""
    if _bench_cache["data"] and (time.time() - _bench_cache["ts"]) < 300:
        return _bench_cache["data"]

    result = {}
    if not HAS_YFINANCE:
        return result

    year_start = f"{date.today().year}-01-01"
    for ticker in ("SPY", "QQQ"):
        try:
            tk = yf.Ticker(ticker)
            hist = tk.history(start=year_start, end=date.today().isoformat())
            if hist.empty:
                continue
            open_price = hist.iloc[0]["Close"]
            current_price, is_futures = _get_benchmark_price(ticker)
            if not current_price:
                current_price = hist.iloc[-1]["Close"]
                is_futures = False
            ytd_pct = ((current_price - open_price) / open_price) * 100
            result[ticker.lower()] = {
                "ticker": ticker,
                "ytd_pct": round(ytd_pct, 2),
                "open_price": round(open_price, 2),
                "current_price": round(current_price, 2),
                "futures_implied": is_futures,
            }
        except Exception as e:
            print(f"  Benchmark {ticker} error: {e}")

    # Compute true portfolio YTD % from Jan 1 baseline
    eq_val = sum(e.get("market_value", 0) for e in store["equities"])
    opt_val = sum(o.get("total_value", 0) for o in store["options"])
    cr_val = sum(c.get("market_value", 0) for c in store["crypto"])
    cash_bal = float(store.get("cash_balance", 0) or 0)
    current_nw = eq_val + opt_val + cr_val + cash_bal
    portfolio_ytd = ((current_nw - JAN1_NET_WORTH) / JAN1_NET_WORTH) * 100
    result["portfolio_ytd_pct"] = round(portfolio_ytd, 2)
    result["portfolio_current_nw"] = round(current_nw, 2)
    result["portfolio_jan1_nw"] = JAN1_NET_WORTH

    _bench_cache["data"] = result
    _bench_cache["ts"] = time.time()
    return result


# ── News Sentiment History ────────────────────────────────────────────
def get_sentiment_history():
    """Get daily sentiment ratios for the past 30 days."""
    history = store.get("news_sentiment_history", [])
    return history[-30:]

def save_daily_sentiment(articles):
    """Save today's sentiment counts. Called on first news fetch of the day."""
    today = date.today().isoformat()
    history = store.get("news_sentiment_history", [])

    # Check if today already saved
    if history and history[-1].get("date") == today:
        return

    portfolio = [a for a in articles if not a.get("is_macro")]
    b = sum(1 for a in portfolio if a.get("sentiment") == "bullish")
    bear = sum(1 for a in portfolio if a.get("sentiment") == "bearish")
    n = sum(1 for a in portfolio if a.get("sentiment") == "neutral")
    total = b + bear + n
    ratio = round(b / (b + bear), 2) if (b + bear) > 0 else 0.5

    history.append({
        "date": today,
        "bullish": b,
        "bearish": bear,
        "neutral": n,
        "total": total,
        "ratio": ratio,
    })

    # Keep 30 days
    store["news_sentiment_history"] = history[-30:]
    save_store()


# ── News Summary (What Moved Today) ──────────────────────────────────
_news_summary_cache = {"data": None, "ts": 0}

def compute_news_summary():
    """Compute daily movers summary for the news tab. Cached 5 min."""
    if _news_summary_cache["data"] and (time.time() - _news_summary_cache["ts"]) < 300:
        return _news_summary_cache["data"]

    movers = []
    now = datetime.now()

    # Get daily changes for each equity position
    for e in store["equities"]:
        ticker = e.get("ticker", "")
        if ticker in ("OPENZ", "OPENL", "OPENW"):
            continue
        price = e.get("live_price", 0) or 0
        if not price or not HAS_YFINANCE:
            continue
        try:
            t = yf.Ticker(ticker)
            info = t.info
            prev_close = info.get("previousClose") or info.get("regularMarketPreviousClose")
            if prev_close and prev_close > 0:
                change_pct = ((price - prev_close) / prev_close) * 100
                change_dollar = price - prev_close
                movers.append({
                    "ticker": ticker,
                    "price": round(price, 2),
                    "prev_close": round(prev_close, 2),
                    "change_pct": round(change_pct, 2),
                    "change_dollar": round(change_dollar, 2),
                    "direction": "up" if change_pct >= 0 else "down",
                    "market_value": round(e.get("market_value", 0), 2),
                })
        except Exception:
            pass

    # Get SPY for market context
    spy_change = None
    if HAS_YFINANCE:
        try:
            spy = yf.Ticker("SPY")
            info = spy.info
            spy_price = info.get("postMarketPrice") or info.get("preMarketPrice") or info.get("currentPrice") or info.get("regularMarketPrice")
            spy_prev = info.get("previousClose") or info.get("regularMarketPreviousClose")
            if spy_price and spy_prev and spy_prev > 0:
                spy_change = round(((spy_price - spy_prev) / spy_prev) * 100, 2)
        except Exception:
            pass

    # Sort by absolute change
    movers.sort(key=lambda m: abs(m["change_pct"]), reverse=True)

    # Get insider/dark pool alerts
    alerts = []
    if HAS_PROVIDERS:
        try:
            from data_providers import get_insider_provider, get_darkpool_provider
            ip = get_insider_provider()
            dp = get_darkpool_provider()
            insider_tickers = []
            darkpool_tickers = []
            for e in store["equities"]:
                ticker = e.get("ticker", "")
                if ticker in ("OPENZ", "OPENL", "OPENW"):
                    continue
                try:
                    insider_data = ip.get_insider_trades(ticker)
                    if isinstance(insider_data, list):
                        today_str = now.strftime("%Y-%m-%d")
                        recent = [t for t in insider_data if t.get("filing_date", "").startswith(today_str) and "Buy" in str(t.get("transaction_type", ""))]
                        if recent:
                            insider_tickers.append(ticker)
                except Exception:
                    pass
                try:
                    dp_data = dp.get_short_volume(ticker)
                    if isinstance(dp_data, dict) and dp_data.get("history"):
                        latest = dp_data["history"][-1] if dp_data["history"] else {}
                        ratio = latest.get("short_ratio", 0) or 0
                        if ratio > 50:
                            darkpool_tickers.append(ticker)
                except Exception:
                    pass
            if insider_tickers:
                alerts.append(f"{len(insider_tickers)} insider buy{'s' if len(insider_tickers) > 1 else ''} ({', '.join(insider_tickers)})")
            if darkpool_tickers:
                alerts.append(f"High dark pool activity: {', '.join(darkpool_tickers)}")
        except Exception:
            pass

    # Generate summary text
    big_movers = [m for m in movers if abs(m["change_pct"]) > 2]
    if big_movers:
        parts = []
        losers = [m for m in big_movers[:3] if m["direction"] == "down"]
        winners = [m for m in big_movers[:3] if m["direction"] == "up"]
        if losers:
            parts.append(", ".join(f"{m['ticker']} {m['change_pct']:+.1f}%" for m in losers))
        if winners:
            winner_strs = [f"{m['ticker']} {m['change_pct']:+.1f}%" for m in winners]
            buck = ", ".join(winner_strs)
            parts.append(f"{buck} bucking the trend" if losers else buck)
        summary_text = ". ".join(parts) + "."
        if alerts:
            summary_text += " " + ". ".join(alerts) + "."
    elif movers:
        max_move = movers[0]["change_pct"] if movers else 0
        summary_text = f"Quiet day — all positions within ±{abs(max_move):.1f}%."
        if spy_change is not None:
            summary_text += f" SPY {spy_change:+.1f}%."
        if alerts:
            summary_text += " " + ". ".join(alerts) + "."
    else:
        summary_text = "No market data available."

    # Portfolio total daily change
    total_prev = sum(m.get("prev_close", 0) * (next((e.get("quantity", 0) for e in store["equities"] if e["ticker"] == m["ticker"]), 0)) for m in movers)
    total_curr = sum(m.get("price", 0) * (next((e.get("quantity", 0) for e in store["equities"] if e["ticker"] == m["ticker"]), 0)) for m in movers)
    portfolio_change_pct = round(((total_curr - total_prev) / total_prev) * 100, 2) if total_prev > 0 else 0

    result = {
        "summary_text": summary_text,
        "movers": movers[:10],
        "alerts": alerts,
        "spy_change": spy_change,
        "portfolio_change_pct": portfolio_change_pct,
        "as_of": now.isoformat(),
    }

    _news_summary_cache["data"] = result
    _news_summary_cache["ts"] = time.time()
    return result


# ── Historical prices for interval P/L ────────────────────────────────
_hist_cache = {}  # key: "TICKER:period" -> {"price": float, "ts": float}
_HIST_CACHE_MAX = 100  # LRU eviction cap — prevents unbounded memory growth

def _hist_cache_set(key, value):
    """Set a _hist_cache entry with LRU eviction."""
    _hist_cache[key] = value
    if len(_hist_cache) > _HIST_CACHE_MAX:
        # Evict oldest entries by timestamp
        entries = sorted(_hist_cache.items(), key=lambda x: x[1].get("ts", 0))
        for k, _ in entries[:len(_hist_cache) - _HIST_CACHE_MAX]:
            del _hist_cache[k]

def get_historical_prices(tickers, period):
    """Get closing price at start of period for each ticker. Cached 15 min."""
    if not HAS_YFINANCE or not tickers:
        return {}

    period_map = {"1d": "2d", "1w": "5d", "1m": "1mo", "3m": "3mo", "ytd": "ytd", "1y": "1y"}
    yf_period = period_map.get(period)
    if not yf_period:
        return {}

    result = {}
    to_fetch = []
    now = time.time()
    for t in tickers:
        cache_key = f"{t}:{period}"
        cached = _hist_cache.get(cache_key)
        if cached and (now - cached["ts"]) < 900:
            result[t] = cached["price"]
        else:
            to_fetch.append(t)

    if to_fetch:
        try:
            tickers_str = " ".join(to_fetch)
            data = yf.download(tickers_str, period=yf_period, progress=False, threads=True)
            close = data.get("Close", data)
            print(f"  Historical: columns={list(close.columns)}, shape={close.shape}, to_fetch={to_fetch}")
            # yfinance returns multi-level columns even for single tickers
            # Flatten: if close is a DataFrame with ticker sub-columns, extract them
            for t in to_fetch:
                try:
                    if t in close.columns:
                        col = close[t]
                    elif len(to_fetch) == 1 and hasattr(close, 'iloc') and len(close.columns) > 0:
                        col = close.iloc[:, 0]
                    else:
                        col = None
                    if col is not None and len(col.dropna()) > 0:
                        price = float(col.dropna().iloc[0])
                        result[t] = price
                        _hist_cache_set(f"{t}:{period}", {"price": price, "ts": now})
                except Exception:
                    pass
        except Exception as e:
            print(f"  Historical prices error: {e}")

    return result


def recalc_equity_cc_income():
    cc_by_ticker = {}
    for entry in store.get("cc_income", []):
        ticker = (entry.get("ticker") or "").upper()
        if not ticker: continue
        premium = float(entry.get("premium_collected", 0) or 0)
        cost = float(entry.get("cost_to_close", 0) or 0)
        cc_by_ticker[ticker] = cc_by_ticker.get(ticker, 0) + premium - cost
    for e in store.get("equities", []):
        ticker = (e.get("ticker") or "").upper()
        e["cc_income"] = cc_by_ticker.get(ticker, 0)
        invested = float(e.get("total_invested", 0) or 0)
        e["cc_yield"] = e["cc_income"] / invested if invested > 0 else 0


class ThreadedHTTPServer(HTTPServer):
    """HTTPServer with a capped thread pool (max 20 concurrent requests)."""
    _pool = ThreadPoolExecutor(max_workers=20)

    def process_request(self, request, client_address):
        self._pool.submit(self._process_request_thread, request, client_address)

    def _process_request_thread(self, request, client_address):
        try:
            self.finish_request(request, client_address)
        except Exception:
            self.handle_error(request, client_address)
        finally:
            self.shutdown_request(request)


class CommandCenterHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(FRONTEND_DIR), **kwargs)

    def do_OPTIONS(self):
        self.send_response(200)
        self._set_cors_headers()
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)

        if path.startswith("/api/"):
            try:
                self._handle_api_get(path, params)
            except Exception as e:
                print(f"API GET error on {path}: {e}")
                try:
                    self._json_response({"error": str(e)}, 500)
                except Exception:
                    pass
        else:
            # Serve frontend
            if path == "/" or path == "":
                self.path = "/index.html"
            super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        content_length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(content_length)) if content_length > 0 else {}
        self._handle_api_post(path, body)

    def do_PUT(self):
        parsed = urlparse(self.path)
        path = parsed.path
        content_length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(content_length)) if content_length > 0 else {}
        self._handle_api_put(path, body)

    def do_DELETE(self):
        parsed = urlparse(self.path)
        self._handle_api_delete(parsed.path)

    def _set_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _json_response(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self._set_cors_headers()
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())

    def _format_equity(self, e):
        qty = e.get("quantity", 0) or 0
        avg = e.get("avg_cost", 0) or 0
        price = e.get("live_price", 0) or 0
        invested = avg * qty if avg > 0 else (e.get("total_invested", 0) or 0)
        mkt_val = price * qty if price > 0 else (e.get("market_value", 0) or 0)
        gain = mkt_val - invested
        gain_pct = gain / invested if invested > 0 else 0
        return {
            **e,
            "current_price": price,
            "market_value": mkt_val,
            "total_invested": invested,
            "gain": gain,
            "gain_pct": gain_pct,
            "pl_dollar": gain,
            "pl_percent": gain_pct,
        }

    def _format_option(self, o):
        avg = float(o.get("avg_cost", 0) or 0)
        contracts = float(o.get("contracts", 0) or 0)
        invested = avg * contracts * 100 if avg > 0 else (o.get("total_invested", 0) or 0)
        total_val = o.get("total_value", 0) or 0
        is_cc = o.get("cc_premium_collected") is not None
        if is_cc:
            # CC: P/L already computed as premium - cost_to_close
            gain = o.get("pl_dollar", 0) or 0
            premium = o.get("cc_premium_collected", 0) or 0
            gain_pct = gain / premium if premium > 0 else 0
        else:
            gain = total_val - invested
            gain_pct = gain / invested if invested != 0 else 0

        result = {
            **o,
            "name": o.get("position_name", ""),
            "current_price": o.get("live_price_long", 0) or 0,
            "total_invested": round(invested, 2),
            "gain": gain,
            "gain_pct": gain_pct,
            "pl_dollar": gain,
            "pl_percent": gain_pct,
        }

        # Dual P/L for CCs with roll history: all-in P/L + current leg P/L
        if is_cc:
            ticker = o.get("occ_long", "").upper()
            cc_entry = next((c for c in store["cc_income"] if c.get("ticker", "").upper() == ticker and not c.get("date_closed")), None)
            if cc_entry and cc_entry.get("roll_history"):
                roll_hist = cc_entry["roll_history"]
                last_leg = roll_hist[-1]
                # Current leg premium = last leg's premium per contract × contracts × 100
                leg_premium = float(last_leg.get("premium", 0)) * contracts * 100
                # Current mark-to-market cost to close = live short price × contracts × 100
                short_mark = float(o.get("live_price_short", 0) or 0) * contracts * 100
                current_leg_pl = round(leg_premium - short_mark, 2)
                current_leg_pct = round(current_leg_pl / leg_premium, 4) if leg_premium > 0 else 0
                result["current_leg_pl"] = current_leg_pl
                result["current_leg_pct"] = current_leg_pct
                result["all_in_pl"] = gain
                result["all_in_pct"] = gain_pct
                result["roll_count"] = len(roll_hist)
                result["current_leg_strike"] = last_leg.get("strike", "")
                result["current_leg_expiry"] = last_leg.get("expiry", "")

        return result

    def _format_crypto(self, c):
        qty = c.get("quantity", 0) or 0
        avg = c.get("avg_price", 0) or 0
        price = c.get("current_price", 0) or 0
        invested = avg * qty
        mkt_val = price * qty if price > 0 else (c.get("market_value", 0) or 0)
        gain = mkt_val - invested
        gain_pct = gain / invested if invested > 0 else 0
        return {
            **c,
            "market_value": mkt_val,
            "gain": gain,
            "gain_pct": gain_pct,
            "pl_dollar": gain,
            "pl_percent": gain_pct,
        }

    def _format_summary(self):
        # Always compute from raw data — never trust cached summary fields
        equities = [self._format_equity(e) for e in store["equities"]]
        options = [self._format_option(o) for o in store["options"]]
        cryptos = [self._format_crypto(c) for c in store["crypto"]]

        eq_val = sum(e["market_value"] for e in equities)
        eq_inv = sum(e["total_invested"] for e in equities)
        # Options value: non-CC options use total_value. CC options excluded from portfolio value
        # because CC premium is already in cash_balance (received when sold). The live cost-to-close
        # is a liability subtracted below. CC P/L still tracked in total_value for display.
        opt_val = sum(o.get("total_value", 0) or 0 for o in options if o.get("cc_premium_collected") is None)
        # CC liability: what it would cost to close all open CCs right now
        cc_liability = sum(
            float(o.get("cc_cost_to_close", 0) or 0) - float(o.get("cc_closed_roll_costs", 0) or 0)
            for o in options if o.get("cc_premium_collected") is not None
        )
        opt_val -= cc_liability  # Subtract live close cost as liability
        opt_inv = sum(
            0 if o.get("cc_premium_collected") is not None else (o.get("total_invested", 0) or 0)
            for o in options
        )
        cr_val = sum(c["market_value"] for c in cryptos)
        cr_inv = sum(c.get("avg_price", 0) * c.get("quantity", 0) for c in store["crypto"])
        card_val = sum(c.get("est_value", 0) or 0 for c in store["cards"])

        total_inv = eq_inv + opt_inv + cr_inv
        total_val = eq_val + opt_val + cr_val
        total_gain = total_val - total_inv
        total_gain_pct = total_gain / total_inv if total_inv > 0 else 0

        tax_sum = get_tax_summary()

        # Unrealized P/L breakdown by holding period + tax-loss harvesting
        today = date.today()
        one_year_ago = today - timedelta(days=365)
        unrealized_st = 0  # short-term (<1yr)
        unrealized_lt = 0  # long-term (>1yr)
        harvest_candidates = []

        # Build earliest acquisition date per ticker from tax ledger (open positions)
        acq_date_lookup = {}
        for tl in store["tax_ledger"]:
            if not tl.get("date_closed"):  # open position
                ticker = tl.get("ticker", "").split()[0].upper()
                d = tl.get("date_acquired", "")
                if d and (ticker not in acq_date_lookup or d < acq_date_lookup[ticker]):
                    acq_date_lookup[ticker] = d

        for e in equities:
            pl = e.get("pl_dollar", 0) or (e["market_value"] - e.get("total_invested", 0))
            acq = e.get("date_acquired", "") or acq_date_lookup.get(e.get("ticker", "").upper(), "")
            try:
                acq_date = datetime.strptime(acq, "%Y-%m-%d").date() if acq else today
            except (ValueError, TypeError):
                acq_date = today
            if acq_date <= one_year_ago:
                unrealized_lt += pl
            else:
                unrealized_st += pl
            if pl < -50:  # loss > $50
                harvest_candidates.append({
                    "ticker": e.get("ticker", ""),
                    "loss": round(pl, 2),
                    "holding": "Long-Term" if acq_date <= one_year_ago else "Short-Term",
                    "date_acquired": acq,
                })

        for o in options:
            is_cc = o.get("cc_premium_collected") is not None
            if is_cc:
                # CC: total_value = premium - cost_to_close (already is the P/L)
                pl = o.get("total_value", 0) or 0
            else:
                pl = (o.get("total_value", 0) or 0) - (o.get("total_invested", 0) or 0)
            unrealized_st += pl  # options are almost always short-term
            if pl < -50:
                # Find earliest tax ledger entry for this option
                # Match on underlying ticker (first word) since naming varies
                opt_name = o.get("position_name", "")
                opt_underlying = opt_name.split()[0].upper() if opt_name else ""
                opt_acq = ""
                for tl in store["tax_ledger"]:
                    if not tl.get("date_closed"):
                        tl_ticker = (tl.get("ticker", "") or "").upper()
                        tl_underlying = tl_ticker.split()[0] if tl_ticker else ""
                        tl_type = (tl.get("asset_type", "") or "").lower()
                        # Match: same underlying + not an equity entry
                        if tl_underlying == opt_underlying and tl_type != "equities" and tl_type != "equity":
                            d = tl.get("date_acquired", "")
                            if d and (not opt_acq or d < opt_acq):
                                opt_acq = d
                harvest_candidates.append({
                    "ticker": opt_name,
                    "loss": round(pl, 2),
                    "holding": "Short-Term",
                    "date_acquired": opt_acq,
                })

        for c in cryptos:
            pl = c.get("pl_dollar", 0) or (c["market_value"] - c.get("avg_price", 0) * c.get("quantity", 0))
            acq = c.get("date_acquired", "")
            try:
                acq_date = datetime.strptime(acq, "%Y-%m-%d").date() if acq else today
            except (ValueError, TypeError):
                acq_date = today
            if acq_date <= one_year_ago:
                unrealized_lt += pl
            else:
                unrealized_st += pl
            if pl < -50:
                harvest_candidates.append({
                    "ticker": c.get("asset", "CRYPTO"),
                    "loss": round(pl, 2),
                    "holding": "Long-Term" if acq_date <= one_year_ago else "Short-Term",
                    "date_acquired": acq,
                })

        harvest_candidates.sort(key=lambda x: x["loss"])
        realized_ytd = tax_sum["total_realized_pl"]
        tax_rate = 30  # default
        potential_savings = round(abs(sum(h["loss"] for h in harvest_candidates)) * (tax_rate / 100) if harvest_candidates else 0, 2)

        return {
            "total_invested": total_inv,
            "market_value": total_val,
            "total_gain": total_gain,
            "total_gain_pct": total_gain_pct,
            "total_pl_dollar": total_gain,
            "total_pl_percent": total_gain_pct,
            "unrealized_pl": round(unrealized_st + unrealized_lt, 2),
            "unrealized_pl_pct": round((unrealized_st + unrealized_lt) / total_inv * 100, 2) if total_inv > 0 else 0,
            "unrealized_st": round(unrealized_st, 2),
            "unrealized_lt": round(unrealized_lt, 2),
            "harvest_candidates": harvest_candidates[:10],
            "potential_tax_savings": potential_savings,
            "equities_value": eq_val,
            "equities_invested": eq_inv,
            "options_value": opt_val,
            "options_invested": opt_inv,
            "crypto_value": cr_val,
            "crypto_invested": cr_inv,
            "card_value": card_val,
            "cash_balance": round(float(store.get("cash_balance", 0) or 0), 2),
            "net_worth": total_val + float(store.get("cash_balance", 0) or 0),
            "cc_income": sum(e.get("premium_collected", 0) for e in store["cc_income"] if (e.get("date_opened", "") or "").startswith(str(date.today().year))),
            "cc_income_all": sum(e.get("premium_collected", 0) for e in store["cc_income"]),
            "realized_gains": tax_sum["total_realized_pl"],
            "realized_pl": tax_sum["total_realized_pl"],
            "tax_reserve": tax_sum["total_tax_reserve"],
        }

    def _handle_api_get(self, path, params):
        # Portfolio
        if path == "/api/portfolio":
            self._json_response({
                "equities": [self._format_equity(e) for e in store["equities"]],
                "options": [self._format_option(o) for o in store["options"]],
                "crypto": [self._format_crypto(c) for c in store["crypto"]],
                "cc_income": store["cc_income"],
                "summary": self._format_summary(),
                "cards": store["cards"],
            })
        elif path == "/api/portfolio/equities":
            self._json_response([self._format_equity(e) for e in store["equities"]])
        elif path == "/api/portfolio/options":
            self._json_response([self._format_option(o) for o in store["options"]])
        elif path == "/api/portfolio/crypto":
            self._json_response([self._format_crypto(c) for c in store["crypto"]])
        elif path == "/api/portfolio/summary":
            self._json_response(self._format_summary())

        elif path == "/api/cash-balance":
            self._json_response({"cash_balance": round(float(store.get("cash_balance", 0) or 0), 2)})

        elif path.startswith("/api/chart/"):
            ticker = path.split("/")[-1].upper()
            period = params.get("period", ["1mo"])[0]
            interval = params.get("interval", ["1d"])[0]
            if not HAS_YFINANCE:
                self._json_response({"error": "yfinance not available"}, 500)
                return
            cache_key = f"chart:{ticker}:{period}:{interval}"
            now = time.time()
            if cache_key in _hist_cache and (now - _hist_cache[cache_key].get("ts", 0)) < 300:
                self._json_response(_hist_cache[cache_key]["data"])
                return
            try:
                t = yf.Ticker(ticker)
                hist = t.history(period=period, interval=interval)
                if hist.empty:
                    self._json_response([])
                    return
                data_points = []
                for idx, row in hist.iterrows():
                    d = str(idx)
                    if "T" in d:
                        d = d[:16].replace("T", " ")
                    else:
                        d = d[:10]
                    data_points.append({
                        "date": d,
                        "open": round(float(row.get("Open", 0)), 2),
                        "high": round(float(row.get("High", 0)), 2),
                        "low": round(float(row.get("Low", 0)), 2),
                        "close": round(float(row.get("Close", 0)), 2),
                        "volume": int(row.get("Volume", 0)),
                    })
                _hist_cache_set(cache_key, {"data": data_points, "ts": now})
                self._json_response(data_points)
            except Exception as e:
                print(f"Chart error for {ticker}: {e}")
                self._json_response({"error": str(e)}, 500)

        elif path == "/api/options/mark-history":
            self._json_response(store.get("option_mark_history", {}))

        # Check if equity exists
        elif path.startswith("/api/portfolio/equities/check/"):
            ticker = path.split("/")[-1].upper()
            existing = next((e for e in store["equities"] if e["ticker"].upper() == ticker), None)
            self._json_response({"exists": existing is not None, "position": existing})

        # Prices
        elif path.startswith("/api/prices/refresh"):
            # Clear benchmark cache so next fetch gets fresh prices
            _bench_cache["data"] = None
            _bench_cache["ts"] = 0
            results = refresh_all_prices()
            self._json_response({"refreshed": len(results), "prices": results, "timestamp": datetime.now().isoformat()})
        elif path == "/api/prices/historical":
            tickers = params.get("tickers", [""])[0].split(",")
            period = params.get("period", ["1m"])[0]
            tickers = [t.strip().upper() for t in tickers if t.strip()]
            self._json_response(get_historical_prices(tickers, period))
        elif path.startswith("/api/prices/"):
            ticker = path.split("/")[-1]
            price = get_live_price(ticker)
            self._json_response({"ticker": ticker, "price": price})

        # Tax Ledger
        elif path == "/api/tax-ledger":
            self._json_response(store["tax_ledger"])
        elif path == "/api/tax-ledger/summary":
            self._json_response(get_tax_summary())

        # CC Income
        elif path == "/api/cc-income":
            # Enrich with current leg P/L for positions with roll history
            enriched = []
            for cc in store["cc_income"]:
                entry = {**cc}
                if not cc.get("date_closed") and cc.get("roll_history"):
                    ticker = cc.get("ticker", "").upper()
                    opt = next((o for o in store["options"] if o.get("occ_long", "").upper() == ticker and o.get("cc_premium_collected") is not None), None)
                    if opt:
                        contracts = float(opt.get("contracts", 0) or 0)
                        last_leg = cc["roll_history"][-1]
                        leg_premium = float(last_leg.get("premium", 0)) * contracts * 100
                        short_mark = float(opt.get("live_price_short", 0) or 0) * contracts * 100
                        entry["current_leg_pl"] = round(leg_premium - short_mark, 2)
                        entry["current_leg_pct"] = round(entry["current_leg_pl"] / leg_premium, 4) if leg_premium > 0 else 0
                        entry["current_leg_strike"] = last_leg.get("strike", "")
                        entry["current_leg_expiry"] = last_leg.get("expiry", "")
                enriched.append(entry)
            self._json_response(enriched)

        # Insider (legacy endpoints)
        elif path == "/api/insider/portfolio":
            results = scrape_insider_portfolio()
            self._json_response(results)
        elif path.startswith("/api/insider/"):
            ticker = path.split("/")[-1]
            self._json_response(scrape_insider(ticker))

        # Dark Pool
        elif path == "/api/darkpool/portfolio":
            if not HAS_PROVIDERS:
                self._json_response({"error": "data_providers not available"}, 500)
                return
            try:
                dp = get_darkpool_provider()
                tickers = list(set(
                    [e["ticker"] for e in store["equities"]] +
                    [c.get("asset", "") for c in store["crypto"] if c.get("asset")]
                ))
                tickers = [t for t in tickers if t and t not in ("OPENZ", "OPENL", "OPENW")]
                results = dp.get_portfolio_latest(tickers)
                self._json_response(results)
            except Exception as e:
                print(f"Dark pool portfolio error: {e}")
                self._json_response({"error": str(e)}, 500)
        elif path == "/api/darkpool/refresh":
            if not HAS_PROVIDERS:
                self._json_response({"error": "data_providers not available"}, 500)
                return
            try:
                dp = get_darkpool_provider()
                fetched = dp.refresh(30)
                self._json_response({"refreshed_days": fetched})
            except Exception as e:
                self._json_response({"error": str(e)}, 500)
        elif path.startswith("/api/darkpool/"):
            if not HAS_PROVIDERS:
                self._json_response({"error": "data_providers not available"}, 500)
                return
            try:
                ticker = path.split("/")[-1].upper()
                dp = get_darkpool_provider()
                data = dp.get_daily_data(ticker, days=30)
                latest = data[-1] if data else None
                avg_ratio = round(sum(d["short_ratio"] for d in data) / len(data), 2) if data else 0
                max_ratio = max((d["short_ratio"] for d in data), default=0)
                min_ratio = min((d["short_ratio"] for d in data), default=0)
                self._json_response({
                    "ticker": ticker,
                    "daily": data,
                    "latest": latest,
                    "stats": {
                        "avg_ratio": avg_ratio,
                        "max_ratio": max_ratio,
                        "min_ratio": min_ratio,
                        "days": len(data),
                    }
                })
            except Exception as e:
                print(f"Dark pool ticker error: {e}")
                self._json_response({"error": str(e)}, 500)

        # Insiders (new provider-backed endpoints)
        elif path == "/api/insiders/portfolio":
            if not HAS_PROVIDERS:
                self._json_response({"error": "data_providers not available"}, 500)
                return
            if _insider_portfolio_cache["data"] and (time.time() - _insider_portfolio_cache["ts"]) < INSIDER_PORTFOLIO_TTL:
                self._json_response(_insider_portfolio_cache["data"])
                return
            try:
                ip = get_insider_provider()
                tickers = list(set(e["ticker"] for e in store["equities"] if e["ticker"] not in ("OPENZ", "OPENL", "OPENW")))
                results = ip.get_bulk_recent(tickers, days=30)
                _insider_portfolio_cache["data"] = results
                _insider_portfolio_cache["ts"] = time.time()
                self._json_response(results)
            except Exception as e:
                print(f"Insiders portfolio error: {e}")
                self._json_response({"error": str(e)}, 500)
        elif path.startswith("/api/insiders/"):
            if not HAS_PROVIDERS:
                self._json_response({"error": "data_providers not available"}, 500)
                return
            try:
                ticker = path.split("/")[-1].upper()
                ip = get_insider_provider()
                trades = ip.get_recent_trades(ticker, days=90)
                self._json_response({"ticker": ticker, "trades": trades})
            except Exception as e:
                print(f"Insiders ticker error: {e}")
                self._json_response({"error": str(e)}, 500)

        # Options Flow
        elif path == "/api/options-flow/portfolio":
            if not HAS_PROVIDERS:
                self._json_response({"error": "data_providers not available"}, 500)
                return
            try:
                ofp = get_options_flow_provider()
                tickers = list(set(e["ticker"] for e in store["equities"] if e["ticker"] not in ("OPENZ", "OPENL", "OPENW")))
                results = ofp.get_portfolio_summary(tickers)
                self._json_response(results)
            except Exception as e:
                print(f"Options flow portfolio error: {e}")
                self._json_response({"error": str(e)}, 500)
        elif path.startswith("/api/options-flow/"):
            if not HAS_PROVIDERS:
                self._json_response({"error": "data_providers not available"}, 500)
                return
            try:
                parts = path.split("/")
                ticker = parts[3].upper()
                # Check for ?exp= query param
                exp = params.get("exp", [None])[0]
                ofp = get_options_flow_provider()
                result = ofp.get_flow(ticker, expiration=exp)
                self._json_response(result)
            except Exception as e:
                print(f"Options flow ticker error: {e}")
                self._json_response({"error": str(e)}, 500)

        # Wheel Strategy
        elif path.startswith("/api/wheel/expirations/"):
            ticker = path.split("/")[-1].upper()
            if HAS_PROVIDERS:
                wp = get_wheel_provider()
                self._json_response({"ticker": ticker, "expirations": wp.get_expirations(ticker)})
            else:
                self._json_response({"error": "providers not available"}, 500)

        elif path.startswith("/api/wheel/analyze/"):
            ticker = path.split("/")[-1].upper()
            if not HAS_PROVIDERS:
                self._json_response({"error": "providers not available"}, 500)
                return
            exps_param = params.get("expirations", [""])[0]
            exps = [e.strip() for e in exps_param.split(",") if e.strip()] if exps_param else []

            wp = get_wheel_provider()
            if not exps:
                all_exps = wp.get_expirations(ticker)
                exps = all_exps[:4]  # Default: next 4

            quote = wp.get_quote(ticker)
            price = quote.get("price", 0)
            hv = wp.get_hv_30d(ticker)
            earnings = wp.get_earnings_date(ticker)

            # Portfolio context
            eq = next((e for e in store["equities"] if e["ticker"].upper() == ticker), None)
            shares_owned = eq["quantity"] if eq else 0
            avg_cost = eq.get("avg_cost", 0) if eq else 0
            cash_avail = float(store.get("cash_balance", 0) or 0)
            eq_val = sum(e.get("market_value", 0) for e in store["equities"])
            opt_val = sum(o.get("total_value", 0) for o in store["options"])
            cr_val = sum(c.get("market_value", 0) for c in store["crypto"])
            net_worth = eq_val + opt_val + cr_val + cash_avail

            # Dark pool + insider signals
            dp_signal = None
            insider_signal = None
            try:
                dp = get_darkpool_provider()
                dp_latest = dp.get_latest(ticker)
                if dp_latest:
                    dp_signal = {"short_ratio": dp_latest.get("short_ratio", 0), "high": dp_latest.get("short_ratio", 0) > 50}
            except Exception:
                pass
            try:
                ip = get_insider_provider()
                trades = ip.get_recent_trades(ticker, days=90)
                if trades:
                    buys = sum(1 for t in trades if t.get("is_purchase"))
                    sells = len(trades) - buys
                    insider_signal = {"recent_trades": len(trades), "buys": buys, "sells": sells}
            except Exception:
                pass

            # Build chain analysis per expiration
            chains = []
            atm_iv = None
            for exp in exps:
                chain = wp.get_chain(ticker, exp)
                dte = max(1, (datetime.strptime(exp, "%Y-%m-%d").date() - date.today()).days)

                for side in ("puts", "calls"):
                    for opt in chain.get(side, []):
                        strike = opt["strike"]
                        mark = opt["mark"]
                        if side == "puts":
                            capital = strike * 100
                            breakeven = round(strike - mark, 2)
                            cost_basis_assigned = breakeven
                            pct_otm = round((price - strike) / price * 100, 2) if price > 0 else 0
                        else:
                            capital = (avg_cost if shares_owned > 0 else price) * 100
                            breakeven = round(strike + mark, 2)
                            cost_basis_assigned = None
                            pct_otm = round((strike - price) / price * 100, 2) if price > 0 else 0

                        ann_yield = round((mark / (capital / 100)) * (365 / dte) * 100, 2) if capital > 0 else 0
                        premium_per_contract = round(mark * 100, 2)

                        # Quick returns grid
                        quick_returns = []
                        for target_pct in (25, 50, 65, 100):
                            close_at = round(mark * (1 - target_pct / 100), 2)
                            net_profit = round((mark - close_at) * 100, 2)
                            est_days = max(1, int(dte * target_pct / 100 * 0.85)) if target_pct < 100 else dte
                            ann_ret = round((net_profit / capital) * (365 / est_days) * 100, 2) if capital > 0 else 0
                            quick_returns.append({
                                "target_pct": target_pct,
                                "cost_to_close": close_at,
                                "net_profit": net_profit,
                                "est_days": est_days,
                                "ann_return": ann_ret,
                            })

                        # Portfolio context per strike
                        cash_pct = round(capital / cash_avail * 100, 2) if cash_avail > 0 else 0
                        position_val = (shares_owned + 100) * price if eq else 100 * price
                        portfolio_pct_if_assigned = round(position_val / net_worth * 100, 2) if net_worth > 0 else 0

                        # Liquidity score
                        spread_ok = opt["bid_ask_spread"] < 0.10
                        oi_ok = opt["open_interest"] > 1000
                        vol_ok = opt["volume"] > 500
                        score_bits = sum([spread_ok, oi_ok, vol_ok])
                        liq_score = {3: "A", 2: "B", 1: "C", 0: "D"}[score_bits]

                        opt["ann_yield"] = ann_yield
                        opt["premium_per_contract"] = premium_per_contract
                        opt["capital_required"] = round(capital, 2)
                        opt["breakeven"] = breakeven
                        opt["cost_basis_if_assigned"] = cost_basis_assigned
                        opt["pct_otm"] = pct_otm
                        opt["quick_returns"] = quick_returns
                        opt["cash_pct_used"] = cash_pct
                        opt["portfolio_pct_if_assigned"] = portfolio_pct_if_assigned
                        opt["liquidity_score"] = liq_score
                        opt["dte"] = dte
                        opt["side"] = side[:-1]  # "put" or "call"

                        # Track ATM IV
                        if side == "puts" and abs(strike - price) < price * 0.02 and opt["iv"] > 0:
                            atm_iv = opt["iv"]

                chains.append({"expiration": exp, "dte": dte, "puts": chain.get("puts", []), "calls": chain.get("calls", []), "source": chain.get("source", "")})

            # Cache ATM IV
            if atm_iv:
                wp._cache_iv(ticker, atm_iv)

            iv_rank, iv_days = wp.get_iv_rank(ticker)
            iv_hv_ratio = round(atm_iv / hv, 2) if atm_iv and hv and hv > 0 else None
            iv_hv_label = "RICH" if iv_hv_ratio and iv_hv_ratio > 1.2 else ("CHEAP" if iv_hv_ratio and iv_hv_ratio < 0.8 else "FAIR") if iv_hv_ratio else None

            self._json_response({
                "ticker": ticker,
                "price": price,
                "name": quote.get("name", ticker),
                "chains": chains,
                "scorecard": {
                    "iv_current": atm_iv,
                    "hv_30d": hv,
                    "iv_hv_ratio": iv_hv_ratio,
                    "iv_hv_label": iv_hv_label,
                    "iv_rank": iv_rank,
                    "iv_rank_days": iv_days,
                    "earnings_date": earnings,
                    "dark_pool": dp_signal,
                    "insider": insider_signal,
                },
                "portfolio": {
                    "shares_owned": shares_owned,
                    "avg_cost": avg_cost,
                    "cash_available": round(cash_avail, 2),
                    "net_worth": round(net_worth, 2),
                    "suggest_mode": "cc" if shares_owned > 0 else "csp",
                },
            })

        # Active Wheels — exit intelligence
        elif path == "/api/wheel/active":
            wheels = []
            today_d = date.today()
            for opt in store["options"]:
                occ_short = opt.get("occ_short", "")
                occ_long = opt.get("occ_long", "")
                if not occ_short:
                    continue
                parsed = parse_occ_symbol(occ_short)
                if not parsed:
                    continue
                ticker_u = parsed["underlying"]
                opt_type = parsed["type"]  # "call" or "put"

                # CC: short call + long is equity ticker (not OCC)
                is_cc = opt_type == "call" and occ_long and not re.match(r'^[A-Z]+\d{6}[CP]\d{8}$', occ_long)
                # CSP: short put + no long leg
                is_csp = opt_type == "put" and (not occ_long or occ_long.upper() == "CASH")
                if not is_cc and not is_csp:
                    continue

                wheel_type = "cc" if is_cc else "csp"
                strike = parsed["strike"]
                expiration = parsed["expiration"]
                dte = max(0, (datetime.strptime(expiration, "%Y-%m-%d").date() - today_d).days)
                contracts = float(opt.get("contracts", 0) or 0)

                # Underlying price + equity info
                eq = next((e for e in store["equities"] if e["ticker"].upper() == ticker_u.upper()), None)
                underlying_price = eq["live_price"] if eq else 0
                shares_owned = eq["quantity"] if eq else 0
                avg_cost = eq.get("avg_cost", 0) if eq else 0

                # Premium from CC Income, fallback to options data
                cc_entry = next((c for c in store["cc_income"] if c.get("ticker", "").upper() == ticker_u.upper() and not c.get("date_closed")), None)
                if cc_entry:
                    premium_collected = float(cc_entry.get("premium_collected", 0) or 0)
                    cost_to_close_existing = float(cc_entry.get("cost_to_close", 0) or 0)
                    date_opened = cc_entry.get("date_opened", "")
                    roll_history = cc_entry.get("roll_history", [])
                else:
                    premium_collected = float(opt.get("total_invested", 0) or 0)
                    cost_to_close_existing = 0
                    date_opened = opt.get("date_acquired", "")
                    roll_history = []

                # Current mark and P/L
                current_mark = float(opt.get("live_price_short", 0) or 0)
                cost_to_close = round(current_mark * contracts * 100, 2)
                # For CCs: P/L = premium - closed roll costs - live cost to close
                unrealized_pl = round(premium_collected - cost_to_close_existing - cost_to_close, 2)
                pl_pct = round(unrealized_pl / premium_collected * 100, 2) if premium_collected > 0 else 0

                # Dual P/L for rolled positions
                current_leg_pl = None
                if roll_history:
                    last_leg = roll_history[-1]
                    leg_prem = float(last_leg.get("premium", 0)) * contracts * 100
                    leg_ctc = current_mark * contracts * 100
                    current_leg_pl = round(leg_prem - leg_ctc, 2)

                # OTM %
                if wheel_type == "cc":
                    pct_otm = round((strike - underlying_price) / underlying_price * 100, 2) if underlying_price > 0 else 0
                else:
                    pct_otm = round((underlying_price - strike) / underlying_price * 100, 2) if underlying_price > 0 else 0
                moneyness = "ITM" if pct_otm < 0 else ("ATM" if abs(pct_otm) < 0.5 else "OTM")

                # Days held
                days_held = (today_d - datetime.strptime(date_opened, "%Y-%m-%d").date()).days if date_opened else 0

                # === SIGNAL ENGINE ===
                all_signals = []
                profit_pct = pl_pct

                # CLOSE signals
                if profit_pct >= 50:
                    pri = "HIGH" if profit_pct >= 65 else "MEDIUM"
                    all_signals.append({"type": "CLOSE", "priority": pri, "reason": f"{profit_pct:.0f}% profit captured — {'at/above target' if profit_pct >= 65 else 'approaching target'}"})
                if date_opened and profit_pct >= 50:
                    try:
                        orig_dte = (datetime.strptime(expiration, "%Y-%m-%d").date() - datetime.strptime(date_opened, "%Y-%m-%d").date()).days
                        if orig_dte > 0 and days_held < orig_dte * 0.33:
                            all_signals.append({"type": "CLOSE", "priority": "HIGH", "reason": f"Fast profit: {profit_pct:.0f}% in {days_held}d ({days_held/orig_dte*100:.0f}% of DTE) — take it and redeploy"})
                    except Exception:
                        pass
                remaining_per_contract = current_mark * 100
                if remaining_per_contract < 10 and dte > 5:
                    all_signals.append({"type": "CLOSE", "priority": "LOW", "reason": f"Only ${remaining_per_contract:.0f}/contract remaining — not worth risk for {dte}d"})

                # ROLL signals
                if wheel_type == "cc" and pct_otm < 2 and dte < 14 and underlying_price > 0:
                    all_signals.append({"type": "ROLL", "priority": "HIGH", "reason": f"Stock ${underlying_price:.2f} within {pct_otm:.1f}% of ${strike} strike with {dte}d — roll up/out to avoid assignment"})
                if wheel_type == "csp" and pct_otm < 3 and dte < 14 and underlying_price > 0:
                    all_signals.append({"type": "ROLL", "priority": "HIGH", "reason": f"Stock ${underlying_price:.2f} within {pct_otm:.1f}% of ${strike} put — roll down/out"})
                if dte > 21 and profit_pct < 25:
                    all_signals.append({"type": "ROLL", "priority": "LOW", "reason": f"{dte} DTE with only {profit_pct:.0f}% captured — consider rolling to shorter DTE"})
                if dte <= 5 and pct_otm < 0:
                    all_signals.append({"type": "ROLL", "priority": "HIGH", "reason": f"ITM with {dte}d to expiry — roll now or accept assignment"})

                # HOLD signals
                if pct_otm > 5 and dte > 7:
                    all_signals.append({"type": "HOLD", "priority": "MEDIUM", "reason": f"Comfortably OTM ({pct_otm:.1f}%) with {dte}d — theta working"})
                if 25 <= profit_pct < 50:
                    all_signals.append({"type": "HOLD", "priority": "LOW", "reason": f"{profit_pct:.0f}% profit building — approaching 50% target"})

                # Earnings guard
                earnings_warning = None
                try:
                    wp = get_wheel_provider()
                    earnings = wp.get_earnings_date(ticker_u)
                    if earnings and earnings <= expiration:
                        earnings_warning = {"type": "WARNING", "priority": "HIGH", "reason": f"Earnings {earnings} falls before {expiration} expiry — high IV risk"}
                except Exception:
                    pass

                # Resolve primary signal
                high_close = [s for s in all_signals if s["type"] == "CLOSE" and s["priority"] == "HIGH"]
                high_roll = [s for s in all_signals if s["type"] == "ROLL" and s["priority"] == "HIGH"]
                if high_close:
                    primary = high_close[0]
                elif high_roll:
                    primary = high_roll[0]
                elif all_signals:
                    primary = all_signals[0]
                else:
                    primary = {"type": "HOLD", "priority": "LOW", "reason": "Position healthy, no action needed"}

                alternatives = [s for s in all_signals if s != primary]

                # Action text
                if primary["type"] == "CLOSE":
                    action_text = f"Buy to close {int(contracts)}x {ticker_u} ${strike}{opt_type[0].upper()} {expiration} @ ~${current_mark:.2f} (${cost_to_close:.0f}). Lock in ${unrealized_pl:.0f} profit."
                elif primary["type"] == "ROLL":
                    action_text = f"Close current {ticker_u} ${strike}{opt_type[0].upper()} and roll to next month at higher strike."
                else:
                    action_text = f"Hold {int(contracts)}x {ticker_u} ${strike}{opt_type[0].upper()} — let theta work."

                # Breakeven
                if wheel_type == "cc":
                    breakeven = round(avg_cost - (premium_collected / (shares_owned or 100)), 2) if shares_owned else 0
                else:
                    breakeven = round(strike - premium_collected / (contracts * 100), 2) if contracts else 0

                wheels.append({
                    "id": opt.get("id"),
                    "ticker": ticker_u,
                    "type": wheel_type,
                    "position_name": opt.get("position_name", ""),
                    "contracts": contracts,
                    "strike": strike,
                    "expiration": expiration,
                    "dte": dte,
                    "underlying_price": underlying_price,
                    "shares_owned": shares_owned,
                    "avg_cost": avg_cost,
                    "premium_collected": premium_collected,
                    "current_mark": current_mark,
                    "cost_to_close": cost_to_close,
                    "unrealized_pl": unrealized_pl,
                    "pl_pct_of_premium": pl_pct,
                    "current_leg_pl": current_leg_pl,
                    "all_in_pl": unrealized_pl,
                    "signals": {
                        "primary": primary,
                        "alternatives": alternatives[:3],
                        "action": action_text,
                        "earnings_warning": earnings_warning,
                    },
                    "risk_metrics": {
                        "moneyness": moneyness,
                        "pct_otm": pct_otm,
                        "breakeven": breakeven,
                        "max_profit": premium_collected,
                    },
                    "context": {
                        "rolls_count": len(roll_history),
                        "roll_history": roll_history,
                        "original_open_date": date_opened,
                        "days_held": days_held,
                    },
                })

            # Summary
            close_ct = sum(1 for w in wheels if w["signals"]["primary"]["type"] == "CLOSE")
            roll_ct = sum(1 for w in wheels if w["signals"]["primary"]["type"] == "ROLL")
            hold_ct = sum(1 for w in wheels if w["signals"]["primary"]["type"] == "HOLD")
            self._json_response({
                "active_wheels": wheels,
                "summary": {
                    "total_active_wheels": len(wheels),
                    "total_premium_at_risk": round(sum(w["premium_collected"] for w in wheels), 2),
                    "total_unrealized_pl": round(sum(w["unrealized_pl"] for w in wheels), 2),
                    "close_signals": close_ct,
                    "roll_signals": roll_ct,
                    "hold_signals": hold_ct,
                },
            })

        # Wheel Scenario Simulator
        elif path.startswith("/api/wheel/simulate/"):
            ticker = path.split("/")[-1].upper()
            strike_p = params.get("strike", ["0"])[0]
            strike = float(strike_p) if strike_p else 0
            opt_type = params.get("type", ["csp"])[0]
            cadence = params.get("expiration_cadence", ["monthly"])[0]
            months = int(params.get("months", ["6"])[0])
            close_at = int(params.get("close_at", ["50"])[0])
            iv_param = params.get("iv_assumption", ["current"])[0]

            if strike <= 0:
                self._json_response({"error": "strike required"}, 400)
                return

            wp = get_wheel_provider() if HAS_PROVIDERS else None
            quote = wp.get_quote(ticker) if wp else {"price": 0}
            price = quote.get("price", 0)
            if not price:
                self._json_response({"error": f"Cannot get price for {ticker}"}, 400)
                return

            hv = wp.get_hv_30d(ticker) if wp else None
            atm_iv = None
            try:
                exps = wp.get_expirations(ticker) if wp else []
                if exps:
                    chain = wp.get_chain(ticker, exps[0])
                    for o in chain.get("puts" if opt_type == "csp" else "calls", []):
                        if abs(o["strike"] - price) < price * 0.03 and o.get("iv", 0) > 0:
                            atm_iv = o["iv"]
                            break
            except Exception:
                pass

            if iv_param == "mean" and atm_iv and hv:
                iv = (atm_iv + hv) / 2
            elif iv_param not in ("current", "mean"):
                try:
                    iv = float(iv_param)
                except ValueError:
                    iv = atm_iv or hv or 0.40
            else:
                iv = atm_iv or hv or 0.40
            iv_label = "current IV" if atm_iv else ("HV proxy" if hv else "default 40%")

            dte_map = {"weekly": 7, "biweekly": 14, "monthly": 30}
            cycle_dte = dte_map.get(cadence, 30)
            total_cycles = max(1, int(months * 30 / cycle_dte))

            from math import sqrt, exp, log
            from statistics import NormalDist
            nd = NormalDist()

            def bsm_premium(s, k, t_days, vol, otype):
                t = max(t_days / 365, 0.001)
                r = 0.05
                d1 = (log(s / k) + (r + vol**2 / 2) * t) / (vol * sqrt(t))
                d2 = d1 - vol * sqrt(t)
                if otype == "csp":
                    return max(k * exp(-r * t) * nd.cdf(-d2) - s * nd.cdf(-d1), 0.01)
                else:
                    return max(s * nd.cdf(d1) - k * exp(-r * t) * nd.cdf(d2), 0.01)

            est_premium = round(bsm_premium(price, strike, cycle_dte, iv, opt_type), 2)
            premium_at_target = round(est_premium * close_at / 100, 2)
            est_days_to_close = max(1, int(cycle_dte * close_at / 100 * 0.85)) if close_at < 100 else cycle_dte

            t_y = max(cycle_dte / 365, 0.001)
            d1 = (log(price / strike) + (0.05 + iv**2 / 2) * t_y) / (iv * sqrt(t_y))
            prob_assigned = round(nd.cdf(-d1) if opt_type == "csp" else nd.cdf(d1), 2)
            prob_otm = round(1 - prob_assigned, 2)

            capital = strike * 100 if opt_type == "csp" else price * 100

            cycles = []
            cum_income = 0
            entry = date.today()
            for i in range(total_cycles):
                exp_date = entry + timedelta(days=cycle_dte)
                income_this = round(premium_at_target * 100, 2)
                cum_income += income_this
                ann_yield = round(income_this / capital * (365 / est_days_to_close) * 100, 2) if capital > 0 else 0
                cycles.append({
                    "cycle": i + 1,
                    "entry_date": entry.isoformat(),
                    "expiration": exp_date.isoformat(),
                    "dte": cycle_dte,
                    "estimated_premium": est_premium,
                    "premium_at_close_target": premium_at_target,
                    "est_days_to_close": est_days_to_close,
                    "prob_otm": prob_otm,
                    "prob_assigned": prob_assigned,
                    "income_if_otm": income_this,
                    "cumulative_income": round(cum_income, 2),
                    "annualized_yield": ann_yield,
                })
                entry = exp_date + timedelta(days=1)

            best_income = round(premium_at_target * 100 * total_cycles, 2)
            expected_income = round(best_income * prob_otm, 2)
            best_yield = round(best_income / capital * (12 / max(months, 1)) * 100, 2) if capital > 0 else 0
            expected_yield = round(expected_income / capital * (12 / max(months, 1)) * 100, 2) if capital > 0 else 0

            worst_price = round(price * 0.80, 2)
            cb = round(strike - est_premium, 2) if opt_type == "csp" else round(price, 2)
            worst_loss = round((worst_price - cb) * 100, 2)
            breakeven_price = round(strike - est_premium * total_cycles, 2) if opt_type == "csp" else round(price - est_premium * total_cycles, 2)
            cc_prem_worst = round(bsm_premium(worst_price, strike, cycle_dte, iv, "cc"), 2)
            recovery_cycles = int(abs(worst_loss) / max(cc_prem_worst * 100, 1)) + 1 if worst_loss < 0 else 0

            scenarios = []
            for pct, label in [(0.05, "+5%"), (0, "Flat"), (-0.10, "-10%"), (-0.20, "-20%"), (-0.30, "-30%")]:
                sp = round(price * (1 + pct), 2)
                s_cb = round(strike - est_premium, 2) if opt_type == "csp" else round(price, 2)
                unr = round((sp - s_cb) * 100, 2)
                rec = int(abs(unr) / max(bsm_premium(sp, strike, cycle_dte, iv, "cc") * 100, 1)) + 1 if unr < 0 else 0
                scenarios.append({"label": label, "stock_price": sp, "cost_basis": s_cb, "unrealized_pl": unr, "recovery_cycles": rec})

            compound = {}
            for h in (3, 6, 12):
                c = max(1, int(h * 30 / cycle_dte))
                bi = round(premium_at_target * 100 * c, 2)
                ei = round(bi * prob_otm, 2)
                compound[f"{h}_month"] = {"cycles": c, "best_income": bi, "expected_income": ei, "yield_on_capital": round(ei / capital * 100, 1) if capital > 0 else 0}

            eq = next((e for e in store["equities"] if e["ticker"].upper() == ticker), None)
            cash = float(store.get("cash_balance", 0) or 0)

            self._json_response({
                "ticker": ticker, "current_price": price, "strike": strike, "type": opt_type,
                "cadence": cadence, "months": months, "close_target_pct": close_at,
                "iv_used": round(iv, 4), "iv_label": iv_label,
                "cycles": cycles,
                "projection_summary": {
                    "total_cycles": total_cycles,
                    "best_case": {"total_income": best_income, "annualized_yield": best_yield, "capital_tied": round(capital, 2)},
                    "expected_case": {"expected_income": expected_income, "expected_assignments": round(total_cycles * prob_assigned, 1), "expected_yield": expected_yield},
                    "worst_case": {"stock_price": worst_price, "net_pnl": worst_loss, "breakeven_price": breakeven_price, "cycles_to_recover": recovery_cycles},
                },
                "assignment_scenarios": scenarios,
                "compound_projection": compound,
                "portfolio_context": {
                    "shares_owned": eq["quantity"] if eq else 0,
                    "avg_cost": eq.get("avg_cost", 0) if eq else 0,
                    "cash_available": round(cash, 2),
                    "capital_per_contract": round(capital, 2),
                    "max_contracts": int(cash / capital) if capital > 0 else 0,
                },
            })

        # Cards
        elif path == "/api/cards":
            self._json_response(store["cards"])
        elif path == "/api/cards/valuation":
            if HAS_PROVIDERS:
                try:
                    cvp = get_card_valuation_provider()
                    if cvp.available:
                        results = cvp.get_bulk_valuation(store["cards"])
                        self._json_response(results)
                        return
                except Exception as e:
                    print(f"Card valuation error: {e}")
            # Fallback to legacy scraper
            results = []
            for card in store["cards"]:
                val = search_ebay_card(card)
                results.append(val)
                time.sleep(1)
            self._json_response(results)
        elif path.startswith("/api/cards/valuation/"):
            card_id = int(path.split("/")[-1])
            card = next((c for c in store["cards"] if c["id"] == card_id), None)
            if not card:
                self._json_response({"error": "Card not found"}, 404)
                return
            if HAS_PROVIDERS:
                try:
                    cvp = get_card_valuation_provider()
                    if cvp.available:
                        self._json_response(cvp.get_valuation(card))
                        return
                except Exception as e:
                    print(f"Card valuation error: {e}")
            self._json_response(search_ebay_card(card))

        # Sales Ledger
        elif path == "/api/sales":
            self._json_response(store["sales"])
        elif path == "/api/box-purchases":
            self._json_response(store["box_purchases"])
        elif path == "/api/sales/tax-summary":
            year = params.get("year", [str(date.today().year)])[0]
            sales = [s for s in store["sales"] if s.get("date_sold", "").startswith(year) and s.get("status") == "completed"]
            gross = sum(float(s.get("sale_price", 0) or 0) for s in sales)
            fees = sum(float(s.get("platform_fees", 0) or 0) for s in sales)
            shipping = sum(float(s.get("shipping_cost", 0) or 0) for s in sales)
            boxes = [b for b in store["box_purchases"] if b.get("date", "").startswith(year)]
            cogs = sum(float(b.get("cost", 0) or 0) for b in boxes)
            purchase_costs = sum(float(s.get("purchase_price", 0) or 0) for s in sales if s.get("purchase_price"))
            expenses = sum(float(s.get("business_expense", 0) or 0) for s in store["sales"] if s.get("business_expense") and s.get("date_sold", "").startswith(year))
            by_cat = {}
            for s in sales:
                cat = s.get("category", "other")
                by_cat.setdefault(cat, {"gross": 0, "fees": 0, "count": 0})
                by_cat[cat]["gross"] += float(s.get("sale_price", 0) or 0)
                by_cat[cat]["fees"] += float(s.get("platform_fees", 0) or 0)
                by_cat[cat]["count"] += 1
            by_plat = {}
            for s in sales:
                p = s.get("platform", "other")
                by_plat.setdefault(p, {"gross": 0, "count": 0})
                by_plat[p]["gross"] += float(s.get("sale_price", 0) or 0)
                by_plat[p]["count"] += 1
            self._json_response({
                "year": year,
                "gross_sales": round(gross, 2),
                "total_fees": round(fees, 2),
                "total_shipping": round(shipping, 2),
                "cost_of_goods": round(cogs + purchase_costs, 2),
                "business_expenses": round(expenses, 2),
                "net_taxable": round(gross - fees - shipping - cogs - purchase_costs - expenses, 2),
                "completed_sales": len(sales),
                "by_category": {k: {kk: round(vv, 2) for kk, vv in v.items()} for k, v in by_cat.items()},
                "by_platform": {k: {kk: round(vv, 2) if isinstance(vv, float) else vv for kk, vv in v.items()} for k, v in by_plat.items()},
            })

        # Health
        elif path == "/api/health":
            self._json_response({
                "status": "ok",
                "yfinance": HAS_YFINANCE,
                "scraping": HAS_SCRAPING,
                "providers": HAS_PROVIDERS,
                "equities": len(store["equities"]),
                "options": len(store["options"]),
                "cards": len(store["cards"]),
                "mark_cache_warm": _mark_cache_warm,
                "mark_cache_warm_ts": _mark_cache_warm_ts,
                "snapshot_count": len(load_snapshots()),
                "options_value": round(sum(o.get("total_value", 0) for o in store["options"]), 2),
            })
        # Portfolio history (reconstructed from position history × prices)
        elif path == "/api/portfolio/history":
            period = params.get("period", ["1m"])[0]
            self._json_response(get_portfolio_history(period))

        # Snapshots (legacy — kept for migration/debug)
        elif path == "/api/snapshots":
            snapshots = load_snapshots()
            self._json_response(snapshots)

        # Fear & Greed
        elif path == "/api/fear-greed":
            self._json_response(get_fear_greed())

        # Benchmarks (SPY / QQQ YTD)
        elif path == "/api/benchmarks":
            self._json_response(get_benchmarks())

        # Cash Flow
        elif path == "/api/cash-flow":
            self._json_response(compute_cash_flow())
        elif path == "/api/cash-flows":
            self._json_response(store["cash_flows"])

        # Watchlist
        elif path == "/api/watchlist":
            # Enrich watchlist with live prices
            enriched = []
            for item in store["watchlist"]:
                ticker = item.get("ticker", "").upper()
                price = get_live_price(ticker) if HAS_YFINANCE and ticker else 0
                target = float(item.get("target_buy", 0) or 0)
                distance_pct = round((price - target) / target * 100, 2) if target > 0 and price else 0
                enriched.append({
                    **item,
                    "live_price": round(price, 2) if price else 0,
                    "distance_pct": distance_pct,
                    "at_target": price <= target if price and target > 0 else False,
                })
            self._json_response(enriched)
        elif path == "/api/watchlist/alerts":
            try:
                self._json_response(compute_watchlist_alerts())
            except Exception as e:
                print(f"Watchlist alerts error: {e}")
                self._json_response({"error": str(e)}, 500)

        # ETH ETF Flows
        elif path == "/api/etf-flows":
            if not HAS_PROVIDERS:
                self._json_response({"error": "data_providers not available"}, 500)
                return
            try:
                self._json_response(get_etf_flow_provider().get_daily_flows(days=30))
            except Exception as e:
                print(f"ETF flows error: {e}")
                self._json_response({"error": str(e)}, 500)

        elif path == "/api/etf-flows/cumulative":
            if not HAS_PROVIDERS:
                self._json_response({"error": "data_providers not available"}, 500)
                return
            try:
                period = params.get("period", ["1m"])[0]
                self._json_response(get_etf_flow_provider().get_cumulative(period))
            except Exception as e:
                print(f"ETF cumulative error: {e}")
                self._json_response({"error": str(e)}, 500)

        elif path == "/api/etf-flows/btc-comparison":
            if not HAS_PROVIDERS:
                self._json_response({"error": "data_providers not available"}, 500)
                return
            try:
                period = params.get("period", ["1m"])[0]
                self._json_response(get_etf_flow_provider().get_btc_comparison(period))
            except Exception as e:
                print(f"ETF BTC comparison error: {e}")
                self._json_response({"error": str(e)}, 500)

        # BMNR NAV
        elif path == "/api/bmnr-nav":
            if not HAS_PROVIDERS:
                self._json_response({"error": "data_providers not available"}, 500)
                return
            try:
                nav = get_bmnr_nav_provider().get_nav()
                nav["history"] = get_bmnr_nav_provider().get_nav_history(days=90)
                self._json_response(nav)
            except Exception as e:
                print(f"BMNR NAV error: {e}")
                self._json_response({"error": str(e)}, 500)

        # Staking Overview
        elif path == "/api/staking":
            if not HAS_PROVIDERS:
                self._json_response({"error": "data_providers not available"}, 500)
                return
            try:
                self._json_response(get_staking_provider().get_staking_overview())
            except Exception as e:
                print(f"Staking error: {e}")
                self._json_response({"error": str(e)}, 500)

        # News Sentiment History
        elif path == "/api/news/sentiment-history":
            try:
                history = get_sentiment_history()
                current = history[-1] if history else {"ratio": 0.5}
                # Determine trend from last 3 days
                trend = "stable"
                if len(history) >= 3:
                    recent = [h["ratio"] for h in history[-3:]]
                    if recent[-1] > recent[0] + 0.05:
                        trend = "improving"
                    elif recent[-1] < recent[0] - 0.05:
                        trend = "declining"
                self._json_response({
                    "history": history,
                    "current_ratio": current.get("ratio", 0.5),
                    "trend": trend,
                })
            except Exception as e:
                self._json_response({"error": str(e)}, 500)

        # News Summary
        elif path == "/api/news/summary":
            try:
                self._json_response(compute_news_summary())
            except Exception as e:
                print(f"News summary error: {e}")
                self._json_response({"error": str(e)}, 500)

        # Heat Map
        elif path == "/api/heatmap":
            if _heatmap_cache["data"] and (time.time() - _heatmap_cache["ts"]) < HEATMAP_TTL:
                self._json_response(_heatmap_cache["data"])
                return
            tiles = []
            total_val = sum(e.get("market_value", 0) for e in store["equities"]) + sum(c.get("market_value", 0) for c in store["crypto"])
            for e in store["equities"]:
                t = e.get("ticker", "")
                if t in ("OPENZ", "OPENL", "OPENW"):
                    continue
                mv = e.get("market_value", 0) or 0
                price = e.get("live_price", 0) or 0
                change_pct = 0
                try:
                    if HAS_YFINANCE and price:
                        info = yf.Ticker(t).info
                        pc = info.get("previousClose") or info.get("regularMarketPreviousClose") or 0
                        if pc > 0:
                            change_pct = round(((price - pc) / pc) * 100, 2)
                except Exception:
                    pass
                tiles.append({"ticker": t, "price": round(price, 2), "daily_change_pct": change_pct,
                    "market_value": round(mv, 2), "weight": round(mv / total_val * 100, 2) if total_val > 0 else 0, "sector": "equity"})
            for c in store["crypto"]:
                asset = c.get("asset", "")
                price = c.get("current_price", 0) or 0
                mv = c.get("market_value", 0) or 0
                change_pct = 0
                yf_t = {"ETH": "ETH-USD", "BTC": "BTC-USD", "SOL": "SOL-USD"}.get(asset)
                if yf_t and HAS_YFINANCE:
                    try:
                        info = yf.Ticker(yf_t).info
                        pc = info.get("previousClose") or 0
                        if pc > 0:
                            change_pct = round(((price - pc) / pc) * 100, 2)
                    except Exception:
                        pass
                tiles.append({"ticker": asset, "price": round(price, 2), "daily_change_pct": change_pct,
                    "market_value": round(mv, 2), "weight": round(mv / total_val * 100, 2) if total_val > 0 else 0, "sector": "crypto"})
            # Option underlying tickers not in equities
            opt_tickers = set()
            for o in store["options"]:
                parsed = parse_occ_symbol(o.get("occ_long", "")) or parse_occ_symbol(o.get("occ_short", ""))
                if parsed:
                    opt_tickers.add(parsed["underlying"])
            eq_tickers = {e["ticker"] for e in store["equities"]}
            for ot in opt_tickers - eq_tickers - {"OPENZ", "OPENL", "OPENW"}:
                try:
                    if HAS_YFINANCE:
                        info = yf.Ticker(ot).info
                        p = info.get("regularMarketPrice") or 0
                        pc = info.get("previousClose") or 0
                        cp = round(((p - pc) / pc) * 100, 2) if pc > 0 else 0
                        tiles.append({"ticker": ot, "price": round(p, 2), "daily_change_pct": cp, "market_value": 0, "weight": 2, "sector": "options"})
                except Exception:
                    pass
            # Macro indices
            for idx in ("SPY", "QQQ", "DIA"):
                try:
                    if HAS_YFINANCE:
                        info = yf.Ticker(idx).info
                        p = info.get("regularMarketPrice") or info.get("previousClose") or 0
                        pc = info.get("previousClose") or 0
                        cp = round(((p - pc) / pc) * 100, 2) if pc > 0 else 0
                        tiles.append({"ticker": idx, "price": round(p, 2), "daily_change_pct": cp, "market_value": None, "weight": 3, "sector": "index"})
                except Exception:
                    pass
            result = {"tiles": tiles, "as_of": datetime.now().isoformat()}
            _heatmap_cache["data"] = result
            _heatmap_cache["ts"] = time.time()
            self._json_response(result)

        # News Feed
        elif path == "/api/news/portfolio":
            if not HAS_PROVIDERS:
                self._json_response({"error": "data_providers not available"}, 500)
                return
            try:
                np = get_news_provider()
                tickers = list(set(e["ticker"] for e in store["equities"] if e["ticker"] not in ("OPENZ", "OPENL", "OPENW")))
                # Add macro tickers for general market news
                macro_tickers = ["SPY", "QQQ", "DIA"]
                articles = np.get_portfolio_news(tickers)
                macro_articles = np.get_portfolio_news(macro_tickers)
                # Tag macro articles
                for a in macro_articles:
                    a["is_macro"] = True
                for a in articles:
                    a["is_macro"] = False
                combined = articles + macro_articles
                combined.sort(key=lambda x: x.get("published_ts", 0), reverse=True)
                # Save daily sentiment snapshot
                try:
                    save_daily_sentiment(combined)
                except Exception:
                    pass
                self._json_response(combined)
            except Exception as e:
                print(f"News portfolio error: {e}")
                self._json_response({"error": str(e)}, 500)
        elif path.startswith("/api/news/"):
            if not HAS_PROVIDERS:
                self._json_response({"error": "data_providers not available"}, 500)
                return
            try:
                ticker = path.split("/")[-1].upper()
                np = get_news_provider()
                articles = np.get_news(ticker)
                self._json_response(articles)
            except Exception as e:
                print(f"News ticker error: {e}")
                self._json_response({"error": str(e)}, 500)

        # Risk Dashboard
        elif path == "/api/risk":
            try:
                self._json_response(compute_risk_dashboard())
            except Exception as e:
                print(f"Risk dashboard error: {e}")
                self._json_response({"error": str(e)}, 500)

        # WheelHouse Config
        elif path == "/api/config":
            config_path = DATA_DIR / "wheelhouse_config.json"
            if config_path.exists():
                with open(config_path) as f:
                    self._json_response(json.load(f))
            else:
                # Default config
                default_config = {
                    "site_name": "WheelHouse",
                    "tagline": "Your personal portfolio command center",
                    "creator": "Ed Espinal",
                    "creator_url": "https://github.com/edwardespinal-prog",
                    "tabs": {
                        "dashboard": True, "equities": True, "options": True,
                        "watchlist": True, "tax_ledger": True, "cc_income": True,
                        "crypto": True, "news": True, "dark_pool_insiders": True,
                        "wheelers_paradise": True, "uncle_eddies": True
                    },
                    "features": {
                        "eth_institutional_intelligence": True,
                        "portfolio_risk_dashboard": True,
                        "cash_flow_tracker": True,
                        "benchmark_comparison": True
                    }
                }
                self._json_response(default_config)

        else:
            self._json_response({"error": "Not found"}, 404)

    def _handle_api_post(self, path, body):
        if path == "/api/watchlist":
            new_id = max((e["id"] for e in store["watchlist"]), default=0) + 1
            entry = {
                "id": new_id,
                "ticker": (body.get("ticker", "") or "").upper(),
                "target_buy": float(body.get("target_buy", 0) or 0),
                "notes": body.get("notes", ""),
                "date_added": date.today().isoformat(),
                "alerts_enabled": body.get("alerts_enabled", True),
            }
            store["watchlist"].append(entry)
            save_store()
            self._json_response(entry, 201)

        elif path == "/api/cash-flows":
            new_id = max((e["id"] for e in store["cash_flows"]), default=0) + 1
            entry = {"id": new_id, **body}
            store["cash_flows"].append(entry)
            # Adjust cash_balance for deposits/dividends/withdrawals
            amt = float(entry.get("amount", 0) or 0)
            if entry.get("type") in ("deposit", "dividend"):
                store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) + amt, 2)
            elif entry.get("type") == "withdrawal":
                store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) - amt, 2)
            save_store()
            self._json_response(entry, 201)

        elif path == "/api/tax-ledger/close":
            # Update an existing open tax ledger entry with close details
            ticker = body.get("ticker", "")
            result = close_tax_ledger_entry(
                ticker=ticker,
                date_closed=body.get("date_closed", date.today().isoformat()),
                sell_price=body.get("sell_price", 0),
                realized_pl=body.get("realized_pl", 0),
                days_held=body.get("days_held", 0),
                tax_status=body.get("tax_status", "Short-term"),
                cost_to_close=body.get("cost_to_close")
            )
            if result:
                self._json_response(result)
            else:
                self._json_response({"error": f"No open tax ledger entry found for {ticker}"}, 404)

        elif path == "/api/tax-ledger":
            new_id = max((e["id"] for e in store["tax_ledger"]), default=0) + 1
            entry = {"id": new_id, **body}
            store["tax_ledger"].append(entry)
            save_store()
            self._json_response(entry, 201)

        elif path == "/api/cc-income/roll":
            cc_id = body.get("cc_income_id")
            cc_entry = next((e for e in store["cc_income"] if e["id"] == cc_id), None)
            if not cc_entry:
                self._json_response({"error": f"CC income entry {cc_id} not found"}, 404)
                return

            close_cost_pc = float(body.get("cost_to_close", 0))
            new_premium_pc = float(body.get("new_premium", 0))
            new_strike = body.get("new_strike", "")
            new_expiry = body.get("new_expiry", "")
            roll_date = body.get("date", date.today().isoformat())

            ticker = cc_entry.get("ticker", "")
            # Find linked options position
            opt = next((o for o in store["options"] if o.get("occ_long", "").upper() == ticker.upper() and o.get("cc_premium_collected") is not None), None)
            contracts = float(opt.get("contracts", 0)) if opt else float(body.get("contracts", 1))

            # Update roll_history
            history = cc_entry.get("roll_history", [])
            new_leg = len(history) + 1
            # Update last leg's cost_to_close if it was 0 (still open)
            if history and history[-1].get("cost_to_close", 0) == 0:
                history[-1]["cost_to_close"] = close_cost_pc
                history[-1]["net_per_contract"] = round(history[-1].get("premium", 0) - close_cost_pc, 2)
            history.append({
                "leg": new_leg,
                "date": roll_date,
                "action": "roll",
                "strike": new_strike,
                "expiry": new_expiry,
                "premium": new_premium_pc,
                "cost_to_close": 0,
                "net_per_contract": new_premium_pc,
            })
            cc_entry["roll_history"] = history

            # Update cumulative totals (all per-contract × contracts × 100)
            cc_entry["premium_collected"] = round(float(cc_entry.get("premium_collected", 0) or 0) + (new_premium_pc * contracts * 100), 2)
            cc_entry["cost_to_close"] = round(float(cc_entry.get("cost_to_close", 0) or 0) + (close_cost_pc * contracts * 100), 2)
            cc_entry["net_profit"] = round(cc_entry["premium_collected"] - cc_entry["cost_to_close"], 2)
            cc_entry["contract_details"] = f"{new_expiry} {new_strike}"

            # Update options position if found
            if opt:
                opt["cc_premium_collected"] = cc_entry["premium_collected"]

            # Update linked tax ledger entry
            for tl in store["tax_ledger"]:
                if tl.get("cc_income_id") == cc_id:
                    tl["premium_received"] = cc_entry["premium_collected"]
                    break

            # Cash flow: deduct cost to close old leg, add new premium (matches RH)
            close_cash = close_cost_pc * contracts * 100
            new_cash = new_premium_pc * contracts * 100
            store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) - close_cash + new_cash, 2)

            recalc_equity_cc_income()
            save_store()
            self._json_response(cc_entry)
            return

        elif path == "/api/cc-income":
            new_id = max((e["id"] for e in store["cc_income"]), default=0) + 1
            entry = {"id": new_id, **body}
            prem = float(entry.get("premium_collected", 0) or 0)
            ctc = float(entry.get("cost_to_close", 0) or 0)
            entry["net_profit"] = prem - ctc
            store["cc_income"].append(entry)
            # CC premium received as cash (matches RH behavior)
            if prem > 0:
                store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) + prem, 2)
            # If closed at creation, deduct cost to close from cash
            if ctc > 0 and entry.get("date_closed"):
                store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) - ctc, 2)
            save_store()
            recalc_equity_cc_income()
            # Auto-create tax ledger entry for the CC open
            if not body.get("skip_tax_ledger"):
                tl_id = max((e["id"] for e in store["tax_ledger"]), default=0) + 1
                tl_entry = {
                    "id": tl_id, "date_acquired": entry.get("date_opened", date.today().isoformat()),
                    "asset_type": "Covered Call", "ticker": entry.get("ticker", ""),
                    "quantity": 1, "avg_cost": 0, "cost_basis": 0,
                    "premium_received": prem, "contract_details": entry.get("contract_details", ""),
                    "tax_status": "Short-term", "realized_pl": 0,
                    "date_closed": "", "sell_price": 0, "cost_to_close": 0,
                    "current_price": 0, "unrealized_pl": 0, "days_held": 0, "tax_reserve": "0",
                    "cc_income_id": new_id
                }
                # If already closed at creation time, fill in close details
                if entry.get("date_closed") and ctc >= 0:
                    dh = 0
                    try:
                        dh = (date.fromisoformat(entry["date_closed"]) - date.fromisoformat(entry.get("date_opened", entry["date_closed"]))).days
                    except Exception:
                        pass
                    tl_entry["date_closed"] = entry["date_closed"]
                    tl_entry["cost_to_close"] = ctc
                    tl_entry["realized_pl"] = round(prem - ctc, 2)
                    tl_entry["days_held"] = dh
                    tl_entry["tax_reserve"] = str(round(max(prem - ctc, 0) * 0.3, 2))
                store["tax_ledger"].append(tl_entry)
            save_store()
            self._json_response(entry, 201)

        elif path == "/api/cards":
            new_id = max((c["id"] for c in store["cards"]), default=0) + 1
            card = {"id": new_id, **body}
            card.setdefault("status", "collection")
            store["cards"].append(card)
            save_store()
            self._json_response(card, 201)

        elif path == "/api/sales":
            new_id = max((s["id"] for s in store["sales"]), default=0) + 1
            entry = {"id": new_id, **body}
            # Auto-calc platform fees if not provided
            if not entry.get("platform_fees") and entry.get("sale_price"):
                rates = {"ebay": 0.1325, "comc": 0.05, "in-person": 0, "other": 0}
                rate = rates.get(entry.get("platform", "other"), 0)
                entry["platform_fees"] = round(float(entry["sale_price"]) * rate, 2)
            entry.setdefault("status", "sold")
            entry.setdefault("date_sold", date.today().isoformat())
            # Compute net profit
            sp = float(entry.get("sale_price", 0) or 0)
            fees = float(entry.get("platform_fees", 0) or 0)
            ship = float(entry.get("shipping_cost", 0) or 0)
            cost = float(entry.get("purchase_price", 0) or 0)
            entry["net_profit"] = round(sp - fees - ship - cost, 2)
            store["sales"].append(entry)
            save_store()
            self._json_response(entry, 201)

        elif path == "/api/box-purchases":
            new_id = max((b["id"] for b in store["box_purchases"]), default=0) + 1
            entry = {"id": new_id, **body}
            entry.setdefault("date", date.today().isoformat())
            store["box_purchases"].append(entry)
            save_store()
            self._json_response(entry, 201)

        elif path == "/api/sales/import-ebay":
            # eBay CSV import — expects JSON body with csv_data string
            csv_text = body.get("csv_data", "")
            if not csv_text:
                self._json_response({"error": "csv_data required"}, 400)
                return
            import csv
            import io
            reader = csv.DictReader(io.StringIO(csv_text))
            preview = []
            for row in reader:
                title = row.get("Item title", row.get("Title", "")).lower()
                gross = float(row.get("Gross transaction amount", row.get("Amount", "0")).replace("$", "").replace(",", "") or 0)
                fees = float(row.get("Total fees", row.get("Fees", "0")).replace("$", "").replace(",", "").replace("--", "0") or 0)
                dt = row.get("Transaction creation date", row.get("Date", ""))
                order = row.get("Order number", "")
                txn_type = row.get("Type", "").lower()
                # Skip refunds — they net out
                if "refund" in txn_type:
                    continue
                # Auto-categorize
                card_kw = ["card", "rookie", "auto", "refractor", "topps", "prizm", "bowman", "chrome"]
                sneak_kw = ["jordan", "nike", "dunk", "yeezy", "size ", "sz "]
                cloth_kw = ["hoodie", "crewneck", "shirt", "jersey", "kith"]
                if any(k in title for k in card_kw):
                    cat = "cards"
                elif any(k in title for k in sneak_kw):
                    cat = "sneakers"
                elif any(k in title for k in cloth_kw):
                    cat = "clothing"
                else:
                    cat = "other"
                preview.append({
                    "item_name": row.get("Item title", row.get("Title", "")),
                    "category": cat,
                    "platform": "ebay",
                    "sale_price": round(gross, 2),
                    "platform_fees": round(abs(fees), 2),
                    "date_sold": dt[:10] if dt else "",
                    "order_number": order,
                    "status": "completed",
                })
            # If confirm flag, actually save
            if body.get("confirm"):
                for p in preview:
                    new_id = max((s["id"] for s in store["sales"]), default=0) + 1
                    entry = {"id": new_id, **p}
                    sp = float(entry.get("sale_price", 0) or 0)
                    f = float(entry.get("platform_fees", 0) or 0)
                    entry["net_profit"] = round(sp - f, 2)
                    entry["date_completed"] = entry["date_sold"]
                    store["sales"].append(entry)
                save_store()
                self._json_response({"imported": len(preview), "sales": preview})
            else:
                self._json_response({"preview": preview, "count": len(preview)})

        elif path == "/api/portfolio/equities":
            ticker = body.get("ticker", "").upper()
            add_qty = float(body.get("quantity", 0))
            add_cost = float(body.get("avg_cost", 0))

            # Check if ticker already exists — aggregate if so
            existing = next((e for e in store["equities"] if e["ticker"].upper() == ticker), None)
            if existing and body.get("aggregate", True):
                old_qty = existing.get("quantity", 0)
                old_cost = existing.get("avg_cost", 0)
                new_qty = old_qty + add_qty
                # Weighted average cost
                if new_qty > 0:
                    new_avg = ((old_qty * old_cost) + (add_qty * add_cost)) / new_qty
                else:
                    new_avg = add_cost
                existing["quantity"] = new_qty
                existing["avg_cost"] = round(new_avg, 4)
                existing["total_invested"] = round(new_avg * new_qty, 2)
                price = existing.get("live_price", 0)
                existing["market_value"] = price * new_qty if price else new_qty * new_avg
                existing["pl_dollar"] = existing["market_value"] - existing["total_invested"]
                existing["pl_percent"] = existing["pl_dollar"] / existing["total_invested"] if existing["total_invested"] > 0 else 0
                save_store()
                self._json_response({"aggregated": True, **existing})
            else:
                new_id = max((e["id"] for e in store["equities"]), default=0) + 1
                entry = {"id": new_id, **body, "ticker": ticker}
                entry.setdefault("market_value", entry.get("live_price", 0) * entry.get("quantity", 0))
                entry.setdefault("total_invested", add_cost * add_qty)
                entry.setdefault("pl_dollar", entry.get("market_value", 0) - entry.get("total_invested", 0))
                entry.setdefault("pl_percent", entry["pl_dollar"] / entry["total_invested"] if entry.get("total_invested", 0) > 0 else 0)
                entry.setdefault("cc_income", 0)
                entry.setdefault("cc_yield", 0)
                store["equities"].append(entry)
                save_store()
                self._json_response(entry, 201)

            # Deduct from cash_balance
            store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) - (add_qty * add_cost), 2)

            # Auto-create tax ledger entry for the new lot
            if not body.get("skip_tax_ledger"):
                date_acq = body.get("date_acquired", date.today().isoformat())
                tl_id = max((e["id"] for e in store["tax_ledger"]), default=0) + 1
                store["tax_ledger"].append({
                    "id": tl_id, "date_acquired": date_acq, "asset_type": "Equity",
                    "ticker": ticker, "quantity": add_qty, "avg_cost": add_cost,
                    "cost_basis": round(add_qty * add_cost, 2), "tax_status": "Short-term",
                    "realized_pl": 0, "date_closed": "", "sell_price": 0,
                    "premium_received": 0, "current_price": 0, "unrealized_pl": 0,
                    "days_held": 0, "tax_reserve": "0"
                })
                save_store()

        elif path == "/api/portfolio/options":
            pos_name = body.get("position_name", "").strip()
            add_c = float(body.get("contracts", 0))
            add_cost = float(body.get("avg_cost", 0))
            existing = next((o for o in store["options"] if o.get("position_name","").strip().lower() == pos_name.lower()), None)
            if existing and add_c > 0:
                old_c = float(existing.get("contracts", 0))
                old_a = float(existing.get("avg_cost", 0))
                nc = old_c + add_c
                old_total = old_c * old_a * 100
                add_total = add_c * add_cost * 100
                new_total = old_total + add_total
                na = new_total / (nc * 100) if nc > 0 else add_cost
                existing["contracts"] = nc
                existing["avg_cost"] = round(na, 4)
                existing["total_invested"] = round(new_total, 2)
                # Recalculate P/L with current live price
                live_p = existing.get("live_price_long", 0) or 0
                short_p = existing.get("live_price_short", 0) or 0
                if live_p > 0:
                    spread = live_p - short_p if short_p > 0 else live_p
                    existing["net_spread"] = spread
                    existing["total_value"] = round(spread * nc * 100, 2)
                    existing["pl_dollar"] = round(existing["total_value"] - existing["total_invested"], 2)
                if body.get("date_acquired"):
                    existing["date_acquired"] = body["date_acquired"]
                save_store()
                self._json_response({"aggregated": True, **existing})
            else:
                new_id = max((o["id"] for o in store["options"]), default=0) + 1
                entry = {"id": new_id, **body}
                entry.setdefault("total_invested", add_cost * add_c * 100)
                store["options"].append(entry)
                save_store()
                self._json_response(entry, 201)

            # Deduct from cash_balance
            store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) - (add_c * add_cost * 100), 2)

            # Auto-create tax ledger entry for the new lot
            if not body.get("skip_tax_ledger"):
                date_acq = body.get("date_acquired", date.today().isoformat())
                tl_id = max((e["id"] for e in store["tax_ledger"]), default=0) + 1
                store["tax_ledger"].append({
                    "id": tl_id, "date_acquired": date_acq, "asset_type": "Option",
                    "ticker": pos_name, "quantity": add_c, "avg_cost": add_cost,
                    "cost_basis": round(add_c * add_cost * 100, 2), "tax_status": "Short-term",
                    "realized_pl": 0, "date_closed": "", "sell_price": 0,
                    "premium_received": 0, "current_price": 0, "unrealized_pl": 0,
                    "days_held": 0, "tax_reserve": "0"
                })
                save_store()

        elif path == "/api/portfolio/crypto":
            asset = body.get("asset", "").upper()
            add_qty = float(body.get("quantity", 0))
            add_cost = float(body.get("avg_price", 0))
            existing = next((c for c in store["crypto"] if c["asset"].upper() == asset), None)
            if existing:
                old_qty = existing.get("quantity", 0)
                old_cost = existing.get("avg_price", 0)
                new_qty = old_qty + add_qty
                if new_qty > 0:
                    new_avg = ((old_qty * old_cost) + (add_qty * add_cost)) / new_qty
                else:
                    new_avg = add_cost
                existing["quantity"] = round(new_qty, 6)
                existing["avg_price"] = round(new_avg, 2)
                price = existing.get("current_price", 0)
                existing["market_value"] = price * new_qty if price else new_qty * new_avg
                existing["pl_dollar"] = existing["market_value"] - (new_avg * new_qty)
                existing["pl_percent"] = existing["pl_dollar"] / (new_avg * new_qty) if (new_avg * new_qty) > 0 else 0
                save_store()
                self._json_response({"aggregated": True, **existing})
            else:
                new_id = max((c["id"] for c in store["crypto"]), default=0) + 1
                entry = {"id": new_id, "asset": asset, "quantity": add_qty, "avg_price": add_cost,
                         "current_price": 0, "market_value": 0, "pl_dollar": 0, "pl_percent": 0}
                store["crypto"].append(entry)
                save_store()
                self._json_response(entry, 201)

            # Deduct from cash_balance
            store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) - (add_qty * add_cost), 2)

            # Auto-create tax ledger entry for the new lot
            if not body.get("skip_tax_ledger"):
                date_acq = body.get("date_acquired", date.today().isoformat())
                tl_id = max((e["id"] for e in store["tax_ledger"]), default=0) + 1
                store["tax_ledger"].append({
                    "id": tl_id, "date_acquired": date_acq, "asset_type": "Crypto",
                    "ticker": asset, "quantity": add_qty, "avg_cost": add_cost,
                    "cost_basis": round(add_qty * add_cost, 2), "tax_status": "Short-term",
                    "realized_pl": 0, "date_closed": "", "sell_price": 0,
                    "premium_received": 0, "current_price": 0, "unrealized_pl": 0,
                    "days_held": 0, "tax_reserve": "0"
                })
                save_store()

        # ── Sell Endpoints ────────────────────────────────────────────────
        elif path == "/api/portfolio/equities/sell":
            ticker = body.get("ticker", "").upper()
            sell_qty = float(body.get("quantity", 0))
            sell_price = float(body.get("sell_price", 0))
            date_sold = body.get("date_sold", date.today().isoformat())

            eq = next((e for e in store["equities"] if e["ticker"].upper() == ticker), None)
            if not eq:
                self._json_response({"error": f"Position {ticker} not found"}, 404)
                return
            old_qty = float(eq.get("quantity", 0))
            if sell_qty > old_qty + 0.001:
                self._json_response({"error": f"Insufficient shares: have {old_qty}, selling {sell_qty}"}, 400)
                return

            avg_cost = float(eq.get("avg_cost", 0))
            proceeds = round(sell_qty * sell_price, 2)
            cost_basis = round(sell_qty * avg_cost, 2)
            realized_pl = round(proceeds - cost_basis, 2)

            # Determine holding period
            date_acquired = eq.get("date_acquired", "")
            days_held = 0
            if date_acquired:
                try:
                    days_held = (date.fromisoformat(date_sold) - date.fromisoformat(date_acquired)).days
                except Exception:
                    pass
            tax_status = "Long-term" if days_held > 365 else "Short-term"

            # Update position
            remaining = round(old_qty - sell_qty, 6)
            if remaining < 0.001:
                store["equities"] = [e for e in store["equities"] if e["ticker"].upper() != ticker]
                eq_result = {"deleted": True, "ticker": ticker}
            else:
                eq["quantity"] = remaining
                old_invested = float(eq.get("total_invested", 0) or avg_cost * old_qty)
                eq["total_invested"] = round(old_invested * (remaining / old_qty), 2)
                price = eq.get("live_price", 0) or 0
                eq["market_value"] = round(price * remaining, 2)
                eq["pl_dollar"] = round(eq["market_value"] - eq["total_invested"], 2)
                eq["pl_percent"] = eq["pl_dollar"] / eq["total_invested"] if eq["total_invested"] > 0 else 0
                eq_result = eq

            # Add proceeds to cash_balance
            store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) + proceeds, 2)

            # Create closed tax ledger entry
            tax_entry = None
            if not body.get("skip_tax_ledger"):
                tl_id = max((e["id"] for e in store["tax_ledger"]), default=0) + 1
                tax_entry = {
                    "id": tl_id, "date_acquired": date_acquired, "asset_type": "Equity",
                    "ticker": ticker, "quantity": sell_qty, "avg_cost": avg_cost,
                    "cost_basis": cost_basis, "sell_price": sell_price,
                    "date_closed": date_sold, "realized_pl": realized_pl,
                    "tax_status": tax_status, "days_held": days_held,
                    "tax_reserve": str(round(max(realized_pl, 0) * 0.30, 2)),
                    "premium_received": 0, "current_price": sell_price, "unrealized_pl": 0,
                }
                store["tax_ledger"].append(tax_entry)

            save_store()
            self._json_response({
                "equity": eq_result, "tax_entry": tax_entry,
                "realized_pl": realized_pl, "cash_balance": store["cash_balance"]
            })

        elif path == "/api/portfolio/options/sell":
            pos_name = body.get("position_name", "").strip()
            pos_id = body.get("id")
            sell_contracts = float(body.get("contracts", 0))
            sell_price = float(body.get("sell_price", 0))
            date_sold = body.get("date_sold", date.today().isoformat())

            if pos_id:
                opt = next((o for o in store["options"] if o["id"] == pos_id), None)
            else:
                opt = next((o for o in store["options"] if o.get("position_name", "").strip().lower() == pos_name.lower()), None)
            if not opt:
                self._json_response({"error": f"Option {pos_name or pos_id} not found"}, 404)
                return

            old_c = float(opt.get("contracts", 0))
            if sell_contracts > old_c + 0.001:
                self._json_response({"error": f"Insufficient contracts: have {old_c}, selling {sell_contracts}"}, 400)
                return

            avg_cost = float(opt.get("avg_cost", 0))
            proceeds = round(sell_contracts * sell_price * 100, 2)
            cost_basis = round(sell_contracts * avg_cost * 100, 2)
            realized_pl = round(proceeds - cost_basis, 2)
            date_acquired = opt.get("date_acquired", "")
            days_held = 0
            if date_acquired:
                try:
                    days_held = (date.fromisoformat(date_sold) - date.fromisoformat(date_acquired)).days
                except Exception:
                    pass
            tax_status = "Long-term" if days_held > 365 else "Short-term"

            remaining = round(old_c - sell_contracts, 6)
            if remaining < 0.001:
                store["options"] = [o for o in store["options"] if o.get("position_name", "").strip().lower() != (opt.get("position_name", "").strip().lower())]
                opt_result = {"deleted": True, "position_name": opt.get("position_name", "")}
            else:
                opt["contracts"] = remaining
                old_total = old_c * avg_cost * 100
                opt["total_invested"] = round(old_total * (remaining / old_c), 2)
                opt["avg_cost"] = avg_cost
                opt_result = opt

            store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) + proceeds, 2)

            tax_entry = None
            if not body.get("skip_tax_ledger"):
                tl_id = max((e["id"] for e in store["tax_ledger"]), default=0) + 1
                tax_entry = {
                    "id": tl_id, "date_acquired": date_acquired, "asset_type": "Option",
                    "ticker": opt.get("position_name", pos_name), "quantity": sell_contracts,
                    "avg_cost": avg_cost, "cost_basis": cost_basis, "sell_price": sell_price,
                    "date_closed": date_sold, "realized_pl": realized_pl,
                    "tax_status": tax_status, "days_held": days_held,
                    "tax_reserve": str(round(max(realized_pl, 0) * 0.30, 2)),
                    "premium_received": 0, "current_price": sell_price, "unrealized_pl": 0,
                }
                store["tax_ledger"].append(tax_entry)

            save_store()
            self._json_response({
                "option": opt_result, "tax_entry": tax_entry,
                "realized_pl": realized_pl, "cash_balance": store["cash_balance"]
            })

        elif path == "/api/portfolio/crypto/sell":
            asset = body.get("asset", "").upper()
            sell_qty = float(body.get("quantity", 0))
            sell_price = float(body.get("sell_price", 0))
            date_sold = body.get("date_sold", date.today().isoformat())

            cr = next((c for c in store["crypto"] if c["asset"].upper() == asset), None)
            if not cr:
                self._json_response({"error": f"Crypto {asset} not found"}, 404)
                return
            old_qty = float(cr.get("quantity", 0))
            if sell_qty > old_qty + 0.000001:
                self._json_response({"error": f"Insufficient {asset}: have {old_qty}, selling {sell_qty}"}, 400)
                return

            avg_price = float(cr.get("avg_price", 0))
            proceeds = round(sell_qty * sell_price, 2)
            cost_basis = round(sell_qty * avg_price, 2)
            realized_pl = round(proceeds - cost_basis, 2)
            date_acquired = cr.get("date_acquired", "")
            days_held = 0
            if date_acquired:
                try:
                    days_held = (date.fromisoformat(date_sold) - date.fromisoformat(date_acquired)).days
                except Exception:
                    pass
            tax_status = "Long-term" if days_held > 365 else "Short-term"

            remaining = round(old_qty - sell_qty, 6)
            if remaining < 0.000001:
                store["crypto"] = [c for c in store["crypto"] if c["asset"].upper() != asset]
                cr_result = {"deleted": True, "asset": asset}
            else:
                cr["quantity"] = remaining
                cr["avg_price"] = avg_price
                price = cr.get("current_price", 0) or 0
                cr["market_value"] = round(price * remaining, 2)
                cr["pl_dollar"] = round(cr["market_value"] - (avg_price * remaining), 2)
                cr["pl_percent"] = cr["pl_dollar"] / (avg_price * remaining) if (avg_price * remaining) > 0 else 0
                cr_result = cr

            store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) + proceeds, 2)

            tax_entry = None
            if not body.get("skip_tax_ledger"):
                tl_id = max((e["id"] for e in store["tax_ledger"]), default=0) + 1
                tax_entry = {
                    "id": tl_id, "date_acquired": date_acquired, "asset_type": "Crypto",
                    "ticker": asset, "quantity": sell_qty, "avg_cost": avg_price,
                    "cost_basis": cost_basis, "sell_price": sell_price,
                    "date_closed": date_sold, "realized_pl": realized_pl,
                    "tax_status": tax_status, "days_held": days_held,
                    "tax_reserve": str(round(max(realized_pl, 0) * 0.30, 2)),
                    "premium_received": 0, "current_price": sell_price, "unrealized_pl": 0,
                }
                store["tax_ledger"].append(tax_entry)

            save_store()
            self._json_response({
                "crypto": cr_result, "tax_entry": tax_entry,
                "realized_pl": realized_pl, "cash_balance": store["cash_balance"]
            })

        elif path == "/api/demo-data":
            # Load demo dataset
            demo_dir = BACKEND_DIR / "demo_data"
            if demo_dir.exists():
                for fname in demo_dir.glob("*.json"):
                    key = fname.stem
                    if key in store:
                        with open(fname) as f:
                            store[key] = json.load(f)
                # Also load supplementary files
                for fname in ["watchlist.json", "cash_flow.json", "sales.json", "box_purchases.json"]:
                    src = demo_dir / fname
                    if src.exists():
                        import shutil
                        shutil.copy2(src, DATA_DIR / fname)
                save_store()
                self._json_response({"status": "demo_data_loaded"})
            else:
                self._json_response({"error": "Demo data directory not found"}, 404)

        else:
            self._json_response({"error": "Not found"}, 404)

    def _handle_api_put(self, path, body):
        if path == "/api/cash-balance":
            store["cash_balance"] = float(body.get("cash_balance", 0))
            save_store()
            self._json_response({"cash_balance": round(store["cash_balance"], 2)})
            return

        if path == "/api/bmnr-nav/treasury":
            if not HAS_PROVIDERS:
                self._json_response({"error": "data_providers not available"}, 500)
                return
            try:
                result = get_bmnr_nav_provider().update_treasury(body)
                # Also update store for persistence
                if "eth_holdings" in body:
                    store["bmnr_eth_holdings"] = body["eth_holdings"]
                if "eth_staked" in body:
                    store["bmnr_eth_staked"] = body["eth_staked"]
                save_store()
                self._json_response(result)
            except Exception as e:
                print(f"BMNR treasury update error: {e}")
                self._json_response({"error": str(e)}, 500)
            return

        if path.startswith("/api/watchlist/"):
            entry_id = int(path.split("/")[-1])
            for i, entry in enumerate(store["watchlist"]):
                if entry["id"] == entry_id:
                    store["watchlist"][i].update(body)
                    if "ticker" in body:
                        store["watchlist"][i]["ticker"] = body["ticker"].upper()
                    save_store()
                    self._json_response(store["watchlist"][i])
                    return
            self._json_response({"error": "Not found"}, 404)

        elif path.startswith("/api/cash-flows/"):
            entry_id = int(path.split("/")[-1])
            for i, entry in enumerate(store["cash_flows"]):
                if entry["id"] == entry_id:
                    # Reverse old cash_balance effect
                    old_amt = float(entry.get("amount", 0) or 0)
                    old_type = entry.get("type", "")
                    if old_type in ("deposit", "dividend"):
                        store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) - old_amt, 2)
                    elif old_type == "withdrawal":
                        store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) + old_amt, 2)
                    store["cash_flows"][i].update(body)
                    # Apply new cash_balance effect
                    new_amt = float(store["cash_flows"][i].get("amount", 0) or 0)
                    new_type = store["cash_flows"][i].get("type", "")
                    if new_type in ("deposit", "dividend"):
                        store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) + new_amt, 2)
                    elif new_type == "withdrawal":
                        store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) - new_amt, 2)
                    save_store()
                    self._json_response(store["cash_flows"][i])
                    return
            self._json_response({"error": "Not found"}, 404)

        elif path.startswith("/api/tax-ledger/"):
            entry_id = int(path.split("/")[-1])
            for i, entry in enumerate(store["tax_ledger"]):
                if entry["id"] == entry_id:
                    store["tax_ledger"][i].update(body)
                    save_store()
                    self._json_response(store["tax_ledger"][i])
                    return
            self._json_response({"error": "Not found"}, 404)

        elif path.startswith("/api/cards/"):
            card_id = int(path.split("/")[-1])
            for i, card in enumerate(store["cards"]):
                if card["id"] == card_id:
                    store["cards"][i].update(body)
                    save_store()
                    self._json_response(store["cards"][i])
                    return
            self._json_response({"error": "Not found"}, 404)

        elif path.startswith("/api/sales/"):
            sale_id = int(path.split("/")[-1])
            for i, s in enumerate(store["sales"]):
                if s["id"] == sale_id:
                    store["sales"][i].update(body)
                    # Recompute net_profit
                    sp = float(store["sales"][i].get("sale_price", 0) or 0)
                    fees = float(store["sales"][i].get("platform_fees", 0) or 0)
                    ship = float(store["sales"][i].get("shipping_cost", 0) or 0)
                    cost = float(store["sales"][i].get("purchase_price", 0) or 0)
                    store["sales"][i]["net_profit"] = round(sp - fees - ship - cost, 2)
                    save_store()
                    self._json_response(store["sales"][i])
                    return
            self._json_response({"error": "Not found"}, 404)

        elif path.startswith("/api/box-purchases/"):
            bp_id = int(path.split("/")[-1])
            for i, b in enumerate(store["box_purchases"]):
                if b["id"] == bp_id:
                    store["box_purchases"][i].update(body)
                    save_store()
                    self._json_response(store["box_purchases"][i])
                    return
            self._json_response({"error": "Not found"}, 404)

        elif path.startswith("/api/portfolio/equities/"):
            entry_id = int(path.split("/")[-1])
            for i, entry in enumerate(store["equities"]):
                if entry["id"] == entry_id:
                    store["equities"][i].update(body)
                    save_store()
                    self._json_response(store["equities"][i])
                    return
            self._json_response({"error": "Not found"}, 404)

        elif path.startswith("/api/portfolio/crypto/"):
            entry_id = int(path.split("/")[-1])
            for i, entry in enumerate(store["crypto"]):
                if entry["id"] == entry_id:
                    store["crypto"][i].update(body)
                    save_store()
                    self._json_response(store["crypto"][i])
                    return
            self._json_response({"error": "Not found"}, 404)

        elif path.startswith("/api/cc-income/"):
            entry_id = int(path.split("/")[-1])
            for i, entry in enumerate(store["cc_income"]):
                if entry["id"] == entry_id:
                    was_open = not entry.get("date_closed")
                    old_ctc = float(entry.get("cost_to_close", 0) or 0)
                    store["cc_income"][i].update(body)
                    prem = float(store["cc_income"][i].get("premium_collected", 0) or 0)
                    ctc = float(store["cc_income"][i].get("cost_to_close", 0) or 0)
                    store["cc_income"][i]["net_profit"] = prem - ctc
                    # If being closed now, deduct the NEW cost_to_close delta from cash (matches RH)
                    now_closed = bool(store["cc_income"][i].get("date_closed"))
                    if was_open and now_closed and ctc > old_ctc:
                        close_debit = ctc - old_ctc  # Only deduct the new close cost, not historical roll costs
                        store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) - close_debit, 2)
                    # Update matching tax ledger entry
                    for tl in store["tax_ledger"]:
                        if tl.get("cc_income_id") == entry_id:
                            tl["premium_received"] = prem
                            if store["cc_income"][i].get("date_closed"):
                                dh = 0
                                try:
                                    dh = (date.fromisoformat(store["cc_income"][i]["date_closed"]) - date.fromisoformat(tl.get("date_acquired", store["cc_income"][i]["date_closed"]))).days
                                except Exception:
                                    pass
                                tl["date_closed"] = store["cc_income"][i]["date_closed"]
                                tl["cost_to_close"] = ctc
                                tl["realized_pl"] = round(prem - ctc, 2)
                                tl["days_held"] = dh
                                tl["tax_reserve"] = str(round(max(prem - ctc, 0) * 0.3, 2))
                            break
                    recalc_equity_cc_income()
                    save_store()
                    self._json_response(store["cc_income"][i])
                    return
            self._json_response({"error": "Not found"}, 404)

        elif path.startswith("/api/portfolio/options/"):
            entry_id = int(path.split("/")[-1])
            for i, entry in enumerate(store["options"]):
                if entry["id"] == entry_id:
                    store["options"][i].update(body)
                    save_store()
                    self._json_response(store["options"][i])
                    return
            self._json_response({"error": "Not found"}, 404)

        elif path == "/api/config":
            config_path = DATA_DIR / "wheelhouse_config.json"
            ensure_data_dir()
            with open(config_path, "w") as f:
                json.dump(body, f, indent=2)
            self._json_response(body)

        else:
            self._json_response({"error": "Not found"}, 404)

    def _handle_api_delete(self, path):
        if path.startswith("/api/watchlist/"):
            entry_id = int(path.split("/")[-1])
            store["watchlist"] = [e for e in store["watchlist"] if e["id"] != entry_id]
            save_store()
            self._json_response({"deleted": entry_id})

        elif path.startswith("/api/cash-flows/"):
            entry_id = int(path.split("/")[-1])
            # Reverse cash_balance effect before deleting
            deleted_entry = next((e for e in store["cash_flows"] if e["id"] == entry_id), None)
            if deleted_entry:
                amt = float(deleted_entry.get("amount", 0) or 0)
                if deleted_entry.get("type") in ("deposit", "dividend"):
                    store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) - amt, 2)
                elif deleted_entry.get("type") == "withdrawal":
                    store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) + amt, 2)
            store["cash_flows"] = [e for e in store["cash_flows"] if e["id"] != entry_id]
            save_store()
            self._json_response({"deleted": entry_id})

        elif path.startswith("/api/tax-ledger/"):
            entry_id = int(path.split("/")[-1])
            store["tax_ledger"] = [e for e in store["tax_ledger"] if e["id"] != entry_id]
            save_store()
            self._json_response({"deleted": entry_id})

        elif path.startswith("/api/cards/"):
            card_id = int(path.split("/")[-1])
            store["cards"] = [c for c in store["cards"] if c["id"] != card_id]
            save_store()
            self._json_response({"deleted": card_id})

        elif path.startswith("/api/portfolio/equities/"):
            entry_id = int(path.split("/")[-1])
            store["equities"] = [e for e in store["equities"] if e["id"] != entry_id]
            save_store()
            self._json_response({"deleted": entry_id})

        elif path.startswith("/api/portfolio/options/"):
            entry_id = int(path.split("/")[-1])
            store["options"] = [o for o in store["options"] if o["id"] != entry_id]
            save_store()
            self._json_response({"deleted": entry_id})

        elif path.startswith("/api/cc-income/"):
            entry_id = int(path.split("/")[-1])
            store["cc_income"] = [e for e in store["cc_income"] if e["id"] != entry_id]
            recalc_equity_cc_income()
            save_store()
            self._json_response({"deleted": entry_id})

        elif path.startswith("/api/cards/"):
            entry_id = int(path.split("/")[-1])
            store["cards"] = [c for c in store["cards"] if c["id"] != entry_id]
            save_store()
            self._json_response({"deleted": entry_id})

        elif path.startswith("/api/sales/"):
            entry_id = int(path.split("/")[-1])
            store["sales"] = [s for s in store["sales"] if s["id"] != entry_id]
            save_store()
            self._json_response({"deleted": entry_id})

        elif path.startswith("/api/box-purchases/"):
            entry_id = int(path.split("/")[-1])
            store["box_purchases"] = [b for b in store["box_purchases"] if b["id"] != entry_id]
            save_store()
            self._json_response({"deleted": entry_id})

        elif path == "/api/demo-data":
            # Clear all data back to empty
            for key in store:
                if isinstance(store[key], list):
                    store[key] = []
                elif isinstance(store[key], dict):
                    store[key] = {}
            save_store()
            # Also clear supplementary files
            for fname in ["watchlist.json", "cash_flow.json", "sales.json", "box_purchases.json",
                         "cash_balance.json", "snapshots.json", "intraday_log.json",
                         "options_value_log.json", "historical_prices_cache.json"]:
                p = DATA_DIR / fname
                if p.exists():
                    p.unlink()
            self._json_response({"status": "all_data_cleared"})

        else:
            self._json_response({"error": "Not found"}, 404)

    def log_message(self, format, *args):
        # Suppress default logging for cleaner output, keep errors
        if args and "404" not in str(args[1] if len(args) > 1 else ""):
            pass  # silent


# ── Portfolio History Reconstruction (Option C) ─────────────────────────
# Instead of trusting pre-saved snapshots, reconstruct daily portfolio values
# from position history (tax ledger) × historical prices (yfinance).

_CRYPTO_YF_MAP = {"ETH": "ETH-USD", "BTC": "BTC-USD", "SOL": "SOL-USD"}
_SKIP_TICKERS = {"OPENZ", "OPENL", "OPENW"}  # Warrants — no yfinance data

def load_options_value_log():
    path = DATA_DIR / "options_value_log.json"
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return []

def save_options_value_log(log):
    ensure_data_dir()
    path = DATA_DIR / "options_value_log.json"
    with open(path, "w") as f:
        json.dump(log, f, indent=2)

def save_options_value():
    """Called after refresh_all_prices() when mark cache is warm. One entry per day."""
    if not _mark_cache_warm:
        return
    opt_val = sum(o.get("total_value", 0) for o in store["options"])
    today = date.today().isoformat()
    log = load_options_value_log()
    log = [e for e in log if e["date"] != today]
    log.append({"date": today, "options_value": round(opt_val, 2)})
    log.sort(key=lambda e: e["date"])
    # Keep 2 years max
    if len(log) > 730:
        log = log[-730:]
    save_options_value_log(log)

def load_intraday_log():
    path = DATA_DIR / "intraday_log.json"
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return []

def save_intraday_value():
    """Append current portfolio value to intraday log. 48-hour rolling window."""
    eq_val = sum(e.get("market_value", 0) for e in store["equities"])
    opt_val = sum(o.get("total_value", 0) for o in store["options"])
    cr_val = sum(c.get("market_value", 0) for c in store["crypto"])
    cash_bal = float(store.get("cash_balance", 0) or 0)
    total = eq_val + opt_val + cr_val + cash_bal

    now = datetime.now()
    entry = {
        "ts": now.strftime("%Y-%m-%dT%H:%M:%S"),
        "value": round(total, 2),
        "eq": round(eq_val, 2),
        "cr": round(cr_val, 2),
        "opt": round(opt_val, 2),
        "cash": round(cash_bal, 2),
    }

    log = load_intraday_log()
    log.append(entry)

    # Trim entries older than 48 hours
    cutoff = (now - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M:%S")
    log = [e for e in log if e["ts"] >= cutoff]

    ensure_data_dir()
    path = DATA_DIR / "intraday_log.json"
    with open(path, "w") as f:
        json.dump(log, f)

def _get_options_value_on_date(log, target_date):
    """Nearest prior options value from log. Returns 0 if no data."""
    best = 0
    for entry in log:
        if entry["date"] <= target_date:
            best = entry["options_value"]
        else:
            break
    return best

def load_historical_price_cache():
    path = DATA_DIR / "historical_prices_cache.json"
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}

def save_historical_price_cache(cache):
    ensure_data_dir()
    path = DATA_DIR / "historical_prices_cache.json"
    with open(path, "w") as f:
        json.dump(cache, f)

def get_holdings_on_date(target_date):
    """
    Reverse-walk from current positions using tax ledger to determine
    what was held on target_date. Returns {ticker: shares}.
    """
    holdings = {}
    # Start with current positions
    for e in store["equities"]:
        t = e["ticker"]
        if t not in _SKIP_TICKERS:
            holdings[t] = e["quantity"]
    for c in store["crypto"]:
        yf_t = _CRYPTO_YF_MAP.get(c.get("asset", ""))
        if yf_t:
            holdings[yf_t] = c["quantity"]

    # Reverse events that happen AFTER target_date
    for entry in store["tax_ledger"]:
        asset_type = entry.get("asset_type", "")
        if asset_type not in ("Equities", "Equity", "Crypto"):
            continue
        ticker = entry.get("ticker", "")
        qty = float(entry.get("quantity", 0) or 0)
        if not ticker or qty <= 0:
            continue
        # Map crypto tickers
        if asset_type == "Crypto" and ticker in _CRYPTO_YF_MAP:
            ticker = _CRYPTO_YF_MAP[ticker]

        # If bought AFTER target_date → undo (subtract)
        acq = entry.get("date_acquired", "")
        if acq and acq > target_date:
            holdings[ticker] = holdings.get(ticker, 0) - qty

        # If sold AFTER target_date → undo (add back)
        closed = entry.get("date_closed", "")
        if closed and closed > target_date:
            holdings[ticker] = holdings.get(ticker, 0) + qty

    return {t: q for t, q in holdings.items() if q > 0.001}

def _nearest_price(price_dict, target_date):
    """For weekends/holidays, find the nearest prior trading day's close."""
    if not price_dict:
        return None
    dates = sorted(d for d in price_dict if d != "_fetched" and d <= target_date)
    return price_dict[dates[-1]] if dates else None

def get_portfolio_history(period):
    """Reconstruct daily portfolio values from position history × historical prices."""
    if not HAS_YFINANCE:
        return {"data_points": [], "period": period, "error": "yfinance not available"}

    # "live" period: return only today's intraday log entries (granular 5-min snapshots)
    if period == "live":
        intraday = load_intraday_log()
        today_str = date.today().isoformat()
        today_entries = [e for e in intraday if e["ts"].startswith(today_str)]
        data_points = []
        for e in today_entries:
            data_points.append({
                "date": e["ts"],
                "portfolio_value": e["value"],
                "equities_value": e.get("eq", 0),
                "crypto_value": e.get("cr", 0),
                "options_value": e.get("opt", 0),
                "cash_balance": e.get("cash", 0),
            })
        if not data_points:
            # No intraday entries yet today — show current live value as single point
            eq_val = sum(e.get("market_value", 0) for e in store["equities"])
            opt_val = sum(o.get("total_value", 0) for o in store["options"])
            cr_val = sum(c.get("market_value", 0) for c in store["crypto"])
            cash_bal = float(store.get("cash_balance", 0) or 0)
            live_total = eq_val + opt_val + cr_val + cash_bal
            data_points.append({
                "date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "portfolio_value": round(live_total, 2),
                "equities_value": round(eq_val, 2),
                "crypto_value": round(cr_val, 2),
                "options_value": round(opt_val, 2),
                "cash_balance": round(cash_bal, 2),
            })
        start_val = data_points[0]["portfolio_value"] if data_points else 0
        end_val = data_points[-1]["portfolio_value"] if data_points else 0
        return {
            "data_points": data_points,
            "period": "live",
            "start_date": data_points[0]["date"] if data_points else today_str,
            "end_date": data_points[-1]["date"] if data_points else today_str,
            "start_value": start_val,
            "end_value": end_val,
            "change_dollar": round(end_val - start_val, 2) if len(data_points) >= 2 else 0,
            "change_pct": round(((end_val - start_val) / start_val) * 100, 2) if start_val > 0 and len(data_points) >= 2 else 0,
        }

    end_date = date.today()
    period_map = {
        "1d": 4, "1w": 10, "1m": 30, "3m": 90, "1y": 365,
    }
    if period == "ytd":
        start_date = date(end_date.year, 1, 1)
    elif period == "max":
        start_date = date(2025, 9, 1)
    else:
        start_date = end_date - timedelta(days=period_map.get(period, 30))

    start_str = start_date.isoformat()
    end_str = end_date.isoformat()

    # Pad lookback for short periods so we always capture Friday's close
    # on weekends (or Thursday for 3-day holiday weekends)
    fetch_start = start_date - timedelta(days=4) if period in ("1d", "1w") else start_date

    # Collect all tickers held at ANY point in range
    # Check at start, at each buy/sell within range, and current
    check_dates = {start_str, end_str}
    for entry in store["tax_ledger"]:
        if entry.get("asset_type", "") not in ("Equities", "Equity", "Crypto"):
            continue
        for dfield in ("date_acquired", "date_closed"):
            d = entry.get(dfield, "")
            if d and start_str <= d <= end_str:
                check_dates.add(d)

    all_tickers = set()
    for d in check_dates:
        all_tickers.update(get_holdings_on_date(d).keys())

    if not all_tickers:
        return {"data_points": [], "period": period}

    # Fetch historical prices (with disk cache)
    prices = load_historical_price_cache()
    yesterday = (end_date - timedelta(days=1)).isoformat()
    missing = [t for t in all_tickers
               if t not in prices or prices[t].get("_fetched", "") < yesterday]

    if missing:
        try:
            import yfinance as yf
            print(f"  Fetching historical prices for {len(missing)} tickers: {missing}")
            df = yf.download(
                missing,
                start=fetch_start.isoformat(),
                end=(end_date + timedelta(days=1)).isoformat(),
                group_by="ticker",
                threads=True,
                progress=False,
            )
            import pandas as pd
            for ticker in missing:
                try:
                    if len(missing) == 1:
                        tdf = df
                    else:
                        if ticker in df.columns.get_level_values(0):
                            tdf = df[ticker]
                        else:
                            continue
                    if tdf is not None and not tdf.empty:
                        td = {}
                        for idx, row in tdf.iterrows():
                            close = row.get("Close")
                            if close is not None and not pd.isna(close):
                                td[idx.strftime("%Y-%m-%d")] = round(float(close), 4)
                        if td:
                            td["_fetched"] = end_str
                            prices[ticker] = td
                except Exception as e:
                    print(f"  Price parse error for {ticker}: {e}")
            save_historical_price_cache(prices)
        except Exception as e:
            print(f"  Historical price fetch error: {e}")

    # Build date list (trading days where we have price data)
    all_price_dates = set()
    for ticker in all_tickers:
        if ticker in prices:
            all_price_dates.update(d for d in prices[ticker] if d != "_fetched")

    all_dates = sorted(d for d in all_price_dates if start_str <= d <= end_str)

    # If no dates in strict range (e.g., weekend for 1D), fall back to nearest
    # prior trading day from the padded fetch window as baseline
    if not all_dates:
        prior_dates = sorted(d for d in all_price_dates if d < start_str)
        if prior_dates:
            all_dates = [prior_dates[-1]]  # Last trading day before range

    # Reconstruct portfolio value for each date
    options_log = load_options_value_log()
    cash_bal = float(store.get("cash_balance", 0) or 0)
    data_points = []

    for d in all_dates:
        holdings = get_holdings_on_date(d)
        eq_val = 0
        crypto_val = 0
        for ticker, qty in holdings.items():
            p = (prices.get(ticker, {}).get(d) or _nearest_price(prices.get(ticker, {}), d))
            if p:
                val = qty * p
                if ticker.endswith("-USD"):
                    crypto_val += val
                else:
                    eq_val += val
        opt_val = _get_options_value_on_date(options_log, d)
        total = eq_val + crypto_val + opt_val + cash_bal
        data_points.append({
            "date": d,
            "portfolio_value": round(total, 2),
            "equities_value": round(eq_val, 2),
            "crypto_value": round(crypto_val, 2),
            "options_value": round(opt_val, 2),
            "cash_balance": round(cash_bal, 2),
        })

    # For 1D: merge intraday log entries for real-time chart movement
    if period == "1d":
        intraday = load_intraday_log()
        if intraday:
            # Keep the daily baseline (last pre-today data point) as first entry
            baseline = [dp for dp in data_points if dp["date"] < end_str]
            # Add intraday entries as timestamped data points
            intraday_points = []
            for e in intraday:
                intraday_points.append({
                    "date": e["ts"],
                    "portfolio_value": e["value"],
                    "equities_value": e.get("eq", 0),
                    "crypto_value": e.get("cr", 0),
                    "options_value": e.get("opt", 0),
                    "cash_balance": e.get("cash", 0),
                })
            if intraday_points:
                data_points = baseline + intraday_points
                # Skip appending live value — last intraday entry IS the live value
            else:
                # No intraday data yet — fall back to daily + live
                eq_val = sum(e.get("market_value", 0) for e in store["equities"])
                opt_val = sum(o.get("total_value", 0) for o in store["options"])
                cr_val = sum(c.get("market_value", 0) for c in store["crypto"])
                live_total = eq_val + opt_val + cr_val + cash_bal
                if data_points and data_points[-1]["date"] == end_str:
                    data_points[-1]["portfolio_value"] = round(live_total, 2)
                else:
                    data_points.append({"date": end_str, "portfolio_value": round(live_total, 2),
                        "equities_value": round(eq_val, 2), "crypto_value": round(cr_val, 2),
                        "options_value": round(opt_val, 2), "cash_balance": round(cash_bal, 2)})
        else:
            # No intraday log — append live value as before
            eq_val = sum(e.get("market_value", 0) for e in store["equities"])
            opt_val = sum(o.get("total_value", 0) for o in store["options"])
            cr_val = sum(c.get("market_value", 0) for c in store["crypto"])
            live_total = eq_val + opt_val + cr_val + cash_bal
            if data_points and data_points[-1]["date"] == end_str:
                data_points[-1]["portfolio_value"] = round(live_total, 2)
            else:
                data_points.append({"date": end_str, "portfolio_value": round(live_total, 2),
                    "equities_value": round(eq_val, 2), "crypto_value": round(cr_val, 2),
                    "options_value": round(opt_val, 2), "cash_balance": round(cash_bal, 2)})
    else:
        # Non-1D: add today's live value as the final point
        eq_val = sum(e.get("market_value", 0) for e in store["equities"])
        opt_val = sum(o.get("total_value", 0) for o in store["options"])
        cr_val = sum(c.get("market_value", 0) for c in store["crypto"])
        live_total = eq_val + opt_val + cr_val + cash_bal
        today_str = end_str
        if data_points and data_points[-1]["date"] == today_str:
            data_points[-1] = {
                "date": today_str,
                "portfolio_value": round(live_total, 2),
                "equities_value": round(eq_val, 2),
                "crypto_value": round(cr_val, 2),
                "options_value": round(opt_val, 2),
                "cash_balance": round(cash_bal, 2),
            }
        else:
            data_points.append({
                "date": today_str,
                "portfolio_value": round(live_total, 2),
                "equities_value": round(eq_val, 2),
                "crypto_value": round(cr_val, 2),
                "options_value": round(opt_val, 2),
                "cash_balance": round(cash_bal, 2),
            })

    start_val = data_points[0]["portfolio_value"] if data_points else 0
    end_val = data_points[-1]["portfolio_value"] if data_points else 0
    return {
        "data_points": data_points,
        "period": period,
        "start_date": data_points[0]["date"] if data_points else start_str,
        "end_date": end_str,
        "start_value": start_val,
        "end_value": end_val,
        "change_dollar": round(end_val - start_val, 2) if len(data_points) >= 2 else 0,
        "change_pct": round(((end_val - start_val) / start_val) * 100, 2) if start_val > 0 and len(data_points) >= 2 else 0,
    }

def seed_options_log_from_snapshots():
    """One-time migration: extract validated options_value from existing snapshots."""
    log = load_options_value_log()
    if log:
        return 0  # Already seeded
    snapshots = load_snapshots()
    if not snapshots:
        return 0
    prev_val = None
    seen_dates = set()
    for snap in snapshots:
        opt_val = snap.get("options_value", 0)
        d = snap.get("date", "")
        if opt_val <= 0 or not d:
            continue
        # Skip >50% drops (deploy artifacts)
        if prev_val and opt_val < prev_val * 0.5:
            continue
        # Keep last value per day
        seen_dates.add(d)
        # Update — later snapshots for same day overwrite
        log = [e for e in log if e["date"] != d]
        log.append({"date": d, "options_value": round(opt_val, 2)})
        prev_val = opt_val
    log.sort(key=lambda e: e["date"])
    save_options_value_log(log)
    print(f"  Seeded options value log: {len(log)} days from snapshots")
    return len(log)


# ── Main ─────────────────────────────────────────────────────────────────
def seed_demo_data():
    """Copy demo dataset into the live data directory on first boot."""
    demo_dir = BACKEND_DIR / "demo_data"
    if not demo_dir.exists():
        print("  No demo_data/ directory found, starting empty.")
        return
    print("  Seeding demo data into data directory...")
    for fname in demo_dir.glob("*.json"):
        key = fname.stem
        if key in store:
            with open(fname) as f:
                store[key] = json.load(f)
    # Also copy supplementary files
    for fname in ["watchlist.json", "cash_flow.json", "sales.json", "box_purchases.json"]:
        src = demo_dir / fname
        if src.exists():
            import shutil
            shutil.copy2(src, DATA_DIR / fname)
    save_store()
    print(f"  Demo data loaded: {len(store['equities'])} equities, {len(store['options'])} options, {len(store['crypto'])} crypto")


def main():
    # Load data
    json_exists = (DATA_DIR / "equities.json").exists()
    if json_exists:
        print("Loading from JSON cache...")
        load_store()
    else:
        load_from_excel()
        # If Excel import didn't populate anything, seed from demo data
        if not store["equities"]:
            seed_demo_data()

    # One-time migration: backfill cash_balance on historical snapshots
    snapshots = load_snapshots()
    migrated = False
    for s in snapshots:
        if "cash_balance" not in s:
            # Post-sell snapshots on 2026-04-01: equities dropped from ~$96K to ~$89K
            if s.get("date") == "2026-04-01" and (s.get("equities_value", 100000) or 100000) < 92000:
                s["cash_balance"] = 7505.0
            else:
                s["cash_balance"] = 0.0
            migrated = True
    if migrated:
        path = DATA_DIR / "snapshots.json"
        with open(path, "w") as f:
            json.dump(snapshots, f, indent=2)
        print(f"  Migrated {sum(1 for s in snapshots if True)} snapshots with cash_balance backfill")

    # One-time migration: RKLB roll history delta ($4,730)
    # The roll backfill changed options_value by -$4,730 (old CC P/L was +$450, new all-in is -$4,280)
    # All snapshots before the roll update overstate options_value/net_worth by $4,730
    RKLB_DELTA = 4730
    rklb_migrated = 0
    for s in snapshots:
        if s.get("_rklb_roll_normalized"):
            continue
        # The roll update deployed on 2026-04-01; snapshots after the roll show lower options_value
        # Pre-roll snapshots have options_value around $3,200-$3,900 (current leg only)
        # Post-roll snapshots have options_value around -$1,500 (all-in with roll costs)
        opt_val = s.get("options_value", 0) or 0
        if opt_val > 0:  # Pre-roll: positive options value means it hasn't been corrected
            s["options_value"] = round(opt_val - RKLB_DELTA, 2)
            s["market_value"] = round((s.get("market_value", 0) or 0) - RKLB_DELTA, 2)
            s["net_worth"] = round((s.get("net_worth", 0) or 0) - RKLB_DELTA, 2)
            total_inv = s.get("total_invested", 0) or 0
            s["pl_dollar"] = round(s["market_value"] - total_inv, 2) if total_inv else s.get("pl_dollar", 0)
            s["pl_percent"] = round(s["pl_dollar"] / total_inv * 100, 4) if total_inv > 0 else 0
            s["_rklb_roll_normalized"] = True
            rklb_migrated += 1
    if rklb_migrated > 0:
        path = DATA_DIR / "snapshots.json"
        with open(path, "w") as f:
            json.dump(snapshots, f, indent=2)
        print(f"  Normalized {rklb_migrated} snapshots for RKLB roll delta (-${RKLB_DELTA})")

    # One-time migration: CC P/L fix for pre-March-30 snapshots
    # The March 29/30 CC P/L overhaul changed how options were valued
    # Pre-fix options_value was ~$27K (included stock value of CCs), post-fix ~$4K
    # After RKLB roll normalization (-$4,730), pre-fix snapshots still have options_value ~$22K
    # Need to subtract remaining delta to bring them to correct ~$4K range
    CC_PL_DELTA = 18761  # 23491 total delta minus 4730 already subtracted by RKLB migration
    cc_pl_fixed = 0
    for s in snapshots:
        if s.get("_cc_pl_normalized"):
            continue
        opt_val = s.get("options_value", 0) or 0
        if opt_val > 15000:  # Still in the inflated range
            corrected = max(0, round(opt_val - CC_PL_DELTA, 2))
            delta = opt_val - corrected
            s["options_value"] = corrected
            s["_cc_pl_normalized"] = True
            cc_pl_fixed += 1
    if cc_pl_fixed > 0:
        path = DATA_DIR / "snapshots.json"
        with open(path, "w") as f:
            json.dump(snapshots, f, indent=2)
        print(f"  Normalized {cc_pl_fixed} snapshots for CC P/L fix (-${CC_PL_DELTA})")

    # One-time migration: fix 2026-04-01 snapshot net_worth from components
    # Some snapshots had net_worth computed without cash_balance or with realized_cc
    nw_fixed = 0
    for s in snapshots:
        eq = s.get("equities_value", 0) or 0
        op = s.get("options_value", 0) or 0
        cr = s.get("crypto_value", 0) or 0
        cb = s.get("cash_balance", 0) or 0
        correct_nw = round(eq + op + cr + cb, 2)
        if abs((s.get("net_worth", 0) or 0) - correct_nw) > 1:
            s["net_worth"] = correct_nw
            s["market_value"] = round(eq + op + cr, 2)
            total_inv = s.get("total_invested", 0) or 0
            s["pl_dollar"] = round(s["market_value"] - total_inv, 2)
            s["pl_percent"] = round(s["pl_dollar"] / total_inv * 100, 4) if total_inv > 0 else 0
            nw_fixed += 1
    if nw_fixed > 0:
        path = DATA_DIR / "snapshots.json"
        with open(path, "w") as f:
            json.dump(snapshots, f, indent=2)
        print(f"  Fixed net_worth on {nw_fixed} snapshots (eq+opt+cr+cash formula)")

    # One-time migration: delete corrupted snapshots from RKLB roll deploy (2026-04-02 01:10 to 02:18)
    pre_count = len(snapshots)
    if not any(s.get("_apr2_deploy_cleaned") for s in snapshots[:1]):
        snapshots = [s for s in snapshots if not (
            (s.get("timestamp", "") or "").startswith("2026-04-02") and
            "2026-04-02 01:10" <= (s.get("timestamp", "") or "") <= "2026-04-02 02:18"
        )]
        removed = pre_count - len(snapshots)
        if removed > 0:
            if snapshots:
                snapshots[0]["_apr2_deploy_cleaned"] = True
            path = DATA_DIR / "snapshots.json"
            with open(path, "w") as f:
                json.dump(snapshots, f, indent=2)
            print(f"  Deleted {removed} corrupted snapshots from 2026-04-02 deploy window")

    # One-time migration v3: fix bad options_value in known deploy windows
    if not any(s.get("_flag_deploy_snapshot_cleanup_v3") for s in snapshots):
        v3_fixed = 0

        # Window 1: Apr 2 01:08-02:20 where options_value is outside $5K-$8K
        for s in snapshots:
            ts = s.get("timestamp", "") or ""
            if ts >= "2026-04-02 01:08" and ts <= "2026-04-02 02:20":
                opt_val = s.get("options_value", 0) or 0
                if opt_val < 5000 or opt_val > 8000:
                    s["options_value"] = 6257
                    eq = s.get("equities_value", 0) or 0
                    cr = s.get("crypto_value", 0) or 0
                    cb = s.get("cash_balance", 0) or 0
                    s["net_worth"] = round(eq + 6257 + cr + cb, 2)
                    s["market_value"] = round(eq + 6257 + cr, 2)
                    total_inv = s.get("total_invested", 0) or 0
                    s["pl_dollar"] = round(s["market_value"] - total_inv, 2)
                    s["pl_percent"] = round(s["pl_dollar"] / total_inv * 100, 4) if total_inv > 0 else 0
                    v3_fixed += 1

        # Window 2: Mar 29-30 where options_value dropped below $5K (cliff)
        # Find the last clean value before the drop
        last_clean_opt = None
        for s in snapshots:
            d = s.get("date", "") or ""
            opt_val = s.get("options_value", 0) or 0
            if d < "2026-03-29":
                if opt_val >= 5000:
                    last_clean_opt = opt_val
            elif d <= "2026-03-30":
                if opt_val < 5000 and last_clean_opt is not None:
                    s["options_value"] = last_clean_opt
                    eq = s.get("equities_value", 0) or 0
                    cr = s.get("crypto_value", 0) or 0
                    cb = s.get("cash_balance", 0) or 0
                    s["net_worth"] = round(eq + last_clean_opt + cr + cb, 2)
                    s["market_value"] = round(eq + last_clean_opt + cr, 2)
                    total_inv = s.get("total_invested", 0) or 0
                    s["pl_dollar"] = round(s["market_value"] - total_inv, 2)
                    s["pl_percent"] = round(s["pl_dollar"] / total_inv * 100, 4) if total_inv > 0 else 0
                    v3_fixed += 1

        # Window 3: Apr 2 after ~02:25 (second deploy) where options_value < $3K
        for s in snapshots:
            ts = s.get("timestamp", "") or ""
            if ts >= "2026-04-02 02:25":
                opt_val = s.get("options_value", 0) or 0
                if opt_val < 3000:
                    s["options_value"] = 6257
                    eq = s.get("equities_value", 0) or 0
                    cr = s.get("crypto_value", 0) or 0
                    cb = s.get("cash_balance", 0) or 0
                    s["net_worth"] = round(eq + 6257 + cr + cb, 2)
                    s["market_value"] = round(eq + 6257 + cr, 2)
                    total_inv = s.get("total_invested", 0) or 0
                    s["pl_dollar"] = round(s["market_value"] - total_inv, 2)
                    s["pl_percent"] = round(s["pl_dollar"] / total_inv * 100, 4) if total_inv > 0 else 0
                    v3_fixed += 1

        if v3_fixed > 0:
            if snapshots:
                snapshots[0]["_flag_deploy_snapshot_cleanup_v3"] = True
            path = DATA_DIR / "snapshots.json"
            with open(path, "w") as f:
                json.dump(snapshots, f, indent=2)
            print(f"  Migration v3: fixed options_value on {v3_fixed} bad snapshots in known deploy windows")
        else:
            # Mark complete even if nothing to fix
            if snapshots:
                snapshots[0]["_flag_deploy_snapshot_cleanup_v3"] = True
                path = DATA_DIR / "snapshots.json"
                with open(path, "w") as f:
                    json.dump(snapshots, f, indent=2)

    # One-time migration v4: fix bad snapshots from 2026-04-02 03:27+ where options_value dropped to $1,527
    # Caused by _mark_cache_warm being set True during load_store() before refresh completed
    if not any(s.get("_flag_deploy_snapshot_cleanup_v4") for s in snapshots):
        v4_fixed = 0
        for s in snapshots:
            ts = s.get("timestamp", "") or ""
            if ts >= "2026-04-02 03:27":
                opt_val = s.get("options_value", 0) or 0
                if opt_val < 3000:
                    s["options_value"] = 6257
                    eq = s.get("equities_value", 0) or 0
                    cr = s.get("crypto_value", 0) or 0
                    cb = s.get("cash_balance", 0) or 0
                    s["net_worth"] = round(eq + 6257 + cr + cb, 2)
                    s["market_value"] = round(eq + 6257 + cr, 2)
                    total_inv = s.get("total_invested", 0) or 0
                    s["pl_dollar"] = round(s["market_value"] - total_inv, 2)
                    s["pl_percent"] = round(s["pl_dollar"] / total_inv * 100, 4) if total_inv > 0 else 0
                    v4_fixed += 1
        if snapshots:
            snapshots[0]["_flag_deploy_snapshot_cleanup_v4"] = True
        path = DATA_DIR / "snapshots.json"
        with open(path, "w") as f:
            json.dump(snapshots, f, indent=2)
        if v4_fixed > 0:
            print(f"  Migration v4: fixed options_value on {v4_fixed} bad snapshots from 2026-04-02 03:27+")
        else:
            print("  Migration v4: no bad snapshots found (already clean)")

    # v5 already ran (flag set, 0 fixes due to bug — no pre-Mar-27 snapshots exist)

    # One-time migration v6: hardcoded fix for Mar 31 deploy cliff + any Apr 2 artifacts
    # Mar 27-30 snapshots show $8,850 (correct). Mar 31 00:01-13:26 show $4,120 (deploy artifact).
    # Mar 31 13:31-13:59 show $3,124 (second drop). Mar 31 14:00+ are live market data (keep as-is).
    if not any(s.get("_flag_deploy_snapshot_cleanup_v6") for s in snapshots):
        v6_fixed = 0
        CLEAN_OPT = 8850  # Matches the Mar 27-30 options_value

        for s in snapshots:
            d = s.get("date", "") or ""
            ts = s.get("timestamp", "") or ""
            opt_val = s.get("options_value", 0) or 0

            should_fix = False
            replacement = CLEAN_OPT

            # Mar 31 overnight deploy artifacts (00:01 through 13:59) — options < $8,000
            if d == "2026-03-31" and ts < "2026-03-31 14:00":
                if opt_val < 8000:
                    should_fix = True

            # Apr 2 deploy artifacts — options < $3,000
            if d == "2026-04-02" and opt_val < 3000:
                replacement = 6257
                should_fix = True

            if should_fix:
                s["options_value"] = replacement
                eq = s.get("equities_value", 0) or 0
                cr = s.get("crypto_value", 0) or 0
                cb = s.get("cash_balance", 0) or 0
                s["net_worth"] = round(eq + replacement + cr + cb, 2)
                s["market_value"] = round(eq + replacement + cr, 2)
                total_inv = s.get("total_invested", 0) or 0
                s["pl_dollar"] = round(s["market_value"] - total_inv, 2)
                s["pl_percent"] = round(s["pl_dollar"] / total_inv * 100, 4) if total_inv > 0 else 0
                v6_fixed += 1

        if snapshots:
            snapshots[0]["_flag_deploy_snapshot_cleanup_v6"] = True
        path = DATA_DIR / "snapshots.json"
        with open(path, "w") as f:
            json.dump(snapshots, f, indent=2)
        if v6_fixed > 0:
            print(f"  Migration v6: fixed {v6_fixed} bad snapshots (Mar 31 cliff → $8,850, Apr 2 → $6,257)")
        else:
            print("  Migration v6: no bad snapshots found (already clean)")

    # Migrate cards: add status + item_type fields if missing
    for card in store["cards"]:
        if "status" not in card:
            card["status"] = "collection"
        if "item_type" not in card:
            card["item_type"] = "card"

    # One-time fix: CC backfill and eBay seed ran multiple times because flags
    # weren't in the initial store dict (load_store skipped them). Fix cash + dedup sales.
    if not store.get("_migration_dedup_fix"):
        # Fix cash_balance: should be $5,632.70 ($5,852.70 original - $220 CC backfill)
        # It's currently $4,312.70 because backfill ran ~7 times
        current_cash = float(store.get("cash_balance", 0) or 0)
        if abs(current_cash - 4312.70) < 1:  # Only fix if it's at the known bad value
            store["cash_balance"] = 5632.70
            print(f"  Fixed cash_balance: ${current_cash} → $5,632.70 (undid repeated CC backfill)")
        # Deduplicate sales: keep only one of each unique item_name + date_sold combo
        seen = set()
        deduped = []
        for s in store["sales"]:
            key = (s.get("item_name", ""), s.get("date_sold", ""))
            if key not in seen:
                seen.add(key)
                deduped.append(s)
        removed = len(store["sales"]) - len(deduped)
        if removed > 0:
            # Reassign IDs
            for i, s in enumerate(deduped, 1):
                s["id"] = i
            store["sales"] = deduped
            print(f"  Deduped sales: removed {removed} duplicates, {len(deduped)} remaining")
        # Mark both backfill and seed as done (they already ran, just need the flags)
        store["_cc_cash_backfilled"] = True
        store["_ebay_sales_seeded"] = True
        store["_migration_dedup_fix"] = True

    # One-time: revert Joan Beringer sale to "sold" (not yet shipped)
    if not store.get("_beringer_reverted"):
        for s in store["sales"]:
            if "Beringer" in s.get("item_name", "") or "Joan" in s.get("item_name", ""):
                s["status"] = "sold"
                s["date_shipped"] = None
                s["date_completed"] = None
                print(f"  Reverted '{s['item_name']}' to status=sold (not shipped)")
                break
        store["_beringer_reverted"] = True

    # One-time: clear intraday log contaminated by bugged cash_balance values
    if not store.get("_intraday_log_cleared"):
        intraday_path = DATA_DIR / "intraday_log.json"
        if intraday_path.exists():
            with open(intraday_path, "w") as f:
                json.dump([], f)
            print("  Cleared intraday_log.json (contained entries with bugged cash_balance)")
        store["_intraday_log_cleared"] = True

    # One-time migration: backfill CC premium into cash_balance
    # Open CCs have premium NOT yet reflected in cash. Add net CC cash
    # (premium received - roll costs paid) for open positions.
    if not store.get("_cc_cash_backfilled"):
        cc_cash_delta = 0
        for cc in store["cc_income"]:
            if not cc.get("date_closed"):
                prem = float(cc.get("premium_collected", 0) or 0)
                ctc = float(cc.get("cost_to_close", 0) or 0)  # Closed roll costs already paid
                cc_cash_delta += prem - ctc
        if cc_cash_delta != 0:
            store["cash_balance"] = round(float(store.get("cash_balance", 0) or 0) + cc_cash_delta, 2)
            print(f"  CC cash backfill: ${cc_cash_delta:+,.2f} added to cash_balance (open CC premiums - roll costs)")
        store["_cc_cash_backfilled"] = True

    # Seed eBay sales data (one-time)
    if not store.get("_ebay_sales_seeded"):
        seed_sales = [
            {"item_name": "Joan Beringer 2023 Bowman Chrome Auto /25", "category": "cards", "platform": "ebay",
             "sale_price": 210.00, "platform_fees": 27.83, "shipping_cost": 0, "purchase_price": 0,
             "status": "sold", "date_sold": "2026-04-05", "requires_authentication": True},
            {"item_name": "Kith x Sesame Street Hoodie", "category": "clothing", "platform": "ebay",
             "sale_price": 146.81, "platform_fees": 19.45, "shipping_cost": 11.81, "purchase_price": 0,
             "status": "completed", "date_sold": "2026-02-12", "date_shipped": "2026-02-13", "date_completed": "2026-02-18"},
            {"item_name": "Jordan 1 Retro High OG Melody Ehsani", "category": "sneakers", "platform": "ebay",
             "sale_price": 140.00, "platform_fees": 18.55, "shipping_cost": 12.21, "purchase_price": 0,
             "status": "completed", "date_sold": "2026-01-27", "date_shipped": "2026-01-28", "date_completed": "2026-02-03",
             "requires_authentication": True},
            {"item_name": "Nike Dunk High Game Royal", "category": "sneakers", "platform": "ebay",
             "sale_price": 85.00, "platform_fees": 11.26, "shipping_cost": 7.77, "purchase_price": 0,
             "status": "completed", "date_sold": "2026-01-17", "date_shipped": "2026-01-18", "date_completed": "2026-01-24"},
            {"item_name": "Kith Crewneck Sweatshirt", "category": "clothing", "platform": "ebay",
             "sale_price": 162.21, "platform_fees": 21.49, "shipping_cost": 0, "purchase_price": 0,
             "status": "completed", "date_sold": "2026-01-13", "date_shipped": "2026-01-14", "date_completed": "2026-01-20"},
            {"item_name": "Kith Palette Bears Crewneck", "category": "clothing", "platform": "ebay",
             "sale_price": 153.00, "platform_fees": 20.27, "shipping_cost": 0, "purchase_price": 0,
             "status": "completed", "date_sold": "2026-01-05", "date_shipped": "2026-01-06", "date_completed": "2026-01-12"},
        ]
        next_id = max((s["id"] for s in store["sales"]), default=0) + 1
        for s in seed_sales:
            sp = float(s.get("sale_price", 0))
            fees = float(s.get("platform_fees", 0))
            ship = float(s.get("shipping_cost", 0))
            cost = float(s.get("purchase_price", 0))
            s["id"] = next_id
            s["net_profit"] = round(sp - fees - ship - cost, 2)
            store["sales"].append(s)
            next_id += 1
        # Business expenses: store subscriptions + shipping labels already in shipping_cost above
        # Add store subscriptions as expense entries
        for month in ("2026-01-01", "2026-02-01", "2026-03-01", "2026-04-01"):
            store["sales"].append({
                "id": next_id, "item_name": "eBay Store Subscription", "category": "other",
                "platform": "ebay", "sale_price": 0, "platform_fees": 0, "shipping_cost": 0,
                "business_expense": 4.95, "status": "completed", "date_sold": month,
                "date_completed": month, "net_profit": -4.95,
            })
            next_id += 1
        store["_ebay_sales_seeded"] = True
        print(f"  Seeded {len(seed_sales)} eBay sales + 4 store subscriptions")

    # One-time migration: add origin/date_moved fields, fix non-card collection items
    if not store.get("_cards_origin_migration"):
        migrated = 0
        for card in store["cards"]:
            if "origin" not in card:
                card["origin"] = None
            if "date_moved" not in card:
                card["date_moved"] = None
            # Non-card items with status "collection" → "available"
            if (card.get("item_type", "card") != "card") and card.get("status") == "collection":
                card["status"] = "available"
                migrated += 1
        store["_cards_origin_migration"] = True
        print(f"  Cards origin migration: added origin/date_moved to all items, moved {migrated} non-card items to 'available'")

    save_store()

    # Seed options value log from existing snapshots (one-time migration)
    seed_options_log_from_snapshots()

    # Auto-refresh live prices in background so server starts immediately
    if HAS_YFINANCE:
        def _bg_refresh():
            print("\nFetching live prices in background...")
            try:
                refresh_all_prices()
                print("Live prices loaded!\n")
            except Exception as e:
                print(f"Price refresh failed: {e}\n")
        import threading
        threading.Thread(target=_bg_refresh, daemon=True).start()
    else:
        print("\nyfinance not installed — using cached prices from Excel\n")

    # Calculate initial summary with cards
    card_val = sum(c["est_value"] for c in store["cards"])
    market_val = store["summary"].get("market_value", 0)
    store["summary"]["card_value"] = card_val
    store["summary"]["net_worth"] = market_val  # Cards excluded until collection is fully built out

    # Tax summary
    tax_sum = get_tax_summary()
    store["summary"]["realized_pl"] = tax_sum["total_realized_pl"]
    store["summary"]["tax_reserve"] = tax_sum["total_tax_reserve"]

    print(f"\n{'='*60}")
    print(f"  WheelHouse")
    print(f"  Portfolio: {len(store['equities'])} equities | {len(store['options'])} options | {len(store['crypto'])} crypto | {len(store['cards'])} cards")
    print(f"  Market Value: ${market_val:,.2f}")
    print(f"  Card Collection: ${card_val:,.2f}")
    print(f"  Net Worth: ${market_val + card_val:,.2f}")
    print(f"{'='*60}")
    print(f"  Server: http://localhost:{PORT}")
    print(f"  API:    http://localhost:{PORT}/api/health")
    print(f"{'='*60}\n")

    server = ThreadedHTTPServer(("0.0.0.0", PORT), CommandCenterHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.server_close()


if __name__ == "__main__":
    main()
