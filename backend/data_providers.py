"""
Data providers for Dark Pool, Insider Trade, and Card Valuation data.
Provider pattern allows swapping data sources (e.g., FINRA → Quiver Quant).
"""

import json
import time
import math
import requests
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────
import os
DATA_DIR = Path("/data") if os.path.isdir("/data") else Path(__file__).parent / "data"
USER_AGENT = "WheelHouse Portfolio Command Center"


# ═══════════════════════════════════════════════════════════════════════
# Abstract Base Classes
# ═══════════════════════════════════════════════════════════════════════

class DarkPoolProvider(ABC):
    @abstractmethod
    def get_daily_data(self, ticker, days=30):
        """Return list of dicts: [{date, short_volume, short_exempt, total_volume, short_ratio}, ...]"""
        pass

    @abstractmethod
    def get_latest(self, ticker):
        """Return single dict with latest day's data."""
        pass


class InsiderTradeProvider(ABC):
    @abstractmethod
    def get_recent_trades(self, ticker, days=90):
        """Return list of insider trade dicts for a single ticker."""
        pass

    @abstractmethod
    def get_bulk_recent(self, tickers, days=30):
        """Return dict of {ticker: [trades]} for multiple tickers."""
        pass


# ═══════════════════════════════════════════════════════════════════════
# FINRA Dark Pool Provider
# ═══════════════════════════════════════════════════════════════════════

class FINRADarkPoolProvider(DarkPoolProvider):
    CACHE_FILE = "darkpool_cache.json"
    BASE_URL = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date}.txt"

    _MEM_CACHE_MAX_DAYS = 10  # Only keep 10 most recent days in memory (vs 45 on disk)

    def __init__(self):
        # Lazy load: only load metadata, not full data. Days loaded on demand.
        self._cache = {"last_fetch": None, "data": {}}
        self._disk_loaded = False

    def _cache_path(self):
        return DATA_DIR / self.CACHE_FILE

    def _load_disk_metadata(self):
        """Load only the last_fetch timestamp from disk, not the data."""
        if self._disk_loaded:
            return
        path = self._cache_path()
        if path.exists():
            try:
                with open(path) as f:
                    disk = json.load(f)
                self._cache["last_fetch"] = disk.get("last_fetch")
                self._disk_loaded = True
            except (json.JSONDecodeError, IOError):
                self._disk_loaded = True

    def _load_day_from_disk(self, date_str):
        """Load a single day's data from disk cache on demand."""
        if date_str in self._cache["data"]:
            return self._cache["data"][date_str]
        path = self._cache_path()
        if path.exists():
            try:
                with open(path) as f:
                    disk = json.load(f)
                day_data = disk.get("data", {}).get(date_str)
                if day_data:
                    self._cache["data"][date_str] = day_data
                    self._trim_memory_cache()
                    return day_data
            except (json.JSONDecodeError, IOError):
                pass
        return None

    def _trim_memory_cache(self):
        """Keep only the most recent N days in memory."""
        data = self._cache.get("data", {})
        if len(data) > self._MEM_CACHE_MAX_DAYS:
            sorted_dates = sorted(data.keys())
            for old in sorted_dates[:-self._MEM_CACHE_MAX_DAYS]:
                del data[old]

    def _load_cache(self):
        """Legacy compat — returns the in-memory cache (lazy, not full disk load)."""
        self._load_disk_metadata()
        return self._cache

    def _save_cache(self):
        """Save to disk — merge in-memory days with existing disk data."""
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        path = self._cache_path()
        # Load existing disk data to merge (don't lose days evicted from memory)
        disk_data = {}
        if path.exists():
            try:
                with open(path) as f:
                    disk_data = json.load(f).get("data", {})
            except (json.JSONDecodeError, IOError):
                pass
        # Merge memory into disk
        disk_data.update(self._cache.get("data", {}))
        # Prune disk to 45 days
        all_dates = sorted(disk_data.keys())
        if len(all_dates) > 45:
            for old in all_dates[:-45]:
                del disk_data[old]
        save_obj = {"last_fetch": self._cache.get("last_fetch"), "data": disk_data}
        with open(path, "w") as f:
            json.dump(save_obj, f, default=str)

    def _get_trading_days(self, days=30):
        """Get list of recent trading day date strings (YYYYMMDD), skipping weekends."""
        dates = []
        d = datetime.now()
        while len(dates) < days:
            d -= timedelta(days=1)
            if d.weekday() < 5:  # Mon-Fri
                dates.append(d.strftime("%Y%m%d"))
        return dates

    def _fetch_day(self, date_str):
        """Fetch and parse one day's FINRA short volume file. Returns {ticker: row_dict}."""
        # Check if already in memory
        if date_str in self._cache.get("data", {}):
            return self._cache["data"][date_str]
        # Check disk
        disk_data = self._load_day_from_disk(date_str)
        if disk_data:
            return disk_data

        url = self.BASE_URL.format(date=date_str)
        try:
            resp = requests.get(url, timeout=20, headers={"User-Agent": USER_AGENT})
            if resp.status_code != 200:
                return None
        except requests.RequestException:
            return None

        day_data = {}
        for line in resp.text.strip().split("\n")[1:]:  # skip header
            parts = line.split("|")
            if len(parts) >= 5:
                symbol = parts[1].strip()
                try:
                    short_vol = float(parts[2])
                    short_exempt = float(parts[3])
                    total_vol = float(parts[4])
                    short_ratio = (short_vol / total_vol * 100) if total_vol > 0 else 0
                    day_data[symbol] = {
                        "date": f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}",
                        "short_volume": int(short_vol),
                        "short_exempt": int(short_exempt),
                        "total_volume": int(total_vol),
                        "short_ratio": round(short_ratio, 2),
                    }
                except (ValueError, ZeroDivisionError):
                    continue

        # Cache this day
        if day_data:
            if "data" not in self._cache:
                self._cache["data"] = {}
            self._cache["data"][date_str] = day_data
            self._trim_memory_cache()
            self._cache["last_fetch"] = datetime.now().isoformat()
            self._save_cache()

        return day_data

    def _should_refresh(self):
        """Check if we should re-fetch today's data."""
        self._load_disk_metadata()
        last = self._cache.get("last_fetch")
        if not last:
            return True
        try:
            last_dt = datetime.fromisoformat(last)
            # Refresh if last fetch was over 6 hours ago
            return (datetime.now() - last_dt).total_seconds() > 21600
        except (ValueError, TypeError):
            return True

    def refresh(self, days=30, max_fetches=10):
        """Fetch recent trading days' data."""
        dates = self._get_trading_days(days)
        fetched = 0
        for date_str in dates:
            if date_str not in self._cache.get("data", {}):
                result = self._fetch_day(date_str)
                if result:
                    fetched += 1
                if fetched >= max_fetches:
                    break
                time.sleep(0.2)  # polite rate limiting
        return fetched

    def get_daily_data(self, ticker, days=30):
        ticker = ticker.upper()
        # Ensure we have recent data
        if self._should_refresh():
            self.refresh(days)

        results = []
        dates = self._get_trading_days(days)
        for date_str in dates:
            # Try memory first, then disk
            day = self._cache.get("data", {}).get(date_str)
            if day is None:
                day = self._load_day_from_disk(date_str) or {}
            if ticker in day:
                results.append(day[ticker])

        # Sort oldest to newest
        results.sort(key=lambda x: x["date"])
        return results

    def get_latest(self, ticker):
        data = self.get_daily_data(ticker, days=5)
        return data[-1] if data else None

    def get_portfolio_latest(self, tickers):
        """Get latest dark pool data for a list of tickers. Fast — only fetches 1-2 days."""
        results = {}
        dates = self._get_trading_days(3)

        # Only fetch days we don't already have (max 2 fetches to stay under timeout)
        fetched = 0
        for date_str in dates:
            if date_str in self._cache.get("data", {}) or self._load_day_from_disk(date_str):
                continue
            if fetched >= 2:
                break
            self._fetch_day(date_str)
            fetched += 1

        # Find latest available data for each ticker
        for ticker in tickers:
            ticker = ticker.upper()
            for date_str in dates:
                day = self._cache.get("data", {}).get(date_str) or self._load_day_from_disk(date_str) or {}
                if ticker in day:
                    results[ticker] = {**day[ticker]}  # copy to avoid mutating cache
                    # Sparkline from whatever is already cached (no new fetches)
                    history = []
                    for d in self._get_trading_days(10):
                        cached_day = self._cache.get("data", {}).get(d) or self._load_day_from_disk(d) or {}
                        if ticker in cached_day:
                            history.append({**cached_day[ticker]})
                    history.sort(key=lambda x: x["date"])
                    results[ticker]["history"] = history[-7:]
                    break
        return results


# ═══════════════════════════════════════════════════════════════════════
# OpenInsider Provider (uses existing scraping logic)
# ═══════════════════════════════════════════════════════════════════════

class OpenInsiderProvider(InsiderTradeProvider):
    CACHE_FILE = "insider_cache.json"
    CACHE_TTL = 14400  # 4 hours

    def __init__(self):
        self._cache = self._load_cache()

    def _cache_path(self):
        return DATA_DIR / self.CACHE_FILE

    def _load_cache(self):
        path = self._cache_path()
        if path.exists():
            try:
                with open(path) as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return {"last_fetch": {}, "data": {}}

    def _save_cache(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(self._cache_path(), "w") as f:
            json.dump(self._cache, f, default=str)

    def _is_stale(self, ticker):
        last = self._cache.get("last_fetch", {}).get(ticker)
        if not last:
            return True
        try:
            return (datetime.now() - datetime.fromisoformat(last)).total_seconds() > self.CACHE_TTL
        except (ValueError, TypeError):
            return True

    def _scrape_ticker(self, ticker):
        """Scrape OpenInsider for a single ticker."""
        try:
            from bs4 import BeautifulSoup
            url = f"http://openinsider.com/screener?s={ticker}&o=&pl=&ph=&st=0&lt=0&lk=&pp=&sp=&lp="
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            resp = requests.get(url, headers=headers, timeout=15)
            soup = BeautifulSoup(resp.text, "lxml")
            table = soup.find("table", {"class": "tinytable"})
            if not table:
                return []

            rows = table.find_all("tr")[1:]
            trades = []
            for row in rows[:25]:
                cols = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cols) >= 12:
                    trade_type = cols[6]
                    is_purchase = "Purchase" in trade_type or "Buy" in trade_type
                    trades.append({
                        "filing_date": cols[1],
                        "trade_date": cols[2],
                        "ticker": cols[3],
                        "insider_name": cols[4],
                        "title": cols[5],
                        "trade_type": trade_type,
                        "price": cols[7],
                        "qty": cols[8],
                        "owned": cols[9],
                        "delta_own": cols[10],
                        "value": cols[11],
                        "is_purchase": is_purchase,
                    })
            return trades
        except Exception as e:
            print(f"OpenInsider scrape error for {ticker}: {e}")
            return []

    def get_recent_trades(self, ticker, days=90):
        ticker = ticker.upper()
        if not self._is_stale(ticker):
            return self._cache.get("data", {}).get(ticker, [])

        trades = self._scrape_ticker(ticker)
        if "data" not in self._cache:
            self._cache["data"] = {}
        if "last_fetch" not in self._cache:
            self._cache["last_fetch"] = {}
        self._cache["data"][ticker] = trades
        self._cache["last_fetch"][ticker] = datetime.now().isoformat()
        self._save_cache()
        return trades

    def get_bulk_recent(self, tickers, days=30):
        results = {}
        for ticker in tickers:
            results[ticker] = self.get_recent_trades(ticker, days)
            time.sleep(0.5)  # rate limit OpenInsider
        return results


# ═══════════════════════════════════════════════════════════════════════
# EDGAR Insider Provider (edgartools - alternative, slower)
# ═══════════════════════════════════════════════════════════════════════

class EDGARInsiderProvider(InsiderTradeProvider):
    """SEC EDGAR Form 4 provider via edgartools. Slower but more authoritative."""

    def __init__(self):
        try:
            from edgar import set_identity
            set_identity("WheelHouse User")
            self._available = True
        except ImportError:
            self._available = False
            print("edgartools not installed, EDGAR provider unavailable")

    def get_recent_trades(self, ticker, days=90):
        if not self._available:
            return []
        try:
            from edgar import Company
            c = Company(ticker)
            filings = c.get_filings(form="4").latest(15)
            trades = []
            for f in filings:
                try:
                    obj = f.obj()
                    ndt = obj.non_derivative_table
                    if ndt and hasattr(ndt, "transactions"):
                        for t in ndt.transactions:
                            if t is None:
                                continue
                            shares = t.shares or 0
                            price = t.price if t.price and not (isinstance(t.price, float) and math.isnan(t.price)) else 0
                            trades.append({
                                "filing_date": str(f.filing_date),
                                "trade_date": str(t.date) if t.date else str(f.filing_date),
                                "ticker": ticker.upper(),
                                "insider_name": obj.insider_name or "",
                                "title": obj.position or "",
                                "trade_type": t.transaction_type or "",
                                "price": f"${price:.2f}" if price else "",
                                "qty": f"{int(shares):,}" if shares else "",
                                "owned": f"{int(t.remaining):,}" if t.remaining else "",
                                "delta_own": "",
                                "value": f"${int(shares * price):,}" if shares and price else "",
                                "is_purchase": t.acquired_disposed == "A",
                            })
                except Exception:
                    continue
                time.sleep(0.15)  # SEC rate limit
            return trades
        except Exception as e:
            print(f"EDGAR error for {ticker}: {e}")
            return []

    def get_bulk_recent(self, tickers, days=30):
        results = {}
        for ticker in tickers:
            results[ticker] = self.get_recent_trades(ticker, days)
        return results


# ═══════════════════════════════════════════════════════════════════════
# Options Flow Provider (yfinance)
# ═══════════════════════════════════════════════════════════════════════

class OptionsFlowProvider:
    CACHE_FILE = "options_flow_cache.json"
    CACHE_TTL = 900  # 15 minutes

    def __init__(self):
        self._cache = self._load_cache()

    def _cache_path(self):
        return DATA_DIR / self.CACHE_FILE

    def _load_cache(self):
        path = self._cache_path()
        if path.exists():
            try:
                with open(path) as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return {"last_fetch": {}, "data": {}}

    def _save_cache(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(self._cache_path(), "w") as f:
            json.dump(self._cache, f, default=str)

    def _is_stale(self, ticker):
        last = self._cache.get("last_fetch", {}).get(ticker)
        if not last:
            return True
        try:
            return (datetime.now() - datetime.fromisoformat(last)).total_seconds() > self.CACHE_TTL
        except (ValueError, TypeError):
            return True

    def _nan_safe(self, val, default=0):
        """Convert NaN/None to default."""
        if val is None:
            return default
        try:
            import math
            if math.isnan(val):
                return default
        except (TypeError, ValueError):
            pass
        return val

    def get_flow(self, ticker, expiration=None):
        """Get options flow data for a ticker. Returns unusual activity, put/call ratio, chain."""
        ticker = ticker.upper()

        # Check cache
        cache_key = f"{ticker}_{expiration or 'near'}"
        if not self._is_stale(cache_key):
            cached = self._cache.get("data", {}).get(cache_key)
            if cached:
                return cached

        try:
            import yfinance as yf
            t = yf.Ticker(ticker)
            expirations = list(t.options)
            if not expirations:
                return {"ticker": ticker, "error": "No options available", "expirations": []}

            # Use requested expiration or nearest
            exp = expiration if expiration and expiration in expirations else expirations[0]
            chain = t.option_chain(exp)

            # Process calls
            unusual = []
            call_vol_total = 0
            put_vol_total = 0

            for _, r in chain.calls.iterrows():
                vol = int(self._nan_safe(r.get("volume"), 0))
                oi = int(self._nan_safe(r.get("openInterest"), 0))
                iv = self._nan_safe(r.get("impliedVolatility"), 0)
                price = self._nan_safe(r.get("lastPrice"), 0)
                bid = self._nan_safe(r.get("bid"), 0)
                ask = self._nan_safe(r.get("ask"), 0)
                call_vol_total += vol
                vol_oi = round(vol / oi, 2) if oi > 0 else 0
                is_unusual = vol > 100 and oi > 0 and vol_oi > 1.0
                if vol > 50:
                    unusual.append({
                        "strike": float(r["strike"]),
                        "exp": exp,
                        "type": "C",
                        "volume": vol,
                        "oi": oi,
                        "vol_oi": vol_oi,
                        "iv": round(iv * 100, 1),
                        "price": round(price, 2),
                        "bid": round(bid, 2),
                        "ask": round(ask, 2),
                        "premium": round(vol * price * 100, 0),
                        "unusual": is_unusual,
                        "itm": bool(r.get("inTheMoney", False)),
                    })

            for _, r in chain.puts.iterrows():
                vol = int(self._nan_safe(r.get("volume"), 0))
                oi = int(self._nan_safe(r.get("openInterest"), 0))
                iv = self._nan_safe(r.get("impliedVolatility"), 0)
                price = self._nan_safe(r.get("lastPrice"), 0)
                bid = self._nan_safe(r.get("bid"), 0)
                ask = self._nan_safe(r.get("ask"), 0)
                put_vol_total += vol
                vol_oi = round(vol / oi, 2) if oi > 0 else 0
                is_unusual = vol > 100 and oi > 0 and vol_oi > 1.0
                if vol > 50:
                    unusual.append({
                        "strike": float(r["strike"]),
                        "exp": exp,
                        "type": "P",
                        "volume": vol,
                        "oi": oi,
                        "vol_oi": vol_oi,
                        "iv": round(iv * 100, 1),
                        "price": round(price, 2),
                        "bid": round(bid, 2),
                        "ask": round(ask, 2),
                        "premium": round(vol * price * 100, 0),
                        "unusual": is_unusual,
                        "itm": bool(r.get("inTheMoney", False)),
                    })

            # Sort by volume descending
            unusual.sort(key=lambda x: x["volume"], reverse=True)

            pc_ratio = round(put_vol_total / call_vol_total, 2) if call_vol_total > 0 else 0
            unusual_count = sum(1 for u in unusual if u["unusual"])

            result = {
                "ticker": ticker,
                "expiration": exp,
                "expirations": expirations[:12],  # limit to nearest 12
                "put_call_ratio": pc_ratio,
                "call_volume": call_vol_total,
                "put_volume": put_vol_total,
                "unusual_count": unusual_count,
                "flow": unusual[:30],  # top 30 by volume
            }

            # Cache it
            if "data" not in self._cache:
                self._cache["data"] = {}
            if "last_fetch" not in self._cache:
                self._cache["last_fetch"] = {}
            self._cache["data"][cache_key] = result
            self._cache["last_fetch"][cache_key] = datetime.now().isoformat()
            self._save_cache()

            return result

        except Exception as e:
            print(f"Options flow error for {ticker}: {e}")
            return {"ticker": ticker, "error": str(e), "expirations": []}

    def get_portfolio_summary(self, tickers):
        """Get put/call ratio + unusual count for all portfolio tickers. Fast — uses nearest expiration."""
        results = {}
        for ticker in tickers:
            ticker = ticker.upper()
            data = self.get_flow(ticker)
            if "error" not in data:
                results[ticker] = {
                    "put_call_ratio": data["put_call_ratio"],
                    "call_volume": data["call_volume"],
                    "put_volume": data["put_volume"],
                    "unusual_count": data["unusual_count"],
                    "expiration": data["expiration"],
                }
            time.sleep(0.3)  # rate limit yfinance
        return results


# ═══════════════════════════════════════════════════════════════════════
# News Feed Provider
# ═══════════════════════════════════════════════════════════════════════

class NewsProvider:
    """Fetches financial news per ticker. Yahoo RSS (free, no key) by default.
    Set FINNHUB_API_KEY env var to upgrade to Finnhub."""

    CACHE_FILE = "news_cache.json"
    CACHE_TTL = 900  # 15 minutes

    def __init__(self):
        self._finnhub_key = os.environ.get("FINNHUB_API_KEY", "")
        self._cache = self._load_cache()

    def _cache_path(self):
        return DATA_DIR / self.CACHE_FILE

    def _load_cache(self):
        path = self._cache_path()
        if path.exists():
            try:
                with open(path) as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return {"last_fetch": {}, "data": {}}

    def _save_cache(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(self._cache_path(), "w") as f:
            json.dump(self._cache, f, default=str)

    def _is_stale(self, key):
        last = self._cache.get("last_fetch", {}).get(key)
        if not last:
            return True
        try:
            return (datetime.now() - datetime.fromisoformat(last)).total_seconds() > self.CACHE_TTL
        except (ValueError, TypeError):
            return True

    # Sources that require subscriptions — filter these out
    PAYWALLED_SOURCES = {"seekingalpha.com", "fool.com/premium", "barrons.com", "wsj.com"}

    @staticmethod
    def _parse_date(date_str):
        """Parse RSS date string to Unix timestamp for reliable sorting."""
        if not date_str:
            return 0
        try:
            from email.utils import parsedate_to_datetime
            return parsedate_to_datetime(date_str).timestamp()
        except Exception:
            pass
        try:
            return datetime.strptime(date_str, "%a, %d %b %Y %H:%M").timestamp()
        except Exception:
            return 0

    @staticmethod
    def _is_paywalled(url):
        """Check if article URL is from a paywalled source."""
        if not url:
            return False
        url_lower = url.lower()
        return any(src in url_lower for src in NewsProvider.PAYWALLED_SOURCES)

    @staticmethod
    def _analyze_sentiment(title):
        """Keyword-based sentiment from title."""
        t = title.lower()
        bullish = ["surge", "soar", "rally", "jump", "gain", "rise", "bull", "upgrade",
                   "beat", "record", "high", "buy", "growth", "profit", "strong", "boom"]
        bearish = ["fall", "drop", "crash", "plunge", "decline", "loss", "bear", "downgrade",
                   "miss", "low", "sell", "weak", "cut", "warn", "risk", "fear", "layoff"]
        if any(w in t for w in bullish):
            return "bullish"
        elif any(w in t for w in bearish):
            return "bearish"
        return "neutral"

    def _fetch_yahoo_rss(self, ticker):
        """Fetch news from Yahoo Finance RSS feed."""
        try:
            from bs4 import BeautifulSoup
            url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
            resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
            if resp.status_code != 200:
                return []

            soup = BeautifulSoup(resp.text, "lxml-xml")
            items = soup.find_all("item")
            articles = []
            for item in items[:15]:
                title = item.find("title")
                link = item.find("link")
                pub = item.find("pubDate")
                desc = item.find("description")
                if title:
                    article_url = link.text.strip() if link else ""
                    if self._is_paywalled(article_url):
                        continue
                    pub_str = pub.text.strip() if pub else ""
                    articles.append({
                        "title": title.text.strip(),
                        "url": article_url,
                        "published": pub_str,
                        "published_ts": self._parse_date(pub_str),
                        "description": desc.text.strip()[:200] if desc else "",
                        "source": "Yahoo Finance",
                        "ticker": ticker.upper(),
                        "sentiment": self._analyze_sentiment(title.text),
                    })
            return articles
        except Exception as e:
            print(f"Yahoo RSS error for {ticker}: {e}")
            return []

    def _fetch_finnhub(self, ticker):
        """Fetch news from Finnhub API (requires API key)."""
        if not self._finnhub_key:
            return []
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            url = f"https://finnhub.io/api/v1/company-news?symbol={ticker}&from={week_ago}&to={today}&token={self._finnhub_key}"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                return []
            data = resp.json()
            articles = []
            for item in data[:15]:
                article_url = item.get("url", "")
                if self._is_paywalled(article_url):
                    continue
                ts = item.get("datetime", 0)
                pub_str = datetime.fromtimestamp(ts).strftime("%a, %d %b %Y %H:%M") if ts else ""
                articles.append({
                    "title": item.get("headline", ""),
                    "url": article_url,
                    "published": pub_str,
                    "published_ts": float(ts) if ts else 0,
                    "description": (item.get("summary", "") or "")[:200],
                    "source": item.get("source", "Finnhub"),
                    "ticker": ticker.upper(),
                    "sentiment": self._analyze_sentiment(item.get("headline", "")),
                })
            return articles
        except Exception as e:
            print(f"Finnhub error for {ticker}: {e}")
            return []

    def get_news(self, ticker):
        """Get news for a single ticker."""
        ticker = ticker.upper()
        if not self._is_stale(ticker):
            cached = self._cache.get("data", {}).get(ticker, [])
            if cached:
                return cached

        # Try Finnhub first if available, fall back to Yahoo RSS
        articles = self._fetch_finnhub(ticker) if self._finnhub_key else []
        if not articles:
            articles = self._fetch_yahoo_rss(ticker)

        if "data" not in self._cache:
            self._cache["data"] = {}
        if "last_fetch" not in self._cache:
            self._cache["last_fetch"] = {}
        self._cache["data"][ticker] = articles
        self._cache["last_fetch"][ticker] = datetime.now().isoformat()
        self._save_cache()
        return articles

    def get_portfolio_news(self, tickers):
        """Get aggregated news for all portfolio tickers, sorted by date."""
        all_articles = []
        seen_titles = set()
        for ticker in tickers:
            articles = self.get_news(ticker)
            for a in articles:
                # Deduplicate: same article can appear for multiple tickers
                title_key = a.get("title", "").strip().lower()
                if title_key in seen_titles:
                    # Add ticker tag to existing article instead of duplicating
                    for existing in all_articles:
                        if existing.get("title", "").strip().lower() == title_key:
                            if a["ticker"] not in existing.get("tickers", [existing["ticker"]]):
                                if "tickers" not in existing:
                                    existing["tickers"] = [existing["ticker"]]
                                existing["tickers"].append(a["ticker"])
                            break
                    continue
                seen_titles.add(title_key)
                a["tickers"] = [a["ticker"]]
                all_articles.append(a)
            time.sleep(0.3)

        # Sort by timestamp descending (most recent first)
        all_articles.sort(key=lambda x: x.get("published_ts", 0), reverse=True)
        return all_articles


# ═══════════════════════════════════════════════════════════════════════
# Card Valuation Provider (eBay)
# ═══════════════════════════════════════════════════════════════════════

class CardValuationProvider:
    """Fetches sold card prices from eBay using the Finding API.
    Requires EBAY_APP_ID env var. Get one free at developer.ebay.com."""

    CACHE_FILE = "card_valuation_cache.json"
    CACHE_TTL = 86400  # 24 hours — card prices don't move fast

    def __init__(self):
        self._app_id = os.environ.get("EBAY_APP_ID", "")
        self._cache = self._load_cache()
        if not self._app_id:
            print("ℹ EBAY_APP_ID not set — card valuations will use manual estimates only")

    @property
    def available(self):
        return bool(self._app_id)

    def _cache_path(self):
        return DATA_DIR / self.CACHE_FILE

    def _load_cache(self):
        path = self._cache_path()
        if path.exists():
            try:
                with open(path) as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return {"last_fetch": {}, "data": {}}

    def _save_cache(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(self._cache_path(), "w") as f:
            json.dump(self._cache, f, default=str)

    def _is_stale(self, cache_key):
        last = self._cache.get("last_fetch", {}).get(cache_key)
        if not last:
            return True
        try:
            return (datetime.now() - datetime.fromisoformat(last)).total_seconds() > self.CACHE_TTL
        except (ValueError, TypeError):
            return True

    def get_valuation(self, card):
        """Get sold comps for a card. Returns avg, median, low, high, recent sales."""
        if not self._app_id:
            return {"error": "EBAY_APP_ID not configured", "card_id": card.get("id")}

        query = f"{card['player']} {card['year']} {card['brand']} {card['card_name']}"
        # Add grade to query if graded
        if card.get("grader") and card.get("grade"):
            query += f" {card['grader']} {card['grade']}"

        cache_key = query.lower().strip()
        if not self._is_stale(cache_key):
            cached = self._cache.get("data", {}).get(cache_key)
            if cached:
                return cached

        try:
            # eBay Finding API - findCompletedItems
            url = "https://svcs.ebay.com/services/search/FindingService/v1"
            params = {
                "OPERATION-NAME": "findCompletedItems",
                "SERVICE-VERSION": "1.13.0",
                "SECURITY-APPNAME": self._app_id,
                "RESPONSE-DATA-FORMAT": "JSON",
                "REST-PAYLOAD": "",
                "keywords": query,
                "categoryId": "213",  # Basketball Cards
                "itemFilter(0).name": "SoldItemsOnly",
                "itemFilter(0).value": "true",
                "itemFilter(1).name": "ListingType",
                "itemFilter(1).value(0)": "AuctionWithBIN",
                "itemFilter(1).value(1)": "FixedPrice",
                "itemFilter(1).value(2)": "Auction",
                "sortOrder": "EndTimeSoonest",
                "paginationInput.entriesPerPage": "20",
            }
            resp = requests.get(url, params=params, timeout=15, headers={"User-Agent": USER_AGENT})
            data = resp.json()

            items = (data.get("findCompletedItemsResponse", [{}])[0]
                        .get("searchResult", [{}])[0]
                        .get("item", []))

            prices = []
            recent_sales = []
            for item in items:
                try:
                    price = float(item["sellingStatus"][0]["currentPrice"][0]["__value__"])
                    title = item.get("title", [""])[0]
                    end_time = item.get("listingInfo", [{}])[0].get("endTime", [""])[0]
                    prices.append(price)
                    recent_sales.append({
                        "price": round(price, 2),
                        "title": title[:80] if isinstance(title, str) else str(title)[:80],
                        "date": end_time[:10] if end_time else "",
                    })
                except (KeyError, IndexError, ValueError, TypeError):
                    continue

            if prices:
                prices.sort()
                avg = sum(prices) / len(prices)
                median = prices[len(prices) // 2]
                result = {
                    "card_id": card.get("id"),
                    "query": query,
                    "num_results": len(prices),
                    "average": round(avg, 2),
                    "median": round(median, 2),
                    "low": round(min(prices), 2),
                    "high": round(max(prices), 2),
                    "recent_sales": recent_sales[:8],
                    "last_updated": datetime.now().isoformat(),
                }
            else:
                result = {
                    "card_id": card.get("id"),
                    "query": query,
                    "num_results": 0,
                    "message": "No sold listings found",
                    "last_updated": datetime.now().isoformat(),
                }

            # Cache
            if "data" not in self._cache:
                self._cache["data"] = {}
            if "last_fetch" not in self._cache:
                self._cache["last_fetch"] = {}
            self._cache["data"][cache_key] = result
            self._cache["last_fetch"][cache_key] = datetime.now().isoformat()
            self._save_cache()
            return result

        except Exception as e:
            print(f"eBay valuation error: {e}")
            return {"card_id": card.get("id"), "error": str(e)}

    def get_bulk_valuation(self, cards):
        """Get valuations for multiple cards."""
        results = []
        for card in cards:
            results.append(self.get_valuation(card))
            time.sleep(0.3)
        return results


# ═══════════════════════════════════════════════════════════════════════
# Provider Factory
# ═══════════════════════════════════════════════════════════════════════

# Default providers — swap these to upgrade data sources
_darkpool_provider = None
_insider_provider = None
_options_flow_provider = None
_card_valuation_provider = None
_news_provider = None


def get_darkpool_provider():
    global _darkpool_provider
    if _darkpool_provider is None:
        _darkpool_provider = FINRADarkPoolProvider()
    return _darkpool_provider


def get_insider_provider():
    global _insider_provider
    if _insider_provider is None:
        _insider_provider = OpenInsiderProvider()
    return _insider_provider


def get_options_flow_provider():
    global _options_flow_provider
    if _options_flow_provider is None:
        _options_flow_provider = OptionsFlowProvider()
    return _options_flow_provider


def get_card_valuation_provider():
    global _card_valuation_provider
    if _card_valuation_provider is None:
        _card_valuation_provider = CardValuationProvider()
    return _card_valuation_provider


def get_news_provider():
    global _news_provider
    if _news_provider is None:
        _news_provider = NewsProvider()
    return _news_provider


# ═══════════════════════════════════════════════════════════════════════
# Wheel Strategy Options Provider
# ═══════════════════════════════════════════════════════════════════════

TRADIER_API_KEY = os.environ.get("TRADIER_API_KEY")

class WheelOptionsProvider:
    """Options chain + greeks provider. Uses Tradier if API key set, else yfinance fallback."""
    CACHE_TTL = 300  # 5 min
    IV_CACHE_FILE = "iv_cache.json"
    _CHAIN_CACHE_MAX = 20  # LRU cap — each entry is ~8KB (full options chain)

    def __init__(self):
        self._chain_cache = {}  # {cache_key: {ts, data}}
        self._exp_cache = {}    # {ticker: {ts, data}}
        self._use_tradier = bool(TRADIER_API_KEY)
        self._iv_cache = self._load_iv_cache()

    def _load_iv_cache(self):
        path = DATA_DIR / self.IV_CACHE_FILE
        if path.exists():
            try:
                with open(path) as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return {}

    def _save_iv_cache(self):
        path = DATA_DIR / self.IV_CACHE_FILE
        with open(path, "w") as f:
            json.dump(self._iv_cache, f)

    def _cache_iv(self, ticker, iv_val):
        today = datetime.now().strftime("%Y-%m-%d")
        if ticker not in self._iv_cache:
            self._iv_cache[ticker] = []
        entries = self._iv_cache[ticker]
        if not entries or entries[-1]["date"] != today:
            entries.append({"date": today, "iv": round(iv_val, 4)})
            if len(entries) > 400:
                self._iv_cache[ticker] = entries[-365:]
            self._save_iv_cache()

    def get_iv_rank(self, ticker):
        entries = self._iv_cache.get(ticker, [])
        if len(entries) < 2:
            return None, 0
        current = entries[-1]["iv"]
        ivs = [e["iv"] for e in entries]
        rank = sum(1 for v in ivs if v <= current) / len(ivs) * 100
        return round(rank, 1), len(entries)

    def get_expirations(self, ticker):
        ticker = ticker.upper()
        cached = self._exp_cache.get(ticker)
        if cached and time.time() - cached["ts"] < 86400:
            return cached["data"]

        if self._use_tradier:
            exps = self._tradier_expirations(ticker)
        else:
            exps = self._yf_expirations(ticker)

        self._exp_cache[ticker] = {"ts": time.time(), "data": exps}
        return exps

    def get_chain(self, ticker, expiration):
        ticker = ticker.upper()
        key = f"{ticker}_{expiration}"
        cached = self._chain_cache.get(key)
        if cached and time.time() - cached["ts"] < self.CACHE_TTL:
            return cached["data"]

        if self._use_tradier:
            chain = self._tradier_chain(ticker, expiration)
        else:
            chain = self._yf_chain(ticker, expiration)

        self._chain_cache[key] = {"ts": time.time(), "data": chain}
        # LRU eviction
        if len(self._chain_cache) > self._CHAIN_CACHE_MAX:
            oldest = min(self._chain_cache, key=lambda k: self._chain_cache[k]["ts"])
            del self._chain_cache[oldest]
        return chain

    def get_quote(self, ticker):
        try:
            import yfinance as yf
            t = yf.Ticker(ticker)
            info = t.info
            price = info.get("regularMarketPrice") or info.get("currentPrice") or 0
            return {
                "price": price,
                "name": info.get("shortName", ticker),
                "market_cap": info.get("marketCap", 0),
            }
        except Exception:
            return {"price": 0, "name": ticker}

    def get_hv_30d(self, ticker):
        try:
            import yfinance as yf
            t = yf.Ticker(ticker)
            hist = t.history(period="2mo")
            if hist.empty or len(hist) < 10:
                return None
            returns = hist["Close"].pct_change().dropna()
            hv = float(returns[-30:].std() * (252 ** 0.5))
            return round(hv, 4)
        except Exception:
            return None

    def get_earnings_date(self, ticker):
        try:
            import yfinance as yf
            t = yf.Ticker(ticker)
            cal = t.calendar
            if cal is not None and not cal.empty:
                if "Earnings Date" in cal.index:
                    dates = cal.loc["Earnings Date"]
                    if hasattr(dates, 'iloc') and len(dates) > 0:
                        return str(dates.iloc[0])[:10]
                    return str(dates)[:10]
            return None
        except Exception:
            return None

    # ── Tradier implementations ──

    def _tradier_expirations(self, ticker):
        try:
            r = requests.get(
                f"https://api.tradier.com/v1/markets/options/expirations",
                params={"symbol": ticker},
                headers={"Authorization": f"Bearer {TRADIER_API_KEY}", "Accept": "application/json"},
                timeout=10
            )
            data = r.json()
            exps = data.get("expirations", {}).get("date", [])
            return exps if isinstance(exps, list) else [exps]
        except Exception:
            return self._yf_expirations(ticker)

    def _tradier_chain(self, ticker, expiration):
        try:
            r = requests.get(
                f"https://api.tradier.com/v1/markets/options/chains",
                params={"symbol": ticker, "expiration": expiration, "greeks": "true"},
                headers={"Authorization": f"Bearer {TRADIER_API_KEY}", "Accept": "application/json"},
                timeout=10
            )
            data = r.json()
            options = data.get("options", {}).get("option", [])
            if not isinstance(options, list):
                options = [options] if options else []

            puts, calls = [], []
            for o in options:
                greeks = o.get("greeks", {}) or {}
                entry = {
                    "strike": o.get("strike", 0),
                    "bid": o.get("bid", 0) or 0,
                    "ask": o.get("ask", 0) or 0,
                    "mark": round(((o.get("bid", 0) or 0) + (o.get("ask", 0) or 0)) / 2, 2),
                    "volume": o.get("volume", 0) or 0,
                    "open_interest": o.get("open_interest", 0) or 0,
                    "iv": round(greeks.get("mid_iv", 0) or 0, 4),
                    "delta": round(greeks.get("delta", 0) or 0, 4),
                    "gamma": round(greeks.get("gamma", 0) or 0, 4),
                    "theta": round(greeks.get("theta", 0) or 0, 4),
                    "vega": round(greeks.get("vega", 0) or 0, 4),
                }
                entry["bid_ask_spread"] = round(entry["ask"] - entry["bid"], 2)
                if o.get("option_type") == "put":
                    puts.append(entry)
                else:
                    calls.append(entry)

            return {"puts": puts, "calls": calls, "source": "tradier"}
        except Exception:
            return self._yf_chain(ticker, expiration)

    # ── yfinance fallback implementations ──

    def _yf_expirations(self, ticker):
        try:
            import yfinance as yf
            t = yf.Ticker(ticker)
            return list(t.options)
        except Exception:
            return []

    def _yf_chain(self, ticker, expiration):
        try:
            import yfinance as yf
            t = yf.Ticker(ticker)
            chain = t.option_chain(expiration)
            puts, calls = [], []

            for df, side, dest in [(chain.puts, "put", puts), (chain.calls, "call", calls)]:
                for _, row in df.iterrows():
                    bid = float(row.get("bid", 0) or 0)
                    ask = float(row.get("ask", 0) or 0)
                    entry = {
                        "strike": float(row.get("strike", 0)),
                        "bid": bid,
                        "ask": ask,
                        "mark": round((bid + ask) / 2, 2),
                        "volume": int(row.get("volume", 0) or 0),
                        "open_interest": int(row.get("openInterest", 0) or 0),
                        "iv": round(float(row.get("impliedVolatility", 0) or 0), 4),
                        "delta": None,
                        "gamma": None,
                        "theta": None,
                        "vega": None,
                    }
                    entry["bid_ask_spread"] = round(ask - bid, 2)
                    dest.append(entry)

            return {"puts": puts, "calls": calls, "source": "yfinance"}
        except Exception as e:
            return {"puts": [], "calls": [], "source": "yfinance", "error": str(e)}


_wheel_provider = None

def get_wheel_provider():
    global _wheel_provider
    if _wheel_provider is None:
        _wheel_provider = WheelOptionsProvider()
    return _wheel_provider


# ═══════════════════════════════════════════════════════════════════════
# ETH ETF Flow Provider (Farside Investors scraper)
# ═══════════════════════════════════════════════════════════════════════

class ETHETFFlowProvider:
    """Scrapes ETH and BTC ETF daily flow data from Farside Investors.
    Falls back to cached data if Farside is behind Cloudflare."""

    ETH_URL = "https://farside.co.uk/eth/"
    ETH_ALL_URL = "https://farside.co.uk/ethereum-etf-flow-all-data/"
    BTC_URLS = ["https://farside.co.uk/bitcoin-etf-flow-data/", "https://farside.co.uk/bitcoin-etf-flow-all-data/"]
    ETH_CACHE_FILE = "etf_flows_cache.json"
    BTC_CACHE_FILE = "btc_etf_flows.json"
    CACHE_TTL = 14400  # 4 hours
    _MEM_MAX = 90  # days in memory
    _DISK_MAX = 365  # days on disk
    BROWSER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"

    def __init__(self):
        self._eth_cache = self._load_cache(self.ETH_CACHE_FILE)
        self._btc_cache = self._load_cache(self.BTC_CACHE_FILE)

    def _load_cache(self, filename):
        path = DATA_DIR / filename
        if path.exists():
            try:
                with open(path) as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return {"last_fetch": None, "flows": [], "stale": False}

    def _save_cache(self, cache, filename):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        # Prune to disk max
        if len(cache.get("flows", [])) > self._DISK_MAX:
            cache["flows"] = cache["flows"][-self._DISK_MAX:]
        with open(DATA_DIR / filename, "w") as f:
            json.dump(cache, f, default=str)

    def _parse_farside_table(self, html):
        """Parse Farside HTML table into list of dicts. Returns [] on failure."""
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            table = soup.find("table")
            if not table:
                return []
            rows = table.find_all("tr")
            if len(rows) < 2:
                return []
            # Get headers from first row
            header_cells = rows[0].find_all(["th", "td"])
            headers = [h.get_text(strip=True) for h in header_cells]
            # Map known column patterns
            results = []
            for row in rows[1:]:
                cells = row.find_all(["td", "th"])
                if len(cells) < 3:
                    continue
                values = [c.get_text(strip=True) for c in cells]
                if len(values) != len(headers):
                    continue
                entry = {}
                for i, h in enumerate(headers):
                    val = values[i]
                    h_lower = h.lower().strip()
                    if h_lower in ("date", ""):
                        if i == 0:
                            entry["date"] = val
                        continue
                    if h_lower == "total":
                        entry["Total"] = self._parse_flow_val(val)
                    else:
                        entry[h] = self._parse_flow_val(val)
                if entry.get("date"):
                    results.append(entry)
            return results
        except Exception as e:
            print(f"  Farside parse error: {e}")
            return []

    def _parse_flow_val(self, val):
        """Parse flow value string like '42.3', '-158', '(42.3)' → float in $M."""
        if not val or val in ("-", "", "—"):
            return 0.0
        val = val.replace(",", "").replace("$", "").strip()
        # Handle parentheses for negatives: (42.3) → -42.3
        if val.startswith("(") and val.endswith(")"):
            val = "-" + val[1:-1]
        try:
            return float(val)
        except ValueError:
            return 0.0

    def _scrape(self, urls, cache, cache_file):
        """Attempt to scrape Farside. Returns True if successful."""
        if not isinstance(urls, list):
            urls = [urls]
        for url in urls:
            try:
                resp = requests.get(url, timeout=15, headers={
                    "User-Agent": self.BROWSER_UA,
                    "Accept": "text/html,application/xhtml+xml",
                    "Accept-Language": "en-US,en;q=0.5",
                })
                if resp.status_code == 200:
                    flows = self._parse_farside_table(resp.text)
                    if flows:
                        cache["flows"] = flows
                        cache["last_fetch"] = datetime.now().isoformat()
                        cache["stale"] = False
                        self._save_cache(cache, cache_file)
                        print(f"  Farside scrape OK: {len(flows)} days from {url}")
                        return True
                    else:
                        print(f"  Farside: page loaded but no table data from {url}")
                else:
                    print(f"  Farside {url}: HTTP {resp.status_code}")
            except Exception as e:
                print(f"  Farside scrape error for {url}: {e}")
        # Mark as stale but keep existing data
        cache["stale"] = True
        return False

    def _ensure_fresh(self, cache, urls, cache_file):
        """Refresh if cache is stale (>TTL)."""
        last = cache.get("last_fetch")
        if last:
            try:
                elapsed = (datetime.now() - datetime.fromisoformat(last)).total_seconds()
                if elapsed < self.CACHE_TTL:
                    return  # Fresh enough
            except (ValueError, TypeError):
                pass
        self._scrape(urls, cache, cache_file)

    # ── Public API: ETH flows ────────────────────────────────────────

    def get_daily_flows(self, days=10):
        """Return last N days of ETH ETF flows."""
        self._ensure_fresh(self._eth_cache, [self.ETH_URL, self.ETH_ALL_URL], self.ETH_CACHE_FILE)
        flows = self._eth_cache.get("flows", [])
        return {
            "flows": flows[-days:] if flows else [],
            "last_updated": self._eth_cache.get("last_fetch"),
            "stale": self._eth_cache.get("stale", False),
            "total_days": len(flows),
        }

    def get_yesterday_flow(self):
        """Return the most recent day's total net flow."""
        data = self.get_daily_flows(1)
        if data["flows"]:
            entry = data["flows"][-1]
            return {"total": entry.get("Total", 0), "date": entry.get("date", ""), "detail": entry, "stale": data["stale"]}
        return {"total": 0, "date": "", "detail": {}, "stale": True}

    def get_cumulative(self, period="1m"):
        """Return cumulative flow data for a period."""
        self._ensure_fresh(self._eth_cache, [self.ETH_URL, self.ETH_ALL_URL], self.ETH_CACHE_FILE)
        flows = self._eth_cache.get("flows", [])
        period_days = {"1w": 7, "1m": 30, "3m": 90, "ytd": 365}.get(period, 30)
        recent = flows[-period_days:] if len(flows) >= period_days else flows
        cumulative = []
        running = 0
        for entry in recent:
            running += entry.get("Total", 0)
            cumulative.append({"date": entry.get("date", ""), "cumulative": round(running, 2), "daily": entry.get("Total", 0)})
        return {
            "data": cumulative,
            "period": period,
            "total": round(running, 2),
            "last_updated": self._eth_cache.get("last_fetch"),
            "stale": self._eth_cache.get("stale", False),
        }

    def get_streak_info(self):
        """Return current consecutive inflow/outflow streak."""
        flows = self._eth_cache.get("flows", [])
        if not flows:
            return {"streak": 0, "total": 0, "direction": "none"}
        streak = 0
        total = 0
        last_sign = None
        for entry in reversed(flows):
            val = entry.get("Total", 0)
            sign = 1 if val > 0 else (-1 if val < 0 else 0)
            if sign == 0:
                continue
            if last_sign is None:
                last_sign = sign
            if sign == last_sign:
                streak += 1
                total += val
            else:
                break
        return {
            "streak": streak * (last_sign or 1),
            "total": round(total, 2),
            "direction": "inflow" if (last_sign or 0) > 0 else "outflow" if (last_sign or 0) < 0 else "none",
        }

    def get_30d_avg(self):
        """Return 30-day average absolute daily flow."""
        flows = self._eth_cache.get("flows", [])
        recent = flows[-30:] if len(flows) >= 30 else flows
        if not recent:
            return 0
        return round(sum(abs(e.get("Total", 0)) for e in recent) / len(recent), 2)

    def get_weekly_regime_change(self):
        """Check if weekly flow direction flipped vs prior week."""
        flows = self._eth_cache.get("flows", [])
        if len(flows) < 10:
            return None
        this_week = sum(e.get("Total", 0) for e in flows[-5:])
        prev_week = sum(e.get("Total", 0) for e in flows[-10:-5])
        if (this_week > 0 and prev_week < 0):
            return {"type": "positive", "message": f"ETH ETF weekly flows flipped positive (${this_week:+.0f}M vs ${prev_week:+.0f}M prior week)"}
        elif (this_week < 0 and prev_week > 0):
            return {"type": "negative", "message": f"ETH ETF weekly flows flipped negative (${this_week:+.0f}M vs ${prev_week:+.0f}M prior week)"}
        return None

    # ── Public API: BTC flows (for comparison) ───────────────────────

    def get_btc_flows(self, days=90):
        """Return BTC ETF flows for comparison."""
        self._ensure_fresh(self._btc_cache, self.BTC_URLS, self.BTC_CACHE_FILE)
        flows = self._btc_cache.get("flows", [])
        return {
            "flows": flows[-days:] if flows else [],
            "last_updated": self._btc_cache.get("last_fetch"),
            "stale": self._btc_cache.get("stale", False),
        }

    def get_btc_comparison(self, period="1m"):
        """Return ETH vs BTC cumulative flows side by side."""
        period_days = {"1w": 7, "1m": 30, "3m": 90, "ytd": 365}.get(period, 30)
        eth_flows = self._eth_cache.get("flows", [])[-period_days:]
        btc_flows = self._btc_cache.get("flows", [])[-period_days:]
        # Build aligned cumulative
        eth_cum, btc_cum = 0, 0
        eth_data, btc_data = [], []
        for e in eth_flows:
            eth_cum += e.get("Total", 0)
            eth_data.append({"date": e.get("date", ""), "cumulative": round(eth_cum, 2)})
        for b in btc_flows:
            btc_cum += b.get("Total", 0)
            btc_data.append({"date": b.get("date", ""), "cumulative": round(btc_cum, 2)})
        return {
            "eth": eth_data, "btc": btc_data, "period": period,
            "eth_total": round(eth_cum, 2), "btc_total": round(btc_cum, 2),
            "last_updated": self._eth_cache.get("last_fetch"),
            "stale": self._eth_cache.get("stale", False) or self._btc_cache.get("stale", False),
        }


_etf_flow_provider = None

def get_etf_flow_provider():
    global _etf_flow_provider
    if _etf_flow_provider is None:
        _etf_flow_provider = ETHETFFlowProvider()
    return _etf_flow_provider


# ═══════════════════════════════════════════════════════════════════════
# BMNR NAV Premium/Discount Provider
# ═══════════════════════════════════════════════════════════════════════

class BMNRNAVProvider:
    """Calculates BMNR NAV premium/discount from yfinance market cap vs ETH holdings."""

    TREASURY_FILE = "bmnr_treasury.json"
    NAV_HISTORY_FILE = "bmnr_nav_history.json"
    CACHE_TTL = 300  # 5 min (tied to price refresh)
    _HISTORY_MAX = 365

    DEFAULT_TREASURY = {
        "eth_holdings": 4803000,
        "eth_staked": 3330000,
        "staking_yield": 0.0278,
        "staking_provider": "Mavan",
        "shares_outstanding": None,  # Auto-fetch from yfinance, manual fallback
        "last_updated": "2026-04-06",
    }

    def __init__(self):
        self._treasury = self._load_treasury()
        self._nav_cache = {"data": None, "ts": 0}

    def _load_treasury(self):
        path = DATA_DIR / self.TREASURY_FILE
        if path.exists():
            try:
                with open(path) as f:
                    data = json.load(f)
                # Merge defaults for any missing keys
                for k, v in self.DEFAULT_TREASURY.items():
                    if k not in data:
                        data[k] = v
                return data
            except (json.JSONDecodeError, IOError):
                pass
        return dict(self.DEFAULT_TREASURY)

    def _save_treasury(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(DATA_DIR / self.TREASURY_FILE, "w") as f:
            json.dump(self._treasury, f, indent=2, default=str)

    def update_treasury(self, updates):
        """Update treasury config (eth_holdings, eth_staked, staking_yield, etc.)."""
        for k in ("eth_holdings", "eth_staked", "staking_yield", "staking_provider", "shares_outstanding"):
            if k in updates and updates[k] is not None:
                self._treasury[k] = updates[k]
        self._treasury["last_updated"] = datetime.now().strftime("%Y-%m-%d")
        self._save_treasury()
        self._nav_cache = {"data": None, "ts": 0}  # Invalidate
        return self._treasury

    def get_nav(self):
        """Calculate BMNR NAV premium/discount."""
        now = time.time()
        if self._nav_cache["data"] and (now - self._nav_cache["ts"]) < self.CACHE_TTL:
            return self._nav_cache["data"]

        try:
            import yfinance as yf
            bmnr = yf.Ticker("BMNR")
            bmnr_info = bmnr.info
            eth = yf.Ticker("ETH-USD")
            eth_info = eth.info

            bmnr_price = bmnr_info.get("currentPrice") or bmnr_info.get("regularMarketPrice") or bmnr_info.get("previousClose") or 0
            shares = bmnr_info.get("sharesOutstanding") or self._treasury.get("shares_outstanding")
            eth_price = eth_info.get("currentPrice") or eth_info.get("regularMarketPrice") or eth_info.get("previousClose") or 0

            if not shares or not bmnr_price or not eth_price:
                return {"error": "Unable to fetch BMNR or ETH price data", "treasury": self._treasury}

            eth_holdings = self._treasury["eth_holdings"]
            market_cap = bmnr_price * shares
            nav = eth_holdings * eth_price
            premium_discount_pct = ((market_cap - nav) / nav * 100) if nav > 0 else 0

            # BMNU implied (2x leveraged)
            bmnu_implied = premium_discount_pct * 2

            result = {
                "bmnr_price": round(bmnr_price, 2),
                "shares_outstanding": shares,
                "market_cap": round(market_cap, 2),
                "eth_price": round(eth_price, 2),
                "eth_holdings": eth_holdings,
                "nav": round(nav, 2),
                "nav_per_share": round(nav / shares, 2) if shares else 0,
                "premium_discount_pct": round(premium_discount_pct, 2),
                "bmnu_implied_pct": round(bmnu_implied, 2),
                "treasury": self._treasury,
                "last_updated": datetime.now().isoformat(),
            }

            self._nav_cache = {"data": result, "ts": now}
            self._append_nav_history(result)
            return result

        except Exception as e:
            print(f"  BMNR NAV error: {e}")
            return {"error": str(e), "treasury": self._treasury}

    def _append_nav_history(self, nav_data):
        """Append daily NAV entry, gated to max once per 24 hours."""
        path = DATA_DIR / self.NAV_HISTORY_FILE
        history = []
        if path.exists():
            try:
                with open(path) as f:
                    history = json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        today = datetime.now().strftime("%Y-%m-%d")
        if history and history[-1].get("date") == today:
            # Update today's entry
            history[-1] = {
                "date": today,
                "premium_discount_pct": nav_data["premium_discount_pct"],
                "bmnr_price": nav_data["bmnr_price"],
                "eth_price": nav_data["eth_price"],
                "market_cap": nav_data["market_cap"],
                "nav": nav_data["nav"],
            }
        else:
            # Check 24-hour gate
            if history:
                try:
                    last_dt = datetime.fromisoformat(history[-1].get("date", "2000-01-01"))
                    if (datetime.now() - last_dt).total_seconds() < 86400:
                        return  # Too soon
                except (ValueError, TypeError):
                    pass
            history.append({
                "date": today,
                "premium_discount_pct": nav_data["premium_discount_pct"],
                "bmnr_price": nav_data["bmnr_price"],
                "eth_price": nav_data["eth_price"],
                "market_cap": nav_data["market_cap"],
                "nav": nav_data["nav"],
            })
        # Cap at 365
        if len(history) > self._HISTORY_MAX:
            history = history[-self._HISTORY_MAX:]
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(history, f)

    def get_nav_history(self, days=90):
        """Return NAV premium/discount history."""
        path = DATA_DIR / self.NAV_HISTORY_FILE
        if path.exists():
            try:
                with open(path) as f:
                    history = json.load(f)
                return history[-days:]
            except (json.JSONDecodeError, IOError):
                pass
        return []


_bmnr_nav_provider = None

def get_bmnr_nav_provider():
    global _bmnr_nav_provider
    if _bmnr_nav_provider is None:
        _bmnr_nav_provider = BMNRNAVProvider()
    return _bmnr_nav_provider


# ═══════════════════════════════════════════════════════════════════════
# ETH Staking Provider
# ═══════════════════════════════════════════════════════════════════════

class StakingProvider:
    """ETH staking metrics from Lido API + BMNR treasury config."""

    LIDO_APR_URL = "https://eth-api.lido.fi/v1/protocol/steth/apr/sma"
    CACHE_TTL = 3600  # 1 hour
    CACHE_FILE = "staking_cache.json"

    def __init__(self):
        self._cache = {"data": None, "ts": 0}

    def _get_lido_apr(self):
        """Fetch current Lido stETH APR."""
        try:
            resp = requests.get(self.LIDO_APR_URL, timeout=10, headers={"User-Agent": USER_AGENT})
            if resp.status_code == 200:
                data = resp.json()
                # Lido API returns {"data": {"smaApr": "3.45", ...}} or similar
                if isinstance(data, dict):
                    apr_data = data.get("data", data)
                    apr_val = apr_data.get("smaApr") or apr_data.get("apr") or apr_data.get("value")
                    if apr_val is not None:
                        return round(float(apr_val), 4)
        except Exception as e:
            print(f"  Lido APR fetch error: {e}")
        return None

    def get_staking_overview(self, treasury=None):
        """Return staking overview combining Lido APR + BMNR treasury data."""
        now = time.time()
        if self._cache["data"] and (now - self._cache["ts"]) < self.CACHE_TTL:
            return self._cache["data"]

        lido_apr = self._get_lido_apr()

        # Get treasury from BMNR provider
        if treasury is None:
            try:
                treasury = get_bmnr_nav_provider()._treasury
            except Exception:
                treasury = BMNRNAVProvider.DEFAULT_TREASURY

        eth_holdings = treasury.get("eth_holdings", 4803000)
        eth_staked = treasury.get("eth_staked", 3330000)
        staking_yield = treasury.get("staking_yield", 0.0278)
        staking_provider = treasury.get("staking_provider", "Mavan")
        staked_pct = (eth_staked / eth_holdings * 100) if eth_holdings > 0 else 0

        # Get ETH price for USD yield calc
        eth_price = 0
        try:
            import yfinance as yf
            eth_info = yf.Ticker("ETH-USD").info
            eth_price = eth_info.get("currentPrice") or eth_info.get("regularMarketPrice") or 0
        except Exception:
            pass

        annual_yield_eth = eth_staked * staking_yield
        annual_yield_usd = annual_yield_eth * eth_price if eth_price else 0

        result = {
            "eth_holdings": eth_holdings,
            "eth_staked": eth_staked,
            "staked_pct": round(staked_pct, 1),
            "staking_yield": staking_yield,
            "staking_provider": staking_provider,
            "annual_yield_eth": round(annual_yield_eth, 2),
            "annual_yield_usd": round(annual_yield_usd, 2),
            "lido_apr": lido_apr,
            "eth_price": round(eth_price, 2) if eth_price else None,
            "last_updated": datetime.now().isoformat(),
            "treasury_updated": treasury.get("last_updated"),
        }

        self._cache = {"data": result, "ts": now}
        return result


_staking_provider = None

def get_staking_provider():
    global _staking_provider
    if _staking_provider is None:
        _staking_provider = StakingProvider()
    return _staking_provider
