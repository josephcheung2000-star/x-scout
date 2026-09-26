#!/usr/bin/env python3
"""X-scout backtest v2: historical drift after catalyst types we could act on (priors for lead scoring).
Method (same as backtest.py): entry = first price >= LAG (3h) after the event becomes public; returns at fixed horizons;
excess vs a benchmark (BTC for crypto, SPY for US stocks, none for commodities / Polymarket). One event per asset per 7 days.
Types: coinbase_listing upbit_krw_listing bithumb_krw_listing token_unlock token_unlock_dm1 governance_buyback
       insider_cluster_buy stock_big_move (-> _up/_down) commodity_shock (-> _up/_down) polymarket_big_move
Usage: python3 backtest2.py [types...]      (no args = all types; "report" = re-aggregate cached events only)
  Each type's events are cached in bt2_cache/<type>.json; every run re-aggregates ALL cached types into
  backtest2_events.json, priors.json and priors_v2.md.  Python 3.12, stdlib only."""
import json, re, sys, os, time, math, html, random, statistics, urllib.request, urllib.parse, http.cookiejar
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

NOW = time.time()
DAY = 86400
LAG = 3 * 3600
WIN = 365 * DAY                   # crypto / insider look-back
CRYPTO_MAX = 120                  # per-type cap on priced crypto events (random sample, seed 7)
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, "bt2_cache")
ET = ZoneInfo("America/New_York")
UTC = timezone.utc
random.seed(7)

def log(*a): print(*a, file=sys.stderr, flush=True)

# ---------------------------------------------------------------- http
def fetch(url, data=None, ct=None, tries=5, raw=False, timeout=40, headers=None):
    h = {"User-Agent": UA, "Accept": "application/json, text/html;q=0.9, */*;q=0.8"}
    if ct: h["Content-Type"] = ct
    if headers: h.update(headers)
    for i in range(tries):
        try:
            b = urllib.request.urlopen(urllib.request.Request(url, data=data, headers=h), timeout=timeout).read()
            return b if raw else json.loads(b)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                if "yahoo" in url: return None          # Yahoo 429s by IP; caller falls back
                time.sleep(min(20 * (i + 1), 90)); continue
            if e.code in (400, 401, 403, 404, 422): return None
            time.sleep(5)
        except Exception:
            time.sleep(5)
    return None

# ---------------------------------------------------------------- crypto prices (hourly, (t, close))
def bn_series(sym, t0, t1):
    out, start = [], int(t0 * 1000)
    while start < t1 * 1000:
        d = fetch(f"https://data-api.binance.vision/api/v3/klines?symbol={sym}USDT&interval=1h&startTime={start}&limit=1000")
        if not d: break
        out += [(k[0] / 1000 + 3600, float(k[4])) for k in d]      # close time of the hour
        if len(d) < 1000: break
        start = d[-1][0] + 3600_000
    return out

def cb_series(product, t0, t1):
    out, s, t1 = [], int(t0), int(t1)
    while s < t1:
        e = min(s + 300 * 3600, int(t1))
        d = fetch(f"https://api.exchange.coinbase.com/products/{product}/candles?granularity=3600"
                  f"&start={datetime.fromtimestamp(s, UTC).isoformat()}&end={datetime.fromtimestamp(e, UTC).isoformat()}")
        if d: out += [(r[0] + 3600, float(r[4])) for r in d]
        s = e; time.sleep(0.35)
    return sorted(set(out))

_cg_ids = {}
def cg_id(sym):
    if sym not in _cg_ids:
        d = fetch(f"https://api.coingecko.com/api/v3/search?query={urllib.parse.quote(sym)}"); time.sleep(4)
        c = [x for x in (d or {}).get("coins", []) if str(x.get("symbol", "")).upper() == sym.upper()
             and (x.get("market_cap_rank") or 10**9) <= 1500]
        c.sort(key=lambda x: x.get("market_cap_rank") or 10**9)
        _cg_ids[sym] = c[0]["id"] if c else None
    return _cg_ids[sym]

def cg_series(cid, t0, t1):
    if not cid: return []
    out = []
    for a in range(int(t0), int(t1), 85 * DAY):                       # <=90d ranges -> hourly granularity
        d = fetch(f"https://api.coingecko.com/api/v3/coins/{cid}/market_chart/range?vs_currency=usd&from={a}&to={min(a + 85 * DAY, int(t1))}")
        time.sleep(4)
        out += [(p[0] / 1000, p[1]) for p in (d or {}).get("prices", [])]
    return sorted(out)

def at(series, ts, tol=None):
    for t, p in series:
        if t >= ts:
            return (t, p) if tol is None or t - ts <= tol else (None, None)
    return None, None

def before(series, ts, tol):
    best = (None, None)
    for t, p in series:
        if t <= ts: best = (t, p)
        else: break
    return best if best[0] is not None and ts - best[0] <= tol else (None, None)

_btc = []
def btc():
    global _btc
    if not _btc: _btc = bn_series("BTC", NOW - 800 * DAY, NOW)
    return _btc

def crypto_series(sym, t0, t1, cid=None, cb_product=None, need=None):
    """Binance hourly if it covers `need` (entry) within 6h, else Coinbase candles, else CoinGecko."""
    s = bn_series(sym, t0, t1) if sym else []
    if s and need and at(s, need, 6 * 3600)[0]: return s, "binance"
    if cb_product:
        s = cb_series(cb_product, t0, t1)
        if s and need and at(s, need, 6 * 3600)[0]: return s, "coinbase"
    cid = cid or (cg_id(sym) if sym else None)
    s = cg_series(cid, t0, t1)
    return s, "coingecko"

def crypto_returns(e, s, entry_ts, horizons, start_mode="after"):
    """horizons: {name: seconds after entry}. Adds r_<h> and ex_<h> (excess vs BTC)."""
    B = btc()
    te, pe = at(s, entry_ts, 6 * 3600) if start_mode == "after" else before(s, entry_ts, 6 * 3600)
    if not pe: return e
    _, b0 = at(B, te, 6 * 3600)
    e["entry_t"], e["entry"] = te, pe
    for k, h in horizons.items():
        if te + h > NOW - 3600: continue
        tx, px = at(s, te + h, 12 * 3600)
        _, bx = at(B, te + h, 12 * 3600)
        if px and b0 and bx:
            e["r_" + k] = round(px / pe - 1, 4)
            e["ex_" + k] = round((px / pe - 1) - (bx / b0 - 1), 4)
    return e

def dedupe(evs, key="asset", days=7):
    evs = sorted(evs, key=lambda e: e["t"]); last, out = {}, []
    for e in evs:
        if e[key] in last and e["t"] - last[e[key]] < days * DAY: continue
        last[e[key]] = e["t"]; out.append(e)
    return out

def cap(evs, n=CRYPTO_MAX):
    return evs if len(evs) <= n else sorted(random.Random(7).sample(evs, n), key=lambda e: e["t"])

STABLE_RE = re.compile(r"^(X|S|W|PY|FD|RL|T|FR|G|EUR)?(USD|EUR|SGD|AUD|GBP|JPY|CHF|CAD|BRL|TRY|MXN|KRW|IDR|ZAR)[A-Z0-9]?$")
def is_stable(sym): return sym.upper() in STABLE or bool(STABLE_RE.match(sym.upper()))
STABLE = {"EURCV", "EURQ", "USDQ", "USDTB", "XAUT", "PAXG", "USDT", "USDC", "DAI", "USDS", "FDUSD", "PYUSD", "USDE", "TUSD", "EURC", "USD1", "RLUSD", "USDG", "EUR", "GBP", "USD", "BUSD", "GUSD"}
# ---------------------------------------------------------------- 1 Coinbase listings
def run_coinbase_listing():
    p = fetch("https://api.coinbase.com/api/v3/brokerage/market/products?product_type=SPOT")["products"]
    first = {}
    for x in p:
        if not x.get("new_at"): continue
        t = datetime.fromisoformat(x["new_at"].replace("Z", "+00:00")).timestamp()
        b = x["base_currency_id"]
        if b not in first or t < first[b][0]: first[b] = (t, x["product_id"])
    evs = [{"type": "coinbase_listing", "asset": b, "t": t, "title": f"Coinbase lists {pid}", "product": pid}
           for b, (t, pid) in first.items()
           if NOW - WIN <= t <= NOW - 7 * DAY and not is_stable(b) and not t == datetime(2023, 1, 1, tzinfo=UTC).timestamp()]
    evs = cap(dedupe(evs))
    log("coinbase events", len(evs))
    out = []
    for i, e in enumerate(evs):
        entry = e["t"] + LAG
        s, src = crypto_series(e["asset"], e["t"] - 7200, min(e["t"] + 32 * DAY, NOW), cb_product=e["product"], need=entry)
        e["src"] = src
        out.append(crypto_returns(e, s, entry, {"1d": DAY, "7d": 7 * DAY, "30d": 30 * DAY}))
        if i % 10 == 0: log(i, e["asset"], src, e.get("ex_7d"))
    return out

# ---------------------------------------------------------------- 2 Upbit / Bithumb KRW listings (first KRW candle)
def krw_listing(exchange):
    base = {"upbit": "https://api.upbit.com/v1", "bithumb": "https://api.bithumb.com/v1"}[exchange]
    mk = [m["market"] for m in fetch(f"{base}/market/all?isDetails=false") if m["market"].startswith("KRW-")]
    iso = lambda ts: datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%dT%H:%M:%S")
    def one(m):
        found = []
        # daily candles back to the window start; stop as soon as a page reaches before the window
        days, to = [], None
        for _ in range(3):
            d = fetch(f"{base}/candles/days?market={m}&count=200" + (f"&to={to}" if to else ""))
            if not d: break
            days += d; time.sleep(0.3)
            first = datetime.fromisoformat(d[-1]["candle_date_time_utc"]).replace(tzinfo=UTC).timestamp()
            if len(d) < 200 or first < NOW - WIN - 40 * DAY: break
            to = iso(first)
        if not days or is_stable(m[4:]): return found
        ts = sorted(datetime.fromisoformat(c["candle_date_time_utc"]).replace(tzinfo=UTC).timestamp() for c in days)
        # ts[0] is either the true first KRW candle or older than the window; later >=30-day gaps = relistings
        starts = [ts[0]] + [t for i, t in enumerate(ts) if i > 0 and t - ts[i - 1] >= 30 * DAY]
        for d0 in starts:
            if not (NOW - WIN <= d0 <= NOW - 8 * DAY): continue
            h = fetch(f"{base}/candles/minutes/60?market={m}&count=200&to={iso(d0 + 3 * DAY)}")
            if not h: continue
            first_h = min(datetime.fromisoformat(c["candle_date_time_utc"]).replace(tzinfo=UTC).timestamp() for c in h)
            found.append({"type": f"{exchange}_krw_listing", "asset": m[4:], "t": first_h, "title": f"{exchange} opens {m}"})
        return found
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(4) as ex:
        evs = [e for r in ex.map(one, mk) for e in r]
    evs = cap(dedupe(evs))
    log(exchange, "events", len(evs))
    out = []
    for i, e in enumerate(evs):
        entry = e["t"] + LAG
        s, src = crypto_series(e["asset"], e["t"] - 7200, min(e["t"] + 32 * DAY, NOW), need=entry)
        e["src"] = src
        out.append(krw_native(exchange, crypto_returns(e, s, entry, {"1d": DAY, "7d": 7 * DAY, "30d": 30 * DAY})))
        if i % 10 == 0: log(i, e["asset"], src, e.get("ex_7d"), e.get("ex_7d_native"))
    return out

def krw_native(exchange, e):
    """Robustness: same event priced on the listing exchange's own KRW-X hourly candles, excess vs KRW-BTC (no ticker mapping)."""
    base = {"upbit": "https://api.upbit.com/v1", "bithumb": "https://api.bithumb.com/v1"}[exchange]
    def ser(m, t0, t1):
        out, to = [], t1
        while to > t0:
            d = fetch(f"{base}/candles/minutes/60?market={m}&count=200&to={datetime.fromtimestamp(to, UTC).strftime('%Y-%m-%dT%H:%M:%S')}")
            if not d: break
            out += [(datetime.fromisoformat(c["candle_date_time_utc"]).replace(tzinfo=UTC).timestamp() + 3600, c["trade_price"]) for c in d]
            to = min(o for o, _ in out) - 3600; time.sleep(0.12)
            if len(d) < 200: break
        return sorted(set(out))
    t0, t1 = e["t"], min(e["t"] + 8 * DAY + 3600, NOW)
    s, b = ser("KRW-" + e["asset"], t0, t1), ser("KRW-BTC", t0, t1)
    te, pe = at(s, e["t"] + LAG, 6 * 3600); _, b0 = at(b, e["t"] + LAG, 6 * 3600)
    if pe and b0:
        tx, px = at(s, te + 7 * DAY, 12 * 3600); _, bx = at(b, te + 7 * DAY, 12 * 3600)
        if px and bx: e["ex_7d_native"] = round((px / pe - 1) - (bx / b0 - 1), 4)
    return e

def run_upbit_krw_listing(): return krw_listing("upbit")
def run_bithumb_krw_listing(): return krw_listing("bithumb")

# ---------------------------------------------------------------- 3 token unlocks (DefiLlama datasets)
NONFLOAT = {"noncirculating", "migration"}
def run_token_unlock():
    B = "https://defillama-datasets.llama.fi/"
    evs = []
    for slug in fetch(B + "emissionsProtocolsList") or []:
        d = fetch(B + "emissions/" + slug)
        if not isinstance(d, dict) or not d.get("gecko_id"): continue
        series = [c["data"] for c in (d.get("documentedData") or {}).get("data", []) if c.get("data")]
        def unlocked(ts):
            return sum(([p["unlocked"] for p in s if p["timestamp"] < ts] or [0])[-1] for s in series)
        for ev in (d.get("metadata") or {}).get("unlockEvents") or []:
            t = ev["timestamp"]
            if not (NOW - WIN <= t <= NOW - 8 * DAY): continue
            amt = sum(a.get("amount", 0) for a in ev.get("cliffAllocations", []) if a.get("category") not in NONFLOAT)
            base = unlocked(t - 3600)
            if amt <= 0 or base <= 0: continue
            pct = amt / base
            if pct >= 0.02:
                evs.append({"type": "token_unlock", "asset": d["gecko_id"], "t": t, "title": f"{d.get('name')} cliff unlock {pct:.1%} of unlocked",
                            "pct": round(pct, 4), "gecko_id": d["gecko_id"]})
    evs = cap(dedupe(evs))
    log("unlock events", len(evs))
    out = []
    for i, e in enumerate(evs):
        s = cg_series(e["gecko_id"], e["t"] - 9 * DAY, min(e["t"] + 32 * DAY, NOW))
        e["src"] = "coingecko"
        # key horizon: from 7d before to 7d after (the scout sees scheduled unlocks well in advance)
        a = dict(e); crypto_returns(a, s, e["t"] - 7 * DAY, {"m7p7": 14 * DAY, "m7p0": 7 * DAY}, "after")
        b = dict(e); crypto_returns(b, s, e["t"] - DAY, {"m1p7": 8 * DAY}, "after")
        e.update({k: v for k, v in a.items() if k.startswith(("r_", "ex_"))})
        e.update({k: v for k, v in b.items() if k.startswith(("r_", "ex_"))})
        e["entry_t"], e["entry"] = a.get("entry_t"), a.get("entry")
        out.append(e)
        if i % 10 == 0: log(i, e["asset"], e.get("ex_m7p7"))
    return out

# ---------------------------------------------------------------- 4 Snapshot governance: buyback / fee switch / revenue share
GOV = re.compile(r"buy[\s-]?backs?|fee[\s-]?switch|revenue[\s-]?shar|repurchase", re.I)
NEG = re.compile(r"\b(pause|halt|stop|end(ing)?|reduc\w*|sunset\w*|terminat\w*|cancel\w*|suspend\w*)\b", re.I)   # proposals that cut buybacks
YES = re.compile(r"^(for|yes|yae|yea|approve|accept|in favou?r|support|aye)\b", re.I)
def run_governance_buyback():
    q = ('query($t:Int!,$s:Int!,$w:String!){proposals(first:1000,skip:$s,where:{created_gte:$t,state:"closed",title_contains:$w},'
         'orderBy:"created",orderDirection:desc){id title end choices scores scores_total quorum space{id name symbol followersCount}}}')
    props = {}
    for w in ["buyback", "buy back", "buy-back", "Buyback", "Buy back", "Buy-Back", "BuyBack", "fee switch", "Fee Switch", "Fee switch",
              "revenue shar", "Revenue Shar", "Revenue shar", "repurchase", "Repurchase"]:
        for s in (0, 1000):
            d = fetch("https://hub.snapshot.org/graphql", json.dumps({"query": q, "variables": {"t": int(NOW - WIN - 30 * DAY), "s": s, "w": w}}).encode(), "application/json")
            ps = ((d or {}).get("data") or {}).get("proposals") or []
            for p in ps: props[p["id"]] = p
            time.sleep(1)
            if len(ps) < 1000: break
    passed = []
    for p in props.values():
        if (p["space"]["followersCount"] or 0) < 1000 or not GOV.search(p["title"]) or NEG.search(p["title"]): continue
        if not (NOW - WIN <= p["end"] <= NOW - 8 * DAY): continue
        sc = p["scores"] or []
        if not sc or not p["choices"] or not YES.match(p["choices"][0].strip()): continue
        no = sum(v for c, v in zip(p["choices"], sc) if re.match(r"^(against|no|nay|reject)", c.strip(), re.I))
        if sc[0] <= no or sc[0] < max(sc) or (p.get("quorum") and p["scores_total"] < p["quorum"]): continue
        passed.append(p)
    log("snapshot matched", len(props), "passed", len(passed))
    # map space -> token: Snapshot space symbol must match a CoinGecko-search coin (rank <=1500) whose name or id
    # shares a word with the space id/name; multiple distinct candidates = ambiguous -> skip
    sp_map = {}
    for p in passed:
        sp = p["space"]
        if sp["id"] in sp_map: continue
        raw = (sp.get("symbol") or "").upper().strip()
        words = set(re.findall(r"[a-z0-9]{3,}", (sp["id"] + " " + (sp.get("name") or "")).lower())) - {"eth", "dao", "snapshot", "governance", "gov", "vote", "finance", "protocol", "network", "labs", "xyz", "org", "com"}
        c = []
        for sym in dict.fromkeys([raw, re.sub(r"^(VL|VE|ST|MA|ES|X|S|V|G)(?=[A-Z0-9]{2,}$)", "", raw)]):   # vlSDT -> SDT
            if not sym or c: continue
            d = fetch(f"https://api.coingecko.com/api/v3/search?query={urllib.parse.quote(sym)}"); time.sleep(4)
            c = [x for x in (d or {}).get("coins", []) if (x.get("market_cap_rank") or 10**9) <= 1500
                 and x["symbol"].upper() == sym and words & set(re.findall(r"[a-z0-9]{3,}", (x["id"] + " " + x["name"]).lower()))]
        if len({x["id"] for x in c}) != 1:
            # fallback: CoinGecko search on the space-id stem; accept the top ranked hit only if the stem is in its id or name
            stem = re.sub(r"(-?snapshot|-?governance|gov|-?dao|-?community|xyz)$", "", sp["id"].split(".")[0].lower())
            d = fetch(f"https://api.coingecko.com/api/v3/search?query={urllib.parse.quote(stem)}"); time.sleep(4)
            c = [x for x in (d or {}).get("coins", []) if (x.get("market_cap_rank") or 10**9) <= 1500][:1]
            c = [x for x in c if len(stem) >= 3 and (stem in x["id"].lower().split("-") or stem in x["name"].lower().split())]
        sp_map[sp["id"]] = (c[0]["id"], c[0]["symbol"].upper()) if len({x["id"] for x in c}) == 1 else None
    evs = [{"type": "governance_buyback", "asset": sp_map[p["space"]["id"]][0], "sym": sp_map[p["space"]["id"]][1], "t": p["end"],
            "title": f"[{p['space']['id']}] {p['title'][:120]}", "space": p["space"]["id"]}
           for p in passed if sp_map.get(p["space"]["id"])]
    log("mapped", len(evs), "unmapped spaces", sorted(k for k, v in sp_map.items() if not v))
    evs = cap(dedupe(evs))
    out = []
    for e in evs:
        entry = e["t"] + LAG
        s, src = crypto_series(e["sym"], e["t"] - 7200, min(e["t"] + 32 * DAY, NOW), cid=e["asset"], need=entry)
        e["src"] = src
        out.append(crypto_returns(e, s, entry, {"1d": DAY, "7d": 7 * DAY, "30d": 30 * DAY}))
        log(e["asset"], src, e.get("ex_7d"))
    return out

# ---------------------------------------------------------------- daily OHLC for stocks / ETFs / futures
_yahoo_ok = [True]
def yahoo_daily(sym, years):
    if not _yahoo_ok[0]: return None
    for host in ("query2", "query1"):
        d = fetch(f"https://{host}.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(sym)}?range={years}y&interval=1d", tries=2)
        if d:
            r = d["chart"]["result"][0]; q = r["indicators"]["quote"][0]
            return [(datetime.fromtimestamp(t, ET).date().isoformat(), o, c) for t, o, c in zip(r["timestamp"], q["open"], q["close"]) if o and c]
    _yahoo_ok[0] = False; log("yahoo blocked (429) -> Nasdaq API fallback for the rest of the run")
    return None

def nasdaq_daily(sym, years):
    f = (datetime.now(ET) - timedelta(days=365 * years + 5)).date().isoformat(); t = datetime.now(ET).date().isoformat()
    num = lambda x: float(x.replace("$", "").replace(",", "")) if x and x not in ("N/A", "--") else None
    for ac in ("stocks", "etf"):
        d = fetch(f"https://api.nasdaq.com/api/quote/{urllib.parse.quote(sym.replace('-', '.'))}/historical?assetclass={ac}&fromdate={f}&todate={t}&limit=9999", tries=3)
        rows = (((d or {}).get("data") or {}).get("tradesTable") or {}).get("rows") or []
        if rows:
            out = [(datetime.strptime(r["date"], "%m/%d/%Y").date().isoformat(), num(r["open"]), num(r["close"])) for r in rows]
            return sorted(x for x in out if x[1] and x[2])
    return None

_daily = {}
def daily(sym, years=2, proxy=None):
    """[(date, open, close)] ascending. Yahoo first; Nasdaq API fallback (proxy ETF for futures)."""
    k = (sym, years)
    if k not in _daily:
        d = yahoo_daily(sym, years); src = "yahoo"
        if not d: d = nasdaq_daily(proxy or sym, years); src = "nasdaq" + (f":{proxy}" if proxy else "")
        _daily[k] = (d or [], src); time.sleep(0.3)
    return _daily[k]

def fwd(rows, i, n):
    """Open of day i -> close of day i+n-1 (n trading days held)."""
    return rows[i + n - 1][2] / rows[i][1] - 1 if i + n - 1 < len(rows) else None

# ---------------------------------------------------------------- 5 OpenInsider cluster buys
def oi_page(fd_from, fd_to, extra):
    u = ("http://openinsider.com/screener?s=&o=&pl=&ph=&ll=&lh=&fd=-1&fdr=" + urllib.parse.quote(f"{fd_from} - {fd_to}").replace("%20", "+")
         + "&td=0&xp=1&grp=2&sortcol=0&cnt=1000&page=1&" + extra)
    s = fetch(u, raw=True, timeout=90)
    if not s: return []
    s = s.decode("latin-1"); m = re.search(r'<table[^>]*class="tinytable".*?</table>', s, re.S)
    if not m: return []
    t = m.group(0); hdr = [html.unescape(re.sub("<[^>]+>", "", h)).replace("\xa0", " ").strip() for h in re.findall(r"<th[^>]*>(.*?)</th>", t, re.S)]
    out = []
    for r in re.findall(r"<tr[^>]*>(.*?)</tr>", t.split("</thead>")[1], re.S):
        c = dict(zip(hdr, [html.unescape(re.sub("<[^>]+>", "", x)).strip() for x in re.findall(r"<td[^>]*>(.*?)</td>", r, re.S)]))
        tk = re.search(r'href="/([A-Z0-9.\-]+)"', r)
        if tk and c.get("Filing Date"): out.append({**c, "ticker": tk.group(1)})
    return out

def run_insider_cluster_buy():
    rows = {}
    d0 = datetime.now(ET).date() - timedelta(days=365)
    while d0 < datetime.now(ET).date() - timedelta(days=30):             # need 20 trading days after
        d1 = min(d0 + timedelta(days=30), datetime.now(ET).date() - timedelta(days=30))
        f, t = d0.strftime("%m/%d/%Y"), d1.strftime("%m/%d/%Y")
        for extra in ("nil=3", "vl=1000"):                                # >=3 insiders  OR  >= $1M
            for r in oi_page(f, t, extra): rows[(r["ticker"], r["Filing Date"])] = r
            time.sleep(1.5)
        d0 = d1 + timedelta(days=1)
    evs = []
    for (tk, fdt), r in rows.items():
        if not re.fullmatch(r"[A-Z]{1,5}", tk): continue                   # plain common-stock tickers only
        ft = datetime.fromisoformat(fdt).replace(tzinfo=ET)
        evs.append({"type": "insider_cluster_buy", "asset": tk, "t": ft.timestamp(), "filing": fdt,
                    "title": f"{tk} {r.get('Ins')} insiders buy {r.get('Value')}", "n_ins": r.get("Ins"), "value": r.get("Value")})
    evs = dedupe(evs)
    log("insider events", len(evs))
    evs = sorted(random.Random(7).sample(evs, min(150, len(evs))), key=lambda e: e["t"])
    spy, _ = daily("SPY", 2)
    si = {d: i for i, (d, _, _) in enumerate(spy)}
    out = []
    for e in evs:
        rows_, src = daily(e["asset"], 2)
        ft = datetime.fromtimestamp(e["t"], ET)
        # next trading-day open strictly after filing: same day if filed before 09:30 ET, else next day
        cut = ft.date().isoformat() if (ft.hour, ft.minute) < (9, 30) else (ft.date() + timedelta(days=1)).isoformat()
        idx = next((i for i, r in enumerate(rows_) if r[0] >= cut), None)
        e["src"] = src
        if idx is not None and rows_[idx][0] in si and (datetime.fromisoformat(rows_[idx][0]).date() - ft.date()).days <= 5:
            j = si[rows_[idx][0]]
            e["entry_date"], e["entry"] = rows_[idx][0], rows_[idx][1]
            for k, n in (("5d", 5), ("20d", 20)):
                a, b = fwd(rows_, idx, n), fwd(spy, j, n)
                if a is not None and b is not None and rows_[idx + n - 1][0] == spy[j + n - 1][0]:
                    e["r_" + k], e["ex_" + k] = round(a, 4), round(a - b, 4)
        out.append(e)
    return out

# ---------------------------------------------------------------- 6 large-cap big-move follow-through
LARGE = ("AAPL MSFT NVDA AMZN GOOGL META AVGO TSLA BRK-B JPM LLY V UNH XOM MA JNJ PG HD COST ABBV WMT NFLX BAC CRM ORCL CVX KO MRK "
         "AMD PEP ADBE TMO LIN ACN MCD CSCO ABT WFC DHR INTU TXN QCOM IBM AMAT GE CAT VZ DIS PM NOW ISRG AMGN PFE GS UBER CMCSA "
         "NEE SPGI T RTX LOW HON UNP BKNG PGR BLK SYK ELV TJX MS COP C LMT SCHW VRTX PLD MU BSX ADP MDT CB REGN ETN PANW ADI "
         "LRCX KLAC SBUX MMC GILD BMY DE CI SO MO ANET SNPS CDNS INTC PYPL CMG NKE").split()
def run_stock_big_move():
    spy, _ = daily("SPY", 2); si = {d: i for i, (d, _, _) in enumerate(spy)}
    out = []
    for tk in LARGE:
        rows_, src = daily(tk, 2)
        evs = []
        for i in range(1, len(rows_) - 1):
            mv = rows_[i][2] / rows_[i - 1][2] - 1
            if abs(mv) >= 0.08 and rows_[i + 1][0] in si:
                d = datetime.fromisoformat(rows_[i][0]).replace(tzinfo=ET, hour=16)
                e = {"type": "stock_big_move_" + ("up" if mv > 0 else "down"), "asset": tk, "t": d.timestamp(), "move": round(mv, 4),
                     "title": f"{tk} {mv:+.1%} on {rows_[i][0]}", "src": src, "entry_date": rows_[i + 1][0], "entry": rows_[i + 1][1]}
                j = si[rows_[i + 1][0]]
                for k, n in (("5d", 5), ("20d", 20)):
                    a, b = fwd(rows_, i + 1, n), fwd(spy, j, n)
                    if a is not None and b is not None and rows_[i + n][0] == spy[j + n - 1][0]:
                        e["r_" + k], e["ex_" + k] = round(a, 4), round(a - b, 4)
                evs.append(e)
        out += dedupe(evs)
    log("big moves", len(out))
    return out

# ---------------------------------------------------------------- 7 commodity shocks
FUT = {"GC=F": "GLD", "SI=F": "SLV", "HG=F": "CPER", "CL=F": "USO", "NG=F": "UNG", "ZC=F": "CORN", "ZW=F": "WEAT",
       "ZS=F": "SOYB", "KC=F": None, "CC=F": None}          # Nasdaq fallback proxies (no listed coffee/cocoa ETF left)
def run_commodity_shock():
    out = []
    for f, px in FUT.items():
        rows_, src = daily(f, 5, proxy=px) if px or _yahoo_ok[0] else ([], "none")
        if not rows_: log(f, "no data"); continue
        rets = [rows_[i][2] / rows_[i - 1][2] - 1 for i in range(1, len(rows_))]          # rets[i-1] = day i
        evs = []
        for i in range(21, len(rows_) - 1):
            prior = rets[i - 21:i - 1]                                                   # 20 returns before day i
            sd = statistics.stdev(prior)
            r = rets[i - 1]
            if sd > 0 and abs(r / sd) >= 2.5:
                d = datetime.fromisoformat(rows_[i][0]).replace(tzinfo=ET, hour=17)
                e = {"type": "commodity_shock_" + ("up" if r > 0 else "down"), "asset": f, "t": d.timestamp(), "z": round(r / sd, 2),
                     "move": round(r, 4), "title": f"{f} {r:+.1%} (z {r / sd:+.1f}) on {rows_[i][0]}", "src": src,
                     "entry_date": rows_[i + 1][0], "entry": rows_[i + 1][1]}
                for k, n in (("5d", 5), ("20d", 20)):
                    a = fwd(rows_, i + 1, n)
                    if a is not None: e["r_" + k] = e["ex_" + k] = round(a, 4)     # absolute, no benchmark
                evs.append(e)
        out += dedupe(evs)
        log(f, src, len(rows_), "shocks", len(evs))
    return out

# ---------------------------------------------------------------- 8 Polymarket big moves
SPORTS = re.compile(r"\b(vs\.?|v\.|nba|nfl|nhl|mlb|ufc|epl|premier league|la liga|serie a|bundesliga|champions league|world cup|super bowl|"
                    r"stanley cup|world series|grand prix|f1|formula 1|wimbledon|open championship|masters|atp|wta|fifa|uefa|ncaa|"
                    r"mvp|playoffs?|finals?|match|game \d|win the \d{4}|o/u|over/under|spread|esports|lol:|cs2|dota|valorant|fight)\b", re.I)
def run_polymarket_big_move():
    mk = {}
    for closed in ("true", "false"):
        for off in range(0, 3000, 100):                                     # gamma caps a page at 100
            d = fetch(f"https://gamma-api.polymarket.com/markets?closed={closed}&limit=100&offset={off}&order=volumeNum&ascending=false"
                      f"&end_date_min={datetime.fromtimestamp(NOW - WIN, UTC).strftime('%Y-%m-%d')}&volume_num_min=1000000")
            if not d: break
            for m in d: mk[m["id"]] = m
            if len(d) < 100: break
    ms = [m for m in mk.values() if not m.get("gameStartTime") and not m.get("sportsMarketType")
          and not SPORTS.search(m["question"] + " " + " ".join(e.get("slug", "") for e in m.get("events") or []))
          and m.get("clobTokenIds")]
    ms.sort(key=lambda m: -float(m.get("volumeNum") or 0)); ms = ms[:300]
    log("polymarket markets", len(mk), "non-sports liquid", len(ms))
    out = []
    for n, m in enumerate(ms):
        tok = json.loads(m["clobTokenIds"])[0]
        st = datetime.fromisoformat(m["startDate"].replace("Z", "+00:00")).timestamp() if m.get("startDate") else NOW - WIN
        en = datetime.fromisoformat(m["endDate"].replace("Z", "+00:00")).timestamp() if m.get("endDate") else NOW
        ct = datetime.fromisoformat(m["closedTime"].replace(" ", "T").replace("+00", "+00:00")).timestamp() if m.get("closedTime") else None
        a0, a1 = max(st, NOW - WIN), min(ct or NOW, en, NOW)
        h = []
        for a in range(int(a0), int(a1), 14 * DAY):
            d = fetch(f"https://clob.polymarket.com/prices-history?market={tok}&startTs={a}&endTs={min(a + 14 * DAY, int(a1))}&fidelity=60")
            h += [(x["t"], x["p"]) for x in (d or {}).get("history", [])]
            time.sleep(0.25)
        h = sorted(set(h))
        evs = []
        resolve = min(x for x in (ct, en) if x)
        for t, p in h:
            _, p0 = before(h, t - DAY, 2 * 3600)
            if p0 is None or abs(p - p0) < 0.15: continue
            if resolve - t < 8 * DAY or not (0.03 <= p <= 0.97): continue     # exclude resolution jumps / pinned prices
            sgn = 1 if p > p0 else -1
            e = {"type": "polymarket_big_move", "asset": m["id"], "t": t, "move": round(p - p0, 3),
                 "title": f"{m['question'][:90]} {p0:.2f}->{p:.2f}", "src": "clob"}
            te, pe = at(h, t + LAG, 3 * 3600)
            if pe is None: continue
            e["entry_t"], e["entry"] = te, pe
            for k, hh in (("3d", 3 * DAY), ("7d", 7 * DAY)):
                tx, px = at(h, te + hh, 6 * 3600)
                if px is not None: e["r_" + k] = e["ex_" + k] = round(sgn * (px - pe), 3)   # continuation in prob. points
            evs.append(e)
        out += dedupe(evs)
        if n % 10 == 0 or not h: log(n, m["question"][:50], len(h), len(evs))
    return out

TYPES = {"coinbase_listing": run_coinbase_listing, "upbit_krw_listing": run_upbit_krw_listing, "bithumb_krw_listing": run_bithumb_krw_listing,
         "token_unlock": run_token_unlock, "governance_buyback": run_governance_buyback, "insider_cluster_buy": run_insider_cluster_buy,
         "stock_big_move": run_stock_big_move, "commodity_shock": run_commodity_shock, "polymarket_big_move": run_polymarket_big_move}

# ---------------------------------------------------------------- aggregation
# (reported type, source-type, key horizon, secondary horizons)
REPORT = [("coinbase_listing", "coinbase_listing", "7d", ["1d", "30d"]),
          ("upbit_krw_listing", "upbit_krw_listing", "7d", ["1d", "30d"]),
          ("bithumb_krw_listing", "bithumb_krw_listing", "7d", ["1d", "30d"]),
          ("token_unlock", "token_unlock", "m7p7", ["m7p0", "m1p7"]),
          ("token_unlock_dm1", "token_unlock", "m1p7", []),
          ("governance_buyback", "governance_buyback", "7d", ["30d"]),
          ("insider_cluster_buy", "insider_cluster_buy", "20d", ["5d"]),
          ("stock_big_move_up", "stock_big_move_up", "20d", ["5d"]),
          ("stock_big_move_down", "stock_big_move_down", "20d", ["5d"]),
          ("commodity_shock_up", "commodity_shock_up", "20d", ["5d"]),
          ("commodity_shock_down", "commodity_shock_down", "20d", ["5d"]),
          ("polymarket_big_move", "polymarket_big_move", "7d", ["3d"]),
          ("polymarket_big_move_up", "polymarket_big_move:up", "7d", ["3d"]),
          ("polymarket_big_move_down", "polymarket_big_move:down", "7d", ["3d"])]
HLABEL = {"m7p7": "-7d..+7d", "m7p0": "-7d..0", "m1p7": "-1d..+7d"}
PLAYBOOK = {  # from playbook_v1.md (audited 2026-09-26); not re-run here
    "binance_new_listing": {"n": 54, "horizon": "7d", "median_excess_7d": -0.18, "mean_excess": None, "hit_rate": 0.30, "ci95": [-0.30, -0.09],
                            "recent120": {"n": None, "median": None}, "note": "Binance listing/HODLer pops fade: short/avoid long", "weak": False, "source": "playbook_v1"},
    "binance_perp_launch": {"n": 24, "horizon": "7d", "median_excess_7d": -0.15, "mean_excess": None, "hit_rate": 0.38, "ci95": [-0.30, 0.15],
                            "recent120": {"n": None, "median": None}, "note": "Mild negative, CI spans 0: no edge", "weak": False, "source": "playbook_v1"},
    "binance_add_existing": {"n": 8, "horizon": "7d", "median_excess_7d": -0.04, "mean_excess": None, "hit_rate": None, "ci95": None,
                             "recent120": {"n": None, "median": None}, "note": "No edge", "weak": True, "source": "playbook_v1"}}

def q(x, p):
    s = sorted(x); k = (len(s) - 1) * p; f = math.floor(k)
    return s[f] + (s[min(f + 1, len(s) - 1)] - s[f]) * (k - f)

def boot_ci(x, n=1000):
    rng = random.Random(11)
    meds = sorted(statistics.median(rng.choices(x, k=len(x))) for _ in range(n))
    return [round(meds[int(0.025 * n)], 4), round(meds[int(0.975 * n) - 1], 4)]

def stats(x):
    return {"n": len(x), "median": round(statistics.median(x), 4), "mean": round(statistics.mean(x), 4),
            "hit_rate": round(sum(v > 0 for v in x) / len(x), 3), "p25": round(q(x, .25), 4), "p75": round(q(x, .75), 4),
            "ci95": boot_ci(x)}

LABEL = {"coinbase_listing": "Coinbase listing", "upbit_krw_listing": "Upbit KRW listing", "bithumb_krw_listing": "Bithumb KRW listing",
         "token_unlock": "Big cliff unlock (entered 7d before)", "token_unlock_dm1": "Big cliff unlock (entered 1d before)",
         "governance_buyback": "Passed buyback/fee-switch vote", "insider_cluster_buy": "Insider cluster buy",
         "stock_big_move_up": "Large-cap +8% day", "stock_big_move_down": "Large-cap -8% day",
         "commodity_shock_up": "Commodity up-shock", "commodity_shock_down": "Commodity down-shock",
         "polymarket_big_move": "Polymarket 15-pt move", "polymarket_big_move_up": "Polymarket 15-pt jump up",
         "polymarket_big_move_down": "Polymarket 15-pt drop"}
def note_for(name, s, weak):
    if s["n"] == 0: return "not measured"
    lo, hi = s["ci95"]; sign = -1 if hi < 0 else 1 if lo > 0 else 0
    L = LABEL.get(name, name); pre = "Weak sample: " if weak else ""
    if sign == 0: return pre + f"{L}: no edge (CI spans 0), score neutral"
    if name.startswith("polymarket_big_move"):
        return pre + f"{L}: {'continues - follow the move' if sign > 0 else 'mean-reverts - fade the move'}"
    if name == "stock_big_move_down" or name == "commodity_shock_down":
        return pre + f"{L}: {'keeps falling - do not catch the knife' if sign < 0 else 'rebounds - reversal long'}"
    if name in ("stock_big_move_up", "commodity_shock_up"):
        return pre + f"{L}: {'fades - do not chase' if sign < 0 else 'continues - momentum long'}"
    return pre + f"{L}: {'drifts down - avoid long / short' if sign < 0 else 'drifts up - supports a long'}"

def score_adj(d):
    """Pre-registered: 0 unless n>=20 and CI excludes 0; +-2 if both halves and recent120 (n>=10) agree in sign, else +-1."""
    if not d.get("ci95") or d["n"] < 20: return 0
    lo, hi = d["ci95"]; sign = -1 if hi < 0 else 1 if lo > 0 else 0
    if not sign: return 0
    halves = d.get("halves_median") or [0, 0]
    rec = d.get("recent120") or {}
    agree = all(h * sign > 0 for h in halves) and (rec.get("n") or 0) >= 10 and (rec.get("median") or 0) * sign > 0
    return sign * (2 if agree else 1)

def pct(v, name):
    if v is None: return "n/a"
    v = round(v, 3) + 0.0
    return f"{v * 100:+.1f} pts" if name.startswith("polymarket") else f"{v * 100:+.1f}%"

def write_md(types):
    L = ["PRIORS (backtest2.py, generated " + datetime.now(UTC).strftime("%Y-%m-%d") + "; entry >=3h after the event / next session open; "
         "excess vs BTC (crypto) or SPY (stocks); commodities absolute; Polymarket in prob. points, + = continuation)"]
    for name, _, _, extra in REPORT:
        d = types.get(name)
        if not d or not d["n"]:
            L.append(f"- {name}: not measured ({NOT_MEASURED.get(name, 'no events')})."); continue
        r = d["recent120"]
        L.append(f"- {name}: {d['horizon']} median {pct(d['median_excess_7d'], name)} (n={d['n']}, 95% CI {pct(d['ci95'][0], name)} to "
                 f"{pct(d['ci95'][1], name)}), hit {d['hit_rate'] * 100:.0f}%; last 120d {pct(r['median'], name)} (n={r['n']})"
                 + (f"; halves {pct(d['halves_median'][0], name)} / {pct(d['halves_median'][1], name)}" if d.get("halves_median") else "")
                 + "".join(f"; {HLABEL.get(k, k)} {pct(d['h_' + HLABEL.get(k, k)]['median'], name)} [CI {pct(d['h_' + HLABEL.get(k, k)]['ci95'][0], name)},"
                           f" {pct(d['h_' + HLABEL.get(k, k)]['ci95'][1], name)}]" for k in extra[:1] if d.get("h_" + HLABEL.get(k, k)))
                 + (". WEAK (n<20)." if d["weak"] else ".") + CAVEAT.get(name, ""))
    L += ["- binance_new_listing (playbook_v1): 7d median -18% (n=54, CI -30% to -9%), hit 30%.",
          "- binance_perp_launch (playbook_v1): 7d median -15% (n=24, CI -30% to +15%), hit 38%. No edge.",
          "- binance_add_existing (playbook_v1): 7d median -4% (n=8). WEAK, no edge."]
    L += ["", "SCORING RULES FROM PRIORS (pre-registered: only n>=20 and CI excluding 0; +-2 only if both halves and last-120d agree in sign)"]
    any_rule = False
    for name, _, _, _ in REPORT:
        d = types.get(name)
        if not d: continue
        a = score_adj(d)
        if a:
            any_rule = True
            what = "an idea that buys in the direction of the jump" if name.startswith("polymarket") else "a LONG idea resting on this catalyst"
            if name == "token_unlock_dm1": continue          # same catalyst as token_unlock
            L.append(f"- {name}: {a:+d} to {what} ({d['note']}).")
    L.append("- Listing catalysts do not stack: apply the single largest listing penalty once per coin (Upbit+Bithumb same day = -2, not -4).")
    L.append("- binance_new_listing (playbook_v1): -2 to a LONG idea whose only catalyst is a listing / HODLer airdrop.")
    L.append("- All other types: 0 (no adjustment; CI spans 0 or n<20). Replace priors once live ledger n>=8 per type at 7d.")
    open(os.path.join(HERE, "priors_v2.md"), "w").write("\n".join(L) + "\n")

NOT_MEASURED = {}
CAVEAT = {"upbit_krw_listing": " Listing time = first KRW candle (trading open, usually 1-3h after the notice); delisted markets missing.",
          "bithumb_krw_listing": " Listing time = first KRW candle; delisted markets missing. 56 coins overlap Upbit: one KRW listing prior per coin, do not add both.",
          "coinbase_listing": " Only currently-listed products (new_at); delisted coins missing.",
          "token_unlock": " Scheduled event, so the -7d entry is actionable; same catalyst as token_unlock_dm1 (do not add both).",
          "commodity_shock_up": " Nasdaq ETF proxies (Yahoo blocked); coffee/cocoa missing.",
          "commodity_shock_down": " Raw return (negative = continuation). Nasdaq ETF proxies; coffee/cocoa missing.",
          "polymarket_big_move_up": " 84 of 133 are 'by <date>' markets, so part is time decay, not only overreaction.",
          "stock_big_move_down": " 21% of events fall in Apr-2025 (tariff shock)."}

def aggregate():
    allev = []
    for f in sorted(os.listdir(CACHE)) if os.path.isdir(CACHE) else []:
        if f.endswith(".json"): allev += json.load(open(os.path.join(CACHE, f)))
    json.dump(allev, open(os.path.join(HERE, "backtest2_events.json"), "w"), indent=0)
    types = {}
    for name, src, hk, extra in REPORT:
        src, _, side = src.partition(":")
        R = [e for e in allev if e["type"] == src and (not side or (e.get("move", 0) > 0) == (side == "up")) and not (e["type"].endswith("listing") and is_stable(e.get("asset", "")))]   # stablecoin "listings" are not catalysts
        x = [e["ex_" + hk] for e in R if e.get("ex_" + hk) is not None]
        if not R and not x:
            continue
        s = stats(x) if x else {"n": 0}
        rec = [e["ex_" + hk] for e in R if e.get("ex_" + hk) is not None and e["t"] > NOW - 120 * DAY]
        weak = len(x) < 20
        d = {"n": len(x), "horizon": HLABEL.get(hk, hk), "median_excess_7d": s.get("median"), "mean_excess": s.get("mean"),
             "hit_rate": s.get("hit_rate"), "p25": s.get("p25"), "p75": s.get("p75"), "ci95": s.get("ci95"),
             "recent120": {"n": len(rec), "median": round(statistics.median(rec), 4) if rec else None},
             "events_found": len(R), "weak": weak, "note": note_for(name, s, weak)}
        # first half / second half robustness
        xs = sorted((e["t"], e["ex_" + hk]) for e in R if e.get("ex_" + hk) is not None)
        if len(xs) >= 10:
            h = len(xs) // 2
            d["halves_median"] = [round(statistics.median([v for _, v in xs[:h]]), 4), round(statistics.median([v for _, v in xs[h:]]), 4)]
        for k in extra:
            y = [e["ex_" + k] for e in R if e.get("ex_" + k) is not None]
            if y: d["h_" + HLABEL.get(k, k)] = {"n": len(y), "median": round(statistics.median(y), 4), "hit_rate": round(sum(v > 0 for v in y) / len(y), 3),
                                                "ci95": boot_ci(y)}
        d["benchmark"] = ("BTC" if name.startswith(("coinbase", "upbit", "bithumb", "token", "governance")) else
                          "SPY" if name.startswith(("insider", "stock")) else "none (absolute)")
        if name.startswith("polymarket"): d["units"] = "probability points, signed in the direction of the move (+ = continuation)"
        types[name] = d
    return allev, types

def main(argv):
    os.makedirs(CACHE, exist_ok=True)
    want = argv or list(TYPES)
    if want == ["report"]: want = []
    for t in want:
        if t not in TYPES: sys.exit(f"unknown type {t}; choose from {', '.join(TYPES)}")
        log("=== running", t)
        evs = TYPES[t]()
        json.dump(evs, open(os.path.join(CACHE, t + ".json"), "w"), indent=0)
    allev, types = aggregate()
    out = {"generated": datetime.now(UTC).isoformat(timespec="seconds"),
           "method": ("Entry = first price >=3h after the event became public (crypto: hourly; stocks/commodities: next session open; "
                      "Polymarket: hourly, +3h). Excess vs BTC (crypto) or SPY (US stocks); commodities absolute; Polymarket in probability "
                      "points signed in the move direction; commodity returns are raw (for down-shocks negative = continuation). "
                      "Insider entry = first regular-session open after the filing timestamp (same day if filed before 09:30 ET). One event per asset per 7 days. Median CI = 1000-resample bootstrap. "
                      "weak = n<20. Crypto types capped at 120 random events (seed 7), insider buys at 150."),
           "types": {**types, **PLAYBOOK}}
    for k, d in types.items(): d["score_adj"] = score_adj(d)
    json.dump(out, open(os.path.join(HERE, "priors.json"), "w"), indent=1)
    write_md(types)
    print(json.dumps(types, indent=1))

if __name__ == "__main__":
    main(sys.argv[1:])
