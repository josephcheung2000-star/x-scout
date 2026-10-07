#!/usr/bin/env python3
"""X-scout second-tier sources (no X, no keys, stdlib only). Run AFTER `ledger.py pull`, in the repo dir.
Writes sources2.json = {at, errors, counts, triggers, <one list per key>}; prints one JSON line of counts only.
Reads/writes ONLY state["src2_seen"] (sorted lists, capped at 5000); state.json is rewritten atomically and only
if it loaded as a dict. Never raises; always exits 0; per-request timeout 25 s; whole run capped at ~170 s.
Usage: python3 sources2.py [hours=3.5]
KEYS
  coinbase_new     Advanced Trade SPOT products new_at in window or not yet seen (+ status RSS: Markets Open /
                   full trading / Auction / Suspension in window)
  upbit_krw_new    KRW market set diff vs seen; kind new | removed | warning_on/off | caution_on/off
                   (market/all?is_details=true: market_event.warning, market_event.caution.{PRICE_FLUCTUATIONS,
                   TRADING_VOLUME_SOARING, DEPOSIT_AMOUNT_SOARING, GLOBAL_PRICE_DIFFERENCES, CONCENTRATION_OF_SMALL_ACCOUNTS})
  bithumb_krw_new  same for Bithumb (market_warning CAUTION = 투자유의 = warning_on/off)
  bithumb_notices  feed-api notices in window (the feed returns the latest 5 only); "events" = parsed title events
  bithumb_wallet   public/assetsstatus/ALL diff vs seen: deposit or withdrawal suspended (wallet_suspend) / resumed
  upbit_notices    Upbit announcements (unofficial api-manager endpoint, often Cloudflare-blocked: then a note, not an
                   error); title events as for bithumb_notices
  unlocks_14d      DefiLlama cliff unlocks in the next 14 days, per protocol per UTC day; kept if >= 1% of
                   DefiLlama unlocked supply or >= $10M (price: CoinGecko /simple/price, fallback coins.llama.fi)
  governance       Snapshot proposals created <= 48h or active, space >= 1000 followers, buyback / fee switch /
                   revenue share / tokenomics / burn (burn: title only); stablecoin/risk-parameter titles dropped
  etf_flows        SoSoValue US BTC + ETH spot ETF daily net flows, last 5 days; z of latest vs prior 60 days
  insider_buys     OpenInsider screener (P, filed <= 3 days, >= $100k) + latest cluster buys filed <= 3 days
  stock_movers     Yahoo predefined screeners day_gainers / day_losers / most_actives; mcap >= $2B and
                   |chg| >= 8% or volume >= 3x 3-month average
  commodities      Yahoo chart range=1mo: futures + DXY, 10y, VIX, SPY, QQQ, TLT; z_1d = 1d return / stdev of the
                   20 prior daily returns
  regime           BTC (Binance data-api klines, Hyperliquid funding/OI) and SPY (Yahoo 1y) vs 200-day SMA
                   rule: dist = price/SMA200-1; risk-on if dist >= +2% (BTC also needs 30d return > 0);
                   risk-off if dist <= -2% (BTC also needs 30d return < 0); else neutral. BTC "crowded" if
                   |funding annualised| >= 30% (reported, does not change the label)
  polymarket_extra gamma markets, liquidity >= $50k: kind new (created <= 48h) | mover (top 24h volume or
                   |1d change| in politics/geopolitics/economy/finance/crypto/business) | overround (neg-risk events,
                   liquidity >= $100k, |sum(Yes) - 1| >= 0.05: a hint, not proof)
TRIGGERS (key is stable per event)
  listing_krw           new KRW market on Upbit / Bithumb                         listing_krw:<ex>:<market>
  listing_coinbase      new Coinbase product with a new base asset, or status "Markets Open"   listing_coinbase:<BASE>
  unlock                >= 2% of unlocked supply within 7 days                    unlock:<slug>:<date>
  governance            buyback / repurchase / fee switch in title or twice in body   governance:<proposal id>
  insider_buy           single purchase >= $1M or cluster of >= 3 insiders        insider_buy:<ticker>:<filing date>
  stock_move            |chg| >= 10% and mcap >= $2B                              stock_move:<ticker>:<trade date>
  commodity_move        |z_1d| >= 2.5 or |5d change| >= 8%                        commodity_move:<symbol>:<last date>
  etf_flow              |z| >= 2.5                                                etf_flow:<btc|eth>:<date>
  polymarket_new        new market (<= 48h), liquidity >= $100k, not sports / daily strike   polymarket_new:<slug>
  polymarket_mispricing overround flag, not sports, and (event not neg-risk-augmented, or best-ask sum < 1 or
                        best-bid sum > 1)                                         polymarket_mispricing:<event slug>
  exchange_flag         Korean exchange flag/notice for an asset: Upbit caution / warning ON or OFF, Bithumb investment
                        warning designated / lifted, Bithumb/Upbit deposit-withdrawal suspended / resumed, delisting
                        notice                                   krw_flag:<upbit|bithumb>:<event>:<ASSET>
                        (event: warning_on|warning_off|caution_on|caution_off|wallet_suspend|wallet_resume|delisting;
                        flag diff and notice share the key, so each fires once per 7 days per asset)"""
import json, os, re, sys, time, math, html, statistics, urllib.request, urllib.parse
import concurrent.futures as cf
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

T0 = time.time()
DEADLINE = T0 + 170                       # hard cap for the whole run (the workflow gives it 300 s)
H = float(sys.argv[1]) if len(sys.argv) > 1 and re.fullmatch(r"[\d.]+", sys.argv[1]) else 3.5
NOW = datetime.now(timezone.utc)
UTC, NY, SEOUL = timezone.utc, ZoneInfo("America/New_York"), ZoneInfo("Asia/Seoul")
UA = "Mozilla/5.0 (compatible; x-scout-research/1.0)"
BUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
SF, CAP = "state.json", 5000
ERRORS = []
NOTES = []                                # soft failures of unofficial endpoints: reported, never an error line

def get(url, data=None, ua=UA, ct=None, raw=False):
    h = {"User-Agent": ua, "Accept": "application/json, text/html;q=0.9, */*;q=0.8"}
    if ct: h["Content-Type"] = ct
    with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=h), timeout=25) as r:
        b = r.read()
    return b if raw else json.loads(b)

def iso(d):
    return d.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

def ts_iso(t):
    return iso(datetime.fromtimestamp(t, UTC))

def parse_iso(s):
    return datetime.fromisoformat(str(s).replace("Z", "+00:00"))

def in_window(at, hours=H):
    try: return NOW - parse_iso(at) <= timedelta(hours=hours)
    except Exception: return False

def err(src, e):
    ERRORS.append(f"{src}: {type(e).__name__} {str(e)[:100]}".strip())

def num(s):
    """'+$1,234,567' / '$81.19' / '+7%' / 'New' -> float or None"""
    try: return float(re.sub(r"[^\d.\-]", "", str(s).replace("+", "")))
    except Exception: return None

def rnd(x, n=2):
    return None if x is None or (isinstance(x, float) and not math.isfinite(x)) else round(x, n)

# ---------------------------------------------------------------- state (only state["src2_seen"])
def load_state():
    try:
        s = json.load(open(SF))
        return s if isinstance(s, dict) else None
    except Exception:
        return None

STATE = load_state()
SEEN = {}
if STATE is not None:
    raw = STATE.get("src2_seen") if isinstance(STATE.get("src2_seen"), dict) else {}
    SEEN = {k: set(v) for k, v in raw.items() if isinstance(v, list)}
NEW_SEEN = {}                                   # key -> set to store (sources only set what they fetched OK)

def baseline(key):
    """Previously seen set, or None when there is no baseline (first run / no state): seed silently."""
    return SEEN.get(key) if STATE is not None else None

# ---------------------------------------------------------------- 1 coinbase
CB_STATUS_RX = re.compile(r"Markets Open|full trading|Auction|Suspension", re.I)

def coinbase_new():
    out, trig = [], []
    try:
        ps = get("https://api.coinbase.com/api/v3/brokerage/market/products?product_type=SPOT")["products"]
        ids = {p["product_id"] for p in ps}
        prev = baseline("coinbase")
        prev_bases = {i.split("-")[0] for i in (prev or ())}
        for p in ps:
            pid, na = p["product_id"], p.get("new_at")
            fresh = bool(na) and in_window(na)
            unseen = prev is not None and pid not in prev
            if not (fresh or unseen): continue
            base = p.get("base_display_symbol") or pid.split("-")[0]
            out.append({"src": "coinbase", "id": pid, "title": f"Coinbase product {pid} ({p.get('base_name')})",
                        "at": na or iso(NOW), "url": f"https://www.coinbase.com/advanced-trade/spot/{pid}",
                        "kind": "unseen" if unseen else "new_at", "base": base, "status": p.get("status"),
                        "new": p.get("new"), "new_at": na, "view_only": p.get("view_only"), "limit_only": p.get("limit_only"),
                        "auction_mode": p.get("auction_mode"), "trading_disabled": p.get("trading_disabled"),
                        "price": num(p.get("price")), "vol24h_quote": num(p.get("approximate_quote_24h_volume"))})
            if prev is not None and pid.split("-")[0] not in prev_bases:
                trig.append({"type": "listing_coinbase", "key": f"listing_coinbase:{base}", "asset": base,
                             "what": f"Coinbase new product {pid}", "url": f"https://www.coinbase.com/advanced-trade/spot/{pid}"})
        if len(ids) >= 100:
            NEW_SEEN["coinbase"] = (prev or set()) | ids
    except Exception as e:
        err("coinbase_products", e)
    try:
        s = get("https://status.exchange.coinbase.com/history.rss", ua=BUA, raw=True).decode("utf-8", "replace")
        for it in re.findall(r"<item>(.*?)</item>", s, re.S):
            f = lambda t: html.unescape((re.search(rf"<{t}>(.*?)</{t}>", it, re.S) or [None, ""])[1].strip())
            title = f("title")
            if not CB_STATUS_RX.search(title): continue
            at = iso(datetime.strptime(f("pubDate"), "%a, %d %b %Y %H:%M:%S %z"))
            if not in_window(at): continue
            m = re.match(r"([A-Z0-9]+)-[A-Z]+", title)
            out.append({"src": "cb_status", "id": f("guid") or f("link"), "title": title, "at": at, "url": f("link"),
                        "kind": "status", "base": m.group(1) if m else None})
            if m and re.search(r"Markets Open", title, re.I):
                trig.append({"type": "listing_coinbase", "key": f"listing_coinbase:{m.group(1)}", "asset": m.group(1),
                             "what": f"Coinbase: {title}", "url": f("link")})
    except Exception as e:
        err("coinbase_status", e)
    return out, trig

# ---------------------------------------------------------------- 2 upbit / bithumb KRW
def krw_diff(ex, rows, flags):
    """rows: {market: english_name}; flags: {flagkey: set(markets)} -> items, triggers; updates NEW_SEEN."""
    out, trig = [], []
    url = (lambda m: f"https://upbit.com/exchange?code=CRIX.UPBIT.{m}") if ex == "upbit" else \
          (lambda m: f"https://www.bithumb.com/react/trade/order/{m.split('-')[1]}-KRW")
    cur = set(rows)
    prev = baseline(f"{ex}_krw")
    if len(cur) < 50 or (prev and len(cur) < 0.5 * len(prev)):
        raise ValueError(f"only {len(cur)} KRW markets returned; diff skipped")
    if prev is not None:
        for m in sorted(cur - prev):
            out.append({"src": ex, "id": f"{m}:new", "title": f"{ex.capitalize()} new KRW market {m} ({rows[m]})",
                        "at": iso(NOW), "url": url(m), "kind": "new", "market": m, "name": rows[m]})
            trig.append({"type": "listing_krw", "key": f"listing_krw:{ex}:{m}", "asset": m.split("-")[1],
                         "what": f"{ex.capitalize()} new KRW market {m} ({rows[m]})", "url": url(m)})
    live = baseline(f"{ex}_krw_live")                          # last run's listing, for "removed" (reported once)
    if live is not None:
        for m in sorted(live - cur):
            out.append({"src": ex, "id": f"{m}:removed", "title": f"{ex.capitalize()} KRW market gone: {m}",
                        "at": iso(NOW), "url": url(m), "kind": "removed", "market": m})
    NEW_SEEN[f"{ex}_krw"] = (prev or set()) | cur                # union: a glitchy partial list never re-fires "new"
    NEW_SEEN[f"{ex}_krw_live"] = set(cur)
    for fk, ms in flags.items():
        pf = baseline(f"{ex}_{fk}")
        if ms is None:                                     # the detail fields were missing: no diff, keep the old set
            continue
        if pf and len(pf) >= 5 and not ms:
            err(f"{ex}_{fk}", ValueError(f"all {len(pf)} {fk} flags vanished at once; diff skipped")); continue
        if pf is not None:
            for m, on in [(m, True) for m in sorted(ms - pf)] + [(m, False) for m in sorted((pf - ms) & cur)]:
                ev_ = f"{fk}_{'on' if on else 'off'}"
                out.append({"src": ex, "id": f"{m}:{ev_}", "title": f"{ex.capitalize()} {fk} flag {'ON' if on else 'OFF'} {m} ({rows.get(m)})",
                            "at": iso(NOW), "url": url(m), "kind": ev_, "market": m, "name": rows.get(m),
                            "delisting_risk": fk == "warning" and on})
                trig.append(flag_trigger(ex, ev_, m.split("-")[1], f"{ex.capitalize()} {fk} flag {'ON' if on else 'OFF'} {m} ({rows.get(m)})", url(m)))
        NEW_SEEN[f"{ex}_{fk}"] = set(ms)                   # current set, so a re-flag fires again
    return out, trig

def flag_trigger(ex, event, asset, what, url):
    """One key per exchange + event + asset: the flag diff and the exchange notice of the same event share it."""
    return {"type": "exchange_flag", "key": f"krw_flag:{ex}:{event}:{asset.upper()}", "asset": asset.upper(),
            "event": event, "what": what, "url": url}

def upbit_krw_new():
    try:
        d = [m for m in get("https://api.upbit.com/v1/market/all?is_details=true") if m["market"].startswith("KRW-")]
        rows = {m["market"]: m.get("english_name") for m in d}
        ev = lambda m: m.get("market_event") or {}
        detailed = any("market_event" in m or "market_warning" in m for m in d)
        flags = {"warning": {m["market"] for m in d if ev(m).get("warning") or m.get("market_warning") == "CAUTION"} if detailed else None,
                 "caution": {m["market"] for m in d if any((ev(m).get("caution") or {}).values())} if detailed else None}
        return krw_diff("upbit", rows, flags)
    except Exception as e:
        err("upbit_krw", e); return [], []

def bithumb_krw_new():
    try:
        d = [m for m in get("https://api.bithumb.com/v1/market/all?isDetails=true") if m["market"].startswith("KRW-")]
        rows = {m["market"]: m.get("english_name") for m in d}
        detailed = any("market_warning" in m for m in d)
        return krw_diff("bithumb", rows, {"warning": {m["market"] for m in d if m.get("market_warning") not in (None, "NONE")} if detailed else None})
    except Exception as e:
        err("bithumb_krw", e); return [], []

# ---------------------------------------------------------------- 3 bithumb / upbit notices
# title -> events; the first matching rule per group wins (an "off" wording is tested before the "on" wording)
NOTICE_RULES = [
    ("warning_off", re.compile(r"(유의\s*종목|투자\s*유의|거래\s*유의|주의\s*종목).{0,30}해제|"
                               r"(?i:(warning|caution) (designation )?(lifted|removed|released)|release of .{0,20}(warning|caution))")),
    ("warning_on", re.compile(r"(투자\s*유의|거래\s*유의|유의)\s*(종목\s*)?(지정|촉구)|(?i:investment warning|designated as .{0,20}(caution|warning))")),
    ("caution_on", re.compile(r"주의\s*종목\s*지정|(?i:caution (designation|flag))")),
    ("wallet_resume", re.compile(r"(입출금|입금|출금).{0,30}(재개|(중지|중단)\s*해제)|(?i:(deposits?|withdrawals?).{0,30}(resum|reopen|re-open))")),
    ("wallet_suspend", re.compile(r"(입출금|입금|출금)\s*(서비스\s*)?(일시\s*)?(중지|중단)|(?i:(deposits?|withdrawals?).{0,30}suspen)")),
    ("delisting", re.compile(r"거래\s*지원\s*종료|상장\s*폐지|(?i:delist|end of trading support|termination of trading support)")),
]
GROUPS = (("warning_off", "warning_on", "caution_on"), ("wallet_resume", "wallet_suspend"), ("delisting",))
TICKER_RX = re.compile(r"\(([A-Z0-9]{2,12})\)")

def notice_events(title):
    """[(event, ASSET)] for an exchange notice title such as '자이(XAI) 거래유의종목 지정' or
    '아이오텍스(IOTX) 입출금 일시 중지 안내'. Assets are the upper-case tickers in parentheses."""
    title = str(title or "")
    assets = [a for a in TICKER_RX.findall(title) if a not in ("KRW", "BTC", "USDT")] or \
             [a for a in TICKER_RX.findall(title)]
    rules = dict(NOTICE_RULES)
    evs = []
    for g in GROUPS:
        hit = next((e for e in g if rules[e].search(title)), None)
        if hit: evs.append(hit)
    return [(e, a) for e in evs for a in dict.fromkeys(assets)]

def _notice_items(ex, rows):
    out, trig = [], []
    for n in rows:
        evs = notice_events(n["title"])
        out.append({**n, "events": [f"{e}:{a}" for e, a in evs]} if evs else n)
        for e, a in evs:
            trig.append(flag_trigger(ex, e, a, f"{ex.capitalize()} notice: {n['title']}", n.get("url")))
    return out, trig

def bithumb_notices():
    rows = []
    try:
        for n in get("https://feed-api.bithumb.com/v1/notices", ua=BUA):
            at = iso(datetime.fromisoformat(n["published_at"]).replace(tzinfo=SEOUL))
            if not in_window(at): continue
            rows.append({"src": "bithumb_notice", "id": str(n["pc_url"]).rsplit("/", 1)[-1], "title": n["title"], "at": at,
                         "url": n["pc_url"], "category": ",".join(n.get("categories") or [])})
    except Exception as e:
        err("bithumb_notices", e)
    return _notice_items("bithumb", rows)

def upbit_notices():
    """Upbit announcements. The api-manager endpoint is unofficial and sits behind Cloudflare (blocked from some cloud
    IPs): any failure or unexpected shape is a NOTE, never an error, and yields nothing."""
    rows = []
    try:
        d = get("https://api-manager.upbit.com/api/v1/announcements?os=web&page=1&per_page=20&category=all", ua=BUA)
        notices = ((d or {}).get("data") or {}).get("notices") if isinstance(d, dict) else None
        if not isinstance(notices, list):
            NOTES.append("upbit_notices: unexpected response shape"); return [], []
        for n in notices:
            if not isinstance(n, dict) or not n.get("title"): continue
            ts = n.get("listed_at") or n.get("first_listed_at") or n.get("created_at")
            try: at = iso(parse_iso(ts) if parse_iso(ts).tzinfo else parse_iso(ts).replace(tzinfo=SEOUL))
            except Exception: continue
            if not in_window(at): continue
            nid = str(n.get("id") or "")
            rows.append({"src": "upbit_notice", "id": nid, "title": n["title"], "at": at, "category": n.get("category"),
                         "url": f"https://upbit.com/service_center/notice?id={nid}"})
    except Exception as e:
        NOTES.append(f"upbit_notices: {type(e).__name__} {str(e)[:60]}"); return [], []
    return _notice_items("upbit", rows)

def bithumb_wallet():
    """Deposit/withdrawal status per asset (public, no key). An asset whose deposits or withdrawals switch off is
    wallet_suspend; back on is wallet_resume. First run seeds silently; a mass flip (>= 30% of assets) is a glitch."""
    out, trig = [], []
    try:
        d = get("https://api.bithumb.com/public/assetsstatus/ALL")
        data = d.get("data") if isinstance(d, dict) and str(d.get("status")) == "0000" else None
        if not isinstance(data, dict) or len(data) < 50:
            raise ValueError(f"unexpected assetsstatus response ({len(data) if isinstance(data, dict) else 'no data'})")
        cur = {a.upper() for a, v in data.items() if isinstance(v, dict)}
        off = {a.upper() for a, v in data.items() if isinstance(v, dict) and
               (str(v.get("deposit_status")) == "0" or str(v.get("withdrawal_status")) == "0")}
        prev = baseline("bithumb_wallet_off")
        if prev is not None:
            on_, back = sorted(off - prev), sorted((prev - off) & cur)
            if len(on_) + len(back) >= 0.3 * len(cur):
                raise ValueError(f"{len(on_) + len(back)} of {len(cur)} wallets flipped at once; diff skipped")
            for a, ev_ in [(a, "wallet_suspend") for a in on_] + [(a, "wallet_resume") for a in back]:
                v = data.get(a) or data.get(a.lower()) or {}
                what = (f"Bithumb {a} deposits/withdrawals {'suspended' if ev_ == 'wallet_suspend' else 'resumed'} "
                        f"(deposit {'on' if str(v.get('deposit_status')) == '1' else 'off'}, withdrawal {'on' if str(v.get('withdrawal_status')) == '1' else 'off'})")
                u = f"https://www.bithumb.com/react/trade/order/{a}-KRW"
                out.append({"src": "bithumb", "id": f"{a}:{ev_}", "title": what, "at": iso(NOW), "url": u, "kind": ev_, "asset": a})
                trig.append(flag_trigger("bithumb", ev_, a, what, u))
        NEW_SEEN["bithumb_wallet_off"] = off
    except Exception as e:
        err("bithumb_wallet", e)
    return out, trig

# ---------------------------------------------------------------- 4 unlocks
LL = "https://defillama-datasets.llama.fi/"

def _unlock_one(slug, now, end):
    d = get(LL + "emissions/" + urllib.parse.quote(slug), ua=BUA)
    if not isinstance(d, dict): return []
    evs = {}
    for ev in (d.get("metadata") or {}).get("unlockEvents") or []:
        t = ev.get("timestamp") or 0
        if not (now <= t <= end): continue
        amt = sum(a.get("amount") or 0 for a in ev.get("cliffAllocations") or [])
        if amt <= 0: continue
        day = datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%d")
        e = evs.setdefault(day, {"t": t, "amount": 0.0, "n": 0, "recipients": set()})
        e["amount"] += amt; e["n"] += 1; e["t"] = min(e["t"], t)
        e["recipients"] |= {a.get("recipient") or a.get("category") for a in ev.get("cliffAllocations") or []}
    if not evs: return []
    unlocked = 0.0
    for c in (d.get("documentedData") or {}).get("data") or []:
        pts = [p for p in c.get("data") or [] if p.get("timestamp", 0) <= now]
        if pts: unlocked += pts[-1].get("unlocked") or 0
    max_sup = (d.get("supplyMetrics") or {}).get("maxSupply")
    return [{"slug": slug, "day": day, "name": d.get("name"), "gecko_id": d.get("gecko_id"), "unlocked": unlocked,
             "max_supply": max_sup, **e} for day, e in evs.items()]

def unlocks_14d():
    out, trig, raw = [], [], []
    try:
        slugs = get(LL + "emissionsProtocolsList", ua=BUA)[:800]
        now = time.time(); end = now + 14 * 86400
        stop = min(DEADLINE - 45, time.time() + 110)      # the scan may not eat the whole budget
        failed = 0
        ex = cf.ThreadPoolExecutor(8)
        futs = [ex.submit(_unlock_one, s, now, end) for s in slugs]
        try:
            for f in cf.as_completed(futs, timeout=max(5, stop - time.time())):
                try: raw += f.result()
                except Exception: failed += 1
        except cf.TimeoutError:
            n = sum(1 for f in futs if not f.done())
            ERRORS.append(f"unlocks: scan capped, {n}/{len(slugs)} protocols not read")
        ex.shutdown(wait=False, cancel_futures=True)
        if failed > len(slugs) * 0.2: ERRORS.append(f"unlocks: {failed}/{len(slugs)} protocol files failed")
        ids = sorted({r["gecko_id"] for r in raw if r.get("gecko_id")})
        px = {}
        if ids:
            try:
                for i in range(0, len(ids), 150):
                    d = get("https://api.coingecko.com/api/v3/simple/price?vs_currencies=usd&ids=" + ",".join(ids[i:i + 150]))
                    px.update({k: v.get("usd") for k, v in d.items() if isinstance(v, dict)})
            except Exception as e:
                err("unlocks_price_coingecko", e)
                try:
                    for i in range(0, len(ids), 100):
                        d = get("https://coins.llama.fi/prices/current/" + ",".join("coingecko:" + x for x in ids[i:i + 100]))
                        px.update({k.split(":", 1)[1]: v.get("price") for k, v in d.get("coins", {}).items()})
                except Exception as e2:
                    err("unlocks_price_llama", e2)
        for r in raw:
            pct = r["amount"] / r["unlocked"] * 100 if r["unlocked"] else None
            price = px.get(r["gecko_id"]) if r.get("gecko_id") else None
            usd = r["amount"] * price if price else None
            if not ((pct is not None and pct >= 1) or (usd is not None and usd >= 1e7)): continue
            days = (r["t"] - now) / 86400
            it = {"src": "defillama_unlock", "id": f"{r['slug']}:{r['day']}", "at": ts_iso(r["t"]),
                  "title": f"{r['name']} cliff unlock {r['amount']:,.0f} tokens" + (f" ({pct:.2f}% of unlocked)" if pct else "") +
                           (f" ~${usd / 1e6:,.1f}M" if usd else ""),
                  "url": f"https://defillama.com/unlocks/{r['slug']}", "name": r["name"], "gecko_id": r.get("gecko_id"),
                  "amount": round(r["amount"]), "pct_supply": rnd(pct), "usd_value": rnd(usd, 0), "price": price,
                  "days_until": rnd(days, 1), "events": r["n"], "recipients": sorted(x for x in r["recipients"] if x)[:6]}
            out.append(it)
            if pct is not None and pct >= 2 and days <= 7:
                trig.append({"type": "unlock", "key": f"unlock:{r['slug']}:{r['day']}", "asset": r.get("gecko_id") or r["slug"],
                             "what": f"{r['name']} unlocks {pct:.1f}% of unlocked supply in {days:.1f}d" +
                                     (f" (~${usd / 1e6:,.1f}M)" if usd else ""), "url": it["url"]})
        out.sort(key=lambda x: x["at"])
    except Exception as e:
        err("unlocks", e)
    return out, trig

# ---------------------------------------------------------------- 5 governance
GOV_RX = re.compile(r"(?i)\b(buy ?-?backs?|repurchas\w*|fee ?switch|revenue[ -]shar\w*|fee (share|sharing|distribution)|tokenomics)")
BURN_RX = re.compile(r"(?i)\bburn(s|ing|ed)?\b")
BUYBACK_RX = re.compile(r"(?i)\b(buy ?-?backs?|repurchas\w*|fee ?switch)")
GOV_NOISE = re.compile(r"(?i)(stablecoin|stability fee|savings rate|\b(dsr|ssr)\b|debt ceiling|risk param|interest rate|"
                       r"borrow rate|supply cap|borrow cap|\bltv\b|liquidation|collateral|e-?mode|oracle|\bpeg\b|psm|gauge|"
                       r"emission rate|incentive program|grant)")

def governance():
    out, trig = [], []
    try:
        fields = "id title body created end state flagged link author space{id name followersCount verified}"
        q1 = '{proposals(first:1000,where:{created_gte:%d},orderBy:"created",orderDirection:desc){%s}}' % (int(time.time()) - 48 * 3600, fields)
        q2 = '{proposals(first:1000,where:{state:"active"},orderBy:"created",orderDirection:desc){%s}}' % fields
        ps = {}
        for q in (q1, q2):
            d = get("https://hub.snapshot.org/graphql", json.dumps({"query": q}).encode(), ct="application/json")
            if d.get("errors"): raise ValueError(str(d["errors"])[:80])
            for p in d["data"]["proposals"] or []: ps[p["id"]] = p
        for p in ps.values():
            sp = p.get("space") or {}
            if (sp.get("followersCount") or 0) < 1000 or p.get("flagged"): continue
            title, body = p.get("title") or "", (p.get("body") or "")[:8000]
            if not (GOV_RX.search(title + " " + body) or BURN_RX.search(title)): continue
            strong = BUYBACK_RX.search(title) or len(BUYBACK_RX.findall(body)) >= 2
            if GOV_NOISE.search(title) and not BUYBACK_RX.search(title): continue
            hits = sorted({m.group(0).lower() for m in GOV_RX.finditer(title + " " + body)} | ({"burn"} if BURN_RX.search(title) else set()))
            it = {"src": "snapshot", "id": p["id"], "title": title[:200], "at": ts_iso(p["created"]), "url": p.get("link"),
                  "space": sp.get("id"), "space_name": sp.get("name"), "followers": sp.get("followersCount"),
                  "verified": sp.get("verified"), "state": p.get("state"), "ends": ts_iso(p["end"]), "matched": hits[:6],
                  "buyback_or_fee_switch": bool(strong)}
            out.append(it)
            if strong:
                trig.append({"type": "governance", "key": f"governance:{p['id']}", "asset": sp.get("id"),
                             "what": f"{sp.get('name') or sp.get('id')} proposal: {title[:120]}", "url": p.get("link")})
        out.sort(key=lambda x: -(x["followers"] or 0))
    except Exception as e:
        err("governance", e)
    return out, trig

# ---------------------------------------------------------------- 6 ETF flows
def etf_flows():
    out, trig = [], []
    for t, a in (("us-btc-spot", "btc"), ("us-eth-spot", "eth")):
        try:
            d = get("https://api.sosovalue.xyz/openapi/v2/etf/historicalInflowChart", json.dumps({"type": t}).encode(),
                    ua=BUA, ct="application/json")["data"]
            d = sorted([r for r in d if r.get("totalNetInflow") is not None], key=lambda r: r["date"], reverse=True)
            prior = [r["totalNetInflow"] for r in d[1:61]]
            z = None
            if len(prior) >= 20 and statistics.pstdev(prior) > 0:
                z = (d[0]["totalNetInflow"] - statistics.mean(prior)) / statistics.pstdev(prior)
            for i, r in enumerate(d[:5]):
                it = {"src": "sosovalue", "id": f"{a}:{r['date']}", "asset": a.upper(), "date": r["date"],
                      "title": f"US spot {a.upper()} ETFs net flow {r['totalNetInflow'] / 1e6:+,.1f}M on {r['date']}",
                      "at": r["date"] + "T00:00:00Z", "url": "https://sosovalue.com/assets/etf/" + t,
                      "net_inflow_usd": round(r["totalNetInflow"]), "net_assets_usd": round(r.get("totalNetAssets") or 0),
                      "value_traded_usd": round(r.get("totalValueTraded") or 0)}
                if i == 0:
                    it["z_60d"] = rnd(z)
                    if z is not None and abs(z) >= 2.5:
                        trig.append({"type": "etf_flow", "key": f"etf_flow:{a}:{r['date']}", "asset": a.upper(),
                                     "what": f"US spot {a.upper()} ETF net flow {r['totalNetInflow'] / 1e6:+,.0f}M on {r['date']} (z={z:+.1f} vs 60d)",
                                     "url": it["url"]})
                out.append(it)
        except Exception as e:
            err("etf_" + a, e)
    return out, trig

# ---------------------------------------------------------------- 7 insider buys
def _oi_table(url):
    s = get(url, ua=BUA, raw=True).decode("latin-1")
    t = re.search(r'<table[^>]*class="tinytable".*?</table>', s, re.S)
    if not t: raise ValueError("no table (layout changed or blocked)")
    t = t.group(0)
    clean = lambda x: html.unescape(re.sub("<[^>]+>", "", x)).replace("\xa0", " ").strip()
    hdr = [clean(h) for h in re.findall(r"<th[^>]*>(.*?)</th>", t, re.S)]
    rows = []
    for r in re.findall(r"<tr[^>]*>(.*?)</tr>", t.split("</thead>", 1)[-1], re.S):
        c = dict(zip(hdr, [clean(x) for x in re.findall(r"<td[^>]*>(.*?)</td>", r, re.S)]))
        tk = re.search(r'href="/([A-Z0-9.\-]+)"', r)
        if tk and c.get("Filing Date"): c["_tk"] = tk.group(1); rows.append(c)
    return rows

def insider_buys():
    out, trig = [], []
    cutoff = NOW - timedelta(days=3)
    for kind, url in (("screener", "http://openinsider.com/screener?fd=3&xp=1&vl=100&sortcol=0&cnt=100&page=1"),
                      ("cluster", "http://openinsider.com/latest-cluster-buys")):
        try:
            for c in _oi_table(url):
                if not str(c.get("Trade Type", "P")).startswith("P"): continue
                filed = datetime.fromisoformat(c["Filing Date"]).replace(tzinfo=NY)
                if filed < cutoff: continue
                tk, val = c["_tk"], num(c.get("Value"))
                n_ins = int(num(c.get("Ins")) or 1) if kind == "cluster" else 1
                who = c.get("Insider Name") or f"{n_ins} insiders"
                it = {"src": "openinsider", "id": f"{kind}|{tk}|{c['Filing Date']}|{who}", "kind": kind,
                      "title": f"{tk} {who} {c.get('Title', '')} buys ${(val or 0) / 1e6:,.2f}M".replace("  ", " "),
                      "at": iso(filed), "url": f"http://openinsider.com/{tk}", "ticker": tk,
                      "company": c.get("Company Name"), "insider": c.get("Insider Name"), "insider_title": c.get("Title"),
                      "insiders": n_ins, "trade_date": c.get("Trade Date"), "value": val, "price": num(c.get("Price")),
                      "d_own": c.get("ΔOwn"), "flags": c.get("X") or None}
                out.append(it)
                if (val or 0) >= 1e6 or n_ins >= 3:
                    day = c["Filing Date"][:10]
                    trig.append({"type": "insider_buy", "key": f"insider_buy:{tk}:{day}", "asset": tk,
                                 "what": f"{tk} insider buying ${(val or 0) / 1e6:,.2f}M" + (f" by {n_ins} insiders" if n_ins > 1 else
                                         f" ({who}, {c.get('Title', '')})") + f", filed {day}", "url": it["url"]})
        except Exception as e:
            err("openinsider_" + kind, e)
    return out, trig

# ---------------------------------------------------------------- 8 stock movers (Yahoo)
def yahoo(path):
    last = None
    for host in ("query2", "query1"):
        try: return get(f"https://{host}.finance.yahoo.com{path}", ua=BUA)
        except Exception as e: last = e
    raise last

def stock_movers():
    out, trig, seen = [], [], set()
    for scr in ("day_gainers", "day_losers", "most_actives"):
        try:
            d = yahoo(f"/v1/finance/screener/predefined/saved?scrIds={scr}&count=100")
            for q in d["finance"]["result"][0]["quotes"]:
                sym = q.get("symbol")
                if not sym or sym in seen or q.get("quoteType") not in ("EQUITY", None): continue
                mcap, chg = q.get("marketCap") or 0, q.get("regularMarketChangePercent")
                vol, avg = q.get("regularMarketVolume") or 0, q.get("averageDailyVolume3Month") or 0
                rvol = vol / avg if avg else None
                if mcap < 2e9 or chg is None: continue
                if not (abs(chg) >= 8 or (rvol or 0) >= 3): continue
                seen.add(sym)
                t = q.get("regularMarketTime")
                at = ts_iso(t) if t else iso(NOW)
                day = datetime.fromtimestamp(t, NY).strftime("%Y-%m-%d") if t else NOW.strftime("%Y-%m-%d")
                it = {"src": "yahoo_screener", "id": f"{sym}:{day}", "title": f"{sym} {q.get('shortName', '')} {chg:+.1f}%",
                      "at": at, "url": f"https://finance.yahoo.com/quote/{sym}", "ticker": sym, "name": q.get("shortName"),
                      "chg_pct": rnd(chg), "price": q.get("regularMarketPrice"), "mcap": mcap, "volume": vol,
                      "avg_volume": avg, "rel_volume": rnd(rvol), "screener": scr, "exchange": q.get("exchange")}
                out.append(it)
                if abs(chg) >= 10:
                    trig.append({"type": "stock_move", "key": f"stock_move:{sym}:{day}", "asset": sym,
                                 "what": f"{sym} ({q.get('shortName')}) {chg:+.1f}% on {day}, mcap ${mcap / 1e9:,.1f}B", "url": it["url"]})
        except Exception as e:
            err("yahoo_" + scr, e)
    out.sort(key=lambda x: -abs(x["chg_pct"] or 0))
    return out, trig

# ---------------------------------------------------------------- 9 commodities / macro
CMDTY = {"GC=F": "Gold", "SI=F": "Silver", "HG=F": "Copper", "PL=F": "Platinum", "CL=F": "WTI crude", "BZ=F": "Brent crude",
         "NG=F": "Natural gas", "ZC=F": "Corn", "ZW=F": "Wheat", "ZS=F": "Soybeans", "KC=F": "Coffee", "CC=F": "Cocoa",
         "SB=F": "Sugar", "LE=F": "Live cattle", "DX-Y.NYB": "US dollar index", "^TNX": "US 10y yield", "^VIX": "VIX",
         "SPY": "S&P 500 ETF", "QQQ": "Nasdaq 100 ETF", "TLT": "20y+ Treasury ETF"}

def closes(sym, rng):
    d = yahoo(f"/v8/finance/chart/{urllib.parse.quote(sym)}?range={rng}&interval=1d")["chart"]["result"][0]
    c = d["indicators"]["quote"][0]["close"]
    pts = [(t, x) for t, x in zip(d.get("timestamp") or [], c) if x is not None]
    return pts, d.get("meta") or {}

def _cmdty_one(sym):
    pts, meta = closes(sym, "1mo")
    if len(pts) < 8: raise ValueError(f"{len(pts)} closes")
    px = [x for _, x in pts]
    rets = [px[i] / px[i - 1] - 1 for i in range(1, len(px))]
    prior = rets[-21:-1]
    sd = statistics.stdev(prior) if len(prior) >= 5 else None
    ch = lambda n: px[-1] / px[-1 - n] - 1 if len(px) > n else None
    c1, c5, c20 = ch(1), ch(5), ch(20) if len(px) > 20 else px[-1] / px[0] - 1
    z = c1 / sd if sd else None
    last_day = datetime.fromtimestamp(pts[-1][0], NY).strftime("%Y-%m-%d")
    return {"src": "yahoo_chart", "id": f"{sym}:{last_day}", "title": f"{CMDTY[sym]} {px[-1]:,.4g} 1d {c1 * 100:+.1f}% 5d {c5 * 100:+.1f}%",
            "at": ts_iso(pts[-1][0]), "url": f"https://finance.yahoo.com/quote/{urllib.parse.quote(sym)}", "symbol": sym,
            "name": CMDTY[sym], "last": px[-1], "last_day": last_day, "chg_1d": rnd(c1 * 100), "chg_5d": rnd(c5 * 100 if c5 is not None else None),
            "chg_20d": rnd(c20 * 100 if c20 is not None else None), "z_1d": rnd(z), "n_returns": len(prior)}

def commodities():
    out, trig = [], []
    with cf.ThreadPoolExecutor(5) as ex:
        futs = {ex.submit(_cmdty_one, s): s for s in CMDTY}
        for f in cf.as_completed(futs):
            try: out.append(f.result())
            except Exception as e: err("commodity_" + futs[f], e)
    order = list(CMDTY)
    out.sort(key=lambda x: order.index(x["symbol"]))
    for it in out:
        z, c5 = it["z_1d"], it["chg_5d"]
        if (z is not None and abs(z) >= 2.5) or (c5 is not None and abs(c5) >= 8):
            trig.append({"type": "commodity_move", "key": f"commodity_move:{it['symbol']}:{it['last_day']}", "asset": it["symbol"],
                         "what": f"{it['name']} 1d {it['chg_1d']:+.1f}% (z={z if z is not None else 'n/a'}), 5d {c5:+.1f}%",
                         "url": it["url"]})
    return out, trig

# ---------------------------------------------------------------- 10 regime
def label(dist, confirm=None):
    if dist >= 0.02 and (confirm is None or confirm > 0): return "risk-on"
    if dist <= -0.02 and (confirm is None or confirm < 0): return "risk-off"
    return "neutral"

def regime():
    out = []
    try:
        k = get("https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=1d&limit=202")
        done = [x for x in k if x[6] < time.time() * 1000]               # closed candles only
        cl = [float(x[4]) for x in done][-200:]
        if len(cl) < 200: raise ValueError(f"{len(cl)} closes")
        sma = sum(cl) / len(cl)
        price = float(k[-1][4])
        ret30 = cl[-1] / cl[-31] - 1
        fund_ann = oi_usd = None
        try:
            meta, ctx = get("https://api.hyperliquid.xyz/info", json.dumps({"type": "metaAndAssetCtxs"}).encode(), ct="application/json")
            c = ctx[[u["name"] for u in meta["universe"]].index("BTC")]
            fund_ann = float(c["funding"]) * 24 * 365 * 100
            oi_usd = float(c["openInterest"]) * float(c["markPx"])
        except Exception as e:
            err("regime_hyperliquid", e)
        dist = price / sma - 1
        out.append({"src": "regime", "id": "BTC:" + NOW.strftime("%Y-%m-%d"), "asset": "BTC", "title": f"BTC {label(dist, ret30)}",
                    "at": iso(NOW), "url": "https://www.binance.com/en/trade/BTC_USDT", "price": price, "sma200": round(sma, 2),
                    "vs_sma200_pct": rnd(dist * 100), "ret_30d_pct": rnd(ret30 * 100), "funding_ann_pct": rnd(fund_ann),
                    "oi_usd": rnd(oi_usd, 0), "crowded": None if fund_ann is None else abs(fund_ann) >= 30,
                    "label": label(dist, ret30),
                    "rule": "risk-on if price >= SMA200 +2% and 30d return > 0; risk-off if <= SMA200 -2% and 30d return < 0; else neutral"})
    except Exception as e:
        err("regime_btc", e)
    try:
        pts, meta = closes("SPY", "1y")
        px = [x for _, x in pts]
        if len(px) < 200: raise ValueError(f"{len(px)} closes")
        sma = sum(px[-200:]) / 200
        price = meta.get("regularMarketPrice") or px[-1]
        dist = price / sma - 1
        out.append({"src": "regime", "id": "SPY:" + NOW.strftime("%Y-%m-%d"), "asset": "SPY", "title": f"SPY {label(dist)}",
                    "at": ts_iso(pts[-1][0]), "url": "https://finance.yahoo.com/quote/SPY", "price": price, "sma200": round(sma, 2),
                    "vs_sma200_pct": rnd(dist * 100), "ret_30d_pct": rnd((px[-1] / px[-22] - 1) * 100), "label": label(dist),
                    "rule": "risk-on if price >= SMA200 +2%; risk-off if <= SMA200 -2%; else neutral"})
    except Exception as e:
        err("regime_spy", e)
    return out, []

# ---------------------------------------------------------------- 11 polymarket
GAMMA = "https://gamma-api.polymarket.com"
PM_TAGS = {"politics": 2, "geopolitics": 100265, "economy": 100328, "finance": 120, "crypto": 21, "business": 107}
SPORT_RX = re.compile(r"(?i)(\bvs\.?\s|\bpremier league\b|\bchampions league\b|\bnba\b|\bnfl\b|\bmlb\b|\bnhl\b|\bufc\b|\bepl\b|"
                      r"\bworld cup\b|\bgrand slam\b|\bsuper bowl\b|\bstanley cup\b|\bworld series\b|\btennis\b|\bgolf\b|"
                      r"\bf1\b|\bformula 1\b|\bla liga\b|\bserie a\b|\bbundesliga\b|\bmvp\b|\bplayoffs?\b|\bchampion\b)")
PM_STRIKE = re.compile(r"(?i)(up or down|(above|below|between) \$[\d,.]+k? (on|at) )")

def _jl(x):
    try: return json.loads(x) if isinstance(x, str) else (x or [])
    except Exception: return []

def _sporty(m, ev=None):
    ev = ev or ((m.get("events") or [{}])[0] if isinstance(m.get("events"), list) else {})
    return bool(m.get("sportsMarketType") or m.get("gameStartTime") or ev.get("gameId") or ev.get("sport") or
                SPORT_RX.search((m.get("question") or "") + " " + (ev.get("title") or "")))

def _pm_item(m, kind, **kw):
    ev = (m.get("events") or [{}])[0] if isinstance(m.get("events"), list) else {}
    return {"src": "polymarket", "id": f"{kind}:{m.get('slug')}", "kind": kind, "title": m.get("question"),
            "at": m.get("createdAt") or iso(NOW), "url": f"https://polymarket.com/event/{ev.get('slug') or m.get('slug')}",
            "question": m.get("question"), "slug": m.get("slug"), "outcomePrices": _jl(m.get("outcomePrices")),
            "liquidity": rnd(float(m.get("liquidityNum") or m.get("liquidity") or 0), 0),
            "volume": rnd(float(m.get("volumeNum") or m.get("volume") or 0), 0),
            "volume24hr": rnd(float(m.get("volume24hr") or 0), 0), "oneDayPriceChange": m.get("oneDayPriceChange"),
            "endDate": m.get("endDate"), "sports": _sporty(m, ev), **kw}

def polymarket_extra():
    out, trig = [], []
    q = "active=true&closed=false&liquidity_num_min=50000&limit=100"
    try:                                                   # (a) new in 48h
        cutoff = NOW - timedelta(hours=48)
        for off in (0, 100, 200):
            ms = get(f"{GAMMA}/markets?{q}&order=createdAt&ascending=false&offset={off}", ua=BUA)
            fresh = [m for m in ms if m.get("createdAt") and parse_iso(m["createdAt"]) >= cutoff]
            for m in fresh:
                it = _pm_item(m, "new")
                if it["sports"] or PM_STRIKE.search(it["question"] or ""): continue    # games / daily strikes: out of scope
                out.append(it)
                if (it["liquidity"] or 0) >= 1e5:
                    trig.append({"type": "polymarket_new", "key": f"polymarket_new:{m.get('slug')}",
                                 "what": f"New Polymarket market (liq ${it['liquidity'] / 1e3:,.0f}k): {it['question']}",
                                 "url": it["url"]})
            if len(fresh) < len(ms) or len(ms) < 100: break
    except Exception as e:
        err("polymarket_new", e)
    try:                                                   # (b) movers in chosen categories
        pool = {}
        for tag, tid in PM_TAGS.items():
            for m in get(f"{GAMMA}/markets?{q}&order=volume24hr&ascending=false&tag_id={tid}", ua=BUA):
                if _sporty(m) or PM_STRIKE.search(m.get("question") or ""): continue
                pool.setdefault(m.get("slug"), (m, tag))
        rows = list(pool.values())
        by_vol = sorted(rows, key=lambda r: -float(r[0].get("volume24hr") or 0))[:12]
        by_move = sorted([r for r in rows if r[0].get("oneDayPriceChange") is not None],
                         key=lambda r: -abs(float(r[0]["oneDayPriceChange"])))[:12]
        done = set()
        for why, lst in (("volume24hr", by_vol), ("price_move", by_move)):
            for m, tag in lst:
                if m.get("slug") in done: continue
                done.add(m.get("slug"))
                out.append(_pm_item(m, "mover", category=tag, why=why))
    except Exception as e:
        err("polymarket_movers", e)
    try:                                                   # (c) over/under-round on neg-risk events
        evs = get(f"{GAMMA}/events?active=true&closed=false&liquidity_min=100000&order=volume24hr&ascending=false&limit=100", ua=BUA)
        for ev in evs:
            if not ev.get("negRisk"): continue
            ms = [m for m in ev.get("markets") or [] if m.get("active") and not m.get("closed")]
            if len(ms) < 2 or float(ev.get("liquidity") or 0) < 1e5: continue
            yes, ask, bid = [], [], []
            for m in ms:
                p = _jl(m.get("outcomePrices"))
                if not p: continue
                yes.append(float(p[0]))
                if m.get("bestAsk") is not None: ask.append(float(m["bestAsk"]))
                if m.get("bestBid") is not None: bid.append(float(m["bestBid"]))
            okb = len(ask) == len(bid) == len(yes)          # book sums only when every outcome has a quote
            if len(yes) < 2: continue
            s = sum(yes)
            if abs(s - 1) < 0.05: continue
            sports = bool(ev.get("gameId") or ev.get("sport") or SPORT_RX.search(ev.get("title") or "") or
                          any(t.get("slug") == "sports" for t in ev.get("tags") or []))
            it = {"src": "polymarket", "id": f"overround:{ev.get('slug')}", "kind": "overround",
                  "title": f"{ev.get('title')}: sum of Yes = {s:.3f} across {len(yes)} outcomes",
                  "at": ev.get("createdAt") or iso(NOW), "url": f"https://polymarket.com/event/{ev.get('slug')}",
                  "question": ev.get("title"), "slug": ev.get("slug"),
                  "outcomePrices": {(m.get("groupItemTitle") or m.get("question") or "")[:60]: _jl(m.get("outcomePrices"))[:1]
                                    for m in sorted(ms, key=lambda m: -float((_jl(m.get("outcomePrices")) or [0])[0]))[:12]},
                  "liquidity": rnd(float(ev.get("liquidity") or 0), 0), "volume24hr": rnd(float(ev.get("volume24hr") or 0), 0),
                  "oneDayPriceChange": None, "endDate": ev.get("endDate"), "n_outcomes": len(yes),
                  "sum_yes": round(s, 4), "overround": round(s - 1, 4),
                  "sum_best_ask": round(sum(ask), 4) if okb else None, "sum_best_bid": round(sum(bid), 4) if okb else None,
                  "arb_hint": bool(okb and (sum(ask) < 1 or sum(bid) > 1)),
                  "neg_risk_augmented": bool(ev.get("negRiskAugmented")), "sports": sports,
                  "note": "hint, not proof: mid/last prices; placeholder or 'Other' outcomes can distort the sum"}
            out.append(it)
            # augmented neg-risk events leave the unnamed "Other" mass unlisted, so a sum < 1 is expected there:
            # trigger only on a book-level hint, or on a non-augmented event
            if not sports and (it["arb_hint"] or not it["neg_risk_augmented"]):
                trig.append({"type": "polymarket_mispricing", "key": f"polymarket_mispricing:{ev.get('slug')}",
                             "what": f"Polymarket '{ev.get('title')}' Yes prices sum to {s:.2f} ({len(yes)} outcomes, liq ${float(ev.get('liquidity') or 0) / 1e3:,.0f}k) - check",
                             "url": it["url"]})
    except Exception as e:
        err("polymarket_events", e)
    return out, trig

# ---------------------------------------------------------------- main
SOURCES = [unlocks_14d, polymarket_extra, commodities, coinbase_new, upbit_krw_new, bithumb_krw_new, bithumb_notices,
           bithumb_wallet, upbit_notices, governance, etf_flows, insider_buys, stock_movers, regime]

def save_state():
    if STATE is None or not NEW_SEEN: return
    try:
        cur = load_state()                                # re-read: only src2_seen is ours, keep everything else as on disk
        if cur is None: return
        seen = cur.get("src2_seen") if isinstance(cur.get("src2_seen"), dict) else {}
        for k, v in list(NEW_SEEN.items()):
            seen[k] = sorted(v)[-CAP:]
        cur["src2_seen"] = seen
        tmp = SF + ".src2.tmp"
        with open(tmp, "w") as f:
            json.dump(cur, f, ensure_ascii=False, indent=1)
        os.replace(tmp, SF)
    except Exception as e:
        err("state", e)

def main():
    res = {"at": iso(NOW), "window_h": H, "errors": ERRORS, "notes": NOTES, "counts": {}, "triggers": []}
    if STATE is None:
        ERRORS.append("state: state.json missing or not a dict; no diffs, nothing saved")
    ex = cf.ThreadPoolExecutor(len(SOURCES))
    futs = {ex.submit(fn): fn.__name__ for fn in SOURCES}
    done, pending = cf.wait(futs, timeout=max(1, DEADLINE - time.time()))
    for f, name in futs.items():
        if f in done:
            try:
                items, trig = f.result()
            except Exception as e:
                err(name, e); items, trig = [], []
        else:
            ERRORS.append(f"{name}: not finished within the time cap"); items, trig = [], []
        res[name] = items
        res["counts"][name] = len(items)
        res["triggers"] += trig
    uniq = {}
    for t in res["triggers"]:
        uniq.setdefault(t["key"], t)
    res["triggers"] = list(uniq.values())
    res["secs"] = round(time.time() - T0, 1)
    save_state()                                        # sources that timed out simply leave their seen sets as they were
    try:
        tmp = "sources2.json.tmp"
        with open(tmp, "w") as f:
            json.dump(res, f, ensure_ascii=False, indent=0, default=str)
        os.replace(tmp, "sources2.json")
    except Exception as e:
        print(json.dumps({"write_error": type(e).__name__}))
    print(json.dumps({"counts": res["counts"], "triggers": len(res["triggers"]), "errors": len(ERRORS),
                      "error_srcs": sorted({e.split(":")[0].split("_")[0] for e in ERRORS}), "secs": res["secs"]}))
    sys.stdout.flush()
    os._exit(0)                                        # do not wait on stuck worker threads

if __name__ == "__main__":
    try:
        main()
    except BaseException as e:                           # never break the pipeline
        try: print(json.dumps({"fatal": type(e).__name__}))
        except Exception: pass
        os._exit(0)
