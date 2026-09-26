#!/usr/bin/env python3
"""X-scout backtest: what happened to prices after past Binance announcements (last ~350 days).
Event types: spot_list (Binance Will List), hodler (HODLer Airdrops = listing), add (Will Add ... on Earn/Margin/Convert,
i.e. already-listed coin gets more access), perp (Futures Will Launch ... Perpetual), delist (Will Delist).
Entry = first hourly price at/after announcement + LAG hours (default 3: our scan latency) - i.e. what WE could have got,
not the instant pop. Returns at +1d/+7d/+30d, excess vs BTC over the same window.
Prices: data-api.binance.vision hourly klines (SYMUSDT) first, CoinGecko hourly (public, <=365d) as fallback.
Usage: python3 backtest.py [lag_hours=3] [max_per_type=80]  -> backtest_events.json + backtest_summary.json"""
import json, re, sys, time, urllib.request, urllib.parse, statistics, random
from datetime import datetime, timezone

LAG = float(sys.argv[1]) if len(sys.argv) > 1 else 3
MAXN = int(sys.argv[2]) if len(sys.argv) > 2 else 80
NOW = time.time()
UA = {"User-Agent": "Mozilla/5.0"}
H = {"1d": 24, "7d": 168, "30d": 720}

def get(url, tries=4):
    for i in range(tries):
        try:
            return json.load(urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=30))
        except urllib.error.HTTPError as e:
            if e.code == 429: time.sleep(65); continue
            if e.code in (400, 404): return None
            time.sleep(5)
        except Exception:
            time.sleep(5)
    return None

def announcements():
    arts = []
    for cat in (48, 161):
        for pg in range(1, 40):
            d = get(f"https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&catalogId={cat}&pageNo={pg}&pageSize=50")
            c = [c for c in (d or {}).get("data", {}).get("catalogs", []) if c["catalogId"] == cat]
            a = c[0]["articles"] if c else []
            if not a: break
            arts += [{"t": x["releaseDate"] / 1000, "title": x["title"]} for x in a]
            if a[-1]["releaseDate"] / 1000 < NOW - 360 * 86400: break
    return [a for a in arts if a["t"] > NOW - 350 * 86400]

def classify(a):
    t = a["title"]; ev = []
    if "HODLer" in t:
        ev = [("hodler", s) for s in re.findall(r"\(([A-Z0-9]{2,12})\)", t)]
    elif re.search(r"Binance Will List", t):
        ev = [("spot_list", s) for s in re.findall(r"\(([A-Z0-9]{2,12})\)", t)]
    elif re.search(r"(?i)tradfi|pre-ipo|pre-market|stock|commodit", t):
        ev = []                                   # TradFi / pre-IPO perps are not crypto listings
    elif re.search(r"Futures Will Launch USD.-Margined ([A-Z0-9]+)USDT Perpetual", t):
        ev = [("perp", re.search(r"Margined ([A-Z0-9]+)USDT", t).group(1))]
    elif re.search(r"^Binance Will Add .*\(([A-Z0-9]+)\) on", t):
        ev = [("add", s) for s in re.findall(r"\(([A-Z0-9]{2,12})\)", t)]
    elif re.search(r"^Binance Will Delist ", t) and "Margin" not in t and "Futures" not in t:
        m = re.match(r"Binance Will Delist (.+?) on \d{4}", t)
        if m: ev = [("delist", s.strip()) for s in re.split(r",|&| and ", m.group(1)) if re.fullmatch(r"[A-Z0-9]{2,12}", s.strip())]
    ev = [(k, s) for k, s in ev if s not in {"XAU", "XAG", "XPT", "XPD", "COPPER", "OIL", "WTI", "BRENT", "NATGAS", "USDBRL", "EURUSD"}]
    return [{"type": k, "sym": s, "t": a["t"], "title": t} for k, s in ev]

def bn_series(sym, t0, t1):
    out = []
    start = int(t0 * 1000)
    while start < t1 * 1000:
        d = get(f"https://data-api.binance.vision/api/v3/klines?symbol={sym}USDT&interval=1h&startTime={start}&limit=1000")
        if not d: break
        out += [(k[0] / 1000, float(k[4])) for k in d]
        if len(d) < 1000: break
        start = d[-1][0] + 3600_000
    return out

cg_ids = {}
def cg_series(sym, t0, t1):
    if sym not in cg_ids:
        d = get(f"https://api.coingecko.com/api/v3/search?query={urllib.parse.quote(sym)}"); time.sleep(4)
        cands = [c for c in (d or {}).get("coins", []) if str(c.get("symbol", "")).upper() == sym
                 and (c.get("market_cap_rank") or 10**9) <= 1500]   # unranked same-ticker tokens are usually impostors
        cands.sort(key=lambda c: c.get("market_cap_rank") or 10**9)
        cg_ids[sym] = cands[0]["id"] if cands else None
    cid = cg_ids[sym]
    if not cid: return []
    d = get(f"https://api.coingecko.com/api/v3/coins/{cid}/market_chart/range?vs_currency=usd&from={int(t0)}&to={int(t1)}"); time.sleep(4)
    return [(p[0] / 1000, p[1]) for p in (d or {}).get("prices", [])]

def at(series, ts):
    for t, p in series:
        if t >= ts: return t, p
    return None, None

def main():
    evs = [e for a in announcements() for e in classify(a)]
    by = {}
    for e in evs: by.setdefault(e["type"], []).append(e)
    random.seed(7)
    sample = [e for k, v in by.items() for e in (v if len(v) <= MAXN else random.sample(v, MAXN))]
    btc = bn_series("BTC", NOW - 355 * 86400, NOW)
    rows = []
    for i, e in enumerate(sample):
        t0, t1 = e["t"] - 7200, min(e["t"] + 31 * 86400, NOW)
        s = bn_series(e["sym"], t0, t1); src = "binance"
        te, pe = at(s, e["t"] + LAG * 3600)
        if pe is None or te - (e["t"] + LAG * 3600) > 6 * 3600:
            s = cg_series(e["sym"], t0, t1); src = "coingecko"; te, pe = at(s, e["t"] + LAG * 3600)
        r = {**e, "src": src, "entry_t": te, "entry": pe}
        if pe:
            _, b0 = at(btc, te)
            r["entry_delay_h"] = round((te - e["t"]) / 3600, 1)
            for k, h in H.items():
                tx, px = at(s, te + h * 3600)
                if px and tx - (te + h * 3600) < 12 * 3600:
                    _, bx = at(btc, te + h * 3600)
                    r[k] = round(px / pe - 1, 4)
                    if b0 and bx: r[k + "_ex"] = round((px / pe - 1) - (bx / b0 - 1), 4)
        rows.append(r)
        if i % 20 == 0: print(f"{i}/{len(sample)} {e['type']} {e['sym']} {src} entry={pe}", flush=True)
    json.dump(rows, open("backtest_events.json", "w"), indent=0)
    summ = {"lag_h": LAG, "window_days": 350, "generated": datetime.now(timezone.utc).isoformat(timespec="minutes"), "types": {}}
    for k in sorted(by):
        R = [r for r in rows if r["type"] == k]
        d = {"events_total": len(by[k]), "sampled": len(R), "priced": sum(1 for r in R if r.get("entry"))}
        for h in H:
            x = [r[h + "_ex"] for r in R if r.get(h + "_ex") is not None]
            if x:
                d[h] = {"n": len(x), "median_excess": round(statistics.median(x), 4), "mean_excess": round(statistics.mean(x), 4),
                        "hit_rate": round(sum(v > 0 for v in x) / len(x), 2),
                        "p25": round(sorted(x)[len(x) // 4], 4), "p75": round(sorted(x)[3 * len(x) // 4], 4)}
            # recency: last 120 days only
            xr = [r[h + "_ex"] for r in R if r.get(h + "_ex") is not None and r["t"] > NOW - 120 * 86400]
            if len(xr) >= 5:
                d[h + "_recent120d"] = {"n": len(xr), "median_excess": round(statistics.median(xr), 4),
                                        "hit_rate": round(sum(v > 0 for v in xr) / len(xr), 2)}
        summ["types"][k] = d
    json.dump(summ, open("backtest_summary.json", "w"), indent=1)
    print(json.dumps(summ, indent=1))

if __name__ == "__main__":
    main()
