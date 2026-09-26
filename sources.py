#!/usr/bin/env python3
"""X-scout first-hand sources (no X). Pulls primary data published at the source and writes
sources.json for the run to review alongside X posts. Read-only public endpoints, no keys.
Usage: python3 sources.py [hours=4]
  binance   - Binance announcements (new listings / futures / collateral) in window
  okx       - OKX new-listing announcements in window
  edgar     - SEC EDGAR full-text: crypto/treasury/ETF filings (8-K, S-1, S-3, 424B5, 19b-4, 10-Q) last 2 days
  polymarket- top markets by 24h volume + biggest 1-day price moves (liquid only)
  hyperliquid - perp funding/OI extremes and 24h movers (crowding / squeeze setups)"""
import json, sys, time, urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta

H = float(sys.argv[1]) if len(sys.argv) > 1 else 4
NOW = datetime.now(timezone.utc)
UA = {"User-Agent": "x-scout research josephcheung2000@gmail.com", "Accept": "application/json"}

def get(url, data=None, headers=None):
    h = dict(UA, **(headers or {}))
    req = urllib.request.Request(url, data=data, headers=h)
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.loads(r.read())

def binance():
    out = []
    try:
        d = get("https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&pageNo=1&pageSize=20")
        for c in d["data"]["catalogs"]:
            if c["catalogId"] not in (48, 49, 161, 157): continue
            for a in c["articles"]:
                ts = datetime.fromtimestamp(a["releaseDate"] / 1000, timezone.utc)
                if NOW - ts <= timedelta(hours=H):
                    out.append({"src": "binance", "cat": c["catalogName"], "title": a["title"], "at": ts.isoformat(timespec="minutes"),
                                "url": f"https://www.binance.com/en/support/announcement/{a.get('code','')}"})
    except Exception as e:
        out.append({"src": "binance", "error": str(e)[:120]})
    return out

def okx():
    out = []
    try:
        d = get("https://www.okx.com/api/v5/support/announcements?annType=announcements-new-listings")
        for a in d["data"][0]["details"]:
            ts = datetime.fromtimestamp(int(a["pTime"]) / 1000, timezone.utc)
            if NOW - ts <= timedelta(hours=H):
                out.append({"src": "okx", "title": a["title"], "at": ts.isoformat(timespec="minutes"), "url": a.get("url")})
    except Exception as e:
        out.append({"src": "okx", "error": str(e)[:120]})
    return out

def edgar():
    out, seen = [], set()
    start = (NOW - timedelta(days=2)).strftime("%Y-%m-%d"); end = NOW.strftime("%Y-%m-%d")
    q = '"bitcoin" OR "ethereum" OR "solana" OR "digital asset treasury" OR "stablecoin" OR "staking" OR "crypto asset"'
    try:
        # NB: do not add "S-1/A" to forms= - EDGAR then returns ONLY S-1/A (S-1 already includes amendments)
        u = ("https://efts.sec.gov/LATEST/search-index?q=" + urllib.parse.quote(q) +
             f"&dateRange=custom&startdt={start}&enddt={end}&forms=8-K,S-1,S-3,424B5,19b-4,10-Q,6-K")
        d = get(u)
        hits = sorted(d["hits"]["hits"], key=lambda x: x["_source"].get("file_date") or "", reverse=True)
        for x in hits:
            s = x["_source"]; adsh, fn = x["_id"].split(":", 1)
            if adsh in seen: continue          # one row per filing (accession), newest first
            seen.add(adsh)
            cik = (s.get("ciks") or ["0"])[0].lstrip("0")
            out.append({"src": "edgar", "id": adsh, "who": s.get("display_names"), "form": s.get("form") or s.get("root_forms"),
                        "filed": s.get("file_date"), "url": f"https://www.sec.gov/Archives/edgar/data/{cik}/{adsh.replace('-', '')}/{fn}"})
    except Exception as e:
        out.append({"src": "edgar", "error": str(e)[:120]})
    return out[:60]

def polymarket():
    out = []
    try:
        d = get("https://gamma-api.polymarket.com/markets?limit=100&active=true&closed=false&order=volume24hr&ascending=false")
        rows = []
        for m in d:
            liq = float(m.get("liquidity") or 0); v = float(m.get("volume24hr") or 0)
            if liq < 50000: continue
            rows.append({"src": "polymarket", "q": m.get("question"), "prices": m.get("outcomePrices"),
                         "chg_1d": m.get("oneDayPriceChange"), "vol24h": round(v), "liq": round(liq),
                         "end": m.get("endDate"), "url": f"https://polymarket.com/market/{m.get('slug')}"})
        top = rows[:15]
        movers = sorted([r for r in rows if r["chg_1d"] is not None], key=lambda r: -abs(r["chg_1d"]))[:10]
        out = top + [r for r in movers if r not in top]
    except Exception as e:
        out.append({"src": "polymarket", "error": str(e)[:120]})
    return out

def hyperliquid():
    out = []
    try:
        meta, ctx = get("https://api.hyperliquid.xyz/info", json.dumps({"type": "metaAndAssetCtxs"}).encode(),
                        {"Content-Type": "application/json"})
        rows = []
        for u, c in zip(meta["universe"], ctx):
            try:
                px, prev = float(c["markPx"]), float(c["prevDayPx"]); vol = float(c["dayNtlVlm"])
                oi_usd = float(c["openInterest"]) * px
            except Exception:
                continue
            if vol < 5e6: continue
            rows.append({"src": "hyperliquid", "coin": u["name"], "px": px, "chg_24h": round(px / prev - 1, 4) if prev else None,
                         "funding_8h_pct": round(float(c["funding"]) * 8 * 100, 4), "oi_usd_m": round(oi_usd / 1e6, 1),
                         "vol24h_usd_m": round(vol / 1e6, 1)})
        f = sorted(rows, key=lambda r: -abs(r["funding_8h_pct"]))[:8]
        m = sorted([r for r in rows if r["chg_24h"] is not None], key=lambda r: -abs(r["chg_24h"]))[:8]
        out = f + [r for r in m if r not in f]
    except Exception as e:
        out.append({"src": "hyperliquid", "error": str(e)[:120]})
    return out

if __name__ == "__main__":
    res = {"window_h": H, "at": NOW.isoformat(timespec="minutes")}
    for fn in (binance, okx, edgar, polymarket, hyperliquid):
        res[fn.__name__] = fn()
    json.dump(res, open("sources.json", "w"), ensure_ascii=False, indent=0)
    print(json.dumps({k: (len(v) if isinstance(v, list) else v) for k, v in res.items()}))
    errs = [x for k, v in res.items() if isinstance(v, list) for x in v if "error" in x]
    if errs: print("ERRORS:", errs)
