#!/usr/bin/env python3
"""X-scout miss log: recall of the scout against the big moves that actually happened.

  python misses.py            (run in the repo dir after `ledger.py pull`; reads/writes state.json)

Once a day it collects the movers of the last completed 24h (crypto, US stocks, commodities, Polymarket),
checks which ones a lead had flagged in the 7 days before the move window ended, and stores:
  state["miss_log"]   one record per UTC date (a re-run on the same date replaces it), 60 days kept
  state["miss_stats"] rolling 14-day recall per kind + precision proxy of score>=7 leads
stdout: a short human summary (private input doc) + one line "MISSES_JSON {...}" with counts only (public log).
Never crashes, never exits non-zero, stays under ~2 minutes. Python 3.12 stdlib only.
"""
import json, os, re, sys, time, traceback, threading, urllib.request, urllib.parse, urllib.error
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

T0 = time.time()
BUDGET = 105                      # seconds for all network work; the rest is local
SF = "state.json"
UTC = timezone.utc
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0 Safari/537.36"
KINDS = ("crypto", "stock", "commodity", "polymarket")
KEEP_DAYS, ROLL_DAYS, LOOKBACK_H, CAP_PER_KIND = 21, 14, 7 * 24, 50
COMMOD = {  # Yahoo symbol -> names a lead might use
    "GC=F": ("GOLD", "XAU", "XAUUSD", "PAXG", "XAUT"), "SI=F": ("SILVER", "XAG", "XAGUSD"), "HG=F": ("COPPER",),
    "PL=F": ("PLATINUM", "XPT"), "CL=F": ("WTI", "OIL", "CRUDE", "USOIL", "CRUDEOIL"), "BZ=F": ("BRENT", "UKOIL"),
    "NG=F": ("NATGAS", "NATURALGAS", "NG", "GAS"), "ZC=F": ("CORN",), "ZW=F": ("WHEAT",), "ZS=F": ("SOYBEANS", "SOYBEAN", "SOY"),
    "KC=F": ("COFFEE",), "CC=F": ("COCOA",), "SB=F": ("SUGAR",), "LE=F": ("CATTLE", "LIVECATTLE")}
NOTES = []                        # source notes for the summary (fallbacks, failures)
_lock = threading.Lock()

def note(s):
    with _lock: NOTES.append(s)

def left():
    return BUDGET - (time.time() - T0)

def get(url, ua=UA, timeout=20, headers=None):
    t = min(timeout, left())
    if t < 2: raise TimeoutError("time budget spent")
    h = {"User-Agent": ua, "Accept": "application/json"}; h.update(headers or {})
    with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=t) as r:
        return json.loads(r.read())

def num(v):
    try:
        if isinstance(v, str): v = v.replace("%", "").replace("$", "").replace(",", "").strip()
        f = float(v); return f if f == f else None
    except Exception:
        return None

def pdt(v):
    try:
        d = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=UTC)
    except Exception:
        return None

def iso(d): return d.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

def norm(s):
    s = str(s or "").strip().upper().lstrip("$")
    return s.split(":")[-1] if ":" in s else s           # "xyz:GOLD" (perp dex prefix) -> GOLD

# ---------------- movers ----------------
def crypto(now):
    rows = []
    for p in (1, 2, 3, 4, 5, 6):
        d = None
        for a in range(4):
            try:
                d = get("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc"
                        f"&per_page=250&page={p}&price_change_percentage=24h,7d", ua="Mozilla/5.0 x-scout"); break
            except urllib.error.HTTPError as e:
                wait = 15 * (a + 1)
                if e.code != 429 or a == 3 or left() < wait + 10: raise
                time.sleep(wait)
        if not d: break
        rows += [r for r in d if (num(r.get("market_cap")) or 0) >= 5e7]
        if (num(d[-1].get("market_cap")) or 0) < 5e7: break
        time.sleep(2.5)
    out, seen = [], set()
    for r in rows:
        if r.get("id") in seen: continue
        seen.add(r.get("id"))
        c24, c7 = num(r.get("price_change_percentage_24h_in_currency")), num(r.get("price_change_percentage_7d_in_currency"))
        sym = str(r.get("symbol") or "").upper()
        if (num(r.get("total_volume")) or 0) < 5e6: continue          # untradeable size: not a real miss
        if (c7 is not None and c7 > 1000) or (c24 is not None and c24 > 1000): continue   # fresh launch / broken 7d base
        if c24 is not None and abs(c24) >= 20:
            out.append({"kind": "crypto", "asset": sym, "id": r["id"], "chg_pct": round(c24, 1), "window": "24h", "_end": now, "_syms": {sym}})
        elif c7 is not None and abs(c7) >= 40:
            out.append({"kind": "crypto", "asset": sym, "id": r["id"], "chg_pct": round(c7, 1), "window": "7d", "_end": now, "_syms": {sym}})
    return out

def stocks(now):
    quotes, src = [], None
    try:
        for scr in ("day_gainers", "day_losers"):
            for host in ("query2", "query1"):
                try:
                    start = 0
                    while start < 300:
                        r = get(f"https://{host}.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds={scr}&count=100&start={start}")
                        q = r["finance"]["result"][0]["quotes"]; quotes += q
                        if len(q) < 100 or abs(num(q[-1].get("regularMarketChangePercent")) or 0) < 8: break
                        start += 100
                    break
                except Exception:
                    if host == "query1": raise
        src = "yahoo"
    except Exception as e:
        note(f"stocks: Yahoo screener blocked ({type(e).__name__}), used Nasdaq screener")
        quotes = []
    if src == "yahoo":
        ts = [q.get("regularMarketTime") for q in quotes if isinstance(q.get("regularMarketTime"), (int, float))]
        last = max(ts) if ts else None
        out = {}
        for q in quotes:
            c, m, t = num(q.get("regularMarketChangePercent")), num(q.get("marketCap")), q.get("regularMarketTime")
            if c is None or abs(c) < 8 or (m or 0) < 2e9 or q.get("region", "US") != "US": continue
            if last and isinstance(t, (int, float)) and last - t > 20 * 3600: continue   # stale quote from an earlier session
            end = datetime.fromtimestamp(t if isinstance(t, (int, float)) else now.timestamp(), UTC)
            s = q["symbol"]
            out[s] = {"kind": "stock", "asset": s, "id": s, "chg_pct": round(c, 1), "_end": end, "_syms": {s, s.replace("-", ".")}}
        sess = datetime.fromtimestamp(last, UTC) if last else now
    else:  # fallback: Nasdaq full-market screener (one request, all US listings)
        r = get("https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&download=true", timeout=30)
        out = {}
        for q in r["data"]["rows"]:
            c, m = num(q.get("pctchange")), num(q.get("marketCap"))
            if c is None or abs(c) < 8 or (m or 0) < 2e9: continue
            s = str(q.get("symbol") or "").strip().replace("/", "-")
            out[s] = {"kind": "stock", "asset": s, "id": s, "chg_pct": round(c, 1), "_end": now, "_syms": {s, s.replace("-", ".")}}
        # Nasdaq gives no timestamp: the last session ends at or before now; use the previous weekday close (20:00 UTC approx)
        d = now if now.hour >= 21 else now - timedelta(days=1)
        while d.weekday() >= 5: d -= timedelta(days=1)
        sess = d.replace(hour=20, minute=0, second=0, microsecond=0)
        for v in out.values(): v["_end"] = sess
    w = "session " + sess.astimezone(timezone(timedelta(hours=-4))).strftime("%Y-%m-%d")
    for v in out.values(): v["window"] = w
    return list(out.values())

def _commod_one(sym):
    r = get(f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(sym)}?range=1mo&interval=1d", timeout=15)
    res = r["chart"]["result"][0]
    ts, cl = res.get("timestamp") or [], (res["indicators"]["quote"][0].get("close") or [])
    bars = [(t, c) for t, c in zip(ts, cl) if c]
    if len(bars) < 7: return []
    end = datetime.fromtimestamp(int((res.get("meta") or {}).get("regularMarketTime") or bars[-1][0]), UTC)
    c1 = (bars[-1][1] / bars[-2][1] - 1) * 100; c5 = (bars[-1][1] / bars[-6][1] - 1) * 100
    names = {sym, sym.split("=")[0], *COMMOD.get(sym, ())}
    base = {"kind": "commodity", "asset": COMMOD.get(sym, (sym,))[0], "id": sym, "_end": end, "_syms": names}
    if abs(c1) >= 3: return [{**base, "chg_pct": round(c1, 1), "window": "1d"}]
    if abs(c5) >= 7: return [{**base, "chg_pct": round(c5, 1), "window": "5d"}]
    return []

def commodities(now):
    out, bad = [], []
    with ThreadPoolExecutor(5) as ex:
        futs = {s: ex.submit(_commod_one, s) for s in COMMOD}
        for s, f in futs.items():
            try: out += f.result(timeout=max(1, left()))
            except Exception: bad.append(s)
    if len(bad) == len(COMMOD): raise RuntimeError("all commodity charts failed")
    if bad: note(f"commodities: {len(bad)}/{len(COMMOD)} charts failed ({' '.join(bad)})")
    return out

def polymarket(now):
    """Gamma pages hold at most 100 rows, so ask for the liquid markets sorted by 1-day change, both ends."""
    out, seen = [], set()
    for asc in ("false", "true"):
        off = 0
        while off < 500:
            d = get("https://gamma-api.polymarket.com/markets?active=true&closed=false&liquidity_num_min=100000"
                    f"&limit=100&offset={off}&order=oneDayPriceChange&ascending={asc}")
            if not d: break
            for m in d:
                ch, liq = num(m.get("oneDayPriceChange")), num(m.get("liquidityNum") if m.get("liquidityNum") is not None else m.get("liquidity"))
                slug = m.get("slug") or str(m.get("id"))
                if ch is None or abs(ch) < 0.15 or (liq or 0) < 1e5 or slug in seen: continue
                seen.add(slug)
                ev = [e.get("slug") for e in (m.get("events") or []) if isinstance(e, dict) and e.get("slug")]
                out.append({"kind": "polymarket", "asset": (m.get("question") or slug)[:80], "id": slug, "chg_pct": round(ch * 100, 1),
                            "window": "24h", "_end": now, "_syms": {slug.upper(), *(x.upper() for x in ev)}})
            if len(d) < 100 or abs(num(d[-1].get("oneDayPriceChange")) or 0) < 0.15: break
            off += 100
    return out

# ---------------- matching ----------------
def lead_keys(l):
    ks = {norm(l.get(k)) for k in ("asset", "ticker", "market_slug", "slug") if l.get(k)}
    if l.get("cg_id"): ks.add("CG:" + str(l["cg_id"]).lower())
    return ks - {""}

def events(l):
    """(time, score, alerted) of every sighting of a lead: first_seen + mentions."""
    ev = []
    t = pdt(l.get("first_seen"))
    if t: ev.append((t, num(l.get("score")), bool(l.get("alerted"))))
    for m in l.get("mentions") or []:
        if isinstance(m, dict) and pdt(m.get("at")):
            ev.append((pdt(m["at"]), num(m.get("score")), bool(m.get("alerted"))))
    return ev

def lead_id(l):
    return str(l.get("id") or f"{norm(l.get('asset'))}-{l.get('first_seen')}")

def matches(l, mv):
    lk = (l.get("kind") or "other").lower()
    if lk not in (mv["kind"], "other", ""): return False
    if mv["kind"] == "crypto":
        if l.get("cg_id"):
            return str(l["cg_id"]).lower() == mv["id"].lower()      # an explicit CoinGecko id beats a symbol clash
        return norm(l.get("asset")) in mv["_syms"] or norm(l.get("ticker")) in mv["_syms"]
    return bool(lead_keys(l) & {s.upper() for s in mv["_syms"]})

def dir_ok(direction, chg):
    d = str(direction or "").lower()
    if d in ("long", "buy", "up", "yes", "bull", "bullish"): return chg > 0
    if d in ("short", "sell", "down", "no", "bear", "bearish"): return chg < 0
    return None

def match_all(movers, leads):
    for mv in movers:
        end = mv["_end"]; wh = {"7d": 168}.get(str(mv.get("window")), 24)
        lo = end - timedelta(hours=wh + LOOKBACK_H)
        latest = end - timedelta(hours=wh / 2)          # "flagged beforehand": seen before the move was half over
        hits = []
        for l in leads:
            if not isinstance(l, dict) or not matches(l, mv): continue
            ev = [e for e in events(l) if lo <= e[0] <= latest]
            if not ev: continue
            first = pdt(l.get("first_seen")) or min(e[0] for e in ev)
            sc = max([e[1] for e in ev if e[1] is not None] or [num(l.get("max_score")) or num(l.get("score")) or 0])
            hits.append({"lid": lead_id(l), "score": sc, "alerted": any(e[2] for e in ev) or bool(l.get("alerted")),
                         "dir_ok": dir_ok(l.get("lean") if str(l.get("direction") or "").lower() == "watch" else l.get("direction"), -mv["chg_pct"] if (l.get("kind") == "polymarket" and str(l.get("outcome") or "yes").strip().lower() != "yes") else mv["chg_pct"]), "hours": (end - min(first, *[e[0] for e in ev])).total_seconds() / 3600})
        # "caught" = flagged beforehand ON THE RIGHT SIDE. A short on a coin that then pumped, or a no-lean watch,
        # did not catch the move; those are kept as "seen" (the scout looked at it) for context.
        right = [h for h in hits if h["dir_ok"] is True]
        best = max(right or hits, key=lambda h: (h["score"], h["hours"])) if hits else None
        mv.update({"caught": bool(right), "seen": bool(hits), "best_score": best and best["score"], "alerted": bool(right) and any(h["alerted"] for h in right),
                   "dir_ok": best and best["dir_ok"], "lead_hours_before": best and round(best["hours"], 1),
                   "lead_hits": [[h["lid"], h["dir_ok"]] for h in hits][:5]})
    return movers

# ---------------- ledger ----------------
def load_state():
    try:
        with open(SF, encoding="utf-8") as f: s = json.load(f)
        return s if isinstance(s, dict) else None
    except Exception:
        return None

def save_state(s):
    tmp = SF + ".misses.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(s, f, ensure_ascii=False, indent=1); f.flush(); os.fsync(f.fileno())
    os.replace(tmp, SF)

def dedupe_multiday(movers, log, today):
    """A 7d crypto / 5d commodity move, or a stock session already logged under another date (weekends),
    is not counted again. 1-day moves are never dropped."""
    seen = set()
    for r in log:
        if not isinstance(r, dict) or r.get("date") == today: continue
        age = (datetime.fromisoformat(today) - datetime.fromisoformat(r["date"])).days if re.match(r"\d{4}-\d\d-\d\d$", str(r.get("date"))) else 99
        for m in r.get("movers") or []:
            if not isinstance(m, dict): continue
            if m.get("window", "").startswith("session"): seen.add((m.get("kind"), m.get("id"), m.get("window")))
            elif m.get("window") in ("7d", "5d") and 0 < age < (7 if m["window"] == "7d" else 5):
                seen.add((m.get("kind"), m.get("id"), "multi", (m.get("chg_pct") or 0) > 0))
    out = []
    for m in movers:
        if m["window"].startswith("session") and (m["kind"], m["id"], m["window"]) in seen: continue
        if m["window"] in ("7d", "5d") and (m["kind"], m["id"], "multi", m["chg_pct"] > 0) in seen: continue
        out.append(m)
    return out

def counts(movers):
    n = {k: 0 for k in KINDS}; c = dict(n); c7 = dict(n)
    for m in movers:
        k = m["kind"]; n[k] += 1; c[k] += bool(m["caught"]); c7[k] += bool(m["caught"] and (m.get("best_score") or 0) >= 7)
    for d in (n, c, c7): d["all"] = sum(d[k] for k in KINDS)
    return n, c, c7

def ratio(a, b): return round(a / b, 3) if b else None

def build_stats(s, now):
    log = [r for r in s.get("miss_log", []) if isinstance(r, dict)]
    cut = (now - timedelta(days=ROLL_DAYS - 1)).strftime("%Y-%m-%d")
    recent = [r for r in log if str(r.get("date", "")) >= cut]
    N = {k: 0 for k in (*KINDS, "all")}; C = dict(N); C7 = dict(N)
    for r in recent:
        for k in N:
            N[k] += int((r.get("n") or {}).get(k) or 0); C[k] += int((r.get("caught") or {}).get(k) or 0)
            C7[k] += int((r.get("caught_s7") or {}).get(k) or 0)
    # precision proxy: leads scored >=7 first seen in the last 14 days that later showed up as a mover the right way
    hit = {}
    for r in recent:
        for m in r.get("movers") or []:
            for lid, ok in (m.get("lead_hits") or []) if isinstance(m, dict) else []:
                hit[lid] = hit.get(lid) or ok is True          # a no-lean watch (None) is not a correct call
    lo = now - timedelta(days=ROLL_DAYS)
    hi = [l for l in s.get("leads", []) if isinstance(l, dict) and (pdt(l.get("first_seen")) or lo) > lo
          and max(num(l.get("max_score")) or 0, num(l.get("score")) or 0) >= 7]
    ph = sum(1 for l in hi if hit.get(lead_id(l)))
    return {"updated": iso(now), "days": len(recent), "window_days": ROLL_DAYS,
            "recall": {k: ratio(C[k], N[k]) for k in N}, "recall_s7": {k: ratio(C7[k], N[k]) for k in N},
            "n": N, "caught": C, "precision_proxy": ratio(ph, len(hi)), "precision_n": len(hi), "precision_hits": ph}

# ---------------- main ----------------
def main():
    now = datetime.now(UTC); today = now.strftime("%Y-%m-%d")
    n = {k: 0 for k in (*KINDS, "all")}; c, c7, ok, wrote = dict(n), dict(n), [], False
    st = load_state()
    if st is None: note("state.json missing or not a dict - nothing will be written")
    leads = [l for l in (st or {}).get("leads", []) if isinstance(l, dict)]
    src, movers = {}, []
    fns = {"crypto": crypto, "stock": stocks, "commodity": commodities, "polymarket": polymarket}
    ex = ThreadPoolExecutor(4)
    futs = {k: ex.submit(f, now) for k, f in fns.items()}
    for k, f in futs.items():
        try:
            r = f.result(timeout=max(1, left() + 5)); movers += r; src[k] = "ok"
        except Exception as e:
            src[k] = "fail"; note(f"{k}: unavailable ({type(e).__name__}: {str(e)[:80]})")
    ex.shutdown(wait=False, cancel_futures=True)
    if src.get("stock") == "ok" and any("Nasdaq" in x for x in NOTES): src["stock"] = "fallback_nasdaq"

    try:
        log = [r for r in (st or {}).get("miss_log", []) if isinstance(r, dict)]
        movers = dedupe_multiday(movers, log, today)
        match_all(movers, leads)
        old = next((r for r in log if r.get("date") == today), None)
        keep = []
        for k in KINDS:   # a re-run whose source failed keeps what the earlier run of the same date got for that kind
            if src.get(k) == "fail" and old and k in (old.get("sources_ok") or []):
                keep += [m for m in old.get("movers") or [] if m.get("kind") == k]
        movers.sort(key=lambda m: -abs(m["chg_pct"]))
        n, c, c7 = counts(movers)
        for m in keep:
            k = m["kind"]; n[k] = old["n"].get(k, 0); c[k] = old["caught"].get(k, 0); c7[k] = (old.get("caught_s7") or {}).get(k, 0)
        n["all"] = sum(n[k] for k in KINDS); c["all"] = sum(c[k] for k in KINDS); c7["all"] = sum(c7[k] for k in KINDS)
        ok = [k for k in KINDS if src.get(k) != "fail" or any(m["kind"] == k for m in keep)]
        stored = []
        for k in KINDS:
            km = [m for m in movers if m["kind"] == k][:CAP_PER_KIND] + [m for m in keep if m["kind"] == k]
            stored += [{x: m.get(x) for x in ("kind", "asset", "id", "chg_pct", "window", "caught", "seen", "best_score", "alerted",
                                               "dir_ok", "lead_hours_before", "lead_hits")} for m in km]
        rec = {"date": today, "at": iso(now), "sources_ok": ok, "movers": stored,
               "recall": {k: (ratio(c[k], n[k]) if (k == "all" or k in ok) else None) for k in (*KINDS, "all")},
               "n": n, "caught": c, "caught_s7": c7}
        if st is not None:
            log = [r for r in log if r.get("date") != today] + [rec]
            cut = (now - timedelta(days=KEEP_DAYS - 1)).strftime("%Y-%m-%d")
            st["miss_log"] = sorted([r for r in log if str(r.get("date", "")) >= cut], key=lambda r: r["date"])
            st["miss_stats"] = build_stats(st, now)
            save_state(st); wrote = True
    except Exception as e:
        note(f"record: {type(e).__name__}: {str(e)[:120]}")
        traceback.print_exc(file=sys.stderr)

    # ---- output ----
    try:
        ms = (st or {}).get("miss_stats") or {}
        f = lambda k: f"{c[k]}/{n[k]}" + (f" ({c[k] / n[k]:.0%})" if n[k] else "") if k in ok or k == "all" else "n/a"
        lines = [f"MISS LOG {today}: recall {f('all')} of big movers caught (score>=7: {c7['all']}/{n['all']})",
                 "  " + " | ".join(f"{k} {f(k)}" for k in KINDS)]
        if ms.get("days"):
            r = ms["recall"]; pp = ms.get("precision_proxy")
            lines.append(f"  14d ({ms['days']}d logged): all {r['all'] if r['all'] is not None else 'n/a'} | "
                         + " ".join(f"{k[:5]} {r[k] if r[k] is not None else '-'}" for k in KINDS)
                         + f" | precision proxy {pp if pp is not None else 'n/a'} (n={ms.get('precision_n')})")
        miss = [m for m in movers if not m["caught"]][:5]
        if miss:
            lines.append("  top missed: " + ", ".join(f"{m['asset'][:28]} {m['chg_pct']:+.0f}%" + ("pt" if m["kind"] == "polymarket" else "")
                                                      + f" ({m['kind'][:5]},{m['window'].replace('session ', '')})" for m in miss))
        caught = [m for m in movers if m["caught"]][:3]
        if caught:
            lines.append("  caught: " + ", ".join(f"{m['asset'][:20]} {m['chg_pct']:+.0f}% s{m['best_score']:g}"
                                                  f"{' A' if m['alerted'] else ''}{'' if m['dir_ok'] is not False else ' WRONG-DIR'}"
                                                  f" {m['lead_hours_before']:.0f}h" for m in caught))
        for x in NOTES[:4]: lines.append("  note: " + x)
        print("\n".join(lines[:12]))
    except Exception as e:
        print(f"MISS LOG {today}: summary failed ({type(e).__name__})")
    try:
        print("MISSES_JSON " + json.dumps({"date": today, "n": n, "caught": c, "caught_s7": c7, "sources": src,
                                           "written": wrote, "secs": round(time.time() - T0, 1)},
                                          separators=(",", ":")))
    except Exception:
        print('MISSES_JSON {"error":"output"}')

if __name__ == "__main__":
    try:
        main()
    except BaseException as e:
        print(f"MISS LOG: failed ({type(e).__name__}: {str(e)[:100]})")
        print('MISSES_JSON ' + json.dumps({"error": type(e).__name__}))
    sys.stdout.flush(); sys.stderr.flush()
    os._exit(0)
