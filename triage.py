#!/usr/bin/env python3
"""X-scout triage: decides, with plain rules and no model, whether this run needs a full analysis.
Reads posts.json, sources.json, state.json (ledger - run AFTER `ledger.py pull`). Writes triage.json, prints a summary,
and records seen EDGAR filings + recent trigger keys in state.json (atomic write, other keys untouched).
Never raises on bad input: malformed items are skipped.
Usage: python3 triage.py      (mode "full" if any fresh trigger fires, or on the 08:00-10:59 JST daily sweep)
TRIGGERS
  listing     Binance notice in window matching listing words (list / launchpool / HODLer airdrop / futures or perpetual
              launch / will add / borrowable / delist / pre-market); every OKX item (the OKX feed is new listings only)
  (edgar)     a NEW filing from a company with a ticker (8-K / S-1 / 424B5 / 6-K / 19b-4) is not a trigger: it is queued
              and handed to the next full run in triage.json "edgar_review"
  funding     Hyperliquid |funding| >= 0.20%/8h with OI >= $50M (re-fires when it escalates to >=0.5% / >=1.0%)
  move        Hyperliquid |24h move| >= 15% with OI >= $100M, or >= 30% with OI >= $30M, or a new perp (no prior price)
              with OI >= $30M
  polymarket  market-relevant question (crypto/macro/policy/geopolitics; not sports, not daily price-strike markets)
              with |1-day change| >= 0.15 and liquidity >= $100k
  x_cluster   the same coin/ticker named by >= 3 different X accounts (>= 4 if > 150 posts; >= 6 for majors; BTC/ETH
              excluded); re-fires when the cluster doubles
  x_blast     a single post with >= 200k views naming a coin/ticker
A trigger with the same key fires at most once per 12h - except within the same slot (env XS_SLOT): a rebuild of a
slot's input sees the triggers its first build saw, so it is never downgraded to light."""
import json, os, re, urllib.request
from datetime import datetime, timezone, timedelta

NOW = datetime.now(timezone.utc); JST = NOW.astimezone(timezone(timedelta(hours=9)))
LIST_RX = re.compile(r"(?i)\b(will list|to list|lists\b|launchpool|hodler airdrops?|megadrop|will add \S+ \(|borrowable|"
                     r"(will )?launch(es)? .{0,40}(perpetual|futures|spot|pre-market)|perpetual contract|will delist|delisting|pre-market)")
LIST_NOISE = re.compile(r"(?i)(leverage and margin tier|margin tiers|maintenance|api update|trading bots|p2p|"
                        r"trading competition|zero fees|earn: enjoy|tokenized securities as collateral)")
PM_RX = re.compile(r"(?i)\b(crypto|bitcoin|btc|ethereum|eth|solana|stablecoins?|tether|usdt|coinbase|binance|hyperliquid|"
                   r"sec|etfs?|fed|fomc|powell|rates?|inflation|cpi|recession|tariffs?|elections?|president|trump|"
                   r"wars?|conflict|iran|israel|china|taiwan|russia|ukraine|venezuela|oil|opec|gold|stocks?|s&p|nasdaq|"
                   r"shutdown|default|ceasefire|hormuz|treasury|ipo|banks?|gdp|unemployment|nominee|nomination)\b")
SPORT_RX = re.compile(r"(?i)(\bvs\.?\s|\bpremier league\b|\bchampions league\b|\bnba\b|\bnfl\b|\bmlb\b|\bnhl\b|\bufc\b|"
                      r"\bworld cup\b|\bgrand slam\b|\bsuper bowl\b|\bstanley cup\b|\bworld series\b|\btennis\b|\bgolf\b|"
                      r"\bf1\b|\bformula 1\b|\bepl\b|\bla liga\b|\bserie a\b|\bbundesliga\b|\bmvp\b|\bplayoffs?\b)")
PM_STRIKE = re.compile(r"(?i)(up or down|(above|below|between) \$[\d,.]+k? (on|at) )")   # daily strike markets only
MAJORS = {"BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "TRX", "USDT", "USDC", "HYPE"}
STOP = {"THE", "AND", "FOR", "NOT", "ALL", "NOW", "NEW", "ONE", "YOU", "ARE", "CEO", "USD", "ETF", "IPO", "SEC", "FED",
        "CPI", "GDP", "ATH", "USA", "API", "BUY", "OUT", "HAS", "WAS", "CAN", "GET", "TOP", "BIG", "WIN", "HOT", "AI",
        "JUST", "BREAKING", "NEWS", "LIVE", "WEB", "TOKEN", "DEFI", "NFT", "DAO", "TVL", "APY", "YES", "LOW", "HIGH",
        "RWA", "OTC", "KYC", "AMA", "DYOR", "NFA", "FOMO", "HODL", "GM", "LFG", "JST", "UTC", "EST", "PST", "HKT", "CASH",
        "TRUMP", "SPX", "SPY", "QQQ", "NDX", "VIX", "DXY", "US", "UK", "EU", "JP", "CN", "HK", "IMF", "OPEC", "GOLD", "OIL",
        "FDV", "OI", "PNL", "ROI", "EPS", "YOY", "QOQ", "MOM", "WTF", "IMO", "LOL", "OMG", "BREAKOUT", "BULL", "BEAR", "LONG",
        "SHORT", "PUMP", "DUMP", "MOON", "SEND", "ALPHA", "BETA", "FREE", "REAL", "OPEN", "CLOSE", "WEEK", "DAY", "YEAR",
        "HASH", "SUN", "EDGE", "META", "DOG", "CAT", "KEY", "BAT", "ONDO_", "SAFE", "LINK_", "POL_"}   # bare-word filter only
COMMON_NAMES = {"compound", "stellar", "optimism", "render", "flare", "midnight", "quantum", "quant", "humanity", "lighter",
                "ultima", "grass", "aster", "cash", "harmony", "status", "origin", "maker", "theta", "gas", "fetch", "graph",
                "the graph", "sonic", "story", "movement", "plume", "vision", "jupiter", "avalanche", "plasma", "derive",
                "sun", "edge", "hash", "dog", "cat", "official trump", "bitcoin cash"}   # ambiguous English words: $TICKER/SYMBOL only

def load(p):
    try:
        d = json.load(open(p))
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}

def num(v):
    try: return float(v)
    except Exception: return 0.0

def lst(v):
    return v if isinstance(v, list) else []

def cg_symbols():
    try:
        req = urllib.request.Request("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1",
                                     headers={"User-Agent": "Mozilla/5.0"})
        d = json.loads(urllib.request.urlopen(req, timeout=25).read())
        syms = {str(c["symbol"]).upper(): c["id"] for c in d if c.get("symbol")}
        names = {}
        for c in d:
            n = str(c.get("name") or "")
            if len(n) >= 5 and n.lower() not in COMMON_NAMES:
                names[n] = str(c["symbol"]).upper()
        return syms, names
    except Exception:
        return {}, {}

def entities(text, syms, name_rx):
    text = text if isinstance(text, str) else ""
    cash = {m.upper() for m in re.findall(r"\$([A-Za-z][A-Za-z0-9]{1,9})\b", text)}   # explicit cashtags always count
    bare = {w for w in re.findall(r"\b[A-Z][A-Z0-9]{2,7}\b", text) if w in syms and w not in STOP}
    named = set()
    if name_rx:
        for m in name_rx.findall(text):
            named.add(name_rx_map.get(m.lower(), m.upper()))
    return cash | bare | named

name_rx_map = {}

def fmt_pct(v):
    try: return f"{float(v):+.1%}"
    except Exception: return "n/a"

def bucket(v, edges):
    b = 0
    for i, x in enumerate(edges):
        if v >= x: b = i
    return b

def main():
    global name_rx_map
    posts = [p for p in lst(load("posts.json").get("posts")) if isinstance(p, dict)]
    src = load("sources.json")
    try:
        state = json.load(open("state.json")); state_ok = isinstance(state, dict)
    except Exception:
        state, state_ok = {}, False
    if not state_ok: state = {}
    trig = []   # each: {"type", "key", "what", "url"}
    for it in lst(src.get("binance")):
        t = it.get("title") if isinstance(it, dict) else None
        if isinstance(t, str) and LIST_RX.search(t) and not LIST_NOISE.search(t):
            trig.append({"type": "listing", "key": "listing:" + t, "what": f"Binance: {t}", "url": it.get("url")})
    for it in lst(src.get("okx")):
        t = it.get("title") if isinstance(it, dict) else None
        if isinstance(t, str):
            trig.append({"type": "listing", "key": "listing:" + t, "what": f"OKX: {t}", "url": it.get("url")})
    seen = set(lst(state.get("seen_edgar")))
    # EDGAR filings are not minute-sensitive and arrive daily: they are QUEUED and reviewed at the next full run
    # (at the latest the 08:00-10:59 JST sweep) instead of forcing a full run on their own.
    queue = [q for q in lst(state.get("edgar_queue")) if isinstance(q, dict)]
    new_ids = []
    for it in lst(src.get("edgar")):
        if not isinstance(it, dict) or not it.get("id") or it["id"] in seen: continue
        new_ids.append(it["id"])
        who = it.get("who"); who = " ".join(map(str, who)) if isinstance(who, list) else str(who or "")
        if re.search(r"\([A-Z]{1,5}(,\s*[A-Z]{1,5})*\)", who) and it.get("form") in ("8-K", "S-1", "424B5", "6-K", "19b-4"):
            queue.append({"what": f"{who} {it.get('form')} {it.get('filed')}", "url": it.get("url"), "id": it["id"]})
    for it in lst(src.get("hyperliquid")):
        if not isinstance(it, dict) or not it.get("coin"): continue
        coin, oi, f = it["coin"], num(it.get("oi_usd_m")), num(it.get("funding_8h_pct"))
        chg = it.get("chg_24h"); mv = abs(num(chg))
        if abs(f) >= 0.20 and oi >= 50:
            trig.append({"type": "funding", "key": f"funding:{coin}:{bucket(abs(f), [0.2, 0.5, 1.0])}:{'+' if f > 0 else '-'}",
                         "what": f"{coin} funding {f}%/8h, OI ${oi}M, 24h {fmt_pct(chg)}"})
        newperp = "chg_24h" in it and it["chg_24h"] is None
        if (mv >= 0.15 and oi >= 100) or (mv >= 0.30 and oi >= 30) or (newperp and oi >= 30):
            trig.append({"type": "move", "key": f"move:{coin}:{bucket(mv, [0, 0.3, 0.6])}:{'-' if num(chg) < 0 else '+'}",
                         "what": f"{coin} 24h {fmt_pct(chg) if chg is not None else 'new perp'}, OI ${oi}M, funding {f}%/8h"})
    for it in lst(src.get("polymarket")):
        if not isinstance(it, dict): continue
        q = str(it.get("q") or ""); c = num(it.get("chg_1d")); liq = num(it.get("liq"))
        if abs(c) >= 0.15 and liq >= 100000 and PM_RX.search(q) and not PM_STRIKE.search(q) \
           and not (SPORT_RX.search(q) and not re.search(r"(?i)\b(election|president|nominee|senate|governor|mayor|primary)\b", q)):
            trig.append({"type": "polymarket", "key": "polymarket:" + str(it.get("url") or q),
                         "what": f"{q} moved {c:+.2f} (liq ${liq:,.0f})", "url": it.get("url")})
    syms, names = cg_symbols()
    name_rx_map = {n.lower(): s for n, s in names.items()}
    name_rx = re.compile(r"\b(" + "|".join(re.escape(n) for n in sorted(names, key=len, reverse=True)) + r")\b", re.I) if names else None
    by_ent, focus = {}, set()
    for p in posts:
        es = entities(p.get("text"), syms, name_rx)
        p["_ents"] = sorted(es)
        for e in es: by_ent.setdefault(e, set()).add(str(p.get("handle") or "?").lower())
        views = num((p.get("m") or {}).get("views") if isinstance(p.get("m"), dict) else 0)
        if es and views >= 200000 and p.get("id"):
            trig.append({"type": "x_blast", "key": "x_blast:" + str(p.get("url") or p["id"]),
                         "what": f"@{p.get('handle')} {views:,.0f} views on {', '.join(sorted(es))}", "url": p.get("url")})
            focus.add(p["id"])
    need = 4 if len(posts) > 150 else 3
    for e, hs in sorted(by_ent.items(), key=lambda kv: -len(kv[1])):
        if e in ("BTC", "ETH"): continue   # always discussed; BTC/ETH ideas come via funding/sources triggers
        n = len(hs)
        if n >= (6 if e in MAJORS else need):
            trig.append({"type": "x_cluster", "key": f"x_cluster:{e}:{bucket(n, [0, 6, 12, 24])}", "ent": e,
                         "what": f"{e} named by {n} accounts"})
    # suppress repeats: the same key fires at most once per 12h
    recent = {}
    for k, v in (state.get("recent_triggers") or {}).items() if isinstance(state.get("recent_triggers"), dict) else []:
        try:
            ts = datetime.fromisoformat(str(v))
            if ts.tzinfo is None: ts = ts.replace(tzinfo=timezone.utc)
            if (NOW - ts).total_seconds() < 12 * 3600: recent[k] = ts.isoformat(timespec="minutes")
        except Exception:
            pass
    rslot = state.get("recent_trigger_slot") if isinstance(state.get("recent_trigger_slot"), dict) else {}
    rslot = {k: v for k, v in rslot.items() if k in recent}
    cur_slot = os.environ.get("XS_SLOT") or None
    fresh, repeat = [], 0
    for t in trig:
        if t["key"] in recent and not (cur_slot and rslot.get(t["key"]) == cur_slot): repeat += 1; continue
        if t["key"] not in recent:
            recent[t["key"]] = NOW.isoformat(timespec="minutes")
            if cur_slot: rslot[t["key"]] = cur_slot
        fresh.append(t)
    trig = fresh
    # per-account activity for the monthly follow/unfollow review: monthly buckets, each post counted once,
    # retweeted/quoted inner posts excluded, X's own follow flag kept when the timeline provides it
    hs = state.get("handle_seen") if isinstance(state.get("handle_seen"), dict) else {}
    counted = set(lst(state.get("handle_seen_ids")))
    month = JST.strftime("%Y-%m"); keep_months = {(JST - timedelta(days=31 * k)).strftime("%Y-%m") for k in range(4)}
    new_counted = []
    for p in posts:
        h = str(p.get("handle") or "").strip().lower(); pid = str(p.get("id") or "")
        if not h or not pid or pid in counted or p.get("nested"): continue
        new_counted.append(pid)
        tab = "following" if str(p.get("tab", "")).strip() in ("正在跟隨", "Following", "正在关注", "フォロー中") else "other"
        rec = hs.get(h) if isinstance(hs.get(h), dict) else {}
        mb = rec.get("m") if isinstance(rec.get("m"), dict) else {}
        b = mb.get(month) if isinstance(mb.get(month), dict) else {}
        b[tab] = int(num(b.get(tab))) + 1; mb[month] = b
        rec["m"] = {k: v for k, v in mb.items() if k in keep_months}
        if isinstance(p.get("followed"), bool): rec["followed"] = p["followed"]; rec["followed_seen"] = JST.strftime("%Y-%m-%d")
        rec["last"] = JST.strftime("%Y-%m-%d"); hs[h] = rec
    if len(hs) > 4000:
        hs = dict(sorted(hs.items(), key=lambda kv: str(kv[1].get("last", "")) if isinstance(kv[1], dict) else "", reverse=True)[:4000])
    hot = {t["ent"] for t in trig if t["type"] == "x_cluster"}
    for p in posts:
        if set(p.get("_ents", [])) & hot and p.get("id"): focus.add(p["id"])
    try: sweep = int(os.environ["XS_SLOT_HOUR"]) == 8          # the pipeline passes the slot being prepared
    except Exception: sweep = 8 <= JST.hour <= 10                # (a feeder-dispatched prep runs ~07:40 JST for 08:45)
    mode = "full" if (trig or sweep) else "light"
    top = sorted(posts, key=lambda p: -num((p.get("m") or {}).get("views") if isinstance(p.get("m"), dict) else 0))[:40]
    focus |= {p["id"] for p in top if p.get("id")}
    out = {"mode": mode, "sweep": sweep, "triggers": trig, "edgar_review": queue[:120] if mode == "full" else [],
           "focus_ids": [p["id"] for p in posts if p.get("id")] if sweep else sorted(focus),
           "entity_counts": {e: len(h) for e, h in sorted(by_ent.items(), key=lambda kv: -len(kv[1]))[:25]}}
    json.dump(out, open("triage.json", "w"), ensure_ascii=False, indent=1)
    if state_ok:   # never rewrite a missing/corrupt state file (it would wipe the ledger)
        state["seen_edgar"] = (lst(state.get("seen_edgar")) + new_ids)[-3000:]
        state["recent_triggers"] = recent
        state["recent_trigger_slot"] = rslot
        state["handle_seen"] = hs
        state["handle_seen_ids"] = (lst(state.get("handle_seen_ids")) + new_counted)[-8000:]
        state["edgar_queue"] = queue[120:] if mode == "full" else queue[-240:]   # oldest 120 reviewed now, rest next full run
        tmp = "state.json.tmp"
        json.dump(state, open(tmp, "w"), ensure_ascii=False, indent=1)
        os.replace(tmp, "state.json")
    print(json.dumps({"mode": mode, "sweep": sweep, "n_triggers": len(trig),
                      "by_type": {t: sum(1 for x in trig if x["type"] == t) for t in sorted({x["type"] for x in trig})},
                      "focus_posts": len(out["focus_ids"]), "posts": len(posts), "new_edgar": len(new_ids),
                      "edgar_queued": len(queue) if mode == "light" else 0, "edgar_to_review": len(out["edgar_review"]),
                      "suppressed_repeats": repeat}))
    for t in trig[:25]: print("-", t["type"], "|", t["what"][:160])

if __name__ == "__main__":
    try:
        main()
    except Exception as e:   # triage must never block the run: fall back to a full analysis
        json.dump({"mode": "full", "sweep": False, "triggers": [], "focus_ids": [], "edgar_review": [], "read_all_posts": True,
                   "error": str(e)[:200]}, open("triage.json", "w"))
        print(json.dumps({"mode": "full", "error": f"triage failed, defaulting to full: {str(e)[:200]}"}))
