#!/usr/bin/env python3
"""X-scout lead ledger: records every shortlisted lead, tracks its price vs a benchmark, and keeps
the learned PLAYBOOK. State (one JSON file) lives as the pinned document in a private Telegram
storage channel, so every cloud run can pull/push it with no local disk.
Env: TG_TOKEN, TG_STORE_CHAT.   All numbers come from price APIs, never from the model.

  ledger.py pull                 download the pinned state -> state.json; EXITS 1 if none/invalid (never starts empty)
  ledger.py pull --init          first-ever run only: start an empty ledger when nothing is pinned
  ledger.py push                 upload state.json and pin it; refuses unless state came from a successful pull
  ledger.py mark-alerted ASSET   flag the newest open lead for ASSET as alerted (run only AFTER the alert was sent)
  ledger.py send-alert FILE ASSET PLAN.json   send the alert text to Joseph WITH "Took it / Skipped" buttons, then
                                 mark the lead alerted and store its trading plan for the paper portfolio (one step)
  ledger.py poll-replies         read Joseph's button presses (took/skipped) and record them on the leads
  ledger.py paper                paper-portfolio report (plan-based simulated trades of every alert)
  ledger.py handles [--save]     follow/unfollow suggestions for @jpihbdu from lead results per X account
                                 (--save stores it for this month's digest line)

PLAN.json: {"direction": "long"|"short", "entry_lo": p, "entry_hi": p, "stop": p, "tps": [{"price": p, "pct": 50}, ...],
 "horizon_days": n, "size_pct": 2.0}   size_pct = % of book; paper trade fills when price trades inside the entry zone,
 exits at stop / take-profits / horizon (same-candle stop+TP counts as stop - conservative)
  ledger.py alerted-recent       JSON list of leads alerted in the last 14 days (for repeat-alert checks)
  ledger.py add leads.json       add shortlisted leads (list of dicts, see LEAD FIELDS) with entry prices
  ledger.py update               refresh prices of open leads; fill 1d/3d/7d/14d/30d checkpoints; close at 30d
  ledger.py stats                performance by score bucket / alerted / catalyst type / component / handle
  ledger.py playbook             print current playbook
  ledger.py set-playbook f.md    replace playbook (old version kept in playbook_history)
  ledger.py brief                compact summary for the run log (open leads, new checkpoints, track record)
  ledger.py runlog run.json      append this run's record (see RUN RECORD) to the run history
  ledger.py digest [--send|--if-due]  daily health digest of the last 24h to TG_ALERT_CHAT; --if-due sends only
                                 at/after 20:30 JST and once per JST day (a later run retries if the send failed)

RUN RECORD (run.json): {"mode": "full"|"light", "x": {"ok": bool, "captured": n, "relevant": n, "kept": n},
 "sources": {"binance": n, "okx": n, "edgar": n, "polymarket": n, "hyperliquid": n}, "source_errors": [..],
 "posts_read": n, "items_read": n, "leads_logged": n, "alerts_sent": ["ASSET", ..],
 "near_misses": [{"asset": "X", "score": 7, "why": "one line"}], "errors": [".."]}

LEAD FIELDS: asset (e.g. "LDO"), kind ("crypto"|"stock"|"other"), cg_id (CoinGecko id, crypto) or
ticker (Yahoo symbol, stock), score (0-10), components {catalyst,timing,asym,liq,cred},
catalyst_type (listing|unlock|regulatory|earnings|flows|token_sale|macro|partnership|tech_upgrade|
polymarket|other), direction ("long"|"short"|"watch"), thesis, handles [..], urls [..], alerted (bool)
"""
import json, os, re, sys, time, urllib.request, urllib.parse, statistics
from datetime import datetime, timezone, timedelta

TOKEN = os.environ.get("TG_TOKEN", "")
STORE = os.environ.get("TG_STORE_CHAT", "")
ALERT_CHAT = os.environ.get("TG_ALERT_CHAT", "")
SF = "state.json"
CHECKPOINTS = [("1d", 1), ("3d", 3), ("7d", 7), ("14d", 14), ("30d", 30)]
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/133 Safari/537.36"}
now = lambda: datetime.now(timezone.utc)

def http(url, data=None, headers=None, timeout=30, raw=False):
    req = urllib.request.Request(url, data=data, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        b = r.read()
    return b if raw else json.loads(b)

def tg(method, **params):
    return http(f"https://api.telegram.org/bot{TOKEN}/{method}", urllib.parse.urlencode(params).encode())

def empty():
    return {"version": 0, "updated": None, "leads": [], "playbook": "", "playbook_history": [], "runs": []}

def load():
    return json.load(open(SF)) if os.path.exists(SF) else empty()

def save(s):
    json.dump(s, open(SF, "w"), ensure_ascii=False, indent=1)

# ---------- storage ----------
def pull(init=False):
    """Fails (exit 1) unless a valid pinned state document is found. `pull --init` (first ever run only) starts empty."""
    chat = tg("getChat", chat_id=STORE)["result"]
    pm = chat.get("pinned_message") or {}
    doc = pm.get("document")
    if not doc:
        if init and not pm:
            s = empty(); s["_pulled"] = 0; s["_pulled_leads"] = 0; save(s); print("PULL: --init, starting an empty ledger"); return
        if os.path.exists(SF): os.remove(SF)
        print("PULL FAILED: pinned message is missing or has no state document - refusing to start empty"); sys.exit(1)
    fp = tg("getFile", file_id=doc["file_id"])["result"]["file_path"]
    b = http(f"https://api.telegram.org/file/bot{TOKEN}/{fp}", raw=True)
    s = json.loads(b)
    if not isinstance(s, dict) or "leads" not in s:
        print("PULL FAILED: pinned document is not a ledger state"); sys.exit(1)
    s["_pulled"] = s.get("version", 0); s["_pulled_leads"] = len(s.get("leads", []))
    save(s); print(f"PULL: state v{s.get('version')} leads={len(s.get('leads', []))} updated={s.get('updated')}")

def push():
    if not os.path.exists(SF):
        print("PUSH REFUSED: no state.json (pull first)"); sys.exit(1)
    s = load()
    if "_pulled" not in s:
        print("PUSH REFUSED: state.json did not come from a successful pull"); sys.exit(1)
    if len(s.get("leads", [])) < s.get("_pulled_leads", 0):
        print("PUSH REFUSED: fewer leads than were pulled - state looks truncated"); sys.exit(1)
    s["version"] = s["_pulled"] + 1; s["updated"] = now().isoformat(timespec="minutes")
    save(s)
    boundary = "xscout" + str(int(time.time()))
    body = b""
    for k, v in (("chat_id", STORE), ("disable_notification", "true"),
                 ("caption", f"x-scout state v{s['version']} {s['updated']} leads={len(s['leads'])}")):
        body += f"--{boundary}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n".encode()
    body += (f"--{boundary}\r\nContent-Disposition: form-data; name=\"document\"; filename=\"xscout_state.json\"\r\n"
             f"Content-Type: application/json\r\n\r\n").encode() + open(SF, "rb").read() + f"\r\n--{boundary}--\r\n".encode()
    r = http(f"https://api.telegram.org/bot{TOKEN}/sendDocument", body,
             {"Content-Type": f"multipart/form-data; boundary={boundary}"}, timeout=60)
    mid = r["result"]["message_id"]
    for attempt in range(3):
        try:
            tg("pinChatMessage", chat_id=STORE, message_id=mid, disable_notification="true"); break
        except Exception as e:
            if attempt == 2: print(f"PUSH FAILED: uploaded but could not pin ({str(e)[:80]})"); sys.exit(1)
            time.sleep(5)
    s["_pulled"] = s["version"]; s["_pulled_leads"] = len(s["leads"]); save(s)   # allows a second push (learning step)
    print(f"PUSH: state v{s['version']} pinned (msg {mid})")

# ---------- prices ----------
def crypto_prices(ids):
    ids = sorted(set(i for i in ids if i))
    out = {}
    for i in range(0, len(ids), 50):
        chunk = ",".join(ids[i:i+50])
        for attempt in range(3):
            try:
                d = http(f"https://api.coingecko.com/api/v3/simple/price?ids={chunk}&vs_currencies=usd", headers=UA)
                out.update({k: v.get("usd") for k, v in d.items()}); break
            except Exception:
                time.sleep(8)
    return out

def stock_prices(tickers):
    out = {}
    for t in sorted(set(x for x in tickers if x)):
        q = urllib.parse.quote(t)
        for url in (f"https://query2.finance.yahoo.com/v8/finance/chart/{q}?range=1d&interval=1d",
                    f"https://query1.finance.yahoo.com/v8/finance/chart/{q}?range=1d&interval=1d",
                    f"https://api.nasdaq.com/api/quote/{q}/info?assetclass=stocks"):
            try:
                d = http(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}, timeout=20)
                if "chart" in d:
                    out[t] = d["chart"]["result"][0]["meta"]["regularMarketPrice"]
                else:
                    out[t] = float(d["data"]["primaryData"]["lastSalePrice"].replace("$", "").replace(",", ""))
                break
            except Exception:
                time.sleep(3)
    return out

def price_map(leads):
    cg = [l.get("cg_id") for l in leads if l.get("kind") == "crypto"] + ["bitcoin"]
    st = [l.get("ticker") for l in leads if l.get("kind") == "stock"] + ["SPY"]
    return crypto_prices(cg), stock_prices(st)

def px(l, cp, sp):
    if l.get("kind") == "crypto": return cp.get(l.get("cg_id")), cp.get("bitcoin")
    if l.get("kind") == "stock": return sp.get(l.get("ticker")), sp.get("SPY")
    return None, None

# ---------- commands ----------
def add(path):
    s = load(); new = json.load(open(path)); cp, sp = price_map(new)
    t = now(); added = merged = 0
    for l in new:
        key = (l.get("asset") or "").upper()
        recent = [x for x in s["leads"] if x["asset"].upper() == key and x["status"] in ("open", "untracked")
                  and (t - datetime.fromisoformat(x["first_seen"])).total_seconds() < 72 * 3600]
        if recent:
            x = recent[0]
            x.setdefault("mentions", []).append({"at": t.isoformat(timespec="minutes"), "score": l.get("score"),
                                                  "alerted": bool(l.get("alerted")), "handles": l.get("handles", [])})
            x["max_score"] = max(x.get("max_score", x["score"]), l.get("score") or 0)
            x["alerted"] = x["alerted"] or bool(l.get("alerted")); merged += 1; continue
        p, b = px(l, cp, sp)
        s["leads"].append({**{k: l.get(k) for k in ("asset", "kind", "cg_id", "ticker", "score", "components",
            "catalyst_type", "direction", "thesis", "handles", "urls")},
            "id": f"{key}-{t.strftime('%Y%m%d%H%M')}", "alerted": bool(l.get("alerted")), "max_score": l.get("score"),
            "first_seen": t.isoformat(timespec="minutes"), "entry": p, "bench_entry": b,
            "bench": "BTC" if l.get("kind") == "crypto" else ("SPY" if l.get("kind") == "stock" else None),
            "status": "open" if p else "untracked", "cp": {}, "mfe": 0.0, "mae": 0.0, "last": p, "last_at": None})
        added += 1
    save(s); print(f"ADD: {added} new, {merged} merged into existing open leads")

def update():
    s = load()
    retry = [l for l in s["leads"] if l["status"] == "untracked" and l.get("kind") in ("crypto", "stock")
             and (now() - datetime.fromisoformat(l["first_seen"])).total_seconds() < 86400]
    if retry:
        cp, sp = price_map(retry)
        for l in retry:
            p, b = px(l, cp, sp)
            if p:
                l.update(entry=p, bench_entry=b, status="open", entry_late=True, last=p)
    open_ = [l for l in s["leads"] if l["status"] == "open"]
    if not open_:
        try: paper_update(s)
        except Exception: pass
        save(s); print("UPDATE: no open leads"); return
    cp, sp = price_map(open_); t = now(); filled = []
    for l in open_:
        p, b = px(l, cp, sp)
        if not p or not l.get("entry"): continue
        sign = -1 if l.get("direction") == "short" else 1
        r = sign * (p / l["entry"] - 1)
        l["mfe"] = round(max(l.get("mfe", 0), r), 4); l["mae"] = round(min(l.get("mae", 0), r), 4)
        l["last"], l["last_at"] = p, t.isoformat(timespec="minutes")
        age_d = (t - datetime.fromisoformat(l["first_seen"])).total_seconds() / 86400
        for name, days in CHECKPOINTS:
            if name not in l["cp"] and age_d >= days:
                br = (b / l["bench_entry"] - 1) if (b and l.get("bench_entry")) else None
                l["cp"][name] = {"price": p, "ret": round(r, 4), "bench_ret": round(br, 4) if br is not None else None,
                                 "excess": round(r - sign * br, 4) if br is not None else None, "age_d": round(age_d, 2)}
                filled.append(f"{l['asset']} {name}: {r:+.1%} (excess {l['cp'][name]['excess']:+.1%})" if br is not None
                              else f"{l['asset']} {name}: {r:+.1%}")
        if age_d >= 30: l["status"] = "closed"
    try: pc = paper_update(s)
    except Exception as e: pc = [f"paper update failed: {str(e)[:60]}"]
    save(s); print(f"UPDATE: {len(open_)} open leads priced; new checkpoints: {filled or 'none'}; paper: {pc or 'no change'}")

def _grp(rows, keyf, h="7d"):
    g = {}
    for l in rows:
        c = l["cp"].get(h)
        if not c or c.get("excess") is None: continue
        for k in (keyf(l) if isinstance(keyf(l), list) else [keyf(l)]):
            g.setdefault(str(k), []).append(c["excess"])
    return {k: {"n": len(v), "mean_excess": round(statistics.mean(v), 4), "median_excess": round(statistics.median(v), 4),
                "hit_rate": round(sum(1 for x in v if x > 0) / len(v), 2)} for k, v in sorted(g.items())}

def stats():
    s = load(); L = s["leads"]; out = {"leads_total": len(L), "open": sum(l["status"] == "open" for l in L)}
    for h in ("1d", "3d", "7d", "14d", "30d"):
        out[h] = {"by_score": _grp(L, lambda l: f"{int(l['score'] or 0)}", h),
                  "by_alerted": _grp(L, lambda l: "alerted" if l["alerted"] else "not_alerted", h)}
    out["7d_detail"] = {
        "by_catalyst_type": _grp(L, lambda l: l.get("catalyst_type") or "other"),
        "by_kind": _grp(L, lambda l: l.get("kind")),
        "by_component": {c: _grp(L, lambda l, c=c: (l.get("components") or {}).get(c)) for c in ("catalyst", "timing", "asym", "liq", "cred")},
        "by_handle_top": dict(sorted(_grp(L, lambda l: l.get("handles") or ["?"]).items(), key=lambda kv: -kv[1]["n"])[:25]),
        "multi_mention": _grp(L, lambda l: "re-mentioned" if l.get("mentions") else "single"),
        "by_decision": _grp([l for l in L if l.get("alerted")], lambda l: l.get("decision") or "no_answer")}
    cl = [l for l in L if (l.get("paper") or {}).get("state") == "closed"]
    out["paper"] = {"closed": len(cl), "book_pnl_pct": round(sum(l["paper"].get("book_pnl_pct", 0) for l in cl), 3),
                    "win_rate": round(sum(1 for l in cl if l["paper"].get("pnl_pct", 0) > 0) / len(cl), 2) if cl else None,
                    "by_exit": {k: sum(1 for l in cl if l["paper"].get("exit_reason") == k) for k in ("stop", "targets", "horizon")}}
    print(json.dumps(out, ensure_ascii=False, indent=1))

def playbook():
    print(load().get("playbook") or "(empty playbook - no learned rules yet)")

def set_playbook(path):
    s = load(); old = s.get("playbook") or ""
    if old:
        s.setdefault("playbook_history", []).append({"until": now().isoformat(timespec="minutes"), "text": old})
        s["playbook_history"] = s["playbook_history"][-20:]
    s["playbook"] = open(path).read().strip(); save(s); print("PLAYBOOK updated")

def brief():
    s = load(); L = s["leads"]; t = now()
    o = [l for l in L if l["status"] == "open"]
    lines = [f"Ledger: {len(L)} leads total, {len(o)} open, playbook {'set' if s.get('playbook') else 'empty'}"]
    for l in sorted(o, key=lambda l: l["first_seen"], reverse=True)[:12]:
        r = (l["last"] / l["entry"] - 1) * (-1 if l.get("direction") == "short" else 1) if l.get("entry") and l.get("last") else None
        lines.append(f"- {l['asset']} s{l['score']}{' ALERTED' if l['alerted'] else ''} since {l['first_seen'][:16]} "
                     f"now {r:+.1%}" if r is not None else f"- {l['asset']} s{l['score']} (no price)")
    a7 = [((l.get("cp") or {}).get("7d") or {}).get("excess") for l in L if l.get("alerted")]
    a7 = [x for x in a7 if x is not None]
    if a7: lines.append(f"Alert track record: n={len(a7)} 7d median excess {statistics.median(a7):+.1%}, hit {sum(x>0 for x in a7)/len(a7):.0%}")
    print("\n".join(lines))

def mark_alerted(asset):
    s = load(); key = asset.upper(); t = now()
    cands = [l for l in s["leads"] if l["asset"].upper() == key and l["status"] in ("open", "untracked")]
    if not cands: print(f"MARK: no open lead {asset}"); sys.exit(1)
    l = max(cands, key=lambda l: l["first_seen"]); l["alerted"] = True; l["alerted_at"] = t.isoformat(timespec="minutes")
    save(s); print(f"MARK: {l['id']} alerted")

def alerted_recent(days=14):
    s = load(); t = now()
    rows = [l for l in s["leads"] if l.get("alerted") and l.get("first_seen")
            and (t - datetime.fromisoformat(l["first_seen"])).total_seconds() < days * 86400]
    print(json.dumps([{"asset": l["asset"], "first_seen": l["first_seen"], "thesis": l.get("thesis"), "score": l.get("score")} for l in rows], ensure_ascii=False))

# ---------- alerts with buttons + paper portfolio ----------
def _cb_id(lead_id):
    import hashlib
    return hashlib.sha1(lead_id.encode()).hexdigest()[:12]

def _kb(lead_id):
    c = _cb_id(lead_id)
    return json.dumps({"inline_keyboard": [[{"text": "✅ Took it", "callback_data": f"took:{c}"},
                                             {"text": "⏭ Skipped", "callback_data": f"skip:{c}"}]]})

def _valid_plan(p):
    try:
        d = str(p.get("direction", "long")).strip().lower()
        d = {"buy": "long", "sell": "short"}.get(d, d)
        if d not in ("long", "short"): return None
        lo, hi, st = float(p["entry_lo"]), float(p["entry_hi"]), float(p["stop"])
        tps = [(float(t["price"]), float(t.get("pct", 100))) for t in p.get("tps", [])]
        if lo > hi: lo, hi = hi, lo
        hz, sz = float(p.get("horizon_days", 14)), float(p.get("size_pct", 1))
        if min(lo, st) <= 0 or not (0 < hz <= 120) or not (0 < sz <= 20) or any(pc <= 0 for _, pc in tps): return None
        if d == "long" and not (st < lo and all(tp > hi for tp, _ in tps)): return None
        if d == "short" and not (st > hi and all(tp < lo for tp, _ in tps)): return None
        return {"direction": d, "entry_lo": lo, "entry_hi": hi, "stop": st, "tps": sorted(tps, reverse=(d == "short")) if d == "short" else sorted(tps),
                "horizon_days": hz, "size_pct": sz}
    except Exception:
        return None

def _chunks(text, lim=3500):
    out, buf = [], ""
    for line in text.splitlines(keepends=True):
        while len(line) > lim:                      # hard-split very long lines
            if buf: out.append(buf); buf = ""
            out.append(line[:lim]); line = line[lim:]
        if len(buf) + len(line) > lim:
            out.append(buf); buf = ""
        buf += line
    if buf: out.append(buf)
    return [c for c in out if c.strip()]

def send_alert(path, asset, plan_path):
    s = load(); key = asset.upper()
    cands = [l for l in s["leads"] if l["asset"].upper() == key and l["status"] in ("open", "untracked")]
    if not cands: print(f"SEND-ALERT: no open lead {asset} - run ledger.py add first"); sys.exit(1)
    l = max(cands, key=lambda l: l["first_seen"])
    text = open(path).read() if os.path.exists(path) else ""
    chunks = _chunks(text)
    if not chunks: print("SEND-ALERT: alert text is empty - nothing sent"); sys.exit(1)
    try: plan = _valid_plan(json.load(open(plan_path)))
    except Exception: plan = None
    sent = 0
    for i, c in enumerate(chunks):
        params = {"chat_id": ALERT_CHAT, "text": c, "disable_web_page_preview": "true"}
        if i == len(chunks) - 1: params["reply_markup"] = _kb(l["id"])
        try:
            tg("sendMessage", **params); sent += 1
        except Exception as e:
            print(f"SEND-ALERT: chunk {i + 1}/{len(chunks)} failed ({str(e)[:80]})"); break
    if sent == 0: print("SEND-ALERT: nothing delivered - lead NOT marked; safe to retry"); sys.exit(1)
    l["alerted"] = True; l["cb"] = _cb_id(l["id"])
    if sent < len(chunks): l["alert_partial"] = True
    if not l.get("alerted_at"): l["alerted_at"] = now().isoformat(timespec="minutes")
    pp = l.get("paper") or {}
    if pp.get("state") in ("waiting", "open", "closed", "not_filled"):
        msg = "existing paper trade kept"
    elif plan:
        l["plan"] = plan; l["paper"] = {"state": "waiting", "last_ts": time.time(), "fills": [], "pnl_pct": 0.0}; msg = "plan stored"
    else:
        l["paper"] = {"state": "no_plan"}; msg = "plan INVALID/missing - paper trade skipped"
    save(s)
    print(f"SEND-ALERT: sent {sent}/{len(chunks)} message(s) for {l['id']}; {msg}" + (" - PARTIAL, lead still marked alerted" if sent < len(chunks) else ""))
    if sent < len(chunks): sys.exit(2)

def poll_replies():
    s = load(); off = s.get("tg_offset", 0); n = 0
    try:
        r = tg("getUpdates", offset=off, timeout=0, allowed_updates=json.dumps(["callback_query"]))
    except Exception as e:
        print(f"POLL: failed ({str(e)[:80]})"); return
    for u in r.get("result", []):
        s["tg_offset"] = u["update_id"] + 1
        cq = u.get("callback_query")
        if not cq: continue
        act, _, cid = str(cq.get("data", "")).partition(":")
        note = "Not recorded"
        if str((cq.get("from") or {}).get("id")) == str(ALERT_CHAT) and act in ("took", "skip"):
            for l in s["leads"]:
                if l.get("cb") == cid or l["id"] == cid:
                    l["decision"] = "took" if act == "took" else "skipped"; l["decision_at"] = now().isoformat(timespec="minutes")
                    n += 1; note = f"Recorded: {l['decision']}"
                    try:
                        m = cq.get("message") or {}
                        tg("editMessageReplyMarkup", chat_id=m.get("chat", {}).get("id"), message_id=m.get("message_id"),
                           reply_markup=json.dumps({"inline_keyboard": [[{"text": f"✔ {l['decision']} (tap below to change)", "callback_data": "noop"}],
                                                    [{"text": "✅ Took it", "callback_data": f"took:{cid}"}, {"text": "⏭ Skipped", "callback_data": f"skip:{cid}"}]]}))
                    except Exception: pass
                    break
        try: tg("answerCallbackQuery", callback_query_id=cq["id"], text=note)
        except Exception: pass
    save(s); print(f"POLL: {n} decision(s) recorded")

def _candles(l, since):
    """[(ts, open, high, low, close)] hourly candles that OPEN at/after `since` (unix). None = data unavailable."""
    out = []
    if l.get("kind") == "crypto":
        sym = (l.get("asset") or "").upper()
        try:
            d = http(f"https://data-api.binance.vision/api/v3/klines?symbol={sym}USDT&interval=1h&startTime={int(since*1000)}&limit=1000", headers=UA)
            out = [(k[0] / 1000, float(k[1]), float(k[2]), float(k[3]), float(k[4])) for k in d]
        except Exception:
            out = []
        if not out and l.get("cg_id"):
            try:
                d = http(f"https://api.coingecko.com/api/v3/coins/{l['cg_id']}/market_chart/range?vs_currency=usd&from={int(since)}&to={int(time.time())}", headers=UA)
                out = [(p[0] / 1000, p[1], p[1], p[1], p[1]) for p in d.get("prices", [])]
            except Exception:
                return None
    elif l.get("kind") == "stock" and l.get("ticker"):
        try:
            d = http(f"https://query2.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(l['ticker'])}?period1={int(since)}&period2={int(time.time())}&interval=60m",
                     headers={"User-Agent": "Mozilla/5.0"})
            r = d["chart"]["result"][0]; q = r["indicators"]["quote"][0]
            out = [(t, o, h, lo, c) for t, o, h, lo, c in zip(r["timestamp"], q["open"], q["high"], q["low"], q["close"]) if None not in (o, h, lo, c)]
        except Exception:
            return None
    else:
        return None
    return [c for c in out if c[0] >= since]

def _sim_lead(l):
    pp, plan = l["paper"], l["plan"]
    start = datetime.fromisoformat(l.get("alerted_at") or l["first_seen"]).timestamp()
    end = start + plan["horizon_days"] * 86400
    since = max(pp.get("last_ts", start), start)
    cs = _candles(l, since)
    if not cs:
        if time.time() > end + 86400:
            if pp["state"] == "waiting":
                pp.update(state="not_filled", note="no price data"); return f"{l['asset']} not filled (no price data)"
            if pp["state"] == "open":   # no data after the horizon: close at the last known mark
                pp["realized"] = pp.get("mark", pp.get("realized", 0.0)); pp["fills"].append(["time_nodata", None, pp.get("remaining", 0)])
                pp.update(state="closed", remaining=0.0, exit_reason="horizon (no data)")
                pp["pnl_pct"] = round(pp["realized"], 4); pp["book_pnl_pct"] = round(pp["realized"] * plan["size_pct"], 4)
                pp["reported"] = True; pp["closed_at"] = now().isoformat(timespec="minutes")
                return f"{l['asset']} closed at last mark (no price data) {pp['pnl_pct']:+.1%}"
        return None
    long_ = plan["direction"] == "long"
    for ts, o, h, lo, c in cs:
        if pp["state"] == "waiting":
            if ts >= end:
                pp.update(state="not_filled"); return f"{l['asset']} not filled"
            if lo <= plan["entry_hi"] and h >= plan["entry_lo"]:
                fill = min(max(o, plan["entry_lo"]), plan["entry_hi"])
                pp.update(state="open", entry=fill, entry_ts=ts, remaining=100.0, realized=0.0)
                ret = lambda p, e=fill: (p / e - 1) * (1 if long_ else -1)
                if (lo <= plan["stop"]) if long_ else (h >= plan["stop"]):      # fill candle: only the stop can trigger
                    ex = plan["stop"]                                                  # order unknown within the candle: assume the stop hit
                    pp["realized"] += ret(ex); pp["fills"].append(["stop", ex, 100.0]); pp.update(state="closed", remaining=0.0, exit_reason="stop")
                    break
                pp["mark"] = ret(c)
            pp["last_ts"] = ts
            continue
        if pp["state"] == "open":
            e = pp["entry"]; ret = lambda p: (p / e - 1) * (1 if long_ else -1)
            if (lo <= plan["stop"]) if long_ else (h >= plan["stop"]):
                ex = min(o, plan["stop"]) if long_ else max(o, plan["stop"])      # gap through the stop fills at the open
                pp["realized"] += pp["remaining"] / 100 * ret(ex); pp["fills"].append(["stop", ex, pp["remaining"]])
                pp.update(state="closed", remaining=0.0, exit_reason="stop"); pp["last_ts"] = ts; break
            done = {f[1] for f in pp["fills"] if f[0] == "tp"}
            for tp, pct in (plan["tps"] if ts != pp.get("entry_ts") else []):   # the fill candle never counts for targets
                if tp in done or pp["remaining"] <= 0: continue
                if (h >= tp) if long_ else (lo <= tp):
                    take = min(pct, pp["remaining"]); pp["realized"] += take / 100 * ret(tp); pp["remaining"] -= take
                    pp["fills"].append(["tp", tp, take])
            if pp["remaining"] <= 0.001:
                pp.update(state="closed", exit_reason="targets"); pp["last_ts"] = ts; break
            if ts >= end:
                pp["realized"] += pp["remaining"] / 100 * ret(c); pp["fills"].append(["time", c, pp["remaining"]])
                pp.update(state="closed", remaining=0.0, exit_reason="horizon"); pp["last_ts"] = ts; break
            pp["mark"] = pp["realized"] + pp["remaining"] / 100 * ret(c)
            pp["last_ts"] = ts
    if pp.get("state") == "closed" and "reported" not in pp:
        pp["pnl_pct"] = round(pp["realized"], 4); pp["book_pnl_pct"] = round(pp["realized"] * plan["size_pct"], 4); pp["reported"] = True
        pp["closed_at"] = now().isoformat(timespec="minutes")
        return f"{l['asset']} closed ({pp['exit_reason']}) {pp['pnl_pct']:+.1%} = {pp['book_pnl_pct']:+.2f}% of book"
    return None

def paper_update(s):
    changed = []
    for l in s["leads"]:
        if not l.get("plan") or (l.get("paper") or {}).get("state") not in ("waiting", "open"): continue
        try:
            m = _sim_lead(l)
            if m: changed.append(m)
        except Exception as e:
            changed.append(f"{l.get('asset')} paper error: {str(e)[:60]}")
    return changed

def paper():
    s = load(); P = [l for l in s["leads"] if l.get("plan")]
    closed = [l for l in P if (l.get("paper") or {}).get("state") == "closed"]
    openp = [l for l in P if (l.get("paper") or {}).get("state") == "open"]
    wait = [l for l in P if (l.get("paper") or {}).get("state") == "waiting"]
    nf = [l for l in P if (l.get("paper") or {}).get("state") == "not_filled"]
    book = sum(l["paper"].get("book_pnl_pct", 0) for l in closed)
    wins = sum(1 for l in closed if l["paper"].get("pnl_pct", 0) > 0)
    took = [l for l in s["leads"] if l.get("decision") == "took"]; skip = [l for l in s["leads"] if l.get("decision") == "skipped"]
    lines = [f"Paper portfolio: {len(closed)} closed (win {wins}/{len(closed)}), book P/L {book:+.2f}%, {len(openp)} open, {len(wait)} awaiting entry, {len(nf)} never filled",
             f"Your decisions: took {len(took)}, skipped {len(skip)}, no answer {sum(1 for l in s['leads'] if l.get('alerted') and not l.get('decision'))}"]
    for l in openp: lines.append(f"- {l['asset']} open, mark {l['paper'].get('mark', 0):+.1%}")
    for l in sorted(closed, key=lambda l: l["paper"].get("closed_at", ""))[-5:]: lines.append(f"- {l['asset']} {l['paper'].get('exit_reason')} {l['paper'].get('pnl_pct', 0):+.1%} ({l.get('decision') or 'no answer'})")
    print("\n".join(lines))

def _norm_handles(v):
    out = set()
    for x in _list(v):
        if not isinstance(x, str): continue
        for part in re.split(r"[,\s]+", x.strip()):
            m = re.search(r"(?:(?:x|twitter)\.com/|@)?([A-Za-z0-9_]{1,15})(?:/|$)", part)
            if m and part: out.add(m.group(1).lower())
    return out

def _wilson(k, n, z=1.64):
    if n == 0: return 0.0, 1.0
    p = k / n; d = 1 + z * z / n; c = p + z * z / (2 * n); r = z * ((p * (1 - p) + z * z / (4 * n)) / n) ** 0.5
    return (c - r) / d, (c + r) / d

def handles(save_it=False):
    """Follow/unfollow suggestions for @jpihbdu (Joseph follows/unfollows manually).
    Per account, last 90 days: one 7d excess result per distinct asset (first lead), credit for original and
    re-mention handles, compared with the median of ALL leads in the window (so a broad rally doesn't flatter everyone).
    follow   = not followed (per X's own follow flag, else no Following-tab appearances in 90d), n>=8,
               Wilson lower bound of beat-the-median rate > 0.5
    unfollow = followed, n>=8, Wilson upper bound < 0.5
    noisy    = followed, >=30 relevant posts in 90d, contributed to no lead"""
    s = load(); t = now(); seen = s.get("handle_seen") if isinstance(s.get("handle_seen"), dict) else {}
    months = {(_jst(t) - timedelta(days=31 * k)).strftime("%Y-%m") for k in range(3)}
    def fsd(x):
        try:
            d = datetime.fromisoformat(str(x)); return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
        except Exception: return None
    window = []
    for l in s.get("leads", []):
        f = fsd(l.get("first_seen")); ex = ((l.get("cp") or {}).get("7d") or {}).get("excess") if isinstance(l.get("cp"), dict) else None
        if f and (t - f).total_seconds() <= 90 * 86400 and isinstance(ex, (int, float)): window.append((l, ex))
    base = statistics.median([ex for _, ex in window]) if window else 0.0
    per = {}
    for l, ex in window:
        hs = _norm_handles(l.get("handles"))
        for m in _list(l.get("mentions")):
            if isinstance(m, dict): hs |= _norm_handles(m.get("handles"))
        for h in hs:
            d = per.setdefault(h, {"assets": {}, "leads": 0}); d["leads"] += 1
            d["assets"].setdefault(str(l.get("asset")).upper(), ex)       # one result per asset
    def seen_stats(h):
        rec = seen.get(h) if isinstance(seen.get(h), dict) else {}
        mb = rec.get("m") if isinstance(rec.get("m"), dict) else {}
        fol = sum(_num((mb.get(k) or {}).get("following")) for k in months if isinstance(mb.get(k), dict))
        oth = sum(_num((mb.get(k) or {}).get("other")) for k in months if isinstance(mb.get(k), dict))
        followed = rec.get("followed") if isinstance(rec.get("followed"), bool) else (fol > 0)
        return int(fol), int(oth), followed
    rows = []
    for h, d in per.items():
        xs = list(d["assets"].values()); k = sum(1 for x in xs if x > base)
        lo, hi = _wilson(k, len(xs)); fol, oth, followed = seen_stats(h)
        rows.append({"handle": h, "assets": len(xs), "beat_median": k, "median_7d_excess": round(statistics.median(xs), 4),
                     "wilson_lo": round(lo, 2), "wilson_hi": round(hi, 2), "followed": followed, "posts_90d": fol + oth})
    follow = [r for r in rows if not r["followed"] and r["assets"] >= 8 and r["wilson_lo"] > 0.5]
    unfollow = [r for r in rows if r["followed"] and r["assets"] >= 8 and r["wilson_hi"] < 0.5]
    noisy = []
    for h in seen:
        fol, oth, followed = seen_stats(h)
        if followed and fol + oth >= 30 and h not in per: noisy.append({"handle": h, "posts_90d": fol + oth})
    out = {"month": _jst(t).strftime("%Y-%m"), "benchmark_median_7d_excess": round(base, 4), "leads_in_window": len(window),
           "follow": sorted(follow, key=lambda r: -r["wilson_lo"])[:10], "unfollow": sorted(unfollow, key=lambda r: r["wilson_hi"])[:10],
           "noisy_following": sorted(noisy, key=lambda r: -r["posts_90d"])[:10],
           "top_accounts": sorted([r for r in rows if r["assets"] >= 3], key=lambda r: -r["median_7d_excess"])[:10],
           "note": "Suggestions only; needs >=8 distinct-asset results per account. Joseph follows/unfollows manually on @jpihbdu."}
    print(json.dumps(out, ensure_ascii=False, indent=1))
    if save_it:
        rep = {"month": out["month"], "follow": [r["handle"] for r in out["follow"]], "unfollow": [r["handle"] for r in out["unfollow"]],
               "noisy": [r["handle"] for r in out["noisy_following"]], "leads_in_window": len(window)}
        rep["id"] = f"{out['month']}:{hash(json.dumps(rep, sort_keys=True)) & 0xffff}"
        s["handles_report"] = rep; save(s)

RUNS_PER_DAY = 8          # scheduled at 02:45, 05:45 ... 23:45 JST
DIGEST_AFTER_JST_HOUR = 20  # first run at/after 20:30 JST sends the digest; a later run retries if it failed

def _jst(dt):
    return dt.astimezone(timezone(timedelta(hours=9)))

def _slot(dt):
    """Scheduled 3-hour slot a run belongs to (runs start at hh:45 JST, finish ~10-25 min later)."""
    j = _jst(dt) - timedelta(hours=2, minutes=15)
    j = j.replace(minute=0, second=0, microsecond=0)
    return (j - timedelta(hours=j.hour % 3)).strftime("%Y-%m-%dT%H")

def _num(v):
    try: return float(v)
    except Exception: return 0.0

def _list(v):
    return v if isinstance(v, list) else ([] if v is None else [v])

def runlog(path):
    try:
        r = json.load(open(path))
        if not isinstance(r, dict): raise ValueError("run record is not a JSON object")
    except Exception as e:
        r = {"errors": [f"runlog: bad run record ({str(e)[:80]})"]}
    def red(x):
        x = str(x)
        x = re.sub(r"bot\d+:[A-Za-z0-9_-]{20,}", "bot<redacted>", x)
        x = re.sub(r"\b\d{6,}:[A-Za-z0-9_-]{25,}\b", "<redacted>", x)
        return re.sub(r"\b[0-9a-f]{32,}\b", "<redacted>", x)[:300]
    r["errors"] = [red(e) for e in _list(r.get("errors"))]
    r["source_errors"] = [red(f"{e.get('src', '?')}: {e.get('error', '')}" if isinstance(e, dict) else e) for e in _list(r.get("source_errors"))]
    nm = []
    for m in _list(r.get("near_misses")):
        if isinstance(m, dict): nm.append({"asset": str(m.get("asset") or "?"), "score": _num(m.get("score")), "why": red(m.get("why") or "")[:160]})
        elif isinstance(m, str) and m.strip(): nm.append({"asset": m.split()[0], "score": 0, "why": red(m)[:160]})
    r["near_misses"] = nm
    r["alerts_sent"] = [str(a) for a in _list(r.get("alerts_sent"))]
    s = load(); t = now(); r["at"] = t.isoformat(timespec="minutes"); r["slot"] = _slot(t)
    runs = [x for x in s.get("runs", []) if x.get("slot") != r["slot"]]  # a retried run replaces its slot
    runs.append(r); s["runs"] = runs[-300:]
    save(s); print(f"RUNLOG: recorded run at {r['at']} slot {r['slot']} ({len(s['runs'])} in history)")

def digest(send=False, if_due=False):
    s = load(); t = now(); tj = _jst(t)
    h = tj.hour + tj.minute / 60
    day = (tj - timedelta(days=1)).strftime("%Y-%m-%d") if h < 3 else tj.strftime("%Y-%m-%d")   # 23:45 run may finish after midnight
    if if_due and ((3 <= h < DIGEST_AFTER_JST_HOUR + 0.5) or s.get("last_digest") == day):
        print("DIGEST: not due"); return
    if send and s.get("last_digest") == day:
        print(f"DIGEST: already sent for {day}"); return
    hist = [r for r in s.get("runs", []) if isinstance(r, dict) and r.get("at")]
    def dt(x):
        try: return datetime.fromisoformat(x)
        except Exception: return None
    runs = [r for r in hist if dt(r["at"]) and (t - dt(r["at"])).total_seconds() < 24 * 3600 - 5400]
    # expected slots: every scheduled 3h slot in the last 24h, but not before run history began
    first = min((dt(r["at"]) for r in hist if dt(r["at"])), default=t)
    exp = set()
    for k in range(RUNS_PER_DAY + 1):
        c = t - timedelta(hours=3 * k)
        if (t - c).total_seconds() < 24 * 3600 - 5400 and c >= first - timedelta(minutes=30):
            exp.add(_slot(c))
    got = {r.get("slot") or _slot(dt(r["at"])) for r in runs}
    missing = sorted(exp - got)
    L = s.get("leads", [])
    problems = []
    if not hist:
        problems.append("no run records at all - runs are not logging (check the scheduled task)")
    elif missing:
        problems.append(f"{len(missing)} scheduled run(s) missing in 24h (the {', '.join((datetime.strptime(m, '%Y-%m-%dT%H') + timedelta(hours=2, minutes=45)).strftime('%H:%M') for m in missing)} JST runs) - crashed or failed to save")
    xbad = [r for r in runs if isinstance(r.get("x"), dict) and r["x"].get("ok") is False]
    if xbad: problems.append(f"X fetch failed in {len(xbad)} run(s) - cookies may be expiring")
    serr = sorted({str(e).split(":")[0] for r in runs for e in _list(r.get("source_errors"))})
    if serr: problems.append("source errors: " + ", ".join(serr))
    errs = [str(e) for r in runs for e in _list(r.get("errors"))]
    if errs: problems.append(f"{len(errs)} run error(s): " + "; ".join(errs[:3])[:300])
    unpriced = [l.get("asset") for l in L if l.get("status") == "untracked" and l.get("kind") in ("crypto", "stock")
                and dt(l.get("first_seen", "")) and (t - dt(l["first_seen"])).total_seconds() < 86400]
    if unpriced: problems.append("no price source for: " + ", ".join(map(str, unpriced[:6])))
    posts = int(sum(_num(r.get("posts_read")) for r in runs)); items = int(sum(_num(r.get("items_read")) for r in runs))
    full = sum(1 for r in runs if r.get("mode") == "full")
    leads = int(sum(_num(r.get("leads_logged")) for r in runs))
    alerts = [str(a) for r in runs for a in _list(r.get("alerts_sent"))]
    alerted_up = {a.upper() for a in alerts}
    nm = sorted([m for r in runs for m in _list(r.get("near_misses")) if isinstance(m, dict)], key=lambda m: -_num(m.get("score")))
    seen, near = set(), []
    for m in nm:
        a = str(m.get("asset") or "?").upper()
        if a in seen or a in alerted_up: continue
        seen.add(a); near.append(m)
    lines = [f"X-SCOUT DAILY {day} JST", ("STATUS: PROBLEMS" if problems else "STATUS: all systems OK")]
    lines += [f"! {p}" for p in problems]
    if hist and len(exp) < RUNS_PER_DAY:
        lines.append(f"(run history started {_jst(first).strftime('%m-%d %H:%M')} JST - partial day)")
    lines.append(f"Runs {len(got & exp) if exp else len(runs)}/{len(exp) or RUNS_PER_DAY} ({full} full analysis) | X posts read {posts} | source items {items}")
    lines.append(f"Leads logged {leads} | alerts sent {len(alerts)}" + (f" ({', '.join(alerts)})" if alerts else ""))
    if near:
        lines.append("Near-misses (not alerted):")
        lines += [f"- {m.get('asset')} {m.get('score')}/10: {str(m.get('why') or '')[:110]}" for m in near[:3]]
    def ret(l): return (l["last"] / l["entry"] - 1) * (-1 if l.get("direction") == "short" else 1)
    o = [l for l in L if l.get("status") == "open" and l.get("entry") and l.get("last")]
    mv = sorted([l for l in o if abs(ret(l)) >= 0.005], key=lambda l: -abs(ret(l)))[:4]
    lines.append("Tracked leads, biggest moves since found: " + ", ".join(f"{l['asset']} {ret(l):+.1%}" for l in mv)
                 if mv else "Tracked leads: no moves above 0.5% yet")
    new_cp = []
    for l in L:
        fs = dt(l.get("first_seen", ""))
        for k, v in (l.get("cp") or {}).items():
            if isinstance(v, dict) and v.get("excess") is not None and k in ("7d", "30d") and fs \
               and (t - fs).total_seconds() / 86400 - _num(v.get("age_d")) < 1:
                new_cp.append(f"{l.get('asset')} {k} {v['excess']:+.1%}")
    if new_cp: lines.append("Results today (vs benchmark): " + ", ".join(new_cp[:6]))
    a7 = [((l.get("cp") or {}).get("7d") or {}).get("excess") for l in L if l.get("alerted")]
    a7 = [x for x in a7 if x is not None]
    lines.append(f"Alert track record: n={len(a7)}, 7d median excess {statistics.median(a7):+.1%}, hit {sum(x > 0 for x in a7)/len(a7):.0%}"
                 if a7 else "Alert track record: no 7-day results yet")
    P = [l for l in L if l.get("plan")]
    if P:
        cl = [l for l in P if (l.get("paper") or {}).get("state") == "closed"]
        lines.append(f"Paper portfolio: {len(cl)} closed, book P/L {sum((l.get('paper') or {}).get('book_pnl_pct', 0) for l in cl):+.2f}%, "
                     f"{sum(1 for l in P if (l.get('paper') or {}).get('state') == 'open')} open | your answers: took "
                     f"{sum(1 for l in L if l.get('decision') == 'took')}, skipped {sum(1 for l in L if l.get('decision') == 'skipped')}")
    hr = s.get("handles_report") or {}
    if hr.get("month") == _jst(t).strftime("%Y-%m") and s.get("handles_reported") != hr.get("id"):
        lines.append(f"Monthly X account review (details in Drive): follow {', '.join('@' + h for h in hr.get('follow', [])) or 'none'}; "
                     f"unfollow {', '.join('@' + h for h in hr.get('unfollow', [])) or 'none'}; noisy {', '.join('@' + h for h in hr.get('noisy', [])[:5]) or 'none'}")
        if send or if_due: s["handles_reported"] = hr.get("id")
    lines.append(f"Ledger {len(L)} leads ({sum(l.get('status') == 'open' for l in L)} open) | playbook {'active' if s.get('playbook') else 'empty (learning)'}")
    text = "\n".join(lines); print(text)
    if send or if_due:
        try:
            r = tg("sendMessage", chat_id=ALERT_CHAT, text=text[:3900], disable_web_page_preview="true")
            if r.get("ok"):
                s["last_digest"] = day; save(s); print("DIGEST: sent")
        except Exception as e:
            print(f"DIGEST: send failed ({str(e)[:120]}) - a later run today will retry")

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "pull": pull(init="--init" in sys.argv); sys.exit(0)
    f = {"push": push, "update": update, "stats": stats, "playbook": playbook, "brief": brief}
    if cmd in f: f[cmd]()
    elif cmd == "add": add(sys.argv[2])
    elif cmd == "set-playbook": set_playbook(sys.argv[2])
    elif cmd == "runlog":
        try: runlog(sys.argv[2])
        except Exception as e: print(f"RUNLOG FAILED (continuing): {str(e)[:120]}")
    elif cmd == "digest":
        try: digest(send="--send" in sys.argv, if_due="--if-due" in sys.argv)
        except Exception as e: print(f"DIGEST FAILED (continuing): {str(e)[:120]}")
    elif cmd == "mark-alerted": mark_alerted(sys.argv[2])
    elif cmd == "send-alert": send_alert(sys.argv[2], sys.argv[3], sys.argv[4] if len(sys.argv) > 4 else "")
    elif cmd == "poll-replies":
        try: poll_replies()
        except Exception as e: print(f"POLL FAILED (continuing): {str(e)[:120]}")
    elif cmd == "paper": paper()
    elif cmd == "handles":
        try: handles("--save" in sys.argv)
        except Exception as e: print(f"HANDLES FAILED (continuing): {str(e)[:120]}")
    elif cmd == "alerted-recent": alerted_recent()
    else: print(__doc__)
