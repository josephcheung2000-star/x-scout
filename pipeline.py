#!/usr/bin/env python3
"""X-scout pipeline on GitHub Actions. All credentialed work happens here, never in the Claude run.

Every 10 minutes the workflow runs `python3 pipeline.py tick`, which does two things:
  PREP  (once per slot, 3-45 min before each Claude run at HH:45 JST, HH = 2,5,...,23):
        ledger pull -> poll-replies -> update prices/paper -> [08:45 slot: miss log] -> X posts (from the Mac feeder via
        Drive) -> sources + sources2 (Korea/Coinbase listings, unlocks, governance, ETF flows, insider buys, US stock
        movers, commodities, regime, Polymarket) -> triage (+ new second-tier triggers) -> write Google Doc "XS-IN <slot>" (one or more parts) into the X-scout Drive folder -> ledger push
  APPLY (whenever Claude has left a "XS-OUT <slot>" JSON file in the folder):
        ledger pull -> add leads -> send alerts (with Took/Skipped buttons) -> playbook -> runlog
        -> digest if due -> ledger push -> rename the file "DONE XS-OUT ..."
Env (repo secrets): X_AUTH_TOKEN, X_CT0, TG_TOKEN, TG_STORE_CHAT, TG_ALERT_CHAT, GWS_DRIVE (authorized_user JSON).
The repo is public, so this script prints only counts and status words - never post text, leads or tokens."""
import json, os, re, subprocess, sys, time, datetime, urllib.request, urllib.error, urllib.parse
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload

FOLDER = os.environ.get("XS_FOLDER", "11yC1KUtWZTUYoZIEuU_AoT8bFhomq6qo")   # Drive "X-scout" (info@)
JST = datetime.timezone(datetime.timedelta(hours=9))
SLOT_HOURS = [2, 5, 8, 11, 14, 17, 20, 23]            # Claude runs at HH:45 JST
PREP_WINDOW = (3, 45)                                  # prep between 45 and 3 minutes before the slot
PART_CHARS = 4000                                      # UTF-8 bytes per XS-IN part. The Claude run's Drive reader cut parts
                                                       # of ~6-7 KB (2026-10-06); 4 KB leaves room for its export overhead
HEALTH = "health.json"                                 # committed by the workflow: once-per-day notice memory
ERRORS = []
SLOT_RX = re.compile(r"\d{4}-\d{2}-\d{2} \d{4}")
FLAG_FORCE_CAP = 5                                     # exchange-flag triggers that may force a full run, per prep
TRIG_TTL_DAYS = 7                                      # a second-tier trigger key re-fires only after this many days
S2_PER_KEY = 10                                        # second-tier items shown per source in the input doc
S2_KEYS = ["coinbase_new", "upbit_krw_new", "bithumb_krw_new", "bithumb_notices", "bithumb_wallet", "upbit_notices",
           "unlocks_14d", "governance", "etf_flows", "insider_buys", "stock_movers", "commodities", "polymarket_extra"]
S2_DROP = {"src", "id", "at", "url", "title", "rule", "recipients", "description"}
# Trigger-relevant second-tier items are never hidden behind "(+N more not shown)": every exchange flag / notice /
# wallet change, every unlock of >= 2% of unlocked supply, every >= $1M or cluster insider buy. A hard cap keeps a
# runaway feed from flooding the doc (each 420-byte line costs ~1/10 of a 4 KB part; more parts are fine).
S2_ALL_KEYS = {"upbit_krw_new", "bithumb_krw_new", "bithumb_notices", "bithumb_wallet", "upbit_notices", "commodities"}
S2_PRIORITY = {"unlocks_14d": lambda it: (it.get("pct_supply") or 0) >= 2,
               "insider_buys": lambda it: (it.get("value") or 0) >= 1e6 or (it.get("insiders") or 1) >= 3}
S2_HARD_CAP = 60


def jst_now():
    return datetime.datetime.now(JST)


def next_slot(now):
    for d in (0, 1):
        day = now.date() + datetime.timedelta(days=d)
        for h in SLOT_HOURS:
            s = datetime.datetime(day.year, day.month, day.day, h, 45, tzinfo=JST)
            if s >= now:
                return s


def slot_name(s):
    return s.strftime("%Y-%m-%d %H%M")


def drive():
    info = json.loads(os.environ["GWS_DRIVE"])
    return build("drive", "v3", credentials=Credentials.from_authorized_user_info(info), cache_discovery=False)


RATE_403 = re.compile(r"(?i)rateLimitExceeded|userRateLimitExceeded|sharingRateLimitExceeded|quotaExceeded")


def _retryable(e):
    """Rate limits, 5xx and network errors are retried; a 403 only when it is a rate limit (a permission 403 is not)."""
    code = getattr(getattr(e, "resp", None), "status", None)
    if code is None:
        return True                                    # network / timeout
    code = int(code)
    if code == 403:
        return bool(RATE_403.search(str(getattr(e, "content", b"") or b"") + " " + str(e)))
    return code in (429, 500, 502, 503, 504)


def _x(req, tries=4, before_retry=None):
    """execute() a Drive request, retrying rate limits / 5xx / network errors: an uncaught Drive error fails the
    whole job (and, mid-prep, leaves a partial XS-IN with the ledger update never pushed). A non-idempotent request
    passes `before_retry`: called after a failure, it returns the result if the first attempt did land after all."""
    for i in range(tries):
        try:
            return req.execute()
        except Exception as e:
            if i == tries - 1 or not _retryable(e):
                raise
            time.sleep(5 * 2 ** i)
            if before_retry:
                done = before_retry()
                if done is not None:
                    return done


def list_files(d, contains, extra="", parent=None):
    q = f"'{parent or FOLDER}' in parents and name contains '{contains}' and trashed = false {extra}"
    out, tok = [], None
    while True:
        r = _x(d.files().list(q=q, orderBy="createdTime", pageSize=100, pageToken=tok,
                              fields="nextPageToken, files(id,name,mimeType,createdTime)"))
        out += r.get("files", [])
        tok = r.get("nextPageToken")
        if not tok:
            return out


def read_file(d, f):
    if f["mimeType"] == "application/vnd.google-apps.document":
        b = d.files().export(fileId=f["id"], mimeType="text/plain").execute()
    else:
        b = d.files().get_media(fileId=f["id"]).execute()
    return b.decode("utf-8-sig")


def create_doc(d, title, text):
    body = {"name": title, "parents": [FOLDER], "mimeType": "application/vnd.google-apps.document"}
    def landed():                                      # an ambiguous failure may have created the file: never twice
        try:
            hit = [f for f in list_files(d, title) if f.get("name") == title]
        except Exception:
            return None
        return {"id": hit[-1]["id"]} if hit else None
    return _x(d.files().create(body=body, media_body=MediaInMemoryUpload(text.encode("utf-8"), mimetype="text/plain"),
                               fields="id"), before_retry=landed)["id"]


def rename(d, f, new):
    _x(d.files().update(fileId=f["id"], body={"name": new}))


def sh(args, timeout=900, env_extra=None):
    """Run a script; return (rc, stdout). Output is NOT printed (public logs)."""
    env = dict(os.environ, **(env_extra or {}))
    try:
        p = subprocess.run(["python3"] + args, capture_output=True, text=True, timeout=timeout, env=env)
        return p.returncode, (p.stdout or "") + (("\n" + p.stderr[-800:]) if p.returncode else "")
    except subprocess.TimeoutExpired:
        return 124, "timeout"


def err(msg):
    msg = re.sub(r"bot\d+:[A-Za-z0-9_-]{20,}", "bot<redacted>", msg)
    ERRORS.append(msg[:200])
    print("ERROR:", msg.splitlines()[0][:120] if msg else "?")


def load_json(p, default):
    try:
        return json.load(open(p))
    except Exception:
        return default


def health_once(key):
    """True the first time `key` is seen today (JST); records it in health.json."""
    h = load_json(HEALTH, {})
    today = jst_now().strftime("%Y-%m-%d")
    if h.get(key) == today:
        return False
    h[key] = today
    json.dump(h, open(HEALTH, "w"), indent=1)
    return True


def notify(kind, reason=""):
    rc, _ = sh(["ledger.py", "notify", kind, reason], timeout=60)
    print(f"notify {kind}: rc={rc}")


def one_line(t, n):
    return re.sub(r"\s+", " ", str(t or "")).strip()[:n]


# ---------------------------------------------------------------- PREP
TRIG_BOOK = ("trig_seen", "trig_slot", "recent_triggers", "recent_trigger_slot")   # trigger bookkeeping in state.json


def prep(d, slot):
    name = "XS-IN " + slot_name(slot)
    forced = bool(os.environ.get("XS_FORCE_PREP"))
    rc, out = sh(["ledger.py", "pull"], timeout=120)
    if rc != 0:
        time.sleep(30)
        rc, out = sh(["ledger.py", "pull"], timeout=120)
    if rc != 0:
        err("ledger pull failed")
        if health_once("pull_alert"):
            notify("pull")
        return False
    # ONE BUILD PER SLOT. The ledger (strongly consistent, unlike Drive's name search) records every slot whose XS-IN
    # was written. A second build for a slot found its triggers already used, went LIGHT, and - being the newest file -
    # replaced the full build in the Claude run (audit 2026-09-28..10-07: 29 of 67 slots built 2-3 times).
    st0 = load_json("state.json", {}) or {}
    prev = (st0.get("prepped_slots") or {}).get(slot_name(slot)) if isinstance(st0.get("prepped_slots"), dict) else None
    if prev and not forced:
        print(f"prep: {name} already built ({prev.get('mode') if isinstance(prev, dict) else '?'}) - skipped")
        return True
    book0 = {k: st0.get(k) for k in TRIG_BOOK}
    for cmd in (["poll-replies"], ["update"]):
        rc, out = sh(["ledger.py"] + cmd, timeout=600)
        if rc != 0:
            err(f"ledger {cmd[0]} failed")
    miss_summary = None
    if slot.hour == 8:                                         # once a day: which big moves did the scout miss?
        rc, out = sh(["misses.py"], timeout=240)
        miss_summary = "\n".join(l for l in out.splitlines() if l.strip() and not l.startswith("MISSES_JSON")).strip()
        mj = [l for l in out.splitlines() if l.startswith("MISSES_JSON")]
        print("misses:", "ok" if rc == 0 and mj and '"error"' not in mj[-1] else f"problem rc={rc}")

    # X posts come from the Mac feeder (X shows cloud/datacenter IPs a bot wall): use every unused
    # XS-POSTS file from the last 3.5 h, newest status wins, posts merged and de-duplicated.
    for f in ("posts.json", "sources.json", "sources2.json", "triage.json"):
        if os.path.exists(f):
            os.remove(f)
    x = {"ok": False, "errors": ["no X posts from the Mac feeder in the last 3.5 h (Mac asleep or offline?)"]}
    cutoff = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=3.5)).strftime("%Y-%m-%dT%H:%M:%S")
    files = [f for f in list_files(d, "XS-POSTS", f"and createdTime > '{cutoff}'")
             if f["name"].startswith("XS-POSTS") or (prev and f["name"].startswith("USED XS-POSTS"))]   # a rebuild re-reads them
    posts_all, seen_ids = [], set()
    for f in reversed(files):                                  # newest first
        try:
            data = json.loads(read_file(d, f))
        except Exception:
            err("unreadable XS-POSTS file")
            continue
        st = data.get("status") if isinstance(data.get("status"), dict) else {}
        if f is files[-1] or (st.get("ok") and not x.get("ok")):
            x = st or x
        for p in data.get("posts") or []:
            if isinstance(p, dict) and p.get("id") and p["id"] not in seen_ids:
                seen_ids.add(p["id"]); posts_all.append(p)
    if files:
        x = dict(x, kept=len(posts_all), files=len(files))
    json.dump({"status": x, "posts": posts_all}, open("posts.json", "w"), ensure_ascii=False)
    h = load_json(HEALTH, {})
    if x.get("ok"):
        h["last_x_ok"] = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="minutes")
        json.dump(h, open(HEALTH, "w"), indent=1)
    else:
        err("no X posts this slot" if not files else "Mac X fetch failed")
        last = h.get("last_x_ok")
        stale = (not last) or (datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromisoformat(last)).total_seconds() > 12 * 3600
        if files and any(k in json.dumps(x).lower() for k in ("401", "403", "auth", "cookie", "login")):
            if health_once("fetch_alert"):
                notify("fetch", "the Mac fetcher could not log in")
        elif stale and health_once("x_missing"):
            notify("other", "no X posts from the Mac feeder for 12+ hours - is the Mac asleep or offline")
    print(f"x: ok={x.get('ok')} files={len(files)} kept={len(posts_all)}")

    # source window = time since the last prep + 30 min (preps drift between 3 and ~70 min before a slot, so a fixed
    # 3.5 h window can leave gaps of up to an hour that no run ever sees); triage de-duplicates repeats by key
    win = 3.5
    try:
        lp = (load_json("state.json", {}) or {}).get("last_prep_at")
        if lp:
            win = min(12.0, max(3.5, (datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromisoformat(lp)).total_seconds() / 3600 + 0.5))
    except Exception:
        pass
    rc, out = sh(["sources.py", f"{win:.2f}"], timeout=300)
    src = load_json("sources.json", {})
    counts = {k: len(v) for k, v in src.items() if isinstance(v, list)}
    src_errs = [f"{it.get('src')}: {one_line(it.get('error'), 80)}" for v in src.values() if isinstance(v, list)
                for it in v if isinstance(it, dict) and "error" in it]
    counts = {k: sum(1 for it in src.get(k, []) if isinstance(it, dict) and "error" not in it) for k in counts}
    rc, out = sh(["sources2.py", f"{win:.2f}"], timeout=240)
    s2 = load_json("sources2.json", {})
    if not isinstance(s2, dict) or "counts" not in s2:
        s2 = {}
        err("second-tier sources (sources2) produced no output")
    for k, v in (s2.get("counts") or {}).items():
        counts[k] = v
    src_errs += [f"s2 {one_line(e, 90)}" for e in (s2.get("errors") or [])][:8]
    rc, out = sh(["triage.py"], timeout=300, env_extra={"XS_SLOT_HOUR": str(slot.hour),      # sweep = the 08:45 slot, not the wall clock
                                                        "XS_SLOT": slot_name(slot)})          # a key fired for THIS slot stays fresh
    tri = load_json("triage.json", {"mode": "full", "triggers": [], "focus_ids": [], "read_all_posts": True})
    # second-tier triggers: each event key counts once per TRIG_TTL_DAYS; any new one makes this a full run
    st = load_json("state.json", None)
    new_trig = []
    if isinstance(st, dict):
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        cut = (now_utc - datetime.timedelta(days=TRIG_TTL_DAYS)).isoformat(timespec="minutes")
        seen = st.get("trig_seen") if isinstance(st.get("trig_seen"), dict) else {}
        seen = {k: v for k, v in seen.items() if str(v) >= cut}
        tslot = st.get("trig_slot") if isinstance(st.get("trig_slot"), dict) else {}
        tslot = {k: v for k, v in tslot.items() if k in seen}
        for t in s2.get("triggers") or []:
            # a key first seen for THIS slot is still new to it (a rebuild must not lose the triggers of the first build)
            if isinstance(t, dict) and t.get("key") and (t["key"] not in seen or tslot.get(t["key"]) == slot_name(slot)):
                new_trig.append(t)
                if t["key"] not in seen:
                    seen[t["key"]] = now_utc.isoformat(timespec="minutes"); tslot[t["key"]] = slot_name(slot)
        st["trig_seen"] = seen
        st["trig_slot"] = tslot
        json.dump(st, open("state.json", "w"), ensure_ascii=False, indent=1)
    # Korean exchange-flag triggers: only warning_on / delisting / non-routine wallet_suspend may force a full run, and at
    # most FLAG_FORCE_CAP of them per prep; the others are listed in the input without forcing (they are noisy: Upbit
    # cautions flip both ways, Bithumb wallets pause for routine network upgrades)
    nforce, forcing = 0, False
    for t in new_trig:
        if t.get("type") == "exchange_flag":
            if t.get("force") and nforce < FLAG_FORCE_CAP:
                nforce += 1; forcing = True
            elif t.get("force"):
                t["force"], t["capped"] = False, True
        else:
            forcing = True
    if new_trig:
        tri["triggers"] = list(tri.get("triggers") or []) + new_trig
        if forcing and tri.get("mode") != "full":
            tri["mode"], tri["read_all_posts"] = "full", True
    if isinstance(prev, dict) and prev.get("mode") == "full" and tri.get("mode") != "full":
        tri["mode"], tri["read_all_posts"] = "full", True     # a forced rebuild never downgrades the first build
    print(f"sources: {len(counts)} feeds, errors={len(src_errs)} | triage mode={tri.get('mode')} triggers={len(tri.get('triggers', []))} (new second-tier {len(new_trig)})")

    sweep = slot.hour == 8
    extra = {}
    for key, cmd in (("LEDGER BRIEF", ["brief"]), ("PLAYBOOK", ["playbook"]), ("ALERTED IN LAST 14 DAYS", ["alerted-recent"]),
                     ("HANDLE TRACK RECORDS (use the weight in scoring)", ["handle-scores"])) + \
            ((("LEDGER STATS", ["stats"]), ("PAPER PORTFOLIO", ["paper"]), ("SHADOW TEST SCORECARD (pre-registered, 2026-09-27 to 2026-11-27)", ["shadow"])) if sweep else ()) + \
            ((("MONTHLY ACCOUNT REVIEW (saved for the digest)", ["handles", "--save"]),) if sweep and slot.day == 1 else ()):
        rc, out = sh(["ledger.py"] + cmd, timeout=300)
        extra[key] = out.strip() if rc == 0 else f"(failed: exit {rc})"
    extra["BACKTESTED PRIORS (v2; base rates per catalyst type - they supersede PLAYBOOK PRIORS where both cover a type)"] = priors_text()
    try:
        fs = json.load(open("family_stats.json"))
        extra["STRATEGY FAMILY STATS (research 2026-09-28; may_alert=false families never alert; context for p_win, not a substitute)"] = "\n".join(
            [fs.get("_about", "")] + [f"{k}: " + " | ".join(f"{a}: {b}" for a, b in v.items()) for k, v in fs.items() if isinstance(v, dict)])
    except Exception:
        extra["STRATEGY FAMILY STATS"] = "(family_stats.json unreadable)"
    reg = [r for r in (s2.get("regime") or []) if isinstance(r, dict) and "error" not in r]
    extra["MARKET REGIME"] = "\n".join(one_line(json.dumps({k: v for k, v in r.items() if k not in ("src", "id", "url")}, ensure_ascii=False), 500)
                                       for r in reg) or "(unavailable this run)"
    if miss_summary is not None:
        extra["MISS LOG (big moves of the last day and whether a lead flagged them)"] = miss_summary or "(miss log failed)"

    # pending prep record, merged into the run log when Claude's output is applied
    s = load_json("state.json", None)
    if isinstance(s, dict):
        pend = s.get("pending_prep") if isinstance(s.get("pending_prep"), dict) else {}
        pend[slot_name(slot)] = {"x": {k: x.get(k) for k in ("ok", "captured", "relevant", "kept")},
                                 "sources": counts, "source_errors": src_errs, "mode": tri.get("mode"),
                                 "prep_errors": list(ERRORS)}
        s["pending_prep"] = dict(sorted(pend.items())[-12:])
        s["last_prep_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="minutes")
        json.dump(s, open("state.json", "w"), ensure_ascii=False, indent=1)

    # ------------- build the input document
    posts = load_json("posts.json", {}).get("posts") or []
    full = tri.get("mode") == "full"
    if not full:
        chosen = []
    elif tri.get("read_all_posts") or not tri.get("focus_ids"):
        chosen = posts
    else:
        ids = set(tri.get("focus_ids"))
        chosen = [p for p in posts if p.get("id") in ids]
    L = [f"X-SCOUT INPUT {name}  (built {datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC by GitHub Actions)",
         f"MODE: {tri.get('mode')}{' (08:00 sweep: read every post)' if tri.get('sweep') else ''}",
         f"X FETCH: {json.dumps({k: x.get(k) for k in ('ok', 'captured', 'relevant', 'kept', 'errors')}, ensure_ascii=False)}",
         f"SOURCES: {json.dumps(counts)}" + (f" errors: {'; '.join(src_errs)}" if src_errs else ""),
         f"PREP ERRORS: {'; '.join(ERRORS) if ERRORS else 'none'}", ""]
    for k, v in extra.items():
        L += [f"=== {k} ===", v, ""]
    L += ["=== TRIAGE TRIGGERS ==="] + [json.dumps(t, ensure_ascii=False) for t in tri.get("triggers", [])] + [""]
    if full:
        L += ["=== EDGAR FILINGS TO REVIEW ==="] + [json.dumps(t, ensure_ascii=False) for t in tri.get("edgar_review", [])] + [""]
        L += ["=== ENTITY COUNTS (accounts naming each asset) ===", json.dumps(tri.get("entity_counts", {}), ensure_ascii=False), ""]
        L += [f"=== PRIMARY SOURCES (last {win:.1f}h) ==="]
        for k in ("binance", "okx", "polymarket", "hyperliquid"):
            for it in src.get(k, []):
                if isinstance(it, dict) and "error" not in it:
                    L.append(f"{k}: " + json.dumps(it, ensure_ascii=False))
        L += ["", "=== SECOND-TIER SOURCES (Korea/Coinbase listings, unlocks, governance, ETF flows, insider buys, US stock movers, commodities, Polymarket) ==="]
        L += s2_lines(s2)
        L += ["", f"=== X POSTS TO READ ({len(chosen)} of {len(posts)} relevant posts; id | @handle (followers) | created UTC | likes/rts/replies/views | tab | text | url) ==="]
        for p in chosen:
            m = p.get("m") or {}
            nested = " [inside a retweet/quote]" if p.get("nested") else ""
            L.append(f"{p.get('id')} | @{p.get('handle')} ({p.get('followers')}) | {str(p.get('created'))[:16]} | "
                     f"{m.get('likes')}/{m.get('rts')}/{m.get('replies')}/{m.get('views')} | {p.get('tab')}{nested} | "
                     f"{one_line(p.get('text'), 700)} | {p.get('url')}")
    L += ["", "END OF INPUT"]
    text = "\n".join(L)
    parts = split_parts(L)
    if not forced and slot_built(list_files(d, name), name):
        # another build of this slot landed while this one ran (or before the ledger knew of it): keep the first.
        # Undo this build's trigger bookkeeping so triggers it alone saw stay new for the next slot; keep the rest
        # (prices, replies) and push it.
        s = load_json("state.json", None)
        if isinstance(s, dict):
            for k, v in book0.items():
                if v is None: s.pop(k, None)
                else: s[k] = v
            json.dump(s, open("state.json", "w"), ensure_ascii=False, indent=1)
        print(f"prep: {name} appeared during this build - not written again")
        parts = []
    if parts:
        write_parts(d, name, parts)
    if parts:
        print(f"input doc: {name}, {len(chosen)} posts, {len(text)} chars, {len(parts)} part(s)")
        for f in files:                                        # consumed only once the input doc exists
            if not f["name"].startswith("USED "):
                rename(d, f, "USED " + f["name"])
        s = load_json("state.json", None)
        if isinstance(s, dict):
            ps = s.get("prepped_slots") if isinstance(s.get("prepped_slots"), dict) else {}
            ps[slot_name(slot)] = {"mode": tri.get("mode"), "posts": len(chosen), "parts": len(parts),
                                   "at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="minutes"),
                                   "builds": int((prev or {}).get("builds", 1) if isinstance(prev, dict) else 0) + 1}
            s["prepped_slots"] = dict(sorted(ps.items())[-24:])
            json.dump(s, open("state.json", "w"), ensure_ascii=False, indent=1)

    rc, out = sh(["ledger.py", "push"], timeout=120)
    if rc != 0:
        time.sleep(30)
        rc, out = sh(["ledger.py", "push"], timeout=120)
    if rc != 0:
        err("ledger push after prep failed")
        return False
    return True


def s2_lines(s2):
    """Input-doc lines for the second-tier sources: trigger-relevant items in full (S2_ALL_KEYS / S2_PRIORITY, up to
    S2_HARD_CAP), the rest up to S2_PER_KEY per source, and a named list of whatever is left out."""
    L = []
    for k in S2_KEYS:
        items = [it for it in (s2.get(k) or []) if isinstance(it, dict) and "error" not in it]
        if k in S2_ALL_KEYS:
            shown = items[:S2_HARD_CAP]
        else:
            pri = S2_PRIORITY.get(k)
            top = [it for it in items if pri and _safe(pri, it)][:S2_HARD_CAP]
            shown = top + [it for it in items if not any(it is x for x in top)][:S2_PER_KEY]
        for it in shown:
            rest = {a: b for a, b in it.items() if a not in S2_DROP and b not in (None, "", [], {})}
            rest = dict(sorted(rest.items(), key=lambda kv: isinstance(kv[1], (list, dict))))   # scalars first: the line is clipped
            L.append(one_line(f"{k}: {it.get('title')} | {it.get('url')} | {json.dumps(rest, ensure_ascii=False, default=str)}", 420))
        hidden = [it for it in items if not any(it is x for x in shown)]
        if hidden:
            names = ", ".join(one_line(it.get("name") or it.get("ticker") or it.get("asset") or it.get("title"), 40) for it in hidden)
            L.append(one_line(f"{k}: (+{len(hidden)} more not shown: {names})", 420))
    return L


def _safe(f, x):
    try:
        return bool(f(x))
    except Exception:
        return False


def slot_built(files, name):
    """True when `files` hold a COMPLETE input for `name`: the single doc, or parts 1..n of some "part k of n" set.
    An incomplete set (a prep that failed mid-way) is not a build, so a later tick rebuilds the slot."""
    names = {f.get("name") for f in files}
    if name in names:
        return True
    sets = {}
    for nm in names:
        m = re.fullmatch(re.escape(name) + r" part (\d+) of (\d+)", str(nm))
        if m:
            sets.setdefault(int(m.group(2)), set()).add(int(m.group(1)))
    return any(got >= set(range(1, n + 1)) for n, got in sets.items())


BUILD_PREFIX = "XS-BUILDING "                          # parts are uploaded under this name (it does not contain "XS-IN")


def write_parts(d, name, parts):
    """Upload every part under a temporary name, then rename them all to the final "XS-IN ..." names, so a failure
    mid-upload never leaves a partial XS-IN set. Earlier (incomplete, or force-replaced) parts of the slot and leftover
    temporary parts are moved out of the way first (renamed SUPERSEDED, trashed for temporaries)."""
    tmp_name = BUILD_PREFIX + name[len("XS-IN "):]
    for f in list_files(d, tmp_name):
        _x(d.files().update(fileId=f["id"], body={"trashed": True}))
    for f in list_files(d, name):
        if f["name"].startswith(name):
            rename(d, f, "SUPERSEDED " + f["name"])
    made = []
    for i, part in enumerate(parts):
        suffix = "" if len(parts) == 1 else f" part {i + 1} of {len(parts)}"
        fid = create_doc(d, tmp_name + suffix, part + f"--- END OF PART {i + 1} OF {len(parts)} ---\n")
        made.append(({"id": fid, "name": tmp_name + suffix}, name + suffix))
    for f, final in made:
        rename(d, f, final)


def split_parts(lines, limit=None):
    """Split the input lines into parts of at most `limit` UTF-8 bytes (+ the end marker), on line boundaries."""
    limit = limit or PART_CHARS
    parts, cur = [], ""
    blen = lambda s: len(s.encode("utf-8"))                   # the reader truncates by size, and CJK text is 3 bytes/char
    for line in lines:                                         # split on line boundaries, never mid-post
        while blen(line) > limit - 200:
            line = line[:-50]
        if cur and blen(cur) + blen(line) + 1 > limit:
            parts.append(cur); cur = ""
        cur += line + "\n"
    parts.append(cur)
    return parts


def priors_text():
    try:
        T = json.load(open("priors.json")).get("types") or {}
    except Exception:
        return "(priors.json unreadable)"
    out = []
    for k, v in T.items():
        if not isinstance(v, dict):
            continue
        ci = v.get("ci95") or [None, None]
        med = v.get("median_excess_7d", v.get("median"))
        pts = "prob" in str(v.get("units", "")).lower() or k.startswith("polymarket")
        f = (lambda x: f"{float(x) * 100:+.1f} pts") if pts else (lambda x: f"{float(x):+.1%}")
        adj = v.get("score_adj")
        try:
            out.append(f"{k}: n={v.get('n')} {v.get('horizon', '7d')} median {'move' if pts else 'excess'} {f(med)}, share of longs that won {float(v.get('hit_rate') or 0):.0%}, "
                       f"CI95 [{f(ci[0])}, {f(ci[1])}], score adj {('%+d' % adj) if isinstance(adj, (int, float)) else 'n/a (see PLAYBOOK)'}{' (weak)' if v.get('weak') else ''} - {one_line(v.get('note'), 140)}")
        except Exception:
            out.append(f"{k}: {one_line(json.dumps(v), 200)}")
    return "\n".join(out) or "(no priors)"


# ---------------------------------------------------------------- ALERT GUARDS (research rounds 2026-09-28, audited)
# Families that lost money in the backtest never alert (they stay leads): see family_stats.json.
NEG_FAMILY_TAGS = {"unlock_short", "unlock_short_7d", "insider_long", "funding_fade", "macro_fade"}
NEG_PAIRS = {("unlock", "short"), ("insider", "long"), ("funding", "short"), ("macro", "short")}
COOLDOWN_DAYS = 14
_HL = None

def _dir(x):
    x = str(x or "").strip().lower()
    return {"sell": "short", "buy": "long"}.get(x, x)

def perp_venues(sym):
    """(venues found, venues that could not be checked). Binance/Bybit often refuse US cloud IPs: an error counts as unknown."""
    global _HL
    sym = re.sub(r"[^A-Za-z0-9]", "", str(sym)).upper()
    found, unknown = [], []
    if not sym: return found, ["all"]
    class NotListed(Exception):
        pass
    def get(u):
        try:
            return json.loads(urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"}), timeout=15).read())
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):          # the venue answered: no such symbol
                raise NotListed()
            raise                             # 403/451/5xx: venue unreachable from here = unknown
    for name, fmt, ok in (("binance", "https://fapi.binance.com/fapi/v1/exchangeInfo?symbol={s}USDT",
                           lambda d, s: any(x.get("symbol") == s + "USDT" and x.get("status") == "TRADING" for x in d.get("symbols", []))),
                          ("bybit", "https://api.bybit.com/v5/market/instruments-info?category=linear&symbol={s}USDT",
                           lambda d, s: any(x.get("symbol") == s + "USDT" and x.get("status") == "Trading" for x in (d.get("result") or {}).get("list", [])))):
        hit, err_ = False, False
        for pre in ("", "1000", "10000", "1M"):
            try:
                if ok(get(fmt.format(s=pre + sym)), pre + sym): hit = True; break
            except NotListed:
                continue
            except Exception:
                err_ = True; break
        if hit: found.append(name)
        elif err_: unknown.append(name)
    try:
        d = get(f"https://www.okx.com/api/v5/public/instruments?instType=SWAP&instFamily={sym}-USDT")
        if any(x.get("instFamily") == sym + "-USDT" and x.get("state") == "live" for x in d.get("data", [])): found.append("okx")
    except NotListed:
        pass
    except Exception:
        unknown.append("okx")
    try:
        if _HL is None:
            req = urllib.request.Request("https://api.hyperliquid.xyz/info", data=b'{"type":"meta"}', headers={"Content-Type": "application/json"})
            _HL = {u["name"] for u in json.loads(urllib.request.urlopen(req, timeout=15).read())["universe"] if not u.get("isDelisted")}
        if sym in _HL or "k" + sym in _HL: found.append("hyperliquid")
    except Exception:
        unknown.append("hyperliquid")
    return found, unknown

# Polymarket backstop for the prompt rules (audit 2026-10-07): entries after a >= 8-pt 24h fall of the side bought went
# 0/11; a target or stop the share price cannot reach (outside [0.01, 0.99]) never resolves the race. Only these
# machine-checkable parts are enforced; "mid-priced bet anchored on an outside forecast" stays a prompt rule.
PM_FALL_PTS = 0.08
PM_LO, PM_HI = 0.01, 0.99
PM_UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/133 Safari/537.36"}


def _getj(url):
    return json.loads(urllib.request.urlopen(urllib.request.Request(url, headers=PM_UA), timeout=15).read())


def pm_snapshot(slug, outcome):
    """(current share price, share price ~24h ago) of a Polymarket outcome: gamma outcomePrices + clob prices-history
    (the same endpoints ledger.py prices and simulates with). Raises when unavailable."""
    d = _getj("https://gamma-api.polymarket.com/markets?slug=" + urllib.parse.quote(str(slug)))
    m = d[0] if isinstance(d, list) and d else None
    if not m: raise ValueError("market not found")
    oc = [str(x).strip().lower() for x in json.loads(m.get("outcomes") or "[]")]
    i = oc.index(str(outcome or "Yes").strip().lower())
    now_p = float(json.loads(m.get("outcomePrices") or "[]")[i])
    tok = json.loads(m.get("clobTokenIds") or "[]")[i]
    t1 = int(time.time()); t0 = t1 - 26 * 3600
    h = _getj(f"https://clob.polymarket.com/prices-history?market={tok}&startTs={t0}&endTs={t1}&fidelity=60").get("history") or []
    pts = sorted((float(x["t"]), float(x["p"])) for x in h if isinstance(x, dict) and "t" in x and "p" in x)
    before = [p for t, p in pts if t <= t1 - 24 * 3600]
    p24 = before[-1] if before else (pts[0][1] if pts and pts[0][0] <= t1 - 20 * 3600 else None)
    return now_p, p24


def _f(x):
    try:
        v = float(x); return v if v == v else None
    except Exception:
        return None


def pm_block(lead, plan, d):
    """Reason string if a Polymarket alert must be blocked, else None. Any data problem -> None (never blocks)."""
    try:
        now_p, p24 = pm_snapshot(lead.get("market_slug"), lead.get("outcome"))
    except Exception:
        now_p, p24 = None, None
    sign = -1 if d == "short" else 1
    if now_p is not None and p24 is not None and sign * (now_p - p24) <= -PM_FALL_PTS:
        return f"Polymarket share moved {abs(now_p - p24) * 100:.0f} pts against the trade in 24h ({p24:.2f} -> {now_p:.2f})"
    e = now_p
    if e is None and plan:
        lo, hi = _f(plan.get("entry_lo")), _f(plan.get("entry_hi"))
        e = (lo + hi) / 2 if lo and hi else None
    lv = {"target": _f(lead.get("target_price")), "stop": _f(lead.get("stop_price"))}
    if plan:
        tps = [_f(t.get("price")) for t in plan.get("tps") or [] if isinstance(t, dict)]
        if lv["target"] is None and tps and None not in tps: lv["target"] = max(tps) if sign > 0 else min(tps)
        if lv["stop"] is None: lv["stop"] = _f(plan.get("stop"))
    if e and 0 < e < 1:
        t, st = _f(lead.get("target_pct")), _f(lead.get("stop_pct"))
        if lv["target"] is None and t: lv["target"] = e * (1 + sign * t / 100)
        if lv["stop"] is None and st: lv["stop"] = e * (1 - sign * st / 100)
    for k, v in lv.items():                            # a level AT resolution (1.0 / 0.0) means "hold to resolution":
        if v is not None and abs(v - 1.0) < 1e-9: lv[k] = PM_HI     # clamp it to the reachable 0.99 / 0.01
        elif v is not None and abs(v) < 1e-9: lv[k] = PM_LO
    bad = [f"{k} {v:.3f}" for k, v in lv.items() if v is not None and not (PM_LO <= v <= PM_HI)]
    if bad:
        return f"Polymarket {' and '.join(bad)} outside the reachable share range [{PM_LO}, {PM_HI}]"
    return None


def alert_block_reason(a, leads, recent, sent_now):
    """None if the alert may go out, else a short reason. Guard errors never block (the alert goes out as before)."""
    try:
        asset = str(a.get("asset") or "").strip().upper()
        lead = next((l for l in leads if str(l.get("asset", "")).strip().upper() == asset), {})
        plan = a.get("plan") if isinstance(a.get("plan"), dict) else {}
        d = _dir(plan.get("direction") or lead.get("direction"))
        fam = str(lead.get("family") or "").strip().lower()
        ct = str(lead.get("catalyst_type") or "").strip().lower()
        if fam in NEG_FAMILY_TAGS or (ct, d) in NEG_PAIRS:
            return "family", f"{fam or ct} {d} lost money in the backtest"
        if asset in recent or asset in sent_now:
            return "cooldown", f"already alerted within {COOLDOWN_DAYS} days"
        if str(lead.get("kind") or "").lower() == "polymarket":
            why = pm_block(lead, plan, d)
            if why:
                return "polymarket", why
        if str(lead.get("kind") or "").lower() == "crypto" and d == "short":
            found, unknown = perp_venues(asset)
            if not found and not unknown:
                return "venue", "no perp venue on Binance, Bybit, OKX or Hyperliquid to short it"
        return None
    except Exception as e:
        return None

# ---------------------------------------------------------------- APPLY
def apply(d):
    outs = [f for f in list_files(d, "XS-OUT") if f["name"].startswith("XS-OUT")]
    if not outs:
        print("apply: nothing to do")
        return
    rc, out = sh(["ledger.py", "pull"], timeout=120)
    if rc != 0:
        err("ledger pull failed (apply)")
        if health_once("pull_alert"):
            notify("pull")
        return
    finished, digest_due = [], False
    for f in outs:
        try:
            o = json.loads(read_file(d, f))
            assert isinstance(o, dict)
            m = SLOT_RX.search(str(o.get("slot") or "")) or SLOT_RX.search(f["name"])
            slot = m.group(0)
        except Exception:
            err("unreadable output file")
            rename(d, f, "BAD " + f["name"])
            if health_once("bad_out"):
                notify("other", "the Claude run wrote an output file the pipeline could not read")
            continue
        s = load_json("state.json", {})
        if slot in (s.get("applied_slots") or []):
            print("duplicate output skipped")
            finished.append((f, "DUPLICATE"))
            continue
        run = o.get("run") if isinstance(o.get("run"), dict) else {}
        run_errors = [str(e) for e in (run.get("errors") or [])]
        leads = [l for l in (o.get("leads") or []) if isinstance(l, dict) and l.get("asset")]
        if not isinstance(run.get("near_misses"), list):
            run["near_misses"] = []
        recent = None
        rc, out = sh(["ledger.py", "alerted-recent"], timeout=60)       # before `add`, so this slot's own leads are not in it
        try:
            recent = {str(x.get("asset", "")).strip().upper() for x in json.loads(out.strip().splitlines()[-1])}
        except Exception:
            run_errors.append("cooldown check unavailable (alerted-recent unreadable)")
            recent = set()
        if leads:
            json.dump(leads, open("leads.json", "w"), ensure_ascii=False)
            rc, out = sh(["ledger.py", "add", "leads.json", "--slot", slot], timeout=300)
            if rc != 0:
                run_errors.append("ledger add failed")
        sent = []
        for i, a in enumerate((o.get("alerts") or [])[:2]):
            if not isinstance(a, dict) or not a.get("asset") or not a.get("text"):
                continue
            asset = str(a["asset"]).strip()
            blk = alert_block_reason(a, leads, recent, {x.upper() for x in sent})
            if blk:
                kind_, why = blk
                if kind_ == "family":
                    run_errors.append(f"alert {asset} blocked: {why}")
                else:
                    lead = next((l for l in leads if str(l.get("asset", "")).strip().upper() == asset.upper()), {})
                    run["near_misses"].append({"asset": asset, "score": lead.get("score") or 8, "why": "alert blocked: " + why})
                print("alert blocked by guard")
                continue
            open(f"alert_{i}.txt", "w").write(str(a["text"]))
            json.dump(a.get("plan") or {}, open(f"plan_{i}.json", "w"))
            rc, out = sh(["ledger.py", "send-alert", f"alert_{i}.txt", asset, f"plan_{i}.json"], timeout=120)
            if rc == 1 and "no open lead" not in out:
                time.sleep(20)
                rc, out = sh(["ledger.py", "send-alert", f"alert_{i}.txt", asset, f"plan_{i}.json"], timeout=120)
            if rc in (0, 2):
                sent.append(asset)
                if rc == 2:
                    run_errors.append(f"alert {asset} only partly sent")
            else:
                run_errors.append(f"alert {asset} could not be sent" + (" (no matching lead)" if "no open lead" in out else ""))
        if isinstance(o.get("playbook"), str) and o["playbook"].strip():
            open("playbook.md", "w").write(o["playbook"])
            rc, out = sh(["ledger.py", "set-playbook", "playbook.md"], timeout=60)
            if rc != 0:
                run_errors.append("set-playbook failed")
        s = load_json("state.json", {})
        pend = (s.get("pending_prep") or {}).pop(slot, {}) if isinstance(s.get("pending_prep"), dict) else {}
        s["applied_slots"] = ((s.get("applied_slots") or []) + [slot])[-60:]
        json.dump(s, open("state.json", "w"), ensure_ascii=False, indent=1)
        slot_at = datetime.datetime.strptime(slot, "%Y-%m-%d %H%M").replace(tzinfo=JST).isoformat()
        rec = {"mode": run.get("mode") or pend.get("mode") or "full", "x": pend.get("x") or {"ok": None},
               "sources": pend.get("sources") or {}, "source_errors": pend.get("source_errors") or [],
               "posts_read": run.get("posts_read", 0), "items_read": run.get("items_read", 0),
               "leads_logged": len(leads), "alerts_sent": sent, "near_misses": run.get("near_misses") or [],
               "errors": (pend.get("prep_errors") or []) + run_errors, "slot_at": slot_at}
        json.dump(rec, open("run.json", "w"), ensure_ascii=False)
        sh(["ledger.py", "runlog", "run.json"], timeout=60)
        finished.append((f, "DONE"))
        digest_due = digest_due or int(slot[-4:-2]) >= 20
        print(f"applied output for slot {slot}: {len(leads)} leads, {len(sent)} alert(s)")
    h = jst_now().hour
    if digest_due or h >= 23 or h < 3:            # the digest waits for the 20:45 run (23:45 run or later retries)
        rc, out = sh(["ledger.py", "digest", "--if-due"], timeout=120)
        print("digest:", "sent" if "DIGEST: sent" in out else "not due / not sent")
    rc, out = sh(["ledger.py", "push"], timeout=120)
    if rc != 0:
        time.sleep(30)
        rc, out = sh(["ledger.py", "push"], timeout=120)
    if rc != 0:
        # alerts may already be out, so never re-apply: mark the files and say so once a day
        err("ledger push after apply failed")
        for f, tag in finished:
            rename(d, f, "NOPUSH " + f["name"])
        if finished and health_once("apply_push"):
            notify("other", "results of a scout run could not be saved to the ledger")
        return
    for f, tag in finished:
        rename(d, f, tag + " " + f["name"])


ARCHIVE_NAME = "X-scout archive"                       # subfolder of the X-scout folder
ARCHIVE_AFTER_DAYS, PURGE_AFTER_DAYS = 7, 60


def archive_folder(d, create=True):
    """Id of the "X-scout archive" subfolder (created when missing and `create`), else None."""
    q = (f"'{FOLDER}' in parents and name = '{ARCHIVE_NAME}' and mimeType = 'application/vnd.google-apps.folder' "
         f"and trashed = false")
    fs = _x(d.files().list(q=q, pageSize=10, fields="files(id,name)")).get("files", [])
    if fs or not create:
        return fs[0]["id"] if fs else None
    body = {"name": ARCHIVE_NAME, "mimeType": "application/vnd.google-apps.folder", "parents": [FOLDER]}
    return _x(d.files().create(body=body, fields="id"))["id"]


def tidy(d):
    """XS-IN / XS-OUT files (any prefix: DONE, BAD, NOPUSH, DUPLICATE) older than 7 days move to the "X-scout archive"
    subfolder - they are the evidence for audits - and are trashed only after 60 days. Raw XS-POSTS files are trashed
    after 7 days as before (the XS-IN doc holds what the run read). The human-readable run logs are kept."""
    now = datetime.datetime.now(datetime.timezone.utc)
    cut = (now - datetime.timedelta(days=ARCHIVE_AFTER_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
    purge = (now - datetime.timedelta(days=PURGE_AFTER_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
    old = [f for key in ("XS-IN", "XS-OUT") for f in list_files(d, key, f"and createdTime < '{cut}'")]
    arch = archive_folder(d, create=bool(old))            # no archive folder -> exception -> nothing is moved or trashed
    for f in old:
        _x(d.files().update(fileId=f["id"], addParents=arch, removeParents=FOLDER, fields="id"))
    for key in ("XS-POSTS", BUILD_PREFIX.strip()):        # raw posts, and temporary parts of failed uploads
        for f in list_files(d, key, f"and createdTime < '{cut}'"):
            _x(d.files().update(fileId=f["id"], body={"trashed": True}))
    if arch:
        for key in ("XS-IN", "XS-OUT"):
            for f in list_files(d, key, f"and createdTime < '{purge}'", parent=arch):
                _x(d.files().update(fileId=f["id"], body={"trashed": True}))


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "tick"
    if mode == "probe":                                    # reachability check of the extra sources; no Drive, no ledger
        rc, out = sh(["sources2.py", "3.5"], timeout=240)
        print("sources2:", (out.strip().splitlines() or ["no output"])[-1][:600])
        rc, out = sh(["misses.py"], timeout=240)
        print("misses:", ([l for l in out.splitlines() if l.startswith("MISSES_JSON")] or ["no MISSES_JSON line"])[-1][:600])
        return
    d = drive()
    now = jst_now()
    slot = next_slot(now)
    mins = (slot - now).total_seconds() / 60
    want_prep = mode == "prep" or (mode in ("tick", "need-prep") and PREP_WINDOW[0] <= mins <= PREP_WINDOW[1])
    if want_prep and not os.environ.get("XS_FORCE_PREP"):
        if slot_built(list_files(d, "XS-IN " + slot_name(slot)), "XS-IN " + slot_name(slot)):
            want_prep = False                              # already prepared for this slot (a second set of parts
                                                           # for one slot lets the Claude run mix parts of two builds)
    if mode == "need-prep":                                # workflow asks first, so Chromium is installed only when needed
        print("yes" if want_prep else "no")
        return
    prep_ok = True
    if want_prep:
        prep_ok = prep(d, slot)
    if mode in ("tick", "apply") and prep_ok:     # never apply on top of a prep whose push failed
        apply(d)
    if now.hour >= 4 and health_once("tidy"):
        try:
            tidy(d)
        except Exception:
            err("tidy failed")
    if ERRORS:
        print(f"{len(ERRORS)} problem(s) this tick")


if __name__ == "__main__":
    main()
