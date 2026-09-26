#!/usr/bin/env python3
"""X-scout cloud fetcher: headless Chromium with the @jpihbdu cookies, scrolls the home
timelines (For you, Following, and the 加密/金融/股票 topic tabs), captures X's own GraphQL
timeline JSON, filters crypto/investment posts from the last WINDOW hours, writes posts.json.
Read-only. Usage: X_AUTH_TOKEN=.. X_CT0=.. python3 xfetch_cloud.py [target=300] [window_h=14]"""
import json, os, re, sys, time
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright

TARGET = int(sys.argv[1]) if len(sys.argv) > 1 else 300
WINDOW_H = float(sys.argv[2]) if len(sys.argv) > 2 else 14
AT, CT = os.environ["X_AUTH_TOKEN"], os.environ["X_CT0"]
KW = re.compile(r"(?i)(crypto|bitcoin|\bbtc\b|\beth\b|ethereum|solana|\bsol\b|\bxrp\b|\bbnb\b|altcoin|"
    r"\btokens?\b|tokenomics|airdrop|\bdefi\b|\bdex\b|stablecoin|\busdt\b|\busdc\b|on-?chain|staking|\byield|"
    r"\betf\b|\bipo\b|stock|equit|\bshares\b|earnings|guidance|valuation|dividend|\bbonds?\b|treasur|"
    r"\bfed\b|rate cut|rate hike|\bcpi\b|inflation|macro|liquidity|nasdaq|s&p|\bspx\b|\bspy\b|\bqqq\b|"
    r"\bgold\b|silver|commodit|\boil\b|uranium|invest|portfolio|breakout|accumulat|bullish|bearish|"
    r"leverage|funding rate|open interest|whale|\bmvrv\b|halving|polymarket|prediction market|"
    r"\$[A-Za-z]{2,6}\b|加密|比特|币|幣|投資|投资|股|仮想通貨|暗号資産|株)")

posts = {}

NESTED_KEYS = ("retweeted_status_result", "quoted_status_result")
FOLLOW_TABS = ("正在跟隨", "Following", "正在关注", "フォロー中")

def walk(o, tab, nested=False):
    if isinstance(o, dict):
        leg = o.get("legacy")
        if isinstance(leg, dict) and "full_text" in leg and o.get("rest_id"):
            add(o, tab, nested)
        for k, v in o.items():
            walk(v, tab, nested or k in NESTED_KEYS)
    elif isinstance(o, list):
        for v in o:
            walk(v, tab, nested)

def add(t, tab, nested=False):
    tid = t["rest_id"]
    if tid in posts:
        p0 = posts[tid]
        if not nested and p0.get("nested"): p0["nested"] = False                    # seen directly too
        if tab in FOLLOW_TABS and not nested: p0["tab"] = tab                       # Following tab wins for attribution
        return
    leg = t["legacy"]
    note = (((t.get("note_tweet") or {}).get("note_tweet_results") or {}).get("result") or {}).get("text")
    text = note or leg.get("full_text", "")
    ures = ((t.get("core") or {}).get("user_results") or {}).get("result") or {}
    handle = (ures.get("core") or {}).get("screen_name") or (ures.get("legacy") or {}).get("screen_name")
    def _find(o, k):
        if isinstance(o, dict):
            if k in o: return o[k]
            for v in o.values():
                r = _find(v, k)
                if r is not None: return r
        return None
    followers = _find(ures, "followers_count")
    rp = ures.get("relationship_perspectives") if isinstance(ures.get("relationship_perspectives"), dict) else {}
    fl = rp.get("following") if "following" in rp else (ures.get("legacy") or {}).get("following")
    followed = fl if isinstance(fl, bool) else None
    try:
        created = datetime.strptime(leg["created_at"], "%a %b %d %H:%M:%S %z %Y")
    except Exception:
        return
    views = (t.get("views") or {}).get("count")
    posts[tid] = {"id": tid, "handle": handle, "followers": followers, "created": created.isoformat(),
        "tab": tab, "nested": nested, "followed": followed, "text": text[:1500],
        "links": [u.get("expanded_url") for u in (leg.get("entities") or {}).get("urls", [])],
        "m": {"likes": leg.get("favorite_count"), "rts": leg.get("retweet_count"), "replies": leg.get("reply_count"),
              "quotes": leg.get("quote_count"), "bookmarks": leg.get("bookmark_count"), "views": int(views) if views else None},
        "url": f"https://x.com/{handle}/status/{tid}", "_ts": created.timestamp()}

def relevant():
    cut = time.time() - WINDOW_H * 3600
    return [p for p in posts.values() if p["_ts"] >= cut and KW.search(p["text"])]

def main():
    cur_tab = {"v": "for-you"}
    status = {"ok": False, "errors": []}
    with sync_playwright() as p:
        b = p.chromium.launch(proxy={"server": os.environ["HTTPS_PROXY"]} if os.environ.get("HTTPS_PROXY") else None)
        ctx = b.new_context(ignore_https_errors=True, locale="en-US", viewport={"width": 1280, "height": 2400},
                            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                                       "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
        ctx.add_cookies([{"name": "auth_token", "value": AT, "domain": ".x.com", "path": "/", "secure": True},
                         {"name": "ct0", "value": CT, "domain": ".x.com", "path": "/", "secure": True}])
        pg = ctx.new_page()
        def on_resp(r):
            if "/graphql/" in r.url and "Timeline" in r.url:
                try:
                    walk(r.json(), cur_tab["v"])
                except Exception:
                    pass
        pg.on("response", on_resp)
        pg.goto("https://x.com/home", timeout=60000)
        pg.wait_for_timeout(6000)
        try:
            pg.wait_for_selector("article", timeout=25000)   # slower hosts (e.g. GitHub runners) render later
        except Exception:
            pass
        if "login" in pg.url or pg.locator("article").count() == 0:
            try:
                seen = f"title={pg.title()[:60]!r} text={pg.inner_text('body')[:160]!r}"
            except Exception:
                seen = "page unreadable"
            status["errors"].append(f"not logged in / no timeline (url={pg.url}; {seen})")
        tabs = pg.locator('[role="tab"]')
        names = [tabs.nth(i).inner_text().strip() for i in range(tabs.count())]
        # order: Following first (true follow attribution), then topic tabs, then For you
        wanted = [n for n in names if n in FOLLOW_TABS]
        wanted += [n for n in names if n in ("加密", "金融", "股票", "Crypto", "Finance", "Stocks")]
        wanted += [n for n in names if n in ("為你推薦", "For you", "为你推荐", "おすすめ")]
        for name in wanted or [None]:
            if name:
                cur_tab["v"] = name
                try:
                    pg.locator('[role="tab"]', has_text=name).first.click(); pg.wait_for_timeout(4000)
                except Exception as e:
                    status["errors"].append(f"tab {name}: {str(e)[:100]}"); continue
            stale = 0
            for _ in range(int(os.environ.get("XS_SCROLLS", "40"))):
                before = len(posts)
                pg.mouse.wheel(0, 6000); pg.wait_for_timeout(1800)
                stale = stale + 1 if len(posts) == before else 0
                if stale >= 4 or len(relevant()) >= TARGET * 1.3:
                    break
            if len(relevant()) >= TARGET * 1.3:
                break
        b.close()
    rel = relevant()
    def score(x):
        m = x["m"]; g = lambda k: m.get(k) or 0
        return g("likes") + 3*g("rts") + 2*g("replies") + 3*g("quotes") + 2*g("bookmarks") + g("views")/200
    rel.sort(key=score, reverse=True)
    keep = rel[:TARGET]
    for x in keep:
        x.pop("_ts", None)
    status.update(ok=bool(posts), captured=len(posts), relevant=len(rel), kept=len(keep), tabs=wanted,
                  stamp=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"))
    json.dump({"status": status, "posts": keep}, open("posts.json", "w"), ensure_ascii=False, indent=0)
    print(json.dumps(status, ensure_ascii=False))
    sys.exit(0 if status["ok"] else 2)

if __name__ == "__main__":
    main()
