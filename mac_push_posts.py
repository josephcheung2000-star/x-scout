#!/usr/bin/env python3
"""X-scout Mac feeder. X blocks cloud/datacenter IPs, so the X timeline is read here on the Mac
(residential IP) with the existing fetch.py (twitter-cli, @jpihbdu cookies, read-only), converted to
the pipeline's post format, and uploaded to the Drive folder "X-scout" as "XS-POSTS <UTC stamp>".
The GitHub pipeline picks up the newest unused XS-POSTS file when it prepares each Claude run.
Run by launchd (jp.tankyu.xscout.feed) at HH:35 JST, ~70 min before each HH+1:45 slot. Never posts, likes or follows."""
import json, os, subprocess, sys
from datetime import datetime, timezone

HOME = os.path.expanduser("~")
BASE = os.path.join(HOME, "x-scout")
FOLDER = "11yC1KUtWZTUYoZIEuU_AoT8bFhomq6qo"
GWSP = os.path.join(HOME, "bin", "gwsp")
TAB = {"following": "Following", "for-you": "For you", "watchlist": "watchlist"}


def main():
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H%MZ")
    env = dict(os.environ, PATH=f"{HOME}/bin:{HOME}/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin")
    status = {"ok": False, "errors": [], "source": "mac twitter-cli"}
    posts = []
    try:
        r = subprocess.run([sys.executable, os.path.join(BASE, "fetch.py"), "300"], env=env,
                           capture_output=True, text=True, timeout=1200)
        st = json.load(open(os.path.join(BASE, "status.json")))
        if r.returncode == 0 and st.get("ok"):
            d = json.load(open(os.path.join(BASE, "latest.json")))
            for p in d.get("posts", []):
                m = p.get("m") or {}
                posts.append({"id": p.get("id"), "handle": p.get("handle"), "followers": None,
                              "created": p.get("created"), "tab": TAB.get(p.get("feed"), p.get("feed")),
                              "nested": bool(p.get("rt_by")), "followed": True if p.get("feed") == "following" else None,
                              "text": p.get("text"), "links": p.get("links") or [],
                              "m": {"likes": m.get("likes"), "rts": m.get("retweets"), "replies": m.get("replies"),
                                    "quotes": m.get("quotes"), "bookmarks": m.get("bookmarks"), "views": m.get("views")},
                              "url": p.get("url")})
            status.update(ok=True, captured=d.get("raw_fetched"), relevant=d.get("relevant"), kept=len(posts),
                          errors=[str(e)[:160] for e in d.get("errors", [])][:5])
        else:
            status["errors"] = [str(e)[:200] for e in (st.get("errors") or [f"fetch.py exit {r.returncode}"])][:5]
            status["hint"] = st.get("hint", "")
    except Exception as e:
        status["errors"] = [f"feeder error: {str(e)[:200]}"]
    status["stamp"] = stamp
    # fetch.py marks posts as seen, so a failed upload must not lose them: keep the file and retry next run
    pend = os.path.join(BASE, "pending_uploads")
    os.makedirs(pend, exist_ok=True)
    json.dump({"status": status, "posts": posts}, open(os.path.join(pend, stamp.replace(" ", "_") + ".json"), "w"), ensure_ascii=False)
    rc_all = 0
    for fn in sorted(os.listdir(pend)):
        path = os.path.join(pend, fn)
        name = "XS-POSTS " + fn[:-5].replace("_", " ")
        body = json.dumps({"name": name, "parents": [FOLDER], "mimeType": "application/json"})
        u = subprocess.run([GWSP, "default", "drive", "files", "create", "--json", body, "--upload",
                            os.path.join("pending_uploads", fn), "--upload-content-type", "application/json"],
                           env=env, cwd=BASE, capture_output=True, text=True, timeout=120)
        if u.returncode == 0:
            os.remove(path)
        else:
            rc_all = 1
            print("UPLOAD FAILED:", fn, (u.stderr or u.stdout)[-300:])
    print(json.dumps({"stamp": stamp, "ok": status["ok"], "kept": len(posts), "upload_rc": rc_all}))
    # GitHub's cron can run hours late, so start the pipeline's prep now
    subprocess.run(["/bin/bash", os.path.join(BASE, "dispatch.sh"), "prep"], env=env, timeout=60)
    sys.exit(rc_all)


if __name__ == "__main__":
    main()
