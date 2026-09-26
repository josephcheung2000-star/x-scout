# x-scout

Joseph's crypto/investment scout. It runs every 3 h, before each Claude run at HH:45 JST (HH = 2, 5, 8, 11, 14, 17, 20, 23). It is read-only on X and never trades.

## How it fits together

1. **Mac feeder** (`~/x-scout/push_posts.py`, launchd `jp.tankyu.xscout.feed` at HH:35):
   - reads the @jpihbdu timeline with twitter-cli (X shows cloud IPs a bot wall, so this has to run on the Mac);
   - uploads `XS-POSTS <stamp>` to the Drive folder "X-scout";
   - dispatches `prep` here.
2. **GitHub pipeline** (`pipeline.py`, workflow `pipeline.yml`; secrets hold every credential):
   - **prep**: ledger pull → reply buttons → prices/paper trades → posts → `sources.py` → `triage.py` → Google Doc `XS-IN <slot>` → ledger push.
   - **apply**: reads `XS-OUT <slot>` → adds leads → sends Telegram alerts with Took/Skipped buttons → playbook → run log → daily digest → ledger push.
3. **Claude scheduled task** "X crypto scout" (holds no credentials):
   - reads `XS-IN`;
   - shortlists, verifies and scores;
   - drafts and fact-checks any alert;
   - writes `XS-OUT <slot>` (JSON) and a human run log.
4. **Kickers**: GitHub's cron can run hours late, so the Mac also dispatches the pipeline (`jp.tankyu.xscout.apply`, 20/35/55 min after each slot) and the listing watcher (`jp.tankyu.xscout.watch`, every 5 min). The cron stays on as a backup.

The ledger state is the pinned JSON document in the private Telegram channel "X-scout storage" (`ledger.py`, which uses an optimistic version lock). `backtest.py` and `playbook_v1.md` hold the audited priors.

This repo is public: scripts only, no credentials, no data. The Actions logs print counts only.
