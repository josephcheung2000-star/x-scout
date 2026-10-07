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

## Signal-quality upgrade (2026-09-27)
- `sources2.py`: second-tier sources (Coinbase/Upbit/Bithumb new markets, DefiLlama unlocks, Snapshot governance, US spot ETF flows,
  OpenInsider buys, US stock movers, commodity futures, BTC/SPY regime, extra Polymarket). New trigger keys fire once per 7 days
  (`state.trig_seen`) and force a full run.
- `misses.py`: daily miss log (08:45 prep) - big movers in crypto / US stocks / commodities / Polymarket and whether a lead
  flagged them; 14-day recall in `state.miss_stats`, shown in the digest.
- `backtest2.py` -> `priors.json` / `priors_v2.md`: backtested base rates per catalyst type, included in every input doc.
- `ledger.py`: lead kinds commodity (Yahoo futures ticker) and polymarket (market_slug + outcome); EV fields (p_win, target,
  stop, horizon) with target-vs-stop resolution and Brier calibration; paper trades net of fees, slippage and funding;
  per-account track records with shrinkage (`handle-scores`); 👍/👎 buttons on digest near-misses (`poll-replies`).

## Research rounds (2026-09-28, audited) - see family_stats.json
- Four families lost money in the backtest and never alert (pipeline guard, by family tag or catalyst+direction):
  unlock shorts, insider-buy longs, funding fades, macro/commodity fades. They are not near-misses either.
- Crypto-short alerts are blocked when no perp venue exists (Binance/Bybit/OKX/Hyperliquid; a venue that cannot be
  reached from the runner counts as unknown and never blocks). Repeat alerts on an asset within 14 days are blocked.
- Tested and rejected: fixed 20/10/14 exits, a $20M volume bar, family-based p_win/EV, regime filter, pump filters,
  entry delay, trailing stop (no out-of-sample support). No family has a proven out-of-sample edge.
- Shadow test restarted: 2026-09-28T02:00Z .. 2026-11-28.

## Changelog

### 2026-10-07 - audit fixes (runs of 2026-09-28 .. 10-07)
Ledger and stats
- 01 Polymarket targets and stops are capped at what a share can reach (0.99 / 0.01), so a market that resolves the
  right way counts as a target hit instead of timing out.
- 02 A "watch" lead is scored on the side its `lean` names; a watch with no lean is price-tracked but kept out of
  directional stats and calibration.
- 03 The learning stats (stats, brief, shadow scorecard) use only leads logged since the 2026-09-28 restart, one per
  instrument per 7 days; returns are capped at +-100% inside group means (so one Polymarket market cannot dominate); the brief says how many leads are still
  within their horizon.
- 04 A re-mention merges into an earlier lead only if it is the same instrument (CoinGecko id / ticker / Polymarket
  market + outcome) on the same side. An alert on a lead first logged more than 6 hours earlier gets its own row priced
  at alert time, so the move before the alert is not credited to it.
- 05 A stock or futures lead logged while its market is shut enters at the first live-session price, not the last
  close; holding periods run from the actual entry.
- 08 An account gets a +1/-1 weight only with at least 8 results and a Wilson bound clear of the base rate; only the
  accounts behind a lead when it was first logged are credited.
- 17 A lead counts as alerted only once its alert was actually delivered (send-alert). The model's own "alerted" flag
  is kept for reference only, so an alert the pipeline blocked starts no 14-day cooldown and stays out of the alert
  stats. The shadow scorecard and the paper "no answer" count skip rows replaced by an alert-time row.
- 20 The LEDGER STATS section starts with the same "Stats sample: N independent leads since ..." line as the brief.
- 15 Leads may carry `lean` as "up" / "down" / "none" (or long / short), and Polymarket leads may carry share-price
  levels `target_price` and `stop_price` next to `target_pct` / `stop_pct`. When valid, the share prices win and the %
  levels are recomputed from the actual entry; impossible prices are ignored and noted. Unknown fields are still ignored (the
  current main ledger already accepts all of these without error, so the prompt can ship first).

Pipeline
- 06 XS-IN parts are at most 4,000 bytes (the Claude run's Drive reader cut ~6-7 KB parts); Drive calls retry on rate
  limits and server errors; the 08:45 daily sweep follows the slot being prepared, not the wall clock; the source window
  covers the time since the last prep plus 30 minutes (3.5-12 h); an explicit `prep` dispatch no longer bypasses the "already prepared" check.
- 09 One build per slot. The ledger now records each slot whose input was written (`prepped_slots`); a second prep for
  that slot does nothing. Just before writing, prep checks Drive again and, if the slot's XS-IN appeared meanwhile,
  writes nothing and gives back the triggers it would have used. A forced rebuild (`XS_FORCE_PREP`) keeps the first
  build's full mode, re-reads the posts the first build consumed, and still sees that slot's own triggers.
- 10 Korean exchange events are triggers (once per 7 days per asset and event; which of them force a full run: see 19): Upbit caution / warning flag on and
  off, Bithumb investment-warning designation and lifting, Bithumb deposit/withdrawal suspension and resumption (new
  public status feed), and the same events in Bithumb (and, when reachable, Upbit) notice titles.
- 11 Second-tier lists no longer hide what matters: every exchange flag / notice / wallet change, every unlock of 2% or
  more of unlocked supply and every insider buy of $1M+ (or cluster) is shown; other sources still show 10 and name the
  ones left out. The input doc may run to more 4 KB parts.
- 12 Old XS-IN / XS-OUT files are moved to the Drive subfolder "X-scout archive" after 7 days and trashed only after
  60 days (they were trashed after 7, which destroyed audit evidence). Raw XS-POSTS files are still trashed after 7 days.
- 18 Input parts are uploaded under a temporary name ("XS-BUILDING ...") and renamed to "XS-IN ..." only when all parts
  exist. A slot counts as prepared only if its input is complete, so a prep that failed half-way is rebuilt by the next
  tick instead of blocking the slot. A manual run can force a rebuild (workflow input force_prep); the older parts are
  renamed "SUPERSEDED ...".
- 19 Korean exchange events no longer all force a full run. Only a new warning designation, a delisting notice or an
  unscheduled deposit/withdrawal suspension does (a suspension announced as maintenance, a network upgrade or for a set
  time does not), and at most 5 of them per prep. All other exchange events are still listed in the input.
- 21 Smaller fixes: a Drive permission error is no longer retried (rate limits still are); a Drive upload that may have
  landed is checked by name before it is sent again, so no duplicate part; a Polymarket level of exactly 1.0 or 0.0
  ("hold to resolution") is treated as 0.99 / 0.01 rather than blocking the alert; the input doc shows the real source
  window; labels such as "(ERC20)" in exchange notices are not taken for coin tickers.
- 13 Polymarket alert backstop: an alert is blocked (logged as a near-miss) when the side bought moved 8 points or
  more against the trade in the past 24 hours, or when its target or stop as a share price is outside 0.01-0.99 (see 21
  for levels of exactly 1.0 / 0.0). If the
  price data cannot be fetched, the alert is not blocked. (The rule against mid-priced bets anchored on an outside
  forecast stays in the prompt: it cannot be checked by code.)

Miss log
- 07 A mover counts as caught only if a lead flagged it beforehand on the right side; wrong-side leads and no-lean
  watches are kept as "seen".
- 14 Sports and novelty Polymarket markets are not counted as misses; movers are ranked by size relative to their
  usual volatility instead of mixing % moves with Polymarket points; a 403 from a source is retried once; a lead on a
  single-asset ETF (e.g. IBIT, GLD, USO) counts for its underlying (multi-stock ETFs are not mapped to their members).
