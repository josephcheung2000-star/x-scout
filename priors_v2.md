PRIORS (backtest2.py, generated 2026-09-26; entry >=3h after the event / next session open; excess vs BTC (crypto) or SPY (stocks); commodities absolute; Polymarket in prob. points, + = continuation)
- coinbase_listing: 7d median -10.7% (n=71, 95% CI -17.1% to -8.4%), hit 28%; last 120d -7.7% (n=7); halves -11.9% / -9.5%; 1d -3.3% [CI -7.6%, +0.0%]. Only currently-listed products (new_at); delisted coins missing.
- upbit_krw_listing: 7d median -25.4% (n=76, 95% CI -28.5% to -20.6%), hit 9%; last 120d -23.1% (n=26); halves -26.5% / -23.7%; 1d -12.0% [CI -15.8%, -7.8%]. Listing time = first KRW candle (trading open, usually 1-3h after the notice); delisted markets missing.
- bithumb_krw_listing: 7d median -20.7% (n=87, 95% CI -24.2% to -14.8%), hit 16%; last 120d -21.5% (n=29); halves -23.0% / -18.4%; 1d -5.7% [CI -8.9%, -3.4%]. Listing time = first KRW candle; delisted markets missing. 56 coins overlap Upbit: one KRW listing prior per coin, do not add both.
- token_unlock: -7d..+7d median -7.0% (n=110, 95% CI -9.6% to -3.6%), hit 33%; last 120d -4.4% (n=29); halves -8.0% / -6.8%; -7d..0 -5.2% [CI -6.8%, -2.0%]. Scheduled event, so the -7d entry is actionable; same catalyst as token_unlock_dm1 (do not add both).
- token_unlock_dm1: -1d..+7d median -2.4% (n=111, 95% CI -5.0% to -0.3%), hit 40%; last 120d -2.8% (n=29); halves -3.6% / -2.0%.
- governance_buyback: 7d median +2.1% (n=10, 95% CI -2.3% to +9.2%), hit 60%; last 120d +2.3% (n=5); halves -0.5% / +2.3%; 30d +4.2% [CI -6.8%, +21.9%]. WEAK (n<20).
- insider_cluster_buy: 20d median +2.2% (n=133, 95% CI -0.9% to +4.3%), hit 56%; last 120d +2.6% (n=34); halves +1.5% / +2.5%; 5d -0.1% [CI -1.4%, +0.8%].
- stock_big_move_up: 20d median +0.3% (n=191, 95% CI -1.9% to +1.8%), hit 51%; last 120d -5.9% (n=34); halves +1.3% / -2.0%; 5d -0.2% [CI -0.8%, +1.1%].
- stock_big_move_down: 20d median +0.5% (n=198, 95% CI -1.8% to +2.2%), hit 50%; last 120d -2.5% (n=38); halves +0.4% / +0.5%; 5d -0.3% [CI -1.0%, +0.9%]. 21% of events fall in Apr-2025 (tariff shock).
- commodity_shock_up: 20d median -0.9% (n=140, 95% CI -2.7% to -0.4%), hit 40%; last 120d +2.6% (n=9); halves -2.3% / -0.3%; 5d +0.5% [CI -0.2%, +1.2%]. Nasdaq ETF proxies (Yahoo blocked); coffee/cocoa missing.
- commodity_shock_down: 20d median +0.9% (n=101, 95% CI -1.0% to +2.5%), hit 56%; last 120d -1.8% (n=4); halves +0.8% / +1.3%; 5d +0.0% [CI -0.5%, +1.0%]. Raw return (negative = continuation). Nasdaq ETF proxies; coffee/cocoa missing.
- polymarket_big_move: 7d median -1.8 pts (n=262, 95% CI -4.5 pts to +0.0 pts), hit 41%; last 120d -1.5 pts (n=28); halves -1.0 pts / -2.5 pts; 3d -1.0 pts [CI -2.5 pts, -0.4 pts].
- polymarket_big_move_up: 7d median -9.0 pts (n=131, 95% CI -12.3 pts to -5.0 pts), hit 29%; last 120d -2.8 pts (n=17); halves -9.0 pts / -10.4 pts; 3d -5.0 pts [CI -6.0 pts, -2.2 pts]. 84 of 133 are 'by <date>' markets, so part is time decay, not only overreaction.
- polymarket_big_move_down: 7d median +1.0 pts (n=131, 95% CI -0.5 pts to +3.5 pts), hit 53%; last 120d -1.0 pts (n=11); halves +0.0 pts / +2.2 pts; 3d +1.0 pts [CI -0.5 pts, +2.9 pts].
- binance_new_listing (playbook_v1): 7d median -18% (n=54, CI -30% to -9%), hit 30%.
- binance_perp_launch (playbook_v1): 7d median -15% (n=24, CI -30% to +15%), hit 38%. No edge.
- binance_add_existing (playbook_v1): 7d median -4% (n=8). WEAK, no edge.

SCORING RULES FROM PRIORS (pre-registered: only n>=20 and CI excluding 0; +-2 only if both halves and last-120d agree in sign)
- coinbase_listing: -1 to a LONG idea resting on this catalyst (Coinbase listing: drifts down - avoid long / short).
- upbit_krw_listing: -2 to a LONG idea resting on this catalyst (Upbit KRW listing: drifts down - avoid long / short).
- bithumb_krw_listing: -2 to a LONG idea resting on this catalyst (Bithumb KRW listing: drifts down - avoid long / short).
- token_unlock: -2 to a LONG idea resting on this catalyst (Big cliff unlock (entered 7d before): drifts down - avoid long / short).
- commodity_shock_up: -1 to a LONG idea resting on this catalyst (Commodity up-shock: fades - do not chase).
- polymarket_big_move_up: -2 to an idea that buys in the direction of the jump (the jump mean-reverts from the signal price, but fading it from a realistic entry 3h later lost money after costs in an independent test on 2026-10-07: n=5,752, mean -2.8%; do not fade either).
- Listing catalysts do not stack: apply the single largest listing penalty once per coin (Upbit+Bithumb same day = -2, not -4).
- binance_new_listing (playbook_v1): -2 to a LONG idea whose only catalyst is a listing / HODLer airdrop.
- All other types: 0 (no adjustment; CI spans 0 or n<20). Replace priors once live ledger n>=8 per type at 7d.
