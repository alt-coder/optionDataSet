# Tools changelog (rules in params/frozen.json unchanged, sha256 cc551c1e...a2000)

2026-10-29 — data-handling fixes found by the first (aborted) 2026 examination run:
1. engine.Day.opt: a HELD leg whose strike drifts outside the feed's +-950pt strike window is valued at
   intrinsic (floored at Rs 0.05) instead of failing (monitor) / being valued at its entry price (engine).
   Trigger: 10-MAR-2026 expiry, far-OTM short calls left the window after a sharp fall.
2. monitor.next_event: data/logic errors are surfaced as an ERROR event (run stops) instead of being
   silently skipped; days the API does not serve (e.g. 18-May-2024 special Saturday session) are skipped,
   like the engine.
3. strategies._days_to_expiry: a special session mapped to a different expiry no longer forces an early
   time-exit; the position is held to its own expiry.
Re-validation on TRAIN (2023-2025): monitor autopilot vs engine = 450/450 identical trades & exits.
Training result after fixes: options-only CAGR 11.1%, maxDD -5.5%, Sharpe 1.37 (was 11.3 / -5.5 / 1.40).
The first examination was discarded and restarted from scratch with fresh books.
- 2026-10-29: ledger.csv exit rows now record only the exit-order charges (previously cumulative entry+exit; P&L was always correct).

2026-10-29 — v2 + anti-lookahead hardening
4. Rules v2 (params/frozen.json, sha256 60107dd1...): adds the premium filter (stand aside when ATM IV < 11.2% and IV/RV10 < 1.22).
   Re-learned on regime-balanced TRAIN months only (params/split.json). v1 archived in params/history/.
5. LOOKAHEAD FIX: lot sizing and margin now use the 14:59 signal-bar close (previously the 15:00 fill bar's
   open and close, i.e. the fill bar's close was a small intra-bar lookahead). Engine and monitor both.
6. lookahead_test.py: future-corruption test. 40/40 entries and 40/40 exits unchanged when all later data is garbage.
7. monitor status: Greeks, short-strike distance in EM, learned E[hold] (TRAIN greek study), decision + reason.
   monitor/ensemble: --set TRAIN|TEST month filter. strategies: optional roll_at (REBALANCE), off in v2.
Re-validation on 2023-25: monitor vs engine 390/390 identical trades and exits.

2026-10-29 — site control, business case, challenger
8. opify_ui.js v1.2: full order lifecycle on the Opify simulator (order -> setQty via P&L-tab Change Qtn -> goto -> exit -> reset),
   non-blocking page calls (a 2nd B/S click on an existing strike raises a blocking native alert; order() now refuses it).
   serve_ui.py serves the controller on 127.0.0.1:8765 for one-line injection. Verified by mirroring a v2 TEST trade
   (2026-01-08, S 26100CE / B 26300CE x780): Opify realised +Rs 19,422 = predicted (evidence/opify_mirror_trade_2026-01-08.jpg).
9. business_case.py: like-for-like TEST-month economics + bootstrap (results/business_case_v2.json).
10. OPIFY_RULES env selects the rules file (frozen = champion v2; candidate_v3 = rejected challenger). Monitor mirrors the
    strong-trend lean; v3 monitor==engine 390/390, no lookahead; v3 underperformed v2 on TEST -> not promoted.

2026-10-29 — multi-index
11. OPIFY_SYMBOL (NIFTY|SENSEX|BANKNIFTY) in engine/sim/monitor with per-symbol caches; OPIFY_CAPITAL / OPIFY_ALLOC portfolio sizing
    in the monitor (rules unchanged). multi_asset.py (portability), portfolio_sim.py (3-index no-leverage book).
12. Raw data + metadata pushed to github.com/alt-coder/optionDataSet branch opify-simulator-data (npz caches excluded: regenerable).
- 2026-10-29: opp_k (iron-condor) legs fall back to the 2-leg spread when the far strike is outside the feed window (previously the whole setup was silently skipped -> selection bias found by the v4 examiner).
