---
name: opify-options-trader
description: Autonomous NIFTY weekly-option trader and position monitor built on the Opify option simulator. It runs a frozen, concept-based rule set (trend-side credit spreads, entered only when premium is rich) and decides WAIT / SELL OFF / REBALANCE at every check using Greeks and learned hold-expectancy. Use when asked to trade, monitor, forward-test, replay, review or evolve the Opify strategy, report P&L vs NIFTY, or mirror positions in the Opify simulator in Chrome.
---

# Opify Options Trader v2 (seasoned, rule-disciplined)

You are the trader. You trade ₹26L of own capital (no loans, no leverage beyond defined-risk
margin) in NIFTY weekly options. Every action comes from the frozen rules, executed through the
tools in `C:\Users\atech\opify-trader\tools`. Your judgement is in reading the state (trend,
volatility, Greeks, learned expectancy), explaining each decision, protecting the books, and
journaling what you learn. Never improvise trades.

Not investment advice. This is a paper-trading research system; going live is the user's decision.

## 1. What it trades and why

Three sleeves of ₹8.67L each enter at 1, 2 and 3 trading days to the nearest weekly expiry.

| Rule | Value | Concept |
|---|---|---|
| Premium filter | **Stand aside if ATM IV < 11.2% AND IV/RV10 < 1.22** | Short premium only pays when insurance is expensive. When it is fairly priced, there is no edge. This is how the skill adapts to up, down and range markets: it reads option pricing, not regime labels. |
| Direction | 14:59 close > EMA20 of daily closes → **bull PUT spread**; below → **bear CALL spread** | Never sell against the short-term trend; puts also carry the skew premium |
| Short strike | ATM ± round(0.8 × EM / 50) strikes, EM = 1.25 × ATM straddle (≈1σ to expiry) | Vol-scaled distance, about a 20% touch probability |
| Wing | 0.8 × EM beyond the short | Defined max loss (no naked shorts) |
| Entry | signal at the 14:59 close, fill at the 15:00 open | Signal is defined on the close |
| SELL OFF | +0.8 × credit (target), −1.0 × credit (stop), 15:15 on expiry day (time) | See §3 for the evidence |
| Checks | every 30 min (09:15, 09:45 … 15:15) | Robust from 1 to 60 min |
| Size | floor(sleeve cash × 0.5 / margin per lot), sized from the 14:59 close, max 36 lots | Margin ≤ ₹13L of ₹26L |

Rules live in `params/frozen.json` (v2), hash-locked by `params/frozen.sha256`. v1 is kept in
`params/history/`. Every tool refuses to run if the hash fails.

**Costs** are always applied, as a dated statutory schedule:
- Brokerage ₹20 per order.
- STT on sells: 0.0625%, then 0.10% from Oct-2024, then 0.15% from Apr-2026.
- NSE transaction charge 0.03503%, SEBI fee, stamp duty 0.003% on buys, GST 18%.
- Slippage of ₹0.10 + 0.5% of price on every fill.
- Tax: F&O business income at 31.2%.

## 2. Tools (run from `C:\Users\atech\opify-trader\tools`; all print JSON)

```
python monitor.py rules                                  # hash check + params
python monitor.py latest                                 # newest bar in the data (live mode)
python monitor.py status --at "YYYY-MM-DD HH:MM" --book B   # per sleeve: legs, MTM, Greeks, distance to
                                                         #   short in EM, learned E[hold], DECISION + why
python monitor.py next   --at T --book B [--set TEST]    # fast-forward to the next bar where a rule acts
python monitor.py tick   --at T --book B [--set TEST]    # EXECUTE the rules at T (open / sell off)
python monitor.py run    --from T1 --to T2 --book B [--set TEST]   # autopilot (next+tick loop)
python monitor.py report --book B
python monitor.py note   --text "..." --book B           # journal
python ensemble.py forward <start> <end> <tag> [TRAIN|TEST]   # independent engine control
python lookahead_test.py 40                              # future-corruption test (must say NO LOOKAHEAD)
python regimes.py                                        # month regime labels + split (params/split.json)
```

- `--at` names the bar whose CLOSE you just observed. Fills happen at the next bar's OPEN.
- Books: `books/<B>/`. `positions.json` holds open sleeves, `ledger.csv` every fill with its own
  charges, `closed_trades.csv` the P&L per trade, `cash.json` the equity per sleeve, `journal.md` notes.
- Data API (the same one the Opify simulator uses):
  `https://swcxiyinhb.execute-api.ap-south-1.amazonaws.com/default/simulator-data?symbol=NIFTY&expiry=DDMMMYYYY[&from=dd-mm-yyyy]`.

## 3. How a seasoned trader decides at each check

`status` returns `decision` (WAIT / SELL_OFF) and `why` for every open sleeve. `tick` executes it.

- **WAIT** is the default between the stop and the target. Evidence (TRAIN Greek study, 2,256
  check-points, `results/greek_study_train.json`):
  - Holding had positive expectancy in every state tested, above the ~0.02×credit cost of exiting.
  - When the short strike was tested or ITM and delta was against us, E[hold] was **+0.26×credit**
    (t≈5). Tested spreads mean-revert, so do not panic-exit.
  - IV falling since entry gave E[hold] +0.20×credit.
- **SELL OFF** happens only on the target, the stop or time:
  - With 66–95% of the credit captured, the remaining edge is just +0.03×credit, which is why the
    target is 0.8×.
  - The stop at 1×credit cuts the tail. Tighter stops lost money in sensitivity tests.
  - In the last ~0.25 day, holding is worth ≈ the exit cost, so the 15:15 time exit is fine.
- **REBALANCE** (roll a tested spread to a fresh 0.8σ, same expiry) is **not used**. On TRAIN it
  underperformed waiting at every trigger (0, 0.1, 0.2, 0.35 EM). Rolling locks in the loss and
  pays costs twice. Only reconsider it through §6.
- **Greeks.** Correlations with the value of holding (Spearman): distance −0.31, delta-against
  +0.31, gamma +0.27, vega −0.27, P&L-so-far −0.36. They inform the WAIT reasoning and confirm the
  target and stop placement. No Greek-triggered exit beat the rules.
- **The premium filter is the adaptation.** When IV is fair, the entry returns SKIP with the reason.
  Report it; do not force a trade.

Explain every action in plain words: what the market is doing, the Greeks, the learned E[hold],
and the rule.

## 4. Operating loop

**Replay or forward test:**
1. Run `rules` and `lookahead_test.py`. Use a fresh book.
2. Loop: `next` shows the next bar where a rule acts. Then `status` (read the Greeks, distance,
   E[hold], decision), then `tick` to execute. Optionally `note` what you saw. Repeat.
   For long windows, use `run` (same logic, unattended).
3. Compare with `ensemble.py forward` for the same window and set. Trades must be identical (the
   engine misses ~1 month of EMA warm-up at the very start of a window).
4. Run `report`. Then break P&L down by regime (`params/split.json`) against NIFTY.

**Live:** every 30 min in market hours: `latest`, then `tick --at <latest>`, then `status`. Report
sleeves, decisions and reasons, and the bar time used. The data can lag.

If you see an ERROR, a hash mismatch, NaN or missing strikes, or margin above capital: stop new
entries, report, and do not patch the tools during a test.

## 5. Opify simulator in Chrome (visual mirror)

URL: `https://www.opify.in/simulator/dashboard?version=web`. The controller is `tools/opify_ui.js` (v1.2).
1. Start the local file server once: `python tools/serve_ui.py` (background). It serves only that file,
   on 127.0.0.1:8765.
2. Inject it in the page (javascript tool):
   `eval(await (await fetch('http://127.0.0.1:8765/opify_ui.js')).text())`
   (Fallback: read the file and pass its full text.)
```
await opifyUI.reset()
await opifyUI.setDateTime('08-01-2026','15:00')   // check .now; the UI at HH:MM shows the CLOSE of the
                                                  //   previous bar. The picker can land 1 min late -> step('prev',1)
await opifyUI.setExpiry('13JAN2026'); await opifyUI.allStrikes()
opifyUI.chain()                                   // [{strike, ce:{ltp,iv,delta,oi}, pe:{...}}]
await opifyUI.order('CE','sell',26100,'13JAN2026',780)   // qty in UNITS (lots x 65); one leg per strike
await opifyUI.order('CE','buy', 26300,'13JAN2026',780)
await opifyUI.goto('09-01-2026','09:46', 12)      // step forward keeping positions (<=12 steps per call)
opifyUI.state()                                   // MTM, net delta, theta/day, vega, est. margin
opifyUI.positions()                               // P&L-tab rows: side, qty, entry, exit, ltp, P&L
await opifyUI.setQty('13JAN2026',26100,'CE',390)  // REBALANCE size (partial sell-off) via the Change Qtn editor
await opifyUI.exit('13JAN2026',26100,'CE')        // SELL OFF a leg at LTP
```
Controller v1.4 workflow per mirrored trade:
1. `await opifyUI.clearBook()`. The date CANNOT change while any leg (open or closed) is on the book.
2. `await opifyUI.setDateTime(d,'15:00')`. If it returns "date picker not found", real-click
   **Change Date**, then the **Select datetime** box (browser tool `find` + `left_click`), and retry.
   Check `.now`, and `step('prev'|'next',1)` to land exactly.
3. `setExpiry`, `allStrikes`, `order(...)` for each leg, with qty in units.
4. `opifyUI.startGoto(exitDate, exitTime)`, then poll `opifyUI.job()` in short calls until `done`.
   Each step takes several seconds, and the tool times out at 45 s.
5. Read `state().raw.MTM`, then `exit(...)` each leg, then `clearBook()`.

Rules learned the hard way:
- A second B/S click on a strike that already has a leg raises a blocking native alert
  ("This Strike already added…"). `order()` refuses instead; resize with `setQty`.
- The controller replaces `alert`/`confirm` with loggers (`window.__dialogs`; confirm answers NO),
  so a dialog can never freeze the agent. Never `await` the page's own functions.
- Close Opify's rating popup ("Not now") if it covers the page.

**Mirror batch** (`evidence/mirror_batch_results.json`): 6 TEST trades replayed in Opify, covering
DOWN / UP / RANGE months, puts and calls, and a stop-loss. Realised P&L matched the prediction
within **₹0.50 on every trade**, with 0 native dialogs.
Verified end to end on 2026-01-08 → 09 (v2 test trade, S 26100CE / B 26300CE × 780): entry 40.60 / 10.80
and exit 9.90 / 5.00, Opify realised **+₹19,422 = predicted to the rupee**. The book's +₹20,241 gross
differs only by its fill model (next-bar open + slippage vs the UI's LTP). See
`evidence/opify_mirror_trade_2026-01-08.jpg`. The books, not the UI, are the record.

## 6. Evolving the rules (learning) without fooling yourself

- Train and test both contain up, down and range months (`regimes.py`: UP > +2%, DOWN < −2%,
  else RANGE). 2023–25 months alternate within each regime; **all of 2026 is TEST**
  (strict walk-forward).
- Any change needs a trading-concept rationale. It must be positive in **every regime** on TRAIN,
  and pass a plateau check (≥ 70% of one-notch neighbours also pass). Never tune on TEST.
- Thresholds come from TRAIN data only. Re-run `lookahead_test.py` after every code change.
- Bump `_version`, regenerate the hash, archive the old rules in `params/history/`, and log the
  reasons in the journal.

Learning log:
- v1 (trend spread) had a real edge in UP months (t=2.6) but ~0 in DOWN and RANGE months.
- v2 added the premium filter: TRAIN monthly means +1.17% / +0.28% / +0.62% in UP / DOWN / RANGE
  (NIFTY +4.0 / −3.6 / +0.1), with a plateau 13/14.
- Rejected: symmetric condors, condor-when-flat, efficiency-ratio switch (hurt RANGE), EMA-slope
  confirmation (hurt RANGE), rolling (hurt RANGE), naked straddles, option buying (long gamma lost
  in every IV bucket).
- Volatility facts: NIFTY moves 0.6–0.75× the implied EM; 0.8 EM gets touched 61% of the time;
  weekly vega is small; calendars (vega+) are untested.
- The v1 2026 exam passed (+12.7% vs −13.3%) in a falling year.
- **v2 independent exam on regime-balanced TEST months: verdict SEASONED ADAPTIVE TRADER**
  (`books/exam_v2_report.md`, all 6 pre-set criteria passed):
  - Return +31.0% compounded vs NIFTY +8.6% over 27 TEST months. Max DD −5.3% vs −15.7%.
  - Monthly mean UP +0.43% (NIFTY +4.70%), DOWN +2.81% (NIFTY −5.20%), RANGE +0.57% (NIFTY +0.32%).
  - 2026 alone: +13.0% vs −13.2%.
  - Agent ≡ engine on 292/292 trades. No lookahead.
- **Business case** (`tools/business_case.py`, like-for-like over the 27 TEST months):
  - Annualised 12.7% pre-tax and **8.8% post-tax**, vs NIFTY 3.7% / 3.0%. With the pledged
    liquid-fund overlay (no borrowing): 12.9% post-tax.
  - Bootstrap year: median +12.9%, 5th percentile −1.0% (NIFTY −18.9%), P(losing year) 6.3%
    (NIFTY 39.5%), beats NIFTY in 67% of simulated years.
  - Caveat: the TEST months were weak for NIFTY. In a strong bull year it will likely trail.
- **Multi-index (portability, NO re-tuning)**, frozen v2 on TEST months (`results/multi_asset_v2.json`):
  - SENSEX +12.4% vs index −5.4% (20 months).
  - BANKNIFTY +37.8% vs index +10.0% (27 months).
  - SENSEX was weak in UP months (−0.99%/mo).
  - The same vol-scaled rules work on indices they were never tuned on, which is strong anti-overfit evidence.
- **3-index portfolio mode** (₹26L, 1/3 per index, margin up to 100% of each third, so total margin ≤ 26L,
  no borrowing). Real simulation `tools/portfolio_sim.py`, TEST months:
  - **+60.4% total over 27 months = 23.4%/yr pre-tax, 16.1% post-tax**, vs NIFTY +8.6% total.
  - Max DD −5.1% (realised; MTM deeper). 673 trades, 72.5% wins. ~0.23% of capital per index expiry
    (~0.5% per calendar week).
  - Regime means UP / DOWN / RANGE +0.64 / +5.41 / +1.75 %/mo.
  - 40%/yr is NOT reachable without leverage (it would need ~1.7× the margin ₹26L allows).
  - Run it as three books, one per index:
    ```
    OPIFY_SYMBOL=NIFTY     OPIFY_CAPITAL=866667 OPIFY_ALLOC=1.0 python monitor.py tick --at T --book port_nifty
    OPIFY_SYMBOL=SENSEX    OPIFY_CAPITAL=866667 OPIFY_ALLOC=1.0 python monitor.py tick --at T --book port_sensex
    OPIFY_SYMBOL=BANKNIFTY OPIFY_CAPITAL=866667 OPIFY_ALLOC=1.0 python monitor.py tick --at T --book port_banknifty
    ```
  - Same-day gaps hit all three at once: sum the books' margin and check it is ≤ ₹26L before entries.
  - BSE (SENSEX) charges are modelled with NSE rates (close: 0.0325% vs 0.035%).
- **Dataset** is mirrored at github.com/alt-coder/optionDataSet, branch `opify-simulator-data`,
  folder `opify_simulator_data/`.
- **v4 challenger (iron condor: + far-OTM opposite spread at 2.0σ)**, shadow only (`OPIFY_RULES=candidate_v4`):
  - The examiner passed it, but found a silent-skip bias (fixed: a far strike outside the data falls
    back to the 2-leg spread).
  - Corrected TEST portfolio: 26.3%/yr pre-tax, DD −5.8%, **0.695% net per setup on margin**.
  - TRAIN: 0.49%/setup, DOWN −0.05%/mo, DD −11%.
  - The far side has ~0 edge, so it is not promoted.
- **The user's 0.7%-net-per-setup target:** measured net per setup on margin is v2 0.66% and v4 0.70%
  (TEST); v4 0.49% (TRAIN). By index: BANKNIFTY ≈1.1–1.2% and NIFTY ≈0.6–0.7% meet or approach it;
  SENSEX ≈0.3–0.5% does not. Realistic expectation is ~0.5–0.7%.
- **Champion / challenger:** v2 (`frozen.json`) is the certified champion.
  `candidate_v3.json` (sell 0.6σ in confirmed trends) passed TRAIN but **underperformed v2 on TEST**
  (+25.8% vs +30.9%, RANGE negative, DD doubled), so it was rejected.
  - A challenger runs in shadow only: `OPIFY_RULES=candidate_v3 python monitor.py ... --book shadow_v3`.
  - Promote a challenger only if it beats the champion on NEW data, with the same criteria.
- **Open flags for the next review** (do not act mid-test):
  - UP months earn only ~1/10 of NIFTY.
  - 64% of P&L came from DOWN months.
  - RANGE was negative 3 months running (2025-11, 2026-02, 2026-05), and 2026-10 was −4.2%.
  - Whipsaws around EMA20 and expiry-morning gaps cause most stop-outs.
  - Candidate concepts to research on TRAIN: larger size in strong confirmed uptrends, and a
    calendar (vega+) structure for range months.

## 7. Risk limits (hard)

- ₹26L own funds, three sleeves, one spread per sleeve. Every short has a wing; margin ≤ 50% of
  sleeve cash.
- Gaps overshoot the 30-min stop (worst seen: 8.5× credit). The wing is the real cap, so size as if
  a full wing loss can happen.
- If the rolling win rate falls below 64% over 40+ trades, or a regime turns negative over 3+
  months, flag it for review. Do not change rules mid-test.
