# opify_trader: Opify option-simulator tools + trading skill

Research code, frozen rules and the agent skill built on the Opify option simulator
(https://www.opify.in/simulator/dashboard?version=web). The 1-minute data it uses sits next to this folder in
`../opify_simulator_data/` (same branch).

Not investment advice. Paper-trading research only.

## What is here
| Path | What |
|---|---|
| `skill/SKILL.md` | The **opify-options-trader** skill. An AI agent reads it to trade and monitor the strategy, use the tools below, and drive the Opify simulator in Chrome |
| `tools/opify_ui.js` | **Opify simulator controller** for Claude-in-Chrome (v1.4): `setDateTime`, `setExpiry`, `allStrikes`, `chain`, `order`, `setQty`, `exit`, `startGoto` + `job`, `clearBook`, `state`, `positions` |
| `tools/serve_ui.py` | Serves `opify_ui.js` on 127.0.0.1:8765 so the page can load it in one line |
| `tools/opify_data.py` | Client for the public data endpoint the simulator uses (expiry discovery, day download, gzip JSON) |
| `tools/engine.py`, `sim.py`, `strategies.py`, `adaptive.py`, `costs.py` | Backtest engine: 1-min fills at next-bar open + slippage, the dated Indian F&O cost schedule (STT/GST/fees), trade books |
| `tools/monitor.py` | Agent-facing monitor: `rules`, `latest`, `status`, `tick`, `next`, `run`, `report`, `note` (frozen rules, JSON output, local paper books). The status view includes Greeks, the learned expected value of holding, and WAIT/SELL_OFF decisions |
| `tools/ensemble.py`, `portfolio_sim.py`, `multi_asset.py` | Forward test, 3-index no-leverage portfolio, portability to SENSEX/BANKNIFTY |
| `tools/lookahead_test.py` | Future-corruption test (must print NO LOOKAHEAD DETECTED) |
| `tools/regimes.py`, `regime_research.py`, `vol_study.py`, `greek_study.py`, `setup_study.py`, `business_case.py` | Research: regime split, volatility study, Greek and hold-value study, per-setup returns, business case |
| `params/frozen.json` + `.sha256` | **v2 champion rules** (hash-locked; tools refuse to run if modified). `candidate_*.json` are challengers. `history/` holds v1. `split.json` is the regime-balanced TRAIN/TEST months |
| `results/` | Study outputs (vol study, Greek study, portfolio sims, business case, multi-asset) |
| `evidence/` | Opify mirror proof: 6 TEST trades replayed in the simulator, realised P&L matched the prediction to within Rs 0.50 |
| `TOOLS_CHANGELOG.md` | Every tool fix with its reason (strike-window valuation, lookahead fix, …) |

## Set up in a new session
1. **Python 3.10+** with `numpy pandas scipy`.
2. **Data:** the tools expect `opify_trader/data/` laid out as
   `data/raw/<SYMBOL>/<EXPIRY>/<date>.json.gz` plus `data/expiries_<SYMBOL>.json` and `data/spot_daily*.json`.
   Link or copy it from this branch:
   ```bash
   mkdir -p data && cp -r ../opify_simulator_data/raw data/ && cp ../opify_simulator_data/meta/expiries_*.json ../opify_simulator_data/meta/spot_daily*.json data/
   ```
   Or download fresh: `python tools/opify_data.py expiries 2023-01-01 2026-12-31 --symbol NIFTY` then
   `python tools/opify_data.py download 2023-01-01 2026-12-31 --days 6 --symbol NIFTY`.
   The `.npz` caches are rebuilt automatically on first use.
3. **Skill (Claude Code):** copy `skill/SKILL.md` to `~/.claude/skills/opify-options-trader/SKILL.md`.
   The skill was written on Windows with the project at `C:\Users\atech\opify-trader`; read its paths as relative
   to this folder.
4. **Check:** `cd tools && python monitor.py rules` (hash check), then `python lookahead_test.py 40`.

## Drive the Opify simulator (Claude in Chrome)
```text
python tools/serve_ui.py            # background; serves only opify_ui.js on 127.0.0.1:8765
# in the Opify tab (javascript tool):
eval(await (await fetch('http://127.0.0.1:8765/opify_ui.js')).text())
await opifyUI.clearBook()                              // the date can't change while legs are on the book
await opifyUI.setDateTime('08-01-2026','15:00')        // if "date picker not found": real-click Change Date, then Select datetime
await opifyUI.setExpiry('13JAN2026'); await opifyUI.allStrikes()
await opifyUI.order('CE','sell',26100,'13JAN2026',780) // qty in units; one leg per strike (a 2nd click raises a blocking alert)
opifyUI.startGoto('09-01-2026','12:46'); opifyUI.job()  // long moves as a polled job (tool calls time out at 45 s)
opifyUI.state(); await opifyUI.exit('13JAN2026',26100,'CE')
```
The UI at HH:MM shows the close of the HH:MM-1 bar; UI prices equal the data API's prices exactly.

## Headline results (details in `skill/SKILL.md`)
- **v2, 3-index no-leverage portfolio** on regime-balanced TEST months: 23.4%/yr pre-tax, 16.1% post-tax,
  max DD −5.1%. NIFTY +8.6% over the same months.
- **Full 2023-01 to 2026-10 scorecard** (partly in-sample, an estimate): v2 22.0%/yr pre-tax, DD −9%; v4 25.2%/yr.
- **Weaknesses:** small gains in strong UP months, SENSEX weakest, gap risk through stops.
- **Live paper forward test** started 2026-10-29; only that can confirm the edge.
