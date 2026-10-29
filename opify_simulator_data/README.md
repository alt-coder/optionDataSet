# Opify option simulator data (NIFTY, SENSEX, BANKNIFTY), 1-minute

Historical 1-minute index option chains and spot data, downloaded from the public data endpoint used by the
Opify option simulator (https://www.opify.in/simulator/dashboard). Collected for the
NIFTY trend-credit-spread research project (frozen rules v2, independent exam).

## Coverage
| Symbol | Day files | Expiries | First day | Last day |
|---|---|---|---|---|
| NIFTY | 1234 | 202 | 2022-11-28 | 2026-10-28 |
| SENSEX | 820 | 137 | 2024-03-28 | 2026-10-28 |
| BANKNIFTY | 737 | 123 | 2022-12-29 | 2026-10-28 |

For each expiry, the **last 6 trading days before (and including) expiry** are stored, which covers weekly-option trading.
SENSEX data starts Apr-2024. BANKNIFTY is weekly until Nov-2024 and monthly after that (SEBI one-weekly-expiry rule).

## Layout
```
raw/<SYMBOL>/<EXPIRY e.g. 13JAN2026>/<YYYY-MM-DD>.json.gz   one trading day for that expiry
meta/expiries_<SYMBOL>.json     {expiry: [trading days with data]}
meta/spot_daily*.json           {date: [open, high, low, close]} of the index (NIFTY file has no suffix)
meta/split.json                 month regime labels (UP/DOWN/RANGE) + TRAIN/TEST split used in the research
```

## Day file schema (gzip JSON)
```
meta:    symbol, expiry, lot_size, strike_step, prev_close, from/to (epoch s), tf="1min"
spot:    time_index[375 epoch s, 09:15..15:29 IST], ohlc[[o,h,l,c,vol,chg,chg%]]
strikes: strikes[K], ce_ohlc[K][375] = [o,h,l,c,volume,oi], pe_ohlc[K][375] likewise
```
Notes: about +-950 points of strikes around spot per day. Some bars contain '-' or null placeholders (treat them as missing). Older
expiries list their trading days as epoch seconds. Bar convention: the simulator UI at HH:MM shows the CLOSE of the HH:MM-1 bar.

## Load example
```python
import gzip, json
j = json.load(gzip.open("raw/NIFTY/13JAN2026/2026-01-08.json.gz"))
k = j["strikes"]["strikes"].index(26100)
print(j["strikes"]["ce_ohlc"][k][345])   # 26100 CE, 15:00 bar: [o,h,l,c,vol,oi]
```
The processed `.npz` caches are not included; they are regenerated automatically by the project's `engine.load()`.
