"""Real 3-index no-leverage portfolio simulation (TEST months, frozen v2 rules, NO re-tuning).

Capital Rs 26L split 1/3 per index (NIFTY, SENSEX, BANKNIFTY), each index's third split over its 3 sleeves.
alloc = 1.0 inside each third -> margin of the whole book <= Rs 26L (no borrowing). SENSEX has no data before
Apr-2024: its third sits in cash until then (conservative). Daily realised equity -> true max drawdown.

    python portfolio_sim.py
"""
import json
import os
import subprocess
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
CAP = 2_600_000
ALLOC = float(os.environ.get("PORT_ALLOC", "1.0"))


def run(sym):
    code = f"""
import datetime as dt, json, os
from sim import Data
from strategies import run
from ensemble import frozen
p, sleeves, h = frozen()
sp = json.load(open(os.path.join('..','params','split.json')))
p = {{**p, 'alloc': {ALLOC}, 'months': [m['month'] for m in sp['months'] if '{os.environ.get("PORT_SET","TEST")}'=='ALL' or m['set']=='{os.environ.get("PORT_SET","TEST")}']}}
S, E = dt.date(2023,1,1), dt.date(2026,10,28)
d = Data(S, E)
eq = {{}}; trades = []
for dte in sleeves:
    b = run('trend_spread', {{**p, 'enter_dte': dte}}, d, S, E, capital={CAP}/3/3)
    for day, e in b.equity: eq[day] = eq.get(day, 0) + e - {CAP}/3/3
    trades += [(t.open_date, t.close_date, t.expiry, round(t.net(),2), t.margin) for t in b.trades]
print('JSON' + json.dumps({{'eq': eq, 'trades': trades}}))
"""
    r = subprocess.run([sys.executable, "-c", code], cwd=HERE, env={**os.environ, "OPIFY_SYMBOL": sym}, capture_output=True, text=True)
    line = [l for l in r.stdout.splitlines() if l.startswith("JSON")]
    if not line:
        raise SystemExit(sym + r.stderr[-1500:])
    return json.loads(line[0][4:])


def main():
    res = {s: run(s) for s in ("NIFTY", "SENSEX", "BANKNIFTY")}
    days = sorted(set().union(*[r["eq"] for r in res.values()]))
    last = {s: 0.0 for s in res}
    curve = []
    for d in days:
        for s in res:
            if d in res[s]["eq"]:
                last[s] = res[s]["eq"][d]
        curve.append((d, CAP + sum(last.values())))
    eq = np.array([e for _, e in curve])
    peak = np.maximum.accumulate(eq)
    dd = (eq / peak - 1).min()
    sp = json.load(open(os.path.join(HERE, "..", "params", "split.json")))
    SET = os.environ.get("PORT_SET", "TEST")
    test = [m["month"] for m in sp["months"] if SET == "ALL" or m["set"] == SET]
    k = len(test)
    tot = eq[-1] / CAP - 1
    ann = (1 + tot) ** (12 / k) - 1
    trades = [t for r in res.values() for t in r["trades"]]
    nets = np.array([t[3] for t in trades])
    exps = {(s, t[2]) for s, r in res.items() for t in r["trades"]}
    # per-symbol contribution and monthly realised returns by entry month
    by_sym = {s: round(sum(t[3] for t in r["trades"])) for s, r in res.items()}
    mon = {}
    for s, r in res.items():
        for t in r["trades"]:
            mon[t[0][:7]] = mon.get(t[0][:7], 0) + t[3]
    reg = {}
    for m in sp["months"]:
        if SET == "ALL" or m["set"] == SET:
            reg.setdefault(m["regime"], []).append(mon.get(m["month"], 0) / CAP * 100)
    nifty = np.prod([1 + m["ret%"] / 100 for m in sp["months"] if SET == "ALL" or m["set"] == SET]) - 1
    import costs
    out = {"alloc_within_each_third": ALLOC, "test_months": k, "total_return%": round(tot * 100, 1),
           "annualised_pre_tax%": round(ann * 100, 1), "annualised_post_tax%": round(ann * (1 - costs.FNO_TAX) * 100, 1),
           "nifty_same_months_total%": round(nifty * 100, 1), "max_drawdown_daily%": round(dd * 100, 1),
           "trades": len(nets), "win%": round(100 * (nets > 0).mean(), 1), "expiries_traded": len(exps),
           "avg_net_per_expiry_%_of_capital": round(nets.sum() / len(exps) / CAP * 100, 3),
           "net_by_index": by_sym, "max_single_trade_loss": round(nets.min()),
           "regime_mean%/mo (sum of entry-month P&L / 26L)": {r: round(sum(v) / len(v), 2) for r, v in reg.items()},
           "max_margin_one_trade": round(max(t[4] for t in trades)),
           "net_per_setup_on_margin%": round(float(np.mean([t[3] / t[4] for t in trades if t[4] > 0])) * 100, 3),
           "net_per_setup_on_margin_by_index%": {s: round(float(np.mean([t[3] / t[4] for t in r["trades"] if t[4] > 0])) * 100, 3) for s, r in res.items()},
           "rules": os.environ.get("OPIFY_RULES", "frozen")}
    json.dump(out, open(os.path.join(HERE, "..", "results", f"portfolio_sim_{os.environ.get('OPIFY_RULES','frozen')}_{os.environ.get('PORT_SET','TEST')}_alloc{ALLOC}.json"), "w"), indent=1)
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
