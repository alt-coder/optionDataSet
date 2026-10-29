"""Portability test: frozen v2 rules, UNCHANGED, on SENSEX and BANKNIFTY (Opify data).

For each symbol and each split set (TRAIN / TEST) report per-expiry and per-regime results vs that
index's own buy & hold over the same months. Regime labels are NIFTY's (same market).
Runs one subprocess per symbol so the engine picks up OPIFY_SYMBOL.

    python multi_asset.py
"""
import json
import os
import subprocess
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
SPLIT = json.load(open(os.path.join(HERE, "..", "params", "split.json")))
CAP = 2_600_000


def run_symbol(sym):
    code = f"""
import datetime as dt, json, csv, glob, os
import ensemble as E
out = {{}}
for st in ("TRAIN", "TEST"):
    tag = "ma_{sym}_" + st
    s = E.forward(dt.date(2023,1,1), dt.date(2026,10,28), tag, st)
    trades = []
    for f in glob.glob(os.path.join("..","books",tag,"sleeve_dte*","trades.csv")):
        trades += [(t["open_date"], t["expiry"], float(t["net"])) for t in csv.DictReader(open(f))]
    from sim import Data
    d = Data(dt.date(2023,1,1), dt.date(2026,10,28)); sd = d.spot_daily()
    mc = {{}}
    for x in d.dates: mc[x.strftime("%Y-%m")] = sd[x][3]
    out[st] = {{"trades": trades, "month_close": mc, "first": d.dates[0].isoformat()}}
print("JSON" + json.dumps(out))
"""
    env = {**os.environ, "OPIFY_SYMBOL": sym}
    r = subprocess.run([sys.executable, "-c", code], cwd=HERE, env=env, capture_output=True, text=True)
    line = [l for l in r.stdout.splitlines() if l.startswith("JSON")]
    if not line:
        raise SystemExit(r.stderr[-2000:])
    return json.loads(line[0][4:])


def analyse(sym, res):
    rows = {}
    for st in ("TRAIN", "TEST"):
        months = [m for m in SPLIT["months"] if m["set"] == st]
        mc = res[st]["month_close"]
        keys = sorted(mc)
        pnl = defaultdict(float)
        per_exp = defaultdict(float)
        for od, exp, net in res[st]["trades"]:
            pnl[od[:7]] += net
            per_exp[exp] += net
        eq, reg, ny, comp_i = CAP, defaultdict(list), defaultdict(list), 1.0
        used = 0
        for m in months:
            mm = m["month"]
            if mm not in mc or keys.index(mm) == 0:
                continue  # symbol has no data for that month (SENSEX starts Apr-2024)
            used += 1
            r = pnl[mm] / eq
            eq *= 1 + r
            idx_r = mc[mm] / mc[keys[keys.index(mm) - 1]] - 1
            comp_i *= 1 + idx_r
            reg[m["regime"]].append(r * 100)
            ny[m["regime"]].append(idx_r * 100)
        pe = [v / CAP * 100 for v in per_exp.values()]
        rows[st] = {"months": used, "trades": len(res[st]["trades"]), "strategy_comp%": round((eq / CAP - 1) * 100, 1),
                    "index_bh_comp%": round((comp_i - 1) * 100, 1),
                    "mean%/expiry": round(sum(pe) / len(pe), 3) if pe else None,
                    "regime_strat%/mo": {k: round(sum(v) / len(v), 2) for k, v in reg.items()},
                    "regime_index%/mo": {k: round(sum(v) / len(v), 2) for k, v in ny.items()}}
    return rows


if __name__ == "__main__":
    out = {}
    for sym in ("SENSEX", "BANKNIFTY"):
        out[sym] = analyse(sym, run_symbol(sym))
        print(sym, json.dumps(out[sym], indent=1), flush=True)
    json.dump(out, open(os.path.join(HERE, "..", "results", "multi_asset_v2.json"), "w"), indent=1)
