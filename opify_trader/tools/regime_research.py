"""Regime-balanced re-learning on TRAIN months only (params/split.json).

Pre-declared selection rule (written before results):
  1. every regime (UP/DOWN/RANGE) must have mean monthly return > 0 on TRAIN months
  2. its plateau neighbours (one parameter moved one notch) must mostly satisfy rule 1 too
  3. among survivors, pick the best WORST-regime mean monthly return; ties -> simpler rule (fewer features)
Monthly return = net P&L of trades ENTERED that month / capital. NIFTY month return from split.json.

    python regime_research.py
"""
import datetime as dt
import json
import os
import sys
from collections import defaultdict
from multiprocessing import Pool

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
SPLIT = json.load(open(os.path.join(HERE, "..", "params", "split.json")))
CAPITAL = 2_600_000
SPAN = (dt.date(2023, 1, 1), dt.date(2025, 12, 31))
SLEEVES = (1, 2, 3)

_d = None


def _init():
    global _d
    from sim import Data
    _d = Data(*SPAN)


def _sleeve(args):
    from strategies import run
    p, dte = args
    b = run("trend_spread", {**p, "enter_dte": dte}, _d, *SPAN, capital=CAPITAL / 3)
    return [(t.open_date[:7], t.net()) for t in b.trades]


def trade_stats(p, pool, which="TRAIN"):
    """Per-trade expectancy by regime with t-stat (more data than monthly means)."""
    months = [m for m in SPLIT["months"] if m["set"] == which and m["month"] <= "2025-12"]
    reg = {m["month"]: m["regime"] for m in months}
    res = pool.map(_sleeve, [({**p, "months": list(reg)}, s) for s in SLEEVES])
    by = defaultdict(list)
    for r in res:
        for mth, net in r:
            by[reg[mth]].append(net)
    out = {}
    for k in ("UP", "DOWN", "RANGE"):
        x = np.array(by[k]) if by[k] else np.zeros(1)
        out[k] = {"n": len(by[k]), "avg": round(float(x.mean())), "t": round(float(x.mean() / (x.std(ddof=1) / np.sqrt(len(x)))) if len(x) > 1 else 0, 2),
                  "win%": round(100 * float((x > 0).mean()), 1)}
    return out


def _sleeve_full(args):
    from strategies import run
    p, dte = args
    b = run("trend_spread", {**p, "enter_dte": dte}, _d, *SPAN, capital=CAPITAL / 3)
    return [(t.expiry, t.open_date, t.net()) for t in b.trades]


def regime_stats(p, pool, which="TRAIN"):
    months = [m for m in SPLIT["months"] if m["set"] == which and m["month"] <= "2025-12"]
    p = {**p, "months": [m["month"] for m in months]}
    res = pool.map(_sleeve, [(p, s) for s in SLEEVES])
    pnl = defaultdict(float)
    ntr = defaultdict(int)
    for r in res:
        for mth, net in r:
            pnl[mth] += net
            ntr[mth] += 1
    out = {}
    for reg in ("UP", "DOWN", "RANGE"):
        ms = [m for m in months if m["regime"] == reg]
        sr = [pnl[m["month"]] / CAPITAL * 100 for m in ms]
        nr = [m["ret%"] for m in ms]
        out[reg] = {"months": len(ms), "strat_mean%": round(float(np.mean(sr)), 2), "nifty_mean%": round(float(np.mean(nr)), 2),
                    "pos_months%": round(100 * float(np.mean([x > 0 for x in sr])), 0), "worst%": round(float(min(sr)), 2),
                    "trades": sum(ntr[m["month"]] for m in ms)}
    allm = [pnl[m["month"]] / CAPITAL * 100 for m in months]
    out["ALL"] = {"months": len(months), "strat_mean%": round(float(np.mean(allm)), 2),
                  "nifty_mean%": round(float(np.mean([m["ret%"] for m in months])), 2),
                  "pos_months%": round(100 * float(np.mean([x > 0 for x in allm])), 0), "worst%": round(float(min(allm)), 2)}
    return out


V1 = dict(mode="concept", entry="15:00", exit="15:15", ema=20, k_sigma=0.8, wing_sigma=0.8, tp_x=0.8, sl_x=1.0,
          alloc=0.5, vrp_min=0, check_every=30)
NOTCH = {"k_sigma": [0.6, 1.0], "wing_sigma": [0.6, 1.0], "tp_x": [0.6, 0.9], "sl_x": [0.75, 1.5], "ema": [10, 30]}


def passes(s):
    return all(s[r]["strat_mean%"] > 0 for r in ("UP", "DOWN", "RANGE"))


def worst(s):
    return min(s[r]["strat_mean%"] for r in ("UP", "DOWN", "RANGE"))


def main_trades():
    cands = {"v1": V1, "v1+slope5": {**V1, "slope_confirm": 5}, "v1+er0.3": {**V1, "er_range": 0.3, "range_k": 1.0},
             "v1+er0.3+slope5": {**V1, "er_range": 0.3, "range_k": 1.0, "slope_confirm": 5}}
    with Pool(3, initializer=_init) as pool:
        for name, p in cands.items():
            ts = trade_stats(p, pool)
            ms = regime_stats(p, pool)
            nb = []
            for k, vals in {**NOTCH, **({"er_range": [0.2, 0.4]} if "er_range" in p else {}), **({"slope_confirm": [3, 10]} if "slope_confirm" in p else {})}.items():
                for v in vals:
                    t2 = trade_stats({**p, k: v}, pool)
                    nb.append(all(t2[r]["avg"] > 0 for r in ("UP", "DOWN", "RANGE")))
            print(name, "per-trade", json.dumps(ts), "| monthly mean%", {r: ms[r]["strat_mean%"] for r in ms},
                  "| all-regime-positive plateau rate", round(sum(nb) / len(nb), 2), flush=True)


if __name__ == "__main__" and len(sys.argv) > 1 and sys.argv[1] == "trades":
    main_trades()
elif __name__ == "__main__":
    cands = {"v1": V1, "v1+band0.25": {**V1, "neutral_band": 0.25}, "v1+band0.5": {**V1, "neutral_band": 0.5}}
    results = {}
    with Pool(3, initializer=_init) as pool:
        for name, p in cands.items():
            s = regime_stats(p, pool)
            nb = []
            for k, vals in NOTCH.items():
                for v in vals:
                    nb.append(passes(regime_stats({**p, k: v}, pool)))
            results[name] = {"stats": s, "passes": passes(s), "worst_regime_mean%": worst(s),
                             "plateau_pass_rate": round(sum(nb) / len(nb), 2)}
            print(name, json.dumps(results[name]), flush=True)
    json.dump(results, open(os.path.join(HERE, "..", "results", "regime_train.json"), "w"), indent=1)
    ok = {n: r for n, r in results.items() if r["passes"] and r["plateau_pass_rate"] >= 0.7}
    order = ["v1", "v1+band0.25", "v1+band0.5"]  # simplicity order for ties
    if ok:
        best = max(ok, key=lambda n: (round(ok[n]["worst_regime_mean%"], 1), -order.index(n)))
        print("SELECTED", best)
    else:
        print("SELECTED none - no candidate profitable in every regime on TRAIN")
