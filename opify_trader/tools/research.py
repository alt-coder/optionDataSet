"""Parameter research on the TRAINING window only (2023-01-01 .. 2025-12-31).

Selection is for robustness, not peak return: a config is scored by its WORST calendar year
(min yearly Calmar-like ratio), with a penalty for few trades. 2026 data is never touched here.

    python research.py cache              # convert all raw files to npz (one-off)
    python research.py search 300         # random search, writes results/search.csv
"""
import csv
import datetime as dt
import itertools
import json
import os
import random
import sys
from multiprocessing import Pool

import numpy as np

TRAIN = (dt.date(2023, 1, 1), dt.date(2025, 12, 31))
CAPITAL = 2_600_000
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results")

SPACE = dict(
    enter_dte=[1, 2, 3, 4, 5],
    entry=["09:30", "11:00", "15:00"],
    short_off=[2, 3, 4, 6, 8],
    wing=[3, 4, 6, 8],
    skew_by_trend=[0, 1],
    fifth_leg=[False, True],
    tp_frac=[0.3, 0.5, 0.7, 0.9],
    sl_frac=[0.75, 1.0, 1.5, 2.0],
    buffer_pts=[0, 30, 60, 100],
    max_adj=[0, 1, 2],
    adj_mode=["roll_untested", "ironfly", "roll_tested"],
    alloc=[0.15, 0.25, 0.4, 0.6],
    delta_cap=[1.0],
)

_data = None


def _init():
    global _data
    from sim import Data
    _data = Data(*TRAIN)


def evaluate(p):
    from sim import metrics, yearly
    from strategies import run
    b = run("adaptive", p, _data, *TRAIN, capital=CAPITAL)
    m = metrics(b.equity, CAPITAL, b.trades)
    ys = yearly(b.equity)
    # per-year drawdown
    eq = np.array([e for _, e in b.equity])
    yrs = np.array([int(d[:4]) for d, _ in b.equity])
    worst = 1e9
    for y in sorted(set(yrs)):
        e = eq[yrs == y]
        dd = (e / np.maximum.accumulate(e) - 1).min()
        r = ys[y] / 100
        worst = min(worst, r / max(-dd, 0.03))
    score = worst if m.get("trades", 0) >= 60 else -9
    return {**{k: p[k] for k in SPACE}, **{k: float(v) if v is not None else None for k, v in m.items()},
            **{f"y{y}": v for y, v in ys.items()}, "score": round(float(worst), 3) if score != -9 else -9}


def cache():
    from sim import Data
    for span in [TRAIN, (dt.date(2026, 1, 1), dt.date(2026, 12, 31))]:
        d = Data(*span)
        d.spot_daily()
        print(span, len(d.dates), "days cached")


def search(n, seed=7):
    rnd = random.Random(seed)
    keys = list(SPACE)
    seen, configs = set(), []
    while len(configs) < n:
        p = {k: rnd.choice(SPACE[k]) for k in keys}
        if p["max_adj"] == 0:
            p["adj_mode"] = "roll_untested"
        key = json.dumps(p, sort_keys=True)
        if key not in seen:
            seen.add(key)
            configs.append(p)
    os.makedirs(OUT, exist_ok=True)
    with Pool(8, initializer=_init) as pool:
        rows = []
        for i, r in enumerate(pool.imap_unordered(evaluate, configs)):
            rows.append(r)
            if i % 25 == 0:
                print(i, file=sys.stderr)
    rows.sort(key=lambda r: -r["score"])
    path = os.path.join(OUT, "search.csv")
    with open(path, "w", newline="") as f:
        cols = sorted({k for r in rows for k in r}, key=lambda k: (k not in SPACE, k))
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    return rows


if __name__ == "__main__":
    if sys.argv[1] == "cache":
        cache()
    else:
        rows = search(int(sys.argv[2]))
        for r in rows[:15]:
            print({k: r[k] for k in ["score", "cagr%", "maxdd%", "sharpe", "trades", "win%"] + [k for k in r if k.startswith("y20")]},
                  {k: r[k] for k in SPACE})
