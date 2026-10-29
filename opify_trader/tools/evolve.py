"""Walk-forward rule evolution (no lookahead).

At each re-learning date D (month start), candidates are evaluated ONLY on data < D
(trailing window, default 2023-01-01 .. D-1). The incumbent rule set is replaced only if a
challenger beats it on the trailing robustness score by `margin`. Each adoption is a new
version in params/history/ with the evidence, so the agent's rules visibly evolve with data.

    python evolve.py run 2026-01-01 2026-10-28      # evolving forward test over 2026
"""
import csv
import datetime as dt
import json
import os
import sys
from multiprocessing import Pool

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
PDIR = os.path.join(HERE, "..", "params")
HIST = os.path.join(PDIR, "history")
CAPITAL = 2_600_000
TRAIN_START = dt.date(2023, 1, 1)
MARGIN = 0.15          # challenger must beat incumbent's trailing score by this much
RECENT_W = 0.5         # weight of last 12 months in the trailing score (rules adapt to regime)

_data, _end = None, None


def _init(end):
    global _data, _end
    from sim import Data
    _end = end
    _data = Data(TRAIN_START, end)


def trailing_score(p):
    """Robust score on data < re-learning date: blend of worst-year ratio (all history) and last-12m ratio."""
    from sim import metrics
    from strategies import run
    b = run("adaptive", p, _data, TRAIN_START, _end, capital=CAPITAL)
    eq = np.array([e for _, e in b.equity])
    ds = [dt.date.fromisoformat(d) for d, _ in b.equity]

    def ratio(mask):
        e = eq[mask]
        if len(e) < 40:
            return 0.0
        r = e[-1] / e[0] - 1
        dd = (e / np.maximum.accumulate(e) - 1).min()
        return r / max(-dd, 0.03)

    yrs = np.array([d.year for d in ds])
    worst = min(ratio(yrs == y) for y in sorted(set(yrs)))
    recent = ratio(np.array([d > _end - dt.timedelta(365) for d in ds]))
    n = len(b.trades)
    s = (1 - RECENT_W) * worst + RECENT_W * recent if n >= 40 else -9
    m = metrics(b.equity, CAPITAL, b.trades)
    return {"score": round(float(s), 3), "worst_year": round(float(worst), 3), "recent": round(float(recent), 3), **m}


def candidates(incumbent, n=24, seed=0):
    """Incumbent + local mutations (one/two params) + top configs of the original search."""
    import random
    from research import SPACE
    rnd = random.Random(seed)
    out = [dict(incumbent)]
    keys = [k for k in SPACE if len(SPACE[k]) > 1]
    while len(out) < n:
        c = dict(incumbent)
        for k in rnd.sample(keys, rnd.choice([1, 2])):
            c[k] = rnd.choice(SPACE[k])
        if c not in out:
            out.append(c)
    return out


def relearn(incumbent, asof, seed):
    end = asof - dt.timedelta(1)
    cands = candidates(incumbent, seed=seed)
    with Pool(8, initializer=_init, initargs=(end,)) as pool:
        res = pool.map(trailing_score, cands)
    inc = res[0]
    best_i = int(np.argmax([r["score"] for r in res]))
    adopt = best_i != 0 and res[best_i]["score"] > inc["score"] + MARGIN
    return (cands[best_i] if adopt else incumbent), adopt, inc, res[best_i], cands[best_i]


def run(start, end):
    from sim import Data, benchmark, metrics, yearly
    from strategies import run as run_strat
    os.makedirs(HIST, exist_ok=True)
    rules = json.load(open(os.path.join(PDIR, "frozen.json")))
    rules = {k: v for k, v in rules.items() if not k.startswith("_")}
    data = Data(start, end)
    # month starts inside the window
    months, d = [], dt.date(start.year, start.month, 1)
    while d <= end:
        months.append(max(d, start))
        d = dt.date(d.year + (d.month == 12), d.month % 12 + 1, 1)
    from engine import Book
    master = Book("evolving", CAPITAL)
    version = 1
    log = []
    for i, ms in enumerate(months):
        me = (months[i + 1] - dt.timedelta(1)) if i + 1 < len(months) else end
        if i > 0:
            new, adopt, inc, best, cand = relearn(rules, ms, seed=i)
            log.append({"asof": ms.isoformat(), "adopted": adopt, "incumbent_score": inc["score"],
                        "challenger_score": best["score"], "challenger": cand})
            if adopt:
                version += 1
                rules = new
                json.dump({"version": version, "asof": ms.isoformat(), "rules": rules, "evidence": best},
                          open(os.path.join(HIST, f"v{version:02d}_{ms}.json"), "w"), indent=1, default=str)
            print(ms, "adopt" if adopt else "keep", inc["score"], "->", best["score"], file=sys.stderr)
        # trade this month with the current rules; positions opened in the month may close after it
        b = run_strat("adaptive", rules, data, ms, me, capital=master.cash)
        for t in b.trades:
            t.notes += f" rules=v{version}"
            master.close(t)
        for dd_, e in b.equity:
            master.equity.append((dd_, e))
    folder = os.path.join(HERE, "..", "books", "forward_2026_evolving")
    master.save(folder)
    json.dump(log, open(os.path.join(folder, "evolution_log.json"), "w"), indent=1, default=str)
    bench = benchmark(data, start, end, CAPITAL)
    res = {"strategy": metrics(master.equity, CAPITAL, master.trades), "strategy_monthly_yearly": yearly(master.equity),
           "nifty": metrics(bench, CAPITAL), "versions_used": version}
    json.dump(res, open(os.path.join(folder, "summary.json"), "w"), indent=1, default=str)
    return res


if __name__ == "__main__":
    s, e = dt.date.fromisoformat(sys.argv[2]), dt.date.fromisoformat(sys.argv[3])
    print(json.dumps(run(s, e), indent=1, default=str))
