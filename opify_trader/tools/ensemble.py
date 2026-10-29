"""Three-sleeve trend credit spread: same rules entered at dte 1, 2 and 3, capital split equally.

Entry day is treated as noise (no concept explains why one day would be better), so instead of
picking the best-backtesting day we hold all three. Optional liquid-fund yield on capital.

    python ensemble.py sens            # sensitivity grid on TRAIN only
"""
import datetime as dt
import itertools
import json
import os
import sys
from multiprocessing import Pool

import numpy as np

CAPITAL = 2_600_000
TRAIN = (dt.date(2023, 1, 1), dt.date(2025, 12, 31))
SLEEVES = (1, 2, 3)
LIQUID_YIELD = 0.06   # pledged LIQUIDBEES / liquid fund, net of expenses (no borrowing involved)

_d = None


def _init(span):
    global _d
    from sim import Data
    _d = Data(*span)


def sleeve(args):
    from strategies import run
    p, dte, span, cap = args
    b = run("trend_spread", {**p, "enter_dte": dte}, _d, *span, capital=cap, book_name=f"sleeve_dte{dte}")
    return dte, b


def combine(books, liquid=False):
    dates = [d for d, _ in books[0].equity]
    eq = np.zeros(len(dates))
    for b in books:
        eq += np.array([e for _, e in b.equity])
    if liquid:
        days = np.array([(dt.date.fromisoformat(d) - dt.date.fromisoformat(dates[0])).days for d in dates])
        eq = eq + CAPITAL * ((1 + LIQUID_YIELD) ** (days / 365.25) - 1)
    return list(zip(dates, eq.tolist()))


def evaluate(p, span, pool):
    res = pool.map(sleeve, [(p, d, span, CAPITAL / len(SLEEVES)) for d in SLEEVES])
    books = [b for _, b in sorted(res, key=lambda x: x[0])]
    return books


def summary(books, span):
    from sim import metrics, yearly
    trades = [t for b in books for t in b.trades]
    eq = combine(books)
    eql = combine(books, liquid=True)
    return {"options_only": {**metrics(eq, CAPITAL, trades), "yearly": yearly(eq)},
            "with_liquid_yield": {**metrics(eql, CAPITAL), "yearly": yearly(eql)}}


BASE = dict(mode="concept", k_sigma=0.8, wing_sigma=0.8, tp_x=0.8, sl_x=1.0, alloc=0.5, entry="15:00", ema=20, vrp_min=0)

def frozen():
    import hashlib
    root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "params")
    name = os.environ.get("OPIFY_RULES", "frozen")  # champion = frozen (v2); challenger = candidate_v3
    raw = open(os.path.join(root, name + ".json"), "rb").read()
    want = open(os.path.join(root, name + ".sha256")).read().split()[0]
    got = hashlib.sha256(raw).hexdigest()
    if got != want:
        raise SystemExit(f"{name}.json hash mismatch: {got} != {want} (rules were modified)")
    j = json.loads(raw)
    p = {k: v for k, v in j.items() if not k.startswith("_") and k not in ("sleeves_dte", "capital")}
    return p, j["sleeves_dte"], got


def forward(start, end, tag, month_set=None):
    import costs
    from sim import benchmark, metrics, yearly
    p, sleeves, h = frozen()
    if month_set:
        # trade only months of one side of the regime-balanced split (params/split.json); equity is flat elsewhere
        sp = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "params", "split.json")))
        p = {**p, "months": [m["month"] for m in sp["months"] if m["set"] == month_set]}
    span = (start, end)
    with Pool(3, initializer=_init, initargs=(span,)) as pool:
        res = pool.map(sleeve, [(p, d, span, CAPITAL / len(sleeves)) for d in sleeves])
    books = [b for _, b in sorted(res, key=lambda x: x[0])]
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "books", tag)
    for b in books:
        b.save(os.path.join(out, b.name))
    trades = [t for b in books for t in b.trades]
    eq, eql = combine(books), combine(books, liquid=True)
    _init(span)
    bench = benchmark(_d, start, end, CAPITAL)
    pnl_year = {}
    for t in trades:
        pnl_year[int(t.close_date[:4])] = pnl_year.get(int(t.close_date[:4]), 0) + t.net()
    tax = costs.fno_tax(pnl_year)
    bgain = bench[-1][1] - CAPITAL
    btax = max(0.0, bgain) * 0.20 if (end - start).days <= 365 else max(0.0, bgain - costs.LTCG_EXEMPT) * costs.LTCG_TAX
    fees = {}
    for t in trades:
        for l in t.legs:
            fees["total"] = fees.get("total", 0) + l.charges
    s = {
        "window": f"{start}..{end}", "frozen_sha256": h, "params": p, "sleeves": sleeves,
        "options_only": {**metrics(eq, CAPITAL, trades), "yearly": yearly(eq)},
        "with_liquid_yield_6pct": {**metrics(eql, CAPITAL), "yearly": yearly(eql)},
        "nifty_buy_hold": {**metrics(bench, CAPITAL), "yearly": yearly(bench)},
        "post_tax": {"options_only_final": round(eq[-1][1] - sum(tax.values())),
                     "fno_tax": {k: round(v) for k, v in tax.items()},
                     "nifty_final_after_cg_tax": round(bench[-1][1] - btax)},
        "max_margin_one_trade": round(max((t.margin for t in trades), default=0)),
        "total_charges_incl_gst_stt": round(fees.get("total", 0)),
        "skipped_days": sum(len(b.skipped) for b in books),
    }
    with open(os.path.join(out, "combined_equity.csv"), "w") as f:
        f.write("date,options_only,with_liquid,nifty_buy_hold\n")
        for (d, a), (_, b2), (_, c) in zip(eq, eql, bench):
            f.write(f"{d},{a:.0f},{b2:.0f},{c:.0f}\n")
    json.dump(s, open(os.path.join(out, "summary.json"), "w"), indent=1, default=str)
    return s


if __name__ == "__main__":
    if sys.argv[1] == "forward":
        s = forward(dt.date.fromisoformat(sys.argv[2]), dt.date.fromisoformat(sys.argv[3]), sys.argv[4],
                    sys.argv[5] if len(sys.argv) > 5 else None)
        print(json.dumps(s, indent=1, default=str))
    elif sys.argv[1] == "sens":
        grid = [dict(BASE)]
        for k, vals in {"k_sigma": [0.6, 1.0], "wing_sigma": [0.6, 1.0], "tp_x": [0.6, 0.9], "sl_x": [0.75, 1.5],
                        "vrp_min": [1.0], "ema": [10, 50], "entry": ["11:00"]}.items():
            for v in vals:
                grid.append({**BASE, k: v})
        with Pool(3, initializer=_init, initargs=(TRAIN,)) as pool:
            for p in grid:
                s = summary(evaluate(p, TRAIN, pool), TRAIN)
                diff = {k: v for k, v in p.items() if BASE.get(k) != v} or "BASE"
                o, l = s["options_only"], s["with_liquid_yield"]
                print(json.dumps({"var": diff, "cagr": o["cagr%"], "dd": o["maxdd%"], "sharpe": o["sharpe"], "yearly": o["yearly"],
                                  "cagr+liq": l["cagr%"], "dd+liq": l["maxdd%"]}, default=str), flush=True)
