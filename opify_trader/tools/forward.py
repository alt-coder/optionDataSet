"""Forward test (out-of-sample) with FROZEN parameters + benchmark comparison + tax + trade books.

    python forward.py params.json 2026-01-01 2026-10-28 forward_2026
"""
import datetime as dt
import json
import os
import sys

import costs
from sim import Data, benchmark, metrics, yearly
from strategies import run

CAPITAL = 2_600_000
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "books")


def report(params, start, end, tag, strat="adaptive"):
    data = Data(start, end)
    book = run(strat, params, data, start, end, capital=CAPITAL, book_name=tag)
    folder = os.path.join(OUT, tag)
    book.save(folder)
    with open(os.path.join(folder, "adjustment_log.txt"), "w") as f:
        for i, t in enumerate(book.trades):
            f.write(f"# trade {i} {t.open_date} -> {t.close_date} net {t.net():.0f} {t.notes}\n")
            for line in getattr(t, "log", []):
                f.write("   " + line + "\n")
    m = metrics(book.equity, CAPITAL, book.trades)
    bench = benchmark(data, start, end, CAPITAL)
    bm = metrics(bench, CAPITAL)
    # taxes: strategy = business income @31.2% (yearly, loss carry-forward); bench = LTCG/STCG approx
    yp = {}
    for t in book.trades:
        y = int(t.close_date[:4])
        yp[y] = yp.get(y, 0) + t.net()
    tax = costs.fno_tax(yp)
    strat_post = book.equity[-1][1] - sum(tax.values())
    bgain = bench[-1][1] - CAPITAL
    held_days = (end - start).days
    btax = max(0, bgain - costs.LTCG_EXEMPT) * costs.LTCG_TAX if held_days > 365 else max(0, bgain) * 0.20
    bench_post = bench[-1][1] - btax
    fee_types = {}
    for t in book.trades:
        for l in t.legs:
            fee_types["total"] = fee_types.get("total", 0) + l.charges
    res = {
        "window": f"{start}..{end}", "params": params,
        "strategy": m, "strategy_yearly%": yearly(book.equity),
        "nifty_buy_hold": bm, "nifty_yearly%": yearly(bench),
        "strategy_post_tax_final": round(strat_post), "strategy_tax": {k: round(v) for k, v in tax.items()},
        "nifty_post_tax_final": round(bench_post),
        "max_margin_used": round(max((t.margin for t in book.trades), default=0)),
        "skipped_days": book.skipped[:5],
    }
    json.dump(res, open(os.path.join(folder, "summary.json"), "w"), indent=1, default=str)
    return res, book


if __name__ == "__main__":
    params = json.load(open(sys.argv[1]))
    s, e = dt.date.fromisoformat(sys.argv[2]), dt.date.fromisoformat(sys.argv[3])
    res, _ = report(params, s, e, sys.argv[4], params.pop("_strategy", "adaptive") if "_strategy" in params else "adaptive")
    print(json.dumps(res, indent=1, default=str))
