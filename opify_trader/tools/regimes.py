"""Label calendar months as UP / DOWN / RANGE from NIFTY's own path, and build a regime-balanced
TRAIN/TEST split (alternate months within each regime, chronological). Labels are used ONLY to split
the data; the strategy never sees them.

    python regimes.py      -> writes ../params/split.json
"""
import json, os, datetime as dt
from collections import defaultdict
import numpy as np
from opify_data import ROOT

UP, DOWN = 0.02, -0.02


def months():
    sd = json.load(open(os.path.join(ROOT, "spot_daily.json")))
    days = sorted(k for k in sd if "2023-01-01" <= k <= "2026-10-28")
    by = defaultdict(list)
    for k in days:
        by[k[:7]].append(sd[k][3])
    prev = None
    out = []
    for mth in sorted(by):
        c = by[mth]
        base = prev if prev is not None else c[0]
        r = c[-1] / base - 1
        path = np.abs(np.diff([base] + c)).sum()
        er = abs(c[-1] - base) / path if path else 0
        lab = "UP" if r > UP else "DOWN" if r < DOWN else "RANGE"
        out.append({"month": mth, "ret%": round(r * 100, 2), "eff_ratio": round(er, 2), "regime": lab, "days": len(c)})
        prev = c[-1]
    return out


def split(ms):
    """2023-2025: alternate months within each regime -> TRAIN/TEST. 2026: always TEST (mandatory forward test)."""
    seq = defaultdict(int)
    for m in ms:
        if m["month"] >= "2026-01":
            m["set"] = "TEST"
            continue
        i = seq[m["regime"]]
        m["set"] = "TRAIN" if i % 2 == 0 else "TEST"
        seq[m["regime"]] += 1
    return ms


if __name__ == "__main__":
    ms = split(months())
    for m in ms:
        print(m)
    summ = defaultdict(lambda: defaultdict(int))
    for m in ms:
        summ[m["set"]][m["regime"]] += 1
    print({k: dict(v) for k, v in summ.items()})
    json.dump({"rule": f"UP if month ret > {UP:+.0%}, DOWN if < {DOWN:+.0%}, else RANGE; 2023-25 alternate within regime, 2026 all TEST",
               "months": ms}, open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "params", "split.json"), "w"), indent=1)
