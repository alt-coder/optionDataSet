"""Business case from the v2 independent exam (TEST months only, books/exam_v2_engine).

Like-for-like: strategy and NIFTY over the SAME traded months, pre- and post-tax, plus a paired
monthly bootstrap (10,000 synthetic years of 12 TEST months) for loss probability and beat-rate.
Run after an exam; it only reads results (no tuning).

    python business_case.py
"""
import csv
import glob
import json
import os
from collections import defaultdict

import numpy as np

import costs

HERE = os.path.dirname(os.path.abspath(__file__))
BOOK = os.path.join(HERE, "..", "books", "exam_v2_engine")
SPLIT = json.load(open(os.path.join(HERE, "..", "params", "split.json")))
CAP = 2_600_000
LIQ = 0.06  # pledged liquid fund on idle capital (optional overlay, reported separately)


def monthly():
    test = [m for m in SPLIT["months"] if m["set"] == "TEST"]
    pnl = defaultdict(float)
    for f in glob.glob(os.path.join(BOOK, "sleeve_dte*", "trades.csv")):
        for t in csv.DictReader(open(f)):
            pnl[t["open_date"][:7]] += float(t["net"])
    eq, rows = CAP, []
    for m in test:
        r = pnl[m["month"]] / eq
        rows.append({"month": m["month"], "regime": m["regime"], "strat": r, "nifty": m["ret%"] / 100})
        eq *= 1 + r
    return rows


def main():
    rows = monthly()
    s = np.array([r["strat"] for r in rows])
    n = np.array([r["nifty"] for r in rows])
    k = len(rows)
    comp_s, comp_n = np.prod(1 + s) - 1, np.prod(1 + n) - 1
    ann_s, ann_n = (1 + comp_s) ** (12 / k) - 1, (1 + comp_n) ** (12 / k) - 1
    # tax per year of trading: F&O business income 31.2%; NIFTY held < 1y in this framing -> STCG 20%
    post_s = ann_s * (1 - costs.FNO_TAX) if ann_s > 0 else ann_s
    post_n = ann_n * (1 - 0.20) if ann_n > 0 else ann_n
    liq_post = LIQ * (1 - costs.FNO_TAX)  # liquid-fund gains at slab
    rng = np.random.default_rng(42)
    idx = rng.integers(0, k, size=(10_000, 12))
    ys = np.prod(1 + s[idx], axis=1) - 1
    yn = np.prod(1 + n[idx], axis=1) - 1
    out = {
        "test_months": k,
        "compounded": {"strategy%": round(comp_s * 100, 1), "nifty%": round(comp_n * 100, 1)},
        "annualised_pre_tax": {"strategy%": round(ann_s * 100, 1), "nifty%": round(ann_n * 100, 1)},
        "annualised_post_tax": {"strategy%": round(post_s * 100, 1), "nifty%": round(post_n * 100, 1),
                                "strategy_plus_liquid_overlay%": round((post_s + liq_post) * 100, 1)},
        "rupees_per_year_on_26L_post_tax": {"strategy": round(post_s * CAP), "strategy_plus_liquid": round((post_s + liq_post) * CAP),
                                            "nifty": round(post_n * CAP)},
        "bootstrap_12m": {
            "strategy_median%": round(float(np.median(ys)) * 100, 1), "strategy_p5%": round(float(np.percentile(ys, 5)) * 100, 1),
            "strategy_p95%": round(float(np.percentile(ys, 95)) * 100, 1), "P(strategy year < 0)": round(float((ys < 0).mean()), 3),
            "nifty_median%": round(float(np.median(yn)) * 100, 1), "nifty_p5%": round(float(np.percentile(yn, 5)) * 100, 1),
            "P(nifty year < 0)": round(float((yn < 0).mean()), 3), "P(strategy beats nifty)": round(float((ys > yn).mean()), 3)},
        "monthly_hit_rate": round(float((s > 0).mean()), 2),
        "worst_month%": round(float(s.min()) * 100, 2), "best_month%": round(float(s.max()) * 100, 2),
    }
    json.dump(out, open(os.path.join(HERE, "..", "results", "business_case_v2.json"), "w"), indent=1)
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
