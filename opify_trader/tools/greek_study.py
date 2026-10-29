"""Do position Greeks predict the value of HOLDING? (TRAIN months only)

For every v2 trade opened in a TRAIN month, at every 30-min check while it is open, record:
  delta_n   net delta per unit of short qty (directional exposure, sign-adjusted so + = against us)
  gamma_n   net gamma per unit x EM (how fast delta grows over a 1-sigma move)
  theta_n   net theta per day / credit (decay earned per day as a fraction of credit)
  vega_n    net vega per vol point / credit
  dist      (short strike - spot) / EM, signed so + = still OTM
  tleft     trading time left (days)
  pnl_frac  MTM / credit
  dIV       ATM IV now - ATM IV at entry
  hold      rupees per unit still to come if we HOLD to the rule exit  (final per-unit P&L - MTM now)
Exit-now cost (slippage+charges) is per unit ~ 2 * (0.10 + 0.5% * price) per leg, also recorded.
If E[hold] < 0 in a bucket, closing early (sell off) beats waiting there.

    python greek_study.py
"""
import datetime as dt
import json
import math
import os
from collections import defaultdict

import numpy as np

import costs
from adaptive import R, iv, market_view, t_years
from engine import load, minute_of
from scipy.stats import norm

HERE = os.path.dirname(os.path.abspath(__file__))
SPLIT = json.load(open(os.path.join(HERE, "..", "params", "split.json")))
TRAIN = {m["month"] for m in SPLIT["months"] if m["set"] == "TRAIN"}
BOOK = os.path.join(HERE, "..", "books", "v2_train_engine")


def greeks(S, K, T, v, kind):
    d1 = (math.log(S / K) + (R + v * v / 2) * T) / (v * math.sqrt(T))
    pdf = norm.pdf(d1)
    delta = norm.cdf(d1) if kind == "CE" else norm.cdf(d1) - 1
    gamma = pdf / (S * v * math.sqrt(T))
    theta = -(S * pdf * v) / (2 * math.sqrt(T)) / 365  # per calendar day, ignoring rate term
    vega = S * pdf * math.sqrt(T) / 100  # per vol point
    return delta, gamma, theta, vega


def load_trades():
    import csv
    out = []
    for s in (1, 2, 3):
        f = os.path.join(BOOK, f"sleeve_dte{s}")
        T = list(csv.DictReader(open(os.path.join(f, "trades.csv"))))
        L = list(csv.DictReader(open(os.path.join(f, "legs.csv"))))
        for t in T:
            if t["open_date"][:7] not in TRAIN:
                continue
            legs = [l for l in L if l["trade_id"] == t["id"]]
            out.append((s, t, legs))
    return out


def study():
    rows = []
    for s, t, legs in load_trades():
        exp = t["expiry"]
        e = dt.datetime.strptime(exp, "%d%b%Y").date()
        qty = abs(int(legs[0]["qty"]))
        L = [(l["kind"], float(l["strike"]), int(l["qty"]), float(l["entry"])) for l in legs]
        credit = sum(-q * px for _, _, q, px in L) / qty
        final = sum((float(l["exit"]) - float(l["entry"])) * int(l["qty"]) for l in legs) / qty  # gross per unit
        short = [x for x in L if x[2] < 0][0]
        side = 1 if short[0] == "CE" else -1  # +1 bear call (hurt by up moves)
        t_open = dt.datetime.strptime(legs[0]["entry_time"], "%Y-%m-%d %H:%M")
        t_exit = dt.datetime.strptime(max(l["exit_time"] for l in legs), "%Y-%m-%d %H:%M")
        d = t_open.date()
        iv0 = None
        while d <= t_exit.date():
            try:
                day = load(exp, d)
            except Exception:
                d += dt.timedelta(1)
                continue
            for m in range(0, 375, 30):
                now = dt.datetime.combine(d, dt.time(9, 15)) + dt.timedelta(minutes=m)
                if now <= t_open or now >= t_exit:
                    continue
                S = float(day.spot[m, 3])
                T = t_years(day, m, e)
                mv = market_view(day, m, e)
                if not np.isfinite(mv["em"]) or mv["em"] <= 0:
                    continue
                if iv0 is None:
                    iv0 = mv["atm_iv"]
                mtm, D, G, TH, V, exit_cost = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
                for kind, K, q, px in L:
                    a = day.opt(kind, K)
                    c = float(a[m, 3])
                    mtm += (c - px) * q
                    vol = iv(c, S, K, T, kind)
                    dl, gm, th, vg = greeks(S, K, T, max(vol, 0.03), kind)
                    D += dl * q; G += gm * q; TH += th * q; V += vg * q
                    exit_cost += (costs.SLIP_FIXED + costs.SLIP_PCT * c) * abs(q)
                mtm /= qty
                rows.append({
                    "trade": f"{s}-{t['id']}", "month": t["open_date"][:7], "side": side,
                    "delta_n": round(D / qty * side, 4),                 # + means position loses if market moves against the short
                    "gamma_n": round(-G / qty * mv["em"], 4),             # short gamma magnitude per 1 EM move
                    "theta_n": round(TH / qty / credit, 4) if credit > 0 else 0,
                    "vega_n": round(V / qty / credit, 4) if credit > 0 else 0,
                    "dist": round((short[1] - S) / mv["em"] * side, 3),   # + = short still OTM
                    "tleft": round(T * 365, 3), "pnl_frac": round(mtm / credit, 3) if credit > 0 else 0,
                    "dIV": round(mv["atm_iv"] - iv0, 4),
                    "hold": round(final - mtm, 3), "hold_frac": round((final - mtm) / credit, 3) if credit > 0 else 0,
                    "exit_cost": round(exit_cost / qty, 3), "credit": round(credit, 2)})
            d += dt.timedelta(1)
    return rows


def buckets(rows, key, edges):
    out = []
    for lo, hi in zip(edges[:-1], edges[1:]):
        b = [r for r in rows if lo <= r[key] < hi]
        if len(b) < 15:
            continue
        h = np.array([r["hold_frac"] for r in b])
        c = np.array([r["exit_cost"] / r["credit"] for r in b if r["credit"] > 0])
        out.append({"bucket": f"{lo}..{hi}", "n": len(b), "trades": len({r['trade'] for r in b}),
                    "E[hold]/credit": round(float(h.mean()), 3), "t": round(float(h.mean() / (h.std(ddof=1) / math.sqrt(len(h)))), 2),
                    "exit_cost/credit": round(float(c.mean()), 3)})
    return out


if __name__ == "__main__":
    rows = study()
    json.dump(rows, open(os.path.join(HERE, "..", "results", "greek_rows_train.json"), "w"))
    print(len(rows), "check-points from", len({r['trade'] for r in rows}), "TRAIN trades")
    keys = {"delta_n": [-1, 0, 0.1, 0.2, 0.3, 0.45, 1.01], "gamma_n": [-1, 0.02, 0.05, 0.1, 0.2, 5],
            "theta_n": [-5, 0, 0.1, 0.25, 0.5, 1, 10], "vega_n": [-5, -0.5, -0.2, -0.1, 0, 5],
            "dist": [-5, 0, 0.25, 0.5, 0.75, 1.0, 1.5, 10], "tleft": [0, 0.5, 1, 1.5, 2.5, 4, 8],
            "pnl_frac": [-5, -0.75, -0.5, -0.25, 0, 0.25, 0.5, 0.8], "dIV": [-1, -0.02, -0.005, 0.005, 0.02, 1]}
    res = {}
    for k, e in keys.items():
        res[k] = buckets(rows, k, e)
        print(f"\n== {k}")
        for b in res[k]:
            print("  ", b)
    # rank correlation of each feature with hold value
    from scipy.stats import spearmanr
    H = [r["hold_frac"] for r in rows]
    corr = {k: round(float(spearmanr([r[k] for r in rows], H).correlation), 3) for k in keys}
    print("\nSpearman corr with E[hold]:", corr)
    json.dump({"buckets": res, "spearman": corr}, open(os.path.join(HERE, "..", "results", "greek_study_train.json"), "w"), indent=1)
