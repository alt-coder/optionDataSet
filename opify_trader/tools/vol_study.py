"""Empirical volatility study on TRAINING data only (2023-01-01 .. 2025-12-31).

For every trading day at 15:00 with DTE in {0..5} to the nearest weekly expiry:
  EM        = 1.25 x ATM straddle (implied 1-sigma move to expiry)
  realised  = |S(expiry 15:15) - S(entry)|            (terminal move)
  excursion = max |S(t) - S(entry)| until expiry       (path move: what touches strikes)
  long straddle P&L (gamma+/vega+) held to expiry 15:15, net of slippage + charges, per unit
Buckets by DTE, IV/RV10 and ATM-IV tercile answer: is premium rich or cheap, when is long gamma better?
"""
import datetime as dt
import json
import math
import os

import numpy as np

import costs
from adaptive import market_view
from engine import minute_of
from sim import Data

S0, S1 = dt.date(2023, 1, 1), dt.date(2025, 12, 31)
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results")


def run():
    data = Data(S0, S1)
    sd = data.spot_daily()
    rows = []
    m_in, m_out = minute_of("15:00"), minute_of("15:15")
    for i, d in enumerate(data.dates):
        e, dte = data.by_date[d]
        if dte > 5 or i < 12:
            continue
        exp = dt.datetime.strptime(e, "%d%b%Y").date()
        days = [x for x in data.dates[i:] if x <= exp and data.by_date[x][0] == e]
        if not days or days[-1] != exp and dte > 0:
            pass
        day0 = data.day(d)
        closes = np.array([sd[x][3] for x in data.dates[max(0, i - 40):i]])
        mv = market_view(day0, m_in - 1, day0.exp_date, closes)
        if not np.isfinite(mv["em"]) or mv["em"] <= 0:
            continue
        S = day0.spot[m_in, 0]
        K = mv["atm"]
        ce0, pe0 = day0.opt("CE", K), day0.opt("PE", K)
        if ce0 is None or pe0 is None:
            continue
        path = [day0.spot[m_in:, 3]] + [data.day(x).spot[:, 3] for x in days[1:]]
        last = data.day(days[-1])
        end_m = m_out if days[-1] == exp else 374
        if len(days) == 1:
            path = [day0.spot[m_in:end_m + 1, 3]]
        else:
            path[-1] = last.spot[:end_m + 1, 3]
        p = np.concatenate(path)
        ST = float(p[-1])
        exc = float(np.max(np.abs(p - S)))
        # long straddle held to expiry exit (exit at exit bar open, with slippage + charges)
        b_ce, b_pe = ce0[m_in, 0], pe0[m_in, 0]
        ce1, pe1 = last.opt("CE", K), last.opt("PE", K)
        x_ce, x_pe = ce1[end_m, 0], pe1[end_m, 0]
        q = 1
        buy = costs.slipped(b_ce, 1) + costs.slipped(b_pe, 1)
        sell = costs.slipped(x_ce, -1) + costs.slipped(x_pe, -1)
        lot = day0.lot
        fees = sum(costs.charges(px, lot, s, d) for px, s in
                   [(b_ce, 1), (b_pe, 1), (x_ce, -1), (x_pe, -1)]) / lot
        long_pnl = sell - buy - fees
        # short straddle: mirror slippage (sell at bid, buy back at ask)
        short_pnl = (costs.slipped(b_ce, -1) + costs.slipped(b_pe, -1)) - (costs.slipped(x_ce, 1) + costs.slipped(x_pe, 1)) - fees
        rows.append({"date": d.isoformat(), "dte": dte, "S": S, "em": mv["em"], "iv": mv["atm_iv"], "rv10": mv["rv10"],
                     "straddle": b_ce + b_pe, "real": abs(ST - S), "exc": exc,
                     "real_over_em": abs(ST - S) / mv["em"], "exc_over_em": exc / mv["em"],
                     "long_straddle": long_pnl, "short_straddle": short_pnl, "year": d.year})
    return rows


def summarise(rows):
    import collections
    out = {}

    def stats(rs):
        if not rs:
            return None
        r = np.array([x["real_over_em"] for x in rs])
        ex = np.array([x["exc_over_em"] for x in rs])
        ls = np.array([x["long_straddle"] for x in rs])
        ss = np.array([x["short_straddle"] for x in rs])
        return {"n": len(rs),
                "median_real/EM": round(float(np.median(r)), 2), "mean_real/EM": round(float(r.mean()), 2),
                "P(terminal>1EM)": round(float((r > 1).mean()), 2), "P(touch 0.8EM)": round(float((ex > 0.8).mean()), 2),
                "P(touch 1EM)": round(float((ex > 1).mean()), 2),
                "long_straddle_avg_pts": round(float(ls.mean()), 2), "long_win%": round(float((ls > 0).mean() * 100), 1),
                "short_straddle_avg_pts": round(float(ss.mean()), 2), "short_win%": round(float((ss > 0).mean() * 100), 1),
                "short_worst_pts": round(float(ss.min()), 1)}

    out["ALL"] = stats(rows)
    for k in sorted({r["dte"] for r in rows}):
        out[f"dte={k}"] = stats([r for r in rows if r["dte"] == k])
    for y in (2023, 2024, 2025):
        out[f"year={y}"] = stats([r for r in rows if r["year"] == y])
    vr = [r for r in rows if r["rv10"]]
    ratio = np.array([r["iv"] / r["rv10"] for r in vr])
    qs = np.quantile(ratio, [1 / 3, 2 / 3])
    out[f"IV/RV low (<{qs[0]:.2f})"] = stats([r for r, x in zip(vr, ratio) if x < qs[0]])
    out[f"IV/RV mid"] = stats([r for r, x in zip(vr, ratio) if qs[0] <= x < qs[1]])
    out[f"IV/RV high (>{qs[1]:.2f})"] = stats([r for r, x in zip(vr, ratio) if x >= qs[1]])
    out["IV/RV < 1.0"] = stats([r for r, x in zip(vr, ratio) if x < 1.0])
    ivs = np.array([r["iv"] for r in rows])
    q2 = np.quantile(ivs, [1 / 3, 2 / 3])
    out[f"IV low (<{q2[0]:.3f})"] = stats([r for r in rows if r["iv"] < q2[0]])
    out["IV mid"] = stats([r for r in rows if q2[0] <= r["iv"] < q2[1]])
    out[f"IV high (>{q2[1]:.3f})"] = stats([r for r in rows if r["iv"] >= q2[1]])
    return out


if __name__ == "__main__":
    rows = run()
    os.makedirs(OUT, exist_ok=True)
    json.dump(rows, open(os.path.join(OUT, "vol_study_rows.json"), "w"))
    s = summarise(rows)
    json.dump(s, open(os.path.join(OUT, "vol_study_summary.json"), "w"), indent=1)
    for k, v in s.items():
        print(f"{k:24s}", v)
