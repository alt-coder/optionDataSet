"""Agent-facing trade monitor for the frozen 3-sleeve trend credit spread.

The agent calls `tick` at each check time; tick applies the frozen rules exactly as the backtest
does (same signal bars, same fills, same slippage + charges) and records everything in a local
paper book: books/<book>/  (positions.json, ledger.csv, closed_trades.csv, cash.json, journal.md).

Time semantics (identical to the backtest):
  --at "YYYY-MM-DD HH:MM" names the 1-min bar whose CLOSE you have just observed.
  Orders fill at the OPEN of the next bar, plus slippage.
  Check bars: every `check_every` minutes from 09:15 (09:15, 09:45, ...).
  Entry signal bar: 14:59 (fills 15:00) on a day whose days-to-expiry matches a sleeve.
  Expiry-day exit: 15:15 bar (fills at its open).

Commands (JSON output):
  python monitor.py rules                         # frozen rules + hash check
  python monitor.py latest                        # newest minute available in the data
  python monitor.py status --at T [--book B]      # view of every sleeve: MTM, target/stop, legs
  python monitor.py tick   --at T [--book B]      # apply rules at T: open/close as required
  python monitor.py next   --at T [--book B]      # fast-forward: first bar >= T where tick acts
  python monitor.py run    --from T1 --to T2      # loop next+tick (agent autopilot / replay)
  python monitor.py report [--book B]             # book P&L vs NIFTY buy & hold
  python monitor.py note   --text "..."           # append a learning to the journal
"""
import argparse
import csv
import datetime as dt
import json
import math
import os
import urllib.error

import numpy as np

import costs
import opify_data as od
from adaptive import market_view
from engine import SYMBOL, Leg, fill, load, margin_estimate, minute_of, mtime
from ensemble import frozen
from sim import ema

HERE = os.path.dirname(os.path.abspath(__file__))
BOOKS = os.path.join(HERE, "..", "books")
CAPITAL = float(os.environ.get("OPIFY_CAPITAL", 2_600_000))  # portfolio mode: 26L/3 per index book


# ---------------------------------------------------------------- data helpers
def expiries():
    return json.load(open(os.path.join(od.ROOT, f"expiries_{SYMBOL}.json")))


def nearest_expiry(d):
    best = None
    for code, ds in expiries().items():
        e = dt.datetime.strptime(code, "%d%b%Y").date()
        if e >= d and d.isoformat() in ds and (best is None or e < best[1]):
            best = (code, e, sorted(ds))
    return best


def day_at(code, d):
    recent = d >= dt.date.today() - dt.timedelta(1)
    od.fetch_day(SYMBOL, code, d, force=recent)
    from engine import CACHE
    p = os.path.join(CACHE, code, d.isoformat() + ".npz")
    if recent and os.path.exists(p):
        os.remove(p)  # today's file may still be growing intraday
    return load(code, d)


def trading_days():
    out = set()
    for ds in expiries().values():
        out.update(dt.date.fromisoformat(x) for x in ds)
    return sorted(out)


def daily_closes_before(d, n=140):
    path = os.path.join(od.ROOT, "spot_daily.json" if SYMBOL == "NIFTY" else f"spot_daily_{SYMBOL}.json")
    cache = json.load(open(path)) if os.path.exists(path) else {}
    days = [x for x in trading_days() if x < d][-n:]
    out, dirty = [], False
    for x in days:
        k = x.isoformat()
        if k not in cache:
            ne = nearest_expiry(x)
            if not ne:
                continue
            s = day_at(ne[0], x).spot
            cache[k] = [float(s[0, 0]), float(s[:, 1].max()), float(s[:, 2].min()), float(s[-1, 3])]
            dirty = True
        out.append(cache[k][3])
    if dirty:
        json.dump(cache, open(path, "w"))
    return np.array(out)


def dte_of(d, code):
    e = dt.datetime.strptime(code, "%d%b%Y").date()
    return len([x for x in expiries()[code] if d < dt.date.fromisoformat(x) <= e])


def parse_at(s):
    d, t = s.split()
    return dt.date.fromisoformat(d), minute_of(t)


def latest():
    today = dt.date.today()
    od.discover_expiries(SYMBOL, today - dt.timedelta(5), today + dt.timedelta(21))
    for back in range(10):
        d = today - dt.timedelta(back)
        ne = nearest_expiry(d)
        if ne:
            day = day_at(ne[0], d)
            valid = np.where(~np.isnan(day.spot[:, 3]))[0]
            return d, int(valid.max())
    raise SystemExit("no data")


# ---------------------------------------------------------------- book state
class PaperBook:
    def __init__(self, name):
        self.dir = os.path.join(BOOKS, name)
        os.makedirs(self.dir, exist_ok=True)
        self.p_pos = os.path.join(self.dir, "positions.json")
        self.p_cash = os.path.join(self.dir, "cash.json")
        self.pos = json.load(open(self.p_pos)) if os.path.exists(self.p_pos) else {}
        self.cash = json.load(open(self.p_cash)) if os.path.exists(self.p_cash) else \
            {str(s): CAPITAL / 3 for s in (1, 2, 3)}

    def save(self):
        json.dump(self.pos, open(self.p_pos, "w"), indent=1)
        json.dump(self.cash, open(self.p_cash, "w"), indent=1)

    def ledger(self, rows):
        f = os.path.join(self.dir, "ledger.csv")
        new = not os.path.exists(f)
        with open(f, "a", newline="") as fh:
            w = csv.writer(fh)
            if new:
                w.writerow(["sim_time", "sleeve", "side", "qty", "strike", "kind", "price", "charges", "reason"])
            w.writerows(rows)

    def closed(self, row):
        f = os.path.join(self.dir, "closed_trades.csv")
        new = not os.path.exists(f)
        with open(f, "a", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(row))
            if new:
                w.writeheader()
            w.writerow(row)


def legs_of(pj):
    return [Leg(**l) for l in pj["legs"]]


# ---------------------------------------------------------------- rule engine (mirrors strategies.trend_spread)
def plan_entry(d, code, day, p, sleeve_cash):
    m = minute_of(p["entry"])
    closes = daily_closes_before(d)
    if len(closes) <= p["ema"]:
        return None, "not enough history for EMA"
    e = ema(closes, p["ema"])[-1]
    S = day.spot[m - 1, 3]
    trend = 1 if S > e else -1
    kind = "PE" if trend > 0 else "CE"
    mv = market_view(day, m - 1, day.exp_date, closes)
    if not np.isfinite(mv["em"]) or mv["em"] <= 0:
        return None, "no expected move (illiquid ATM)"
    if p.get("vrp_min") and mv["rv10"] and mv["atm_iv"] < p["vrp_min"] * mv["rv10"]:
        return None, "VRP filter: IV below realised"
    if p.get("rich_iv") is not None:
        rich = mv["atm_iv"] >= p["rich_iv"] or (mv["rv10"] and mv["atm_iv"] >= p.get("rich_ratio", 9) * mv["rv10"])
        if not rich:
            if p.get("fair_scale", 0.5) <= 0:
                return None, f"premium fair (IV {mv['atm_iv']:.1%} < {p['rich_iv']:.1%} and IV/RV {mv['atm_iv']/mv['rv10']:.2f} < {p['rich_ratio']}): no edge, stand aside"
            p = {**p, "alloc": p["alloc"] * p["fair_scale"]}
    atm = day.atm(m - 1)
    k_use = p["k_sigma"]
    if p.get("strong_em") is not None:  # mirrors strategies.trend_spread (confirmed-trend lean)
        e_prev = ema(closes[:-5], p["ema"])[-1] if len(closes) > p["ema"] + 5 else e
        strong = abs(S - e) > p["strong_em"] * mv["em"] and ((trend > 0) == (e > e_prev))
        if strong and (p.get("strong_side", "both") == "both" or (p["strong_side"] == "up" and trend > 0)):
            k_use = p.get("strong_k", 0.6)
    off = max(1, int(round(k_use * mv["em"] / day.step)))
    w = max(1, int(round(p["wing_sigma"] * mv["em"] / day.step)))
    sgn = 1 if kind == "CE" else -1
    short_k, long_k = atm + sgn * off * day.step, atm + sgn * (off + w) * day.step
    opp = []
    if p.get("opp_k"):  # mirrors strategies.trend_spread: far-OTM opposite spread (iron condor)
        okind = "CE" if kind == "PE" else "PE"
        oo = max(1, int(round(p["opp_k"] * mv["em"] / day.step)))
        ow = max(1, int(round(p.get("opp_wing_sigma", p["wing_sigma"]) * mv["em"] / day.step)))
        osg = 1 if okind == "CE" else -1
        opp = [(okind, atm + osg * oo * day.step, -1), (okind, atm + osg * (oo + ow) * day.step, 1)]
        if any(day.idx(k) is None for _, k, _ in opp):
            opp = []  # far strike outside the data window -> plain 2-leg spread (same as engine)
    # margin per lot with entry-bar opens (same as strategies._probe_margin)
    pl = []
    for kd, k, s in [(kind, short_k, -1), (kind, long_k, 1)] + opp:
        a = day.opt(kd, k) if day.idx(k) is not None else None  # entries need a real quoted strike
        if a is None or np.isnan(a[m, 0]) or np.isnan(a[m - 1, 3]):
            return None, f"strike {k}{kd} missing"
        l = Leg(kd, k, s * day.lot)
        l.entry = a[m - 1, 3]  # size from the signal bar close (no lookahead into the fill bar)
        pl.append(l)
    mpl = margin_estimate(pl, day.spot[m - 1, 3], day.lot)
    lots = int(max(0, min(36, math.floor(sleeve_cash * p["alloc"] / mpl))))
    view = {"spot": round(S, 2), "ema20": round(e, 2), "trend": trend, "expected_move": round(mv["em"], 1),
            "atm_iv": round(mv["atm_iv"], 4), "rv10": mv["rv10"] and round(mv["rv10"], 4),
            "call_oi_wall": mv["call_wall"], "put_oi_wall": mv["put_wall"],
            "short": f"{int(short_k)}{kind}", "wing": f"{int(long_k)}{kind}", "lots": lots, "margin_per_lot": round(mpl)}
    if lots == 0:
        return None, "insufficient capital for 1 lot"
    if opp:
        view["opposite"] = f"S {int(opp[0][1])}{opp[0][0]} / B {int(opp[1][1])}{opp[1][0]}"
    return {"kind": kind, "short": short_k, "long": long_k, "opp": opp, "lots": lots, "margin": mpl * lots, "view": view}, ""


def tick_at(book, d, m, p, dry=False):
    """Apply rules at bar (d, m). Returns list of action dicts."""
    acts = []
    ne = nearest_expiry(d)
    if not ne:
        return acts
    exit_m, entry_sig = minute_of(p["exit"]), minute_of(p["entry"]) - 1
    # ---- manage open sleeves
    for s, pj in list(book.pos.items()):
        code = pj["expiry"]
        e = dt.datetime.strptime(code, "%d%b%Y").date()
        day = day_at(code, d)
        legs = legs_of(pj)
        units = abs(legs[0].qty)
        why = None
        if d == e and m == exit_m:
            why, fm = "time", m           # backtest closes at the OPEN of the 15:15 bar
        elif m % p["check_every"] == 0 and not (d == e and m > exit_m):
            mtm = sum((day.opt(l.kind, l.strike)[m, 3] - l.entry) * l.qty for l in legs) / units
            if mtm <= -pj["credit"] * p["sl_x"]:
                why, fm = "combo_sl", m + 1
            elif mtm >= pj["credit"] * p["tp_x"]:
                why, fm = "combo_tp", m + 1
        if why:
            if dry:
                acts.append({"sleeve": s, "action": "CLOSE", "reason": why})
                continue
            rows, pnl = [], 0.0
            for l in legs:
                a = day.opt(l.kind, l.strike)
                before = l.charges
                fill(l, a[fm, 0], -1 if l.qty > 0 else 1, d, f"{d} {mtime(fm)}", entry=False)
                rows.append([f"{d} {mtime(fm)}", s, "BUY" if l.qty < 0 else "SELL", abs(l.qty), int(l.strike), l.kind, l.exit, round(l.charges - before, 2), why])
                pnl += (l.exit - l.entry) * l.qty - l.charges
            book.cash[s] += pnl
            book.ledger(rows)
            book.closed({"sleeve": s, "expiry": code, "opened": pj["opened"], "closed": f"{d} {mtime(fm)}",
                         "structure": " / ".join(f"{'S' if l.qty<0 else 'B'} {int(l.strike)}{l.kind}" for l in legs),
                         "lots": pj["lots"], "credit_per_unit": round(pj["credit"], 2), "net_pnl": round(pnl, 2),
                         "reason": why, "cash_after": round(book.cash[s], 2)})
            del book.pos[s]
            acts.append({"sleeve": s, "action": "CLOSE", "reason": why, "net_pnl": round(pnl), "fills": rows})
    # ---- entries (a sleeve is flat and today's dte matches it)
    if m == entry_sig and p.get("months") and d.strftime("%Y-%m") not in p["months"]:
        pass  # month outside the selected split set: manage open sleeves only, no new entries
    elif m == entry_sig:
        code, e, _ = ne
        dte = dte_of(d, code)
        s = str(dte)
        if dte in frozen()[1] and s not in book.pos:
            day = day_at(code, d)
            plan, why_not = plan_entry(d, code, day, p, book.cash[s])
            if plan is None:
                acts.append({"sleeve": s, "action": "SKIP", "reason": why_not})
            elif dry:
                acts.append({"sleeve": s, "action": "OPEN", "plan": plan["view"]})
            else:
                fm = m + 1
                q = plan["lots"] * day.lot
                legs = [Leg(plan["kind"], plan["short"], -q), Leg(plan["kind"], plan["long"], q)] +                        [Leg(kd, k, sg * q) for kd, k, sg in plan.get("opp", [])]
                rows = []
                for l in legs:
                    fill(l, day.opt(l.kind, l.strike)[fm, 0], 1 if l.qty > 0 else -1, d, f"{d} {mtime(fm)}")
                    rows.append([f"{d} {mtime(fm)}", s, "SELL" if l.qty < 0 else "BUY", abs(l.qty), int(l.strike), l.kind, l.entry, round(l.charges, 2), "entry"])
                credit = sum(-l.entry * l.qty for l in legs) / q
                book.pos[s] = {"expiry": code, "opened": f"{d} {mtime(fm)}", "lots": plan["lots"], "credit": credit,
                               "view": plan["view"], "legs": [l.__dict__ for l in legs]}
                book.ledger(rows)
                acts.append({"sleeve": s, "action": "OPEN", "credit_per_unit": round(credit, 2), "view": plan["view"], "fills": rows})
    if not dry:
        book.save()
    return acts


_HOLD = None


def hold_expectancy(dist, pnl_frac):
    """Learned (TRAIN-only greek_study) E[value of holding]/credit for checkpoints in a similar state."""
    global _HOLD
    if _HOLD is None:
        f = os.path.join(HERE, "..", "results", "greek_rows_train.json")
        _HOLD = json.load(open(f)) if os.path.exists(f) else []
    if not _HOLD:
        return None
    dd = np.array([r["dist"] for r in _HOLD]); pf = np.array([r["pnl_frac"] for r in _HOLD]); h = np.array([r["hold_frac"] for r in _HOLD])
    # nearest ~150 checkpoints in (distance, P&L) space
    z = ((dd - dist) / (dd.std() or 1)) ** 2 + ((pf - pnl_frac) / (pf.std() or 1)) ** 2
    idx = np.argsort(z)[:150]
    x = h[idx]
    return {"E_hold_over_credit": round(float(x.mean()), 3), "P_hold_loses": round(float((x < 0).mean()), 2), "n_similar": int(len(x))}


def position_greeks(day, m, legs, exp_date):
    from adaptive import iv, t_years, R
    from scipy.stats import norm
    S = float(day.spot[m, 3]); T = t_years(day, m, exp_date)
    D = G = TH = V = 0.0
    for l in legs:
        c = float(day.opt(l.kind, l.strike)[m, 3]); v = max(iv(c, S, l.strike, T, l.kind), 0.03)
        d1 = (math.log(S / l.strike) + (R + v * v / 2) * T) / (v * math.sqrt(T)); pdf = norm.pdf(d1)
        D += (norm.cdf(d1) if l.kind == "CE" else norm.cdf(d1) - 1) * l.qty
        G += pdf / (S * v * math.sqrt(T)) * l.qty
        TH += -(S * pdf * v) / (2 * math.sqrt(T)) / 365 * l.qty
        V += S * pdf * math.sqrt(T) / 100 * l.qty
    return {"net_delta_units": round(D, 1), "delta_notional_L": round(D * S / 1e5, 2), "gamma_per_EM_move_units": None,
            "theta_rupees_per_day": round(TH), "vega_rupees_per_volpt": round(V), "_gamma": G}


def status(book, d, m, p):
    out = {"time": f"{d} {mtime(m)}", "cash_by_sleeve": {k: round(v) for k, v in book.cash.items()}, "open": []}
    for s, pj in book.pos.items():
        day = day_at(pj["expiry"], d)
        legs = legs_of(pj)
        units = abs(legs[0].qty)
        mtm = sum((day.opt(l.kind, l.strike)[m, 3] - l.entry) * l.qty for l in legs)
        out["open"].append({"sleeve": s, "expiry": pj["expiry"], "opened": pj["opened"], "spot": float(day.spot[m, 3]),
                            "legs": [f"{'S' if l.qty<0 else 'B'} {abs(l.qty)} {int(l.strike)}{l.kind} @{l.entry}" for l in legs],
                            "credit_per_unit": round(pj["credit"], 2), "mtm_rupees": round(mtm),
                            "mtm_per_unit": round(mtm / units, 2),
                            "take_profit_at_per_unit": round(pj["credit"] * p["tp_x"], 2),
                            "stop_at_per_unit": round(-pj["credit"] * p["sl_x"], 2),
                            **seasoned_view(day, m, legs, pj, p)})
    return out


def seasoned_view(day, m, legs, pj, p):
    """Greeks + learned hold-expectancy + the decision a disciplined trader takes under the frozen rules."""
    from adaptive import market_view
    e = dt.datetime.strptime(pj["expiry"], "%d%b%Y").date()
    units = abs(legs[0].qty)
    mtm_u = sum((day.opt(l.kind, l.strike)[m, 3] - l.entry) * l.qty for l in legs) / units
    pf = mtm_u / pj["credit"] if pj["credit"] > 0 else 0
    mv = market_view(day, m, e)
    short = [l for l in legs if l.qty < 0][0]
    S = float(day.spot[m, 3])
    dist = ((short.strike - S) if short.kind == "CE" else (S - short.strike)) / mv["em"] if mv["em"] > 0 else None
    g = position_greeks(day, m, legs, e)
    g["gamma_per_EM_move_units"] = round(g.pop("_gamma") * mv["em"], 2) if mv["em"] > 0 else None
    he = hold_expectancy(dist, pf) if dist is not None else None
    last = day.date == e
    if last and m >= minute_of(p["exit"]):
        dec, why = "SELL_OFF", "time exit on expiry day"
    elif pf >= p["tp_x"]:
        dec, why = "SELL_OFF", f"take profit: {pf:.0%} of credit captured; learned remaining edge is small"
    elif pf <= -p["sl_x"]:
        dec, why = "SELL_OFF", "stop: loss >= 1x credit (tail protection)"
    else:
        dec = "WAIT"
        why = (f"learned E[hold] = {he['E_hold_over_credit']:+.2f} x credit vs exit cost ~0.02 x credit "
               f"(P(hold loses) {he['P_hold_loses']:.0%}); TRAIN study: tested spreads mean-revert, rolling (REBALANCE) underperformed waiting"
               if he else "rules say hold until target/stop/time")
    return {"greeks": g, "short_strike_distance_EM": round(dist, 2) if dist is not None else None,
            "captured_frac_of_credit": round(pf, 2), "learned_hold": he, "decision": dec, "why": why,
            "decision_menu": {"WAIT": "default while between stop and target",
                              "SELL_OFF": "target 0.8x / stop 1.0x credit / expiry 15:15 (executed by tick)",
                              "REBALANCE": "not used: rolling tested spreads lost vs waiting on TRAIN (all trigger levels)"}}


def candidate_bars(d, m0, p):
    exit_m, entry_sig = minute_of(p["exit"]), minute_of(p["entry"]) - 1
    bars = {x for x in range(0, 375) if x % p["check_every"] == 0} | {exit_m, entry_sig}
    return sorted(x for x in bars if x >= m0)


def next_event(book, d, m, p, until=None):
    days = [x for x in trading_days() if x >= d and (until is None or x <= until[0])]
    for x in days:
        codes = {nearest_expiry(x)[0]} | {pj["expiry"] for pj in book.pos.values()}
        try:  # day listed but not served by the API (e.g. special Saturday sessions): skip, like the engine
            for c in codes:
                day_at(c, x)
        except (urllib.error.HTTPError, FileNotFoundError, OSError):
            continue
        for mm in candidate_bars(x, m if x == d else 0, p):
            if until and (x, mm) > until:
                return None
            try:
                acts = tick_at(book, x, mm, p, dry=True)
            except Exception as ex:  # surface logic/data problems to the agent instead of silently skipping
                return x, mm, [{"action": "ERROR", "reason": repr(ex)[:300]}]
            if acts:
                return x, mm, acts
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["rules", "latest", "status", "tick", "next", "run", "report", "note"])
    ap.add_argument("--at")
    ap.add_argument("--from", dest="frm")
    ap.add_argument("--to")
    ap.add_argument("--book", default="paper")
    ap.add_argument("--text")
    ap.add_argument("--set", dest="month_set", help="TRAIN or TEST: only enter in that side of params/split.json")
    a = ap.parse_args()
    p, sleeves, h = frozen()
    if os.environ.get("OPIFY_ALLOC"):
        # portfolio mode sizing only (rules unchanged): e.g. 1.0 inside each 1/3-capital index book -> total margin <= 26L
        p = {**p, "alloc": float(os.environ["OPIFY_ALLOC"])}
    if a.month_set:
        sp = json.load(open(os.path.join(HERE, "..", "params", "split.json")))
        p = {**p, "months": [x["month"] for x in sp["months"] if x["set"] == a.month_set]}
    if a.cmd == "rules":
        print(json.dumps({"sha256": h, "sleeves_dte": sleeves, "params": p}, indent=1))
        return
    if a.cmd == "latest":
        d, m = latest()
        print(json.dumps({"latest_bar": f"{d} {mtime(m)}"}))
        return
    book = PaperBook(a.book)
    if a.cmd == "note":
        with open(os.path.join(book.dir, "journal.md"), "a") as f:
            f.write(f"- {dt.datetime.now().isoformat(timespec='minutes')}: {a.text}\n")
        print(json.dumps({"ok": True}))
        return
    if a.cmd == "report":
        rows = list(csv.DictReader(open(os.path.join(book.dir, "closed_trades.csv")))) if os.path.exists(os.path.join(book.dir, "closed_trades.csv")) else []
        net = sum(float(r["net_pnl"]) for r in rows)
        wins = sum(float(r["net_pnl"]) > 0 for r in rows)
        out = {"closed_trades": len(rows), "win%": round(100 * wins / len(rows), 1) if rows else None,
               "net_pnl": round(net), "equity": round(sum(book.cash.values())), "return%": round(100 * (sum(book.cash.values()) / CAPITAL - 1), 2),
               "open_sleeves": list(book.pos)}
        if rows:
            first = dt.date.fromisoformat(rows[0]["opened"][:10])
            last = dt.date.fromisoformat(rows[-1]["closed"][:10])
            c0 = daily_closes_before(first + dt.timedelta(1), 1)
            c1 = daily_closes_before(last + dt.timedelta(1), 1)
            out["nifty_same_window%"] = round(100 * (c1[-1] / c0[-1] - 1), 2)
        print(json.dumps(out, indent=1))
        return
    if a.cmd in ("status", "tick", "next"):
        d, m = parse_at(a.at) if a.at else latest()
    if a.cmd == "status":
        print(json.dumps(status(book, d, m, p), indent=1))
    elif a.cmd == "tick":
        print(json.dumps({"time": f"{d} {mtime(m)}", "actions": tick_at(book, d, m, p)}, indent=1))
    elif a.cmd == "next":
        r = next_event(book, d, m, p)
        print(json.dumps({"next": f"{r[0]} {mtime(r[1])}", "would": r[2]} if r else {"next": None}, indent=1))
    elif a.cmd == "run":
        d, m = parse_at(a.frm)
        until = parse_at(a.to)
        n = 0
        while True:
            r = next_event(book, d, m, p, until)
            if not r:
                break
            d, m, would = r
            if would and would[0].get("action") == "ERROR":
                print(json.dumps({"time": f"{d} {mtime(m)}", **would[0]}), flush=True)
                break
            acts = tick_at(book, d, m, p)
            for x in acts:
                print(json.dumps({"time": f"{d} {mtime(m)}", **{k: v for k, v in x.items() if k != "fills"}}), flush=True)
            n += 1
            m += 1
            if m >= 375:
                d, m = d + dt.timedelta(1), 0
        print(json.dumps({"done": True, "events": n, "equity": round(sum(book.cash.values()))}))


if __name__ == "__main__":
    main()
