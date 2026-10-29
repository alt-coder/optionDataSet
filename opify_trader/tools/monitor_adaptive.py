"""Trade monitor for an AI agent: paper book + rule engine on Opify data.

State lives in books/<book>/position.json (open legs + trade meta) and books/<book>/ledger.csv
(every executed order with slippage + charges). All prices come from the Opify simulator API, so
the agent can run it "live" on the latest trading day or replayed at any historical minute.

Commands (all print JSON):
  python monitor.py plan   --at "2026-10-23 09:30"            # what to open now (frozen params)
  python monitor.py open   --at "2026-10-23 09:30" --book paper
  python monitor.py status --at "2026-10-23 13:05" --book paper  # MTM, BE, delta, margin, ACTION
  python monitor.py next   --at "2026-10-23 09:31" --book paper  # fast-forward: first event time
  python monitor.py act    --at "2026-10-23 13:05" --book paper  # execute the recommended action
  python monitor.py latest                                       # latest timestamp the data has

--at defaults to the latest available minute. Params: ../params/frozen.json
"""
import argparse
import csv
import datetime as dt
import json
import os
import sys

import numpy as np

import adaptive as A
import costs
import opify_data as od
from engine import Leg, load, minute_of, mtime
from sim import ema

HERE = os.path.dirname(os.path.abspath(__file__))
BOOKS = os.path.join(HERE, "..", "books")
PARAMS = os.path.join(HERE, "..", "params", "frozen.json")


def params():
    p = json.load(open(PARAMS))
    return {**A.DEFAULT, **{k: v for k, v in p.items() if not k.startswith("_")}}


# ---------------------------------------------------------------- data access
def expiries():
    return json.load(open(os.path.join(od.ROOT, "expiries_NIFTY.json")))


def refresh_expiries(today):
    od.discover_expiries("NIFTY", today - dt.timedelta(3), today + dt.timedelta(21))


def nearest_expiry(d):
    best = None
    for code, ds in expiries().items():
        e = dt.datetime.strptime(code, "%d%b%Y").date()
        if e >= d and d.isoformat() in ds and (best is None or e < best[1]):
            best = (code, e, sorted(ds))
    return best


def day_at(code, d):
    od.fetch_day("NIFTY", code, d, force=(d >= dt.date.today() - dt.timedelta(1)))
    p = os.path.join(od.ROOT, "npz", code, d.isoformat() + ".npz")
    if d >= dt.date.today() - dt.timedelta(1) and os.path.exists(p):
        os.remove(p)  # latest day may still be growing intraday
    return load(code, d)


def parse_at(s):
    if not s:
        return None
    d, t = s.split()
    return dt.date.fromisoformat(d), minute_of(t)


def latest():
    today = dt.date.today()
    refresh_expiries(today)
    for back in range(0, 10):
        d = today - dt.timedelta(back)
        ne = nearest_expiry(d)
        if ne:
            day = day_at(ne[0], d)
            valid = np.where(~np.isnan(day.spot[:, 3]))[0]
            return d, int(valid.max()), ne[0]
    raise SystemExit("no data")


def dte_of(d, ds, e):
    return len([x for x in ds if d < dt.date.fromisoformat(x) <= e])


def trend_at(d, m, day):
    p = params()
    closes = []
    x = d - dt.timedelta(1)
    while len(closes) < p["ema"] + 30 and x > d - dt.timedelta(120):
        ne = nearest_expiry(x)
        if ne:
            try:
                closes.append(day_at(ne[0], x).spot[-1, 3])
            except Exception:
                pass
        x -= dt.timedelta(1)
    closes = closes[::-1]
    if len(closes) <= p["ema"]:
        return 0
    return 1 if day.spot[m - 1, 3] > ema(np.array(closes), p["ema"])[-1] else -1


# ---------------------------------------------------------------- book persistence
def book_dir(name):
    f = os.path.join(BOOKS, name)
    os.makedirs(f, exist_ok=True)
    return f


def load_pos(name):
    f = os.path.join(book_dir(name), "position.json")
    if not os.path.exists(f):
        return None, {}
    j = json.load(open(f))
    pos = A.Position("adaptive_ic", j["expiry"], dt.date.fromisoformat(j["exp_date"]), j["lot"])
    pos.adjustments = j["adjustments"]
    for kind, coll in (("open", pos.open), ("closed", pos.closed)):
        for l in j[kind]:
            coll.append(Leg(**l))
    return pos, j


def save_pos(name, pos, meta):
    f = os.path.join(book_dir(name), "position.json")
    meta.update({"expiry": pos.expiry, "exp_date": pos.exp_date.isoformat(), "lot": pos.lot,
                 "adjustments": pos.adjustments,
                 "open": [l.__dict__ for l in pos.open], "closed": [l.__dict__ for l in pos.closed]})
    json.dump(meta, open(f, "w"), indent=1)


def ledger(name, rows):
    f = os.path.join(book_dir(name), "ledger.csv")
    new = not os.path.exists(f)
    with open(f, "a", newline="") as fh:
        w = csv.writer(fh)
        if new:
            w.writerow(["logged_at", "event"])
        for r in rows:
            w.writerow([dt.datetime.now().isoformat(timespec="seconds"), r])


def equity(name):
    f = os.path.join(book_dir(name), "cash.json")
    if not os.path.exists(f):
        json.dump({"cash": 2_600_000, "history": []}, open(f, "w"))
    return json.load(open(f))


def book_close(name, pos, meta):
    c = equity(name)
    pnl = pos.realized()
    c["cash"] += pnl
    c["history"].append({"opened": meta.get("opened"), "closed": max(l.exit_t for l in pos.closed), "net": round(pnl, 2),
                         "adjustments": pos.adjustments, "expiry": pos.expiry})
    json.dump(c, open(os.path.join(book_dir(name), "cash.json"), "w"), indent=1)
    arch = os.path.join(book_dir(name), "closed_trades.jsonl")
    with open(arch, "a") as fh:
        fh.write(json.dumps({**meta, "open": [], "closed": [l.__dict__ for l in pos.closed], "net": pnl}) + "\n")
    os.remove(os.path.join(book_dir(name), "position.json"))
    return pnl


# ---------------------------------------------------------------- rule evaluation at one minute
def evaluate(pos, meta, day, m, p, cash):
    pnl = float(pos.mtm_series(day)[m])
    S = float(day.spot[m, 3])
    lo, hi = pos.breakevens(S)
    dlt, _ = pos.greeks(day, m)
    lim = p["delta_cap"] * min(cash, p["capital_cap"])
    target, maxloss = meta["credit"] * p["tp_frac"], meta["credit"] * p["sl_frac"]
    last_day = day.date == pos.exp_date
    action, why = "HOLD", ""
    if last_day and m >= minute_of(p["exit"]):
        action, why = "CLOSE_ALL", "time exit on expiry day"
    elif pnl >= target:
        action, why = "CLOSE_ALL", f"target {target:.0f} reached"
    elif pnl <= -maxloss:
        action, why = "CLOSE_ALL", f"stop {-maxloss:.0f} hit"
    elif (hi is not None and S >= hi - p["buffer_pts"]) or (lo is not None and S <= lo + p["buffer_pts"]):
        side = 1 if hi is not None and S >= hi - p["buffer_pts"] else -1
        if pos.adjustments >= p["max_adj"] or (last_day and m > minute_of("14:00")):
            action, why = "CLOSE_ALL", "breakeven touched, no adjustments left"
        else:
            action, why = f"ADJUST_{p['adj_mode'].upper()}_{'UP' if side > 0 else 'DOWN'}", "spot within buffer of breakeven"
    elif abs(dlt) * S > lim:
        action, why = "DELTA_TRIM", f"net delta notional {abs(dlt)*S/1e5:.1f}L > cap {lim/1e5:.1f}L"
    return {
        "time": f"{day.date} {mtime(m)}", "spot": S, "pnl": round(pnl), "target": round(target), "stop": round(-maxloss),
        "breakevens": [lo, hi], "buffer": p["buffer_pts"], "net_delta_units": round(dlt, 1),
        "delta_notional_L": round(abs(dlt) * S / 1e5, 2), "delta_cap_L": round(lim / 1e5, 2),
        "margin_est": round(pos.margin(S)), "adjustments": pos.adjustments, "max_adj": p["max_adj"],
        "legs": [f"{'B' if l.qty>0 else 'S'} {abs(l.qty)} {int(l.strike)}{l.kind} @{l.entry}" for l in pos.open],
        "action": action, "why": why,
    }


def execute(pos, meta, day, m, p, cash, ev):
    nm = min(m + 1, A.LAST)
    a = ev["action"]
    n0 = len(pos.log)
    if a == "CLOSE_ALL":
        pos.close_all(day, nm, ev["why"])
    elif a.startswith("ADJUST"):
        side = 1 if a.endswith("UP") else -1
        if not A.adjust(pos, day, nm, p, side):
            pos.close_all(day, nm, "adj_failed")
    elif a == "DELTA_TRIM":
        dlt, S = pos.greeks(day, nm)
        lim = p["delta_cap"] * min(cash, p["capital_cap"])
        pos.reduce(day, nm, 1 - lim / (abs(dlt) * S), "delta_cap_trim")
    return pos.log[n0:]


# ---------------------------------------------------------------- CLI
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["plan", "open", "status", "next", "act", "latest"])
    ap.add_argument("--at")
    ap.add_argument("--book", default="paper")
    a = ap.parse_args()
    p = params()
    if a.cmd == "latest":
        d, m, code = latest()
        print(json.dumps({"latest": f"{d} {mtime(m)}", "expiry": code}))
        return
    at = parse_at(a.at)
    if at is None:
        d, m, _ = latest()
    else:
        d, m = at
    cash = equity(a.book)["cash"]
    pos, meta = load_pos(a.book)

    if a.cmd in ("plan", "open"):
        if pos:
            print(json.dumps({"error": "position already open", "expiry": pos.expiry}))
            return
        ne = nearest_expiry(d)
        code, e, ds = ne
        dte = dte_of(d, ds, e)
        out = {"time": f"{d} {mtime(m)}", "expiry": code, "dte": dte, "enter_dte": p["enter_dte"],
               "entry_time": p["entry"]}
        if dte != p["enter_dte"]:
            out["action"] = "WAIT"
            out["why"] = f"entries only at dte={p['enter_dte']} ({p['entry']}); today dte={dte}"
            print(json.dumps(out, indent=1))
            return
        day = day_at(code, d)
        tr = trend_at(d, m, day)
        mpl = A.probe_margin_per_lot(day, m, p, day.lot)
        lots = int(min(36, (min(cash, p["capital_cap"]) * p["alloc"]) // mpl))
        out.update({"trend": tr, "lots": lots, "margin_per_lot": round(mpl)})
        if a.cmd == "plan":
            atm, st = day.atm(m - 1), day.step
            sk = p["skew_by_trend"] * tr
            ce, pe = atm + (p["short_off"] + sk) * st, atm - (p["short_off"] - sk) * st
            out["orders"] = [f"SELL {lots} lots {ce}CE", f"SELL {lots} lots {pe}PE",
                             f"BUY {lots} lots {ce + p['wing']*st}CE", f"BUY {lots} lots {pe - p['wing']*st}PE"]
            out["action"] = "OPEN"
            print(json.dumps(out, indent=1))
            return
        pos = A.Position("adaptive_ic", code, e, day.lot)
        if not A.build_entry(pos, day, m, p, tr, lots):
            print(json.dumps({"error": "entry failed (illiquid strike)"}))
            return
        credit = -sum(l.entry * l.qty for l in pos.open)
        meta = {"opened": f"{d} {mtime(m)}", "credit": credit, "trend": tr, "lots": lots}
        save_pos(a.book, pos, meta)
        ledger(a.book, pos.log)
        out.update({"action": "OPENED", "fills": pos.log, "credit_rupees": round(credit)})
        print(json.dumps(out, indent=1))
        return

    if not pos:
        print(json.dumps({"action": "FLAT", "cash": cash}))
        return
    day = day_at(pos.expiry, d)
    if a.cmd == "status":
        print(json.dumps(evaluate(pos, meta, day, m, p, cash), indent=1))
    elif a.cmd == "next":
        # fast-forward from --at until the first non-HOLD minute (across days to expiry)
        ds = sorted(expiries()[pos.expiry])
        for x in [dt.date.fromisoformat(s) for s in ds if dt.date.fromisoformat(s) >= d]:
            dd = day_at(pos.expiry, x)
            valid = np.where(~np.isnan(dd.spot[:, 3]))[0]
            for mm in range(m if x == d else 0, int(valid.max()) + 1):
                ev = evaluate(pos, meta, dd, mm, p, cash)
                if ev["action"] != "HOLD":
                    print(json.dumps(ev, indent=1))
                    return
        print(json.dumps({"action": "HOLD", "why": "no event through latest data"}))
    elif a.cmd == "act":
        ev = evaluate(pos, meta, day, m, p, cash)
        if ev["action"] == "HOLD":
            print(json.dumps(ev, indent=1))
            return
        fills = execute(pos, meta, day, m, p, cash, ev)
        ledger(a.book, fills)
        out = {**ev, "fills": fills}
        if not pos.open:
            out["closed_net"] = round(book_close(a.book, pos, meta))
        else:
            save_pos(a.book, pos, meta)
        print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
