"""Backtest engine on Opify 1-minute NIFTY option data.

Conventions
- Minute index m in [0, 375): 0 = 09:15 bar, 374 = 15:29 bar.
- Signals are computed on the CLOSE of bar m and filled at the OPEN of bar m+1, plus slippage.
- Short-leg stop losses trigger on bar HIGH (long legs on LOW); fill = max(stop, open) + slippage (gap-aware).
- Every executed order pays the full costs.charges() schedule.
"""
import datetime as dt
import json
import os
from dataclasses import dataclass, field

import numpy as np

import costs
import opify_data as od

SYMBOL = os.environ.get("OPIFY_SYMBOL", "NIFTY")  # NIFTY | SENSEX | BANKNIFTY (rules are vol-scaled, so portable)
CACHE = os.path.join(od.ROOT, "npz") if SYMBOL == "NIFTY" else os.path.join(od.ROOT, "npz_" + SYMBOL)


def mtime(m):
    t = dt.datetime(2000, 1, 1, 9, 15) + dt.timedelta(minutes=int(m))
    return t.strftime("%H:%M")


def minute_of(hhmm):
    h, mm = map(int, hhmm.split(":"))
    return (h - 9) * 60 + mm - 15


@dataclass
class Day:
    date: dt.date
    expiry: str
    exp_date: dt.date
    lot: int
    step: int
    prev_close: float
    spot: np.ndarray      # (375, 4) o h l c
    strikes: np.ndarray   # (K,)
    ce: np.ndarray        # (K, 375, 6) o h l c vol oi  (ffilled; nan before first trade)
    pe: np.ndarray

    def idx(self, k):
        i = np.searchsorted(self.strikes, k)
        if i < len(self.strikes) and self.strikes[i] == k:
            return i
        return None

    def atm(self, m):
        return int(round(self.spot[m, 3] / self.step) * self.step)

    def opt(self, kind, k):
        i = self.idx(k)
        if i is not None:
            return (self.ce if kind == "CE" else self.pe)[i]
        # The feed only carries ~+-950 pts around spot. A held leg that drifted OUTSIDE that window is
        # worth ~intrinsic (deep ITM) or the 0.05 floor (far OTM); value it so instead of failing.
        if len(self.strikes) and (k < self.strikes[0] or k > self.strikes[-1]) and k % self.step == 0:
            sp = self.spot[:, :4]
            intr = np.maximum(sp - k, 0) if kind == "CE" else np.maximum(k - sp, 0)
            a = np.full((sp.shape[0], 6), np.nan)
            a[:, :4] = np.maximum(intr, costs.TICK)
            if kind == "CE":  # call high/low map to spot high/low
                a[:, 1], a[:, 2] = np.maximum(np.maximum(sp[:, 1] - k, 0), costs.TICK), np.maximum(np.maximum(sp[:, 2] - k, 0), costs.TICK)
            else:
                a[:, 1], a[:, 2] = np.maximum(np.maximum(k - sp[:, 2], 0), costs.TICK), np.maximum(np.maximum(k - sp[:, 1], 0), costs.TICK)
            a[:, 4] = 0
            return a
        return None


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):  # '-' / None placeholders in the feed
        return np.nan


def _to_arr(rows, n):
    a = np.full((n, 6), np.nan)
    for i, b in enumerate(rows[:n]):
        if isinstance(b, list) and len(b) >= 4:
            v = [_num(x) for x in b[:6]]
            if not np.isnan(v[3]):
                a[i, :len(v)] = v
    # forward fill OHLC where a minute had no trade (o=h=l=c=prev close, vol 0)
    for i in range(1, n):
        if np.isnan(a[i, 3]) and not np.isnan(a[i - 1, 3]):
            c = a[i - 1, 3]
            a[i, :4] = c
            a[i, 4] = 0
            a[i, 5] = a[i - 1, 5]
    return a


def load(expiry, date: dt.date) -> Day:
    p = os.path.join(CACHE, expiry, date.isoformat() + ".npz")
    if os.path.exists(p):
        z = np.load(p)
        meta = json.loads(str(z["meta"]))
        return Day(date, expiry, dt.datetime.strptime(expiry, "%d%b%Y").date(), meta["lot"], meta["step"],
                   meta["prev_close"], z["spot"], z["strikes"], z["ce"], z["pe"])
    j = od.load_day(SYMBOL, expiry, date)
    n = 375
    sp = np.full((n, 4), np.nan)
    for i, b in enumerate(j["spot"]["ohlc"][:n]):
        sp[i] = [_num(x) for x in b[:4]]
    for i in range(1, n):
        if np.isnan(sp[i, 3]) or np.isnan(sp[i]).any():
            sp[i] = sp[i - 1, 3]
    st = j["strikes"]
    ks = np.array(st["strikes"], dtype=float)
    order = np.argsort(ks)
    ce = np.stack([_to_arr(st["ce_ohlc"][i], n) for i in order])
    pe = np.stack([_to_arr(st["pe_ohlc"][i], n) for i in order])
    meta = {"lot": int(j["meta"]["lot_size"]), "step": int(j["meta"]["strike_step"]),
            "prev_close": float(j["meta"].get("prev_close") or sp[0, 0])}
    os.makedirs(os.path.dirname(p), exist_ok=True)
    np.savez_compressed(p, spot=sp, strikes=ks[order], ce=ce, pe=pe, meta=json.dumps(meta))
    return load(expiry, date)


def calendar(start: dt.date, end: dt.date):
    """[(date, expiry_code, dte_trading_days_incl_today_is_0)] using the NEAREST expiry that has data."""
    exps = json.load(open(os.path.join(od.ROOT, f"expiries_{SYMBOL}.json")))
    rows = {}
    for code, ds in exps.items():
        e = dt.datetime.strptime(code, "%d%b%Y").date()
        days = [dt.date.fromisoformat(x) for x in ds]
        for d in days:
            if start <= d <= end and d <= e and os.path.exists(od.raw_path(SYMBOL, code, d)):
                if d not in rows or e < rows[d][1]:
                    rows[d] = (code, e, days)
    out = []
    for d in sorted(rows):
        code, e, days = rows[d]
        dte = len([x for x in days if d < x <= e])
        out.append((d, code, dte))
    return out


# ---------------------------------------------------------------- positions / books
@dataclass
class Leg:
    kind: str        # CE / PE
    strike: float
    qty: int         # +long / -short (contracts = lots * lot_size)
    entry: float = 0.0
    exit: float = None
    entry_t: str = ""
    exit_t: str = ""
    sl: float = None  # absolute premium stop for this leg (short: price above, long: price below)
    reason: str = ""
    charges: float = 0.0


@dataclass
class Trade:
    strategy: str
    expiry: str
    legs: list = field(default_factory=list)
    open_date: str = ""
    close_date: str = ""
    margin: float = 0.0
    notes: str = ""

    def gross(self):
        return sum((l.exit - l.entry) * l.qty for l in self.legs if l.exit is not None)

    def fees(self):
        return sum(l.charges for l in self.legs)

    def net(self):
        return self.gross() - self.fees()


def fill(leg, price, side, d, when, entry=True):
    px = costs.slipped(float(price), side)
    fee = costs.charges(px, abs(leg.qty), side, d)
    leg.charges += fee
    if entry:
        leg.entry, leg.entry_t = px, when
    else:
        leg.exit, leg.exit_t = px, when
    return px


def bar(day, leg, m):
    a = day.opt(leg.kind, leg.strike)
    if a is None or m >= a.shape[0] or np.isnan(a[m, 3]):
        return None
    return a[m]


def margin_estimate(legs, spot, lot):
    """Approximate SPAN+exposure. Defined-risk -> max loss; naked shorts -> ~ 9% notional + 2% ELM."""
    shorts = [l for l in legs if l.qty < 0]
    longs = [l for l in legs if l.qty > 0]
    if not shorts:
        return sum(l.entry * l.qty for l in longs)
    # check if every short is covered by a long of same kind further OTM
    covered = True
    width = 0
    for s in shorts:
        cands = [l for l in longs if l.kind == s.kind and ((s.kind == "CE" and l.strike > s.strike) or (s.kind == "PE" and l.strike < s.strike))]
        if not cands:
            covered = False
            break
        width = max(width, min(abs(c.strike - s.strike) for c in cands))
    q = abs(shorts[0].qty)
    if covered:
        # SPAN on hedged spreads ~ wing width (credit not netted) + 2% exposure margin on one short side
        return width * q + 0.02 * spot * q
    return 0.11 * spot * q * len(shorts) * 0.85  # naked: ~9% SPAN + 2% ELM, strangle offset benefit


class Book:
    """Local trade book: trades.csv (one row per trade), legs.csv (per leg), equity.csv (daily)."""

    def __init__(self, name, capital):
        self.name, self.capital0, self.cash = name, capital, capital
        self.trades, self.equity, self.skipped = [], [], []

    def close(self, t: Trade):
        self.trades.append(t)
        self.cash += t.net()

    def mark(self, d, mtm=0.0):
        self.equity.append((d.isoformat(), self.cash + mtm))

    def save(self, folder):
        import csv
        os.makedirs(folder, exist_ok=True)
        with open(os.path.join(folder, "trades.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["id", "strategy", "expiry", "open_date", "close_date", "legs", "lots_qty", "margin", "gross", "charges", "net", "notes"])
            for i, t in enumerate(self.trades):
                w.writerow([i, t.strategy, t.expiry, t.open_date, t.close_date,
                            " | ".join(f"{'B' if l.qty>0 else 'S'} {int(l.strike)}{l.kind}" for l in t.legs),
                            abs(t.legs[0].qty), round(t.margin), round(t.gross(), 2), round(t.fees(), 2), round(t.net(), 2), t.notes])
        with open(os.path.join(folder, "legs.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["trade_id", "kind", "strike", "qty", "entry", "entry_time", "exit", "exit_time", "exit_reason", "charges"])
            for i, t in enumerate(self.trades):
                for l in t.legs:
                    w.writerow([i, l.kind, int(l.strike), l.qty, l.entry, l.entry_t, l.exit, l.exit_t, l.reason, round(l.charges, 2)])
        with open(os.path.join(folder, "equity.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["date", "equity"])
            w.writerows(self.equity)
