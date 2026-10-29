"""Position simulator + strategy library + metrics."""
import datetime as dt
import math

import numpy as np

import costs
from engine import Book, Leg, Trade, bar, calendar, fill, load, margin_estimate, minute_of, mtime

LAST = 374


# ------------------------------------------------------------------ core simulator
def open_legs(day, m, spec, lots):
    """spec: list of (kind, offset_steps_from_ATM, sign). Signal at close m-1, fill open m."""
    atm = day.atm(m - 1)
    legs = []
    for kind, off, sign in spec:
        k = atm + off * day.step * (1 if kind == "CE" else -1)  # positive offset = OTM
        if day.idx(k) is None:
            return None
        b = bar(day, Leg(kind, k, 0), m)
        if b is None or np.isnan(b[0]):
            return None
        legs.append(Leg(kind, k, sign * lots * day.lot))
    for l in legs:
        b = bar(day, l, m)
        fill(l, b[0], 1 if l.qty > 0 else -1, day.date, f"{day.date} {mtime(m)}")
    return legs


def leg_mtm(legs, day, m):
    tot = 0.0
    for l in legs:
        px = l.exit
        if px is None:
            b = bar(day, l, m)
            px = b[3] if b is not None else l.entry
        tot += (px - l.entry) * l.qty
    return tot


def close_leg(l, day, m, price, reason):
    fill(l, price, -1 if l.qty > 0 else 1, day.date, f"{day.date} {mtime(m)}", entry=False)
    l.reason = reason


def run_position(days, start_m, legs, exit_m, leg_sl=None, combo_sl=None, combo_tp=None,
                 sl_to_cost=False, hedge_follow=True, spot_stop=None, check_every=1):
    """Walk minutes from start_m on days[0] to exit_m on days[-1].
    leg_sl: fraction for short legs (0.3 => stop at 1.3x entry).
    combo_sl/combo_tp: rupees per contract-unit (per 1 qty), compared to MTM on bar close.
    spot_stop: (direction, level) exits all if spot close crosses level (for directional trades).
    Returns final equity effect is in legs; returns the list of legs."""
    units = abs(legs[0].qty)
    for l in legs:
        if l.qty < 0 and leg_sl:
            l.sl = l.entry * (1 + leg_sl)
    for di, day in enumerate(days):
        m0 = start_m if di == 0 else 0
        mend = exit_m if di == len(days) - 1 else LAST
        for m in range(m0, mend + 1):
            final = m == mend and di == len(days) - 1
            # 1) intrabar leg stops
            if leg_sl:
                for l in legs:
                    if l.exit is None and l.qty < 0 and l.sl:
                        b = bar(day, l, m)
                        if b is not None and b[1] >= l.sl:
                            # gap open above stop -> filled at open
                            px = max(l.sl, b[0])
                            close_leg(l, day, m, px, "leg_sl")
                            if hedge_follow:
                                for h in legs:
                                    if h.exit is None and h.qty > 0 and h.kind == l.kind:
                                        hb = bar(day, h, m)
                                        close_leg(h, day, m, hb[3] if hb is not None else h.entry, "hedge_follow")
                            if sl_to_cost:
                                for o in legs:
                                    if o.exit is None and o.qty < 0:
                                        o.sl = min(o.sl, o.entry)
            if all(l.exit is not None for l in legs):
                return legs
            if final:
                for l in legs:
                    if l.exit is None:
                        b = bar(day, l, m)
                        close_leg(l, day, m, b[0] if b is not None else l.entry, "time")
                return legs
            # 2) combo rules on close -> exit next bar open (only when the agent is looking)
            trig = None
            if check_every > 1 and m % check_every != 0:
                continue
            mtm = leg_mtm(legs, day, m) / units
            if combo_sl is not None and mtm <= -combo_sl:
                trig = "combo_sl"
            elif combo_tp is not None and mtm >= combo_tp:
                trig = "combo_tp"
            elif spot_stop is not None:
                dirn, lvl, tgt = spot_stop
                c = day.spot[m, 3]
                if (dirn > 0 and c <= lvl) or (dirn < 0 and c >= lvl):
                    trig = "spot_sl"
                elif tgt is not None and ((dirn > 0 and c >= tgt) or (dirn < 0 and c <= tgt)):
                    trig = "spot_tp"
            if trig:
                nm, nday = (m + 1, day) if m < LAST else (0, days[di + 1] if di + 1 < len(days) else day)
                for l in legs:
                    if l.exit is None:
                        b = bar(nday, l, nm)
                        close_leg(l, nday, nm, b[0] if b is not None else l.entry, trig)
                return legs
    return legs


# ------------------------------------------------------------------ helpers
class Data:
    """Lazy cache of Day objects + daily spot series."""

    def __init__(self, start, end):
        self.cal = calendar(start, end)
        self.by_date = {d: (e, dte) for d, e, dte in self.cal}
        self.dates = [d for d, _, _ in self.cal]
        self._c = {}
        self.daily = {}

    LRU = 12  # a weekly cycle touches <= 6 days; keep memory bounded for multi-process search

    def day(self, d, expiry=None):
        e = expiry or self.by_date[d][0]
        key = (e, d)
        if key in self._c:
            self._c[key] = self._c.pop(key)  # mark recently used
        else:
            self._c[key] = load(e, d)
            while len(self._c) > self.LRU:
                self._c.pop(next(iter(self._c)))
        return self._c[key]

    def spot_daily(self):
        if not self.daily:
            import json, os
            from opify_data import ROOT
            from engine import SYMBOL
            path = os.path.join(ROOT, "spot_daily.json" if SYMBOL == "NIFTY" else f"spot_daily_{SYMBOL}.json")
            disk = json.load(open(path)) if os.path.exists(path) else {}
            dirty = False
            for d in self.dates:
                k = d.isoformat()
                if k not in disk:
                    s = self.day(d).spot
                    disk[k] = [float(s[0, 0]), float(s[:, 1].max()), float(s[:, 2].min()), float(s[-1, 3])]
                    dirty = True
                self.daily[d] = tuple(disk[k])
            if dirty:
                json.dump(disk, open(path, "w"))
        return self.daily


def ema(x, n):
    a, out, e = 2 / (n + 1), [], None
    for v in x:
        e = v if e is None else a * v + (1 - a) * e
        out.append(e)
    return np.array(out)


def lots_for(equity, margin_per_lot, alloc, max_lots=36):
    """max_lots ~ NIFTY freeze-qty scale; beyond this, impact cost makes the slippage model optimistic."""
    if margin_per_lot <= 0:
        return 0
    return int(max(0, min(max_lots, math.floor(equity * alloc / margin_per_lot))))


# ------------------------------------------------------------------ metrics
def metrics(equity_rows, capital0, trades=None):
    eq = np.array([e for _, e in equity_rows], dtype=float)
    if len(eq) < 2:
        return {}
    dates = [dt.date.fromisoformat(d) for d, _ in equity_rows]
    yrs = max((dates[-1] - dates[0]).days / 365.25, 1e-9)
    r = np.diff(eq) / eq[:-1]
    peak = np.maximum.accumulate(eq)
    dd = (eq / peak - 1).min()
    cagr = (eq[-1] / capital0) ** (1 / yrs) - 1 if eq[-1] > 0 else -1
    sharpe = r.mean() / r.std() * math.sqrt(252) if r.std() > 0 else 0
    out = {"final": round(eq[-1]), "return%": round((eq[-1] / capital0 - 1) * 100, 1), "cagr%": round(cagr * 100, 1),
           "maxdd%": round(dd * 100, 1), "sharpe": round(sharpe, 2),
           "calmar": round(cagr / -dd, 2) if dd < 0 else None}
    if trades:
        nets = np.array([t.net() for t in trades])
        out.update({"trades": len(nets), "win%": round((nets > 0).mean() * 100, 1),
                    "avg_win": round(nets[nets > 0].mean()) if (nets > 0).any() else 0,
                    "avg_loss": round(nets[nets <= 0].mean()) if (nets <= 0).any() else 0,
                    "charges": round(sum(t.fees() for t in trades)),
                    "gross": round(sum(t.gross() for t in trades))})
    return out


def yearly(equity_rows):
    out, prev = {}, None
    for d, e in equity_rows:
        y = int(d[:4])
        out.setdefault(y, [prev if prev is not None else e, e])
        out[y][1] = e
        prev = e
    return {y: round((b / a - 1) * 100, 1) for y, (a, b) in out.items()}


def benchmark(data, start, end, capital0):
    sd = data.spot_daily()
    ds = [d for d in data.dates if start <= d <= end]
    p0 = sd[ds[0]][0]
    rows = [(d.isoformat(), capital0 * sd[d][3] / p0) for d in ds]
    return rows
