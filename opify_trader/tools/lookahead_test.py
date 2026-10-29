"""Future-corruption test for lookahead bias.

If a decision only uses information available at decision time, overwriting EVERYTHING after that
moment with random garbage must not change it.

ENTRY test: on the entry day, every bar >= 15:00 is garbage except the 15:00 OPEN (the fill price);
            all later days are garbage.      -> side, strikes, lots, entry fills must be identical.
EXIT test:  every bar after the exit fill (and every later day) is garbage
            (the fill bar keeps only its OPEN). -> exit time, reason and P&L must be identical.

    python lookahead_test.py [n_samples]
"""
import copy
import datetime as dt
import random
import sys

import numpy as np

import sim
import strategies as ST
from engine import minute_of
from ensemble import frozen

RNG = np.random.default_rng(7)


def garbage_like(a):
    g = RNG.uniform(0.05, 3 * np.nanmax(a[np.isfinite(a)]) if np.isfinite(a).any() else 100, size=a.shape)
    return g


def corrupt(day, from_m, keep_open_at=None):
    d2 = copy.copy(day)
    sp, ce, pe = day.spot.copy(), day.ce.copy(), day.pe.copy()
    if from_m < 375:
        sp[from_m:, :] = garbage_like(sp[from_m:, :]) + sp[0, 0] * 0.5
        ce[:, from_m:, :4] = garbage_like(ce[:, from_m:, :4])
        pe[:, from_m:, :4] = garbage_like(pe[:, from_m:, :4])
        if keep_open_at is not None:
            sp[keep_open_at, 0] = day.spot[keep_open_at, 0]
            ce[:, keep_open_at, 0] = day.ce[:, keep_open_at, 0]
            pe[:, keep_open_at, 0] = day.pe[:, keep_open_at, 0]
    d2.spot, d2.ce, d2.pe = sp, ce, pe
    return d2


class CorruptData(sim.Data):
    """Data whose bars after (cut_date, cut_m) are garbage."""

    def __init__(self, base, cut_date, cut_m, keep_open_at=None):
        self.__dict__.update({k: v for k, v in base.__dict__.items() if k != "_c"})
        self._c = {}
        self.base, self.cut_date, self.cut_m, self.keep = base, cut_date, cut_m, keep_open_at

    def day(self, d, expiry=None):
        x = self.base.day(d, expiry)
        if d < self.cut_date:
            return x
        if d == self.cut_date:
            return corrupt(x, self.cut_m, self.keep)
        return corrupt(x, 0)


def sig(t):
    return [(l.kind, l.strike, l.qty, l.entry) for l in t.legs]


def exit_sig(t):
    return [(l.kind, l.strike, l.exit, l.exit_t, l.reason) for l in t.legs], round(t.net(), 2)


def main(n=40):
    p, sleeves, h = frozen()
    data = sim.Data(dt.date(2023, 1, 1), dt.date(2026, 10, 28))
    data.spot_daily()
    cand = [(d, dte) for d, e, dte in data.cal if dte in sleeves and d >= dt.date(2023, 3, 1)]
    random.Random(1).shuffle(cand)
    m_in = minute_of(p["entry"])
    ent_ok = ex_ok = tested = 0
    fails = []
    for d, dte in cand:
        if tested >= n:
            break
        pp = {**p, "enter_dte": dte}
        base = ST.trend_spread(data, d, 866_667, pp)
        if not base:
            continue
        t0 = base[0]
        tested += 1
        # ENTRY: garbage from 15:00 on (keep 15:00 open = fill), later days garbage
        cd = CorruptData(data, d, m_in, keep_open_at=m_in)
        t1 = ST.trend_spread(cd, d, 866_667, pp)
        if t1 and sig(t1[0]) == sig(t0):
            ent_ok += 1
        else:
            fails.append(("entry", d.isoformat(), sig(t0), t1 and sig(t1[0])))
        # EXIT: garbage after the exit fill bar (keep its open)
        xt = max(l.exit_t for l in t0.legs)
        xd = dt.date.fromisoformat(xt[:10])
        xm = minute_of(xt[11:])
        cd2 = CorruptData(data, xd, xm, keep_open_at=xm)
        t2 = ST.trend_spread(cd2, d, 866_667, pp)
        if t2 and exit_sig(t2[0]) == exit_sig(t0):
            ex_ok += 1
        else:
            fails.append(("exit", d.isoformat(), exit_sig(t0), t2 and exit_sig(t2[0])))
    print(f"rules {h[:12]}..  sampled trades: {tested}")
    print(f"ENTRY decisions unchanged under future corruption: {ent_ok}/{tested}")
    print(f"EXIT  decisions unchanged under future corruption: {ex_ok}/{tested}")
    for f in fails[:5]:
        print("FAIL", f)
    return ent_ok == tested and ex_ok == tested


if __name__ == "__main__":
    ok = main(int(sys.argv[1]) if len(sys.argv) > 1 else 40)
    print("NO LOOKAHEAD DETECTED" if ok else "LOOKAHEAD DETECTED")
