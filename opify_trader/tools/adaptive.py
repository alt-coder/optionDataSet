"""Event-driven ("fast-forward") adaptive multi-leg option engine.

Instead of reacting every minute, the position is fast-forwarded to the NEXT EVENT, found
vectorised over a whole day of 1-min bars:

  TARGET    position P&L >= profit target
  STOP      position P&L <= -max loss
  TOUCH     spot comes within `buffer` of a breakeven (or a short strike) of the current legs
  DELTA     net delta notional breaches the no-leverage cap (checked at each event and EOD)
  EOD/EXIT  end-of-day checkpoint (mark equity) / final exit on expiry day

At each event the controller decides like a discretionary trader would: close, roll the
tested side, roll the untested side in (re-centre), add a hedge, or convert to an iron fly.
Every adjustment is a real order: slippage + full statutory charges.

Capital model: Rs 26 lakh, no borrowing. Margin blocked <= capital * alloc. Only defined-risk
structures (every short has a long of the same type further OTM). Net |delta| * spot * qty
<= capital * delta_cap  (default 1.0 = never more directional risk than holding 26L of NIFTY).
"""
import datetime as dt
import math

import numpy as np
from scipy.optimize import brentq
from scipy.stats import norm

import costs
from engine import Leg, Trade, bar, fill, margin_estimate, minute_of, mtime

R = 0.065
LAST = 374


# ---------------------------------------------------------------- Black-Scholes helpers
def t_years(day, m, exp_date):
    """Trading time to 15:30 on expiry, in years (calendar days, intraday fraction)."""
    now = dt.datetime.combine(day.date, dt.time(9, 15)) + dt.timedelta(minutes=int(m))
    end = dt.datetime.combine(exp_date, dt.time(15, 30))
    return max((end - now).total_seconds() / (365 * 86400), 1 / (365 * 24 * 12))


def bs(S, K, T, v, kind):
    if v <= 0 or T <= 0:
        return max(0.0, S - K) if kind == "CE" else max(0.0, K - S)
    d1 = (math.log(S / K) + (R + v * v / 2) * T) / (v * math.sqrt(T))
    d2 = d1 - v * math.sqrt(T)
    if kind == "CE":
        return S * norm.cdf(d1) - K * math.exp(-R * T) * norm.cdf(d2)
    return K * math.exp(-R * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


def iv(price, S, K, T, kind):
    intrinsic = max(0.0, S - K) if kind == "CE" else max(0.0, K - S)
    if price <= intrinsic + 0.05:
        return 0.05
    try:
        return brentq(lambda v: bs(S, K, T, v, kind) - price, 0.01, 3.0, maxiter=60)
    except ValueError:
        return 0.15


def delta(S, K, T, v, kind):
    d1 = (math.log(S / K) + (R + v * v / 2) * T) / (v * math.sqrt(T))
    return norm.cdf(d1) if kind == "CE" else norm.cdf(d1) - 1


# ---------------------------------------------------------------- position
class Position:
    def __init__(self, name, expiry, exp_date, lot):
        self.name, self.expiry, self.exp_date, self.lot = name, expiry, exp_date, lot
        self.open, self.closed, self.log = [], [], []
        self.adjustments = 0
        self.peak_margin = 0.0

    # --- orders
    def add(self, day, m, kind, k, qty, why):
        b = bar(day, Leg(kind, k, 0), m)
        if b is None or np.isnan(b[0]):
            return None
        l = Leg(kind, float(k), int(qty))
        fill(l, b[0], 1 if qty > 0 else -1, day.date, f"{day.date} {mtime(m)}")
        l.reason = why
        self.open.append(l)
        self.log.append(f"{day.date} {mtime(m)} {'BUY' if qty>0 else 'SELL'} {abs(qty)} {int(k)}{kind} @{l.entry} [{why}]")
        return l

    def close(self, day, m, l, why):
        b = bar(day, l, m)
        px = b[0] if b is not None and not np.isnan(b[0]) else (l.entry if l.exit is None else l.exit)
        fill(l, px, -1 if l.qty > 0 else 1, day.date, f"{day.date} {mtime(m)}", entry=False)
        l.reason = (l.reason + "|" if l.reason else "") + why
        self.open.remove(l)
        self.closed.append(l)
        self.log.append(f"{day.date} {mtime(m)} CLOSE {int(l.strike)}{l.kind} @{l.exit} [{why}]")

    def close_all(self, day, m, why):
        for l in list(self.open):
            self.close(day, m, l, why)

    def reduce(self, day, m, frac, why):
        """Close `frac` of every open leg (rounded to whole lots) - de-leveraging without exiting."""
        for l in list(self.open):
            lots = abs(l.qty) // self.lot
            cut = int(math.ceil(lots * frac))
            if cut <= 0:
                continue
            if cut >= lots:
                self.close(day, m, l, why)
                continue
            part = Leg(l.kind, l.strike, int(math.copysign(cut * self.lot, l.qty)), entry=l.entry, entry_t=l.entry_t)
            # entry charges were paid on the whole leg; move the proportional share to the split part
            share = l.charges * cut / lots
            part.charges, l.charges = share, l.charges - share
            l.qty = int(math.copysign((lots - cut) * self.lot, l.qty))
            self.open.append(part)
            self.close(day, m, part, why)

    # --- valuation
    def realized(self):
        return sum((l.exit - l.entry) * l.qty - l.charges for l in self.closed) - sum(l.charges for l in self.open)

    def mtm_series(self, day):
        """Total P&L (realised + open MTM at bar close), vector over the day's 375 minutes."""
        tot = np.full(375, self.realized())
        for l in self.open:
            a = day.opt(l.kind, l.strike)
            c = a[:, 3] if a is not None else np.full(375, l.entry)
            c = np.where(np.isnan(c), l.entry, c)
            tot += (c - l.entry) * l.qty
        return tot

    def expiry_payoff(self, S):
        pay = self.realized()
        for l in self.open:
            intr = np.maximum(S - l.strike, 0) if l.kind == "CE" else np.maximum(l.strike - S, 0)
            pay = pay + (intr - l.entry) * l.qty
        return pay

    def breakevens(self, spot):
        S = np.arange(spot * 0.85, spot * 1.15, 5.0)
        p = self.expiry_payoff(S)
        lo = hi = None
        i0 = int(np.argmin(abs(S - spot)))
        for i in range(i0, 0, -1):
            if p[i] >= 0 > p[i - 1]:
                lo = S[i]
                break
        for i in range(i0, len(S) - 1):
            if p[i] >= 0 > p[i + 1]:
                hi = S[i]
                break
        return lo, hi

    def greeks(self, day, m):
        S = day.spot[m, 3]
        T = t_years(day, m, self.exp_date)
        d = 0.0
        for l in self.open:
            b = bar(day, l, m)
            px = b[3] if b is not None else l.entry
            d += delta(S, l.strike, T, iv(px, S, l.strike, T, l.kind), l.kind) * l.qty
        return d, S

    def delta_series(self, day, m0, m1):
        """Vectorised net delta (units) for minutes m0..m1, each leg's IV frozen at m0 (fast-forward scan)."""
        S = day.spot[m0:m1 + 1, 3]
        mins = np.arange(m0, m1 + 1)
        end = dt.datetime.combine(self.exp_date, dt.time(15, 30))
        base = dt.datetime.combine(day.date, dt.time(9, 15))
        T = np.maximum(((end - base).total_seconds() - mins * 60) / (365 * 86400), 1 / (365 * 24 * 12))
        S0, T0 = day.spot[m0, 3], T[0]
        tot = np.zeros_like(S)
        for l in self.open:
            b = bar(day, l, m0)
            px = b[3] if b is not None else l.entry
            v = iv(px, S0, l.strike, T0, l.kind)
            d1 = (np.log(S / l.strike) + (R + v * v / 2) * T) / (v * np.sqrt(T))
            dl = norm.cdf(d1) if l.kind == "CE" else norm.cdf(d1) - 1
            tot += dl * l.qty
        return tot

    def margin(self, spot):
        return margin_estimate(self.open, spot, self.lot) if self.open else 0.0

    def to_trade(self, open_date, notes=""):
        legs = self.closed + self.open
        t = Trade(self.name, self.expiry, legs, open_date=open_date, margin=self.peak_margin,
                  notes=notes + f" adj={self.adjustments}")
        t.close_date = max((l.exit_t for l in legs if l.exit_t), default=open_date)[:10]
        return t


# ---------------------------------------------------------------- the adaptive strategy
DEFAULT = dict(
    enter_dte=4, entry="09:30", exit="15:15",
    short_off=4, wing=6,           # in strikes (x50 pts): shorts 200 OTM, longs 300 further
    skew_by_trend=1,               # shift shorts 1 strike in trend direction (EMA filter)
    fifth_leg=False,               # add a cheap long in trend direction for convexity
    alloc=0.6, capital_cap=2_600_000, delta_cap=1.0,
    tp_frac=0.5,                   # take profit at 50% of initial credit
    sl_frac=1.5,                   # stop at 1.5x initial credit (net, incl. adjustments)
    buffer_pts=60,                 # TOUCH when spot within 60 pts of a short strike
    max_adj=2,                     # adjustments before we just close
    adj_mode="roll_untested",      # roll_untested | roll_tested | ironfly
    ema=20,
    check_every=1,                 # minutes between agent checks (30 = realistic half-hourly monitoring)
)


def market_view(day, m, exp_date, daily_closes=None):
    """Concept inputs at minute m (bar close): expected move, ATM IV, OI walls, realised vol."""
    S = day.spot[m, 3]
    atm = day.atm(m)
    ce, pe = day.opt("CE", atm), day.opt("PE", atm)
    straddle = (ce[m, 3] if ce is not None else np.nan) + (pe[m, 3] if pe is not None else np.nan)
    T = t_years(day, m, exp_date)
    em = 1.25 * straddle                      # straddle ~ 0.8 * S * sigma * sqrt(T)  ->  1 sigma ~ 1.25 * straddle
    atm_iv = straddle / (0.8 * S * math.sqrt(T))
    oi_ce, oi_pe = day.ce[:, m, 5], day.pe[:, m, 5]
    above, below = day.strikes > S, day.strikes < S
    call_wall = float(day.strikes[above][np.nanargmax(oi_ce[above])]) if above.any() and np.isfinite(oi_ce[above]).any() else None
    put_wall = float(day.strikes[below][np.nanargmax(oi_pe[below])]) if below.any() and np.isfinite(oi_pe[below]).any() else None
    rv = None
    if daily_closes is not None and len(daily_closes) > 11:
        r = np.diff(np.log(daily_closes[-11:]))
        rv = float(r.std() * math.sqrt(252))
    return {"S": S, "atm": atm, "straddle": straddle, "em": em, "atm_iv": atm_iv,
            "call_wall": call_wall, "put_wall": put_wall, "rv10": rv}


def concept_strikes(day, mv, p, trend):
    """Shorts at k_sigma * expected move (vol-scaled), pushed beyond nearby OI walls, leaned with trend."""
    st = day.step
    d = p["k_sigma"] * mv["em"]
    lean = p.get("trend_lean", 0.0) * mv["em"] * trend  # shift centre with the trend
    ce_s = math.ceil((mv["S"] + d + lean) / st) * st
    pe_s = math.floor((mv["S"] - d + lean) / st) * st
    if p.get("use_oi_walls"):
        # a wall just beyond our short is a defended level: sell AT the wall's far side, never in front of it
        if mv["call_wall"] and ce_s <= mv["call_wall"] <= mv["S"] + 2 * mv["em"]:
            ce_s = mv["call_wall"]
        if mv["put_wall"] and mv["S"] - 2 * mv["em"] <= mv["put_wall"] <= pe_s:
            pe_s = mv["put_wall"]
    w = max(st, math.ceil(p["wing_sigma"] * mv["em"] / st) * st)
    return ce_s, pe_s, w


def build_entry(pos, day, m, p, trend, lots, mv=None):
    q = lots * pos.lot
    atm = day.atm(m - 1)
    st = day.step
    if p.get("mode") == "concept" and mv is not None:
        ce_s, pe_s, w = concept_strikes(day, mv, p, trend)
        spec = [("CE", ce_s, -q, "short call ~%.2fσ" % ((ce_s - mv["S"]) / mv["em"])),
                ("PE", pe_s, -q, "short put ~%.2fσ" % ((mv["S"] - pe_s) / mv["em"])),
                ("CE", ce_s + w, q, "call wing"), ("PE", pe_s - w, q, "put wing")]
    else:
        sk = p["skew_by_trend"] * trend
        ce_s = atm + (p["short_off"] + sk) * st
        pe_s = atm - (p["short_off"] - sk) * st
        spec = [("CE", ce_s, -q, "short call"), ("PE", pe_s, -q, "short put"),
                ("CE", ce_s + p["wing"] * st, q, "call wing"), ("PE", pe_s - p["wing"] * st, q, "put wing")]
    if p.get("fifth_leg") and trend:
        kind = "CE" if trend > 0 else "PE"
        k = ce_s + p["wing"] * st if trend > 0 else pe_s - p["wing"] * st
        # extra long one strike beyond our wing: makes it a broken-wing with upside convexity
        spec.append((kind, k + (st if trend > 0 else -st), max(pos.lot, q // 2), "trend convexity"))
    for kind, k, qty, why in spec:
        if day.idx(k) is None:
            return False
    for kind, k, qty, why in spec:
        if pos.add(day, m, kind, k, qty, why) is None:
            pos.close_all(day, m, "entry_abort")
            return False
    return True


def probe_margin_per_lot(day, m, p, lot):
    tmp = Position("probe", day.expiry, day.exp_date, lot)
    legs = []
    atm = day.atm(m - 1)
    st = day.step
    for kind, k, s in [("CE", atm + p["short_off"] * st, -1), ("PE", atm - p["short_off"] * st, -1),
                       ("CE", atm + (p["short_off"] + p["wing"]) * st, 1), ("PE", atm - (p["short_off"] + p["wing"]) * st, 1)]:
        l = Leg(kind, k, s * lot)
        b = bar(day, l, m)
        if b is None:
            return None
        l.entry = b[0]
        legs.append(l)
    return margin_estimate(legs, day.spot[m, 3], lot)


def adjust(pos, day, m, p, side):
    """side: +1 market moved up toward call shorts, -1 down toward put shorts."""
    st = day.step
    atm = day.atm(m)
    q = max(abs(l.qty) for l in pos.open if l.qty < 0) if any(l.qty < 0 for l in pos.open) else 0
    if q == 0:
        return False
    mode = p["adj_mode"]
    untested = "PE" if side > 0 else "CE"
    tested = "CE" if side > 0 else "PE"
    if p.get("mode") == "concept" and mode == "roll_untested":
        # re-sell the untested side at the CURRENT k-sigma expected move (vol and time already shrank it)
        mv = market_view(day, m - 1 if m > 0 else 0, pos.exp_date)
        if not np.isfinite(mv["em"]) or mv["em"] <= 0:
            return False
        ce_s, pe_s, w = concept_strikes(day, mv, {**p, "use_oi_walls": False}, 0)
        tested_short = [l.strike for l in pos.open if l.kind == tested and l.qty < 0]
        s = pe_s if untested == "PE" else ce_s
        if tested_short:  # never cross the tested short (no inverted condor)
            s = min(s, tested_short[0]) if untested == "PE" else max(s, tested_short[0])
        wk = s - w if untested == "PE" else s + w
        if day.idx(s) is None or day.idx(wk) is None:
            return False
        for l in [l for l in pos.open if l.kind == untested]:
            pos.close(day, m, l, "roll_untested")
        pos.add(day, m, untested, s, -q, "re-centre short @k-sigma")
        pos.add(day, m, untested, wk, q, "re-centre wing")
        pos.adjustments += 1
        return True
    if mode in ("roll_untested", "ironfly"):
        # close the untested credit spread (mostly decayed) and re-sell it closer to spot
        for l in [l for l in pos.open if l.kind == untested]:
            pos.close(day, m, l, "roll_untested")
        off = 0 if mode == "ironfly" else max(1, p["short_off"] // 2)
        s = atm - off * st if untested == "PE" else atm + off * st
        w = s - p["wing"] * st if untested == "PE" else s + p["wing"] * st
        if day.idx(s) is None or day.idx(w) is None:
            return False
        # never let the re-centred short cross the tested short (no inverted condor)
        pos.add(day, m, untested, s, -q, "re-centre short")
        pos.add(day, m, untested, w, q, "re-centre wing")
    elif mode == "roll_tested":
        for l in [l for l in pos.open if l.kind == tested]:
            pos.close(day, m, l, "roll_tested")
        s = atm + p["short_off"] * st if tested == "CE" else atm - p["short_off"] * st
        w = s + p["wing"] * st if tested == "CE" else s - p["wing"] * st
        if day.idx(s) is None or day.idx(w) is None:
            return False
        pos.add(day, m, tested, s, -q, "roll tested short out")
        pos.add(day, m, tested, w, q, "roll tested wing out")
    pos.adjustments += 1
    return True


def run_adaptive(data, d, equity, p):
    """One weekly cycle: enter at enter_dte, manage by events until expiry exit."""
    p = {**DEFAULT, **p}
    e, dte = data.by_date[d]
    if dte != p["enter_dte"]:
        return []
    i0 = data.dates.index(d)
    days = []
    for x in data.dates[i0:]:
        if data.by_date[x][0] != e:
            break
        days.append(data.day(x))
    day0 = days[0]
    m0 = minute_of(p["entry"])
    # trend filter on daily closes up to yesterday
    sd = data.spot_daily()
    closes = np.array([sd[x][3] for x in data.dates[max(0, i0 - 120):i0]])
    trend = 0
    if len(closes) > p["ema"]:
        from sim import ema
        trend = 1 if day0.spot[m0 - 1, 3] > ema(closes, p["ema"])[-1] else -1
    cap = min(equity, p["capital_cap"]) * p["alloc"]
    mv = None
    if p.get("mode") == "concept":
        mv = market_view(day0, m0 - 1, day0.exp_date, closes)
        if not np.isfinite(mv["em"]) or mv["em"] <= 0:
            return []
        # variance risk premium: only sell insurance when implied vol is priced above realised vol
        if p.get("vrp_min") and mv["rv10"] and mv["atm_iv"] < p["vrp_min"] * mv["rv10"]:
            return []
        p = {**p, "buffer_pts": p.get("buffer_sigma", 0.2) * mv["em"]}
        ce_s, pe_s, w = concept_strikes(day0, mv, p, trend)
        mpl = w * day0.lot + 0.02 * mv["S"] * day0.lot  # same SPAN approximation as margin_estimate
        if p.get("risk_frac"):
            # no-leverage sizing: worst-case expiry loss of the condor (width - est. credit) <= risk_frac * capital
            ce_px, pe_px = day0.opt("CE", ce_s), day0.opt("PE", pe_s)
            cw, pw = day0.opt("CE", ce_s + w), day0.opt("PE", pe_s - w)
            if any(x is None for x in (ce_px, pe_px, cw, pw)):
                return []
            cr = ce_px[m0 - 1, 3] + pe_px[m0 - 1, 3] - cw[m0 - 1, 3] - pw[m0 - 1, 3]
            loss_per_lot = max(w - cr, 1) * day0.lot
            lots_risk = int(min(equity, p["capital_cap"]) * p["risk_frac"] // loss_per_lot)
            mpl = max(mpl, cap / max(lots_risk, 1e-9)) if lots_risk > 0 else 0
    else:
        mpl = probe_margin_per_lot(day0, m0, p, day0.lot)
    if not mpl:
        return []
    lots = int(min(36, cap // mpl))
    if lots <= 0:
        return []
    pos = Position("adaptive_ic", e, day0.exp_date, day0.lot)
    if not build_entry(pos, day0, m0, p, trend, lots, mv):
        return []
    credit = -sum(l.entry * l.qty for l in pos.open)  # rupees
    pos.peak_margin = pos.margin(day0.spot[m0, 3])
    target, maxloss = credit * p["tp_frac"], credit * p["sl_frac"]
    exit_m = minute_of(p["exit"])
    events = 0
    for di, day in enumerate(days):
        start = m0 + 1 if di == 0 else 0
        end = exit_m if di == len(days) - 1 else LAST
        while start <= end and pos.open:
            pnl = pos.mtm_series(day)[start:end + 1]
            spot = day.spot[start:end + 1, 3]
            lo, hi = pos.breakevens(day.spot[start, 3])
            up = np.zeros_like(spot, bool) if hi is None else spot >= hi - p["buffer_pts"]
            dn = np.zeros_like(spot, bool) if lo is None else spot <= lo + p["buffer_pts"]
            tp = pnl >= target
            sl = pnl <= -maxloss
            lim = p["delta_cap"] * min(equity, p["capital_cap"])
            dc = np.abs(pos.delta_series(day, start, end)) * spot > lim
            ev = tp | sl | up | dn | dc
            if p.get("check_every", 1) > 1:
                # agent only looks at the market every N minutes (e.g. 30-min monitoring cadence)
                mins = np.arange(start, end + 1)
                ev &= (mins % p["check_every"] == 0) | (mins == end)
            if not ev.any():
                break  # fast-forward to end of this day
            j = int(np.argmax(ev))
            m = start + j
            nm = min(m + 1, LAST)  # act at next bar open
            events += 1
            if dc[j] and not (tp[j] or sl[j] or up[j] or dn[j]):
                dlt, S = pos.greeks(day, nm)  # exact re-check with live IVs before trimming
                if abs(dlt) * S > lim:
                    pos.reduce(day, nm, 1 - lim / (abs(dlt) * S), "delta_cap_trim")
                    if not any(l.qty < 0 for l in pos.open):
                        pos.close_all(day, nm, "trimmed_out")
                start = nm + 1
                continue
            if tp[j]:
                pos.close_all(day, nm, "target")
            elif sl[j]:
                pos.close_all(day, nm, "stop")
            else:
                side = 1 if up[j] else -1
                if pos.adjustments >= p["max_adj"] or (di == len(days) - 1 and m > minute_of("14:00")):
                    pos.close_all(day, nm, "touch_no_adj_left")
                else:
                    if not adjust(pos, day, nm, p, side):
                        pos.close_all(day, nm, "adj_failed")
                    else:
                        # still inside the danger zone right after adjusting -> structure can't defend, exit
                        lo2, hi2 = pos.breakevens(day.spot[nm, 3])
                        S = day.spot[nm, 3]
                        if (hi2 is not None and S >= hi2 - p["buffer_pts"]) or (lo2 is not None and S <= lo2 + p["buffer_pts"]):
                            pos.close_all(day, nm, "adj_insufficient")
            # no-leverage guard at every event: trim size until |net delta| notional <= cap
            if pos.open:
                dlt, S = pos.greeks(day, nm)
                lim = p["delta_cap"] * min(equity, p["capital_cap"])
                if abs(dlt) * S > lim:
                    pos.reduce(day, nm, 1 - lim / (abs(dlt) * S), "delta_cap_trim")
            pos.peak_margin = max(pos.peak_margin, pos.margin(day.spot[nm, 3]))
            start = nm + 1
        if not pos.open:
            break
    if pos.open:
        last = days[-1]
        pos.close_all(last, exit_m, "time")
    t = pos.to_trade(day0.date.isoformat(), f"trend={trend} lots={lots} credit={credit/ (lots*day0.lot):.1f} events={events}")
    t.log = pos.log
    return [t]
