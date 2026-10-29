"""Strategy library. Each strategy: fn(data, d, equity, p) -> list[Trade] closed (may span days)."""
import datetime as dt

import numpy as np

from engine import Book, Leg, Trade, bar, margin_estimate, minute_of, mtime
from sim import LAST, Data, ema, lots_for, open_legs, run_position


def _days_to_expiry(data, d):
    """Consecutive trading days from d until its expiry (same expiry code)."""
    e, _ = data.by_date[d]
    exp_date = dt.datetime.strptime(e, "%d%b%Y").date()
    out = []
    for x in data.dates[data.dates.index(d):]:
        if x > exp_date:
            break
        if data.by_date[x][0] != e:
            continue  # e.g. a special session mapped to another expiry: skip it, keep holding
        out.append(data.day(x))
    return out


def _probe_margin(data, d, m, spec):
    """Margin per 1 lot for a spec (uses 1-lot dry run prices, no fills recorded)."""
    day = data.day(d)
    legs = []
    atm = day.atm(m - 1)
    for kind, off, sign in spec:
        k = atm + off * day.step * (1 if kind == "CE" else -1)
        b = bar(day, Leg(kind, k, 0), m - 1)      # size from the signal bar (known before the order is sent)
        if b is None or day.idx(k) is None or bar(day, Leg(kind, k, 0), m) is None:
            return None
        l = Leg(kind, k, sign * day.lot)
        l.entry = b[3]
        legs.append(l)
    return margin_estimate(legs, day.spot[m - 1, 3], day.lot)


def _trade(name, day, legs, lots, margin, notes=""):
    t = Trade(name, day.expiry, legs, open_date=day.date.isoformat(), margin=margin * lots, notes=notes)
    t.close_date = max(l.exit_t for l in legs)[:10]
    return t


# --------------------------------------------------------------- 1. intraday hedged short straddle
def short_straddle(data, d, equity, p):
    """Sell ATM CE+PE at entry, buy wings `wing` strikes OTM. Per-leg SL; exit at `exit`."""
    _, dte = data.by_date[d]
    if p.get("dte") is not None and dte not in p["dte"]:
        return []
    day = data.day(d)
    if p.get("gap_max") is not None and abs(day.spot[0, 0] / day.prev_close - 1) > p["gap_max"]:
        return []
    m = minute_of(p.get("entry", "09:20"))
    w = p.get("wing", 8)
    spec = [("CE", 0, -1), ("PE", 0, -1)] + ([("CE", w, +1), ("PE", w, +1)] if w else [])
    mpl = _probe_margin(data, d, m, spec)
    if not mpl:
        return []
    lots = lots_for(equity, mpl, p.get("alloc", 0.5))
    if lots == 0:
        return []
    legs = open_legs(day, m, spec, lots)
    if not legs:
        return []
    credit = sum(-l.entry * l.qty for l in legs) / (lots * day.lot)
    run_position([day], m + 1, legs, minute_of(p.get("exit", "15:15")), leg_sl=p.get("leg_sl", 0.3),
                 combo_sl=p.get("combo_sl_x") and credit * p["combo_sl_x"],
                 combo_tp=p.get("tp_x") and credit * p["tp_x"],
                 sl_to_cost=p.get("sl_to_cost", False), hedge_follow=p.get("hedge_follow", False))
    return [_trade("short_straddle", day, legs, lots, mpl, f"dte={dte} credit={credit:.1f}")]


# --------------------------------------------------------------- 2. positional iron fly to expiry
def iron_fly(data, d, equity, p):
    _, dte = data.by_date[d]
    if dte != p.get("enter_dte", 3):
        return []
    days = _days_to_expiry(data, d)
    day = days[0]
    m = minute_of(p.get("entry", "15:00"))
    w = p.get("wing", 6)
    spec = [("CE", 0, -1), ("PE", 0, -1), ("CE", w, +1), ("PE", w, +1)]
    mpl = _probe_margin(data, d, m, spec)
    if not mpl:
        return []
    lots = lots_for(equity, mpl, p.get("alloc", 0.5))
    if lots == 0:
        return []
    legs = open_legs(day, m, spec, lots)
    if not legs:
        return []
    credit = sum(-l.entry * l.qty for l in legs) / (lots * day.lot)
    run_position(days, m + 1, legs, minute_of(p.get("exit", "15:15")),
                 combo_sl=credit * p.get("sl_x", 0.5), combo_tp=credit * p.get("tp_x", 0.5))
    return [_trade("iron_fly", day, legs, lots, mpl, f"dte={dte} credit={credit:.1f}")]


# --------------------------------------------------------------- 3. opening range breakout option buy
def orb_buy(data, d, equity, p):
    _, dte = data.by_date[d]
    if p.get("dte") is not None and dte not in p["dte"]:
        return []
    day = data.day(d)
    R = p.get("range_min", 30)
    hi, lo = day.spot[:R, 1].max(), day.spot[:R, 2].min()
    rng = hi - lo
    if rng <= 0 or rng / day.spot[0, 0] > p.get("max_range", 0.01):
        return []
    last_entry = minute_of(p.get("last_entry", "13:00"))
    for m in range(R, last_entry):
        c = day.spot[m, 3]
        dirn = 1 if c > hi else -1 if c < lo else 0
        if not dirn:
            continue
        kind = "CE" if dirn > 0 else "PE"
        itm = p.get("itm", 0)
        spec = [(kind, -itm, +1)]
        atm = day.atm(m)
        k = atm - itm * day.step * (1 if kind == "CE" else -1)
        b = bar(day, Leg(kind, k, 0), m + 1)
        if b is None:
            return []
        risk_budget = equity * p.get("risk", 0.01)
        stop_lvl = (hi + lo) / 2 if p.get("stop", "mid") == "mid" else (lo if dirn > 0 else hi)
        # premium risk per unit ~ delta(0.5..0.6) * spot distance to stop + slippage
        est = 0.55 * abs(c - stop_lvl) + 2
        lots = int(min(risk_budget / (est * day.lot), equity * p.get("alloc", 0.3) / (b[0] * day.lot)))
        if lots <= 0:
            return []
        legs = open_legs_at(day, m + 1, [(kind, k, +1)], lots)
        if not legs:
            return []
        tgt = c + dirn * p.get("tgt_r", 2.0) * abs(c - stop_lvl) if p.get("tgt_r") else None
        run_position([day], m + 2, legs, minute_of(p.get("exit", "15:15")), spot_stop=(dirn, stop_lvl, tgt))
        return [_trade("orb_buy", day, legs, lots, b[0] * day.lot, f"dir={dirn} rng={rng:.0f}")]
    return []


def open_legs_at(day, m, spec_abs, lots):
    from engine import fill
    legs = []
    for kind, k, sign in spec_abs:
        b = bar(day, Leg(kind, k, 0), m)
        if b is None:
            return None
        l = Leg(kind, k, sign * lots * day.lot)
        fill(l, b[0], 1 if sign > 0 else -1, day.date, f"{day.date} {mtime(m)}")
        legs.append(l)
    return legs


# --------------------------------------------------------------- 4. trend-filtered credit spread to expiry
def trend_spread(data, d, equity, p):
    _, dte = data.by_date[d]
    if dte != p.get("enter_dte", 3):
        return []
    sd = data.spot_daily()
    i = data.dates.index(d)
    n = p.get("ema", 20)
    if i < n + 1:
        return []
    closes = np.array([sd[x][3] for x in data.dates[: i]])  # up to yesterday (no lookahead)
    e = ema(closes, n)[-1]
    days = _days_to_expiry(data, d)
    day = days[0]
    m = minute_of(p.get("entry", "15:00"))
    trend = 1 if day.spot[m - 1, 3] > e else -1
    off, w = p.get("short_off", 2), p.get("wing", 4)
    kind = "PE" if trend > 0 else "CE"
    if p.get("mode") == "concept":
        # vol-scaled: short at k_sigma x expected move (1 sigma ~ 1.25 x ATM straddle), wing wing_sigma x EM
        from adaptive import market_view
        mv = market_view(day, m - 1, day.exp_date, closes)
        if not np.isfinite(mv["em"]) or mv["em"] <= 0:
            return []
        if p.get("vrp_min") and mv["rv10"] and mv["atm_iv"] < p["vrp_min"] * mv["rv10"]:
            return []
        if p.get("rich_iv") is not None:
            # premium-regime sizing (vol study): short premium only pays when IV is rich -> cut size when it is fair
            rich = mv["atm_iv"] >= p["rich_iv"] or (mv["rv10"] and mv["atm_iv"] >= p.get("rich_ratio", 9) * mv["rv10"])
            if not rich:
                p = {**p, "alloc": p.get("alloc", 0.5) * p.get("fair_scale", 0.5)}
                if p["alloc"] <= 0:
                    return []
        atm = day.atm(m - 1)
        k_use = p.get("k_sigma", 0.6)
        if p.get("strong_em") is not None:
            # lean into CONFIRMED strength: far beyond the EMA and EMA rising/falling the same way -> sell closer
            e_prev = ema(closes[:-5], n)[-1] if len(closes) > n + 5 else e
            strong = abs(day.spot[m - 1, 3] - e) > p["strong_em"] * mv["em"] and ((trend > 0) == (e > e_prev))
            if strong and (p.get("strong_side", "both") == "both" or (p["strong_side"] == "up" and trend > 0)):
                k_use = p.get("strong_k", 0.6)
        off = max(1, int(round(k_use * mv["em"] / day.step)))
        w = max(1, int(round(p.get("wing_sigma", 0.8) * mv["em"] / day.step)))
    spec = [(kind, off, -1), (kind, off + w, +1)]
    if p.get("mode") == "concept" and p.get("opp_k"):
        # margin-efficient extra credit: far-OTM spread on the OTHER side (iron condor). SPAN only charges the
        # worse side, so the extra spread adds credit with ~no extra margin.
        okind = "CE" if kind == "PE" else "PE"
        oo = max(1, int(round(p["opp_k"] * mv["em"] / day.step)))
        ow = max(1, int(round(p.get("opp_wing_sigma", p.get("wing_sigma", 0.8)) * mv["em"] / day.step)))
        atm0 = day.atm(m - 1)
        osg = 1 if okind == "CE" else -1
        if day.idx(atm0 + osg * oo * day.step) is not None and day.idx(atm0 + osg * (oo + ow) * day.step) is not None:
            spec = spec + [(okind, oo, -1), (okind, oo + ow, +1)]
        # else: far strike outside the data window -> trade the plain 2-leg spread (never silently skip the setup)
    if p.get("mode") == "concept" and p.get("slope_confirm"):
        # trend confirmation: EMA must be moving in the same direction as price's side of it (avoid selling into a lagging EMA)
        e_prev = ema(closes[:-p["slope_confirm"]], n)[-1] if len(closes) > n + p["slope_confirm"] else e
        if (trend > 0) != (e > e_prev):
            return []
    if p.get("mode") == "concept" and p.get("er_range"):
        # choppy market (low 10-day efficiency ratio): no trend to ride -> two-sided condor further out
        c10 = closes[-11:]
        path = np.abs(np.diff(c10)).sum()
        er = abs(c10[-1] - c10[0]) / path if path > 0 else 0
        if er < p["er_range"]:
            if p.get("range_action") == "skip":
                return []
            trend = 0
            ko = max(1, int(round(p.get("range_k", 1.0) * mv["em"] / day.step)))
            spec = [("CE", ko, -1), ("CE", ko + w, +1), ("PE", ko, -1), ("PE", ko + w, +1)]
    if p.get("mode") == "concept" and p.get("neutral_band"):
        # trend ambiguous (spot within band x EM of the EMA): no side has an edge -> sell BOTH sides (iron condor)
        if abs(day.spot[m - 1, 3] - e) < p["neutral_band"] * mv["em"]:
            trend = 0
            spec = [("CE", off, -1), ("CE", off + w, +1), ("PE", off, -1), ("PE", off + w, +1)]
    mpl = _probe_margin(data, d, m, spec)
    if not mpl:
        return []
    lots = lots_for(equity, mpl, p.get("alloc", 0.5))
    if lots == 0:
        return []
    legs = open_legs(day, m, spec, lots)
    if not legs:
        return []
    credit = sum(-l.entry * l.qty for l in legs) / (lots * day.lot)
    exit_m = minute_of(p.get("exit", "15:15"))
    all_legs, rolls = list(legs), 0
    cur_days, cur_m, cur_legs, cur_credit = days, m + 1, legs, credit
    while True:
        roll_stop = None
        if p.get("roll_at") is not None and trend != 0 and rolls < p.get("max_rolls", 1):
            # REBALANCE trigger: spot within roll_at x EM of the (single) short strike
            sk = [l for l in cur_legs if l.qty < 0][0].strike
            lvl = sk + p["roll_at"] * mv["em"] if kind == "PE" else sk - p["roll_at"] * mv["em"]
            roll_stop = (1 if kind == "PE" else -1, lvl, None)
        run_position(cur_days, cur_m, cur_legs, exit_m, combo_sl=cur_credit * p.get("sl_x", 1.0),
                     combo_tp=cur_credit * p.get("tp_x", 0.8), check_every=p.get("check_every", 1), spot_stop=roll_stop)
        if roll_stop is None or not any(l.reason == "spot_sl" for l in cur_legs):
            break
        # re-sell a fresh k-sigma spread on the same side from the bar after the roll exit
        xt = max(l.exit_t for l in cur_legs)
        xd, xm = dt.date.fromisoformat(xt[:10]), minute_of(xt[11:])
        rest = [x for x in cur_days if x.date >= xd]
        if not rest or xm + 1 >= exit_m and rest[0].date == rest[-1].date:
            break
        d0 = rest[0]
        mv = market_view(d0, xm, d0.exp_date)
        if not np.isfinite(mv["em"]) or mv["em"] <= 0:
            break
        off2 = max(1, int(round(p.get("k_sigma", 0.8) * mv["em"] / d0.step)))
        w2 = max(1, int(round(p.get("wing_sigma", 0.8) * mv["em"] / d0.step)))
        new = open_legs(d0, xm + 1, [(kind, off2, -1), (kind, off2 + w2, +1)], lots)
        if not new:
            break
        rolls += 1
        for l in new:
            l.reason = "rolled_in"
        cur_days, cur_m, cur_legs = rest, xm + 2, new
        cur_credit = sum(-l.entry * l.qty for l in new) / (lots * day.lot)
        all_legs += new
    return [_trade("trend_spread", day, all_legs, lots, mpl, f"trend={trend} credit={credit:.1f} rolls={rolls}")]


STRATS = {"short_straddle": short_straddle, "iron_fly": iron_fly, "orb_buy": orb_buy, "trend_spread": trend_spread}


# --------------------------------------------------------------- runner
def run(name, p, data, start, end, capital=1_000_000, book_name=None):
    fn = STRATS[name]
    book = Book(book_name or name, capital)
    busy_until, pending = None, []
    allowed = set(p.get("months") or [])
    for d in data.dates:
        if d < start or d > end:
            continue
        if (busy_until is None or d > busy_until) and (not allowed or d.strftime("%Y-%m") in allowed):
            try:
                trades = fn(data, d, book.cash, p)
            except Exception as ex:  # data gaps: skip the day, but record why
                trades = []
                book.skipped.append((d.isoformat(), repr(ex)))
            for t in trades:
                pending.append(t)
                busy_until = dt.date.fromisoformat(t.close_date)
                if busy_until <= d:
                    busy_until = None
        # realise P&L only on the trade's close date (no lookahead in the equity curve)
        for t in [t for t in pending if dt.date.fromisoformat(t.close_date) <= d]:
            book.close(t)
            pending.remove(t)
        book.mark(d)
    for t in pending:
        book.close(t)
    return book


from adaptive import run_adaptive  # noqa: E402
STRATS["adaptive"] = run_adaptive
