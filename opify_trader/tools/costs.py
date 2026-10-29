"""Realistic Indian index-option (NSE NIFTY) transaction cost + slippage model.

All rates are applied per leg, per side, on premium turnover (price * qty).
Rates are date-dependent where regulation changed:

  STT (sell side, options premium):  0.0625% (<2024-10-01), 0.10% (>=2024-10-01), 0.15% (>=2026-04-01, Budget 2026)
  NSE transaction charge (options):  0.053%  (<2024-10-01), 0.03503% (>=2024-10-01, SEBI true-to-label)
  SEBI turnover fee:                  Rs 10 / crore
  IPFT (NSE investor protection):     Rs 0.50 / crore... negligible but included
  Stamp duty (buy side):              0.003%
  GST:                                18% on (brokerage + exchange txn + SEBI fee)
  Brokerage:                          Rs 20 flat per executed order (discount broker)

Slippage: no bid/ask in the Opify data, so every fill is moved against us by
    slip = SLIP_FIXED + SLIP_PCT * price   (rounded up to the 0.05 tick)
Deep OTM options also have a 0.05 minimum price floor.

Income tax (not a transaction cost) is handled at report time: F&O P&L is
non-speculative business income -> assumed 30% slab + 4% cess = 31.2% on positive yearly net P&L.
"""
import datetime as dt
import math

BROKERAGE_PER_ORDER = 20.0
SEBI_RATE = 10 / 1e7
IPFT_RATE = 0.5 / 1e7
STAMP_BUY = 0.00003
GST = 0.18
SLIP_FIXED = 0.10
SLIP_PCT = 0.005
TICK = 0.05
FNO_TAX = 0.312        # 30% slab + 4% cess
LTCG_TAX = 0.125       # equity LTCG (benchmark), above 1.25L exemption
LTCG_EXEMPT = 125000


def stt_sell(d: dt.date):
    if d >= dt.date(2026, 4, 1):
        return 0.0015
    if d >= dt.date(2024, 10, 1):
        return 0.0010
    return 0.000625


def txn_rate(d: dt.date):
    return 0.0003503 if d >= dt.date(2024, 10, 1) else 0.00053


def round_tick(x, up=True):
    f = math.ceil if up else math.floor
    return round(f(round(x / TICK, 6)) * TICK, 2)


def slipped(price, side):
    """Fill price after slippage. side: +1 buy, -1 sell."""
    s = round_tick(SLIP_FIXED + SLIP_PCT * price)
    p = price + s if side > 0 else price - s
    return max(TICK, round(p, 2))


def charges(price, qty, side, d: dt.date):
    """Statutory charges + brokerage for one order (one leg, one side). Returns rupees."""
    turnover = price * qty
    brk = BROKERAGE_PER_ORDER
    txn = turnover * txn_rate(d)
    sebi = turnover * SEBI_RATE
    ipft = turnover * IPFT_RATE
    stt = turnover * stt_sell(d) if side < 0 else 0.0
    stamp = turnover * STAMP_BUY if side > 0 else 0.0
    gst = GST * (brk + txn + sebi + ipft)
    return brk + txn + sebi + ipft + stt + stamp + gst


def breakdown(price, qty, side, d):
    turnover = price * qty
    txn = turnover * txn_rate(d)
    sebi = turnover * SEBI_RATE
    return {
        "brokerage": BROKERAGE_PER_ORDER,
        "stt": turnover * stt_sell(d) if side < 0 else 0.0,
        "exchange": txn + turnover * IPFT_RATE,
        "sebi": sebi,
        "stamp": turnover * STAMP_BUY if side > 0 else 0.0,
        "gst": GST * (BROKERAGE_PER_ORDER + txn + sebi + turnover * IPFT_RATE),
    }


def fno_tax(yearly_pnl: dict):
    """yearly_pnl {year: net pnl}. Losses carried forward up to 8 yrs vs business income."""
    tax, carry = {}, 0.0
    for y in sorted(yearly_pnl):
        p = yearly_pnl[y] + carry
        if p > 0:
            tax[y], carry = p * FNO_TAX, 0.0
        else:
            tax[y], carry = 0.0, p
    return tax


if __name__ == "__main__":
    d = dt.date(2026, 6, 1)
    # example: short ATM straddle 1 lot (65) @ 150 each, buy back @ 100
    q = 65
    tot = 0
    for p, s in [(150, -1), (150, -1), (100, 1), (100, 1)]:
        tot += charges(p, q, s, d)
    print("charges for 1-lot straddle round trip:", round(tot, 2))
    print("slipped sell 150 ->", slipped(150, -1), " buy 3.2 ->", slipped(3.2, 1))
