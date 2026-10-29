"""Opify simulator data client.

The Opify option simulator (https://www.opify.in/simulator/dashboard) loads its
historical data from a public endpoint:

    GET {BASE}?symbol=NIFTY&expiry=03NOV2026              -> {"all_timestamp": [dd-mm-yyyy, ...]}
    GET {BASE}?symbol=NIFTY&expiry=03NOV2026&from=dd-mm-yyyy -> gzip JSON, one trading day of 1-min bars

Day payload:
    meta:    symbol, expiry, lot_size, strike_step, prev_close, from/to (epoch s)
    spot:    time_index[epoch s], ohlc[[o,h,l,c,vol,chg,chg%]]
    strikes: strikes[...], ce_ohlc[strike][minute] = [o,h,l,c,vol,oi], pe_ohlc likewise

Usage:
    python opify_data.py expiries 2025-01-01 2026-12-31     # discover expiries
    python opify_data.py download 2025-01-01 2026-12-31 --days 5
"""
import argparse
import datetime as dt
import gzip
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = "https://swcxiyinhb.execute-api.ap-south-1.amazonaws.com/default/simulator-data"
ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
RAW = os.path.join(ROOT, "raw")


def _get(url, retries=4):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=60) as r:
                b = r.read()
            if b[:2] == b"\x1f\x8b":
                b = gzip.decompress(b)
            return json.loads(b)
        except Exception as e:  # network hiccups / 5xx
            if i == retries - 1:
                raise
            time.sleep(1.5 * (i + 1))


def expiry_code(d):
    return d.strftime("%d%b%Y").upper()


def parse_day(s):
    s = str(s)
    if s.isdigit():  # older expiries list epoch seconds (IST midnight)
        return (dt.datetime.utcfromtimestamp(int(s)) + dt.timedelta(hours=5, minutes=30)).date()
    return dt.datetime.strptime(s, "%d-%m-%Y").date()


def trading_days(symbol, expiry):
    """Trading dates that have data for this expiry (sorted), or [] if not an expiry."""
    try:
        j = _get(f"{BASE}?symbol={symbol}&expiry={expiry}")
    except Exception:
        return []
    return sorted(parse_day(x) for x in j.get("all_timestamp", []))


def day_payload(symbol, expiry, day):
    return _get(f"{BASE}?symbol={symbol}&expiry={expiry}&from={day.strftime('%d-%m-%Y')}")


def discover_expiries(symbol, start, end, workers=16):
    days = [start + dt.timedelta(n) for n in range((end - start).days + 1)]
    days = [d for d in days if d.weekday() < 5]
    with ThreadPoolExecutor(workers) as ex:
        res = list(ex.map(lambda d: (d, trading_days(symbol, expiry_code(d))), days))
    out = {expiry_code(d): [x.isoformat() for x in t] for d, t in res if t}
    path = os.path.join(ROOT, f"expiries_{symbol}.json")
    old = json.load(open(path)) if os.path.exists(path) else {}
    old.update(out)
    json.dump(dict(sorted(old.items(), key=lambda kv: dt.datetime.strptime(kv[0], "%d%b%Y"))), open(path, "w"), indent=0)
    return old


def raw_path(symbol, expiry, day):
    return os.path.join(RAW, symbol, expiry, day.isoformat() + ".json.gz")


def fetch_day(symbol, expiry, day, force=False):
    p = raw_path(symbol, expiry, day)
    if os.path.exists(p) and not force:
        return p
    j = day_payload(symbol, expiry, day)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with gzip.open(p, "wt") as f:
        json.dump(j, f)
    return p


def load_day(symbol, expiry, day):
    with gzip.open(raw_path(symbol, expiry, day), "rt") as f:
        return json.load(f)


def download(symbol, start, end, days_before=5, workers=8):
    exps = json.load(open(os.path.join(ROOT, f"expiries_{symbol}.json")))
    jobs = []
    for code, ds in exps.items():
        e = dt.datetime.strptime(code, "%d%b%Y").date()
        if not (start <= e <= end):
            continue
        for d in [parse_day_iso(x) for x in ds][-days_before:]:
            jobs.append((code, d))
    print(f"{len(jobs)} day files", file=sys.stderr)

    def job(a):
        try:
            fetch_day(symbol, a[0], a[1])
            return None
        except Exception as e:
            return (a, str(e))

    with ThreadPoolExecutor(workers) as ex:
        errs = [r for r in ex.map(job, jobs) if r]
    print(f"done, {len(errs)} errors", errs[:5], file=sys.stderr)


def parse_day_iso(s):
    return dt.date.fromisoformat(s)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["expiries", "download"])
    ap.add_argument("start")
    ap.add_argument("end")
    ap.add_argument("--symbol", default="NIFTY")
    ap.add_argument("--days", type=int, default=5)
    a = ap.parse_args()
    s, e = dt.date.fromisoformat(a.start), dt.date.fromisoformat(a.end)
    if a.cmd == "expiries":
        ex = discover_expiries(a.symbol, s, e)
        print(len(ex), "expiries;", list(ex)[:3], "...", list(ex)[-3:])
    else:
        download(a.symbol, s, e, a.days)
