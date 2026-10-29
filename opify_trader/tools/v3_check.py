import datetime as dt, csv, glob, subprocess, sys, shutil, os
import ensemble as E
if __name__=="__main__":
    for d in ["../books/v3_train_engine","../books/v3_selftest"]: shutil.rmtree(d, ignore_errors=True)
    s=E.forward(dt.date(2023,1,1),dt.date(2025,12,31),"v3_train_engine")
    print("engine v3 2023-25:",{k:s['options_only'][k] for k in ['cagr%','maxdd%','sharpe','trades']},flush=True)
    out=subprocess.run([sys.executable,"monitor.py","run","--from","2023-01-02 09:15","--to","2025-12-31 15:29","--book","v3_selftest"],capture_output=True,text=True).stdout
    print([l for l in out.splitlines() if "ERROR" in l or "done" in l])
    m=list(csv.DictReader(open('../books/v3_selftest/closed_trades.csv')))
    mk={(r['opened'][:10],r['structure'].replace(' / ',' | ')):(r['closed'],r['reason']) for r in m}
    ek={}
    for f in sorted(glob.glob('../books/v3_train_engine/sleeve_dte*/')):
        T=list(csv.DictReader(open(f+'trades.csv'))); L=list(csv.DictReader(open(f+'legs.csv')))
        for t in T:
            lg=[l for l in L if l['trade_id']==t['id']]
            ek[(t['open_date'],t['legs'])]=(max(l['exit_time'] for l in lg),lg[0]['exit_reason'])
    om=sorted(set(mk)-set(ek)); oe=sorted(set(ek)-set(mk))
    print("common",len(set(mk)&set(ek)),"only monitor after Feb-23",[k for k in om if k[0]>'2023-02-01'],"only engine",oe,"exit mismatches",sum(mk[k]!=ek[k] for k in set(mk)&set(ek)))
