"""Per-setup return on MARGIN and on sleeve capital, TRAIN months only, structure variants on v2."""
import json, os, sys
from multiprocessing import Pool
import numpy as np
import regime_research as R
from ensemble import frozen

def _sl(args):
    from strategies import run
    p,dte=args
    b=run("trend_spread",{**p,"enter_dte":dte},R._d,*R.SPAN,capital=R.CAPITAL/3)
    return [(t.open_date[:7],t.net(),t.margin) for t in b.trades]

def stats(p,pool):
    months=[m["month"] for m in R.SPLIT["months"] if m["set"]=="TRAIN" and m["month"]<="2025-12"]
    res=pool.map(_sl,[({**p,"months":months},s) for s in R.SLEEVES])
    t=[x for r in res for x in r]
    net=np.array([x[1] for x in t]); mg=np.array([x[2] for x in t])
    ms=R.regime_stats(p,pool)
    return {"setups":len(t),"net/margin%":round(100*(net/mg).mean(),2),"net/sleeve%":round(100*net.mean()/(R.CAPITAL/3),2),
            "win%":round(100*(net>0).mean(),1),"worst_setup/margin%":round(100*(net/mg).min(),1),
            "monthly":{r:ms[r]['strat_mean%'] for r in ms}}

if __name__=="__main__":
    v2,_,_=frozen()
    C={"v2 vertical":v2}
    for k in [1.2,1.5,2.0]: C[f"+opp spread @{k}s"]={**v2,"opp_k":k}
    with Pool(3,initializer=R._init) as pool:
        for n,p in C.items(): print(f"{n:20s}",stats(p,pool),flush=True)
