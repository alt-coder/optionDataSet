"""Return per EXPIRY vs capital usage (TRAIN months only, NIFTY, frozen v2 rules; alloc varies)."""
import json, os
from collections import defaultdict
from multiprocessing import Pool
import numpy as np
import regime_research as R
from ensemble import frozen

def run(alloc, pool):
    v2,_,_=frozen()
    months=[m["month"] for m in R.SPLIT["months"] if m["set"]=="TRAIN" and m["month"]<="2025-12"]
    res=pool.map(R._sleeve_full,[({**v2,"alloc":alloc,"months":months},s) for s in R.SLEEVES])
    by=defaultdict(float); seq=[]
    for r in res:
        for exp,od,net in r: by[exp]+=net
    pnl=np.array([by[k] for k in sorted(by,key=lambda c:__import__('datetime').datetime.strptime(c,'%d%b%Y'))])
    eq=R.CAPITAL+np.cumsum(pnl); dd=(eq/np.maximum.accumulate(np.r_[R.CAPITAL,eq][1:].clip(min=1))-1)
    peak=np.maximum.accumulate(np.r_[R.CAPITAL,eq]); ddv=((np.r_[R.CAPITAL,eq])/peak-1).min()
    per=pnl/R.CAPITAL*100
    return {"alloc":alloc,"expiries":len(per),"mean%/expiry":round(per.mean(),3),"median%":round(float(np.median(per)),3),
            "worst%":round(per.min(),2),"p5%":round(float(np.percentile(per,5)),2),"pos%":round(100*(per>0).mean(),0),"maxDD%":round(ddv*100,1),
            "annualised_simple%":round(per.mean()*50,1)}

if __name__=="__main__":
    with Pool(3,initializer=R._init) as pool:
        for a in [0.5,0.75,1.0]:
            print(run(a,pool),flush=True)
