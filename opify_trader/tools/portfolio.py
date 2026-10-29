"""3-index no-leverage portfolio on TEST months: 1/3 capital per index, margin up to 100% of each third
(=> total margin <= capital). Approximated from the alloc-0.5 / 26L books: each index's monthly return x (1/3)*(1.0/0.5).
When an index has no data (SENSEX before Apr-2024) its third is split across the others (same scale factor)."""
import csv, glob, json, os
from collections import defaultdict
import numpy as np
import costs
HERE=os.path.dirname(os.path.abspath(__file__)); B=os.path.join(HERE,"..","books")
SPLIT=json.load(open(os.path.join(HERE,"..","params","split.json"))); CAP=2_600_000
books={"NIFTY":"exam_v2_engine","SENSEX":"ma_SENSEX_TEST","BANKNIFTY":"ma_BANKNIFTY_TEST"}
first={"NIFTY":"2023-01","SENSEX":"2024-04","BANKNIFTY":"2023-01"}
pnl={s:defaultdict(float) for s in books}; nexp={s:defaultdict(set) for s in books}
for s,tag in books.items():
    for f in glob.glob(os.path.join(B,tag,"sleeve_dte*","trades.csv")):
        for t in csv.DictReader(open(f)): pnl[s][t["open_date"][:7]]+=float(t["net"]); nexp[s][t["open_date"][:7]].add(t["expiry"])
test=[m for m in SPLIT["months"] if m["set"]=="TEST"]
eq,rows=CAP,[]
for m in test:
    mm=m["month"]; live=[s for s in books if mm>=first[s]]
    r=sum(pnl[s][mm]/CAP for s in live)*(1/len(live))*2   # each index on 1/len(live) of capital at 2x the alloc
    exps=sum(len(nexp[s][mm]) for s in live)
    rows.append((mm,m["regime"],r,m["ret%"]/100,exps)); eq*=1+r
s=np.array([x[2] for x in rows]); n=np.array([x[3] for x in rows]); k=len(rows)
curve=np.cumprod(1+s); dd=(curve/np.maximum.accumulate(np.r_[1,curve])[1:]-1).min()
ann=(curve[-1])**(12/k)-1; annN=np.prod(1+n)**(12/k)-1
reg=defaultdict(list)
for mm,rg,r,_,_ in rows: reg[rg].append(r*100)
rng=np.random.default_rng(1); idx=rng.integers(0,k,(10000,12)); ys=np.prod(1+s[idx],1)-1; yn=np.prod(1+n[idx],1)-1
weeks=sum(x[4] for x in rows)
out={"test_months":k,"compounded%":round((curve[-1]-1)*100,1),"annualised_pre_tax%":round(ann*100,1),"annualised_post_tax%":round(ann*(1-costs.FNO_TAX)*100,1),
 "nifty_annualised%":round(annN*100,1),"maxDD_monthly%":round(dd*100,1),"worst_month%":round(s.min()*100,2),"pos_months%":round(100*(s>0).mean()),
 "regime_mean%/mo":{k2:round(sum(v)/len(v),2) for k2,v in reg.items()},
 "avg_return_per_expiry_traded%":round(float(np.sum(s))*100/weeks,3),"expiries_traded":weeks,
 "bootstrap":{"median%":round(float(np.median(ys))*100,1),"p5%":round(float(np.percentile(ys,5))*100,1),"P(loss year)":round(float((ys<0).mean()),3),"P(beat nifty)":round(float((ys>yn).mean()),3)}}
json.dump(out,open(os.path.join(HERE,"..","results","portfolio_3index_v2.json"),"w"),indent=1); print(json.dumps(out,indent=1))
