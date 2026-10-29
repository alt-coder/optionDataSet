from multiprocessing import Pool
import regime_research as R
from ensemble import frozen
if __name__=="__main__":
    v2,_,_=frozen()
    base={**v2,"strong_em":1.0,"strong_k":0.6,"strong_side":"both"}
    notch={**R.NOTCH,"strong_em":[0.5,1.5],"strong_k":[0.5,0.7],"rich_iv":[0.10,0.125],"rich_ratio":[1.1,1.35]}
    res=[]
    with Pool(3,initializer=R._init) as pool:
        ms=R.regime_stats(base,pool); print("CANDIDATE",{r:ms[r]['strat_mean%'] for r in ms},{r:ms[r]['pos_months%'] for r in ms},flush=True)
        for k,vals in notch.items():
            for v in vals:
                m2=R.regime_stats({**base,k:v},pool); ok=all(m2[r]['strat_mean%']>0 for r in ("UP","DOWN","RANGE")); res.append(ok)
                print(f"  {k}={v}",{r:m2[r]['strat_mean%'] for r in m2},"PASS" if ok else "fail",flush=True)
    print("plateau",round(sum(res)/len(res),2))
