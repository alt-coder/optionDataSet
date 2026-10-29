from multiprocessing import Pool
import regime_research as R
from ensemble import frozen
if __name__=="__main__":
    v2,_,_=frozen()
    C={"v2":v2}
    for side in ["up","both"]:
        for sem in [0.5,1.0,1.5]:
            for sk in [0.5,0.6]:
                C[f"strong {side} >{sem}EM k={sk}"]={**v2,"strong_em":sem,"strong_k":sk,"strong_side":side}
    with Pool(3,initializer=R._init) as pool:
        for n,p in C.items():
            ms=R.regime_stats(p,pool)
            ok=all(ms[r]['strat_mean%']>0 for r in ("UP","DOWN","RANGE"))
            print(f"{n:26s}",{r:ms[r]['strat_mean%'] for r in ms},"PASS" if ok else "fail",flush=True)
