from multiprocessing import Pool
import regime_research as R
from setup_study import stats
from ensemble import frozen
if __name__=="__main__":
    v2,_,_=frozen(); base={**v2,"opp_k":2.0}
    V={"base opp 2.0":base,"opp 1.75":{**base,"opp_k":1.75},"opp 2.5":{**base,"opp_k":2.5},
       "opp wing 0.5":{**base,"opp_wing_sigma":0.5},"opp wing 1.2":{**base,"opp_wing_sigma":1.2},
       "k 0.6":{**base,"k_sigma":0.6},"k 1.0":{**base,"k_sigma":1.0},"tp 0.6":{**base,"tp_x":0.6},"tp 0.9":{**base,"tp_x":0.9},
       "sl 0.75":{**base,"sl_x":0.75},"sl 1.5":{**base,"sl_x":1.5}}
    with Pool(3,initializer=R._init) as pool:
        for n,p in V.items():
            s=stats(p,pool); ok=all(s['monthly'][r]>0 for r in ("UP","DOWN","RANGE"))
            print(f"{n:14s} net/margin {s['net/margin%']}%  monthly {s['monthly']}  {'PASS' if ok else 'fail'}",flush=True)
