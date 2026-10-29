import datetime as dt, json, sys
from multiprocessing import Pool
import ensemble as E
if __name__=="__main__":
    base={**E.BASE,"check_every":30}
    vs=[("v1 (frozen)",{}),
        ("rich iv>=0.114 or iv/rv>=1.23, fair x0.5",{"rich_iv":0.114,"rich_ratio":1.23,"fair_scale":0.5}),
        ("rich iv>=0.10 or iv/rv>=1.1, fair x0.5",{"rich_iv":0.10,"rich_ratio":1.1,"fair_scale":0.5}),
        ("rich iv>=0.13 or iv/rv>=1.4, fair x0.5",{"rich_iv":0.13,"rich_ratio":1.4,"fair_scale":0.5}),
        ("rich iv>=0.114 or iv/rv>=1.23, fair x0 (skip)",{"rich_iv":0.114,"rich_ratio":1.23,"fair_scale":0.0}),
        ("rich iv>=0.114 only, fair x0.5",{"rich_iv":0.114,"rich_ratio":9,"fair_scale":0.5})]
    with Pool(3,initializer=E._init,initargs=(E.TRAIN,)) as pool:
        for name,v in vs:
            s=E.summary(E.evaluate({**base,**v},E.TRAIN,pool),E.TRAIN)
            o,l=s["options_only"],s["with_liquid_yield"]
            print(f"{name:48s}",{k:o[k] for k in ["cagr%","maxdd%","sharpe","trades"]},o["yearly"],"| +liq",l["cagr%"],flush=True)
