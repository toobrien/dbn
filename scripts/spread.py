import  numpy                   as      np
import  plotly.graph_objects    as      go
import  polars                  as      pl
from    sys                     import  argv, path
from    time                    import  time

path.append(".")

from    util                    import  read_storage


# python scripts/spread.py nqh4_sample


if __name__ == "__main__":

    t0  = time()
    df  = read_storage(argv[1])

    pass

    print(f"elapsed: {time() - t0:0.1f}s")

    
    