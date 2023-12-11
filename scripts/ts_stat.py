import  numpy                   as      np
import  plotly.graph_objects    as      go
import  polars                  as      pl
from    sys                     import  argv, path
from    time                    import  time

path.append(".")

from    util                    import  read_batch


# python scripts/ts_stat.py GLBX-20231125-WA5564KMNG glbx-mdp3-20231123.mbp-1.dbn.zst RBH4 RBM4 RBU4 RBZ4 RBH4-RBM4


if __name__ == "__main__":

    t0      = time()
    job_id  = argv[1]
    fn      = argv[2]
    symbols = argv[3:]
    all     = read_batch(job_id, fn)

    print(f"{'symbol':10}{'mean':10}{'std':10}{'5%':10}{'15%':10}{'50%':10}{'85%':10}{'95%':10}")

    for sym in symbols:

        data = all.filter(pl.col("symbol") == sym).to_numpy()

        ts_diff = np.diff(data[:, 0]) / 1e6
        
        ts_diff.sort()

        n       = len(ts_diff)
        ts_mean = int(np.mean(ts_diff))
        ts_std  = int(np.std(ts_diff))
        p_5     = int(ts_diff[int(n * 0.05)])
        p_15    = int(ts_diff[int(n * 0.15)])
        p_50    = int(ts_diff[int(n * 0.50)])
        p_85    = int(ts_diff[int(n * 0.85)])
        p_95    = int(ts_diff[int(n * 0.95)])

        print(f"{sym:10}{ts_mean:<10}{ts_std:<10}{p_5:<10}{p_15:<10}{p_50:<10}{p_85:<10}{p_95:<10}")


    print(f"elapsed {time() - t0:0.2f}s")

