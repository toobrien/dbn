import  plotly.graph_objects    as      go
import  polars                  as      pl
from    sys                     import  argv, path
from    time                    import  time

path.append(".")

from    util                    import  read_storage, strptime


# python scripts/sweep.py 20240301_esh4_mbo 5


pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_rows(-1)


if __name__ == "__main__":

    t0      = time()
    fn      = argv[1]
    min_len = int(argv[2])
    df      = read_storage(fn).with_row_index()
    df      = strptime(df, "ts_event", "ts", "%Y-%m-%dT%H:%M:%S.%f", -8)
    
    groups  = df.filter(
                    (pl.col("action") == "T") | (pl.col("action") == "F")
                ).group_by(
                    [ "ts" ]
                )
    
    for _, group in groups:

        if group["price"].n_unique() > min_len:
        
            print(group.select([ "ts", "price", "size", "side" ]))

    print(f"{time() - t0:0.1f}s")