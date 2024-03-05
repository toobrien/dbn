import  numpy                   as      np
import  plotly.graph_objects    as      go
import  polars                  as      pl
from    sys                     import  argv, path
from    time                    import  time

path.append(".")

from    util                    import  read_storage


# python scripts/order_life.py 20240301_esh4_mbo


if __name__ == "__main__":

    pl.Config.set_tbl_cols(-1)
    pl.Config.set_tbl_rows(-1)

    fn      = argv[1]
    min_qty = int(argv[2])

    df  = read_storage(fn)
    ids = list(df.filter((pl.col("size") >= min_qty) & (pl.col("action") == "A")).select("order_id").unique())
    
    for id in ids:

        print(id)

    pass