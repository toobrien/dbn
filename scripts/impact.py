from    enum                    import  IntEnum
from    os                      import  path
import  plotly.graph_objects    as      go
import  polars                  as      pl
from    sys                     import  argv, path
from    time                    import  time
from    typing                  import  Callable


path.append(".")

from    util                    import  strptime


pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_rows(-1)


class mbp(IntEnum):

    ts_event        = 0
    action          = 1
    side            = 2
    price           = 3
    size            = 4
    bid_px_00       = 5
    ask_px_00       = 6
    bid_sz_00       = 7
    ask_sz_00       = 8


# python scripts/impact.py ESH4


def measure(
    df:             pl.DataFrame,
    side_px_col:    int,
    better:         Callable
):

    for row in df.iter_rows():

        pass

    pass


if __name__ == "__main__":

    t0  = time()
    sym = argv[1]
    fn  = path.join(".", "csvs", f"{sym}.csv")
    df  = pl.read_csv(fn).select(
            [
                "ts_event", 
                "action", 
                "side", 
                "price", 
                "size",
                "bid_px_00",
                "ask_px_00",
                "bid_sz_00",
                "ask_sz_00"
            ]
        )
    
    print(df.head(n = 25))

    n_recs          = len(df)
    action          = list(df["action"])
    ts              = list(df["ts_event"])

    start           = ts[0]
    end             = ts[-1]
    cancel_ct       = action.count("C")
    add_ct          = action.count("A")
    trade_ct        = action.count("T")
    modify_ct       = action.count("M")
    clear_book_ct   = action.count("R")
    fill_ct         = action.count("F")
    total           = cancel_ct + add_ct + trade_ct + modify_ct + clear_book_ct + fill_ct

    print(f"start:  {start}")
    print(f"end:    {end}")
    print(f"C:      {cancel_ct}")
    print(f"A:      {add_ct}")
    print(f"T:      {trade_ct}")
    print(f"M:      {modify_ct}")
    print(f"R:      {clear_book_ct}")
    print(f"F:      {fill_ct}")
    print(f"total:  {total}\n") 

    bid_results = measure(df, mbp.bid_px_00, lambda a, b: a < b)
    ask_results = measure(df, mbp.ask_px_00, lambda a, b: a > b)

    print(f"{time() - t0:0.1f}s")

    pass