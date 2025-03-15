import  plotly.graph_objects    as      go
import  polars                  as      pl
from    sys                     import  argv, path
from    time                    import  time

path.append(".")

from    util                    import  read_storage, strptime, combine_trades


# python scripts/orders.py 20240301_esh4_mbo size 500
# python scripts/oders.py  20240307_rbj4_mbo trades 5


def get_ids(
    df:         pl.DataFrame,
    mode:       str, 
    min_qty:    int
):
    
    ids = []

    if mode == "size":

        ids = df.filter(
                (pl.col("size") >= min_qty)
            ).select(
                "order_id"
            ).unique()

    else:

        ids = df.filter(
                pl.col("action") == "T"
            ).group_by(
                [ "order_id", "ts" ]
            ).len().filter(
                pl.col("len") >= min_qty
            ).select(
                "order_id"
            ).unique()

    dfs = [
            df.filter(pl.col("order_id") == order_id)
            for order_id in ids.iter_rows()
        ]
    
    return dfs
                        

if __name__ == "__main__":

    t0 = time()

    pl.Config.set_tbl_cols(-1)
    pl.Config.set_tbl_rows(-1)

    fn          = argv[1]
    mode        = argv[2]
    min_qty     = int(argv[3])
    df          = read_storage(fn).with_row_index()
    df          = strptime(df, "ts_event", "ts", "%Y-%m-%dT%H:%M:%S.%f", "America/Los_Angeles")
    df          = df.select([ "index", "order_id", "ts", "action", "side", "price", "size" ])
    dfs         = get_ids(df, mode, min_qty)
    trades      = combine_trades(df.filter((pl.col("action") == "T") | (pl.col("action") == "F")).select([ "index", "ts", "price", "size" ]))
    traces      = [ ( trades[0], trades[1], trades[3], "trades", "#0000FF", "lines" ) ]
    fig         = go.Figure()

    fig.update_layout(title_text = fn)

    for df_ in dfs:

        id_ = df_["order_id"][0]
        x   = df_["index"]
        y   = df_["price"]
        t   = df_["ts"]

        print(f"{id_}: {len(x)}" )

        traces.append(( x, y, t, id_, "#FF00FF", "markers+lines" ))
    

    for trace in traces:

        fig.add_trace(
            go.Scattergl(
                {
                    "x":        trace[0],
                    "y":        trace[1],
                    "text":     trace[2],
                    "name":     trace[3],
                    "marker":   { "color": trace[4]},
                    "mode":     trace[5]
                }
            )
        )

    for df_ in dfs:

        print(df_)

    fig.show()

    print(f"elapsed: {time() - t0:0.2f}s")