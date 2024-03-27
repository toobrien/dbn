import  plotly.graph_objects    as      go
import  polars                  as      pl
from    sys                     import  argv, path
from    time                    import  time

path.append(".")

from    util                    import  read_storage, strptime, combine_trades


# python scripts/sweep.py 20240301_esh4_mbo 5


pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_rows(-1)


if __name__ == "__main__":

    t0      = time()
    fn      = argv[1]
    min_len = int(argv[2])
    df      = read_storage(fn).with_row_index()
    df      = strptime(df, "ts_event", "ts", "%Y-%m-%dT%H:%M:%S.%f", -8)
    trades  = combine_trades(df.filter((pl.col("action") == "T") | (pl.col("action") == "F")).select([ "index", "ts", "price", "size" ]))
    traces  = [ ( trades[0], trades[1], trades[3], "trades", "#0000FF", "lines" ) ]
    groups  = df.filter((pl.col("action") == "T") | (pl.col("action") == "F")).group_by([ "ts" ])
    fig     = go.Figure()

    for _, group in groups:

        if group["price"].n_unique() > min_len:
        
            # print(group.select([ "ts", "price", "size", "side", "order_id", "action" ]))

            idx         = group["index"][0]
            ts          = group["ts"][0]
            qty         = group["size"].sum()
            min_price   = group["price"].min()
            max_price   = group["price"].max()

            traces.append(
                ( 
                    [ idx, idx ], 
                    [ min_price, max_price ],
                    [ ts, ts ],
                    ts,
                    "#FF00FF",
                    "markers+lines",
                    qty                  
                )
            )

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

    
    for trace in traces[1:]:
        
        print(trace[3], f"{trace[1][0]:10}", f"{trace[1][1]:10}", f"{trace[1][1] - trace[1][0]:10}", f"{trace[-1]:10}")

    fig.update_layout(title_text = fn)

    fig.show()

    print(f"{time() - t0:0.1f}s")