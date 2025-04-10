import  os
import  plotly.graph_objects    as      go
from    plotly.subplots         import  make_subplots
import  polars                  as      pl
from    sys                     import  argv, path
from    time                    import  time

path.append(".")

from    util                    import  combine_trades, plt_fmt


# python trades_plot.py 'LE:BF J5-M5-Q5' 1 2024-01-01 -


if __name__ == "__main__":

    t0              = time()
    sym             = argv[1]
    omit_no_side    = bool(int(argv[2]))
    start           = argv[3]
    end             = argv[4]
    fn              = os.path.join(".", "csvs", f"{sym}_trades.csv")
    df              = pl.read_csv(fn).select(
                        [
                            "ts_event",
                            "action",
                            "side",
                            "price",
                            "size"
                        ]
                    )
    df              = plt_fmt(df, start, end)
    x, y, z, t, s   = combine_trades(df.select([ "index", "ts", "price", "size", "side" ]))
    c_map           = { "A": "#FF0000", "B": "#0000FF", "N": "#CCCCCC" }
    c               = [ c_map[i] for i in s ]

    fig = make_subplots(
            rows                = 2, 
            cols                = 1,
            row_heights         = [ 0.75, 0.25 ],
            vertical_spacing    = 0.025
        )
    
    fig.add_trace(
        go.Scattergl(
            {
                "name":         f"{sym} tick",
                "x":            x,
                "y":            y,
                "mode":         "markers",
                "marker_size":  z,
                "text":         [ f"{t[i]}<br>{z[i]}" for i in range(len(t)) ],
                "marker":       {
                                    "color":    c,
                                    "sizemode": "area",
                                    "sizeref":  2. * max(z) / (40.**2),
                                    "sizemin":  4
                                }
            }
        ),
        row = 1,
        col = 1
    )

    date    = [ ts.split("T")[0] for ts in t ]
    df_     = pl.DataFrame(
                { 
                    "date":     date,
                    "price":    y
                }
            ).group_by(
                "date"
            ).agg(
                [
                    pl.col("price").first().alias("open"),
                    pl.col("price").max().alias("high"),
                    pl.col("price").min().alias("low"),
                    pl.col("price").last().alias("close")
                ]
            )

    fig.add_trace(
        go.Candlestick(
            {
                "name":         f"{sym} daily",
                "x":            df_["date"],
                "open":         df_["open"],
                "high":         df_["high"],
                "low":          df_["low"],
                "close":        df_["close"],
                "increasing":   { "line": { "color": "blue" } },
                #"decreasing":   { "line": { "color": "blue" } }
            }
        ),
        row = 2,
        col = 1
    )

    fig.update_layout(xaxis2_rangeslider_visible = False)

    fig.show()

    print(f"{time() - t0:0.1f}s")