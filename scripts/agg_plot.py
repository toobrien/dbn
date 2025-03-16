import  os
import  plotly.graph_objects    as      go
import  polars                  as      pl
from    sys                     import  argv, path
from    time                    import  time

path.append(".")

from    util                    import  combine_trades, strptime


# python agg_plot.py 'LE:BF J5-M5-Q5'


if __name__ == "__main__":

    t0              = time()
    sym             = argv[1]
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
    df              = strptime(df, "ts_event", "ts", "%Y-%m-%dT%H:%M:%S.%f", "America/Los_Angeles")
    df              = df.with_row_index(name = "index")
    x, y, z, t, s   = combine_trades(df.select([ "index", "ts", "price", "size", "side" ]))
    c_map           = { "A": "#0000FF", "B": "#FF0000", "N": "#CCCCCC" }
    c               = [ c_map[i] for i in s ]

    fig = go.Figure()
    
    fig.add_trace(
        go.Scattergl(
            {
                "name":         sym,
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
        )
    )

    fig.show()

    print(f"{time() - t0:0.1f}s")

    pass