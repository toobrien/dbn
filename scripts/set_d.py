import  os
import  plotly.graph_objects    as      go
import  polars                  as      pl
from    sys                     import  argv, path
from    time                    import  time

path.append(".")
path.append(os.path.join("..", "daily") )

from    data.cat_df             import  get_futc
from    util                    import  strptime


# python scripts/set_d.py "NQM5-NQU5" CME 1 - -
# python scripts/set_d.py "LE:BF J5-M5-Q5" CME 1 2024-01-01 -


pl.Config.set_tbl_cols(-1)


DECADE      = "202"
FIN         = [ "ES", "NQ", "YM", "EMD", "RTY", "VX" ]


def get_spread(
    sym:        str,
    exchange:   str,
    start:      str,
    end:        str
):

    if ":" not in sym:

        # rcal

        sym     = sym.split("-")
        root    = sym[0][:-2]
        legs    = [ sym[0][-2:], sym[1][-2:] ]
        qtys    = [ 1, -1 ] if root not in FIN else [ -1, 1 ]

    else:

        # fly

        sym     = sym.split(":")
        root    = sym[0]
        legs    = sym[1].split(" ")[1].split("-")
        qtys    = [ 1, -2, 1 ]

    ids     = [ f"{exchange}_{root}{legs[i][-2]}{DECADE + legs[i][-1]}" for i in range(len(legs)) ]
    df      = get_futc(root, start = start, end = end)
    dfs     = []
    start   = "0"
    end     = "9"

    for id in ids:

        con     = df.filter((pl.col("contract_id") == id))
        start   = max(start, con["date"][0])
        end     = min(end, con["date"][-1])

        dfs.append(con)

    dfs = [ 
            df_.filter((pl.col("date") >= start) & (pl.col("date") <= end))
            for df_ in dfs 
        ]

    ids     = [ f"{root}{leg}" for leg in legs ]
    dates   = dfs[0]["date"]
    settles = [ df_["settle"] for df_ in dfs ]
    spread  = sum([ settles[i] * qtys[i] for i in range(len(legs)) ])
    df      = pl.DataFrame({ "date": dates })

    for i in range(len(ids)):

        df = df.with_columns(pl.Series(ids[i], settles[i]))

    df = df.with_columns(pl.Series("settle", spread))

    return df


if __name__ == "__main__":

    t0              = time()
    sym             = argv[1]
    exchange        = argv[2]
    omit_no_side    = bool(int(argv[3]))
    start           = argv[4] if argv[4] != '-' else "0"
    end             = argv[5] if argv[5] != '-' else "9"
    fn              = os.path.join(".", "csvs", f"{sym}_trades.csv")
    spread          = get_spread(sym, exchange, start, end)
    trades          = pl.read_csv(fn).select(
                        [
                            "ts_event",
                            "side",
                            "price"
                        ]
                    )
    trades          = trades.filter(pl.col("side") != "N") if omit_no_side else trades
    trades          = strptime(trades, "ts_event", "ts", "%Y-%m-%dT%H:%M:%S.%f", "America/Los_Angeles")
    trades          = trades.with_columns(pl.col("ts").str.slice(0, 10).alias("date"))
    trades          = trades.filter(pl.col("date") >= start)
    trades          = trades.filter(pl.col("date") <= end)
    trades          = trades.group_by("date").agg(
                        [
                            pl.col("price").first().alias("open"),
                            pl.col("price").max().alias("high"),
                            pl.col("price").min().alias("low"),
                            pl.col("price").last().alias("close")
                        ]
                    ).sort(pl.col("date"))
    merged          = spread.join(trades, on = "date", how = "left")

    fig = go.Figure()

    date    = merged["date"]
    x       = date[1:]
    y       = merged["settle"][:-1]
    #o       = (merged["open"][1:] - y).fill_null(0)
    h       = (merged["high"][1:] - y).fill_null(0)
    l       = (merged["low"][1:] - y).fill_null(0)
    c       = (merged["close"][1:] - y).fill_null(0)

    fig.add_trace(
        go.Ohlc(
            {
                "name":         sym,
                "x":            x,
                "open":         c, # dont show open
                "high":         h,
                "low":          l,
                "close":        c,
                "increasing":   { "line": { "color": "#bbbbbb" } },
                "decreasing":   { "line": { "color": "#bbbbbb" } },
                "text":         [ f"prior: {date[i]}, {y[i]}" for i in range(len(x)) ]
            }
        )
    )

    fig.add_hline(y = 0, line_color = "#FF00FF")

    fig.show()

    print(f"{'mu:':<10s}{c.mean():>10.4f}")
    print(f"{'std:':<10s}{c.std():>10.4f}")
    print(f"{'max:':<10s}{c.max():>10.4f}")


    print(f"{time() - t0:0.1f}s")
    
    pass