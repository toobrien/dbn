import  plotly.graph_objects    as go
import  polars                  as pl
import  os
from    sys                     import argv, path
from    time                    import time

path.append(".")

from    util                    import  std_fmt

# python scripts/t_cost.py 'MRBK5' 1 - -


def run(
    sym:    str,
    omit_n: bool,
    start:  str = "-",
    end:    str = "-"
):
    
    f_path  = os.path.join(".", "csvs", f"{sym}_mbp-1.csv")
    cols    = [ "index", "ts", "action", "side", "size",  "bid_ct_00", "bid_px_00", "ask_px_00", "ask_ct_00", "price" ]
    df      = std_fmt(pl.read_csv(f_path), start, end, omit_n).select(cols)
    df      = df.with_columns(
                ((pl.col("ask_px_00") + pl.col("bid_px_00")) / 2).alias("mid"),
                (pl.col("ask_px_00") - pl.col("bid_px_00")).alias("s_width")
            )
    trades  = df.filter(pl.col("action") == "T")
    trades  = trades.with_columns(
                pl.when(
                    pl.col("side") == "B"
                ).then(
                    pl.col("price") - pl.col("bid_px_00")
                ).when(
                    pl.col("side") == "A"
                ).then(
                    pl.col("ask_px_00") - pl.col("price")
                ).alias("cost")
            ).with_columns(
                (pl.col("cost") / pl.col("s_width") * 100).alias("cost_pct")
            )

    return df, trades


if __name__ == "__main__":

    t0 = time()

    pl.Config.set_tbl_rows(-1)
    pl.Config.set_tbl_cols(-1)

    sym         = argv[1]
    omit_n      = bool(int(argv[2]))
    start       = argv[3]
    end         = argv[4]
    df, trades  = run(sym, omit_n, start, end)
    fig         = go.Figure()

    traces = [
                ( "costs",  trades["cost"],  "#FF00FF" ),
                ( "spread", df["s_width"],   "#0000FF" )
            ]

    for trace in traces:

        fig.add_trace(go.Histogram(x = trace[1], name = trace[0], marker = { "color": trace[2] }))

    fig.update_layout(title = " ".join(argv[1:]))
    fig.show()

    print(trades)

    print(f"{time() - t0:0.1f}s")

    pass