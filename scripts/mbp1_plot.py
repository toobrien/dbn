import  os
import  plotly.graph_objects    as      go
from    plotly.subplots         import  make_subplots
import  polars                  as      pl
from    sys                     import  argv, path
from    time                    import  time

path.append(".")

from    util                    import  combine_trades, std_fmt


# python scripts/mbp1_plot.py 'MRBK5' 1 - -


def plot_mbp_1(
    sym:    str,
    omit_n: bool    = True,
    start:  str     = "-",
    end:    str     = "-"
):

    #cols    = [ "index", "ts", "action", "side", "price", "size", "bid_px_00", "ask_px_00" ]
    f_path  = os.path.join(".", "csvs", f"{sym}_mbp-1.csv")
    df      = std_fmt(pl.read_csv(f_path), start, end, omit_n)
    #df      = df.select(cols)
    trades  = df.filter(pl.col("action") == "T").select([ "index", "ts", "price", "side", "size" ])
    traces  = [
                ( "trades", trades["index"], trades["price"], "markers", "#FF00FF" ),
                ( "bid", df["index"], df["bid_px_00"], "lines", "#0000FF" ),
                ( "ask", df["index"], df["ask_px_00"], "lines", "#FF0000" )
            ]
    
    fig = go.Figure()

    for trace in traces:
        
        t = {
            "name": trace[0],
            "x":    trace[1],
            "y":    trace[2],
            "mode": trace[3]
        }

        t[trace[3][:-1]] = { "color": trace[4] }

        fig.add_trace(go.Scattergl(t))

    fig.show()

    pass


if __name__ == "__main__":

    pl.Config.set_tbl_rows(-1)
    pl.Config.set_tbl_cols(-1)

    sym     = argv[1]
    omit_n  = bool(int(argv[2]))
    start   = argv[3]
    end     = argv[4]
    
    plot_mbp_1(sym, omit_n, start, end)