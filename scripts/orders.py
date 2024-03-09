import  plotly.graph_objects    as      go
import  polars                  as      pl
from    sys                     import  argv, path
from    time                    import  time

path.append(".")

from    util                    import  read_storage, strptime


# python scripts/orders.py 20240301_esh4_mbo size 500
# python scripts/oders.py  20240307_rbj4_mbo trades 5


'''
publisher_id 	uint16_t 	The publisher ID assigned by Databento, which denotes the dataset and venue.
instrument_id 	uint32_t 	The numeric instrument ID.
ts_event 	    uint64_t 	The matching-engine-received timestamp expressed as the number of nanoseconds since the UNIX epoch.
order_id 	    uint64_t 	The order ID assigned by the venue.
price 	        int64_t 	The order price where every 1 unit corresponds to 1e-9, i.e. 1/1,000,000,000 or 0.000000001.
size 	        uint32_t 	The order quantity.
flags 	        uint8_t 	A bit field indicating packet end, message characteristics, and data quality.
channel_id 	    uint8_t 	The channel ID assigned by Databento as an incrementing integer starting at zero.
action 	        char 	    The event action. Can be Add, Cancel, Modify, cleaR book, Trade, or Fill.
side 	        char 	    The order side. Can be Ask, Bid, or None.
ts_recv 	    uint64_t 	The capture-server-received timestamp expressed as the number of nanoseconds since the UNIX epoch.
ts_in_delta 	int32_t 	The matching-engine-sending timestamp expressed as the number of nanoseconds before ts_recv.
sequence 	    uint32_t 	The message sequence number assigned at the venue.
'''


def combine_trades(df: pl.DataFrame):

    x           = []
    y           = []
    z           = []
    t           = []
    cur_i       = 0
    prev_price  = df["price"][0]
    prev_size   = 0

    for row in df.iter_rows():

        cur_i       = row[0]
        cur_ts      = row[1]
        cur_price   = row[2]
        cur_size    = row[3]

        if cur_price == prev_price:

            prev_size += cur_size
        
        else:

            x.append(cur_i)
            y.append(prev_price)
            z.append(prev_size)
            t.append(cur_ts)

            prev_price  = cur_price
            prev_size   = cur_size

    x.append(cur_i)
    y.append(cur_price)
    z.append(prev_size)
    t.append(cur_ts)

    print(f"trades: {len(x)}")
    print(f"min_price: {min(y)}")
    print(f"max_price: {max(y)}")

    return x, y, z, t


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
    df          = strptime(df, "ts_event", "ts", "%Y-%m-%dT%H:%M:%S.%f", -8)
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