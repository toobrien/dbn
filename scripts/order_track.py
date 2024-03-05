import  numpy                   as      np
import  plotly.graph_objects    as      go
import  polars                  as      pl
from    sys                     import  argv, path
from    time                    import  time

path.append(".")

from    util                    import  read_storage


# python scripts/order_life.py 20240301_esh4_mbo

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


if __name__ == "__main__":

    pl.Config.set_tbl_cols(-1)
    pl.Config.set_tbl_rows(-1)

    fn      = argv[1]
    min_qty = int(argv[2])

    df  = read_storage(fn)
    ids = df.filter((pl.col("size") >= min_qty)).select("order_id").unique()
    
    for id in ids.iter_rows():

        df_ = df.filter(pl.col("order_id") == id).select([ "order_id", "ts_event", "price", "size", "action", "side" ])

        print(df_)

    pass