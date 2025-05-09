from    databento   import  DBNStore
from    os          import  path
import  polars      as      pl
from    typing      import  List


def get_dt_rng(
    rng:    List[str], 
    start:  str, 
    end:    str
) -> List[str]:

    start   = start if start != "-" else rng["start"]
    end     = end   if end   != "-" else rng["end"]
    
    return start, end


def read_batch(
    job_id: str, 
    fn:     str
) -> pl.DataFrame:

    fn      = path.join(".", f"{job_id}", f"{fn}")
    data    = pl.DataFrame(DBNStore.from_file(fn).to_df())

    return data


def read_storage(fn: str) -> pl.DataFrame:

    fn      = path.join(".", "storage", f"{fn}.dbn.zst")
    data    = pl.DataFrame(DBNStore.from_file(fn).to_df())

    return data


def strptime(
    df:         pl.DataFrame,
    from_col:   str,
    to_col:     str, 
    FMT:        str, 
    tz:         str
) -> pl.DataFrame:

    if df[from_col].dtype == pl.String:

        df = df.with_columns(
            pl.col(
                from_col
            ).map_elements(
                function        = lambda dt: f"{dt[0:10]}T{dt[10:]}+0000" if " " in dt else dt, # hack, fix serialization in dbn.get_csv
                return_dtype    = pl.String
            ).cast(
                pl.Datetime
            ).dt.convert_time_zone(
                tz
            ).dt.strftime(
                FMT
            ).alias(
                to_col
            )
        )
    
    else:

        # datetime

        df = df.with_columns(
            pl.col(
                from_col
            ).dt.convert_time_zone(
                tz
            ).dt.strftime(
                FMT
            ).alias(
                to_col
            )
        )

    return df


def combine_trades(df: pl.DataFrame):

    x           = []
    y           = []
    z           = []
    t           = []
    s           = []
    prev_price  = df["price"][0]
    prev_side   = df["side"][0]
    prev_ts     = df["ts"][0]
    prev_i      = df["index"][0]
    prev_size   = 0

    for row in df.iter_rows():

        cur_i       = row[0]
        cur_ts      = row[1]
        cur_price   = row[2]
        cur_size    = row[3]
        cur_side    = row[4]

        if cur_price == prev_price and cur_side == prev_side:

            prev_size += cur_size
        
        else:

            x.append(prev_i)
            y.append(prev_price)
            z.append(prev_size)
            t.append(prev_ts)
            s.append(prev_side)

            prev_price  = cur_price
            prev_side   = cur_side
            prev_ts     = cur_ts
            prev_i      = cur_i
            prev_size   = cur_size

    x.append(cur_i)
    y.append(cur_price)
    z.append(prev_size)
    t.append(prev_ts)
    s.append(prev_side)

    #print(f"trades: {len(x)}")
    #print(f"min_price: {min(y)}")
    #print(f"max_price: {max(y)}")

    return x, y, z, t, s


def std_fmt(
        df:     pl.DataFrame,
        start:  str     = "-",
        end:    str     = "-",
        omit_n: bool    = True,
        tz:     str     = "America/Los_Angeles"
    ):

    df  = df.filter(pl.col("side") != "N") if omit_n else df
    df  = strptime(df, "ts_event", "ts", "%Y-%m-%dT%H:%M:%S.%f", tz)
    df  = df.filter(pl.col("ts") > start) if start != "-" else df
    df  = df.filter(pl.col("ts") < end) if end != "-" else df
    df  = df.with_row_index(name = "index")

    return df