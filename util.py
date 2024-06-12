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
    utc_offset: int
) -> pl.DataFrame:
    
    df = df.with_columns(
        pl.col(
            from_col
        ).dt.offset_by(
            f"{utc_offset}h"
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

    #print(f"trades: {len(x)}")
    #print(f"min_price: {min(y)}")
    #print(f"max_price: {max(y)}")

    return x, y, z, t