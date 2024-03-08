from    databento   import  DBNStore
from    datetime    import  datetime, timedelta
import  polars      as      pl
from    typing      import  List


def get_dt_rng(
    rng: List[str], 
    start: str, 
    end: str
) -> List[str]:

    start   = start if start != "-" else rng["start_date"]
    end     = end if end != "-" else (datetime.strptime(rng["end_date"], "%Y-%m-%d") + timedelta(days = 1)).strftime("%Y-%m-%dT00:00:00")

    return start, end

def read_batch(
    job_id: str, 
    fn: str
) -> pl.DataFrame:

    data = pl.DataFrame(DBNStore.from_file(path = f"./{job_id}/{fn}").to_df())

    return data


def read_storage(fn: str) -> pl.DataFrame:

    data = pl.DataFrame(DBNStore.from_file(path = f"./storage/{fn}.dbn.zst").to_df())

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