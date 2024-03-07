from    databento   import  DBNStore
import  polars      as      pl
from    typing      import  List


def read_batch(job_id: str, fn: str) -> pl.DataFrame:

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
):
    
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