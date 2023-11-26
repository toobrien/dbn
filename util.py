from    databento   import  DBNStore
import  polars      as      pl


def read_batch(job_id: str, fn: str) -> pl.DataFrame:

    data = pl.DataFrame(DBNStore.from_file(path = f"./{job_id}/{fn}").to_df())

    return data