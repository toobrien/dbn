from    databento   import  DBNStore
from    sys         import  argv
from    time        import  time
import  polars      as      pl


pl.Config.set_tbl_rows(1000)
pl.Config.set_tbl_cols(50)


if __name__ == "__main__":

    t0      = time()
    job_id  = argv[1]
    fn      = argv[2]
    path    = f"./{argv[1]}/{argv[2]}"

    data = pl.DataFrame(DBNStore.from_file(path = path).to_df())

    print(type(data))

    print(data)

    print(f"{path} read in {time() - t0:0.2f}s")

