from    databento   import  DBNStore
import  polars      as      pl
from    sys         import  argv
from    time        import  time


# python print.py 1d_sample df


if __name__ == "__main__":

    t0      = time()

    fn      = f"storage/{argv[1]}.dbn.zst"
    fmt     = argv[2]
    data    = DBNStore.from_file(fn)
    df      = pl.DataFrame(data.to_df())

    if fmt == "df":

        with pl.Config(tbl_rows = -1, tbl_cols = -1):
        
            print(df)

    elif fmt == "rows":

        rows    = df.rows()
        for row in rows:

            print("\t".join([ str(i) for i in row ]))

