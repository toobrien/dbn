from    databento   import  DBNStore
import  polars      as      pl
from    sys         import  argv
from    time        import  time


# python print.py storage 1d_sample df


if __name__ == "__main__":

    t0      = time()

    folder  = argv[1]
    fn      = f"{folder}/{argv[2]}.dbn.zst"
    fmt     = argv[3]
    data    = DBNStore.from_file(fn)
    df      = data.to_df()

    if not df.index.empty:

        df = df.reset_index()

    df = pl.DataFrame(df)

    if fmt == "df":

        with pl.Config(tbl_rows = -1, tbl_cols = -1):
        
            print(df)

        print(f"\ncount: {len(df)}")

    elif fmt == "rows":

        rows    = df.rows()
        for row in rows:

            print("\t".join([ str(i) for i in row ]))

        print(f"\ncount: {len(rows)}")