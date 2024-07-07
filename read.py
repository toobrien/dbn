from    databento   import  DBNStore
from    os          import  path
import  polars      as      pl
from    sys         import  argv
from    time        import  time


# python read.py storage 1d_sample df


if __name__ == "__main__":

    t0      = time()

    folder  = argv[1]
    fn      = path.join(".", folder, f"{argv[2]}.dbn.zst")
    fmt     = argv[3]
    data    = DBNStore.from_file(fn)
    df      = data.to_df()
    columns = argv[4:] if len(argv) > 4 else None

    if not df.index.empty:

        df = df.reset_index()

    df = pl.DataFrame(df)

    if fmt == "df":

        if columns:

            df = df[columns]

        with pl.Config(tbl_rows = -1, tbl_cols = -1):
        
            print(df)

        print(f"\ncount: {len(df)}")

    elif fmt == "rows":

        rows = df.rows()

        for row in rows:

            print("\t".join([ str(i) for i in row ]))

        print(f"\ncount:   {len(rows)}")

    elif fmt == "csv":

        data.to_csv(path = f"./csvs/{argv[2]}.csv")
    
    print(f"elapsed: {time() - t0:0.1f}s")