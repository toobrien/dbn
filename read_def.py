from    databento   import  DBNStore
from    sys         import  argv
from    time        import  time
import  polars      as      pl


pl.Config.set_tbl_rows(1000)
pl.Config.set_tbl_cols(50)


if __name__ == "__main__":

    t0      = time()
    path    = f"./{argv[1]}"

    df      = pl.DataFrame(DBNStore.from_file(path = path).to_df())
    rows    = df.filter(df["secsubtype"] == "").select([ "raw_symbol", "expiration", "maturity_year" ]).rows()
    seen    = set()

    for row in rows:

        if row[0] not in seen:

            seen.add(row[0])
        
        else:

            continue

        print("\t".join([ str(i) for i in row ]))

    print(f"{len(seen)} symbols")

    print(f"{path} read in {time() - t0:0.2f}s")

