from    databento   import  Historical
from    os          import  path
import  polars      as      pl
from    sys         import  argv
from    time        import  time
from    util        import  get_dt_rng


# python get_csv.py mbp-1 - - raw_symbol 'HO:BF M4-U4-Z4' 0
# python get_csv.py ohlcv-1m - - continuous HO.c.0 1

FMT = "%Y-%m-%dT%H:%M:%S.%f+0000"


if __name__ == "__main__":

    t0          = time()
    client      = Historical()
    rng         = client.metadata.get_dataset_range(dataset = "GLBX.MDP3")
    start, end  = get_dt_rng(rng, argv[2], argv[3])
    schema      = argv[1]
    stype       = argv[4]
    instrument  = argv[5]
    keep_index  = bool(int(argv[6]))
    fn          = path.join(".", "csvs", f"{instrument}.csv")
    old_df      = pl.DataFrame()
    mode        = "create"

    if path.exists(fn):

        mode    = "append"
        old_df  = pl.read_csv(fn, schema_overrides = { "ts_event": pl.Datetime("ns") })
        start   = old_df["ts_event"].dt.strftime(FMT)[-1]

    args = {
            "dataset": "GLBX.MDP3",
            "schema":   schema,
            "stype_in": stype,
            "symbols":  [ instrument ],
            "start":    start,
            "end":      end 
    }

    cost = client.metadata.get_cost(**args)
    size = client.metadata.get_billable_size(**args)
    data = client.timeseries.get_range(**args)

    if old_df.is_empty():

        df = pl.from_pandas(data.to_df(), include_index = keep_index)

    else:

        old_df = old_df[:-1]
        df = data.to_df()
        df = pl.from_pandas(df, include_index = keep_index, schema_overrides = old_df.schema)
        df = old_df.vstack(df)

    df_ = df.with_columns(df["ts_event"].dt.strftime(FMT))

    print(df_.head())
    print(df_.tail())

    df_.write_csv(fn)

    print(f"{'instrument:':<15}{instrument}")
    print(f"{'mode:':<15}{mode}")
    print(f"{'start:':<15}{start}")
    print(f"{'end:':<15}{end}")
    print(f"{'cost':<15}{cost:0.4f}")
    print(f"{'size':<15}{size} ({size / 1073741824:0.2f} GB)")
    print(f"{'elapsed:':<15}{time() - t0:<0.1f}s")