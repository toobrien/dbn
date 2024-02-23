from    databento   import  Historical
from    os          import  path
import  polars      as      pl
from    sys         import  argv
from    time        import  time


# python get_csv.py mbp-1 - - raw_symbol 'HO:BF M4-U4-Z4'


if __name__ == "__main__":

    t0          = time()
    client      = Historical()
    rng         = client.metadata.get_dataset_range(dataset = "GLBX.MDP3")
    schema      = argv[1]
    start       = argv[2] if argv[2] != "-" else rng["start_date"]
    end         = argv[3] if argv[3] != "-" else rng["end_date"]
    stype       = argv[4]
    instrument  = argv[5]
    fn          = f"./csvs/{instrument}.csv"
    old_df      = pl.DataFrame()
    mode        = "create"

    if path.exists(fn):

        mode    = "append"
        old_df  = pl.read_csv(fn)
        start   = old_df["ts_event"][-1]

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

        df = pl.from_pandas(data.to_df())

    else:

        old_df = old_df[:-1]
        df = data.to_df()
        df = pl.from_pandas(df, schema_overrides = old_df.schema)
        df = old_df.vstack(df)

    df.write_csv(fn)

    print(f"{'instrument:':<15}{instrument}")
    print(f"{'mode:':<15}{mode}")
    print(f"{'start:':<15}{start}")
    print(f"{'end:':<15}{end}")
    print(f"{'cost':<15}{cost:0.4f}")
    print(f"{'size':<15}{size} ({size / 1073741824:0.2f} GB)")
    print(f"{'elapsed:':<15}{time() - t0:<0.1f}s")