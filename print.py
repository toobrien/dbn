from    databento   import  Historical
from    polars      import  DataFrame
from    sys         import  argv
from    time        import  time


if __name__ == "__main__":

    t0      = time()
    client  = Historical()
    rng     = client.metadata.get_dataset_range(dataset = "GLBX.MDP3")
    start   = argv[1] if argv[1] != "-" else rng["start_date"]
    end     = argv[2] if argv[2] != "-" else rng["end_date"]
    symbols = argv[3:]


    data = client.timeseries.get_range(
        dataset = "GLBX.MDP3",
        symbols = symbols,
        schema  = "ohlcv-1d",
        start   = start,
        end     = end
    )

    rows = DataFrame(data.to_df()).rows()

    for row in rows:

        print("\t".join([ str(i) for i in row ]))

