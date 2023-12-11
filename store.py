import  databento   as      db
from    sys         import  argv
from    time        import  time


# python store.py mbp-1 2023-12-07 2023-12-08 raw_symbol nqh4_sample NQH4


if __name__ == "__main__":

    t0      = time()
    client  = db.Historical()
    schema  = argv[1]
    rng     = client.metadata.get_dataset_range(dataset = "GLBX.MDP3")
    start   = argv[2] if argv[2] != "-" else rng["start_date"]
    end     = argv[3] if argv[3] != "-" else rng["end_date"]
    stype   = argv[4]
    fn      = argv[5]
    symbols = argv[6:]

    data = client.timeseries.get_range(
        dataset     = "GLBX.MDP3",
        schema      = schema,
        stype_in    = stype,
        symbols     = symbols,
        start       = start,
        end         = end 
    )

    data.to_file(path = f"./storage/{fn}.dbn.zst")
    
    print(f"elapsed:  {time() - t0: 0.1f}s")