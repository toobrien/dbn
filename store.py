from    datetime    import  datetime, timedelta
from    databento   import  Historical
from    sys         import  argv
from    time        import  time


# python store.py mbp-1 2023-12-07 2023-12-08 raw_symbol nqh4_sample NQH4


if __name__ == "__main__":

    t0      = time()
    client  = Historical()
    rng     = client.metadata.get_dataset_range(dataset = "GLBX.MDP3")
    schema  = argv[1]
    start   = argv[2] if argv[2] != "-" else rng["start_date"]
    end     = argv[3] if argv[3] != "-" else (datetime.strptime(rng["end_date"], "%Y-%m-%d") + timedelta(days = 1)).strftime("%Y-%m-%dT00:00:00")
    stype   = argv[4]
    fn      = f"./storage/{argv[5]}.dbn.zst"
    symbols = argv[6:]

    data = client.timeseries.get_range(
        dataset     = "GLBX.MDP3",
        schema      = schema,
        stype_in    = stype,
        symbols     = symbols,
        start       = start,
        end         = end 
    )

    data.to_file(path = fn)
    
    print(f"elapsed:  {time() - t0: 0.1f}s")