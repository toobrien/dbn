from    databento   import  DBNStore, Historical
from    sys         import  argv
from    time        import  time


# python store.py create mbp-1 2023-12-07 2023-12-08 raw_symbol nqh4_sample NQH4
# python store.py new mbo 2023-12-15 zrh4_mbo


if __name__ == "__main__":

    t0      = time()
    client  = Historical()
    mode    = argv[1]
    rng     = client.metadata.get_dataset_range(dataset = "GLBX.MDP3")

    if mode == "create":

        schema  = argv[2]
        start   = argv[3] if argv[3] != "-" else rng["start_date"]
        end     = argv[4] if argv[4] != "-" else rng["end_date"]
        stype   = argv[5]
        fn      = f"./storage/{argv[6]}.dbn.zst"
        symbols = argv[7:]

        data = client.timeseries.get_range(
            dataset     = "GLBX.MDP3",
            schema      = schema,
            stype_in    = stype,
            symbols     = symbols,
            start       = start,
            end         = end 
        )

        data.to_file(path = fn)
    
    elif mode == "new":

        fn  = f"./storage/{argv[3]}.dbn.zst"
        old = DBNStore.from_file(path = fn)
        end = argv[2] if argv[2] != "-" else rng["end_date"]

        pass

        new = client.timeseries.get_range(
            dataset     = "GLBX.MDP3",
            schema      = old.schema,
            stype_in    = old.stype_in,
            symbols     = old.symbols,
            start       = old.end,
            end         = end

        )

        # TODO: append old + new into "data"

        # data.to_file(path = fn)

    print(f"elapsed:  {time() - t0: 0.1f}s")