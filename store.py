from    databento   import  Historical
from    os          import  path
from    sys         import  argv
from    time        import  time
from    util        import  get_dt_rng


# python store.py mbp-1 2023-12-07 2023-12-08 raw_symbol nqh4_sample NQH4


if __name__ == "__main__":

    t0          = time()
    client      = Historical()
    rng         = client.metadata.get_dataset_range(dataset = "GLBX.MDP3")
    schema      = argv[1]
    start, end  = get_dt_rng(rng, argv[2], argv[3]) 
    stype       = argv[4]
    fn          = path.join(".", "storage", f"{argv[5]}.dbn.zst")
    symbols     = argv[6:]

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