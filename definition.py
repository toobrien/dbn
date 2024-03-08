import  databento   as      db
from    sys         import  argv
from    util        import  get_dt_rng


# python definition.py - - HO.FUT


if __name__ == "__main__":

    client  = db.Historical()
    rng     = client.metadata.get_dataset_range(dataset = "GLBX.MDP3")
    start, end  = get_dt_rng(rng, argv[1], argv[2]) 
    symbol  = argv[3]

    dfn = client.timeseries.get_range(
        dataset     = "GLBX.MDP3",
        symbols     = symbol,
        schema      = "definition",
        stype_in    = "parent",
        start       = start,
        end         = end
    )

    dfn.to_file(f"./definitions/{symbol}_dfn.dbn.zst")