import  databento   as      db
from    json        import  dumps
from    sys         import  argv


# python definition.py - - HO.FUT


if __name__ == "__main__":

    client  = db.Historical()
    rng     = client.metadata.get_dataset_range(dataset = "GLBX.MDP3")
    start   = argv[1] if argv[1] != "-" else rng["start_date"]
    end     = argv[2] if argv[2] != "-" else rng["end_date"]
    symbol  = argv[3]

    dfn = client.timeseries.get_range(
        dataset     = "GLBX.MDP3",
        symbols     = symbol,
        schema      = "definition",
        stype_in    = "parent",
        start       = start,
        end         = end
    )

    dfn.to_file("./definitions/{symbol}_dfn.dbn.zst")