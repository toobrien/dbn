from databento  import Historical
from json       import dumps
from sys        import argv
from time       import time
from util       import get_dt_rng


# python definition.py - - ESZ3 ESH4


if __name__ == "__main__":

    t0          = time()
    client      = Historical()
    rng         = client.metadata.get_dataset_range(dataset = "GLBX.MDP3")
    start, end  = get_dt_rng(rng, argv[1], argv[2]) 
    symbols     = argv[3:]

    res = client.symbology.resolve(
        dataset     = "GLBX.MDP3",
        symbols     = symbols,
        stype_in    = "raw_symbol",
        stype_out   = "instrument_id",
        start_date  = start,
        end_date    = end
    )

    print(dumps(res, indent = 2))
