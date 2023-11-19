from databento  import Historical, SType
from datetime   import datetime, timedelta
from json       import dumps
from sys        import argv
from time       import time


if __name__ == "__main__":

    t0      = time()
    client  = Historical()
    start   = argv[1] if argv[1] != "-" else "2017-05-21"
    end     = argv[2] if argv[2] != "-" else (datetime.now() - timedelta(days = 1)).strftime("%Y-%m-%d")
    symbols = argv[3:]

    res = client.symbology.resolve(
        dataset     = "GLBX.MDP3",
        symbols     = symbols,
        stype_in    = "raw_symbol",
        stype_out   = "instrument_id",
        start_date  = start,
        end_date    = end
    )

    print(dumps(res, indent = 2))
