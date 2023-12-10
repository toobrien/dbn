from databento  import Historical
from json       import dumps
from sys        import argv
from time       import time

# python symbology.py - - ES.FUT


if __name__ == "__main__":

    t0      = time()
    client  = Historical()
    rng     = client.metadata.get_dataset_range(dataset = "GLBX.MDP3")
    start   = argv[1] if argv[1] != "-" else rng["start_date"]
    end     = argv[2] if argv[2] != "-" else rng["end_date"]
    symbols = argv[3:]

    res = client.symbology.resolve(
        dataset     = "GLBX.MDP3", 
        symbols     = symbols,
        stype_in    = "parent",
        stype_out   = "instrument_id",
        start_date  = start,
        end_date    = end
    )

    count = 0

    for symbol, dfn in res["result"].items():

        if ":" in symbol or "-" in symbol or " " in symbol:

            continue

        for rec in dfn:

            print(f"{symbol:30}{rec['s']}\t{rec['d0']}\t{rec['d1']}")

        count += 1

    print(f"\ncount: {count}")