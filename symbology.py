from databento  import Historical
from sys        import argv
from time       import time
from util       import get_dt_rng


# python symbology.py - - ES.FUT


if __name__ == "__main__":

    t0          = time()
    client      = Historical()
    rng         = client.metadata.get_dataset_range(dataset = "GLBX.MDP3")
    start, end  = get_dt_rng(rng, argv[1], argv[2])
    symbols     = argv[3:]

    res = client.symbology.resolve(
        dataset     = "GLBX.MDP3", 
        symbols     = symbols,
        stype_in    = "parent",
        stype_out   = "instrument_id",
        start       = start,
        end         = end
    )

    symbols = []

    for symbol, dfn in res["result"].items():

        if ":" in symbol or "-" in symbol or " " in symbol:

            continue

        two_digit_year = symbol[-2].isdigit()

        year = symbol[-2:] if not two_digit_year else symbol[-3:]
        month = symbol[-3:-2] if not two_digit_year else symbol[-4:-3]
        parent = symbol[:-3] if not two_digit_year else symbol[:-4]

        symbols.append(
            (
                parent, 
                month,
                year
            )
        )

        #for rec in dfn:

            # print(f"{symbol:30}{rec['s']}\t{rec['d0']}\t{rec['d1']}")

    for symbol in symbols:

        print(f"{symbol[0]}{symbol[1]}{symbol[2]}")

    print(f"\ncount: {len(symbols)}")