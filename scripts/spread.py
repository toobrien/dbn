import  numpy                   as      np
import  plotly.graph_objects    as      go
import  polars                  as      pl
from    sys                     import  argv, path
from    time                    import  time

path.append(".")

from    util                    import  read_storage


# python scripts/spread.py nqh4_sample 2 0.25


if __name__ == "__main__":

    t0 = time()

    fn          = argv[1]
    precision   = int(argv[2])
    tick_size   = float(argv[3])
    rows        = read_storage(fn).rows()

    BID = 12
    ASK = 13

    prev_bid = rows[0][BID]
    prev_ask = rows[0][ASK]

    spreads = []

    for row in rows[1:]:

        bid = row[BID]
        ask = row[ASK]

        if bid != prev_bid or ask != prev_ask:

            spread = prev_ask - prev_bid

            prev_bid = bid
            prev_ask = ask

            spreads.append(spread)

    print(f"avg:     {np.average(spreads):0.{precision}f}")
    print(f"median:  {np.median(spreads):0.{precision}f}")
    print(f"min:     {min(spreads):0.{precision}f}")
    print(f"max:     {max(spreads):0.{precision}f}")
    print(f"samples: {len(spreads)}")

    fig = go.Figure()

    fig.add_trace(
        go.Histogram(x = spreads, nbinsx = int((max(spreads) - min(spreads)) / tick_size))
    )

    fig.show()

    print(f"\nelapsed: {time() - t0:0.1f}s")

    
    