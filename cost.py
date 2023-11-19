import  databento   as      db
from    datetime    import  datetime, timedelta
from    json        import  dumps
from    sys         import  argv
from    time        import  time


if __name__ == "__main__":

    t0      = time()
    client  = db.Historical()
    schema  = argv[1]
    start   = argv[2] if argv[2] != "-" else "2017-05-21"
    end     = argv[3] if argv[3] != "-" else (datetime.now() - timedelta(days = 1)).strftime("%Y-%m-%d")
    symbols = argv[4:]

    cost = client.metadata.get_cost(
        dataset = "GLBX.MDP3",
        symbols = symbols,
        schema  = schema,
        start   = start,
        end     = end
    )

    print(f"{cost:0.4f}")
