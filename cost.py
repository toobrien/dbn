import  databento   as      db
from    sys         import  argv
from    time        import  time


if __name__ == "__main__":

    t0      = time()
    client  = db.Historical()
    schema  = argv[1]
    rng     = client.metadata.get_dataset_range(dataset = "GLBX.MDP3")
    start   = argv[2] if argv[2] != "-" else rng["start_date"]
    end     = argv[3] if argv[3] != "-" else rng["end_date"]
    symbols = argv[4:]

    cost = client.metadata.get_cost(
        dataset = "GLBX.MDP3",
        symbols = symbols,
        schema  = schema,
        start   = start,
        end     = end
    )

    print(f"{start} - {end}: {cost:0.4f}")
