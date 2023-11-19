import  databento   as      db
from    sys         import  argv
from    time        import  time


# python cost.py mbp-1 - - raw_symbol HOZ3 RBZ3 CLZ3 NGZ3
# python cost.py definition - - parent HO.FUT


if __name__ == "__main__":

    t0      = time()
    client  = db.Historical()
    schema  = argv[1]
    rng     = client.metadata.get_dataset_range(dataset = "GLBX.MDP3")
    start   = argv[2] if argv[2] != "-" else rng["start_date"]
    end     = argv[3] if argv[3] != "-" else rng["end_date"]
    stype   = argv[4]
    symbols = argv[5:]

    args = {
        "dataset":  "GLBX.MDP3",
        "symbols":  symbols,
        "schema":   schema,
        "stype_in": stype,
        "start":    start,
        "end":      end
    }

    cost = client.metadata.get_cost(**args)
    size = client.metadata.get_billable_size(**args)

    print(f"{start} - {end}")
    print(f"cost:      {cost:0.4f}")
    print(f"size:      {size} ({size / 1073741824:0.2f} GB)")
    print(f"elapsed:  {time() - t0: 0.1f}s")
