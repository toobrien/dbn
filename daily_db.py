from    json        import  dumps, loads
from    databento   import  DBNStore, Historical
import  polars      as      pl
from    sys         import  argv
from    time        import  time


# python daily_db.py 


if __name__ == "__main__":

    t0      = time()

    client  = Historical()
    rng     = client.metadata.get_dataset_range(dataset = "GLBX.MDP3")
    config  = loads(open("./config.json", "r+").read())
    start   = config["daily_db_checkpoint"]
    end     = rng["end_date"]
    syms    = config["daily_db_futs"]
    args    = {
                "dataset":      "GLBX.MDP3",
                "symbols":      syms,
                "schema":       "statistics",
                "stype_in":     "parent",
                "start":        start,
                "end":          end
            }

    cost = client.metadata.get_cost(**args)
    size = client.metadata.get_billable_size(**args)

    print(f"{start} - {end}")
    print(f"cost:       {cost:0.4f}")
    print(f"size:       {size} ({size / 1073741824:0.2f} GB)")

    
    # TODO: request definitions for outrights, record expirations (if necessary)


    print(f"elapsed:    {time() - t0:0.1f}s")