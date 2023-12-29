from    datetime    import  datetime
from    enum        import  IntEnum
from    json        import  dump, loads
from    databento   import  DBNStore, Historical
import  polars      as      pl
from    sys         import  argv
from    typing      import  Dict, List
from    time        import  time


# python daily_db.py 


DATE_FMT = "%Y-%m-%d"


class rec(IntEnum):

    open            = 0
    high            = 1
    low             = 2
    settle          = 3
    volume          = 4
    open_interest   = 5
    dte             = 6


def format_df(recs: Dict[str, List[float]]):

    pass


if __name__ == "__main__":

    t0 = time()

    client          = Historical()
    rng             = client.metadata.get_dataset_range(dataset = "GLBX.MDP3")
    config_fd       = open("./config.json", "r+")
    config          = loads(config_fd.read())
    expirations_fd  = open("./expirations.json", "r+")
    expirations     = loads(expirations_fd.read())
    start           = config["daily_db_checkpoint"]
    end             = rng["end_date"]
    syms            = config["daily_db_futs"]
    args            = {
                        "dataset":      "GLBX.MDP3",
                        "symbols":      syms,
                        "schema":       "statistics",
                        "stype_in":     "parent",
                        "start":        start,
                        "end":          end
                    }
    to_write        = {}

    cost = client.metadata.get_cost(**args)
    size = client.metadata.get_billable_size(**args)

    print(f"{start} - {end}")
    print(f"cost:       {cost:0.4f}")
    print(f"size:       {size} ({size / 1073741824:0.2f} GB)")

    stats = client.timeseries.get_range(**args)
    stats = stats.to_df()
    stats = stats[[ "symbol", "ts_event", "stat_type", "price", "quantity" ]]
    
    stats["date"] = stats["ts_event"].dt.date

    dates = stats["date"].dt.date.unique()

    for date in dates:

        to_write[date] = {}

    for row in stats:

        date        = row["date"]
        symbol      = row["symbol"]
        batch       = to_write[date]
        statistic   = row["stat_type"]

        # skip spreads, etc.

        if ":" in symbol or "-" in symbol or " " in symbol:

            continue

        # https://docs.databento.com/knowledge-base/new-users/fields-by-schema/statistics-statistics
        
        if symbol not in batch:

            batch[symbol] = [ None, None, None, None, None, None, None ]

        sym_rec = batch[symbol]

        if statistic == 1:

            sym_rec[rec.open] = row["price"]

        elif statistic == 3:

            sym_rec[rec.settle] = row["price"]
        
        elif statistic == 4:

            sym_rec[rec.low] = row["price"]

        elif statistic == 5:

            sym_rec[rec.high] = row["price"]
        
        elif statistic == 6:

            sym_rec[rec.volume] = row["quantity"]

        elif statistic == 9:

            sym_rec[rec.open_interest] = row["quantity"]

    # calc dte
    
    args["schema"]      = "definition"
    args["stype_in"]    = "raw_symbol"
    args["symbols"]     = None

    for row in stats:

        date    = row["date"]
        symbol  = row["symbol"]
            
        if symbol not in to_write[date]:

            # spread or other non-tracked symbol

            continue

        if symbol not in expirations:

            # no expiration recorded, need to consult definition schema

            args["symbols"] = [ symbol ]        
            dfns            = client.timeseries.get_range(**args)
            dfns            = dfns.to_df()

            # assume expiration is uniform across definition records

            expiry              = dfns.iloc[0]["expiration"].dt.date
            expirations[symbol] = expiry
        
        sym_rec = to_write[date][symbol]
        expiry  = expirations[symbol]
        dte     = (datetime.strptime(expiry, DATE_FMT) - datetime.strptime(date, DATE_FMT)).days

        sym_rec[rec.dte] = dte

    # TODO: process to_write into dfs, format, and write to parquet
        
    # ...
    
    # write config, expirations

    config["daily_db_checkpoint"] = end

    for to_write in [ 
        (config, config_fd),
        (expirations, expirations_fd)
    ]:

        json    = to_write[0]
        fd      = to_write[1]

        fd.seek(0)
        dump(json, fd)
        fd.truncate()
        fd.close()
    
    print(f"elapsed:    {time() - t0:0.1f}s")