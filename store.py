import  databento   as      db
from    sys         import  argv
from    time        import  time


# python store.py mbp-1 2023-11-23 2023-11-24 raw_symbol energy HOZ3 RBZ3 CLZ3 NGZ3
# python store.py definition 2023-11-23 2023-11-24 parent ho HO.FUT 


if __name__ == "__main__":

    t0 = time()

    
    print(f"elapsed:  {time() - t0: 0.1f}s")