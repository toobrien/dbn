import  databento   as      db
from    json        import  dumps
from    sys         import  argv
from    time        import  time


if __name__ == "__main__":

    t0      = time()
    client  = db.Historical()
    job_id  = argv[1]

    #jobs = client.batch.list_jobs()
    #print(dumps(jobs, indent = 2))

    client.batch.download(
        output_dir  = ".",
        job_id      = job_id
    )

    print(f"{job_id} downloaded in {time() - t0:0.2f}s")