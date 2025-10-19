# worker.py
import os, sys
from redis import Redis
from rq import Queue
from rq.worker import Worker

if __name__ == "__main__":
    os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")
    sys.path.insert(0, os.path.dirname(__file__))

    conn = Redis.from_url(os.environ["REDIS_URL"])
    q = Queue("analytical-reports", connection=conn)
    Worker([q], connection=conn).work(with_scheduler=True)
