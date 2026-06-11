'''
Orchestrator is a background job that clears out stuck jobs that are claimed by failed consumer nodes. 
In a real implementation, we would also have a separate health check service that monitors the health of consumer nodes and proactively restarts any failed nodes to minimize downtime.
'''

import os
import time
import psycopg2
from datetime import datetime, timedelta


STUCK_TIMEOUT_MINUTES = int(os.getenv("STUCK_TIMEOUT_MINUTES", 10))
POLL_INTERVAL = 60  


class Orchestrator:

    def __init__(self):
        self.conn = psycopg2.connect(os.getenv("POSTGRES_DSN"))
        self.running = False

    def start(self):
        self.running = True
        while self.running:
            self._clear_stuck_jobs()
            time.sleep(POLL_INTERVAL)

    def stop(self):
        self.running = False

    def _clear_stuck_jobs(self):
        cutoff = datetime.utcnow() - timedelta(minutes=STUCK_TIMEOUT_MINUTES)
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE job_log
                SET status = 'pending',
                    worker_id = NULL,
                    dt_claimed_at = NULL
                WHERE status = 'processing'
                  AND dt_claimed_at < %s
            """, (cutoff,))
            count = cur.rowcount
        self.conn.commit()

        if count:
            print(f"[Orchestrator] Cleared {count} stuck jobs, requeued as pending")


if __name__ == "__main__":
    Orchestrator().start()