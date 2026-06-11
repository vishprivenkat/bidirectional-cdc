'''
jobLog functions as both a queue as well as as a dead letter queue. 
Monitors insert change events into job_log with status "pending" 
Consumers claim "pending" or "failed" jobs 
'''

import uuid
import os
import psycopg2
from datetime import datetime
from monitors.base import ChangeEvent

MAX_RETRY_COUNT = int(os.getenv("MAX_RETRY_COUNT", 3)) 

class JobLog:

    def __init__(self):
        self.conn = psycopg2.connect(os.getenv("POSTGRES_DSN"))

    def insert(self, event: ChangeEvent):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO job_log (
                    id, table_name, operation, query_executed,
                    dt_operation_executed, source, status,
                    worker_id, dt_claimed_at, retry_count,
                    error_message, dt_completed_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    'pending', NULL, NULL, 0, NULL, NULL
                )
            """, (
                str(uuid.uuid4()),
                event.table_name,
                event.operation,
                event.query_executed,
                event.dt_operation_executed,
                event.source
            ))
        self.conn.commit()
    def claim(self, worker_id: str) -> dict | None:
      with self.conn.cursor() as cur:
          cur.execute("""
              UPDATE job_log
              SET status = 'processing',
                  worker_id = %s,
                  dt_claimed_at = %s
              WHERE id = (
                  SELECT id FROM job_log
                  WHERE (status = 'pending' OR (status = 'failed' AND retry_count < %s))
                  ORDER BY dt_operation_executed ASC
                  FOR UPDATE SKIP LOCKED
                  LIMIT 1
              )
              RETURNING id, table_name, operation, query_executed, source
          """, (worker_id, datetime.utcnow(), MAX_RETRY_COUNT))
          row = cur.fetchone()
      self.conn.commit()

      if row:
          return {
              "id": row[0],
              "table_name": row[1],
              "operation": row[2],
              "query_executed": row[3],
              "source": row[4]
          }
      return None
    
    def complete(self, job_id: str):
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE job_log
                SET status = 'completed',
                    dt_completed_at = %s
                WHERE id = %s
            """, (datetime.utcnow(), job_id))
        self.conn.commit()

    def fail(self, job_id: str, error_message: str):
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE job_log
                SET status = 'failed',
                    error_message = %s,
                    retry_count = retry_count + 1
                WHERE id = %s
            """, (error_message, job_id))
        self.conn.commit()

    def free_worker(self, worker_id: str):
        """Called on consumer restart — releases any job this worker held."""
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE job_log
                SET status = 'pending',
                    worker_id = NULL,
                    dt_claimed_at = NULL
                WHERE worker_id = %s
                  AND status = 'processing'
            """, (worker_id,))
        self.conn.commit()