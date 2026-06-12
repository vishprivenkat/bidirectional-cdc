'''
Audit log tracks successful and failed operations for record-level observability and debugging 
'''

import os
import uuid
import psycopg2
from datetime import datetime

class AuditLog:

    def __init__(self):
        self.conn = psycopg2.connect(os.getenv("POSTGRES_DSN"))

    def log_success(self, job_id: str, table_name: str, operation: str, worker_id: str):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO audit_log (
                    id, job_id, table_name, operation,
                    worker_id, dt_completed_at
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                str(uuid.uuid4()),
                job_id,
                table_name,
                operation,
                worker_id,
                datetime.utcnow()
            ))
        self.conn.commit()

    def log_failure(self, job_id: str, table_name: str, operation: str, worker_id: str, error_message: str, retry_count: int):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO error_log (
                    id, job_id, table_name, operation,
                    worker_id, error_message, retry_count, dt_failed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                str(uuid.uuid4()),
                job_id,
                table_name,
                operation,
                worker_id,
                error_message,
                retry_count,
                datetime.utcnow()
            ))
        self.conn.commit()