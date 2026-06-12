'''
consumer.py - processor nodes that perform the task published to the job_log by monitors 
Note: In real implementation, each consumer node is a separate microservice, which is scaled using EKS. 

'''
import os
import time
import uuid
import psycopg2
import cx_Oracle
from resilience.job_log import JobLog
from resilience.audit_log import AuditLog

POLL_INTERVAL = 5
WORKER_ID = str(uuid.uuid4())


class Consumer:

    def __init__(self):
        self.job_log = JobLog()
        self.audit_log = AuditLog()
        self.running = False

        self.pg_conn = psycopg2.connect(os.getenv("POSTGRES_DSN"))
        self.oracle_conn = cx_Oracle.connect(os.getenv("ORACLE_DSN"))

        # On startup, free any job this worker held before crashing
        self.job_log.free_worker(WORKER_ID)

    def start(self):
        self.running = True
        while self.running:
            job = self.job_log.claim(WORKER_ID)
            if job:
                self._handle(job)
            else:
                time.sleep(POLL_INTERVAL)

    def stop(self):
        self.running = False



    def _handle(self, job: dict):
        try:
            if job["source"] == "postgres":
                self._apply_to_oracle(job["query_executed"])
            elif job["source"] == "oracle":
                self._apply_to_postgres(job["query_executed"])

            self.job_log.complete(job["id"])
            self.audit_log.log_success(
                job_id=job["id"],
                table_name=job["table_name"],
                operation=job["operation"],
                worker_id=WORKER_ID
            )

        except Exception as e:
            self.job_log.fail(job["id"], str(e))
            self.audit_log.log_failure(
                job_id=job["id"],
                table_name=job["table_name"],
                operation=job["operation"],
                worker_id=WORKER_ID,
                error_message=str(e),
                retry_count=0
            )


    def _apply_to_postgres(self, operation: str, table_name: str, query: str):
        '''
        Note: In real implementation, we would need to: 
        1. transform the query from Oracle syntax to Postgres syntax. 
        2. handle the translation of data from XML to JSON  
        3. handle splitting large XML data into chunks for translation 
        4. handle data compression to store large data in BYTEA column in postgres 
        5. handle Latest Write Wins conflict resolution if the same record is being updated in both sides 
        '''
        with self.pg_conn.cursor() as cur:
            cur.execute(query)
        self.pg_conn.commit()

    def _apply_to_oracle(self, operation: str, table_name: str, query: str):
        '''
        Note: In real implementation, we would need to: 
        1. transform the query from Oracle syntax to Postgres syntax. 
        2. handle the translation of data from XML to JSON  
        3. handle splitting large XML data into chunks for translation 
        4. handle data compression to store large data in BYTEA column in postgres 
        5. handle Latest Write Wins conflict resolution if the same record is being updated in both sides 
        '''
        cur = self.oracle_conn.cursor()
        cur.execute(query)
        self.oracle_conn.commit()



if __name__ == "__main__":
    Consumer().start()