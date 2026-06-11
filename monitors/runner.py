'''
runner.py - Publish changeEvents from WALMonitor and RedoMonitor to SQS queue
'''
# monitors/runner.py
import time
import os
from monitors.wal_monitor import WALMonitor
from monitors.redo_monitor import RedoMonitor
from resilience.job_log import JobLog

POLL_INTERVAL = 5


class MonitorRunner:

    def __init__(self):
        self.wal_monitor = WALMonitor(dsn=os.getenv("POSTGRES_DSN"))
        self.redo_monitor = RedoMonitor(
            dsn=os.getenv("ORACLE_DSN"),
            watched_tables=os.getenv("WATCHED_TABLES", "").split(",")
        )
        self.job_log = JobLog()

    def start(self):
        self.wal_monitor.start()
        self.redo_monitor.start()

        while True:
            for event in self.wal_monitor.poll():
                event.source = "postgres"
                self.job_log.insert(event)

            for event in self.redo_monitor.poll():
                event.source = "oracle"
                self.job_log.insert(event)

            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    MonitorRunner().start()