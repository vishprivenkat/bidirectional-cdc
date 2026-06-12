'''
Monitors the Write-Ahead Log (WAL) of a PostgreSQL database. 
'''
import psycopg2
import psycopg2.extras
from datetime import datetime
from monitors.base import BaseMonitor, ChangeEvent


class WALMonitor(BaseMonitor):

    def __init__(self, dsn: str, slot_name: str = "cdc_slot", poll_interval: int = 5):
        self.dsn = dsn # e.g. postgresql://user:password@host:port/dbname 
        self.slot_name = slot_name # to trace which slot is being used for replication 
        self.poll_interval = poll_interval # default 5 seconds 
        self.conn = None
        self.running = False

    def start(self):
        self.conn = psycopg2.connect(self.dsn)
        self.conn.set_isolation_level(0)  # autocommit required for replication
        self.running = True

    def stop(self):
        self.running = False
        if self.conn:
            self.conn.close()

    def poll(self) -> list[ChangeEvent]:
        events = []
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM pg_logical_slot_get_changes(%s, NULL, NULL, 'include-timestamp', 'on')",
                (self.slot_name,)
            )
            rows = cur.fetchall()
            for row in rows:
                # row: (lsn, xid, data)
                data = row[2]
                event = self._parse(data)
                if event:
                    events.append(event)
        return events

    def _parse(self, data: str) -> ChangeEvent | None:
        # wal2json or pgoutput produces structured output
        # This parses the logical replication message
        try:
            lines = data.strip().split("\n")
            operation = None
            table_name = None
            dt_operation_executed = datetime.utcnow()

            for line in lines:
                if line.startswith("table"):
                    parts = line.split(":")
                    table_name = parts[0].replace("table", "").strip().split(".")[-1]
                if any(op in line for op in ["INSERT", "UPDATE", "DELETE"]):
                    for op in ["INSERT", "UPDATE", "DELETE"]:
                        if op in line:
                            operation = op
                            break

            if table_name and operation:
                return ChangeEvent(
                    table_name=table_name,
                    operation=operation,
                    query_executed=data,
                    dt_operation_executed=dt_operation_executed
                )
        except Exception:
            return None