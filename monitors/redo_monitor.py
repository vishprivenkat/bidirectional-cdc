'''
Redo Monitor to read Oracle redo logs using LogMiner 
'''
import cx_Oracle
from datetime import datetime
from monitors.base import BaseMonitor, ChangeEvent


class RedoMonitor(BaseMonitor):

    def __init__(self, dsn: str, watched_tables: list[str], poll_interval: int = 5):
        self.dsn = dsn
        self.watched_tables = [t.upper() for t in watched_tables]
        self.poll_interval = poll_interval
        self.conn = None
        self.running = False
        self._last_scn = None

    def start(self):
        self.conn = cx_Oracle.connect(self.dsn)
        self._last_scn = self._current_scn()
        self.running = True

    def stop(self):
        self.running = False
        if self.conn:
            self.conn.close()

    def _current_scn(self) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT CURRENT_SCN FROM V$DATABASE")
        return cur.fetchone()[0]

    def poll(self) -> list[ChangeEvent]:
        events = []
        current_scn = self._current_scn()

        table_filter = ", ".join(f"'{t}'" for t in self.watched_tables)

        query = f"""
            SELECT
                seg_name,
                operation,
                sql_redo,
                timestamp
            FROM v$logmnr_contents
            WHERE scn > :start_scn
              AND scn <= :end_scn
              AND seg_name IN ({table_filter})
              AND operation IN ('INSERT', 'UPDATE', 'DELETE')
        """

        cur = self.conn.cursor()
        cur.execute(query, start_scn=self._last_scn, end_scn=current_scn)
        rows = cur.fetchall()

        for row in rows:
            table_name, operation, sql_redo, timestamp = row
            events.append(ChangeEvent(
                table_name=table_name,
                operation=operation,
                query_executed=sql_redo,
                dt_operation_executed=timestamp or datetime.utcnow()
            ))

        self._last_scn = current_scn
        return events