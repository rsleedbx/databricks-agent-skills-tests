"""engine: run SQL on a SQL warehouse through the Statement Execution API, with the part of SparkSession consumers
use: sql(query, args=None).collect() -> rows read by name (r.version) or position (r[0]). So code written for a Spark
session (a change source's SQL, lfc-medallion's merge route, the conformance checks) runs on either.

    from lfc_change_source.engine import Warehouse
    wh = Warehouse("abc123")                 # the WorkspaceClient from the environment (a job: its own identity)
    wh.sql("SELECT 1 AS one").collect()[0].one
"""
from __future__ import annotations

import datetime as dt
import decimal
import json
import time

INTS = {"BYTE", "SHORT", "INT", "LONG"}
FLOATS = {"FLOAT", "DOUBLE"}
NESTED = {"MAP", "STRUCT", "ARRAY"}


class Row(tuple):
    """A result row: a tuple with its columns by name too, like pyspark's Row."""
    __slots__ = ()
    _fields: tuple = ()

    def __getattr__(self, name):
        try:
            return self[type(self)._fields.index(name)]
        except ValueError:
            raise AttributeError(name) from None

    def asDict(self) -> dict:  # noqa: N802  (pyspark's name)
        return dict(zip(type(self)._fields, self))


def row_type(names: list[str]) -> type:
    return type("Row", (Row,), {"__slots__": (), "_fields": tuple(names)})


def value(text, type_name: str):
    """One value of a JSON_ARRAY result (text, or None) as Python, by its column type."""
    if text is None:
        return None
    if type_name in INTS:
        return int(text)
    if type_name in FLOATS:
        return float(text)
    if type_name == "DECIMAL":
        return decimal.Decimal(text)
    if type_name == "BOOLEAN":
        return text in (True, "true", "True")
    if type_name in ("TIMESTAMP", "TIMESTAMP_NTZ"):
        t = dt.datetime.fromisoformat(str(text).replace("Z", "+00:00").replace(" ", "T"))
        return t if t.tzinfo or type_name == "TIMESTAMP_NTZ" else t.replace(tzinfo=dt.timezone.utc)
    if type_name == "DATE":
        return dt.date.fromisoformat(str(text))
    if type_name in NESTED:
        return json.loads(text) if isinstance(text, str) else text
    return text


class _Result:
    def __init__(self, rows: list, statement_id: str | None = None):
        self._rows, self.statement_id = rows, statement_id   # the id: its phases in query history (route.phases)

    def collect(self) -> list:
        return list(self._rows)


class Warehouse:
    """sql() runs at once (a DDL or DML statement needs no collect, as on Spark), waits for the statement, reads
    every chunk, and raises RuntimeError with the error's class and message (so a caller's checks for
    CONCURRENT... or SCHEMA_NOT_FOUND work as on Spark)."""

    def __init__(self, warehouse_id: str, workspace=None, poll_s: float = 0.5):
        if workspace is None:
            from databricks.sdk import WorkspaceClient
            workspace = WorkspaceClient()
        self.id, self.w, self.poll_s = warehouse_id, workspace, poll_s

    def state(self) -> str:
        """The warehouse's state (RUNNING, STOPPED, STARTING, ...): a stopped one costs its start first."""
        return str(self.w.warehouses.get(self.id).state).split(".")[-1]

    def phases(self, statement_ids: list[str], wait_s: float = 30.0) -> dict:
        """{statement id: {compile_s, execute_s, queue_s, read_bytes, read_files}} from query history, once each
        statement's entry is final (it lags the statement by seconds); missing ones are left out."""
        from databricks.sdk.service.sql import QueryFilter
        out, todo, end = {}, list(dict.fromkeys(i for i in statement_ids if i)), time.time() + wait_s
        while todo and time.time() < end:
            for i in range(0, len(todo), 100):
                for q in self.w.query_history.list(filter_by=QueryFilter(statement_ids=todo[i:i + 100]),
                                                   include_metrics=True, max_results=100):
                    m = q.metrics
                    if not q.is_final or m is None:
                        continue
                    queued = ((m.query_compilation_start_timestamp or 0) - (q.query_start_time_ms or 0)) / 1e3
                    out[q.query_id] = {"compile_s": (m.compilation_time_ms or 0) / 1e3,
                                       "execute_s": (m.execution_time_ms or 0) / 1e3,
                                       "queue_s": max(0.0, queued) if m.query_compilation_start_timestamp else None,
                                       "read_bytes": m.read_bytes, "read_files": m.read_files_count}
            todo = [i for i in todo if i not in out]
            if todo:
                time.sleep(5)
        return out

    def sql(self, query: str, args: dict | None = None) -> _Result:
        from databricks.sdk.service.sql import (Disposition, ExecuteStatementRequestOnWaitTimeout, Format,
                                                StatementParameterListItem, StatementState)
        params = [StatementParameterListItem(name=k, value=None if v is None else str(v))
                  for k, v in (args or {}).items()] or None
        r = self.w.statement_execution.execute_statement(
            statement=query, warehouse_id=self.id, parameters=params, wait_timeout="50s",
            on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE, disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY)
        while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(self.poll_s)
            r = self.w.statement_execution.get_statement(r.statement_id)
        if r.status.state != StatementState.SUCCEEDED:
            e = r.status.error
            raise RuntimeError(f"{e.error_code if e else r.status.state}: {e.message if e else ''}")
        cols = (r.manifest.schema.columns or []) if r.manifest and r.manifest.schema else []
        names = [c.name for c in cols]
        types = [str(c.type_name).split(".")[-1] if c.type_name else "STRING" for c in cols]
        make = row_type(names)
        data, chunk = list((r.result.data_array if r.result else None) or []), r.result
        while chunk is not None and chunk.next_chunk_index is not None:
            chunk = self.w.statement_execution.get_statement_result_chunk_n(r.statement_id, chunk.next_chunk_index)
            data += chunk.data_array or []
        return _Result([make(value(x, t) for x, t in zip(row, types)) for row in data], r.statement_id)
