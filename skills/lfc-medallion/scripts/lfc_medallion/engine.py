"""engine: run lfc_medallion's SQL on a SQL warehouse instead of the job's Spark session. Warehouse(id) has the part of
SparkSession this code uses, sql(query, args=None).collect() -> rows read by name (r.version) or position (r[0]), so
merge_all and merge_route run on either: a merge task can send its statements to the warehouse through the Statement
Execution API and keep the job's compute for nothing else.

Measured 2026-11-22 (scale1, TPC-B, 124 LOBs): one table's MERGE of every LOB's changes (a UNION ALL of their change
feeds) took 141 s in the serverless merge job and 33 s on a serverless SQL warehouse, three tables at once on each.

    from lfc_medallion.engine import Warehouse
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
    def __init__(self, rows: list):
        self._rows = rows

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

    def units(self) -> float:
        """Its capacity for paths.py: the clusters running now (stopped: its minimum) times its size (2X-Small = 1,
        each size up doubles). Not its maximum: autoscaling adds a cluster only after queries have queued a while
        (2026-11-23: 1 to 2 clusters a minute into a burst, never the 4 allowed), too late for MERGEs of a minute."""
        from .paths import units
        w = self.w.warehouses.get(self.id)
        return units(w.cluster_size, max(w.num_clusters or 0, w.min_num_clusters or 1))

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
        return _Result([make(value(x, t) for x, t in zip(row, types)) for row in data])
