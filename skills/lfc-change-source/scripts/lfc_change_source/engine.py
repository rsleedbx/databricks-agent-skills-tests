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
        """workspace: an lfc_rest.Rest or a WorkspaceClient (none: the environment's login, a notebook's own)."""
        from ._rest import Rest
        self.id, self.w, self.poll_s = warehouse_id, workspace, poll_s
        self.api = Rest.of(workspace) if workspace is not None else Rest.connect()

    @classmethod
    def pick(cls, workspace=None, poll_s: float = 0.5) -> Warehouse:
        """A warehouse of the workspace's: a running one first, then a serverless one (no cluster start to wait for)."""
        from ._rest import Rest
        api = Rest.of(workspace) if workspace is not None else Rest.connect()
        whs = api.get("/api/2.0/sql/warehouses").get("warehouses") or []
        if not whs:
            raise RuntimeError("no SQL warehouse in this workspace")
        best = sorted(whs, key=lambda w: (w.get("state") != "RUNNING", not w.get("enable_serverless_compute")))[0]
        return cls(best["id"], workspace, poll_s)

    def state(self) -> str:
        """The warehouse's state (RUNNING, STOPPED, STARTING, ...): a stopped one costs its start first."""
        return self.api.get(f"/api/2.0/sql/warehouses/{self.id}").get("state") or ""

    def phases(self, statement_ids: list[str], wait_s: float = 30.0) -> dict:
        """{statement id: {compile_s, execute_s, queue_s, read_bytes, read_files}} from query history, once each
        statement's entry is final (it lags the statement by seconds); missing ones are left out."""
        out, todo, end = {}, list(dict.fromkeys(i for i in statement_ids if i)), time.time() + wait_s
        while todo and time.time() < end:
            for i in range(0, len(todo), 100):
                for q in self.api.pages("/api/2.0/sql/history/queries", "res", filter_by={"statement_ids": todo[i:i + 100]},
                                        include_metrics=True, max_results=100):
                    m = q.get("metrics")
                    if not q.get("is_final") or m is None:
                        continue
                    cs = m.get("query_compilation_start_timestamp")
                    queued = ((cs or 0) - (q.get("query_start_time_ms") or 0)) / 1e3
                    out[q["query_id"]] = {"compile_s": (m.get("compilation_time_ms") or 0) / 1e3,
                                          "execute_s": (m.get("execution_time_ms") or 0) / 1e3,
                                          "queue_s": max(0.0, queued) if cs else None,
                                          "read_bytes": m.get("read_bytes"), "read_files": m.get("read_files_count")}
            todo = [i for i in todo if i not in out]
            if todo:
                time.sleep(5)
        return out

    def sql(self, query: str, args: dict | None = None) -> _Result:
        params = [{"name": k, "value": None if v is None else str(v)} for k, v in (args or {}).items()] or None
        # wait_timeout 0s: the POST returns at once and the statement is polled; a POST held open longer than the
        # client's read timeout (60 s, a statement with a long plan) was retried, and a retried POST can start it again
        body = {"statement": query, "warehouse_id": self.id, "wait_timeout": "0s", "on_wait_timeout": "CONTINUE",
                "disposition": "INLINE", "format": "JSON_ARRAY", **({"parameters": params} if params else {})}
        r = self.api.post("/api/2.0/sql/statements", body)
        sid = r.get("statement_id")
        try:
            while (r.get("status") or {}).get("state") in ("PENDING", "RUNNING"):
                time.sleep(self.poll_s)
                r = self.api.get(f"/api/2.0/sql/statements/{sid}")
        except BaseException:   # interrupted or killed (SIGINT as KeyboardInterrupt): don't leave it running
            try:
                self.api.post(f"/api/2.0/sql/statements/{sid}/cancel")
            except Exception:  # noqa: BLE001  the cancel is best effort; the interruption is what to raise
                pass
            raise
        st = r.get("status") or {}
        if st.get("state") != "SUCCEEDED":
            e = st.get("error") or {}
            raise RuntimeError(f"{e.get('error_code') or st.get('state')}: {e.get('message') or ''}")
        cols = ((r.get("manifest") or {}).get("schema") or {}).get("columns") or []
        names = [c.get("name") for c in cols]
        types = [c.get("type_name") or "STRING" for c in cols]
        make = row_type(names)
        chunk = r.get("result") or {}
        data = list(chunk.get("data_array") or [])
        while chunk.get("next_chunk_index") is not None:
            chunk = self.api.get(f"/api/2.0/sql/statements/{sid}/result/chunks/{chunk['next_chunk_index']}")
            data += chunk.get("data_array") or []
        return _Result([make(value(x, t) for x, t in zip(row, types)) for row in data], sid)
