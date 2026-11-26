"""live_features.py: append-only, foreign key actions and drift on a warehouse, from a synthetic Debezium change log
and snapshot tables, through silver.run, each table against its known rows. Not an offline test: it needs a warehouse.

    PROFILE=<p> WAREHOUSE=<id> CATALOG=<c> python3 -I skills/lfc-log-silver/tests/live_features.py
"""
import json
import sys

import os

T = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
sys.path[:0] = [f"{T}/lfc-change-source/scripts", f"{T}/lfc-log-silver/scripts", f"{T}/lfc-routing/scripts"]
from databricks.sdk import WorkspaceClient  # noqa: E402
from lfc_change_source.cdc_log import CdcLog  # noqa: E402
from lfc_change_source.contract import Position  # noqa: E402
from lfc_change_source.engine import Warehouse  # noqa: E402
from lfc_log_silver import fk, silver  # noqa: E402

CAT, SCH = os.environ["CATALOG"], os.environ.get("SCHEMA", "lfc_log_silver_live")
S = f"`{CAT}`.`{SCH}`"
wh = Warehouse(os.environ["WAREHOUSE"], WorkspaceClient(profile=os.environ["PROFILE"]))
q = lambda s: wh.sql(s).collect()  # noqa: E731
q(f"CREATE SCHEMA IF NOT EXISTS {S}")
LOG = f"{S}.cdc_log"
q(f"CREATE OR REPLACE TABLE {LOG} (destination STRING, operation STRING, key VARIANT, value VARIANT)")
# snapshots: customers 1, 2; orders 10 (cust 1), 11 (cust 2); notes 20 (cust 1), 21 (cust 2); events e1
for t, cols, rows in (("customers", "id INT, name STRING", "(1, 'a'), (2, 'b')"),
                      ("orders", "id INT, cust INT", "(10, 1), (11, 2)"),
                      ("notes", "id INT, cust INT", "(20, 1), (21, 2)"),
                      ("events", "e STRING, n INT", "('e1', 1)")):
    q(f"CREATE OR REPLACE TABLE {S}.snap_{t} ({cols})")
    q(f"INSERT INTO {S}.snap_{t} VALUES {rows}")


def ev(dest, op, key, after, n):
    k = f"parse_json('{json.dumps(key)}')" if key is not None else "NULL"
    v = json.dumps({"after": after, "ts_ns": n})
    return f"('my1.s.{dest}', '{op}', {k}, parse_json('{v}'))"


q(f"INSERT INTO {LOG} VALUES " + ", ".join([
    ev("customers", "delete", {"id": 1}, None, 100),                     # cascades to orders 10, sets notes 20's cust NULL
    ev("events", "create", None, {"e": "e2", "n": 2}, 110),              # appended
    ev("events", "create", None, {"e": "e2", "n": 2}, 110),              # a replay at one position: once
    ev("events", "update", None, {"e": "e1", "n": 9}, 120),              # keyless: an update can't name a row
    ev("customers", "update", {"id": 2}, {"id": 2, "name": "b2", "extra": 1}, 130),   # a column it doesn't know: drift new
    ev("orders", "update", {"id": 11}, {"id": 11, "cust": "notanumber"}, 140),        # drift type: NULL
]))
mk = lambda n, k, c: CdcLog("my1", LOG, n, f"my1.s.{n}", k, c, seq="ts_ns", cast="TRY_CAST")  # noqa: E731
srcs = {"customers": mk("customers", ["id"], {"id": "INT", "name": "STRING"}),
        "orders": mk("orders", ["id"], {"id": "INT", "cust": "INT"}),
        "notes": mk("notes", ["id"], {"id": "INT", "cust": "INT"}),
        "events": mk("events", [], {"e": "STRING", "n": "INT"})}
pos = Position("ts_ns", "50")
snaps = {t: (f"SELECT * FROM {S}.snap_{t}", pos) for t in srcs}
edges = {"orders": [{"cols": ["cust"], "parent": "customers", "parent_cols": ["id"], "on_delete": "CASCADE"}],
         "notes": [{"cols": ["cust"], "parent": "customers", "parent_cols": ["id"], "on_delete": "SET NULL"}]}
w = fk.with_actions(srcs, edges, snaps)
tables = {t: silver.Table(w[t], "merge" if t != "events" else "append", snapshot_table=f"{S}.snap_{t}", position=pos)
          for t in srcs}
q(f"DROP TABLE IF EXISTS {S}._log_silver")
q(f"DROP TABLE IF EXISTS {S}.schema_drift")
silver.run(wh, tables, S, Position("ts_ns", "200"), threads=1, first=True)
silver.run(wh, tables, S, Position("ts_ns", "200"), threads=1)   # a round: drift recorded from the kept positions


def rows(t):
    return sorted(tuple(r) for r in q(f"SELECT * FROM {S}.`{t}`"))


want = {"customers": [("2", "b2")], "orders": [("11", None)], "notes": [("20", None), ("21", "2")],
        "events": [("e1", "1"), ("e2", "2")]}
ok = True
for t, exp in want.items():
    got = [tuple(None if v is None else str(v) for v in r) for r in rows(t)]
    print(t, "ok" if got == exp else f"got {got}, want {exp}")
    ok &= got == exp
print("drift:", sorted((r[0], r[2], r[3]) for r in q(f"SELECT tbl, _source, kind, col FROM {S}.schema_drift")))
print("ALL OK" if ok else "MISMATCH")
sys.exit(0 if ok else 1)
