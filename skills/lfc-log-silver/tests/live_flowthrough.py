"""live_flowthrough.py: a keyless table through flowthrough's observe and then step (pinned): observe appends it from
the start (a view can't pick a keyless table's rows) and pins it as append, later steps append, and the saved route
says append; a keyed table is a view while observed, then its route. Not an offline test.

    PROFILE=<p> WAREHOUSE=<id> CATALOG=<c> [SCHEMA=<scratch>] python3 -I live_flowthrough.py
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
for s in ("lfc-change-source", "lfc-log-silver", "lfc-routing"):
    sys.path.insert(0, os.path.join(HERE, "..", "..", s, "scripts"))
from databricks.sdk import WorkspaceClient  # noqa: E402
from lfc_change_source.cdc_log import CdcLog  # noqa: E402
from lfc_change_source.contract import Position  # noqa: E402
from lfc_change_source.engine import Warehouse  # noqa: E402
from lfc_log_silver import flowthrough  # noqa: E402
from lfc_routing import routing  # noqa: E402

CAT, SCH = os.environ["CATALOG"], os.environ.get("SCHEMA", "lfc_log_silver_flowthrough")
S = f"`{CAT}`.`{SCH}`"
wh = Warehouse(os.environ["WAREHOUSE"], WorkspaceClient(profile=os.environ["PROFILE"]))
q = lambda s: wh.sql(s).collect()  # noqa: E731
LOG = f"{S}.cdc_log"
q(f"CREATE SCHEMA IF NOT EXISTS {S}")
for t in ("customers", "events", "_log_silver", "_flowthrough", "_shared_0", "schema_drift"):
    for k in ("VIEW", "TABLE"):
        try:
            q(f"DROP {k} IF EXISTS {S}.`{t}`")
        except RuntimeError:   # the other kind
            pass
q(f"CREATE OR REPLACE TABLE {LOG} (destination STRING, operation STRING, key VARIANT, value VARIANT)")
q(f"CREATE OR REPLACE TABLE {S}.snap_customers (id INT, name STRING)")
q(f"INSERT INTO {S}.snap_customers VALUES (1, 'a')")
q(f"CREATE OR REPLACE TABLE {S}.snap_events (e STRING, n INT)")
q(f"INSERT INTO {S}.snap_events VALUES ('e1', 1)")


def ev(dest, op, key, after, n):
    k = f"parse_json('{json.dumps(key)}')" if key is not None else "NULL"
    return f"('my1.s.{dest}', '{op}', {k}, parse_json('{json.dumps({'after': after, 'ts_ns': n})}'))"


def log(*events):
    q(f"INSERT INTO {LOG} VALUES " + ", ".join(events))


mk = lambda n, k, c: CdcLog("my1", LOG, n, f"my1.s.{n}", k, c, seq="ts_ns")  # noqa: E731
sources = {"customers": mk("customers", ["id"], {"id": "INT", "name": "STRING"}),
           "events": mk("events", [], {"e": "STRING", "n": "INT"})}
pos = Position("ts_ns", "50")
extra = {n: {"snapshot_table": f"{S}.snap_{n}", "position": pos} for n in sources}
policy = routing.Policy(shared_log=LOG)
log(ev("customers", "create", {"id": 2}, {"id": 2, "name": "b"}, 100), ev("events", "create", None, {"e": "e2", "n": 2}, 100))
flowthrough.observe(wh, S, sources, policy, ready={n: f"{S}.snap_{n}" for n in sources})
kind = lambda t: q(f"DESCRIBE TABLE EXTENDED {S}.`{t}`")  # noqa: E731
is_view = lambda t: any(r[0] == "Type" and r[1] == "VIEW" for r in kind(t))  # noqa: E731
ok = is_view("customers") and not is_view("events")
print("observe: customers a view, events a table", "ok" if ok else "MISMATCH")
flowthrough.step(wh, S, sources, Position("ts_ns", "200"), policy, extra=extra)
log(ev("events", "create", None, {"e": "e3", "n": 3}, 300), ev("customers", "update", {"id": 1}, {"id": 1, "name": "a2"}, 300))
flowthrough.step(wh, S, sources, Position("ts_ns", "400"), policy, extra=extra)
want = {"events": [("e1", "1"), ("e2", "2"), ("e3", "3")], "customers": [("1", "a2"), ("2", "b")]}
for t, exp in want.items():
    got = sorted(tuple(None if v is None else str(v) for v in r) for r in q(f"SELECT * FROM {S}.`{t}`"))
    print(t, "ok" if got == exp else f"got {got}, want {exp}")
    ok &= got == exp
view = is_view("events")
saved = flowthrough.load(wh, S)["events"][0].get("method")
print("events a table after step:", "ok" if not view else "still a view", "| saved method:", saved)
ok &= not view and saved == "append"
print("ALL OK" if ok else "MISMATCH")
sys.exit(0 if ok else 1)
