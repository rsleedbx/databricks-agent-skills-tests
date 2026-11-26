"""live_provisional.py: routing's step 2 live on a kept zerobus-debezium-test PostgreSQL run: an introspection request,
a cdc_request (the run's Debezium Server started with every table introspected), then a table added in the source,
a second introspection (it shows the table added), a second cdc_request (the server restarted with it, from its
offsets), and a change of the new table in the run's log. The server is stopped at the end (the run is kept), the
added table dropped. Not an offline test.

    PROFILE=<p> WAREHOUSE=<id> CATALOG=<c> SCHEMA=<scratch> VM_IP=<ip> SSH_KEY=<key> RUN=<run id> CONNECTION=<uc conn> \
      python3 -I live_provisional.py
"""
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
for s in ("lfc-routing", "lfc-change-source", "lfc-introspection", "lfc-source-statschema", "lfc-source-contract"):
    sys.path.insert(0, os.path.join(HERE, "..", "..", s, "scripts"))
from databricks.sdk import WorkspaceClient  # noqa: E402
from lfc_change_source import producer  # noqa: E402
from lfc_change_source.engine import Warehouse  # noqa: E402
from lfc_introspection import history  # noqa: E402
from lfc_routing import provisional  # noqa: E402
from lfc_source_contract import cdc, psql as psql_cursor  # noqa: E402

E, CAT, CONN = os.environ["RUN"], os.environ["CATALOG"], os.environ["CONNECTION"]
RD, SRC, NET = f"/tmp/dbz-pg-{E}", f"robertlee-pg-{E}", f"pg-dbz-{E}"
S = f"`{CAT}`.`{os.environ['SCHEMA']}`"
LOG = f"{CAT}.robertlee_pg_tpch_dbz_{E}.cdc_log"   # as Debezium's sink names it
LOGQ = f"`{CAT}`.`robertlee_pg_tpch_dbz_{E}`.cdc_log"   # in SQL
NAME = f"robertlee-debezium-{E}"
wh = Warehouse(os.environ["WAREHOUSE"], WorkspaceClient(profile=os.environ["PROFILE"]))
q = lambda s: wh.sql(s).collect()  # noqa: E731
vm = producer.ssh_run(os.environ["VM_IP"], os.environ["SSH_KEY"])
psql = lambda sql: vm(f"podman exec -i {SRC} psql -U postgres -d tpch1 -At -v ON_ERROR_STOP=1 <<'SQL'\n{sql}\nSQL\n").strip()  # noqa: E731
TEMPLATE = f"{RD}/template.properties"   # tests/live_split.py made it: the run's config with every table


CUR = psql_cursor.cursor(vm, f"podman exec -i {SRC} psql -U postgres -d tpch1")


def publish(tables):   # the run's publication (dbz_pub, FOR TABLE) has every table the server will carry
    print("publication:", cdc.publication(CUR, "dbz_pub", ["tpch1"], log=lambda m: None,
                                          only=[t.split(".", 1)[1] for t in tables]), flush=True)


def make(spec):
    return producer.DebeziumServer(spec, vm, RD, TEMPLATE, "quay.io/debezium/server:3.7.0.Final", NET, slot="dbz_slot_1",
                                   folder=f"{RD}/src1", prepare=publish)


for t in (history.REQUEST, history.RUNS, history.SCHEMA, history.STATS, provisional.REQUEST, provisional.RUNS):
    q(f"DROP TABLE IF EXISTS {S}.{t}")
for stmt in history.ddl(S) + provisional.ddl(S):
    q(stmt)
psql("DROP TABLE IF EXISTS tpch1.lfc_added")
ok = True


def request(table, n, values):
    q(f"INSERT INTO {S}.{table} VALUES ({n}, {values}, 'live_provisional', current_timestamp(), 'test')")


request(history.REQUEST, 1, f"'{CONN}', 'postgres', 'tpch1', 'tpch1'")
r1 = history.run_pending(wh, S)
print("introspection 1:", [(r["status"], r["tables"]) for r in r1], flush=True)
request(provisional.REQUEST, 1, f"'{CONN}', '{LOG}', '{NAME}'")
c1 = provisional.run_cdc_requests(wh, S, make=make, running=lambda src: None)
print("cdc 1:", [(r["action"], r["status"], r["tables"], r.get("error")) for r in c1], flush=True)
ok &= c1[0]["status"] == "ok" and c1[0]["action"] == "start" and c1[0]["tables"] == r1[0]["tables"]
psql("CREATE TABLE tpch1.lfc_added (id INT PRIMARY KEY, v TEXT)")
request(history.REQUEST, 2, f"'{CONN}', 'postgres', 'tpch1', 'tpch1'")
r2 = history.run_pending(wh, S)
print("introspection 2:", [(r["status"], r["tables"], r["changes"]) for r in r2], flush=True)
ok &= r2[0]["changes"] == ["table added: lfc_added"]
request(provisional.REQUEST, 2, f"'{CONN}', '{LOG}', '{NAME}'")
running = producer.Spec(NAME, LOG, tuple(provisional.tables_of(history.latest(wh, S, CONN, 1)[0], "tpch1")))
c2 = provisional.run_cdc_requests(wh, S, make=make, running=lambda src: running)
print("cdc 2:", [(r["action"], r["status"], r["added"], r.get("error")) for r in c2], flush=True)
ok &= c2[0]["action"] == "restart" and c2[0]["added"] == ["tpch1.lfc_added"]
at = int(time.time() * 1000)
psql("INSERT INTO tpch1.lfc_added VALUES (1, 'after the restart')")
for _ in range(60):
    n = int(q(f"SELECT count(*) FROM {LOGQ} WHERE destination = 'pg1.tpch1.lfc_added'")[0][0])
    if n:
        break
    time.sleep(5)
print("the added table's change in the log:", n, f"({(int(time.time() * 1000) - at) / 1000:.0f} s)", flush=True)
ok &= n > 0
vm(f"podman stop -t 60 {NAME} >/dev/null 2>&1 || true")
psql("DROP TABLE IF EXISTS tpch1.lfc_added")
print("ALL OK" if ok else "MISMATCH")
sys.exit(0 if ok else 1)
