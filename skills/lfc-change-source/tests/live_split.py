"""live_split.py: a live split of one table to a log of its own, through lfc_change_source.producer, on a kept
zerobus-debezium-test PostgreSQL run (its source container, its run folder on the VM, its change log and service
principal). The run's Debezium Server is started again from its offsets; then make before break: a second server for
the moved table into a new log (its own slot), until a change of it lands there, then the first restarted without it.
Checked: the moved table's changes after the cut are only in the new log, the other tables' only in the old one, and
the overlap holds no gap. Both servers are stopped at the end (the run is kept). Not an offline test.

    PROFILE=<p> WAREHOUSE=<id> CATALOG=<c> VM_IP=<ip> SSH_KEY=<key> RUN=<run id> [TABLE=orders] python3 -I live_split.py
"""
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
from databricks.sdk import WorkspaceClient  # noqa: E402
from lfc_change_source import producer  # noqa: E402
from lfc_change_source.engine import Warehouse  # noqa: E402

from lfc_change_source._rest import Rest, target_catalog  # noqa: E402
E, CAT = os.environ["RUN"], target_catalog(Rest.connect(os.environ["PROFILE"]), os.environ.get("CATALOG"))
TABLE = os.environ.get("TABLE", "orders")
RD, SRC, NET = f"/tmp/dbz-pg-{E}", f"robertlee-pg-{E}", f"pg-dbz-{E}"
SCH = f"robertlee_pg_tpch_dbz_{E}"
T = f"`{CAT}`.`{SCH}`"
OLD, NEW = f"{T}.cdc_log", f"{T}.cdc_log_{TABLE}"
IMAGE = "quay.io/debezium/server:3.7.0.Final"
wh = Warehouse(os.environ["WAREHOUSE"], WorkspaceClient(profile=os.environ["PROFILE"]))
q = lambda s: wh.sql(s).collect()  # noqa: E731


vm = producer.ssh_run(os.environ["VM_IP"], os.environ["SSH_KEY"])   # a shell command on the VM as root


def psql(sql: str) -> str:
    return vm(f"podman exec -i {SRC} psql -U postgres -d tpch1 -At -v ON_ERROR_STOP=1 <<'SQL'\n{sql}\nSQL\n").strip()


def lsn(text: str) -> int:
    hi, lo = text.split("/")
    return (int(hi, 16) << 32) + int(lo, 16)


def touch(table: str) -> None:
    """A change of one row (a no-op UPDATE: pgoutput logs it with the whole row)."""
    key = {"orders": "o_orderkey", "region": "r_regionkey", "nation": "n_nationkey"}[table]
    psql(f"UPDATE tpch1.{table} SET {key} = {key} WHERE {key} = (SELECT min({key}) FROM tpch1.{table})")


def events(log: str, table: str, after: int = -1, since: int | None = None) -> int:
    """The table's events in a log past `after` (a cut: strictly), or at or past `since` (a WAL position read just
    before a change: the change is recorded at it)."""
    cond = f">= {since}" if since is not None else f"> {after}"
    return int(q(f"SELECT count(*) FROM {log} WHERE destination = 'pg1.tpch1.{table}' "
                 f"AND value:source.lsn::BIGINT {cond}")[0][0])


TEMPLATE = f"{RD}/template.properties"   # the run's own config with every table: a start rewrites a server's own file
TPCH = ("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")
tables = tuple(f"tpch1.{t}" for t in TPCH)
if not vm(f"test -f {TEMPLATE} && echo yes || true").strip():
    text = producer.debezium_properties(vm(f"cat {RD}/src1/application.properties"), tables=tables)
    vm(f"umask 077; cat > {TEMPLATE} <<'LFC_PROPS'\n{text}LFC_PROPS\n")
principal = vm(f"grep '^debezium.sink.zerobus.client.id=' {TEMPLATE}").split("=", 1)[1].strip()
moved = f"tpch1.{TABLE}"
NEWNAME = f"robertlee-debezium-{E}-{TABLE}"
slots = {f"robertlee-debezium-{E}": "dbz_slot_1", NEWNAME: f"dbz_slot_1_{TABLE}"}
# this test's own leftovers from an earlier run: the second server, its folder and slot, the new log
vm(f"podman rm -f --ignore {NEWNAME} >/dev/null; rm -rf -- '{RD}/{NEWNAME}'")
psql(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'dbz_slot_1_{TABLE}'")
q(f"DROP TABLE IF EXISTS {NEW}")


def make(spec):
    return producer.DebeziumServer(spec, vm, RD, TEMPLATE, IMAGE, NET, slot=slots[spec.name],
                                   folder=f"{RD}/src1" if spec.name == f"robertlee-debezium-{E}" else None,
                                   position_fn=lambda s: psql(f"SELECT confirmed_flush_lsn FROM pg_replication_slots "
                                                              f"WHERE slot_name = '{s}'"))


old = producer.Spec(f"robertlee-debezium-{E}", OLD, tables)
ok = True
print("starting the run's server again from its offsets", flush=True)
make(old).start()
start = lsn(psql("SELECT pg_current_wal_lsn()"))
touch("region")
for _ in range(60):
    if events(OLD, "region", since=start):
        break
    time.sleep(5)
print("old log carries region again:", events(OLD, "region", since=start) > 0, flush=True)
for s in producer.zerobus_log_sql(NEW, principal):
    q(s)
steps = producer.split(old, [moved], NEW, make, new_name=NEWNAME)


def landed(p):
    touch(TABLE)
    time.sleep(5)
    return events(NEW, TABLE) > 0


out = producer.carry_out(steps, landed=landed)
restarted_ms = int(time.time() * 1000)
# the cut: the moved table's last position in the old log, now that its old producer stopped carrying it
cut = int(q(f"SELECT max(value:source.lsn::BIGINT) FROM {OLD} WHERE destination = 'pg1.tpch1.{TABLE}'")[0][0])
print("split:", out, "cut", cut, flush=True)
after = lsn(psql("SELECT pg_current_wal_lsn()"))
print("changes after", after, flush=True)
touch(TABLE)
touch("region")
for _ in range(60):
    if events(NEW, TABLE, since=after) and events(OLD, "region", since=after):
        break
    time.sleep(5)
checks = {
    f"{TABLE} changed after the split: in the new log": events(NEW, TABLE, since=after) > 0,
    f"{TABLE} committed after the restart: not in the old log": int(q(
        f"SELECT count(*) FROM {OLD} WHERE destination = 'pg1.tpch1.{TABLE}' "
        f"AND value:source.ts_ms::BIGINT > {restarted_ms}")[0][0]) == 0,
    f"{TABLE} after the cut: in the new log": events(NEW, TABLE, cut) > 0,
    "region changed after the split: in the old log": events(OLD, "region", since=after) > 0,
    "region: never in the new log": events(NEW, "region") == 0,
    # no gap: the new log's first change of the table is at or before the cut (the logs overlap)
    "no gap: the new log starts at or before the cut": int(q(f"SELECT min(value:source.lsn::BIGINT) FROM {NEW} WHERE "
                                                            f"destination = 'pg1.tpch1.{TABLE}'")[0][0]) <= cut,
}
for k, v in checks.items():
    print(("ok   " if v else "FAIL ") + k, flush=True)
    ok &= v
for name in slots:
    vm(f"podman stop -t 60 {name} >/dev/null 2>&1 || true")
print("both servers stopped (kept); the new slot stays:", slots[NEWNAME])
print("new log:", q(f"SELECT destination, value:source.lsn::BIGINT FROM {NEW} ORDER BY 2"))
print("ALL OK" if ok else "MISMATCH")
sys.exit(0 if ok else 1)
