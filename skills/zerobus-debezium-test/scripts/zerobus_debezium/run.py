"""run: the bronze rounds for one engine, Debezium Server in a workspace notebook, the source a slot database.

    zerobus-debezium --profile <p> --catalog <c> --schema <s> --engine sqlserver \\
        --slot-host <vm> --slot-name dbz-ms --notebook /Users/<me>/debezium-server-zerobus \\
        --package /Volumes/<c>/<s>/debezium/wheels/zerobus_debezium-0.1.0-py3-none-any.whl \\
        --zb-client-id <application id> --scope <scope> [--admin-key ~/.ssh/<key> --owner <owner>]   # Db2

Rounds (engines.Engine.changes): 0 a row before the first start; 1 two inserts, an update, a delete while streaming;
2 an insert and an update while stopped; 3 a delete after the restart. Each round's events must land in cdc_log once,
in order, and nothing else from this producer. Prints one line per claim and a JSON summary at the end.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
SKILLS = os.path.join(HERE, "..", "..", "..")
for dep in ("lfc-rest", "lfc-change-source", "lfc-source-contract", "lfc-routing", "lfc-log-silver"):   # a checkout: the sibling skills' packages, when not installed
    sys.path.insert(0, os.path.join(SKILLS, dep, "scripts"))

from lfc_change_source import SNAPSHOTS
from lfc_change_source.cdc_log import SEQUENCES, CdcLog
from lfc_change_source.engine import Warehouse
from lfc_change_source.producer import Spec, WorkspaceDebeziumServer
from lfc_log_silver import flowthrough
from lfc_rest import Rest
from lfc_routing.routing import Policy, snapshot_method
from lfc_source_contract import federation, jdbc

from zerobus_debezium import destination, engines
from zerobus_debezium.source import Slot


def landed(wh, log: str, prefix: str, key: str, seq: str, ids: range | None = None) -> list:
    """(operation, id) of this producer's change rows in cdc_log, in the source's order (seq: a SEQUENCES kind);
    ids: only those."""
    rows = wh.sql(f"SELECT operation, CAST(key:{key} AS BIGINT) AS id FROM {log} "
                  f"WHERE startswith(destination, '{prefix}.') ORDER BY {SEQUENCES[seq]}, value:ts_ns::BIGINT").collect()
    return [(r[0], r[1]) for r in rows if ids is None or r[1] in ids]


def settled(wh, log, prefix, key, seq, ids: range, want: int, settle_s: int, timeout_s: int = 300) -> list:
    """This run's events (ids) once there are want of them, then settle_s more (a duplicate or a late event lands
    after the count is reached)."""
    end = time.time() + timeout_s
    while len(landed(wh, log, prefix, key, seq, ids)) < want and time.time() < end:
        time.sleep(10)
    time.sleep(settle_s)
    return landed(wh, log, prefix, key, seq, ids)


def _ls(api, folder: str) -> list:
    try:
        return list(api.ls(folder + "/"))
    except Exception:  # noqa: BLE001 -- no folder yet: a first start
        return []


def take_snapshot(api, wh, e, conn: dict, a, snap_schema: str, pipeline: str, log: str) -> dict:
    """The test table's snapshot by the method routing picks for the engine (lfc_change_source.SNAPSHOTS: qbc, else
    jdbc), through a UC connection of the kind it reads (federation's type, or TYPE JDBC), into snap_schema."""
    src = e.source(conn)
    how = snapshot_method(src["table"], 0, Policy(shared_log=log, engine=e.name))
    name = f"{a.connection_prefix}-{a.engine}-{a.slot_name}"
    if how == "qbc":
        federation.upsert(api, federation.body(name, e.name, conn["host"], conn["port"], src["login"], conn["password"],
                                               service=src.get("service"), comment="zerobus-debezium-test snapshot"))
        n = SNAPSHOTS["qbc"].source_names(e.name, src["database"], src["schema"], src["table"])
        wh.sql(f"CREATE SCHEMA IF NOT EXISTS {destination.q(a.catalog, snap_schema)}")
        got = SNAPSHOTS["qbc"].snapshot(api, pipeline, name, a.catalog, snap_schema, n["catalog"], n["schema"],
                                        {n["table"]: [e.key]}, full_refresh=True)   # each round's snapshot from scratch
        return {"how": how, **got, "table": SNAPSHOTS["qbc"].snapshot_table(a.catalog, snap_schema, n["table"]),
                "link": f"{api.host}/pipelines/{got['pipeline_id']}"}
    if how == "jdbc":
        if not a.change_source_package:
            raise ValueError(f"{e.name}: the jdbc snapshot needs --change-source-package")
        url = jdbc.url(e.name, conn["host"], conn["port"], src["database"], tls=src.get("tls", True))
        federation.upsert(api, jdbc.connection_body(name, e.name, url, src["login"], conn["password"],
                                                    comment="zerobus-debezium-test snapshot"))
        t = jdbc.table_name(e.name, src["schema"], src["table"])
        got = SNAPSHOTS["jdbc"].snapshot(api, name, [t], a.catalog, snap_schema,
                                         f"{os.path.dirname(a.notebook)}/jdbc_snapshot", a.change_source_package)
        return {"how": how, **got, "table": SNAPSHOTS["jdbc"].snapshot_table(a.catalog, snap_schema, t),
                "link": got["url"]}
    raise ValueError(f"{e.name}: snapshot method {how}: not one this test takes")


TABLE_COLUMNS = {"id": "INT", "name": "STRING", "balance": "DECIMAL(12,2)"}   # engines' test table, as silver types it


def silver_source(wh, e, log: str, prefix: str) -> CdcLog:
    """The test table as FlowThrough's CDC layer gives it: a CdcLog over this producer's events (its destination read
    from the log; Oracle's and Db2's names in upper case, as their events carry them; the engine's order kind)."""
    d = wh.sql(f"SELECT DISTINCT destination FROM {log} WHERE startswith(destination, '{prefix}.')").collect()
    if len(d) != 1:
        raise ValueError(f"{prefix}: {len(d)} destinations in the log, want the test table's one")
    up = e.key.isupper()
    cols = {(c.upper() if up else c): t for c, t in TABLE_COLUMNS.items()}
    return CdcLog(prefix, log, "customers", d[0][0], keys=[e.key], columns=cols, seq=e.seq)


def silver_round(wh, schema: str, src: CdcLog, snapshot_table: str, log: str, engine: str) -> dict:
    """One FlowThrough round (layer 4) for the test table: routing's plan, the snapshot read in place at position
    None (before every change in the log: each key's later changes win), up to the log's watermark."""
    wh.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    return flowthrough.step(wh, schema, {"customers": src}, src.watermark(wh), Policy(shared_log=log, engine=engine),
                            extra={"customers": {"snapshot_table": snapshot_table, "position": None}})


def differ(wh, a: str, b: str) -> int:
    """Rows in one table and not the other, both ways (EXCEPT ALL), on the test table's columns as text."""
    q = lambda t: f"SELECT CAST(id AS BIGINT), name, CAST(CAST(balance AS DECIMAL(12,2)) AS STRING) FROM {t}"  # noqa: E731
    return int(wh.sql(f"SELECT (SELECT count(*) FROM ({q(a)} EXCEPT ALL {q(b)})) + "
                      f"(SELECT count(*) FROM ({q(b)} EXCEPT ALL {q(a)}))").collect()[0][0])


def rows(wh, table: str, ids: range) -> dict:
    """{id: (name, balance as text)} of the snapshot's rows with those ids (names fold: Oracle's are upper)."""
    got = wh.sql(f"SELECT CAST(id AS BIGINT), name, CAST(CAST(balance AS DECIMAL(12,2)) AS STRING) FROM {table} "
                 f"WHERE id BETWEEN {ids.start} AND {ids.stop - 1}").collect()
    return {r[0]: (r[1], r[2]) for r in got}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--profile", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--volume", default="debezium")
    # Informix has no test table of its own (its setup is capture only): it runs through stream.py on existing tables
    ap.add_argument("--engine", required=True, choices=sorted(e for e in engines.ENGINES if e != "informix"))
    ap.add_argument("--slot-host", required=True)
    ap.add_argument("--slot-name", required=True)
    ap.add_argument("--slot-key", default="~/.ssh/lfc_slot_ed25519")
    ap.add_argument("--admin-key", help="the VM admin login, for an engine with shell setup steps (Db2)")
    ap.add_argument("--admin-user", default="azureuser")
    ap.add_argument("--owner", help="your owner name on the slot VM (the container's name), for shell steps")
    ap.add_argument("--notebook", required=True, help="the workspace path of notebooks/debezium-server-zerobus.py")
    ap.add_argument("--package", required=True, help="what the notebook pip-installs: this package's wheel or URL")
    ap.add_argument("--zb-client-id", required=True, help="the service principal's application id")
    ap.add_argument("--scope", required=True, help="the secret scope with zb-client-secret; the db password goes there")
    ap.add_argument("--base", type=int, default=100, help="the first id the rounds use")
    ap.add_argument("--skip-setup", action="store_true", help="the source is prepared already")
    ap.add_argument("--snapshot", choices=["auto", "none"], default="auto",
                    help="after round 1: a query-based pipeline through a UC connection, or (Db2) Spark's jdbc source "
                         "through a TYPE JDBC one; none: no snapshot")
    ap.add_argument("--connection-prefix", default="dbz", help="the UC connection's name: <prefix>-<engine>-<slot>")
    ap.add_argument("--change-source-package", help="the jdbc snapshot (Db2): what its notebook pip-installs for "
                                                     "lfc_change_source (its wheel in a Volume, or the repo archive)")
    a = ap.parse_args(argv)

    e = engines.get(a.engine)
    api = Rest.connect(a.profile)
    wh = Warehouse.pick(api)
    slot = Slot(a.slot_host, a.slot_name, key=a.slot_key, owner=a.owner, admin_user=a.admin_user)
    info = slot.up(a.engine, over_limit=True)
    conn = {"host": info["host"], "port": info["port"], "user": info["user"], "password": info.pop("password")}
    if info.get("service"):
        conn["service"] = info["service"]
    if a.engine == "db2":
        conn["database"] = info.get("database", "LFCDB")
    tag = a.slot_name.replace("-", "_")
    conn.update(slot=f"dbz_{tag}", publication=f"dbz_pub_{tag}")   # PostgreSQL: the producer's own slot and publication
    login = dict(conn, user="c##dbzuser") if a.engine == "oracle" else conn   # Oracle: LogMiner's common user
    name = f"dbz-{a.engine}-{a.slot_name}"
    prefix = name.replace("-", "_")
    pw_key = f"{a.slot_name}-password"
    destination.put_secret(api, a.scope, pw_key, login["password"])
    log = destination.change_log(wh, a.catalog, a.schema, principal=a.zb_client_id)
    results = []

    def claim(cid, ok, detail):
        results.append({"claim": cid, "ok": bool(ok), "detail": detail})
        print(f"{'PASS' if ok else 'FAIL'} {a.engine} {cid}: {detail}", flush=True)

    if not a.skip_setup:
        slot.setup(e.setup(conn), admin_key=a.admin_key, owner=a.owner)
    slot.sql(e.changes(0, a.base))
    params = {"engine": a.engine, "package": a.package, "catalog": a.catalog, "schema": a.schema, "volume": a.volume,
              "db_host": login["host"], "db_port": str(login["port"]), "db_user": login["user"],
              "db_name": conn.get("service") or conn.get("database") or "",
              "secret_scope": a.scope, "secret_key": pw_key, "zb_client_id": a.zb_client_id,
              "zb_secret_key": "zb-client-secret", "slot_name": conn["slot"], "publication_name": conn["publication"],
              "idle_timeout_sec": "", "max_runtime_sec": "2400"}
    folder = f"/Volumes/{a.catalog}/{a.schema}/{a.volume}/{name}"
    p = WorkspaceDebeziumServer(Spec(name, log), api, a.notebook, folder, params)
    # a producer that ran before resumes from its offsets: the round-0 row, inserted after them, is a change it must
    # carry; on a first start (no offsets) it's before the stream begins and must not appear (no snapshot)
    fresh = not any(f["name"] == "offsets.dat" for f in _ls(api, folder))
    ids = range(a.base, a.base + 10)   # this run's rows: the checks look only at them
    silver_schema = destination.q(a.catalog, f"{a.schema}_silver_{tag}")
    snap = src = None
    old = landed(wh, log, prefix, e.key, e.seq)
    if landed(wh, log, prefix, e.key, e.seq, ids):
        raise SystemExit(f"ids {a.base}..{a.base + 9} are in the log already: pick another --base")
    t0 = time.time()
    try:
        p.start()
        claim("streaming", True, f"Debezium Server streaming {int(time.time() - t0)} s after submit ({p.run()['url']})")

        slot.sql(e.changes(1, a.base))
        first = [] if fresh else [("create", a.base)]
        want = first + e.expected(1, a.base)
        got = settled(wh, log, prefix, e.key, e.seq, ids, len(want), e.lag_s)
        new = got
        if fresh:
            claim("no-snapshot", ("create", a.base) not in new, "the row inserted before the first start isn't in the log")
        else:
            claim("resume", new[:1] == first, "the row inserted while stopped (after the offsets) is carried on resume")
        claim("round-1", got == want, f"inserts, update, delete while streaming: {new}")
        if a.snapshot == "auto":   # Debezium streams and round 1 has landed: the snapshot's turn
            try:   # a failed snapshot is a failed claim: the rounds after it still run
                snap = take_snapshot(api, wh, e, conn, a, f"{a.schema}_snap_{tag}", f"{name}-snapshot", log)
                have = rows(wh, snap["table"], ids) if snap["state"] == "COMPLETED" else {}
                claim("snapshot", snap["state"] == "COMPLETED" and have == e.state_after(1, a.base),
                      f"{snap['how']} {snap['state']} in {snap['seconds']} s into {snap['table']}: {have} "
                      f"({snap['link']}){' ' + str(snap['error'])[:300] if snap.get('error') else ''}")
            except Exception as x:  # noqa: BLE001 -- recorded as the claim's failure, with why
                snap = None
                claim("snapshot", False, f"{type(x).__name__}: {str(x)[:400]}")
            if snap and snap["state"] == "COMPLETED":   # FlowThrough's silver over the snapshot and the log
                try:
                    src = silver_source(wh, e, log, prefix)
                    got_s = silver_round(wh, silver_schema, src, snap["table"], log, a.engine)
                    have = rows(wh, f"{silver_schema}.`customers`", ids)
                    claim("silver-1", have == e.state_after(1, a.base),
                          f"routes {got_s['routes']}: {have} ({api.host}/explore/data/{a.catalog}/"
                          f"{a.schema}_silver_{tag}/customers)")
                except Exception as x:  # noqa: BLE001
                    src = None
                    claim("silver-1", False, f"{type(x).__name__}: {str(x)[:400]}")

        r = p.stop()
        claim("stop", r == "SUCCESS", f"STOP file: run {r}, status {p.status().get('why')}")
        slot.sql(e.changes(2, a.base))
        p.start()
        slot.sql(e.changes(3, a.base))
        mark = len(want)
        want = want + e.expected(2, a.base) + e.expected(3, a.base)
        got = settled(wh, log, prefix, e.key, e.seq, ids, len(want), e.lag_s)
        claim("restart", got[:len(want)] == want, f"while stopped, then after the restart: {got[mark:]}")
        claim("no-duplicates", got == want, f"{len(got)} events for {len(want)} changes")
        if src is not None:   # the next round: silver after the restart's changes, then the whole table vs the source
            try:
                silver_round(wh, silver_schema, src, snap["table"], log, a.engine)
                have = rows(wh, f"{silver_schema}.`customers`", ids)
                claim("silver-3", have == e.state_after(3, a.base), f"after the restart's changes: {have}")
                ver = take_snapshot(api, wh, e, conn, a, f"{a.schema}_verify_{tag}", f"{name}-verify", log)
                n = differ(wh, f"{silver_schema}.`customers`", ver["table"]) if ver["state"] == "COMPLETED" else -1
                total = wh.sql(f"SELECT count(*) FROM {silver_schema}.`customers`").collect()[0][0]
                claim("silver-equals-source", n == 0, f"{n} rows differ of {total}, against a new {ver['how']} "
                      f"snapshot of the source ({ver['link']})")
            except Exception as x:  # noqa: BLE001
                claim("silver-3", False, f"{type(x).__name__}: {str(x)[:400]}")
        late = [x for x in landed(wh, log, prefix, e.key, e.seq) if x[1] not in ids][len(old):]
        if late:
            print(f"OBSERVED {a.engine} late: {late} from an earlier run arrived during this one (not lost)", flush=True)
        r = p.stop()
        claim("stop-2", r == "SUCCESS", f"run {r}")
    finally:   # a failed round mustn't leave the workspace run streaming (and holding this producer's offsets)
        try:
            if p.active():
                p.cancel()   # not the STOP file: the notebook clears one written before its loop starts
        except Exception as x:  # noqa: BLE001 -- the round's own error is the one to raise
            print(f"cleanup: {name}: {x}", file=sys.stderr)
    ok = all(x["ok"] for x in results)
    print(json.dumps({"engine": a.engine, "ok": ok, "producer": name, "log": log,
                      "results": results}, indent=1))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
