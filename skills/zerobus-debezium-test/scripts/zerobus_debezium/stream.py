"""stream: a source's existing tables through FlowThrough by Debezium Server and Zerobus, the way a customer's would go:
change capture on the tables (optional), Debezium Server in the workspace streaming them into the log, a snapshot of
each once its first change has landed (routing's method, through a UC connection that's there already), then
FlowThrough silver rounds over the snapshot and the log. The source keeps changing, so each silver table is checked
against lfc-log-silver's combine.current_sql at the round's position (the snapshot and the log up to it), as the
gateway's live tests do, not against the source.

    python -m zerobus_debezium.stream --profile <p> --catalog <c> --schema <s> --engine mysql \\
        --slot-host <vm> --slot-name mysql --database lfcddemo --source-schema lfcddemo --tables intpk,dtix \\
        --keys intpk=pk --connection <uc connection> --notebook /Users/<me>/debezium-server-zerobus \\
        --package <zerobus_debezium wheel> --zb-client-id <application id> --scope <scope> [--capture] [--restart-ok]

Writes, in <catalog>: <schema>.cdc_log (the shared log), <schema>_snap_dbz_<name>, <schema>_silver_dbz_<name> (dropped
and made again each run: a run's silver starts from its own snapshot); the
producer's folder in <schema>.<volume>. Prints one line a claim and a JSON summary.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
SKILLS = os.path.join(HERE, "..", "..", "..")
for dep in ("lfc-rest", "lfc-change-source", "lfc-source-contract", "lfc-routing", "lfc-log-silver"):
    sys.path.insert(0, os.path.join(SKILLS, dep, "scripts"))

from lfc_change_source import SNAPSHOTS, debezium
from lfc_change_source.cdc_log import SEQUENCES, CdcLog
from lfc_change_source.contract import Position
from lfc_change_source.engine import Warehouse
from lfc_change_source.producer import Spec, WorkspaceDebeziumServer
from lfc_log_silver import combine, flowthrough
from lfc_rest import Rest
from lfc_routing.routing import Policy, snapshot_method
from lfc_source_contract import jdbc

from zerobus_debezium import destination, engines
from zerobus_debezium.source import Slot


def destinations(wh, log: str, prefix: str, tables: list) -> dict:
    """{table: its destination in the log}, matched on the destination's last part (case folded)."""
    rows = [r[0] for r in wh.sql(f"SELECT DISTINCT destination FROM {log} WHERE startswith(destination, '{prefix}.')")
            .collect()]
    return {t: d for t in tables for d in rows if d.rsplit(".", 1)[-1].lower() == t.lower()}


def columns_of(wh, table: str) -> dict:
    """{column: Databricks type} of a snapshot table, in its order."""
    return {r[0]: r[1].upper() for r in wh.sql(f"DESCRIBE TABLE {table}").collect()
            if r[0] and not r[0].startswith("#") and not r[0].startswith("_")}


def take_snapshots(api, wh, e, a, tables: list, keys: dict, snap_schema: str, name: str) -> dict:
    """Each table's snapshot by routing's method for the engine, through the existing connection a.connection:
    {table: snapshot table}. qbc: one pipeline for all of them; jdbc: one job."""
    how = snapshot_method(tables[0], 0, Policy(shared_log="", engine=e.name))
    wh.sql(f"CREATE SCHEMA IF NOT EXISTS {destination.q(a.catalog, snap_schema)}")
    if how == "qbc":
        q = SNAPSHOTS["qbc"]
        names = {t: q.source_names(e.name, a.database or a.service, a.source_schema, t) for t in tables}
        n0 = next(iter(names.values()))
        got = q.snapshot(api, f"{name}-snapshot", a.connection, a.catalog, snap_schema, n0["catalog"], n0["schema"],
                         {names[t]["table"]: keys.get(t, []) for t in tables})
        if got["state"] != "COMPLETED":
            raise RuntimeError(f"qbc snapshot {got['state']}: {api.host}/pipelines/{got['pipeline_id']}")
        return {t: q.snapshot_table(a.catalog, snap_schema, names[t]["table"]) for t in tables}
    j = SNAPSHOTS["jdbc"]
    src = [jdbc.table_name(e.name, a.source_schema, t) for t in tables]
    got = j.snapshot(api, a.connection, src, a.catalog, snap_schema, f"{os.path.dirname(a.notebook)}/jdbc_snapshot",
                     a.change_source_package)
    if got["state"] != "COMPLETED":
        raise RuntimeError(f"jdbc snapshot {got['state']}: {got['url']} {got.get('error') or ''}")
    return {t: j.snapshot_table(a.catalog, snap_schema, s) for t, s in zip(tables, src)}


def sources_for(wh, e, log: str, prefix: str, dests: dict, snaps: dict, keys: dict) -> dict:
    """FlowThrough's CDC layer for each table: a CdcLog typed like its snapshot (names as the events carry them; a
    temporal column decoded by its kind, Debezium's unit taken from 6 digits of fraction, every lfcddemo engine's)."""
    out = {}
    for t, d in dests.items():
        cols = columns_of(wh, snaps[t])
        kinds = {c: debezium.kind_of(ty) for c, ty in cols.items() if debezium.kind_of(ty)}
        out[t] = CdcLog(prefix, log, t, d, keys=keys.get(t, []), columns=cols, seq=e.seq, kinds=kinds,
                        precision={c: 6 for c in kinds})
    return out


def positions(wh, log: str, e, dests: dict) -> dict:
    """{table: its newest event's position in the log}, read just before the snapshot: the snapshot's position. Changes
    after it are applied on the snapshot; those before it are in it already. A keyed table's later changes carry whole
    rows, so one landing during the snapshot's read reapplies harmlessly; a keyless table's insert in that window counts
    twice (a short window: the snapshot runs right after)."""
    out = {}
    for t, d in dests.items():
        v = wh.sql(f"SELECT max({SEQUENCES[e.seq]}) FROM {log} WHERE destination = '{d}'").collect()[0][0]
        out[t] = Position(e.seq, str(v)) if v is not None else None
    return out


def check(wh, silver_schema: str, srcs: dict, snaps: dict, to, at: dict) -> dict:
    """{table: (extra, missing)}: silver against the snapshot at its position and the log up to `to`
    (combine.current_sql)."""
    out = {}
    for t, s in srcs.items():
        want = combine.current_sql(s, f"SELECT * FROM {snaps[t]}", at.get(t), to)
        cols = ", ".join(f"`{c}`" for c in s.columns)
        have = f"{silver_schema}.`{t}`"
        out[t] = tuple(int(wh.sql(f"SELECT count(*) FROM ((SELECT {cols} FROM {x}) EXCEPT ALL (SELECT {cols} FROM {y}))")
                           .collect()[0][0]) for x, y in ((have, f"({want})"), (f"({want})", have)))
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    for k in ("profile", "catalog", "schema", "engine", "slot-host", "slot-name", "source-schema", "tables",
              "connection", "notebook", "package", "zb-client-id", "scope"):
        ap.add_argument(f"--{k}", required=True)
    ap.add_argument("--database", default="", help="SQL Server, MySQL, PostgreSQL, Db2: the tables' database")
    ap.add_argument("--service", default="", help="Oracle: the PDB (the connector's pdb.name, QBC's catalog)")
    ap.add_argument("--keys", default="", help="table=col1+col2,...: each keyed table's key; others are keyless")
    ap.add_argument("--volume", default="debezium")
    ap.add_argument("--publication", default="", help="PostgreSQL: the publication with the tables")
    ap.add_argument("--slot-key", default="~/.ssh/lfc_slot_ed25519")
    ap.add_argument("--admin-key", help="the VM admin login, for an engine with shell setup steps (Db2)")
    ap.add_argument("--admin-user", default="azureuser")
    ap.add_argument("--owner", help="your owner name on the VM (the container's name), for shell steps")
    ap.add_argument("--capture", action="store_true", help="set up change capture on the tables first")
    ap.add_argument("--restart-ok", action="store_true", help="the capture may restart the database (Db2's ASN)")
    ap.add_argument("--change-source-package", help="the jdbc snapshot's lfc_change_source wheel or archive")
    ap.add_argument("--host", help="with --user: the database's address (else the slot's up gives the login)")
    ap.add_argument("--port", type=int)
    ap.add_argument("--user", help="Debezium's login, instead of the slot's lfc_user (Oracle: c##dbzuser)")
    ap.add_argument("--password-file", help="--user's password: a file (0600)")
    ap.add_argument("--password-container-env", help="--user's password: this variable in the database's container, "
                                                      "read through the VM admin login (Informix's informix)")
    ap.add_argument("--rounds", type=int, default=2)
    ap.add_argument("--round-wait", type=int, default=90, help="seconds between silver rounds")
    ap.add_argument("--keep-running", action="store_true", help="leave Debezium streaming at the end")
    a = ap.parse_args(argv)

    e = engines.get(a.engine)
    if snapshot_method("t", 0, Policy(shared_log="", engine=e.name)) == "jdbc" and not a.change_source_package:
        raise SystemExit(f"{e.name}: its snapshot is the jdbc route, which needs --change-source-package (checked "
                         "before Debezium starts)")
    tables = [t.strip() for t in a.tables.split(",") if t.strip()]
    keys = {k: [c for c in v.split("+") if c] for k, v in (x.split("=", 1) for x in a.keys.split(",") if x)}
    api = Rest.connect(a.profile)
    wh = Warehouse.pick(api)
    slot = Slot(a.slot_host, a.slot_name, key=a.slot_key, owner=a.owner, admin_user=a.admin_user)
    if a.user:   # a login of the test's own: no slot up (on a long-running demo database it re-applies lfc_user)
        if a.password_file:
            with open(os.path.expanduser(a.password_file)) as f:
                pw = f.read().strip()
        elif a.password_container_env:
            pw = slot.container(f"printenv {a.password_container_env}", a.admin_key, a.owner).strip()
        else:
            raise SystemExit("--user needs --password-file or --password-container-env")
        conn = {"host": a.host, "port": a.port, "user": a.user, "password": pw}
    else:
        info = slot.up(a.engine)
        conn = {"host": info["host"], "port": info["port"], "user": info["user"], "password": info.pop("password")}
    for k, v in (("database", a.database), ("service", a.service), ("owner", a.source_schema),
                 ("publication", a.publication)):
        if v:
            conn[k] = v
    login = dict(conn, user="c##dbzuser") if a.engine == "oracle" and not a.user else conn   # Oracle: LogMiner's user
    name = f"dbz-{a.engine}-{a.slot_name}"
    prefix, tag = name.replace("-", "_"), a.slot_name.replace("-", "_")
    results = []

    def claim(cid, ok, detail):
        results.append({"claim": cid, "ok": bool(ok), "detail": detail})
        print(f"{'PASS' if ok else 'FAIL'} {a.engine} {cid}: {detail}", flush=True)

    if a.capture:
        slot.setup(e.capture(conn, a.source_schema, tables, restart=a.restart_ok), admin_key=a.admin_key, owner=a.owner)
        claim("capture", True, f"change capture on {a.source_schema}.{','.join(tables)}")
    pw_key = f"{a.slot_name}-dbz-password"
    destination.put_secret(api, a.scope, pw_key, login["password"])
    log = destination.change_log(wh, a.catalog, a.schema, principal=a.zb_client_id)
    params = {"engine": a.engine, "package": a.package, "catalog": a.catalog, "schema": a.schema, "volume": a.volume,
              "db_host": login["host"], "db_port": str(login["port"]), "db_user": login["user"],
              "db_name": a.service or a.database, "db_schema": a.source_schema if a.engine == "oracle" else "",
              "tables": e.include(a.source_schema, tables, a.database), "secret_scope": a.scope, "secret_key": pw_key,
              "zb_client_id": a.zb_client_id, "zb_secret_key": "zb-client-secret",
              "slot_name": f"dbz_{tag}", "publication_name": a.publication or "dbz_publication",
              "idle_timeout_sec": "", "max_runtime_sec": "7200"}
    folder = f"/Volumes/{a.catalog}/{a.schema}/{a.volume}/{name}"
    p = WorkspaceDebeziumServer(Spec(name, log), api, a.notebook, folder, params)
    snap_schema, silver_schema = f"{a.schema}_snap_dbz_{tag}", destination.q(a.catalog, f"{a.schema}_silver_dbz_{tag}")
    try:
        t0 = time.time()
        p.start()
        claim("streaming", True, f"Debezium Server streaming {int(time.time() - t0)} s after submit ({p.run()['url']})")
        end, dests = time.time() + 600, {}
        while time.time() < end:   # the source's own changes: each table's first event in the log
            dests = destinations(wh, log, prefix, tables)
            if len(dests) == len(tables):
                break
            time.sleep(20)
        claim("changes-landed", len(dests) == len(tables), f"destinations {dests}")
        if len(dests) != len(tables):
            return 1
        at = positions(wh, log, e, dests)   # the snapshot's position: each table's newest event, just before it
        snaps = take_snapshots(api, wh, e, a, tables, keys, snap_schema, name)
        claim("snapshot", True, f"{snaps}")
        srcs = sources_for(wh, e, log, prefix, dests, snaps, keys)
        # a new snapshot, so a new silver: the test's own schema is dropped first (a rerun would otherwise continue
        # the last run's _flowthrough state and tables, built on the old snapshot)
        wh.sql(f"DROP SCHEMA IF EXISTS {silver_schema} CASCADE")
        wh.sql(f"CREATE SCHEMA {silver_schema}")
        for i in range(a.rounds):
            if i:
                time.sleep(a.round_wait)   # the source's changes go on; the next round picks them up
            # the newest of the tables' positions: numbers by length then digits (text would put 9 after 10); the TEXT
            # kinds are fixed-width, so the same key orders them too
            to = max((s.watermark(wh) for s in srcs.values()), key=lambda x: (len(x.value), x.value))
            got = flowthrough.step(wh, silver_schema, srcs, to, Policy(shared_log=log, engine=a.engine),
                                   extra={t: {"snapshot_table": snaps[t], "position": at.get(t)} for t in srcs})
            bad = check(wh, silver_schema, srcs, snaps, to, at)
            n = {t: int(wh.sql(f"SELECT count(*) FROM {silver_schema}.`{t}`").collect()[0][0]) for t in srcs}
            claim(f"silver-{i + 1}", all(b == (0, 0) for b in bad.values()),
                  f"routes {got['routes']} at {to}: rows {n}, (extra, missing) vs snapshot + log {bad}")
    except Exception as x:  # noqa: BLE001 -- recorded as a failed claim; the producer is stopped below
        claim("error", False, f"{type(x).__name__}: {str(x)[:500]}")
    finally:
        if not a.keep_running:
            try:
                if p.active():
                    claim("stop", p.stop() == "SUCCESS", "STOP file")
            except Exception as x:  # noqa: BLE001
                print(f"cleanup: {name}: {x}", file=sys.stderr)
    ok = all(x["ok"] for x in results)
    print(json.dumps({"engine": a.engine, "ok": ok, "producer": name, "log": log, "silver": silver_schema,
                      "results": results}, indent=1))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
