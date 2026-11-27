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
for dep in ("lfc-rest", "lfc-change-source", "lfc-source-contract"):   # a checkout: the sibling skills' packages, when not installed
    sys.path.insert(0, os.path.join(SKILLS, dep, "scripts"))

from lfc_change_source.engine import Warehouse
from lfc_change_source.producer import Spec, WorkspaceDebeziumServer
from lfc_rest import Rest

from zerobus_debezium import destination, engines, snapshot
from zerobus_debezium.source import Slot


def landed(wh, log: str, prefix: str, key: str, seq: str, ids: range | None = None) -> list:
    """(operation, id) of this producer's change rows in cdc_log, in the source's order; ids: only those."""
    rows = wh.sql(f"SELECT operation, CAST(key:{key} AS BIGINT) AS id FROM {log} "
                  f"WHERE startswith(destination, '{prefix}.') ORDER BY {seq}, value:ts_ns::BIGINT").collect()
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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--profile", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--volume", default="debezium")
    ap.add_argument("--engine", required=True, choices=sorted(engines.ENGINES))
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
        if a.snapshot == "auto":   # Debezium streams and round 1 has landed: the snapshot's turn (qbc, else jdbc)
            snap = snapshot.take(api, e, conn, f"{a.connection_prefix}-{a.engine}-{a.slot_name}", a.catalog,
                                 f"{a.schema}_snap_{tag}", pipeline=f"{name}-snapshot", wh=wh,
                                 notebook_path=f"{os.path.dirname(a.notebook)}/jdbc_snapshot",
                                 package=a.change_source_package)
            have = snapshot.rows(wh, snap["table"], ids) if snap["state"] == "COMPLETED" else {}
            claim("snapshot", snap["state"] == "COMPLETED" and have == e.state_after(1, a.base),
                  f"{snap['how']} {snap['state']} in {snap['seconds']} s into {snap['table']}: {have} "
                  f"({snap['link']}){' ' + str(snap['error'])[:300] if snap.get('error') else ''}")

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
