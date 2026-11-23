#!/usr/bin/env python3
# fill.py <engine> <schema> [fill | plan | status]: fill.sh in Python, through a driver instead of the database's CLI
# (references/python-drivers.md). Engines: postgres, mysql, sqlserver, oracle, db2, informix (or pg, my, ms, ora, ifx);
# cockroachdb (crdb) and yugabytedb (yb) use the postgres driver, mariadb (maria) the mysql one.
#   fill (default): fill every table of the schema to its target (lfc:rows), one level at a time: a level starts only
#     when every table of the level before is complete; at most --rows per statement and --sessions statements at once,
#     each on its own connection; rounds repeat until every table of the level is at its target, and it stops if a
#     round adds no rows. --mode append adds --rows rows to every table instead, once.
#   plan: print the statements, one per line (each starts with /* lfc level N */).
#   status: print table|min|max|rows|target, as fill.sh does.
# The schema is a Postgres schema (in --database), a MySQL database, a SQL Server schema (in --database), an Oracle or
# Db2 schema (user), or an Informix database. The connection: --host, --port, --user, --database (Oracle: the service,
# e.g. FREEPDB1), --password-file (or LFC_PASSWORD_FILE); Informix also --server (the DBSERVERNAME). Run it with the
# engine's driver installed, e.g.  uv run --with 'psycopg[binary]' fill.py pg tpch --host db --user postgres --password-file pg.pw
import argparse, os, sys, threading, time
from dataclasses import replace
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import planner, drivers
from engines import ENGINES



def fill(a, connect=None):
    """fill.py's work on parsed options `a` (the command line's names): connect() returns a new DB-API connection,
    one per session (default: the engine's driver, drivers.py, from a's connection options; another, e.g. a Unity
    Catalog JDBC connection's DB-API adapter, works the same). Returns the exit code; prints what fill.py prints."""
    if connect is None:
        connect = lambda: drivers.connect(ENGINES[a.engine]().name, a)  # noqa: E731
    eng = ENGINES[a.engine]()
    if not a.database and hasattr(eng, "default_db"): a.database = eng.default_db(a.schema)   # Informix: the schema is the database
    o = planner.Opts(rows=a.rows or eng.rows, mode=a.mode, grow=a.grow, skip=frozenset(filter(None, (a.skip or '').split(','))), order=a.order, data=a.data, plain_every=a.plain_every, block=a.block, lob_len=a.lob_len, seed=a.seed)
    sch = eng.schema(a.schema) if hasattr(eng, "schema") else a.schema
    con = connect(); cur = con.cursor()
    if hasattr(eng, "session"): eng.session(cur)
    tabs = eng.catalog(cur, sch)
    if not tabs: print(f"no tables in {sch}", file=sys.stderr); return 1
    stmts = planner.plan(eng, tabs, sch, o)
    for line in planner.report(tabs): print(line, file=sys.stderr)
    planned = [t for t in tabs if not t.skipped]


    def status(ts):                                    # {table: (lo, hi)}
        out = {}
        for q in eng.status(sch, ts):
            cur.execute(q)
            out.update({r[0].strip(): (r[1], r[2]) for r in cur.fetchall()})
        return out


    def show(st):
        for t in sorted(planned, key=lambda t: t.name):
            lo, hi = st.get(t.name, (None, None))
            print(f"{t.name}|{'' if lo is None else int(lo)}|{'' if hi is None else int(hi)}|{0 if lo is None else int(hi) - int(lo) + 1}|{t.target or ''}")


    if a.mode == "append" and a.grow:              # an append by scale factor runs as fill rounds to the rows at the start
        cnt = status(planned)                      # plus growth, so --rows still caps a statement (a second run grows again)
        now = lambda n: 0 if cnt.get(n, (None,))[0] is None else int(cnt[n][1]) - int(cnt[n][0]) + 1
        targets = {t.name: now(t.name) + t.grow_rows for t in planned}
        o = replace(o, mode="fill", grow=None, targets=targets, skip=o.skip | {t.name for t in tabs if t.skipped})
        a.mode = "fill"
        tabs = eng.catalog(cur, sch)
        stmts = planner.plan(eng, tabs, sch, o)
        planned = [t for t in tabs if not t.skipped]
    if a.action == "plan":
        for _, _, s in stmts: print(s + ";")
        return 0
    if a.action == "status":
        show(status(planned)); return 0

    eng.prepare(cur, o)
    errors = []
    def run(batch):                                    # the statements, --sessions at a time, each session its own connection
        q, lock = list(batch), threading.Lock()
        def worker():
            c = connect(); k = c.cursor()
            if hasattr(eng, "session"): eng.session(k)
            while True:
                with lock:
                    if not q: break
                    name, s = q.pop(0)
                t0 = time.time()
                try:
                    for part in eng.split(s) if hasattr(eng, "split") else [s]: k.execute(part)
                    print(f"  {name}: {time.time() - t0:.1f} s", flush=True)
                except Exception as x:
                    errors.append(name); print(f"  {name}: {type(x).__name__}: {' '.join(str(x).split())[:400]}", file=sys.stderr, flush=True)
            c.close()
        ts = [threading.Thread(target=worker) for _ in range(min(a.sessions, len(q)))]
        for t in ts: t.start()
        for t in ts: t.join()


    T0 = time.time()
    for lv in sorted({l for l, _, _ in stmts}):
        batch = [(n, s) for l, n, s in stmts if l == lv]
        names = {n for n, _ in batch}
        ts = [t for t in planned if t.name in names]
        for rnd in range(10 ** 6):
            st = status(ts)
            rows = lambda n: 0 if st.get(n, (None,))[0] is None else int(st[n][1]) - int(st[n][0]) + 1
            todo = [(n, s) for n, s in batch if a.mode == "append" or rows(n) < next(t.target for t in ts if t.name == n)]
            if not todo or (a.mode == "append" and rnd > 0): break
            print(f"level {lv}, round {rnd + 1}: {len(todo)} tables ({time.time() - T0:.0f} s)", flush=True)
            run(todo)
            if status(ts) == st:                       # a round that adds nothing would repeat forever: stop and show why
                print(f"level {lv}: no rows added; fix the failing statements above, then rerun", file=sys.stderr); return 1
    show(status(planned))
    print(f"done in {time.time() - T0:.0f} s" + (f", {len(errors)} statement errors" if errors else ""))
    return 1 if errors else 0


def append_many(engine, schemas, rows, connect, skip=(), per_trip=50, done=None):
    """Add `rows` rows to every table but `skip` of each schema in `schemas`, which hold the same tables (one model's
    copies, e.g. a module's LOBs), in a few round trips: one catalog read (the first schema's), each schema planned from
    it (an append's INSERT finds its own start in SQL), then per level one execute per `per_trip` schemas, their INSERTs
    together (on PostgreSQL one transaction). fill() per schema repeats the catalog and the status reads, a round trip
    each: through a UC JDBC connection about 0.7 s apiece, most of a batch's time. done(schemas) runs after each part's
    last level. An engine that splits its statements (eng.split) runs them one by one."""
    eng = ENGINES[engine]()
    o = planner.Opts(rows=rows, mode="append", skip=frozenset(skip))
    sch = (lambda s: eng.schema(s)) if hasattr(eng, "schema") else (lambda s: s)
    con = connect(); cur = con.cursor()
    if hasattr(eng, "session"): eng.session(cur)
    tabs = eng.catalog(cur, sch(schemas[0]))
    if not tabs: raise ValueError(f"no tables in {schemas[0]}")
    plans = {x: planner.plan(eng, tabs, sch(x), o) for x in schemas}
    eng.prepare(cur, o)
    levels = sorted({lv for p in plans.values() for lv, _, _ in p})
    for k in range(0, len(schemas), per_trip):
        part = schemas[k:k + per_trip]
        for lv in levels:
            stmts = [s for x in part for l, _, s in plans[x] if l == lv]
            if hasattr(eng, "split"):
                for s in stmts:
                    for piece in eng.split(s): cur.execute(piece)
            elif stmts:
                cur.execute(f"/* lfc append, level {lv}, {len(part)} schemas */\n" + ";\n".join(stmts))
        if done: done(part)
    con.close()


def options(argv=None):
    """The command line's options (also for a caller that builds them: options([engine, schema, "--sessions", "2"]))."""
    ap = argparse.ArgumentParser(description="fill a schema's tables to their lfc:rows targets, through a driver")
    ap.add_argument("engine"); ap.add_argument("schema"); ap.add_argument("action", nargs="?", default="fill", choices=["fill", "plan", "status"])
    ap.add_argument("--host", default="127.0.0.1"); ap.add_argument("--port", type=int); ap.add_argument("--user"); ap.add_argument("--database")
    ap.add_argument("--password-file"); ap.add_argument("--server", help="Informix DBSERVERNAME (default informix)")
    ap.add_argument("--trust-server-certificate", action="store_true", help="SQL Server: accept a self-signed certificate")
    ap.add_argument("--grow", type=float, help="with --mode append: add SF scale factors of rows, each table its lfc:sf_rows x SF");
    ap.add_argument("--skip", help="tables to leave out, comma-separated");
    ap.add_argument("--rows", type=int, help="rows per statement (default: the engine's)"); ap.add_argument("--sessions", type=int, default=4)
    ap.add_argument("--mode", default="fill", choices=["fill", "append"]); ap.add_argument("--order", default="random", choices=["random", "asc", "desc"])
    ap.add_argument("--data", default="scramble", choices=["seq", "scramble", "random"]); ap.add_argument("--plain-every", type=int, default=4)
    ap.add_argument("--block", type=int, default=1000); ap.add_argument("--lob-len", type=int, default=100); ap.add_argument("--seed", type=int, default=0)
    return ap.parse_intermixed_args(argv)                     # the action can come after the options too


if __name__ == "__main__":
    sys.exit(fill(options()))
