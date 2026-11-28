#!/usr/bin/env python3
# fill.py <engine> <schema> [fill | plan | status] [options]: fill every table of a schema to its target (lfc:rows), one
# level at a time: a level starts only when every table of the level before is complete (formulas that point at a parent,
# lfc:v). At most --rows per statement and --sessions statements at once; rounds repeat until every table of the level is
# at its target, and it stops if a round adds no rows (a statement keeps failing). Rerunning resumes.
#   fill (default): the rows as above. --mode append adds --rows rows to every table instead, once.
#   plan: print the statements, one per line (each starts with /* lfc level N */).
#   status: print table|min|max|rows|target.
# Two ways to run the statements, one loop:
#   driver (default): the Python planner (planner.py, engines/<engine>.py) through a pip-installable DB-API driver
#     (drivers.py, references/python-drivers.md); the connection options: --host, --port, --user, --database,
#     --password-file (or LFC_PASSWORD_FILE). Every engine, including Db2 and Informix.
#   client: the engine's own SQL client (a subprocess: psql, mysql, sqlcmd, sqlplus) runs the SQL planner files of the
#     engine's folder (postgres/, mysql/, sqlserver/, oracle/: cast.sql, plan-insert.sql, plan-status.sql), for a
#     database reached through its client only and a host with no Python driver. Chosen by --client CMD, or by the
#     environment variable the shell version read (PSQL, MYSQL, MSSQL, ORACLE); the planner files are in --dir (env DIR;
#     default: the engine's folder next to this one). Engines: pg, my, ms, ora. The shell's environment still sets the
#     defaults: ROWS, SESSIONS, ORDER, DATA, PLAIN_EVERY, BLOCK, LOB_LEN, and SRC (pg: seed, or series for a database with
#     no lfc_seed.seed, e.g. YugabyteDB).
# The schema is a Postgres schema (in --database), a MySQL database, a SQL Server schema (in --database), an Oracle or
# Db2 schema (user), or an Informix database. Run it with the engine's driver installed, e.g.
#   uv run --with 'psycopg[binary]' fill.py pg tpch --host db --user postgres --password-file pg.pw
# Engine names: pg (also postgres or yugabytedb or yb), my (also mysql, mariadb or maria), ms (SQL Server), ora (Oracle),
# db2, informix (or ifx); cockroachdb (crdb) and yugabytedb (yb) use the postgres driver, mariadb (maria) the mysql one.
# CockroachDB's planner functions live in pg_temp, which it refuses: fill it through the driver (fill.py postgres).

from __future__ import annotations

import argparse
import os
import re
import shlex
import subprocess
import sys
import threading
import time
from dataclasses import replace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import drivers
import planner
from engines import ENGINES


def _rounds(label, batch, status, run, append):
    """One level's rounds: batch is [(table, statement)]; status(names) -> {table: (rows, target)} (target None: no
    target). A round runs the statements of the tables below target (every one, in an append), then reads the status
    again; a round that adds nothing stops it, and the level fails (False). Rounds repeat until every table is at target."""
    names = {n for n, _ in batch}
    for rnd in range(10 ** 6):
        st = status(names)
        todo = [(n, s) for n, s in batch if append or (st.get(n, (0, None))[1] is not None and st[n][0] < st[n][1])]
        if not todo or (append and rnd > 0):
            return True
        print(f"level {label}, round {rnd + 1}: {len(todo)} tables ({time.time() - T0:.0f} s)", flush=True)
        run(todo)
        if status(names) == st:                    # a round that adds nothing would repeat forever: stop and show why
            print(f"level {label}: no rows added; fix the failing statements above, then rerun", file=sys.stderr)
            return False
    return True


T0 = time.time()


def fill(a, connect=None):
    """fill.py's work on parsed options `a` (the command line's names): connect() returns a new DB-API connection,
    one per session (default: the engine's driver, drivers.py, from a's connection options; another, e.g. a Unity
    Catalog JDBC connection's DB-API adapter, works the same). Returns the exit code; prints what fill.py prints.
    A client (a's --client, or the environment's PSQL, MYSQL, MSSQL, ORACLE) runs the planner files instead."""
    client = client_for(a)
    if client is not None and connect is None:
        return fill_client(a, client)
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


    global T0
    T0 = time.time()
    for lv in sorted({l for l, _, _ in stmts}):
        batch = [(n, s) for l, n, s in stmts if l == lv]
        names = {n for n, _ in batch}
        ts = [t for t in planned if t.name in names]
        def level_status(ns, ts=ts):                   # {table: (rows, target)} for the level's tables
            st = status([t for t in ts if t.name in ns])
            return {t.name: (0 if st.get(t.name, (None,))[0] is None else int(st[t.name][1]) - int(st[t.name][0]) + 1, t.target)
                    for t in ts if t.name in ns}
        if not _rounds(lv, batch, level_status, run, a.mode == "append"): return 1
    show(status(planned))
    print(f"done in {time.time() - T0:.0f} s" + (f", {len(errors)} statement errors" if errors else ""))
    return 1 if errors else 0


# ---- client: the engine's SQL client runs the planner files -------------------------------------------

ENGINE_KEY = {"postgres": "pg", "cockroachdb": "pg", "crdb": "pg", "yugabytedb": "pg", "yb": "pg", "pg": "pg",
              "mysql": "my", "mariadb": "my", "maria": "my", "my": "my",
              "sqlserver": "ms", "mssql": "ms", "ms": "ms", "oracle": "ora", "ora": "ora"}
CLIENT = {   # key: (the environment variable that names the client, its default, the flags every run gets, the planner folder,
             # rows per statement); the variable replaces the client's command, the flags follow it (the client's command, as the shell fill had it)
    "pg": ("PSQL", "psql", ["-X", "-At", "-q", "-v", "ON_ERROR_STOP=1"], "postgres", 1000000),
    "my": ("MYSQL", "mysql", ["-N", "-B", "-r"], "mysql", 100000),
    "ms": ("MSSQL", "sqlcmd -y 0", [], "sqlserver", 1000000),
    "ora": ("ORACLE", "sqlplus -s /nolog", [], "oracle", 1000000),
}
MS_NOISE = (re.compile(r"^ *-* *$"), re.compile(r"^\([0-9]+ rows? affected\)$"))
NAME_RE = re.compile(r"\bINTO\s+[^\s.]+\.[\"\[`]?([A-Za-z_][A-Za-z0-9_$]*)")


def client_for(a):
    """The client key (pg, my, ms, ora) when this run uses a client, else None (the driver)."""
    key = ENGINE_KEY.get(a.engine)
    if a.client is None and not (key and os.environ.get(CLIENT[key][0])):
        return None
    if key is None:
        raise SystemExit(f"fill.py: a client runs pg, my, ms and ora only; {a.engine} fills through its driver")
    return key


def _sql(argv, text):
    """Run a client with text on stdin; (exit code, output with its errors)."""
    p = subprocess.run(argv, input=text, capture_output=True, text=True, check=False)
    return p.returncode, p.stdout + p.stderr


def _statements(plan_text):
    """The statements of a plan: (level, table, statement), one per '/* lfc level N */' line, continuation lines kept."""
    out, cur = [], None
    for line in plan_text.splitlines():
        m = re.match(r"/\* lfc level ([0-9]+) \*/", line)
        if m:
            if cur: out.append(cur)
            cur = [int(m.group(1)), line]
        elif cur is not None:
            cur[1] += "\n" + line
    if cur: out.append(cur)
    res = []
    for lv, s in out:
        n = NAME_RE.search(s)
        res.append((lv, n.group(1) if n else "", s))
    return res


def fill_client(a, key):
    """The planner files through the client: the same rounds, levels and no-progress stop as the driver's."""
    env_name, default, flags, folder, rows = CLIENT[key]
    d = a.schema.upper() if key == "ora" else a.schema
    if a.mode == "append": raise SystemExit("fill.py: --mode append is for the driver (the client fills to the targets)")
    client = shlex.split(a.client or os.environ.get(env_name) or default) + flags + ([d] if key == "my" else [])
    rows = a.rows or int(os.environ.get("ROWS", rows))
    folder = a.dir or os.environ.get("DIR") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", folder)
    src = a.src or os.environ.get("SRC", "seed")
    def read(f):
        with open(os.path.join(folder, f)) as fh:
            return fh.read()
    cast = read("cast.sql")
    if key == "my":
        _, ver = _sql(client, "SELECT VERSION();\n")
        if "mariadb" in ver.lower(): cast = re.sub(r"c\.srs_id", "0", cast)   # MariaDB has no information_schema srs_id
    head = {"pg": "", "my": "", "ms": "", "ora": ""}
    if key == "my":
        pre = f"SET @db='{d}', @rows={rows}, @src='seed', @mode='fill', @order='{a.order}', @data='{a.data}', @plain_every={a.plain_every}, @block={a.block}, @lob_len={a.lob_len};\n"
    elif key == "ms":
        pre = (f"DECLARE @schema sysname = N'{d}', @rows bigint = {rows}, @mode varchar(10) = 'fill', @order varchar(10) = '{a.order}', "
               f"@data varchar(10) = '{a.data}', @plain_every int = {a.plain_every}, @block int = {a.block}, @lob_len int = {a.lob_len};\n")
    elif key == "ora":
        pre = ("SET FEEDBACK OFF HEADING OFF PAGESIZE 0 LINESIZE 32767 TRIMOUT ON TRIMSPOOL ON VERIFY OFF SERVEROUTPUT ON SIZE UNLIMITED FORMAT WRAPPED\n"
               f"DEFINE schema = '{d}'\nDEFINE rows = {rows}\nDEFINE mode = fill\nDEFINE order = {a.order}\nDEFINE data = {a.data}\n"
               f"DEFINE plain_every = {a.plain_every}\nDEFINE block = {a.block}\nDEFINE lob_len = {a.lob_len}\n")
        head["ora"] = "SET AUTOCOMMIT ON FEEDBACK OFF HEADING OFF PAGESIZE 0 LINESIZE 32767 TRIMOUT ON TRIMSPOOL ON SQLBLANKLINES ON DEFINE OFF"
    else:
        pre = ""
    go = "GO" if key == "ms" else ""
    if key == "pg":
        pgvars = ["-v", f"schema={d}", "-v", f"rows={rows}", "-v", f"src={src}", "-v", "mode=fill", "-v", f"order={a.order}",
                  "-v", f"data={a.data}", "-v", f"plain_every={a.plain_every}", "-v", f"block={a.block}", "-v", f"lob_len={a.lob_len}"]
        plan_argv, plan_in = client + pgvars, cast + read("plan-insert.sql")
    else:
        plan_argv, plan_in = client, pre + cast + read("plan-insert.sql")

    def filtered(text):                            # sqlcmd prints a blank line and dashes before each result: not rows
        return "\n".join(l for l in text.splitlines() if not any(p.match(l) for p in MS_NOISE)) + "\n" if key == "ms" else text

    def plan_text():
        return filtered(_sql(plan_argv, plan_in)[1])

    def status_text():                             # two client runs: the status SQL, then the SQL it writes
        if key == "pg":
            rc, sql = _sql(client + ["-v", f"schema={d}"], read("plan-status.sql"))
            rc2, out = _sql(client, sql)
            rc = rc or rc2
        elif key == "my":
            rc, sql = _sql(client, f"SET @db='{d}';\n" + read("plan-status.sql"))   # (the status SQL, then its client)
            rc2, out = _sql(client, sql)
            rc, out = rc or rc2, out.replace("\t", "|")
        elif key == "ms":
            rc, sql = _sql(client, pre + cast + read("plan-status.sql"))
            rc2, out = _sql(client, filtered(sql))
            rc, out = rc or rc2, filtered(out)
        else:
            rc, sql = _sql(client, pre + cast + read("plan-status.sql"))
            rc2, out = _sql(client, head["ora"] + "\n" + sql)
            rc = rc or rc2
            out = "\n".join(l for l in out.splitlines() if "|" in l) + "\n"
        return rc, out

    def checked():                                 # a status query that fails (or prints an error) stops the fill
        rc, out = status_text()
        if rc != 0 or "ERROR" in out or "Traceback" in out:
            print(out, file=sys.stderr); raise SystemExit(1)
        return [l.split("|") for l in out.splitlines() if l.count("|") >= 4]

    if a.action == "plan":
        sys.stdout.write(plan_text()); return 0
    if a.action == "status":
        for f in checked(): print("|".join(f))
        return 0
    text = plan_text()
    recs = _statements(text)
    levels = sorted({lv for lv, _, _ in recs})
    if not levels:
        print("the planner wrote no statements:", file=sys.stderr); print("\n".join(text.splitlines()[:20]), file=sys.stderr)
        return 1

    def run(todo):                                 # round robin into --sessions part files, each run by its own client
        names = {n for n, _ in todo}
        stmts = [s for _, n, s in todo_recs if n in names]
        parts = [[] for _ in range(max(1, min(a.sessions, len(stmts))))]
        for k, s in enumerate(stmts):
            parts[k % len(parts)].append(s + (f"\n{go}" if go else ""))
        procs = []
        for p in parts:
            if not p: continue
            body = (head[key] + "\n" if head[key] else "") + "\n".join(p) + "\n"
            procs.append(subprocess.Popen(client, stdin=subprocess.PIPE, text=True))
            procs[-1].stdin.write(body); procs[-1].stdin.close()
        for pr in procs: pr.wait()

    global T0
    T0 = time.time()
    for lv in levels:
        todo_recs = [(l, n, s) for l, n, s in recs if l == lv]
        def level_status(ns):                      # {table: (rows, target)} from the status lines
            out = {}
            for f in checked():
                if f[0] in ns:
                    out[f[0]] = (int(f[3] or 0), int(f[4]) if len(f) > 4 and f[4].strip() else None)
            return out
        batch = [(n, s) for _, n, s in todo_recs]
        if not _rounds(lv, batch, level_status, run, False): return 1
    for f in checked(): print("|".join(f))
    return 0


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




def _env(name, default, cast=str):
    """The default from the environment variable the shell fill read (SESSIONS, ORDER, DATA, PLAIN_EVERY, BLOCK, LOB_LEN)."""
    v = os.environ.get(name)
    return default if v in (None, "") else cast(v)


def options(argv=None):
    """The command line's options (also for a caller that builds them: options([engine, schema, "--sessions", "2"]))."""
    ap = argparse.ArgumentParser(description="fill a schema's tables to their lfc:rows targets, through a driver or a client")
    ap.add_argument("engine"); ap.add_argument("schema"); ap.add_argument("action", nargs="?", default="fill", choices=["fill", "plan", "status"])
    ap.add_argument("--host", default="127.0.0.1"); ap.add_argument("--port", type=int); ap.add_argument("--user"); ap.add_argument("--database")
    ap.add_argument("--password-file"); ap.add_argument("--server", help="Informix DBSERVERNAME (default informix)")
    ap.add_argument("--trust-server-certificate", action="store_true", help="SQL Server: accept a self-signed certificate")
    ap.add_argument("--client", help="run the planner files through this SQL client (reads SQL on stdin), e.g. \"psql -h db -U lfc\"; "
                    "default: the environment's PSQL, MYSQL, MSSQL or ORACLE when one is set, else the driver")
    ap.add_argument("--dir", help="the planner files' folder (client mode; default: the engine's folder here, or env DIR)")
    ap.add_argument("--src", help="client mode, pg: seed (the seed table) or series (generate_series); default env SRC or seed")
    ap.add_argument("--grow", type=float, help="with --mode append: add SF scale factors of rows, each table its lfc:sf_rows x SF");
    ap.add_argument("--skip", help="tables to leave out, comma-separated");
    ap.add_argument("--rows", type=int, help="rows per statement (default: the engine's; client mode: env ROWS or the engine's)")
    ap.add_argument("--sessions", type=int, default=_env("SESSIONS", 4, int))
    ap.add_argument("--mode", default="fill", choices=["fill", "append"]); ap.add_argument("--order", default=_env("ORDER", "random"), choices=["random", "asc", "desc"])
    ap.add_argument("--data", default=_env("DATA", "scramble"), choices=["seq", "scramble", "random"]); ap.add_argument("--plain-every", type=int, default=_env("PLAIN_EVERY", 4, int))
    ap.add_argument("--block", type=int, default=_env("BLOCK", 1000, int)); ap.add_argument("--lob-len", type=int, default=_env("LOB_LEN", 100, int))
    ap.add_argument("--seed", type=int, default=0)
    return ap.parse_intermixed_args(argv)                     # the action can come after the options too


def main(argv=None):
    return fill(options(argv))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
