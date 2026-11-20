"""lfc_source_datagen.sources: any TPC model (tpcb, tpcc, tpch, tpcds, tpce, tpcdi) as several business sources in one
PostgreSQL database, one schema per source, each at its own scale, ready for Lakeflow Connect CDC: n lines of business
(or acquired companies) with the same tables and repeating keys, for an n:1 test (lfc-medallion).

It runs on DB-API connections made by connect(): in a notebook or job, lfc_source_contract.jdbc.dbapi (writes through
the writer's TYPE JDBC connection, reads through the regular POSTGRESQL one), so no password or driver is in the code;
on the laptop, connector() (psycopg):

    from lfc_source_contract.jdbc import dbapi
    from lfc_source_datagen.sources import build
    connect = lambda: dbapi(spark, "orders_jdbc", read_connection="orders_pg", database="orders")
    build(connect, "tpcc", {"east": 0.01, "west": 0.02}, login="lfc_cdc")   # the reader gets SELECT
    # laptop: connect = connector("orders", lakebase="my-project", profile="..."), or connector("orders", host=..., password=...)

Each source is built in copy <model>_<n> and takes its name last, so a schema with the name is a finished source and a
rerun after any failure resumes in the copy; every step is safe to repeat (a JDBC connection runs a statement again
on a task retry):
  1. the model's DDL at the source's scale, all or nothing (a guarded DO block): the spec's own for tpch and tpcb
     (tpc_schema), statschema's model shaped for the others (statschema.tools model, shape, ddl);
  2. SELECT for `login` (the reads may go through it);
  3. the fill: python/fill.py's fill(), its INSERTs sized from the key already there;
  4. the spec's checks where the model has them (tpc/<model>-verify.sql), once (a schema comment marks them);
  5. `steps`, the model's own deterministic SQL (lfc_source_datagen.tpch: TPC-H's words and lead times);
  6. the source's name;
then lfc_source_contract.cdc.publication over every source. Returns one row per source from `summary`, or its table and
row counts.
"""
from __future__ import annotations

import argparse
import os
import re
import sys

from lfc_source_contract import cdc
from lfc_source_contract.jdbc import guarded, ident, literal

from .tpc_schema import tpc_schema

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.dirname(HERE)          # a checkout's scripts/; an installed package has the same files under _tpc, _planner
TPC = os.path.join(HERE, "_tpc") if os.path.isdir(os.path.join(HERE, "_tpc")) else os.path.join(SCRIPTS, "tpc")
PLANNER = os.path.join(HERE, "_planner") if os.path.isdir(os.path.join(HERE, "_planner")) else os.path.join(SCRIPTS, "python")
SPEC = ("tpch", "tpcb")                  # the models tpc_schema writes from the spec (keys, prices, dates across tables)
NAME = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
CHECKED = "lfc:spec-checked"             # a copy's schema comment once its spec checks passed


def check_name(name: str, what: str) -> None:
    if not NAME.match(name or ""):
        raise ValueError(f"{what} '{name}': lower-case letters, digits and _ only")


def connector(database: str, host: str | None = None, password: str | None = None, user: str = "postgres",
              port: int = 5432, admin_database: str = "postgres", lakebase: str | None = None,
              branch: str | None = None, profile: str | None = None):
    """The laptop's connect(): psycopg in autocommit to `database` (created when missing), as an administrator: a
    login given here, or for a Lakebase project you, its owner (lfc_source_contract.lakebase.owner_login)."""
    import psycopg
    from psycopg import sql
    check_name(database, "database")
    if lakebase:
        from lfc_source_contract.lakebase import owner_login
        o = owner_login(lakebase, branch, profile)
        host, user, password, admin_database = o["host"], o["user"], o["password"], "databricks_postgres"
    if not (host and password):
        raise ValueError("connector: host and password, or lakebase")

    def connect(db=database):
        return psycopg.connect(host=host, port=port, user=user, password=password, dbname=db, autocommit=True)
    with connect(admin_database) as c:
        if not c.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,)).fetchone():
            c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    return connect


def parse(sources) -> list[tuple[str, float, str]]:
    """{name: sf | (sf, extra)}, ["name:sf[:extra]", ...] or parse()'s own output -> [(name, sf, extra or "")],
    names checked."""
    items = sources.items() if isinstance(sources, dict) else \
        ((s[0], (s[1], s[2])) if isinstance(s, tuple) else s.split(":", 1) for s in sources)
    out = []
    for name, spec in items:
        sf, extra = (spec, "") if isinstance(spec, (int, float)) else \
                    (spec if isinstance(spec, tuple) else (tuple(str(spec).split(":", 1)) + ("",))[:2])
        check_name(name, "source")
        try:
            sf = float(sf)
        except ValueError:
            raise ValueError(f"{name}: scale factor '{sf}'") from None
        if sf <= 0:
            raise ValueError(f"{name}: scale factor above 0")
        if name in [o[0] for o in out]:
            raise ValueError(f"source {name} twice")
        out.append((name, sf, str(extra or "")))
    if not out:
        raise ValueError("no sources")
    return out


def _planner(module: str):
    """A module of the skill's Python planner folder (python/: fill, sq, planner), imported as its scripts do."""
    import importlib
    sys.path.insert(0, PLANNER)
    try:
        return importlib.import_module(module)
    finally:
        sys.path.remove(PLANNER)


def statements(text: str) -> list[str]:
    """SQL text -> its statements, split by the skill's own splitter (python/sq.py), -- comment lines dropped (so a
    statement's first word is its verb)."""
    out = []
    for s in _planner("sq").statements(text):
        body = "\n".join(line for line in s.splitlines() if not line.lstrip().startswith("--")).strip()
        if body:
            out.append(body)
    return out


def psql_vars(text: str, values: dict) -> str:
    """psql's :'name' (a quoted literal) and :name (as is) for the names given; :: casts stay."""
    text = re.sub(r":'([a-z_]+)'", lambda m: literal(str(values[m.group(1)])), text)
    return re.sub(r"(?<![:\w]):([a-z_]+)\b", lambda m: str(values[m.group(1)]) if m.group(1) in values else m.group(0), text)


def read_sql(name: str) -> str:
    with open(os.path.join(TPC, name)) as f:
        return f.read()


def model_ddl(model: str, sf: float, use_spec: bool | None = None) -> tuple[str, list[str]]:
    """The model's PostgreSQL DDL at scale factor sf, and its table names."""
    import yaml
    from statschema.tools import ddl, ddl_to_schema, shape
    from statschema.tools import model as model_path
    spec = model in SPEC if use_spec is None else use_spec
    schema = ddl_to_schema(tpc_schema(model, sf), "mysql") if spec else shape(model_path(model), sf=sf)[0]
    return ddl(schema, "postgres"), [t["name"] for t in yaml.safe_load(schema)["tables"]]


def _fill(connect, schema: str, sessions: int) -> None:
    """python/fill.py's fill() on one schema, through connect(): the same planner and rounds as its command line."""
    fillpy = _planner("fill")
    if fillpy.fill(fillpy.options(["postgres", schema, "--sessions", str(sessions)]), connect):
        raise RuntimeError(f"fill of {schema} failed (its output above)")


def _all(cur, q: str, params=None) -> list[tuple]:
    cur.execute(q, params)
    return [tuple(r) for r in cur.fetchall()]


def _verify(cur, model: str, copy: str, tables: list[str]) -> int:
    """The spec checks' broken-rule count (0 when the model has none). The checks name the tables bare: qualified
    here, as a JDBC read resolves names before any SET search_path."""
    path = os.path.join(TPC, f"{model}-verify.sql")
    if not os.path.exists(path):
        return 0
    text = re.sub(r"DATEDIFF\(([^,]+), ([^)]+)\)", r"(\1 - \2)", read_sql(f"{model}-verify.sql"))
    text = re.sub(r"(\b(?:FROM|JOIN)\s+)(" + "|".join(map(re.escape, tables)) + r")\b",
                  lambda m: f"{m.group(1)}{ident(copy)}.{m.group(2)}", text, flags=re.IGNORECASE)
    return sum(1 for s in statements(text) for r in _all(cur, s)
               if str(r[0]).startswith("bad") for v in r[1:] if v not in (0, None))


def build(connect, model: str, sources, login: str | None = None, publication: str | None = None,
          sessions: int = 4, steps=None, summary=None, use_spec: bool | None = None, log=print) -> list[dict]:
    """Build the sources in connect()'s database (the module's docstring). steps(name, extra) -> SQL statements run
    in the copy (search_path set) before its rename; summary(cur, name) -> a dict for the result rows."""
    srcs = parse(sources)
    if login is not None:
        check_name(login, "login")
    con = connect()
    cur = con.cursor()
    database = _all(cur, "SELECT current_database()")[0][0]
    pub = publication or f"{database}_pub"
    check_name(pub, "publication")
    for n, (name, sf, extra) in enumerate(srcs, 1):
        if _all(cur, "SELECT 1 FROM pg_namespace WHERE nspname = %s", (name,)):
            log(f"sources: {database}.{name} exists: kept")
            continue
        cp = f"{model}_{n:04d}"
        pg_ddl, tables = model_ddl(model, sf, use_spec)
        cur.execute(guarded(f"SELECT 1 FROM information_schema.tables WHERE table_schema = {literal(cp)}",
                            f"CREATE SCHEMA IF NOT EXISTS {ident(cp)}", f"SET LOCAL search_path = {ident(cp)}",
                            *statements(pg_ddl)))
        if login:
            cur.execute(f"DO $g$ BEGIN EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), "
                        f"{literal(login)}); END $g$")
            cur.execute(f"GRANT USAGE ON SCHEMA {ident(cp)} TO {ident(login)}")
            cur.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {ident(cp)} TO {ident(login)}")
        log(f"sources: {name}: {model} at {sf:g} in {database}.{cp}, filling")
        _fill(connect, cp, sessions)
        if _all(cur, "SELECT obj_description(oid, 'pg_namespace') FROM pg_namespace WHERE nspname = %s", (cp,))[0][0] != CHECKED:
            bad = _verify(cur, model, cp, tables)
            if bad:
                raise RuntimeError(f"{bad} {model} spec rules broken in {database}.{cp}")
            cur.execute(f"COMMENT ON SCHEMA {ident(cp)} IS {literal(CHECKED)}")
        if steps:
            scon = connect()
            scur = scon.cursor()
            scur.execute(f"SET search_path = {ident(cp)}")
            for s in steps(name, extra):
                scur.execute(s)
            scon.close()
        cur.execute(guarded(f"SELECT 1 FROM pg_namespace WHERE nspname = {literal(name)}",
                            f"COMMENT ON SCHEMA {ident(cp)} IS NULL", f"ALTER SCHEMA {ident(cp)} RENAME TO {ident(name)}"))
    names = [s[0] for s in srcs]
    cdc.publication(cur, pub, names, log=log)
    rows = []
    for name in names:
        if summary:
            rows.append({"source": name} | summary(cur, name))
        else:
            tabs = [r[0] for r in _all(cur, "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                                            "WHERE n.nspname = %s AND c.relkind = 'r' ORDER BY 1", (name,))]
            n_rows = _all(cur, " UNION ALL ".join(f"SELECT COUNT(*) FROM {ident(name)}.{ident(t)}" for t in tabs))
            rows.append({"source": name, "tables": len(tabs), "rows": sum(int(r[0]) for r in n_rows)})
    con.close()
    log(f"sources: {database} ready: {model} sources {' '.join(names)}, publication {pub}")
    return rows


def arguments(prog: str, extra_help: str = "") -> argparse.ArgumentParser:
    """The command line shared by lfc-sources and the per-model ones (lfc-tpch-sources)."""
    a = argparse.ArgumentParser(prog=prog, description=__doc__.split("\n")[0])
    a.add_argument("--lakebase", metavar="PROJECT"); a.add_argument("--profile"); a.add_argument("--branch")
    a.add_argument("--host"); a.add_argument("--port", type=int, default=5432); a.add_argument("--user", default="postgres")
    a.add_argument("--password-file"); a.add_argument("--admin-database", default="postgres")
    a.add_argument("--login", help="the reading login (the connection's): SELECT on each source")
    a.add_argument("--publication"); a.add_argument("--sessions", type=int, default=4)
    a.add_argument("--slot", help="create this replication slot (with --drop: drop it) instead of building sources")
    a.add_argument("--drop", action="store_true")
    a.add_argument("database"); a.add_argument("sources", nargs="*", help=f"<name>:<sf>{extra_help}")
    return a


def run(x, model: str, steps=None, summary=None, check=None) -> int:
    """Parsed arguments -> build or slot on the laptop; prints one row per source (tab-separated)."""
    try:
        if not x.slot:
            srcs = parse(x.sources)
            if check:
                check(srcs)                  # bad arguments stop before any connection
        check_name(x.database, "database")
        if x.lakebase:
            connect = connector(x.database, lakebase=x.lakebase, branch=x.branch, profile=x.profile)
        elif x.host and x.password_file:
            with open(os.path.expanduser(x.password_file)) as f:
                pw = f.readline().rstrip("\r\n")
            connect = connector(x.database, host=x.host, password=pw, user=x.user, port=x.port,
                                admin_database=x.admin_database)
        else:
            print("sources: give --lakebase PROJECT, or --host and --password-file", file=sys.stderr)
            return 2
        if x.slot:
            r = cdc.slot(connect().cursor(), x.slot, x.drop)
            if r:
                print(f"{r['slot']}\t{'t' if r['active'] else 'f'}\t{r['held']}")
            return 0
        rows = build(connect, model, srcs, x.login, x.publication, x.sessions, steps, summary,
                     log=lambda m: print(m, file=sys.stderr))
    except (ValueError, RuntimeError) as e:
        print(f"sources: {e}", file=sys.stderr)
        return 2
    for r in rows:
        print("\t".join(str(v) for v in r.values()))
    return 0


def main(argv: list[str] | None = None) -> int:
    a = arguments("lfc-sources")
    a.add_argument("--model", required=True, help="tpcb, tpcc, tpch, tpcds, tpce or tpcdi")
    x = a.parse_args(argv)
    return run(x, x.model)


if __name__ == "__main__":
    sys.exit(main())
