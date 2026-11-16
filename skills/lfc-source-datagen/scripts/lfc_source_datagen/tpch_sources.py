"""lfc_source_datagen.tpch_sources: TPC-H as several business sources in one Postgres database, one schema per source,
each at its own scale, ready for Lakeflow Connect CDC: n lines of business (or acquired companies) with the same tables
and repeating keys, for an n:1 test (lfc-medallion). The Python form of tpc/tpch-sources.sh, for a notebook, a job or
a pipeline as well as the laptop, through a direct connection (no psql, no CLI):

    from lfc_source_datagen.tpch_sources import Pg, lakebase, tpch_sources, slot
    pg = lakebase("my-project")                        # in a workspace: its own login; on a laptop: lakebase(..., profile=)
    # pg = Pg(host=..., port=5432, user=..., password=...)   # any PostgreSQL, as an administrator
    rows = tpch_sources(pg, "orders_db", {"express": (0.05, "1,0,1,1,5,0,0,1,2"), "grocery": 0.1}, login="lfc_cdc")
    slot(pg, "orders_db", "orders_slot")               # after the gateway and ingestion pipeline exist

Per source, as tpch-sources.sh: spec TPC-H at the source's scale (tpc_schema, then statschema's ddl_to_schema and ddl),
filled by the skill's Python planner (python/fill.py, run unchanged), checked against the spec (tpc/tpch-verify.sql),
renamed to the source, the spec's words in its text columns (tpc/tpch-labels.sql), optional lead times a model can
learn (tpc/tpch-lead-times.sql: commit,handle_min,handle_span,slow_nation,slow_every,air,truck,sea,jitter), and SELECT
for the connection's login. Then one publication for every source: by schema on Postgres 15 and later, by table before;
one that already has exactly these sources is kept (changing a publication's schemas needs a superuser, which a managed
service's owner, Lakebase's, isn't). A schema that already exists is kept as it is. Returns one row per source: name,
lines, % late, median days from order to receipt.

  lfc-tpch-sources [--lakebase P [--profile X] [--branch B] | --host H --port N --user U --password-file F]
                   [--login R] [--labels spec|anonymous] [--publication NAME] <database> <name>:<sf>[:<lead times>] ...
  lfc-tpch-sources [connection options] --slot <slot> [--drop] <database>
"""
from __future__ import annotations

import argparse
import contextlib
import io
import os
import re
import runpy
import sys
import tempfile
from dataclasses import dataclass

from .tpc_schema import tpc_schema

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.dirname(HERE)          # a checkout's scripts/; an installed package has the same files under _tpc, _planner
TPC = os.path.join(HERE, "_tpc") if os.path.isdir(os.path.join(HERE, "_tpc")) else os.path.join(SCRIPTS, "tpc")
PLANNER = os.path.join(HERE, "_planner") if os.path.isdir(os.path.join(HERE, "_planner")) else os.path.join(SCRIPTS, "python")
LEAD = ("commit", "handle_min", "handle_span", "slow_nation", "slow_every", "air", "truck", "sea", "jitter")
NAME = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


@dataclass
class Pg:
    """A PostgreSQL connection as an administrator (it creates the database and the publication)."""
    host: str
    password: str
    user: str = "postgres"
    port: int = 5432
    admin_database: str = "postgres"     # the database to connect to before <database> exists

    def connect(self, database: str | None = None):
        import psycopg
        return psycopg.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                               dbname=database or self.admin_database, autocommit=True)


def lakebase(project: str, branch: str | None = None, profile: str | None = None) -> Pg:
    """A Lakebase project's read-write endpoint, signed in as you (the project owner) with an OAuth database credential:
    in a workspace (notebook, job) its own login, on a laptop a CLI profile. The credential lasts about an hour."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.postgres import EndpointType
    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    name = f"projects/{project}"
    br = f"{name}/branches/{branch}" if branch else w.postgres.get_project(name).status.default_branch
    if not br.startswith("projects/"):
        br = f"{name}/branches/{br}"
    ep = next((e for e in w.postgres.list_endpoints(br)
               if e.status and e.status.endpoint_type == EndpointType.ENDPOINT_TYPE_READ_WRITE), None)
    if ep is None:
        raise RuntimeError(f"no read-write endpoint on {br}")
    token = w.postgres.generate_database_credential(endpoint=ep.name).token
    return Pg(host=ep.status.hosts.host, password=token, user=w.current_user.me().user_name,
              admin_database="databricks_postgres")


def _check(name: str, what: str) -> None:
    if not NAME.match(name or ""):
        raise ValueError(f"{what} '{name}': lower-case letters, digits and _ only")


def _sources(sources) -> list[tuple[str, float, str]]:
    """{name: sf | (sf, lead times)} or ["name:sf[:lead times]", ...] -> [(name, sf, lead times or "")], checked."""
    items = sources.items() if isinstance(sources, dict) else (s.split(":", 1) for s in sources)
    out = []
    for name, spec in items:
        sf, lt = (spec, "") if isinstance(spec, (int, float)) else \
                 (spec if isinstance(spec, tuple) else (tuple(str(spec).split(":", 1)) + ("",))[:2])
        _check(name, "source")
        try:
            sf = float(sf)
        except ValueError:
            raise ValueError(f"{name}: scale factor '{sf}'") from None
        if sf < 0.03:
            raise ValueError(f"{name}: TPC-H needs scale factor 0.03 or more (230 suppliers); got {sf:g}")
        lt = str(lt or "")
        if lt:
            if not re.fullmatch(r"[0-9]+(,[0-9]+){8}", lt):
                raise ValueError(f"{name}: lead times are 9 whole numbers, comma-separated")
            v = dict(zip(LEAD, map(int, lt.split(","))))
            if min(v["handle_span"], v["slow_every"], v["jitter"]) < 1:
                raise ValueError(f"{name}: handle_span, slow_every and jitter at least 1")
        if name in [o[0] for o in out]:
            raise ValueError(f"source {name} twice")
        out.append((name, sf, lt))
    if not out:
        raise ValueError("no sources")
    return out


def _statements(text: str) -> list[str]:
    """SQL text -> its statements (each ends with ; at the end of a line; -- comments dropped)."""
    body = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--"))
    return [s.strip() for s in re.split(r";[ \t]*(?:\n|$)", body) if s.strip()]


def _psql_vars(text: str, values: dict) -> str:
    """psql's :'name' (a quoted literal) and :name (as is) for the names given; :: casts stay."""
    def lit(m):
        return "'" + str(values[m.group(1)]).replace("'", "''") + "'"
    text = re.sub(r":'([a-z_]+)'", lit, text)
    return re.sub(r"(?<![:\w]):([a-z_]+)\b", lambda m: str(values[m.group(1)]) if m.group(1) in values else m.group(0), text)


def _read(name: str) -> str:
    with open(os.path.join(TPC, name)) as f:
        return f.read()


def _fill(pg: Pg, database: str, schema: str, sessions: int) -> str:
    """The skill's Python planner (python/fill.py, run unchanged) on one schema; its output, or RuntimeError."""
    out = io.StringIO()
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        f.write(pg.password + "\n")
    os.chmod(f.name, 0o600)
    argv = ["fill.py", "pg", schema, "--host", pg.host, "--port", str(pg.port), "--user", pg.user,
            "--database", database, "--password-file", f.name, "--sessions", str(sessions)]
    old = sys.argv, list(sys.path)
    try:
        sys.argv = argv
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            try:
                runpy.run_path(os.path.join(PLANNER, "fill.py"), run_name="__main__")
                code = 0
            except SystemExit as e:
                code = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
                if not isinstance(e.code, int) and e.code is not None:
                    print(e.code)
    finally:
        sys.argv, sys.path[:] = old[0], old[1]
        os.unlink(f.name)
    if code:
        raise RuntimeError(f"fill of {database}.{schema} failed:\n" + "\n".join(out.getvalue().splitlines()[-5:]))
    return out.getvalue()


def _one(cur, q, args=None):
    cur.execute(q, args)
    r = cur.fetchone()
    return r[0] if r else None


def tpch_sources(pg: Pg, database: str, sources, login: str | None = None, labels: str = "spec",
                 publication: str | None = None, sessions: int = 4, log=print) -> list[dict]:
    """Build the sources (see the module's docstring); returns [{source, lines, late_pct, median_days}]."""
    from psycopg import sql
    _check(database, "database")
    srcs = _sources(sources)
    if labels not in ("spec", "anonymous"):
        raise ValueError("labels: spec or anonymous")
    pub = publication or f"{database}_pub"
    _check(pub, "publication")
    if login is not None and not login:
        raise ValueError("login: the connection's Postgres login, or None for no grants")

    c0 = pg.connect()                   # autocommit: CREATE DATABASE can't run in a transaction
    k = c0.cursor()
    if not _one(k, "SELECT COUNT(*) FROM pg_database WHERE datname = %s", (database,)):
        k.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    c0.close()
    con = pg.connect(database)
    cur = con.cursor()
    pg_ddl = None
    for n, (name, sf, lt) in enumerate(srcs, 1):
        if _one(cur, "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = %s", (name,)):
            log(f"tpch-sources: {database}.{name} exists: kept")
            continue
        # 1. spec TPC-H at the source's scale, filled as copy n, checked against the spec
        from statschema.tools import ddl, ddl_to_schema
        pg_ddl = ddl(ddl_to_schema(tpc_schema("tpch", sf), "mysql"), "postgres")
        cp = f"tpch_{n:04d}"
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(cp)))
        cur.execute(sql.SQL("SET search_path = {}").format(sql.Identifier(cp)))
        if not _one(cur, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = %s", (cp,)):
            for s in _statements(pg_ddl):
                cur.execute(s)
        log(f"tpch-sources: {name}: TPC-H at {sf:g} in {database}.{cp}, filling")
        _fill(pg, database, cp, sessions)
        verify = re.sub(r"DATEDIFF\(([^,]+), ([^)]+)\)", r"(\1 - \2)", _read("tpch-verify.sql"))
        bad = 0
        for s in _statements(verify):
            cur.execute(s)
            for r in cur.fetchall():
                if str(r[0]).startswith("bad"):
                    bad += sum(1 for v in r[1:] if v not in (0, None))
        if bad:
            raise RuntimeError(f"{bad} TPC-H rules broken in {database}.{cp}")
        # 2. the source's name, the spec's words, and the lead times
        cur.execute(sql.SQL("ALTER SCHEMA {} RENAME TO {}").format(sql.Identifier(cp), sql.Identifier(name)))
        cur.execute(sql.SQL("SET search_path = {}").format(sql.Identifier(name)))
        for s in _statements(_psql_vars(_read("tpch-labels.sql"), {"names": labels})):
            cur.execute(s)
        if lt:
            for s in _statements(_psql_vars(_read("tpch-lead-times.sql"), dict(zip(LEAD, lt.split(","))))):
                cur.execute(s)
        # 3. the connection's login reads it
        if login:
            cur.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(database), sql.Identifier(login)))
            cur.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(sql.Identifier(name), sql.Identifier(login)))
            cur.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(sql.Identifier(name), sql.Identifier(login)))

    # 4. one publication for every source; kept when it already has exactly these
    names = [s[0] for s in srcs]
    schemas = sql.SQL(", ").join(sql.Identifier(x) for x in names)
    if int(_one(cur, "SHOW server_version_num")) >= 150000:
        what = sql.SQL("TABLES IN SCHEMA {}").format(schemas)
        want = ",".join(sorted(names))
        have = _one(cur, "SELECT string_agg(n.nspname, ',' ORDER BY n.nspname COLLATE \"C\") FROM pg_publication p "
                         "JOIN pg_publication_namespace pn ON pn.pnpubid = p.oid JOIN pg_namespace n ON n.oid = pn.pnnspid "
                         "WHERE p.pubname = %s", (pub,))
    else:
        cur.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' "
                    "AND table_schema = ANY(%s) ORDER BY 1, 2", (names,))
        tabs = cur.fetchall()
        what = sql.SQL("TABLE {}").format(sql.SQL(", ").join(sql.Identifier(a, b) for a, b in tabs))
        want = ",".join(sorted(f"{a}.{b}" for a, b in tabs))
        cur.execute("SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = %s", (pub,))
        have = ",".join(sorted(f"{a}.{b}" for a, b in cur.fetchall())) or None
    exists = _one(cur, "SELECT COUNT(*) FROM pg_publication WHERE pubname = %s", (pub,))
    if not exists:
        cur.execute(sql.SQL("CREATE PUBLICATION {} FOR ").format(sql.Identifier(pub)) + what)
    elif have == want:
        log(f"tpch-sources: publication {pub} already has these sources: kept")
    else:
        try:
            cur.execute(sql.SQL("ALTER PUBLICATION {} SET ").format(sql.Identifier(pub)) + what)
        except Exception as e:
            raise RuntimeError(f"publication {pub} has other tables, and changing it failed ({e.__class__.__name__}: "
                               f"{str(e).strip()}): drop it as its owner (DROP PUBLICATION {pub}, after stopping the gateway "
                               "that reads it) and rerun, or name another publication") from None

    # 5. what each source looks like
    rows = []
    for name in names:
        cur.execute(sql.SQL("SELECT COUNT(*), ROUND(100.0 * AVG((l_receiptdate > l_commitdate)::int), 1), "
                            "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY l_receiptdate - o_orderdate) "
                            "FROM {}.lineitem JOIN {}.orders ON o_orderkey = l_orderkey").format(
                                sql.Identifier(name), sql.Identifier(name)))
        lines, late, median = cur.fetchone()
        rows.append({"source": name, "lines": int(lines), "late_pct": float(late), "median_days": float(median)})
    con.close()
    log(f"tpch-sources: {database} ready: sources {' '.join(names)}, publication {pub}")
    return rows


def slot(pg: Pg, database: str, name: str, drop: bool = False) -> dict | None:
    """Create the pgoutput replication slot (kept when it exists), or with drop=True drop it when not active. Returns
    {slot, active, held} (WAL the slot holds), or None when there is none."""
    _check(database, "database"); _check(name, "slot")
    con = pg.connect(database)
    cur = con.cursor()
    if drop:
        cur.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s AND NOT active",
                    (name,))
    elif not _one(cur, "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = %s", (name,)):
        cur.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (name,))
    cur.execute("SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) "
                "FROM pg_replication_slots WHERE slot_name = %s", (name,))
    r = cur.fetchone()
    con.close()
    return {"slot": r[0], "active": r[1], "held": r[2]} if r else None


def main(argv: list[str] | None = None) -> int:
    a = argparse.ArgumentParser(prog="lfc-tpch-sources", description=__doc__.split("\n")[0])
    a.add_argument("--lakebase", metavar="PROJECT"); a.add_argument("--profile"); a.add_argument("--branch")
    a.add_argument("--host"); a.add_argument("--port", type=int, default=5432); a.add_argument("--user", default="postgres")
    a.add_argument("--password-file"); a.add_argument("--admin-database", default="postgres")
    a.add_argument("--login", help="the connection's Postgres login: SELECT on each source")
    a.add_argument("--labels", default="spec", choices=["spec", "anonymous"]); a.add_argument("--publication")
    a.add_argument("--sessions", type=int, default=4)
    a.add_argument("--slot", help="create this replication slot (with --drop: drop it) instead of building sources")
    a.add_argument("--drop", action="store_true")
    a.add_argument("database"); a.add_argument("sources", nargs="*", help="<name>:<sf>[:<lead times>]")
    x = a.parse_args(argv)
    try:
        if not x.slot:
            srcs = _sources(x.sources)       # bad arguments stop before any connection
        if x.lakebase:
            pg = lakebase(x.lakebase, x.branch, x.profile)
        elif x.host and x.password_file:
            with open(os.path.expanduser(x.password_file)) as f:
                pg = Pg(host=x.host, port=x.port, user=x.user, password=f.readline().rstrip("\r\n"),
                        admin_database=x.admin_database)
        else:
            a.error("give --lakebase PROJECT, or --host and --password-file")
        if x.slot:
            r = slot(pg, x.database, x.slot, x.drop)
            if r:
                print(f"{r['slot']}\t{'t' if r['active'] else 'f'}\t{r['held']}")
            return 0
        rows = tpch_sources(pg, x.database, [f"{n}:{sf:g}" + (f":{lt}" if lt else "") for n, sf, lt in srcs], x.login,
                            x.labels, x.publication, x.sessions, log=lambda m: print(m, file=sys.stderr))
    except (ValueError, RuntimeError) as e:
        print(f"tpch-sources: {e}", file=sys.stderr)
        return 2
    for r in rows:
        print(f"{r['source']}\t{r['lines']}\t{r['late_pct']}\t{r['median_days']:g}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
