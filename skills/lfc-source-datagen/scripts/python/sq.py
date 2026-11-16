#!/usr/bin/env python3
# sq.py <engine> [connection options as fill.py's] [--schema S]: SQL statements on stdin -> tab-separated rows, each
# result's header row first, through the same drivers as fill.py: a client for an engine whose own (Db2's CLP,
# Informix's dbaccess) prints no tab-separated form. Statements end with ";" at the end of a line; one that starts
# with CREATE TRIGGER, CREATE OR REPLACE TRIGGER or BEGIN (it may hold ";" inside) ends at a line holding only "@". NULL prints as NULL, other values
# as export.py writes them (timestamps to the microsecond, decimals in plain notation, binary as hex). --schema sets the
# current schema first (Db2's SET SCHEMA), for unqualified names. Autocommit: each statement commits by itself.
# Exit 1 at the first statement that fails, after printing its error on stderr.
import argparse, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import drivers
from engines import ENGINES



def text(v):
    if v is None:
        return "NULL"
    import datetime, decimal
    if hasattr(v, "read"):
        v = v.read()
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, datetime.datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(v, (datetime.date, datetime.time)):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return format(v, "f")
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex()
    return str(v).replace("\t", " ").replace("\n", " ")


def statements(src):
    cur = []
    for line in src.splitlines():
        if line.strip() == "@":
            if cur:
                yield "\n".join(cur)
            cur = []
            continue
        cur.append(line)
        if line.rstrip().endswith(";") and not any(l.strip().upper().startswith(("CREATE TRIGGER", "CREATE OR REPLACE TRIGGER", "BEGIN"))
                                                     for l in cur[:1]):
            s = "\n".join(cur).rstrip().rstrip(";")
            if s.strip():
                yield s
            cur = []
    if any(l.strip() for l in cur):
        yield "\n".join(cur).rstrip().rstrip(";")



def main():
    ap = argparse.ArgumentParser(description="run SQL from stdin through a driver, print TSV")
    ap.add_argument("engine"); ap.add_argument("--schema")
    ap.add_argument("--host", default="127.0.0.1"); ap.add_argument("--port", type=int); ap.add_argument("--user"); ap.add_argument("--database")
    ap.add_argument("--password-file"); ap.add_argument("--server", help="Informix DBSERVERNAME (default informix)")
    ap.add_argument("--trust-server-certificate", action="store_true")
    a = ap.parse_args()
    name = {"pg": "postgres", "my": "mysql", "ms": "sqlserver", "mssql": "sqlserver", "ora": "oracle", "ifx": "informix"}.get(a.engine, a.engine)
    ENGINES[a.engine]   # an unknown engine stops here

    con = drivers.connect(name, a)
    c = con.cursor()
    if a.schema:
        c.execute(f"SET SCHEMA {a.schema}")
    for s in statements(sys.stdin.read()):
        try:
            c.execute(s)
        except Exception as x:
            print(f"error in: {s[:200]}\n{x}", file=sys.stderr)
            sys.exit(1)
        if c.description:
            print("\t".join(d[0] for d in c.description))
            for r in c.fetchall():
                print("\t".join(text(v) for v in r))
    con.close()



if __name__ == "__main__":
    main()
