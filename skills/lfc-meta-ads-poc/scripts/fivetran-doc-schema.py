#!/usr/bin/env python3
"""fivetran-doc-schema.py [<connector-schema.md URL or file>] > fivetran-doc.tsv

The Fivetran Facebook Ads tables and columns as Fivetran documents them, in schema.sh's TSV format (table, column,
type, position), for schema-diff.py --fivetran before the customer's own Fivetran schema is exported. Default source:
https://fivetran.com/docs/connectors/applications/facebook-ads/connector-schema.md

Fivetran types become the Databricks types Fivetran writes (Long -> bigint, Instant -> timestamp, Json -> string, ...),
so type differences show; unknown types pass through lower-cased. The customer's export (schema.sh on their Fivetran
schema) is the real reference: column blocking, connector version and history mode change what lands.
"""
import re
import sys
import urllib.request

URL = "https://fivetran.com/docs/connectors/applications/facebook-ads/connector-schema.md"
TYPES = {"long": "bigint", "integer": "int", "int": "int", "short": "smallint", "instant": "timestamp",
         "localdatetime": "timestamp", "localdate": "date", "string": "string", "json": "string",
         "boolean": "boolean", "double": "double", "float": "float", "bigdecimal": "decimal(38,10)",
         "binary": "binary"}


def main():
    src = sys.argv[1] if len(sys.argv) > 1 else URL
    if re.match(r"https?://", src):
        req = urllib.request.Request(src, headers={"User-Agent": "Mozilla/5.0"})
        text = urllib.request.urlopen(req, timeout=60).read().decode()
    else:
        with open(src) as f:
            text = f.read()
    table, pos, n = None, 0, 0
    print("table\tcolumn\ttype\tposition")
    for line in text.splitlines():
        m = re.match(r"^###\s+(\S+)\s*$", line)
        if m:
            table, pos = m.group(1), 0
            continue
        m = re.match(r"^\|\s*([A-Za-z0-9_]+)\s*\|\s*([A-Za-z0-9_]+)\s*\|", line)
        if table and m and m.group(1) not in ("Column",):
            pos += 1
            n += 1
            print(f"{table}\t{m.group(1)}\t{TYPES.get(m.group(2).lower(), m.group(2).lower())}\t{pos}")
    if n == 0:
        sys.exit(f"no tables found in {src}: the page layout changed")


if __name__ == "__main__":
    main()
