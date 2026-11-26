#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["pyyaml", "sqlglot"]
# ///
# uv run to-canonical.py raw.json schema.yaml stats.yaml
# A collector's output (collect-postgres.sql, or collect-mysql.sql through mysql-normalize.jq) -> statschema's canonical schema.yaml and stats.yaml, the format every other step reads and writes.
# Types become statschema's canonical types (canon.py: integer, long, decimal, string, char, date, ...), with the source's own type kept as
# native_type. Most-common-value frequencies become fractions of the non-null rows (statschema's convention; Postgres and
# MySQL store fractions of all rows). Hidden values (values=none) stay null, and the file says which values it holds.
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import yaml

from statschema.tools.canonical import convert, write  # noqa: E402

schema, stats = convert(json.load(open(sys.argv[1])))
write(schema, stats, sys.argv[2], sys.argv[3])
print(f"{len(schema['tables'])} tables -> {sys.argv[2]}, {sys.argv[3]}", file=sys.stderr)
