"""canonical_sql: statschema's canonical columns as Databricks SQL, one copy for every script of this skill.

silver-from-schema.py (the pipeline), merge-silver.py (the merge job), model-sql.py (the laptop) and placement.py
import it from their own folder (`sys.path` gets the folder first: a job, a pipeline and `python3 -I` don't add it
the same way). The type names are lfc_medallion.coltypes'. Plain Python and yaml, no Spark: the functions build
SQL text.
"""
from __future__ import annotations

import os
import re
import sys
import zlib

import yaml

# ctype and spark_type: lfc_medallion's (lfc-medallion/scripts in a checkout; on a workspace lfc_medallion/ is uploaded
# next to this file), so the n:1 layer and this skill name types the same way
# and Debezium's value encodings: lfc_change_source.debezium (lfc-change-source/scripts in a checkout; on a workspace
# lfc_change_source/ is uploaded next to this file too), the one copy cdc_log's columns use as well
for _skill in ("lfc-medallion", "lfc-change-source"):
    _PKG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", _skill, "scripts")
    if os.path.isdir(_PKG):
        sys.path.insert(0, _PKG)
# re-exported: the scripts import ctype and spark_type from here
from lfc_change_source import debezium  # noqa: E402
from lfc_medallion.coltypes import ctype, spark_type  # noqa: E402

TEMPORAL = debezium.TEMPORAL

def path(base, n):
    """A VARIANT field: the plain form for simple names, brackets for the rest (TPC-DI's mixed-case names)."""
    # a bracket right after a column is a map's; on a VARIANT column it follows a colon (key:['SK_TimeID'])
    return f"{base}:{n}" if re.fullmatch(r"[a-z_][a-z0-9_]*", n) else f"{base}{'' if ':' in base else ':'}['{n}']"


def shard_of(name, n):
    """The shard of a table, crc32(name) % n: fanout.py's Spark crc32 is the same CRC-32, so every part agrees."""
    return zlib.crc32(name.encode()) % n


def decode(c, s, cast="CAST"):
    """s, a STRING expression with the value as text or a number -> the column's type: Debezium's encodings
    (lfc_change_source.debezium.decode) by the canonical column's kind and precision."""
    return debezium.decode(ctype(c), spark_type(c), c.get("precision"), s, cast)


def typed(c, v, cast, binhex=False):
    """A VARIANT field v as the column's type; binhex: binary comes as hex text (Debezium's binary.handling.mode=hex)."""
    if ctype(c) == "binary" and binhex:
        return f"unhex({v}::STRING)"
    k = ctype(c)
    return debezium.typed(v, spark_type(c), k if k in TEMPORAL else None, c.get("precision"), cast)



def snapshot_value(c, have, binhex=False):
    """A snapshot file's column as the column's type: its default when the file lacks it (added after T); binhex: a
    CSV snapshot holds binary as hex text (Parquet holds it as binary)."""
    if c["name"] not in have:
        return f"CAST({c.get('default') or 'NULL'} AS {spark_type(c)})"
    x = f"`{c['name']}`"
    if ctype(c) == "binary" and binhex:
        return f"CASE WHEN typeof({x}) = 'binary' THEN CAST({x} AS BINARY) ELSE unhex(CAST({x} AS STRING)) END"
    return decode(c, f"CAST({x} AS STRING)") if ctype(c) in TEMPORAL else f"CAST({x} AS {spark_type(c)})"

def load(vol):
    """The canonical schema {table: columns}: <vol>/_schema/schema.yaml, then any _schema/tables/<table>.yaml (a
    table's evolved schema) over it."""
    with open(f"{vol}/_schema/schema.yaml") as f:
        out = {t["name"]: t["columns"] for t in yaml.safe_load(f)["tables"]}
    d = f"{vol}/_schema/tables"
    for name in sorted(os.listdir(d)) if os.path.isdir(d) else []:
        if name.endswith(".yaml"):
            with open(f"{d}/{name}") as f:
                out.update({t["name"]: t["columns"] for t in yaml.safe_load(f)["tables"]})
    return out
