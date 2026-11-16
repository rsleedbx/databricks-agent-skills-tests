"""canonical_sql: statschema's canonical columns as Databricks SQL, one copy for every script of this skill.

silver-from-schema.py (the pipeline), merge-silver.py (the merge job), model-sql.py (the laptop) and placement.py
import it from their own folder (`sys.path` gets the folder first: a job, a pipeline and `python3 -I` don't add it
the same way). Plain Python and yaml, no Spark: the functions build SQL text.
"""
from __future__ import annotations

import os
import re
import zlib

import yaml

# the type names the TPC schema files use besides statschema's canonical ones (ddl.py takes the same)
ALIAS = {"int": "integer", "smallint": "integer", "tinyint": "integer", "bigint": "long", "numeric": "decimal",
         "varchar": "string", "text": "string", "real": "float", "double precision": "double", "bool": "boolean"}
TEMPORAL = ("date", "timestamp", "timestamptz", "time", "timetz")


def ctype(c):
    """A column's canonical type, its alias resolved."""
    return ALIAS.get(c["type"], c["type"])


def spark_type(c):
    """statschema's canonical type -> a Databricks SQL type (char, string, time and the rest: STRING)."""
    t = ctype(c)
    if t == "decimal":
        return f"DECIMAL({c.get('precision') or 38},{c.get('scale') or 0})"
    if c.get("unsigned") and t in ("integer", "long"):   # MySQL's INT UNSIGNED goes past INT, BIGINT UNSIGNED past BIGINT
        return "BIGINT" if t == "integer" else "DECIMAL(20,0)"
    return {"integer": "INT", "long": "BIGINT", "float": "FLOAT", "double": "DOUBLE", "boolean": "BOOLEAN",
            "date": "DATE", "timestamp": "TIMESTAMP_NTZ", "timestamptz": "TIMESTAMP", "binary": "BINARY"}.get(t, "STRING")


def path(base, n):
    """A VARIANT field: the plain form for simple names, brackets for the rest (TPC-DI's mixed-case names)."""
    # a bracket right after a column is a map's; on a VARIANT column it follows a colon (key:['SK_TimeID'])
    return f"{base}:{n}" if re.fullmatch(r"[a-z_][a-z0-9_]*", n) else f"{base}{'' if ':' in base else ':'}['{n}']"


def shard_of(name, n):
    """The shard of a table, crc32(name) % n: fanout.py's Spark crc32 is the same CRC-32, so every part agrees."""
    return zlib.crc32(name.encode()) % n


def decode(c, s, cast="CAST"):
    """s, a STRING expression with the value as text or a number -> the column's type. A number is Debezium's: days
    for a date; for a timestamp, epoch milliseconds when the column holds 3 digits of fraction or fewer, else
    microseconds (without a precision too: PostgreSQL's and CockroachDB's default is 6 digits);
    microseconds of the day for a time; with 7 to 9 digits of fraction (SQL Server's DATETIME2 and TIME), Debezium's
    nanoseconds. cast: CAST, or TRY_CAST (NULL where the text doesn't fit)."""
    t, ty = ctype(c), spark_type(c)
    num = f"{s} RLIKE '^-?[0-9]+$'"
    p = c.get("precision")
    n = f"CAST({s} AS BIGINT)"
    # no precision: 6 digits, as PostgreSQL and CockroachDB default to, so microseconds (a guess by size took a
    # date near 1970 for milliseconds: 1 s after the epoch, 1000000 us, read as 1000 s)
    us = n if p is None or 3 < int(p) <= 6 else f"{n} * 1000" if int(p) <= 3 else f"{n} DIV 1000"   # 7-9 digits: nanoseconds
    if t == "date":
        return f"CASE WHEN {num} THEN date_add(DATE'1970-01-01', CAST({s} AS INT)) ELSE {cast}({s} AS DATE) END"
    if t == "timestamp":
        return (f"CASE WHEN {num} THEN timestampadd(MICROSECOND, {us}, TIMESTAMP_NTZ'1970-01-01 00:00:00') "
                f"ELSE {cast}({s} AS TIMESTAMP_NTZ) END")
    if t == "timestamptz":   # text from PostgreSQL ends +00; Spark takes +00:00
        return (f"CASE WHEN {num} THEN timestamp_micros({us}) "
                f"ELSE {cast}(regexp_replace({s}, '([+-][0-9]{{2}})$', '$1:00') AS TIMESTAMP) END")
    if t in ("time", "timetz"):
        tus = f"{n} * 1000" if p is not None and int(p) <= 3 else f"{n} DIV 1000" if p is not None and int(p) > 6 else n
        return (f"date_format(CASE WHEN {num} THEN timestampadd(MICROSECOND, {tus}, TIMESTAMP_NTZ'1970-01-01 00:00:00') "
                # a snapshot's text: HH:MM:SS, or a whole timestamp where the source keeps a time as one (Oracle's
                # TIMESTAMP(0)): its time of day
                f"WHEN {s} RLIKE '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}[ T]' THEN {cast}({s} AS TIMESTAMP_NTZ) "
                f"ELSE {cast}(concat('1970-01-01 ', {s}) AS TIMESTAMP_NTZ) END, 'HH:mm:ss.SSSSSS')")
    return f"{cast}({s} AS {ty})"


def typed(c, v, cast, binhex=False):
    """A VARIANT field v as the column's type; binhex: binary comes as hex text (Debezium's binary.handling.mode=hex)."""
    if ctype(c) == "binary" and binhex:
        return f"unhex({v}::STRING)"
    return decode(c, f"{v}::STRING", cast) if ctype(c) in TEMPORAL else f"{cast}({v} AS {spark_type(c)})"



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
