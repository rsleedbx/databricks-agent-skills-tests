"""coltypes: one column's type across many sources. The rule both intakes use: zerobus-debezium-test's
schema-union.py on statschema's canonical schemas (raw change-log values, decoded after), stack and merge here on Spark
types (typed values). Plain Python, no Spark.

widen: integer < long < decimal (the most integer digits and the most scale any source has; past 38 digits the
scale is kept, so a value too large fails loudly instead of being rounded) < string; types that don't widen into each
other become string. typed=True (values already typed, as in a Delta table) also takes a date into a timestamp and a
float or integer into a double; on raw values a date is a count of days, which a timestamp would read as
microseconds, so they become string there.
"""
from __future__ import annotations

import re

# the type names the TPC schema files use besides statschema's canonical ones (ddl.py takes the same)
ALIAS = {"int": "integer", "smallint": "integer", "tinyint": "integer", "bigint": "long", "numeric": "decimal",
         "varchar": "string", "text": "string", "real": "float", "double precision": "double", "bool": "boolean"}
RANK = {"integer": 0, "long": 1, "decimal": 2}
SPARK = {"tinyint": "integer", "byte": "integer", "smallint": "integer", "short": "integer", "int": "integer",
         "integer": "integer", "bigint": "long", "long": "long", "float": "float", "real": "float", "double": "double",
         "boolean": "boolean", "date": "date", "timestamp": "timestamptz", "timestamp_ltz": "timestamptz",
         "timestamp_ntz": "timestamp", "string": "string", "varchar": "string", "char": "string", "binary": "binary"}
# Delta's type widening, in place (ALTER COLUMN TYPE): integers to larger integers or decimal, float to double, a
# decimal to more digits without less scale, a date to a timestamp without time zone
IN_PLACE = {("integer", "long"), ("integer", "decimal"), ("long", "decimal"), ("float", "double"),
            ("integer", "double"), ("date", "timestamp")}


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


def widen(a, b, typed=False):
    """Two column definitions of one column -> the type that holds both."""
    ta, tb = a["type"], b["type"]
    if ta == tb and ta != "decimal":
        out = {k: v for k, v in a.items() if k != "length"}
        if a.get("length") and b.get("length"):   # a length only when both have one (none is unbounded)
            out["length"] = max(a["length"], b["length"])
        return out
    if ta in RANK and tb in RANK:
        if "decimal" not in (ta, tb):
            return dict(a, type="long")
        # integers as decimal(10,0) / (19,0); typed, a long as (20,0): Delta widens a BIGINT in place only to a decimal
        # with 20 integer digits or more (NOT_SUPPORTED_CHANGE_COLUMN for 19: a serverless warehouse, 2026-11-16)
        def ds(c):
            if c["type"] == "decimal":
                p, s = int(c.get("precision") or 38), int(c.get("scale") or 0)
                return p - s, s
            return (10, 0) if c["type"] == "integer" else (20 if typed else 19, 0)
        (ia, sa), (ib, sb) = ds(a), ds(b)
        i, s = max(ia, ib), max(sa, sb)
        return dict(a, type="decimal", precision=min(i + s, 38), scale=s)
    if typed and {ta, tb} in ({"date", "timestamp"}, {"date", "timestamptz"}):
        return dict(a, type="timestamptz" if "timestamptz" in (ta, tb) else "timestamp")
    if typed and {ta, tb} <= {"float", "double", "integer", "long"} and {ta, tb} & {"float", "double"}:
        return {k: v for k, v in a.items() if k not in ("precision", "scale", "length")} | {"type": "double"}
    if {ta, tb} <= {"string", "char"}:
        return dict({k: v for k, v in a.items() if k != "length"}, type="string")
    return dict({k: v for k, v in a.items() if k not in ("length", "precision", "scale")}, type="string")


def from_spark(t: str) -> dict:
    """A Spark type (DataFrame.dtypes, DESCRIBE) as a canonical column: Spark's TIMESTAMP has a time zone
    (timestamptz), TIMESTAMP_NTZ hasn't (timestamp). A type outside the map (array, struct, ...) keeps its name."""
    t = str(t).strip().lower()
    m = re.fullmatch(r"decimal\((\d+),\s*(\d+)\)", t)
    if m:
        return {"type": "decimal", "precision": int(m.group(1)), "scale": int(m.group(2))}
    if t == "decimal":
        return {"type": "decimal", "precision": 10, "scale": 0}
    return {"type": SPARK.get(t.split("(")[0], t)}


def widest(spark_types) -> str:
    """The Spark type that holds every source's (typed values): DECIMAL(10,2) and DECIMAL(12,4) -> DECIMAL(12,4)."""
    cols = [from_spark(t) for t in spark_types]
    w = cols[0]
    for c in cols[1:]:
        w = widen(w, c, typed=True)
    return spark_type(w)


def widens_in_place(old: str, new: str) -> bool:
    """Whether Delta can change a column from Spark type `old` to `new` in place (type widening)."""
    a, b = from_spark(old), from_spark(new)
    if a["type"] == b["type"] == "decimal":
        return b["scale"] >= a["scale"] and b["precision"] - b["scale"] >= a["precision"] - a["scale"]
    if a["type"] in ("integer", "long") and b["type"] == "decimal":   # integer digits: an INT 10, a BIGINT 20
        return b["precision"] - b["scale"] >= (10 if a["type"] == "integer" else 20)
    return (a["type"], b["type"]) in IN_PLACE
