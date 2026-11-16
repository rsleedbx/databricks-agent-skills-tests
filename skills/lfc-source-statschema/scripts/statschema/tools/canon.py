"""A native column type (any sqlglot dialect) -> statschema's canonical type: integer, long, float, double, decimal
(precision, scale), string (length), char (length), boolean, date, timestamp, timestamptz, time, timetz, binary (length).
Text, json, uuid, enum, and other types are strings. Used by to-canonical.py and ddl-to-schema.py."""
from sqlglot import exp

T = exp.DataType.Type
CANON = {T.TINYINT: "integer", T.SMALLINT: "integer", T.MEDIUMINT: "integer", T.INT: "integer", T.UTINYINT: "integer",
         T.USMALLINT: "integer", T.UMEDIUMINT: "integer", T.UINT: "integer", T.BIGINT: "long", T.UBIGINT: "long",
         T.FLOAT: "float", T.DOUBLE: "double", T.DECIMAL: "decimal", T.MONEY: "decimal", T.BOOLEAN: "boolean",
         T.DATE: "date", T.DATETIME: "timestamp", T.TIMESTAMP: "timestamp", T.TIMESTAMPTZ: "timestamptz",
         T.TIME: "time", T.TIMETZ: "timetz", T.BINARY: "binary", T.VARBINARY: "binary", T.BLOB: "binary",
         T.MEDIUMBLOB: "binary", T.LONGBLOB: "binary", T.TINYBLOB: "binary", T.CHAR: "char", T.NCHAR: "char"}

def canon(d, dialect=None):
    """d: a sqlglot DataType, or a type's text in `dialect`."""
    if not isinstance(d, exp.DataType): native, d = d, exp.DataType.build(d, dialect=dialect)
    else: native = d.sql(dialect)
    args = [int(e.name) for e in d.expressions if e.name.isdigit()]
    c = {"type": CANON.get(d.this, "string")}
    if d.this.name.startswith("U"): c["unsigned"] = True
    if c["type"] == "decimal" and args: c["precision"], c["scale"] = args[0], (args[1] if len(args) > 1 else 0)
    if c["type"] in ("string", "char", "binary") and args and d.this.name in ("CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "BINARY", "VARBINARY"):
        c["length"] = args[0]
    if dialect == "mysql" and native.lower().startswith("tinyint(1)"): c = {"type": "boolean"}
    # MySQL's DATETIME and TIMESTAMP hold no fraction unless declared (fsp 0), where PostgreSQL's default is 6: the
    # precision says which, and a change log's epoch number is milliseconds up to 3 digits (Debezium), microseconds above
    if dialect == "mysql" and c["type"] == "timestamp": c["precision"] = args[0] if args else 0
    return c
