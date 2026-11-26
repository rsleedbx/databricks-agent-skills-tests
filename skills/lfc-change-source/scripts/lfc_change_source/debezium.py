"""debezium: Debezium's value encodings as Databricks SQL, the one copy (cdc_log's columns and zerobus-debezium-test's
silver both use it). Debezium Server's JSON sends a date as days since the epoch, a timestamp as epoch milli-, micro-
or nanoseconds by the column's precision, a time as microseconds of the day; a snapshot's text, or another producer's,
comes as text. Each function builds SQL from a STRING expression, nothing else: plain Python, no Spark.

    from lfc_change_source import debezium
    debezium.decode("date", "DATE", None, "value:after:d::STRING")      # a date from days or text
    debezium.typed("value:after:ts", "TIMESTAMP_NTZ", precision=3)       # a VARIANT field as the column's type
"""
from __future__ import annotations

TEMPORAL = ("date", "timestamp", "timestamptz", "time", "timetz")
# a Databricks SQL type -> the temporal kind decode() needs (TIMESTAMP is with a time zone, TIMESTAMP_NTZ without)
SQL_KIND = {"DATE": "date", "TIMESTAMP": "timestamptz", "TIMESTAMP_NTZ": "timestamp"}


def kind_of(sql_type: str) -> str | None:
    """The temporal kind of a Databricks SQL type (DATE, TIMESTAMP, TIMESTAMP_NTZ), else None."""
    return SQL_KIND.get(sql_type.strip().upper())


def decode(kind: str | None, sql_type: str, precision, s: str, cast: str = "CAST") -> str:
    """s, a STRING expression with the value as text or a number -> sql_type. A number is Debezium's: days for a
    date; for a timestamp, epoch milliseconds when the column holds 3 digits of fraction or fewer, else microseconds
    (without a precision too: PostgreSQL's and CockroachDB's default is 6 digits); microseconds of the day for a time;
    with 7 to 9 digits of fraction (SQL Server's DATETIME2 and TIME), Debezium's nanoseconds. cast: CAST, or TRY_CAST
    (NULL where the text doesn't fit)."""
    num = f"{s} RLIKE '^-?[0-9]+$'"
    p = precision
    n = f"CAST({s} AS BIGINT)"
    # no precision: 6 digits, as PostgreSQL and CockroachDB default to, so microseconds (a guess by size took a
    # date near 1970 for milliseconds: 1 s after the epoch, 1000000 us, read as 1000 s)
    us = n if p is None or 3 < int(p) <= 6 else f"{n} * 1000" if int(p) <= 3 else f"{n} DIV 1000"   # 7-9 digits: nanoseconds
    if kind == "date":
        return f"CASE WHEN {num} THEN date_add(DATE'1970-01-01', CAST({s} AS INT)) ELSE {cast}({s} AS DATE) END"
    if kind == "timestamp":
        return (f"CASE WHEN {num} THEN timestampadd(MICROSECOND, {us}, TIMESTAMP_NTZ'1970-01-01 00:00:00') "
                f"ELSE {cast}({s} AS TIMESTAMP_NTZ) END")
    if kind == "timestamptz":   # text from PostgreSQL ends +00; Spark takes +00:00
        return (f"CASE WHEN {num} THEN timestamp_micros({us}) "
                f"ELSE {cast}(regexp_replace({s}, '([+-][0-9]{{2}})$', '$1:00') AS TIMESTAMP) END")
    if kind in ("time", "timetz"):
        tus = f"{n} * 1000" if p is not None and int(p) <= 3 else f"{n} DIV 1000" if p is not None and int(p) > 6 else n
        return (f"date_format(CASE WHEN {num} THEN timestampadd(MICROSECOND, {tus}, TIMESTAMP_NTZ'1970-01-01 00:00:00') "
                # a snapshot's text: HH:MM:SS, or a whole timestamp where the source keeps a time as one (Oracle's
                # TIMESTAMP(0)): its time of day
                f"WHEN {s} RLIKE '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}[ T]' THEN {cast}({s} AS TIMESTAMP_NTZ) "
                f"ELSE {cast}(concat('1970-01-01 ', {s}) AS TIMESTAMP_NTZ) END, 'HH:mm:ss.SSSSSS')")
    return f"{cast}({s} AS {sql_type})"


def typed(v: str, sql_type: str, kind: str | None = None, precision=None, cast: str = "CAST", binhex: bool = False) -> str:
    """A VARIANT field v as sql_type: a temporal one decoded (kind: from sql_type when not given), binary as hex text
    when binhex (Debezium's binary.handling.mode=hex), the rest cast."""
    k = kind or kind_of(sql_type)
    if sql_type.strip().upper() == "BINARY" and binhex:
        return f"unhex({v}::STRING)"
    return decode(k, sql_type, precision, f"{v}::STRING", cast) if k in TEMPORAL else f"{cast}({v} AS {sql_type})"
