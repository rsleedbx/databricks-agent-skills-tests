"""cdc_log: an append-only change log of Debezium events as a change source: one Delta table for every table (and
source), as Debezium Server writes it through its Zerobus sink (zerobus-debezium-test). Each row is one event:
`destination` (<prefix>.<schema>.<table>), `operation` (create, update, delete), `key` and `value` (VARIANT: the
event's key and its envelope, value:after, value:before, value:source). shared: one scan serves every table.

Positions are the source's own order for its changes, a kind from SEQUENCES (the event field that gives commit order:
an LSN, a binlog position, an SCN, the event time); its value is that number. A table's
snapshot is a Delta table of its own (intake, qbc), not part of this source: silver combines the two.

    from lfc_change_source.cdc_log import CdcLog
    src = CdcLog("crdb1", "`<catalog>`.`zb`.`cdc_log`", "orders", "crdb1.public.orders", keys=["id"],
                 columns={"id": "BIGINT", "total": "DECIMAL(12,2)"}, seq="ts_ns")
"""
from __future__ import annotations

from dataclasses import dataclass, field

from . import debezium
from .contract import DELETE, OP, SEQ, UPSERT, Position, Stats, last_change_sql, quote

# the event field that orders a source's changes, as a BIGINT (moved from zerobus-debezium-test's silver generator)
SEQUENCES = {
    "ts_ns": "value:ts_ns::BIGINT",
    "binlog": "CAST(regexp_extract(value:source:file::STRING, '([0-9]+)$', 1) AS BIGINT) * 17592186044416"
              " + value:source:pos::BIGINT * 4096 + COALESCE(value:source:row::BIGINT, 0)",
    "lsn": "value:source:lsn::BIGINT",
    "scn": "value:source:scn::BIGINT",
    "ifx_lsn": "value:source:change_lsn::BIGINT",
    "db2_lsn": "CAST(conv(substring(split_part(COALESCE(value:source:change_lsn, value:source:commit_lsn)::STRING, "
               "':', 3), -15), 16, 10) AS BIGINT)",
    "change_lsn": "CAST(conv(split_part(value:source:change_lsn::STRING, ':', 1), 16, 10) AS BIGINT) * 281474976710656"
                  " + CAST(conv(split_part(value:source:change_lsn::STRING, ':', 2), 16, 10) AS BIGINT) * 65536"
                  " + CAST(conv(split_part(value:source:change_lsn::STRING, ':', 3), 16, 10) AS BIGINT)",
    # a Lakeflow Connect gateway's events landed by land.gateway: its cursor, text of fixed width in commit order
    "gateway_cursor": "value:cursor.sequence::STRING",
    # a CockroachDB changefeed's events landed by land.changefeed: the HLC, <wall ns>.<logical>, fixed width
    "hlc": "value:updated::STRING",
}
TEXT = {"gateway_cursor", "hlc"}   # positions compared as text
# a kind whose transport says how far every change has arrived (CockroachDB's RESOLVED files, landed as rows)
RESOLVED = {"hlc": "value:resolved::STRING"}
EVENT = "operation <> 'resolved'"   # a change event, not a landed watermark


def column_sql(name: str, sql_type: str, keys: list[str], precision=None, binhex: bool = False, kind=None,
               cast: str = "CAST", default: str | None = None) -> str:
    """A column from an event: a key column from the event's key (a delete has no after image), the rest from the
    after image, as its type: Debezium's encodings decoded (debezium.typed: a date as days, a timestamp as epoch
    milli-, micro- or nanoseconds by its precision; binary as hex text with binhex; kind: a temporal kind its SQL
    type doesn't say, a time kept as STRING; cast: TRY_CAST to record drift, a value that doesn't fit becoming NULL;
    default: a column's default when the row lacks it). A key is always cast strictly."""
    if name in keys:
        return f"{debezium.typed(f'key:{name}', sql_type, kind, precision, binhex=binhex)} AS {quote(name)}"
    v = f"value:after:{name}"
    e = debezium.typed(v, sql_type, kind, precision, cast, binhex)
    if default:
        e = (f"CASE WHEN value:after IS NOT NULL AND NOT is_variant_null(value:after) AND {v} IS NULL "
             f"THEN CAST({default} AS {sql_type}) ELSE {e} END")
    return f"{e} AS {quote(name)}"


@dataclass
class CdcLog:
    name: str                       # the source (the destination's prefix)
    fq: str                         # the change log table, quoted
    table: str
    destination: str                # <prefix>.<schema>.<table> as the events name it
    keys: list[str]
    columns: dict                   # {column: SQL type}, in order
    seq: str = "ts_ns"              # a SEQUENCES kind
    exprs: dict = field(default_factory=dict)   # {column: SQL expression AS column}: a caller's own typing
    shared: bool = True
    precision: dict = field(default_factory=dict)   # {column: digits of fraction}: a temporal column's (Debezium's unit)
    binhex: bool = False                            # binary values come as hex text (binary.handling.mode=hex)
    kinds: dict = field(default_factory=dict)       # {column: temporal kind} its SQL type doesn't say (a time: STRING)
    cast: str = "CAST"                              # TRY_CAST: drift recorded, a misfit NULL (drift_sql)
    defaults: dict = field(default_factory=dict)    # {column: its default}, for a row that lacks it

    def __post_init__(self):
        if self.seq not in SEQUENCES:
            raise ValueError(f"seq {self.seq!r}: one of {', '.join(SEQUENCES)}")

    def _kind(self, p: Position | None) -> str | None:
        """A position as a SQL literal of its kind (a number, or text for TEXT kinds)."""
        if p is None:
            return None
        if p.kind != self.seq:
            raise ValueError(f"{self.destination}: a position is a {self.seq}, not {p.kind}")
        return "'" + p.value.replace("'", "") + "'" if self.seq in TEXT else str(int(p.value))

    def column_exprs(self) -> list[str]:
        """Each column's SQL from an event (`<expression> AS <name>`), in order."""
        return [self.exprs.get(c) or column_sql(c, t, self.keys, self.precision.get(c), self.binhex, self.kinds.get(c),
                                                self.cast, self.defaults.get(c))
                for c, t in self.columns.items()]

    def drift_sql(self, after: Position | None, to: Position | None) -> str:
        """What its events after `after` up to `to` hold that its columns don't (destination, kind, col, seq, sample):
        a value its type can't hold (type), a known column without a default missing from the row (missing), a column
        it doesn't know (new). One row an event and finding."""
        seq = SEQUENCES[self.seq]
        items = []
        for c, t in self.columns.items():
            if c in self.keys:
                continue
            v = f"value:after:{c}"
            items.append(f"IF({v} IS NOT NULL AND NOT is_variant_null({v}) AND "
                         f"{debezium.typed(v, t, self.kinds.get(c), self.precision.get(c), 'TRY_CAST', self.binhex)} IS NULL, "
                         f"named_struct('kind', 'type', 'col', '{c}'), NULL)")
            if not self.defaults.get(c):
                items.append(f"IF({v} IS NULL, named_struct('kind', 'missing', 'col', '{c}'), NULL)")
        known = ", ".join(f"'{c}'" for c in self.columns)
        new = f"transform(array_except(json_object_keys(to_json(value:after)), array({known})), x -> named_struct('kind', 'new', 'col', x))"
        none = "CAST(NULL AS STRUCT<kind: STRING, col: STRING>)"
        found = f"filter(concat(array({', '.join(items) or none}), {new}), x -> x IS NOT NULL)"
        return (f"SELECT destination, d.kind AS kind, d.col AS col, {seq} AS seq, substr(to_json(value:after), 1, 500) "
                f"AS sample FROM {self.fq} LATERAL VIEW explode({found}) e AS d WHERE {self._where(after, to)} AND "
                "value:after IS NOT NULL AND NOT is_variant_null(value:after)")

    def column_values(self) -> dict:
        """{column: its SQL expression from an event}, column_exprs without the alias, in order."""
        return {c: e.removesuffix(f" AS {quote(c)}") for c, e in zip(self.columns, self.column_exprs())}

    ROW = "value:after"   # the event's row, the same expression for every table of the transport

    def row_sql(self) -> str:
        """The event's row as a VARIANT (one expression for every table of this log's transport)."""
        return self.ROW

    def stored_key_values(self, col: str = "k") -> dict:
        """{key column: its SQL expression} over a stored key (the event's key::STRING kept in a column `col`, as a
        shared group keeps it): typed as column_values types it, so a snapshot's row can be matched to its changes."""
        return {c: debezium.typed(f"parse_json({col}):{c}", self.columns[c], self.kinds.get(c), self.precision.get(c),
                                  binhex=self.binhex) for c in self.keys}

    def stored_values(self, col: str = "data") -> dict:
        """{column: its SQL expression} over a stored row (row_sql's value kept in a column `col`): the same typing as
        column_values, read from that column."""
        out = {}
        for c, t in self.columns.items():
            e = self.exprs.get(c)
            out[c] = (e.removesuffix(f" AS {quote(c)}").replace(self.ROW + ":", col + ":").replace(self.ROW + ".", col + ":")
                      if e else debezium.typed(f"{col}:{quote(c)}", t, self.kinds.get(c), self.precision.get(c), binhex=self.binhex))
        return out

    def position_sql(self, p: Position | None) -> str | None:
        """A position as a SQL literal of this source's kind (None: None)."""
        return self._kind(p)

    def _cols(self) -> str:
        return ", ".join(self.column_exprs())

    def snapshot_sql(self, at: Position | None = None) -> str:
        """The table as of `at` (None: the latest) from the log's changes alone (a log that starts with the table:
        each key's last change, deletes dropped). The log carries changes only: a snapshot is a table of its own, and
        silver combines the two (lfc-log-silver combine)."""
        names = ", ".join(quote(c) for c in self.columns)
        changes = self.changes_sql(None, at)
        return (f"SELECT {names}, '{UPSERT}' AS {OP} FROM ({last_change_sql(changes, self.keys)}) "
                f"WHERE {OP} <> '{DELETE}'")

    def _where(self, after: Position | None, to: Position | None) -> str:
        seq = SEQUENCES[self.seq]
        lo, hi = self._kind(after), self._kind(to)
        return " AND ".join([f"destination = '{self.destination}'", EVENT] + ([f"{seq} > {lo}"] if lo is not None else [])
                            + ([f"{seq} <= {hi}"] if hi is not None else []))

    def inserts_sql(self, after: Position | None, to: Position | None) -> str:
        """Its inserts (operation create) after `after` up to `to`: a keyless table's rows (append-only), whose updates
        and deletes can't name a row. _seq as changes_sql's; _event: a hash of what names the event (its row and its
        source or cursor block, not the processing times a replay may change), so a replayed event counts once and
        another event with the same row at the same position counts too."""
        return (f"SELECT {self._cols()}, named_struct('v', {SEQUENCES[self.seq]}, 'o', 1) AS {SEQ}, "
                f"xxhash64(to_json(value:after), to_json(value:source), to_json(value:cursor), to_json(value:data)) "
                f"AS _event FROM {self.fq} "
                f"WHERE {self._where(after, to)} AND operation = 'create'")

    def changes_sql(self, after: Position | None, to: Position | None) -> str:
        """Its events after `after` up to `to` (None: from the start, to the latest); _seq: the source's order, then
        a delete before an insert at one position. Any event but a delete is an upsert."""
        seq = SEQUENCES[self.seq]
        where = [self._where(after, to)]
        return (f"SELECT {self._cols()}, CASE operation WHEN 'delete' THEN '{DELETE}' ELSE '{UPSERT}' END AS {OP}, "
                f"named_struct('v', {seq}, 'o', CASE operation WHEN 'delete' THEN 0 ELSE 1 END) AS {SEQ} "
                f"FROM {self.fq} WHERE {' AND '.join(where)}")

    def watermark(self, session, kind: str | None = None) -> Position:
        """The latest event's position for this table (the log is append-only: every earlier event is in), or, for a
        kind with RESOLVED, the newest resolved position landed for it (later events can still arrive below the
        newest event's: a changefeed's files from several nodes)."""
        if self.seq in RESOLVED:
            rows = session.sql(f"SELECT max({RESOLVED[self.seq]}) AS v FROM {self.fq} "
                               f"WHERE destination = '{self.destination}' AND operation = 'resolved'").collect()
        else:
            rows = session.sql(f"SELECT max({SEQUENCES[self.seq]}) AS v FROM {self.fq} "
                               f"WHERE destination = '{self.destination}'").collect()
        return Position(self.seq, str(rows[0][0]))

    def stats(self, session, after: Position | None) -> Stats:
        lo = self._kind(after)
        cut = f" AND {SEQUENCES[self.seq]} > {lo}" if lo is not None else ""
        rows = session.sql(f"SELECT count(*) FROM {self.fq} WHERE destination = '{self.destination}' AND {EVENT}{cut}"
                           ).collect()
        return Stats(int(rows[0][0]), None, None)


def build(session, schema: str, snapshot: list, steps: list, columns: list[str], keys: list[str]):
    """The conformance scenario as a change log: the snapshot as a Delta table (the rows as of T), then each step's
    events, one source position a step (ts_ns: 1000 a step), as Debezium writes them (key and value as VARIANT)."""
    cat, sch = schema.split(".", 1)
    log = f"{quote(cat)}.{quote(sch)}.{quote('lfc_change_source_cdc_log')}"
    snap = f"{quote(cat)}.{quote(sch)}.{quote('lfc_change_source_cdc_snapshot')}"
    k, v = columns
    session.sql(f"CREATE OR REPLACE TABLE {snap} ({quote(k)} INT, {quote(v)} STRING)")
    session.sql(f"INSERT INTO {snap} VALUES " + ", ".join(f"({a}, '{b}')" for a, b in snapshot))
    session.sql(f"CREATE OR REPLACE TABLE {log} (destination STRING, operation STRING, key VARIANT, value VARIANT)")
    dest = "conformance.public.t"
    rows = []
    for n, step in enumerate(steps, start=1):
        for op, key, val in step:
            after = "null" if op == "delete" else f'{{"{k}": {key}, "{v}": "{val}"}}'
            operation = {"insert": "create", "update": "update", "delete": "delete"}[op]
            rows.append(f"('{dest}', '{operation}', parse_json('{{\"{k}\": {key}}}'), "
                        f"parse_json('{{\"after\": {after}, \"ts_ns\": {n * 1000}}}'))")
    session.sql(f"INSERT INTO {log} VALUES " + ", ".join(rows))
    src = CdcLog("conformance", log, "t", dest, list(keys), {k: "INT", v: "STRING"}, seq="ts_ns")
    return src, [Position("ts_ns", str(n * 1000)) for n in range(len(steps) + 1)], f"SELECT * FROM {snap}"

