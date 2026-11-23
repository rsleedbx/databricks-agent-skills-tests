"""lfc_bronze: a Lakeflow Connect bronze table (the ingestion pipeline's streaming table, one per source and table) as
a change source. Positions are the table's Delta versions (delta_version), or its commit times (commit_ts: the table's
in-commit timestamps, which only increase), so a consumer can read changes from a saved time without first reading the
table's latest version; changes come from its change data feed (table_changes).

By time, Delta resolves a start to the first commit at or after it by the second: a commit in the same second as the
saved time is read again (an SCD type 1 consumer applies it again with the same result; one that counts changes must
read by version), and a start more than a second after the last commit fails (nothing_newer: nothing changed). An end
after the last commit fails too, so a consumer reads to the latest (to=None) and saves the time it took before reading.

    from lfc_change_source.lfc_bronze import Bronze
    src = Bronze("lob001", "`main`.`sales_bronze_lob001`.`account`", "account", keys=["id"], columns=["id", "v"])
    src.changes_sql(Position("delta_version", "12"), Position("delta_version", "15"))
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .contract import DELETE, OP, SEQ, UPSERT, Position, Stats, quote

KIND, TS = "delta_version", "commit_ts"
NOTHING_NEWER = "DELTA_TIMESTAMP_GREATER_THAN_COMMIT"   # a start or end time after the table's last commit


def version(p: Position | None) -> int | None:
    if p is None:
        return None
    if p.kind != KIND:
        raise ValueError(f"a bronze table's position is a {KIND} or {TS}, not {p.kind}")
    return int(p.value)


def _bound(p: Position, after: bool) -> str:
    """A position as a table_changes bound: a version (after: the next one), or a time literal (after: a microsecond
    later, so the commit at the saved time isn't read again)."""
    if p.kind == TS:
        return f"TIMESTAMP'{p.value}' + INTERVAL 1 MICROSECOND" if after else f"TIMESTAMP'{p.value}'"
    return str(version(p) + 1 if after else version(p))


def nothing_newer(error: Exception) -> bool:
    """Whether a changes_sql by time failed only because the table has no commit after its start: nothing changed."""
    return NOTHING_NEWER in str(error)


@dataclass
class Bronze:
    name: str                 # the source
    fq: str                   # its bronze table, quoted (`catalog`.`schema`.`table`)
    table: str
    keys: list[str]
    columns: list[str]
    shared: bool = field(default=False)

    def select(self, data: str | None = None) -> str:
        return data or ", ".join(quote(c) for c in self.columns)

    def snapshot_sql(self, at: Position | None = None, data: str | None = None) -> str:
        """The table at version `at` (pinned, so a commit in between isn't read twice); data: the column expressions
        in place of the columns."""
        pin = "" if at is None else (f" TIMESTAMP AS OF TIMESTAMP'{at.value}'" if at.kind == TS
                                     else f" VERSION AS OF {version(at)}")
        return f"SELECT {self.select(data)}, '{UPSERT}' AS {OP} FROM {self.fq}{pin}"

    def changes_sql(self, after: Position, to: Position | None, data: str | None = None) -> str:
        """Its changes after `after` up to `to` (both versions or both commit times; to=None: up to its latest
        commit, which a time range needs, as Delta refuses an end after the last commit), update pre-images dropped;
        _seq: the commit version, then a delete before an insert of the same key in one version (a replacement: the
        insert wins). Also _change_type and _commit_timestamp (what a consumer saves after reading by time). A start
        time after the last commit fails with DELTA_TIMESTAMP_GREATER_THAN_COMMIT: nothing_newer() says so."""
        if to is not None and to.kind != after.kind:
            raise ValueError(f"a range's positions are one kind, not {after.kind} and {to.kind}")
        name = self.fq.replace("`", "")
        end = "" if to is None else f", {_bound(to, False)}"
        return (f"SELECT {self.select(data)}, CASE _change_type WHEN 'delete' THEN '{DELETE}' ELSE '{UPSERT}' END "
                f"AS {OP}, named_struct('v', _commit_version, 'o', CASE _change_type WHEN 'delete' THEN 0 ELSE 1 END) "
                f"AS {SEQ}, _change_type, _commit_timestamp FROM table_changes('{name}', {_bound(after, True)}{end}) "
                "WHERE _change_type != 'update_preimage'")

    def watermark(self, session, kind: str = KIND) -> Position:
        """Its latest commit, as a version or (kind commit_ts) its time."""
        col = "max(version)" if kind == KIND else "date_format(max(timestamp), \"yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX\")"
        rows = session.sql(f"SELECT {col} AS v FROM (DESCRIBE HISTORY {self.fq})").collect()
        return Position(kind, str(rows[0][0]))

    def stats(self, session, after: Position | None) -> Stats:
        """Rows written since `after`, from the table's history (operation metrics): no data read."""
        where = "" if after is None else (f" WHERE timestamp > TIMESTAMP'{after.value}'" if after.kind == TS
                                         else f" WHERE version > {version(after)}")
        rows = session.sql(
            "SELECT sum(coalesce(CAST(operationMetrics['numTargetRowsInserted'] AS BIGINT), 0) "
            "+ coalesce(CAST(operationMetrics['numTargetRowsUpdated'] AS BIGINT), 0) "
            "+ coalesce(CAST(operationMetrics['numTargetRowsDeleted'] AS BIGINT), 0) "
            "+ coalesce(CAST(operationMetrics['numOutputRows'] AS BIGINT), 0)) AS r, "
            "sum(coalesce(CAST(operationMetrics['numTargetBytesAdded'] AS BIGINT), "
            "CAST(operationMetrics['numAddedBytes'] AS BIGINT), CAST(operationMetrics['numOutputBytes'] AS BIGINT), 0)) "
            "AS b, sum(coalesce(CAST(operationMetrics['numTargetFilesAdded'] AS BIGINT), "
            "CAST(operationMetrics['numAddedFiles'] AS BIGINT), CAST(operationMetrics['numFiles'] AS BIGINT), 0)) AS f "
            f"FROM (DESCRIBE HISTORY {self.fq}){where}").collect()
        r = rows[0]
        return Stats(*(int(x) if x is not None else None for x in (r[0], r[1], r[2])))


def build(session, schema: str, snapshot: list, steps: list, columns: list[str], keys: list[str], kind: str = KIND):
    """The conformance scenario as a bronze-like Delta table with its change data feed: the snapshot as one commit,
    then each step as one commit (a replacement, a key deleted and inserted at one position, as INSERT ... REPLACE
    WHERE; the rest of a step as one MERGE). (the Bronze, the positions after the snapshot and each step)."""
    from .contract import quote
    cat, sch = schema.split(".", 1)
    fq = f"{quote(cat)}.{quote(sch)}.{quote('lfc_change_source_conformance')}"
    lit = lambda v: "NULL" if v is None else (str(v) if isinstance(v, int) else "'" + str(v).replace("'", "''") + "'")  # noqa: E731
    k, v = (quote(c) for c in columns)
    session.sql(f"CREATE OR REPLACE TABLE {fq} ({k} INT, {v} STRING) "
                "TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    session.sql(f"INSERT INTO {fq} VALUES " + ", ".join(f"({lit(a)}, {lit(b)})" for a, b in snapshot))
    src = Bronze("conformance", fq, "conformance", list(keys), list(columns))
    positions = [src.watermark(session, kind)]
    for step in steps:
        replaced = {key for op, key, _ in step if op == "delete"} & {key for op, key, _ in step if op == "insert"}
        if replaced:
            rows = [(key, val) for op, key, val in step if op == "insert" and key in replaced]
            session.sql(f"INSERT INTO {fq} REPLACE WHERE {k} IN ({', '.join(lit(x) for x in replaced)}) VALUES "
                        + ", ".join(f"({lit(a)}, {lit(b)})" for a, b in rows))
        rest = [(op, key, val) for op, key, val in step if key not in replaced]
        if rest:
            vals = ", ".join(f"({lit(op)}, {lit(key)}, {lit(val)})" for op, key, val in rest)
            session.sql(f"MERGE INTO {fq} t USING (SELECT * FROM VALUES {vals} AS v(op, {k}, {v})) s ON t.{k} = s.{k} "
                        f"WHEN MATCHED AND s.op = 'delete' THEN DELETE WHEN MATCHED THEN UPDATE SET t.{v} = s.{v} "
                        f"WHEN NOT MATCHED AND s.op != 'delete' THEN INSERT ({k}, {v}) VALUES (s.{k}, s.{v})")
        positions.append(src.watermark(session, kind))
    return src, positions
