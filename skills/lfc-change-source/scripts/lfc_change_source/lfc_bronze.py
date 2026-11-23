"""lfc_bronze: a Lakeflow Connect bronze table (the ingestion pipeline's streaming table, one per source and table) as
a change source. Positions are the table's Delta versions; changes come from its change data feed.

    from lfc_change_source.lfc_bronze import Bronze
    src = Bronze("lob001", "`main`.`sales_bronze_lob001`.`account`", "account", keys=["id"], columns=["id", "v"])
    src.changes_sql(Position("delta_version", "12"), Position("delta_version", "15"))
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .contract import DELETE, OP, SEQ, UPSERT, Position, Stats, quote

KIND = "delta_version"


def version(p: Position | None) -> int | None:
    if p is None:
        return None
    if p.kind != KIND:
        raise ValueError(f"a bronze table's position is a {KIND}, not {p.kind}")
    return int(p.value)


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
        v = version(at)
        pin = f" VERSION AS OF {v}" if v is not None else ""
        return f"SELECT {self.select(data)}, '{UPSERT}' AS {OP} FROM {self.fq}{pin}"

    def changes_sql(self, after: Position, to: Position, data: str | None = None) -> str:
        """Its changes after `after` up to `to`, update pre-images dropped; _seq: the commit version, then a delete
        before an insert of the same key in one version (a replacement: the insert wins). Also _change_type."""
        name = self.fq.replace("`", "")
        return (f"SELECT {self.select(data)}, CASE _change_type WHEN 'delete' THEN '{DELETE}' ELSE '{UPSERT}' END "
                f"AS {OP}, named_struct('v', _commit_version, 'o', CASE _change_type WHEN 'delete' THEN 0 ELSE 1 END) "
                f"AS {SEQ}, _change_type FROM table_changes('{name}', {version(after) + 1}, {version(to)}) "
                "WHERE _change_type != 'update_preimage'")

    def watermark(self, session) -> Position:
        rows = session.sql(f"SELECT max(version) AS v FROM (DESCRIBE HISTORY {self.fq})").collect()
        return Position(KIND, str(rows[0][0]))

    def stats(self, session, after: Position | None) -> Stats:
        """Rows written since `after`, from the table's history (operation metrics): no data read."""
        v = version(after)
        where = f" WHERE version > {v}" if v is not None else ""
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


def build(session, schema: str, snapshot: list, steps: list, columns: list[str], keys: list[str]):
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
    positions = [src.watermark(session)]
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
        positions.append(src.watermark(session))
    return src, positions
