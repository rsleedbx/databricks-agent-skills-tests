"""gateway_staging: a Lakeflow Connect gateway's staging volume as a change source, read-only: the files the gateway
writes into the customer's own volume, read directly, with no ingestion pipeline in between. One folder for every table
of the gateway (shared). The layout (lfc-medallion-pack's references/gateway-staging.md):

  metadata/table_schemas/<table id>/target_schema.<n>.json   each table's columns: name, type, and the column id
  snapshot/<table id>/<snapshot time>/part_*.parquet         the snapshot, columns by id, with __sequence: its cursor
                                                             (empty for SQL Server change tracking: the change files
                                                             start before the snapshot); also qbc_checkpoint/ and
                                                             _spark_metadata/, not parquet: reads take *.parquet only
  cdc/<producer start>/<segment>                             JSON lines, one change each, every table's: table_id,
                                                             cursor.sequence, operation (INSERT, UPDATE, DELETE), data
                                                             (columns by id)

Positions are the gateway's cursor (`cursor.sequence`, text of fixed width: <commit LSN>-<counter>, so it sorts in
commit order). A snapshot's rows carry the cursor it was taken at, so the changes after a snapshot are exactly those
with a larger sequence. The newest snapshot of a table is the one read; target_schema.<n> with the largest n is the
table's current schema.

    from lfc_change_source import gateway_staging as gs
    tables = gs.schemas(spark, root)                  # one read of every table's schema file
    src = gs.Staging("lob_001", root, tables[("febar", "scale1_lob_001", "account")])
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

from .contract import DELETE, OP, SEQ, UPSERT, Position, Stats, quote

KIND = "cursor"
TYPES = {"INTEGER": "INT", "LONG": "BIGINT", "SHORT": "SMALLINT", "BYTE": "TINYINT"}


def sql_type(t: str) -> str:
    """A schema file's column type as a SQL type (DECIMAL(12, 2) -> DECIMAL(12,2), INTEGER -> INT)."""
    t = re.sub(r"\s+", "", t.upper())
    return TYPES.get(t, t)


def schemas(session, root: str) -> dict:
    """{(catalog, schema, table): {"id", "columns": [(name, SQL type, column id)], "keys"}}: every table's current
    schema file (the largest n), in one read of the metadata folder."""
    rows = session.sql(f"SELECT _metadata.file_path AS p, value FROM read_files('{root}/metadata/table_schemas/', "
                       "format => 'text', wholeText => true, recursiveFileLookup => true)").collect()
    latest: dict = {}
    for r in rows:
        m = re.search(r"/table_schemas/([^/]+)/target_schema\.(\d+)\.json$", r.p)
        if m and int(m.group(2)) >= latest.get(m.group(1), (-1, None))[0]:
            latest[m.group(1)] = (int(m.group(2)), r.value)
    out = {}
    for tid, (_, text) in latest.items():
        for s in json.loads(text)["schemas"]:
            for t in s["tables"]:
                out[(s["catalog"], s["schema"], t["name"])] = {
                    "id": t["uoid"], "keys": list((t.get("primaryKey") or {}).get("columns") or []),
                    "columns": [(c["name"], sql_type(c["type"]), c["uoid"]) for c in t["columns"]]}
    return out


@dataclass
class Staging:
    name: str              # the source (the n:1 table's source column)
    root: str              # the gateway's staging folder: /Volumes/.../<gateway id>
    entry: dict            # schemas()'s entry for the table
    table: str = ""
    keys: list[str] = field(default_factory=list)
    shared: bool = True

    def __post_init__(self):
        self.keys = self.keys or self.entry["keys"]
        self.columns = [c for c, _, _ in self.entry["columns"]]

    def _cols(self) -> str:
        return ", ".join(f"data.{quote(cid)}::{t} AS {quote(c)}" for c, t, cid in self.entry["columns"])

    @staticmethod
    def _seq(p: Position | None) -> str | None:
        if p is None:
            return None
        if p.kind != KIND:
            raise ValueError(f"a gateway staging position is a {KIND}, not {p.kind}")
        return p.value.replace("'", "")

    def snapshot_sql(self, at: Position | None = None) -> str:
        """The table's newest snapshot (its rows carry the cursor it was taken at: snapshot_position)."""
        cols = ", ".join(f"{quote(cid)}::{t} AS {quote(c)}" for c, t, cid in self.entry["columns"])
        return (f"SELECT {cols}, '{UPSERT}' AS {OP} FROM read_files('{self.root}/snapshot/{self.entry['id']}/', "
                "format => 'parquet', pathGlobFilter => '*.parquet') WHERE _metadata.file_path LIKE concat('%/', (SELECT max(regexp_extract("
                f"_metadata.file_path, '/snapshot/{self.entry['id']}/([0-9]+)/', 1)) FROM read_files("
                f"'{self.root}/snapshot/{self.entry['id']}/', format => 'parquet', pathGlobFilter => '*.parquet')), '/%')")

    def snapshot_position(self, session) -> Position:
        """The cursor the newest snapshot was taken at: the changes after it start there."""
        rows = session.sql(f"SELECT max(__sequence) FROM read_files('{self.root}/snapshot/{self.entry['id']}/', "
                           "format => 'parquet', pathGlobFilter => '*.parquet')").collect()
        return Position(KIND, str(rows[0][0]))

    def changes_sql(self, after: Position | None, to: Position | None) -> str:
        """The table's changes after `after` up to `to` (None: from the first, to the latest); _seq: the cursor, then
        a delete before an insert at one cursor (an update of a keyless table is a DELETE and an INSERT)."""
        lo, hi = self._seq(after), self._seq(to)
        where = [f"table_id.uoid = '{self.entry['id']}'"] + ([f"cursor.sequence > '{lo}'"] if lo else []) + \
                ([f"cursor.sequence <= '{hi}'"] if hi else [])
        return (f"SELECT {self._cols()}, CASE operation WHEN 'DELETE' THEN '{DELETE}' ELSE '{UPSERT}' END AS {OP}, "
                f"named_struct('v', cursor.sequence, 'o', CASE operation WHEN 'DELETE' THEN 0 ELSE 1 END) AS {SEQ} "
                f"FROM read_files('{self.root}/cdc/', format => 'json') WHERE {' AND '.join(where)}")

    def watermark(self, session, kind: str | None = None) -> Position:
        """The newest cursor in any change file, or the table's newest snapshot when that's later (a snapshot taken
        after the last change): the gateway writes in commit order, so every earlier change of every table is in."""
        rows = session.sql(f"SELECT max(cursor.sequence) FROM read_files('{self.root}/cdc/', format => 'json')"
                           ).collect()
        newest = str(rows[0][0] or "")
        try:
            newest = max(newest, self.snapshot_position(session).value)
        except Exception:  # noqa: BLE001  no snapshot folder
            pass
        return Position(KIND, newest)

    def stats(self, session, after: Position | None) -> Stats:
        lo = self._seq(after)
        cut = f" AND cursor.sequence > '{lo}'" if lo else ""
        rows = session.sql(f"SELECT count(*), count(DISTINCT _metadata.file_path) FROM read_files('{self.root}/cdc/', "
                           f"format => 'json') WHERE table_id.uoid = '{self.entry['id']}'{cut}").collect()
        return Stats(int(rows[0][0]), None, int(rows[0][1]))


def _cursor(n: int, i: int = 0) -> str:
    """A cursor of the gateway's form: <LSN>-<counter>, fixed width."""
    return f"00000000/{n:08X}-{i:019x}"


def build(session, schema: str, snapshot: list, steps: list, columns: list[str], keys: list[str]):
    """The conformance scenario as a staging folder in a scratch volume (<schema>.lfc_change_source): a schema file,
    the snapshot as parquet (its __sequence the first cursor), then each step's changes as one change file, one
    cursor a step (a replacement: a DELETE and an INSERT at one cursor). Needs a session with a workspace client
    (session.w: lfc_change_source.engine.Warehouse) for the files."""
    cat, sch = schema.split(".", 1)
    session.sql(f"CREATE VOLUME IF NOT EXISTS {quote(cat)}.{quote(sch)}.lfc_change_source")
    root = f"/Volumes/{cat}/{sch}/lfc_change_source/staging"
    k, v = columns
    tid, cid = "T_conf", {k: "C_conf_1", v: "C_conf_2"}
    w = session.w
    try:
        _clear(w, root)   # a rerun starts empty
    except Exception:  # noqa: BLE001  not there yet
        pass
    target = {"schemas": [{"catalog": "conformance", "schema": "public", "tables": [{
        "name": "t", "uoid": tid, "primaryKey": {"columns": list(keys)},
        "columns": [{"name": k, "type": "INTEGER", "uoid": cid[k]}, {"name": v, "type": "STRING", "uoid": cid[v]}]}]}]}
    _put(w, f"{root}/metadata/table_schemas/{tid}/target_schema.0.json", json.dumps(target))
    snap_rows = " UNION ALL ".join(f"SELECT {a} AS {cid[k]}, '{b}' AS {cid[v]}, '{_cursor(0)}' AS __sequence, "
                                   "'INSERT' AS __operation" for a, b in snapshot)
    session.sql(f"INSERT OVERWRITE DIRECTORY '{root}/snapshot/{tid}/1000/' USING parquet {snap_rows}")
    for n, step in enumerate(steps, start=1):
        lines = []
        for i, (op, key, val) in enumerate(step):
            data = {cid[k]: key} | ({} if op == "delete" else {cid[v]: val})
            lines.append(json.dumps({"table_id": {"uoid": tid, "catalog": "conformance", "schema": "public",
                                                  "name": "t"}, "operation": op.upper(),
                                     "cursor": {"sequence": _cursor(n)}, "data": data}))
        _put(w, f"{root}/cdc/1/{n}", "\n".join(lines) + "\n")
    src = Staging("conformance", root, schemas(session, root)[("conformance", "public", "t")])
    return src, [Position(KIND, _cursor(n)) for n in range(len(steps) + 1)]


def _put(w, path: str, text: str) -> None:
    import io
    w.files.upload(path, io.BytesIO(text.encode()), overwrite=True)


def _clear(w, root: str) -> None:
    """Remove a scratch staging folder's files, so a rerun starts empty."""
    for e in list(w.files.list_directory_contents(root)):
        if e.is_directory:
            _clear(w, e.path)
            w.files.delete_directory(e.path)
        else:
            w.files.delete(e.path)
