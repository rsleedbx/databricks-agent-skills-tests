"""lfc_source_contract.psql: a DB-API cursor over psql where only a shell reaches the database (a PostgreSQL in a
container on a VM, from the laptop), so cdc.publication and cdc.slot run there unchanged:

    from lfc_source_contract import cdc, psql
    cur = psql.cursor(run, "podman exec -i <container> psql -U postgres -d tpch1")   # run(command) -> output
    cdc.publication(cur, "dbz_pub", ["tpch1"], only=["orders", "lfc_added"])

Each execute() is one psql call in autocommit (-v ON_ERROR_STOP=1: a failed statement raises); %s parameters are
filled in as SQL literals (jdbc.literal), and the rows come back unaligned and tab-separated, every value as text.
"""
from __future__ import annotations

from .jdbc import literal


class Cursor:
    def __init__(self, run, psql: str):
        self.run, self.psql, self._rows = run, psql, []
        self.connection = None   # cdc._autocommit: every psql call commits by itself

    def execute(self, sql: str, params=None) -> None:
        if params:
            parts = sql.split("%s")
            if len(parts) != len(params) + 1:
                raise ValueError(f"psql: {len(params)} parameters for {len(parts) - 1} placeholders")
            sql = parts[0] + "".join(literal(p) + rest for p, rest in zip(params, parts[1:]))
        out = self.run(f"{self.psql} -X -At -F $'\\t' -v ON_ERROR_STOP=1 <<'LFC_SQL'\n{sql};\nLFC_SQL\n")
        self._rows = [tuple(line.split("\t")) for line in out.splitlines() if line != ""]

    def fetchall(self) -> list:
        return list(self._rows)


def cursor(run, psql: str) -> Cursor:
    return Cursor(run, psql)
