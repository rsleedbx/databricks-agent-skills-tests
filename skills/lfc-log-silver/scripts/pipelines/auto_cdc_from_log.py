# A Lakeflow pipeline source: a streaming table per source table, kept by an AUTO CDC flow (SCD type 1) from the shared
# change log (lfc_change_source), all tables in one pipeline update. Configuration:
#   cdc.logs     the change log tables, comma separated (a table moved between logs: each log's events are read;
#                positions order them)
#   cdc.tables   a JSON file (workspace path): [{"destination", "target", "keys": [...], "exprs": [...], "seq": ...}]:
#                each column's SQL from an event and the position's SQL (lfc_log_silver.silver.auto_cdc_tables writes it
#                from any lfc_change_source source)
#   cdc.package  the folder holding lfc_change_source, a workspace path
import json
import sys

from pyspark import pipelines as dp

conf = spark.conf  # noqa: F821  spark: the pipeline's session
sys.path.insert(0, conf.get("cdc.package"))
from lfc_change_source.contract import DELETE, OP, SEQ, UPSERT  # noqa: E402

LOGS = [x.strip() for x in conf.get("cdc.logs").split(",") if x.strip()]
with open(conf.get("cdc.tables")) as f:
    TABLES = json.load(f)


def changes(t: dict):
    """One table's events from every log, its columns typed, with _op and _seq (the gateway's cursor, then a delete
    before an insert at one cursor); snapshot rows (operation read) are upserts that sort first (cursor '' or the
    snapshot's own)."""
    from functools import reduce
    cols = t["exprs"]
    op = f"CASE operation WHEN 'delete' THEN '{DELETE}' ELSE '{UPSERT}' END AS {OP}"
    seq = f"named_struct('v', {t['seq']}, 'o', CASE operation WHEN 'delete' THEN 0 ELSE 1 END) AS {SEQ}"
    dest = t["destination"].replace("'", "")
    parts = [spark.readStream.table(log).where(  # noqa: F821
        f"destination = '{dest}' AND operation <> 'resolved'").selectExpr(*cols, op, seq) for log in LOGS]
    return reduce(lambda a, b: a.unionByName(b), parts)


def define(t: dict) -> None:
    view = f"_v_{t['target']}"
    dp.view(name=view)(lambda: changes(t))
    dp.create_streaming_table(name=t["target"])
    dp.create_auto_cdc_flow(target=t["target"], source=view, keys=t["keys"], sequence_by=SEQ,
                            apply_as_deletes=f"{OP} = '{DELETE}'", except_column_list=[OP, SEQ],
                            stored_as_scd_type=1)


for table in TABLES:
    define(table)
