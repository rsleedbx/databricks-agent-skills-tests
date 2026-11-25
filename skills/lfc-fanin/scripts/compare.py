"""compare.py <warehouse id> <profile> <catalog> <silver schema A> <silver schema B>: every silver table in B against
the same table in A (count and a hash of every row; the change log, state and pipeline views left out), as JSON.

    python3 -I skills/lfc-fanin/scripts/compare.py <warehouse> <profile> main dbz_run_schema fanin_schema
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "lfc-change-source", "scripts"))
from databricks.sdk import WorkspaceClient  # noqa: E402
from lfc_change_source.engine import Warehouse  # noqa: E402


def internal(name: str) -> bool:
    return (name.startswith("_") or name.startswith("cdc_log") or name == "schema_drift" or name.endswith("_snapshot")
            or name.endswith("_changes"))


def compare(s, w, cat: str, a: str, b: str) -> dict:
    out = {}
    for t in sorted(x.name for x in w.tables.list(cat, b) if not internal(x.name)):
        def agg(x):
            cols = [r[0] for r in s.sql(f"DESCRIBE `{cat}`.`{x}`.`{t}`").collect() if r[0] and not r[0].startswith("#")]
            e = ", ".join(f"`{c}`" for c in cols)
            return list(s.sql(f"SELECT count(*), CAST(sum(CAST(xxhash64({e}) AS DECIMAL(38,0))) AS STRING) "
                              f"FROM `{cat}`.`{x}`.`{t}`").collect()[0])
        try:
            ra, rb = agg(a), agg(b)
            out[t] = {"a": ra, "b": rb, "equal": ra == rb}
        except Exception as e:  # noqa: BLE001  recorded per table, and counted as not equal
            out[t] = {"error": str(e)[:200], "equal": False}
    return {"tables": len(out), "equal": sum(v["equal"] for v in out.values()), "by_table": out}


if __name__ == "__main__":
    wh, profile, cat, a, b = sys.argv[1:6]
    w = WorkspaceClient(profile=profile)
    print(json.dumps(compare(Warehouse(wh, w), w, cat, a, b)))
