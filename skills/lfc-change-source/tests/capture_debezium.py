"""capture_debezium.py: real Debezium events from a kept zerobus-debezium-test run's change log, each with the row it
decodes to (that run's silver, checked equal to the source), as tests/samples/debezium/<engine>.jsonl: a create and an
update a table, each its key's last event, so the silver row is that event's, and a delete, its key what it decodes to.
Not an offline test.

    PROFILE=<p> WAREHOUSE=<id> CATALOG=<c> python3 -I capture_debezium.py <engine> <run schema> [<table> ...]
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
from databricks.sdk import WorkspaceClient  # noqa: E402

from lfc_change_source.engine import Warehouse  # noqa: E402


def schema_of(w, cat: str, run: str) -> dict:
    """{table: canonical columns} from the run's Volume (_schema/schema.yaml): the types the events decode to."""
    import yaml
    d = yaml.safe_load(w.files.download(f"/Volumes/{cat}/{run}/snapshot/_schema/schema.yaml").contents.read())
    return {t["name"]: [{k: c[k] for k in ("name", "type", "precision", "scale", "primary_key") if k in c}
                        for c in t["columns"]] for t in d["tables"]}


def capture(wh, cat: str, run: str, tables: list, schema: dict) -> list:
    q = lambda s: [r.asDict() if hasattr(r, "asDict") else r for r in wh.sql(s).collect()]  # noqa: E731
    out = []
    for t in tables:
        log, silver = f"`{cat}`.`{run}`.`cdc_log`", f"`{cat}`.`{run}`.`{t}`"
        # each key's last event, then one of each operation
        rows = q(f"SELECT destination, operation, key::STRING AS k, value::STRING AS v FROM (SELECT *, ROW_NUMBER() OVER "
                 f"(PARTITION BY key::STRING ORDER BY value:ts_ns::BIGINT DESC) AS n FROM {log} WHERE "
                 f"substring_index(destination, '.', -1) = '{t}') WHERE n = 1 "
                 "QUALIFY ROW_NUMBER() OVER (PARTITION BY operation ORDER BY k) = 1")
        # a delete need not be its key's last event (a key deleted, then inserted again): only its key decodes
        rows += q(f"SELECT destination, operation, key::STRING AS k, value::STRING AS v FROM {log} WHERE "
                  f"substring_index(destination, '.', -1) = '{t}' AND operation = 'delete' ORDER BY key::STRING LIMIT 1")
        rows = [r for i, r in enumerate(rows) if r["operation"] != "delete" or i == len(rows) - 1]
        cols = [c[0] for c in wh.sql(f"DESCRIBE {silver}").collect() if c[0] and not c[0].startswith(("#", "_"))]
        for r in rows:
            d = r if isinstance(r, dict) else dict(zip(("destination", "operation", "k", "v"), r))
            key = json.loads(d["k"])
            want = {c: str(v) for c, v in key.items()} if d["operation"] == "delete" else None   # its key
            if d["operation"] != "delete":
                where = " AND ".join(f"`{c}` = {json.dumps(v) if isinstance(v, str) else v}".replace('"', "'")
                                     for c, v in key.items())
                got = wh.sql(f"SELECT {', '.join(f'CAST(`{c}` AS STRING)' for c in cols)} FROM {silver} WHERE {where}").collect()
                want = dict(zip(cols, got[0])) if got else None
            out.append({"table": t, "destination": d["destination"], "operation": d["operation"], "key": key,
                        "value": json.loads(d["v"]), "expected": want, "columns": schema[t]})
    return out


if __name__ == "__main__":
    engine, run, *tables = sys.argv[1:]
    w = WorkspaceClient(profile=os.environ["PROFILE"])
    from lfc_change_source._rest import Rest, target_catalog  # noqa: E402
    cat = target_catalog(Rest.connect(os.environ["PROFILE"]), os.environ.get("CATALOG"))
    got = capture(Warehouse(os.environ["WAREHOUSE"], w), cat, run, tables or ["lineitem", "orders"], schema_of(w, cat, run))
    path = os.path.join(HERE, "samples", "debezium", f"{engine}.jsonl")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.writelines(json.dumps(x, sort_keys=True) + "\n" for x in got)
    print(f"{path}: {len(got)} events ({', '.join(sorted({x['operation'] for x in got}))})")
