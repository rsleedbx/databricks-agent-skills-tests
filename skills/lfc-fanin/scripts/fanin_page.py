"""fanin_page.py <results dir> <source> [<docs dir>]: the docs page of one database's fan-in test, from fanin_test.py's
results: docs/database-connect/<engine>/debezium-zerobus.md (what was tested, the result, your side by
zerobus-debezium-test, the fan-in bundle's deploy). Refuses a test that didn't pass end to end.

    python3 -I skills/lfc-fanin/scripts/fanin_page.py ~/.lfc-test/results/fanin pg
"""
import json
import os
import re
import sys

ENGINES = {  # SOURCE: (docs folder, name, source_type)
    "mssql": ("sqlserver", "SQL Server", "SQLSERVER"), "pg": ("postgresql", "PostgreSQL", "POSTGRESQL"),
    "mysql": ("mysql", "MySQL", "MYSQL"), "oracle": ("oracle", "Oracle", "ORACLE"), "db2": ("db2", "Db2", "DB2"),
    "informix": ("informix", "Informix", "INFORMIX"), "crdb": ("cockroachdb", "CockroachDB", "COCKROACHDB"),
    "yb": ("yugabytedb", "YugabyteDB", "YUGABYTEDB"),
}


def header(md: str, key: str) -> str:
    """A `- <key>: ...` line of the skill's result file, its image digest left out."""
    m = re.search(rf"^- {re.escape(key)}: (.*)$", md, re.M)
    return re.sub(r" ?\(sha256:[0-9a-f]+\)", "", m.group(1)).strip() if m else ""


def page(res: str, src: str) -> tuple[str, str]:
    folder, name, st = ENGINES[src]
    skill = json.load(open(os.path.join(res, f"{src}-skill.json")))
    rounds = [json.loads(x) for x in open(os.path.join(res, f"{src}-rounds.jsonl")) if x.strip()]
    times = json.load(open(os.path.join(res, f"{src}-times.json")))
    md = open(os.path.join(res, f"{src}-skill.md")).read()
    ok = lambda r: r["fanin_job"].startswith("TERMINATED SUCCESS") and r["compare"]["tables"] and \
        r["compare"]["equal"] == r["compare"]["tables"]   # noqa: E731
    if skill["skill_exit"] != 0 or skill["skill_fail"] or len(rounds) < 2 or not all(ok(r) for r in rounds):
        raise SystemExit(f"{src}: not passed end to end ({skill}, rounds {rounds}): no page")
    cmp_ = rounds[-1]["compare"]
    sk, fa = times.get("skill") or [], times.get("fanin") or []
    cell = lambda u: f"{u['total_s']} ({u['running_s']})" if u else "-"   # noqa: E731
    rows = "\n".join(f"| {r['round']} | {cell(sk[i] if i < len(sk) else None)} | {cell(fa[i] if i < len(fa) else None)} | "
                     f"{r['job_s']} |" for i, r in enumerate(rounds))
    date = header(md, "Date (UTC)")
    raw = header(md, "Source")
    ver = re.search(r"\d+\.\d+(?:\.\d+)*", raw.replace(name, "", 1))
    img = re.search(r"image (\S+)", raw)
    source = f"{name} {ver.group(0) if ver else ''}".strip() + (f" ({img.group(1)})" if img else "")
    dbz = header(md, "Debezium").split(",")[0]
    base_n = cmp_["tables"] // 2   # each table and its _history
    text = f"""# {name}: create an ingestion pipeline with Debezium through Zerobus

You run Debezium Server next to {name}; its Zerobus sink writes every change into one append-only change log in
Unity Catalog. The fan-in bundle ([lfc-fanin](../../../skills/lfc-fanin/SKILL.md), `connector_type: fanin_dbz_zbus`)
builds silver from that log. Back to [database connectors](../README.md).

## Tested

{date}: {source}, and Debezium Server ({dbz}, Zerobus sink), in containers on a cloud VM, into a serverless
workspace. The source holds [TPC-H](../../tpc-models.md#tpc-h) at SF 0.001 ({base_n} tables).

| Step | Result |
|---|---|
| Your side: snapshot, Debezium, Zerobus, the change log | {skill['skill_pass']} checks passed, none failed (zerobus-debezium-test, its silver equal to the source after two rounds) |
| The fan-in bundle: `bundle deploy`, then `bundle run fanin_job` after each round | the view `cdc_log` over your log and the silver pipeline |
| The fan-in's silver, after each round | all {cmp_['tables']} tables ({base_n} tables and their `_history`) equal to the skill's, every round (row count and a hash of every row) |

Silver update times, the skill's pipeline and the fan-in's (the same pipeline code; seconds, of which its flows ran):

| Round | zerobus-debezium-test's silver | The fan-in's silver | The fan-in's job, all of it |
|---|---|---|---|
{rows}

Repeat the whole test, both sides and the check: [fanin_test.py](../../../skills/lfc-fanin/scripts/fanin_test.py)
`engine --source {src} --source-type {st}`; this page is written from its results by `fanin_page.py`.

## Your side: {name}, Debezium, Zerobus

The skill [zerobus-debezium-test](../../../skills/zerobus-debezium-test/SKILL.md) sets it up and checks it. The
fan-in needs the change log and the snapshot Volume it leaves with `KEEP=1`:

```bash
PROFILE=<cli-profile> CATALOG=<catalog> VM_IP=<vm-ip> SOURCE={src} MODEL=tpch NSRC=1 PHASE=2 KEEP=1 \\
  bash skills/zerobus-debezium-test/scripts/debezium-zerobus.sh
# prints the run's schema: the change log <catalog>.<run schema>.cdc_log, the Volume /Volumes/<catalog>/<run schema>/snapshot
```

## The fan-in: deploy

```bash
cd skills/lfc-fanin/bundle
export BUNDLE_VAR_pipeline_name=<name> BUNDLE_VAR_connector_type=fanin_dbz_zbus BUNDLE_VAR_source_type={st} \\
  BUNDLE_VAR_dest_catalog=<catalog> BUNDLE_VAR_dest_schema=<fan-in schema> \\
  BUNDLE_VAR_change_log=<catalog>.<run schema>.cdc_log BUNDLE_VAR_snapshot_volume=/Volumes/<catalog>/<run schema>/snapshot
databricks bundle deploy -p <cli-profile>
databricks bundle run fanin_job -p <cli-profile>      # each run brings silver up to date; -t scheduled: every 15 minutes
```

It creates, in `<catalog>.<fan-in schema>`: a view `cdc_log` over your log, the silver pipeline `<name>-silver` (a
table per source table, SCD type 1, and `<table>_history`, SCD type 2; `schema_drift` records values that didn't
fit), and `_fanin`. Running it again changes nothing it made, and runs one silver update.
"""
    return folder, text.replace("{rows}", rows)


if __name__ == "__main__":
    res, src = sys.argv[1], sys.argv[2]
    docs = sys.argv[3] if len(sys.argv) > 3 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..",
                                                              "..", "docs", "database-connect")
    folder, text = page(res, src)
    out = os.path.join(docs, folder, "debezium-zerobus.md")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    open(out, "w").write(text)
    print(out)
