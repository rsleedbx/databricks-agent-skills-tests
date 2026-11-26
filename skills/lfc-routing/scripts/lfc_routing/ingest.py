"""ingest: routing's steps 6 and 7 (docs/design-intelligent.md, "The steps"): the real change capture and snapshots,
from the latest plan, after a person confirms it by adding a row to ingestion_start (n, source, by, at, note).
run_ingestion() carries out each pending request through the source's producer (lfc_change_source.producer: a
Debezium Server, a gateway) and appends ingestion_runs:

  6  the logs the plan names (snapshots.logs_of): a producer that lands (a gateway, land(log, only)) lands each log's
     tables into it, a split being only that; one that writes its log itself (Debezium) is split make before break
     (producer.split) for each table with a log of its own
  7  each table's snapshot: the producer's own copied first (copy_snapshots, a gateway's), recorded in snapshot_runs;
     then keep or take (snapshots.keep_or_take, against the first position each of its logs holds); take asks the
     producer for a new one (resnapshot) when it can

    from lfc_routing import ingest
    ingest.run_ingestion(session, "`c`.`s`", "pg1", producer, shared_log="`c`.`s`.`cdc_log`", prefix="lfcddemo.lfcddemo.")
"""
from __future__ import annotations

import json

from . import snapshots

REQUEST, RUNS = "ingestion_start", "ingestion_runs"


def ddl(schema: str) -> list:
    return [f"CREATE TABLE IF NOT EXISTS {schema}.{REQUEST} (n INT, source STRING, `by` STRING, at TIMESTAMP, note STRING) "
            "TBLPROPERTIES (delta.enableChangeDataFeed = true) "
            "COMMENT 'lfc_routing: add a row (n + 1, the source) when the plan is confirmed: the real ingestion runs'",
            f"CREATE TABLE IF NOT EXISTS {schema}.{RUNS} (run INT, source STRING, at TIMESTAMP, status STRING, "
            "error STRING, plan_round INT, logs STRING, snapshots STRING) "
            "COMMENT 'lfc_routing: each real ingestion carried out: its logs and each table''s snapshot kept or taken'"]


def _lit(v) -> str:
    return "NULL" if v is None else "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _dicts(rs) -> list:
    return [r.asDict() if hasattr(r, "asDict") else dict(r) for r in rs]


def plan_of(session, schema: str) -> tuple:
    """(round, {tbl: {"route": {"log", "cell", "snapshot_by"}}}) of the latest round in routing_plan."""
    rows = _dicts(session.sql(f"SELECT * FROM {schema}.routing_plan WHERE round = (SELECT max(round) FROM "
                              f"{schema}.routing_plan)").collect())
    if not rows:
        raise ValueError("routing_plan has no round yet (routing_iteration first)")
    return int(rows[0]["round"]), {r["tbl"]: {"route": {"log": r["log"], "cell": r["cell"],
                                                         "snapshot_by": r["snapshot_by"]}} for r in rows}


def first_positions(session, logs: dict, prefix: str, kind_sql: str = "value:cursor.sequence::STRING") -> dict:
    """{tbl: {log: the first position it holds for the table, or None}} for {log: [tbl]}."""
    out: dict = {}
    for log, tbls in logs.items():
        got = {}
        try:
            got = {r["destination"][len(prefix):]: r["p"] for r in _dicts(session.sql(
                f"SELECT destination, min({kind_sql}) AS p FROM {log} WHERE destination IN "
                f"({', '.join(_lit(prefix + t) for t in tbls)}) GROUP BY destination").collect())}
        except Exception as e:  # noqa: BLE001  a new log not made yet holds nothing
            if "TABLE_OR_VIEW_NOT_FOUND" not in str(e):
                raise
        for t in tbls:
            out.setdefault(t, {})[log] = got.get(t)
    return out


def record_snapshots(session, schema: str, source: str, copied: list, method: str = "transport") -> None:
    for c in copied:
        session.sql(f"INSERT INTO {schema}.{snapshots.RUNS} VALUES ({_lit(source)}, {_lit(c['tbl'])}, current_timestamp(), "
                    f"{_lit(method)}, 'ok', NULL, {_lit(c['table'])}, {_lit(c.get('kind'))}, {_lit(c['position'])}, "
                    f"{c.get('rows') or 'NULL'}, NULL, NULL)").collect()


def run_ingestion(session, schema: str, source: str, producer, shared_log: str, prefix: str,
                  snapshot_schema: str | None = None, make=None) -> list:
    """Each pending ingestion_start row of `source` carried out (module doc). make(Spec) -> Producer: for a split of a
    producer that writes its own log (Debezium). The runs' rows."""
    from lfc_change_source import producer as prod
    reqs = [r for r in _dicts(session.sql(f"SELECT * FROM {schema}.{REQUEST} WHERE source = {_lit(source)}").collect())]
    done = {int(r["run"]) for r in _dicts(session.sql(f"SELECT run FROM {schema}.{RUNS} WHERE source = {_lit(source)}").collect())}
    out = []
    for req in sorted((r for r in reqs if int(r["n"]) not in done), key=lambda r: int(r["n"])):
        row = {"run": int(req["n"]), "source": source}
        try:
            rnd, plan = plan_of(session, schema)
            logs = snapshots.logs_of({"tables": plan}, shared_log)
            if hasattr(producer, "land"):          # a gateway: one staging, a log a landing
                landed = {log: producer.land(log, [prefix + t for t in tbls]) for log, tbls in logs.items()}
            else:                                  # writes its log itself: a split for each log of its own
                landed = {}
                for log, tbls in logs.items():
                    if log != shared_log:
                        steps = prod.split(prod.Spec(producer.name, producer.log, producer.tables),
                                           [f"{prefix.split('.', 1)[-1]}{t}" for t in tbls], log, make)
                        landed[log] = prod.carry_out(steps, landed=lambda p: True)
            copied = producer.copy_snapshots(snapshot_schema) if hasattr(producer, "copy_snapshots") and snapshot_schema else []
            record_snapshots(session, schema, source, copied)
            runs = {r["tbl"]: r for r in _dicts(session.sql(
                f"SELECT * FROM {schema}.{snapshots.RUNS} WHERE source = {_lit(source)} AND status = 'ok' "
                "QUALIFY row_number() OVER (PARTITION BY tbl ORDER BY at DESC) = 1").collect())}
            firsts = first_positions(session, logs, prefix)
            verdicts = {t: snapshots.keep_or_take(runs.get(t), firsts.get(t, {})) for t in plan}
            for t, v in verdicts.items():
                if not v["keep"] and hasattr(producer, "resnapshot"):
                    producer.resnapshot(t)
                    v["asked"] = True
            row.update(status="ok", plan_round=rnd, logs={k: {"tables": v, "landed": landed.get(k)} for k, v in logs.items()},
                       snapshots=verdicts)
        except Exception as e:  # noqa: BLE001  recorded as a failed run, with why
            row.update(status="failed", error=f"{type(e).__name__}: {str(e)[:500]}")
        session.sql(f"INSERT INTO {schema}.{RUNS} VALUES ({row['run']}, {_lit(source)}, current_timestamp(), "
                    f"{_lit(row['status'])}, {_lit(row.get('error'))}, {row.get('plan_round') or 'NULL'}, "
                    f"{_lit(json.dumps(row.get('logs'), default=str))}, {_lit(json.dumps(row.get('snapshots')))})").collect()
        out.append(row)
    return out
