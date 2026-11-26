"""provisional: routing's step 2 (docs/design-intelligent.md, "The steps"): every table of a source's latest
introspection into one change log, to observe, started again when the introspection or a person says so. A person asks
by adding a row to cdc_request (n, source, log, by, at, note); run_cdc_requests() runs the pending ones where the
producer can be reached (a Debezium Server on a VM: from a machine that reaches it; a gateway: a job), through
lfc_change_source.producer (reconcile, then start or restart), and appends cdc_runs.

    from lfc_routing import provisional
    provisional.run_cdc_requests(session, "`c`.`s`", make=lambda spec: DebeziumServer(spec, ...), running=lambda src: spec_or_None)
"""
from __future__ import annotations

import json

from lfc_change_source import producer
from lfc_introspection import history

REQUEST, RUNS = "cdc_request", "cdc_runs"


def ddl(schema: str) -> list:
    return [f"CREATE TABLE IF NOT EXISTS {schema}.{REQUEST} (n INT, source STRING, log STRING, producer STRING, "
            "`by` STRING, at TIMESTAMP, note STRING) TBLPROPERTIES (delta.enableChangeDataFeed = true) "
            "COMMENT 'lfc_routing: add a row (n + 1, the source) to start or restart its provisional change capture'",
            f"CREATE TABLE IF NOT EXISTS {schema}.{RUNS} (run INT, source STRING, at TIMESTAMP, action STRING, "
            "status STRING, error STRING, log STRING, tables INT, added STRING, dropped STRING, introspection_run INT) "
            "COMMENT 'lfc_routing: each start or restart of a source''s provisional change capture'"]


def tables_of(schema_doc: dict, src_schema: str) -> list:
    """The connector's names (schema.table) of an introspection's tables."""
    return [f"{src_schema}.{t['name']}" for t in schema_doc.get("tables") or []]


def run_cdc_requests(session, schema: str, make, running, introspection_schema: str | None = None) -> list:
    """Each pending cdc_request carried out: the source's latest introspection's tables reconciled with what its
    producer carries (running(source) -> producer.Spec or None), then started or restarted (make(Spec) -> Producer).
    A failure is a run too. The runs' rows."""
    q = lambda s: [r.asDict() if hasattr(r, "asDict") else dict(r) for r in session.sql(s).collect()]  # noqa: E731
    reqs = q(f"SELECT * FROM {schema}.{REQUEST}")
    done = {(r["source"], int(r["run"])) for r in q(f"SELECT run, source FROM {schema}.{RUNS}")}
    out = []
    for r in sorted((r for r in reqs if (r["source"], int(r["n"])) not in done), key=lambda r: int(r["n"])):
        row = {"run": int(r["n"]), "source": r["source"], "log": r["log"]}
        try:
            got = q(f"SELECT run, src_schema FROM {introspection_schema or schema}.{history.RUNS} r JOIN "
                    f"{introspection_schema or schema}.{history.REQUEST} q ON q.n = r.run AND q.source = r.source "
                    f"WHERE r.source = {history._lit(r['source'])} AND r.status = 'ok' ORDER BY r.run DESC LIMIT 1")
            if not got:
                raise ValueError(f"{r['source']}: no introspection run yet (introspection_request first)")
            docs = history.latest(session, introspection_schema or schema, r["source"], got[0]["run"])
            want = tables_of(docs[0], got[0]["src_schema"])
            plan = producer.reconcile(running(r["source"]), want, r.get("producer") or r["source"], r["log"])
            if plan["action"] != "none":
                p = make(plan["spec"])
                if plan["action"] == "restart":
                    p.stop()
                p.start()
            row.update(action=plan["action"], status="ok", tables=len(want), added=list(plan["added"]),
                       dropped=list(plan["dropped"]), introspection_run=got[0]["run"])
        except Exception as e:  # noqa: BLE001  recorded as a failed run, with why
            row.update(action=None, status="failed", error=f"{type(e).__name__}: {str(e)[:500]}")
        L = history._lit
        session.sql(f"INSERT INTO {schema}.{RUNS} VALUES ({row['run']}, {L(row['source'])}, current_timestamp(), "
                    f"{L(row.get('action'))}, {L(row['status'])}, {L(row.get('error'))}, {L(row['log'])}, "
                    f"{L(row.get('tables'))}, {L(json.dumps(row.get('added', [])))}, "
                    f"{L(json.dumps(row.get('dropped', [])))}, {L(row.get('introspection_run'))})").collect()
        out.append(row)
    return out
