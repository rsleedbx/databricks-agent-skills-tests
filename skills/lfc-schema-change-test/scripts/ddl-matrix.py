#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyyaml"]
# ///
"""ddl-matrix.py: the schema-change matrix. One case catalog (ddl-cases.yaml) for every engine and mechanism.

  uv run ddl-matrix.py sql <engine> <mechanism> [--setup none|utility-1.7|utility-1.7-cleanup] [--cases a,b] [--schema S] [--login L]
  uv run ddl-matrix.py bundle --connection C --catalog CAT --schema SCH --source-catalog DB [--engine E] [--mechanism M] [--mode integrated|standard] [--cases a,b] [--out databricks.yml]
  uv run ddl-matrix.py collect --pipeline ID --profile P --warehouse WH --dest CAT.SCH --env ENV.yaml [--since UTC] [--cases a,b] --out results/ddl-matrix/<name>.yaml
  uv run ddl-matrix.py collect --events-json EV.json --tables-json T.json --env ENV.yaml --out OUT.yaml     (the same, from saved data)
  uv run ddl-matrix.py convert SCRATCH.yaml --env ENV.yaml --out results/ddl-matrix/<name>.yaml             (a scratch run, one line of prose per case)
  uv run ddl-matrix.py render [FILES...] [--detail]                       the comparison tables in Markdown (default: results/ddl-matrix/*.yaml)

sql prints the SQL for the source operator to run with their own client. bundle writes the pipeline bundle, one table per case.
collect reads the pipeline's events and the destination rows and writes the run's result file; a case passes only when its
destination row is verified. convert turns a scratch run into a result file. Pipeline and warehouse IDs are never written to a result.
"""
from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
# the repo's results, plus the user's own when installed as a plugin (lfc-test-results results_dir.py)
sys.path.insert(0, str(HERE.parents[1] / "lfc-test-results" / "scripts"))
from results_dir import read_dirs

CAT = yaml.safe_load((HERE / "ddl-cases.yaml").read_text())
CASES = {c["id"]: c for c in CAT["cases"]}
ALIAS: dict[str, str] = {}
for _c in CAT["cases"]:
    for _a in [_c["id"], *_c.get("aliases", [])]:
        if ALIAS.get(_a, _c["id"]) != _c["id"]:
            sys.exit(f"ddl-cases.yaml: the alias {_a} names two cases")
        ALIAS[_a] = _c["id"]
ENGINE_ORDER = ["sqlserver", "postgres", "mysql", "oracle"]
SETUP_ORDER = {"none": 0, "utility-1.7": 1, "utility-1.7-cleanup": 2}
DEFAULT_SCHEMA = {"sqlserver": "dbo", "postgres": "public", "mysql": "", "oracle": ""}
UTILITY_URL = "https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/utility_script.sql"
STOP_RE = re.compile(r"CDC_INCOMPATIBLE_SCHEMA_CHANGES|INGESTION_GATEWAY_(?:DDL_|MISSING_TABLE|TABLE_DROPPED|UNTRACEABLE|CDC_NOT)"
                     r"|DDL_OBJECTS_MISSING|PRIMARY_KEY_CHANGE|PIPELINE_NON_RETRYABLE|SNAPSHOT_FAILED")
RESTART_RE = re.compile(r"FLOW_SCHEMA_CHANGED|CDC_APPLIER_SCHEMA_CHANGED_DURING_STREAMING|will be restarted")
CLASS_RE = re.compile(r"\b((?:CDC_INCOMPATIBLE_SCHEMA_CHANGES|INGESTION_GATEWAY_[A-Z_]+|FLOW_SCHEMA_CHANGED|CDC_[A-Z_]+|INTEGRATED_CDC_[A-Z_]+"
                      r"|PIPELINE_NON_RETRYABLE_[A-Z_]+|SNAPSHOT_FAILED_[A-Z_]+|TLS_VALIDATION_FAILURE)(?:\.[A-Z_]+)?)\b")
TYPE_WORDS = r"(bigint|integer|int|decimal\([\d,]+\)|numeric\([\d,]+\)|nvarchar\(\d+\)|varchar\(\d+\)|string|timestamp|timestamptz|date)"


def sub(text: str, **kw: str) -> str:
    """Replace {key} placeholders. Keys are plain names, so {t} and {tn} don't clash: each is replaced whole."""
    for k, v in kw.items():
        text = text.replace("{" + k + "}", v)
    return text


def tname(cid: str) -> str:
    return cid.replace("-", "_")


def resolve(ids: str | None, engine: str | None = None, mech: str | None = None) -> list[dict]:
    cases = [CASES[ALIAS[i]] for i in ids.split(",")] if ids else list(CAT["cases"])
    if engine and mech:
        cases = [c for c in cases if mech in c.get("applies", {}).get(engine, [])]
    return cases


def check_mech(engine: str, mech: str) -> None:
    if mech not in CAT["mechanisms"].get(engine, []):
        sys.exit(f"{engine} has no mechanism {mech}; one of {CAT['mechanisms'].get(engine)}")


def render_case(text: str, case: dict, engine: str, schema: str | None = None) -> str:
    q = "" if engine in ("mysql", "oracle") else (schema or DEFAULT_SCHEMA[engine]) + "."
    tn = tname(case["id"])
    return sub(text, t=q + tn, q=q, tn=tn)


def shape(case: dict, engine: str) -> str:
    kind = "keyed" if case.get("keyed", True) else "keyless"
    n = case.get("n", {}).get(engine) or CAT["default_types"]["n"][engine]
    v = case.get("v", {}).get(engine) or CAT["default_types"]["v"][engine]
    return sub(CAT["shapes"][kind][engine], n=n, v=v, tn=tname(case["id"]))


def seed_rows(case: dict) -> list[str]:
    ns = case.get("seed_n", CAT["seed_n"])
    return [f"INSERT INTO {{t}} (id, n, v) VALUES ({i}, {ns[i - 1]}, '{'ab'[i - 1]}');" for i in (1, 2)]


def setup_lines(engine: str, mech: str, setup: str, tables: list[str], schema: str, login: str,
                publication: str, slot: str) -> list[str]:
    """The source's own setup for the mechanism; the utility script's calls when the setup uses it."""
    out: list[str] = []
    listed = ",".join(f"{schema}.{t}" for t in tables)
    if engine == "sqlserver":
        if setup != "none":
            out.append(f"-- install utility script 1.7 first, as the admin login in the database: {UTILITY_URL}")
        if mech == "ct":
            out.append("ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 3 DAYS, AUTO_CLEANUP = ON);")
            if setup == "none":
                for t in tables:
                    out.append(f"ALTER TABLE {schema}.{t} ENABLE CHANGE_TRACKING;")
                    out.append(f"GRANT SELECT, VIEW CHANGE TRACKING ON {schema}.{t} TO [{login}];")
                out += [f"GRANT VIEW DEFINITION TO [{login}];", f"GRANT VIEW DATABASE STATE TO [{login}];"]
            else:
                out.append(f"EXEC dbo.lakeflowSetupChangeTracking @Tables = '{listed}', @User = '{login}', @CreateDdlSupportingObjects = 1;")
                out.append(f"EXEC dbo.lakeflowFixPermissions @User = '{login}', @Tables = 'SCHEMAS:{schema}';")
        if mech == "cdc":
            if setup == "none":
                out.append("EXEC sys.sp_cdc_enable_db;")
                for t in tables:
                    out.append(f"EXEC sys.sp_cdc_enable_table @source_schema = N'{schema}', @source_name = N'{t}', @role_name = NULL;")
                out.append(f"GRANT SELECT ON SCHEMA::{schema} TO [{login}];")
            else:
                out.append(f"EXEC dbo.lakeflowSetupChangeDataCapture @Tables = '{listed}', @User = '{login}';")
                out.append(f"EXEC dbo.lakeflowFixPermissions @User = '{login}', @Tables = 'SCHEMAS:{schema}';")
        if setup == "utility-1.7-cleanup":
            out.append("-- CLEANUP removes the utility's CDC objects; change tracking tables in the same database are affected too")
            out.append("EXEC dbo.lakeflowSetupChangeDataCapture @Mode = 'CLEANUP';")
    elif engine == "postgres":
        out.append("-- the publication and the slot are created by the table owner (the gateway didn't create them)")
        out.append(f"CREATE PUBLICATION {publication} FOR TABLE " + ", ".join(f"{schema}.{t}" for t in tables) + ";")
        out.append(f"SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput');")
    elif engine == "mysql":
        out.append("-- binlog_format ROW and binlog_row_image FULL are server settings; no per-table setup")
    elif engine == "oracle":
        out.append("-- template, not run yet: supplemental logging for LogMiner")
        out.append("ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;")
        out += [f"ALTER TABLE {t} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;" for t in tables]
    return out


def cmd_sql(a: argparse.Namespace) -> int:
    check_mech(a.engine, a.mechanism)
    cases = resolve(a.cases, a.engine, a.mechanism)
    if not cases:
        sys.exit("no case in the selection applies to this engine and mechanism")
    schema = a.schema or DEFAULT_SCHEMA[a.engine]
    end = "\nGO" if a.engine == "sqlserver" else ""
    tables = [tname(c["id"]) for c in cases]
    lines = [f"-- ddl-matrix: {a.engine} {a.mechanism}, setup {a.setup}, {len(cases)} cases. Placeholders: <login> <publication> <slot>.",
             "-- 1. tables and the two starting rows"]
    for c in cases:
        t = render_case("{t}", c, a.engine, schema)
        lines.append(f"CREATE TABLE {t} ({shape(c, a.engine)});{end}")
        if not c.get("keyed", True) and a.engine == "postgres":
            lines.append(f"ALTER TABLE {t} REPLICA IDENTITY FULL;{end}")
        lines += [render_case(r, c, a.engine, schema) + end for r in seed_rows(c)]
    lines.append("-- 2. the source's setup for the mechanism" + (f", then the {a.setup} setup" if a.setup != "none" else ""))
    for s in setup_lines(a.engine, a.mechanism, a.setup, tables, schema, a.login, a.publication, a.slot):
        lines.append(s if s.startswith("--") or not end else s + end)
    lines.append("-- 3. the cases: run one case's DDL, then its post-change insert, and let the pipeline update. "
                 "Note the UTC time before the first DDL (collect --since).")
    for c in cases:
        lines.append(f"-- case {c['id']}: {c['label']}")
        ddl = c.get("ddl", {}).get(a.engine)
        if ddl:
            lines.append(render_case(ddl, c, a.engine, schema) + end)
        else:
            lines.append("-- no DDL: the control. Start the pipeline and let its first update run.")
        if c.get("post"):
            lines.append(render_case(c["post"], c, a.engine, schema) + end)
    print("\n".join(lines))
    return 0


def cmd_bundle(a: argparse.Namespace) -> int:
    check_mech(a.engine, a.mechanism)
    cases = resolve(a.cases, a.engine, a.mechanism)
    objects = [{"table": {"source_catalog": a.source_catalog, "source_schema": a.source_schema,
                          "source_table": tname(c["id"]), "destination_catalog": a.catalog,
                          "destination_schema": a.schema, "table_configuration": {"scd_type": "SCD_TYPE_1"}}}
               for c in cases]
    name = f"ddl-matrix-{a.engine}-{a.mechanism}"
    pipes: dict = {}
    if a.mode == "standard":
        pipes["ddl_matrix_gateway"] = {
            "name": f"{name}-gateway", "catalog": a.catalog, "schema": f"{a.schema}_stg", "serverless": True,
            "continuous": True,
            "gateway_definition": {"connection_name": a.connection, "gateway_storage_catalog": a.catalog,
                                   "gateway_storage_schema": f"{a.schema}_stg", "gateway_storage_name": f"{name}-gateway"}}
        ingestion: dict = {"ingestion_gateway_id": "${resources.pipelines.ddl_matrix_gateway.id}"}
    else:
        ingestion = {"connection_name": a.connection, "connector_type": "CDC"}
    if a.engine == "postgres":
        ingestion.update({"source_type": "POSTGRESQL", "source_configurations": [
            {"catalog": {"source_catalog": a.source_catalog, "postgres": {"slot_config": {
                "slot_name": a.slot, "publication_name": a.publication}}}}]})
    ingestion["objects"] = objects
    pipes["ddl_matrix"] = {"name": name, "catalog": a.catalog, "schema": a.schema, "serverless": True,
                           "continuous": False, "ingestion_definition": ingestion}
    doc = {"bundle": {"name": name}, "targets": {"dev": {"default": True}}, "resources": {"pipelines": pipes}}
    Path(a.out).write_text(yaml.safe_dump(doc, sort_keys=False, width=120))
    print(f"wrote {a.out}: {len(objects)} tables, {a.mode}, {a.engine} {a.mechanism}; each table is named after its case id "
          f"with _ for -. Deploy: databricks bundle deploy --profile <profile>")
    return 0


# ---- collect ---------------------------------------------------------------------------------------------------------

def cli_json(args: list[str]) -> object:
    out = subprocess.run(["databricks", *args], capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"databricks {' '.join(args[:3])} failed: {out.stderr.strip()[:300]}")
    return json.loads(out.stdout or "[]")


def fetch_events(pipeline: str, profile: str) -> list[dict]:
    """All events of the pipeline, with their details (the runtime is in one). The REST call pages 250 at a time: the
    CLI's list-pipeline-events drops the details."""
    events, tok = [], None
    for _ in range(40):
        path = f"/api/2.0/pipelines/{pipeline}/events?max_results=250" + (f"&page_token={tok}" if tok else "")
        page = cli_json(["api", "get", path, "--profile", profile, "-o", "json"])
        events += page.get("events", []) if isinstance(page, dict) else (page or [])
        tok = page.get("next_page_token") if isinstance(page, dict) else None
        if not tok:
            break
    return events


def runtime_from_events(events: list[dict]) -> dict | None:
    """The Lakeflow runtime of the latest runtime_details event: DBR version, channel, image key, the dp build in it,
    and the cloud and region. None if the events have no such event."""
    rts = [e for e in events if e.get("event_type") == "runtime_details"]
    if not rts:
        return None
    e = max(rts, key=lambda x: x.get("timestamp", ""))
    rv = ((e.get("details") or {}).get("runtime_details") or {}).get("runtime_version") or {}
    image = rv.get("image_key") or None
    m = re.search(r"dp-\d{8}-rc\d+", image or "") or re.search(r"dp-\d{8}", image or "")
    origin = e.get("origin") or {}
    cloud_region = f"{origin['cloud']} {origin['region']}" if origin.get("cloud") and origin.get("region") else None
    return {"dbr_version": rv.get("dbr_version"), "channel": rv.get("channel"), "image_key": image,
            "dp_build": m.group(0) if m else None, "cloud_region": cloud_region,
            "basis": "read from this run's pipeline (the runtime_details event)"}


def sql_rows(profile: str, warehouse: str, statement: str) -> tuple[list[str], list[list]]:
    body = {"warehouse_id": warehouse, "statement": statement, "wait_timeout": "50s",
            "disposition": "INLINE", "format": "JSON_ARRAY"}
    resp = cli_json(["api", "post", "/api/2.0/sql/statements", "--profile", profile, "--json", json.dumps(body)])
    if resp.get("status", {}).get("state") != "SUCCEEDED":
        sys.exit(f"statement failed: {resp.get('status', {}).get('error', {}).get('message', '')[:300]}")
    cols = [c["name"] for c in resp["manifest"]["schema"]["columns"]]
    return cols, resp.get("result", {}).get("data_array", [])


def fetch_tables(profile: str, warehouse: str, dest: str, cases: list[dict]) -> dict:
    """{table: {columns: {name: type}, row: {column: value} | None}} for the destination schema; row is id 3."""
    catalog, schema = dest.split(".", 1)
    _, rows = sql_rows(profile, warehouse, f"SELECT table_name, column_name, data_type FROM {catalog}.information_schema.columns "
                                           f"WHERE table_schema = '{schema}'")
    tables: dict = {}
    for tn, cn, dtype in rows:
        tables.setdefault(tn, {"columns": {}, "row": None})["columns"][cn] = dtype
    for c in cases:
        tn = tname(c["id"])
        if tn in tables and c.get("post"):
            names, data = sql_rows(profile, warehouse, f"SELECT * FROM {catalog}.{schema}.{tn} WHERE id = 3")
            tables[tn]["row"] = dict(zip(names, data[0])) if data else None
    return tables


def table_of(event: dict, names: list[str]) -> str | None:
    """The destination table an event is about: from its flow name, or the table named in its message (longest name first)."""
    flow = (event.get("origin") or {}).get("flow_name") or ""
    text = (event.get("message") or "") + " " + json.dumps(event.get("error") or "")
    for t in sorted(names, key=len, reverse=True):
        if re.search(rf"_{re.escape(t)}_(?:cdc|snapshot)_flow\b", flow) or \
                re.search(rf"\.{re.escape(t)}['\"\s,.]", text) or re.search(rf"_{re.escape(t)}_(?:cdc|snapshot)_flow", text):
            return t
    return None


def event_classes(event: dict) -> list[str]:
    found: list[str] = []
    for exc in (event.get("error") or {}).get("exceptions") or []:
        m = re.match(r"\[([A-Z][A-Z0-9_.]+)\]", exc.get("message") or "")
        if m and m.group(1) not in found:
            found.append(m.group(1))
    for m in CLASS_RE.finditer(event.get("message") or ""):
        if m.group(1) not in found:
            found.append(m.group(1))
    return found


def update_state(events: list[dict]) -> str | None:
    msgs = [e.get("message") or "" for e in events if e.get("event_type") == "update_progress"]
    m = re.search(r"is (COMPLETED|FAILED|CANCELED)", msgs[-1]) if msgs else None
    return m.group(1) if m else None


def since_ok(event: dict, since: str | None) -> bool:
    if not since:
        return True
    ts = dt.datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    return ts >= dt.datetime.fromisoformat(since.replace("Z", "+00:00"))


def load_env(path: str) -> tuple[dict, dict]:
    """The run's environment block, and the options beside it (class_map, confound_no_error, overrides, ...)."""
    doc = yaml.safe_load(Path(path).read_text())
    return doc["environment"], {k: v for k, v in doc.items() if k != "environment"}


def split_types(cs: dict, engine: str) -> tuple[str | None, str | None]:
    t = (cs.get("types") or {}).get(engine)
    if not t or t == "-":
        return None, None
    if "->" in t:
        before, after = (x.strip() for x in t.split("->", 1))
        return before, after
    return t, t


def case_entry(spec: dict, engine: str, env: dict, events: list[dict], tables: dict, state: str | None) -> dict:
    tn = tname(spec["id"])
    names = list(tables)
    mine = [e for e in events if table_of(e, names) == tn and (e.get("level") in ("ERROR", "WARN") or event_classes(e))]
    ev_out: list[dict] = []
    for e in mine:
        level = {"ERROR": "error", "WARN": "warning"}.get(e.get("level"), "info")
        for cls in event_classes(e) or [None]:
            item = {"class": cls, "level": level, "message": (e.get("message") or "")[:200]}
            if item not in ev_out:
                ev_out.append(item)
    stops = [e for e in ev_out if e["class"] and STOP_RE.search(e["class"])]
    restarts = [e for e in ev_out if e["class"] and RESTART_RE.search(e["class"])]
    if stops:
        outcome, frn = "table-stops-full-refresh", True
    elif restarts:
        outcome, frn = "restart-no-full-refresh", False
    else:
        outcome, frn = "no-interruption", False
    if restarts and not any(e["class"] for e in restarts):
        ev_out.append({"class": None, "level": "warning", "message": "restart (class not recorded)"})
    # one DDL batch with a failed update: the rows of the tables that had no event of their own are confounded
    confounded = env.get("method") == "batch" and state == "FAILED" and not mine
    before, after = split_types(spec, engine)
    entry_tab = tables.get(tn) or {}
    row, cols = entry_tab.get("row"), entry_tab.get("columns", {})
    main_col = spec.get("column", "n")
    value_check = None
    if spec.get("post") is None:
        landed = None
    elif confounded or state is None:
        landed = "unverified"
    elif spec.get("expect_absent"):
        landed = row is not None
        value_check = "row found under the old name" if row is not None else "row not in the destination (the new name is on the source)"
    elif row is None:
        landed = False
    else:
        exp = spec.get("expect") or {}
        wrong = [k for k, v in exp.items() if str(row.get(k)) != ("None" if v is None else str(v))]
        landed = True
        want = " ".join(f"{k}={v}" for k, v in exp.items())
        value_check = want + " landed" if not wrong else want + " landed as " + " ".join(f"{k}={row.get(k)}" for k in wrong)
    dtype = cols.get(main_col)
    return {"id": spec["id"], "ddl": render_case(spec.get("ddl", {}).get(engine) or "", spec, engine) or None,
            "column_before": before, "column_after": after, "source_accepted": None, "events": ev_out,
            "outcome": outcome, "full_refresh_needed": frn, "auto_full_refresh_applies": None,
            "destination_type_after": dtype if dtype else ("absent" if tn in tables else None),
            "post_change_row_landed": landed, "value_check": value_check,
            "basis": "confounded" if confounded else "tested",
            "notes": "collected from the pipeline's events and the destination; source refusals are not in the events, add them by hand"}


def verdict(entry: dict) -> str:
    o, p = entry["outcome"], entry["post_change_row_landed"]
    if o == "table-stops-full-refresh" and p is not True:
        return "STOP"
    if p == "unverified":
        return "UNVERIFIED"
    if p is None:
        return "OBSERVED"
    if p is False:
        return "FAIL" if o in ("restart-no-full-refresh", "no-interruption") else "NOT LANDED"
    return "VALUE" if entry.get("value_check") and " as " in entry["value_check"] else "PASS"


def check_doc(doc: dict, name: str) -> list[str]:
    spec = importlib.util.spec_from_file_location("ddl_matrix_validate", HERE / "ddl-matrix-validate.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.check_doc(name, doc)


def write_checked(doc: dict, out: str) -> None:
    probs = check_doc(doc, Path(out).name)
    if probs:
        sys.exit("not written, the result fails validation:\n" + "\n".join(probs))
    Path(out).write_text(yaml.safe_dump(doc, sort_keys=False, width=200, allow_unicode=True))


def cmd_collect(a: argparse.Namespace) -> int:
    env, _opts = load_env(a.env)
    check_mech(env["engine"], env["mechanism"])
    cases = resolve(a.cases, env["engine"], env["mechanism"])
    if a.events_json:
        events = json.loads(Path(a.events_json).read_text())
        tables = json.loads(Path(a.tables_json).read_text())
    else:
        events = fetch_events(a.pipeline, a.profile)
        tables = fetch_tables(a.profile, a.warehouse, a.dest, cases)
        if a.save_replay:
            Path(a.save_replay).write_text(json.dumps({"events": events, "tables": tables}, indent=1))
    rt = runtime_from_events(events)   # read before anything is torn down: the runtime event is in the pipeline's events
    if rt is not None:
        env["runtime"] = rt
    elif not env.get("runtime"):
        env["runtime"] = {"dbr_version": None, "channel": None, "image_key": None, "dp_build": None, "cloud_region": None,
                          "reason": "no runtime_details event in the events read; run collect before the pipeline is destroyed"}
    events = [e for e in events if since_ok(e, a.since)]
    state = update_state(events)
    present = [c for c in cases if tname(c["id"]) in tables]
    entries = [case_entry(c, env["engine"], env, events, tables, state) for c in present]
    write_checked({"environment": env, "cases": entries}, a.out)
    for c, e in zip(present, entries):
        print(f"{verdict(e):<11} {c['id']:<28} {e['outcome']:<26} row={e['post_change_row_landed']}")
    missing = [c["id"] for c in cases if tname(c["id"]) not in tables]
    if missing:
        print(f"not in the destination (not run, or not in the bundle): {', '.join(missing)}")
    print(f"wrote {a.out}: {len(entries)} cases; the last update is {state or 'not finished'}")
    return 0


# ---- convert: a scratch run (one line of prose per case) -> a result file -------------------------------------------

def convert_one(item: dict, env: dict, opts: dict) -> dict:
    raw = str(item.get("case", ""))
    cid = ALIAS.get(re.sub(r"\s*\(.*\)$", "", raw).strip())
    if cid is None:
        sys.exit(f"convert: no case in ddl-cases.yaml for {raw!r}; add the name to the case's aliases")
    spec = CASES[cid]
    engine = env["engine"]
    ddl = item.get("ddl")
    if ddl and str(ddl).endswith("(not run)"):
        ddl = str(ddl)[: -len("(not run)")].strip()
    res = str(item.get("result", ""))
    for old, new in (opts.get("redact") or {}).items():   # a source name the run notes quote, replaced in text and DDL
        res = res.replace(old, new)
        ddl = None if ddl is None else str(ddl).replace(old, new)
    for short, full in (opts.get("class_map") or {}).items():
        res = re.sub(rf"(?<![A-Za-z_.]){re.escape(short)}(?![A-Za-z_])", full, res)
    low, ddl_low = res.lower(), str(ddl or "").lower()
    not_run = "not run" in ddl_low or low.startswith(("not run", "skipped"))
    refused = bool(re.search(r"refused", res, re.I))
    notes = [f"recorded: {res.strip()}"]
    if raw != cid:
        notes.append(f"recorded as {raw!r}")
    if item.get("same_as_without_utility"):
        notes.append("same as without the utility script (run notes)")
    if not_run and ddl:
        notes.append(f"planned, not run: {ddl}")
    basis = "confounded" if (opts.get("confound_no_error") and not CLASS_RE.search(res)) else "tested"
    if not_run:
        outcome, frn = "not-run", None
    elif refused:
        outcome, frn = "not-applicable", None
    elif STOP_RE.search(res) or re.search(r"table stops|\bstops\b|full refresh (?:needed|required)|full refresh not run", res):
        outcome, frn = "table-stops-full-refresh", True
    elif re.search(r"(?<!no )restart", res):
        outcome, frn = "restart-no-full-refresh", False
    else:
        outcome, frn = "no-interruption", False
    events: list[dict] = []
    for m in CLASS_RE.finditer(res):
        cls = m.group(1)
        if any(e["class"] == cls for e in events):
            continue
        seg = next((s for s in re.split(r";\s*", res) if cls in s), res)
        events.append({"class": cls, "level": "warning" if RESTART_RE.search(cls) else "error",
                       "message": seg.strip()[:200]})
    if not_run:
        events = [dict(e) for e in opts.get("not_run_events") or []]
    if outcome == "restart-no-full-refresh" and not any(e["class"] and RESTART_RE.search(e["class"]) for e in events):
        events.append({"class": None, "level": "warning", "message": "restart (class not recorded in the run notes)"})
    if not_run or ddl is None:
        accepted = None
    elif refused:
        msg = re.search(r"Msg \d+: [^(;]+", res)
        quoted = re.search(r'"([^"]+)"', res)
        accepted = (msg.group(0) if msg else quoted.group(1) if quoted else res).strip()
    else:
        accepted = True
    if not spec.get("post"):
        landed = None
    elif not_run or basis == "confounded" or re.search(r"not verified|unverified", low):
        landed = "unverified"
    elif re.search(r"not landed|not ingested|not applied", low):
        landed = False
    elif "landed" in low:
        landed = True
    else:
        landed = "unverified"
    vc = None
    if landed is True:
        m = re.search(r"([^;(),]*\blanded\b[^;(),]*)", res)
        vc = m.group(1).strip() if m else None
    dest = re.search(r"destination[^;]*?->\s*" + TYPE_WORDS, res) or \
        re.search(r"destination(?: [\w(),]+){0,3}? " + TYPE_WORDS, res)
    before, after = split_types(spec, engine)
    return {"id": cid, "ddl": None if ddl is None else str(ddl), "column_before": before, "column_after": after,
            "source_accepted": accepted, "events": events, "outcome": outcome, "full_refresh_needed": frn,
            "auto_full_refresh_applies": None, "destination_type_after": dest.group(1) if dest else None,
            "post_change_row_landed": landed, "value_check": vc, "basis": None if not_run else basis,
            "notes": "; ".join(notes)}


def red(text, opts):
    """Replace the source names the notes quote: literal pairs (redact) and patterns (redact_re), in any text."""
    if text is None:
        return None
    text = str(text)
    for a, b in (opts.get("redact") or {}).items():
        text = text.replace(a, b)
    for pat, b in (opts.get("redact_re") or {}).items():
        text = re.sub(pat, b, text)
    return text


def parse_events(raw, opts: dict) -> list[dict]:
    """Events from a list or from one text with '; ' between them: 'CLASS (WARN): message', or prose ('none' = no event)."""
    if raw is None:
        return []
    pieces = raw if isinstance(raw, list) else re.split(r";\s*", str(raw))
    out: list[dict] = []
    for piece in pieces:
        p = red(str(piece).strip().strip('"').strip(), opts)
        if not p or re.match(r"(?i)none\b", p):
            continue
        m = re.search(r"\b([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+(?:\.[A-Z][A-Z0-9_]+)?)", p)   # a class, after any prefix
        cls = m.group(1) if m else None
        rest = p[m.end():] if cls else p
        msg = re.sub(r"^[\s:(]*(WARN|ERROR|INFO)?[\s):]*", "", rest).strip()[:200] or p[:200]
        if cls and RESTART_RE.search(cls) or cls is None and "WARN" in p:
            level = "warning"
        else:
            level = "error"
        out.append({"class": cls, "level": level, "message": msg})
    return out


def settle(entry: dict) -> dict:
    """A stop, a failed pipeline or a refusal whose error class or refusal was seen: the row is not landed (false), tested."""
    if entry["outcome"] == "not-applicable":   # a source refusal: no post-change row exists
        entry["post_change_row_landed"] = None
        return entry
    stop_like = entry["outcome"] in ("table-stops-full-refresh", "pipeline-fails")
    evidence = any(e["class"] for e in entry["events"])
    if stop_like and evidence and entry["post_change_row_landed"] == "unverified":
        entry["post_change_row_landed"] = False
        if entry["basis"] != "partial":
            entry["basis"] = "tested"
    return entry


BASIS_HEAD = re.compile(r"(tested|confounded|code-only|partial)\b")


def structured_case(item: dict, env: dict, opts: dict) -> dict:
    """A case from a structured run file (cases: id/case, type_before, type_after, events as text, basis with a note)."""
    ident, casename = str(item.get("id") or ""), str(item.get("case") or "")
    case_map = opts.get("case_map") or {}
    if ident in case_map:   # the run's own case names (MySQL: c01_add_null), mapped in the environment file
        name, run_id = case_map[ident], ident
    elif re.sub(r"\s*\(.*\)$", "", ident).strip() in ALIAS or not casename:   # the name is in id (PostgreSQL) or in case (Oracle)
        name, run_id = ident, (casename or None)
    else:
        name, run_id = casename, ident
    cid = ALIAS.get(re.sub(r"\s*\(.*\)$", "", name).strip())
    if cid is None:
        sys.exit(f"convert: no case in ddl-cases.yaml for {name!r}; add the name to the case's aliases")
    spec = CASES[cid]
    src = item.get("source_accepted")
    not_run_text = isinstance(src, str) and src.lower().startswith("not run")
    raw_outcome = str(item.get("outcome") or "")
    outcome = "not-run" if (raw_outcome == "not-run" or not_run_text) else raw_outcome
    if outcome not in OUTCOMES_KNOWN:
        sys.exit(f"convert: {name}: outcome {raw_outcome!r} is not one of {sorted(OUTCOMES_KNOWN)}")
    basis_raw = str(item.get("basis") or "").strip()
    bm = BASIS_HEAD.match(basis_raw)
    if bm:
        basis, basis_note = bm.group(1), basis_raw[bm.end():].strip(" ;:()")
    else:
        basis, basis_note = None, basis_raw
    if outcome == "not-run" and basis == "tested":
        basis = None
    fr = item.get("full_refresh_needed")
    fr = {"yes": True, "true": True, "no": False, "false": False}.get(str(fr).lower(), None) if not isinstance(fr, bool) else fr
    landed = item.get("post_change_row_landed")
    if isinstance(landed, str):
        landed = None if landed.lower() in ("none", "null") else "unverified"
    if spec.get("post") is None:   # no post-change insert in the catalog (a drop): nothing to land
        landed = None
    vc = red(item.get("value_check"), opts)
    vc = None if vc in (None, "none") else vc
    if isinstance(src, bool):
        accepted = src
    elif src is None or not_run_text:
        accepted = None
    else:
        accepted = red(src, opts)
    before = red(item.get("type_before"), opts)
    after = red(item.get("type_after"), opts)
    before = "absent" if before == "none" else before
    notes = [red(item.get("notes"), opts) or ""]
    if basis_note:
        notes.append(red(basis_note, opts))
    if run_id:
        notes.append(f"run case {run_id}")
    if name != cid:
        notes.append(f"recorded as {name!r}")
    dest = red(item.get("destination_type_after"), opts)
    if dest is None and item.get("destination_describe_after_update"):
        try:
            cols = json.loads(str(item["destination_describe_after_update"]))
            hit = next((r[1] for r in cols if r and r[0] == spec.get("column", "n")), None)
            dest = hit if hit else "absent"
        except (ValueError, IndexError, TypeError):
            dest = None
    note_text = " ".join(notes)
    if opts.get("snapshot_after_reset") and outcome in ("pipeline-fails", "table-stops-full-refresh") \
            and landed in (True, False) and re.search(r"reset|snapshot", note_text):
        landed = False   # a row seen only after a table reset came from the snapshot, never from CDC
        vc = vc or "landed by snapshot after the table reset (not CDC)"
    return {"id": cid, "ddl": red(item.get("ddl"), opts), "column_before": before, "column_after": after,
            "source_accepted": accepted, "events": parse_events(item.get("events"), opts), "outcome": outcome,
            "full_refresh_needed": fr, "auto_full_refresh_applies": None,
            "destination_type_after": dest, "post_change_row_landed": landed,
            "value_check": vc, "basis": basis, "notes": "; ".join(n for n in notes if n)}


OUTCOMES_KNOWN = {"restart-no-full-refresh", "no-interruption", "table-stops-full-refresh", "pipeline-fails",
                  "not-applicable", "not-run"}


def cmd_convert(a: argparse.Namespace) -> int:
    env, opts = load_env(a.env)
    for kv in a.set or []:
        k, v = kv.split("=", 1)
        env[k] = v
    raw = yaml.safe_load(Path(a.scratch).read_text())
    if isinstance(raw, dict):   # a structured run file: cases: [...] (its own environment block is replaced by --env)
        cases = [structured_case(i, env, opts) for i in raw["cases"]]
    else:
        cases = [convert_one(i, env, opts) for i in raw]
    for c in cases:
        c.update((opts.get("overrides") or {}).get(c["id"], {}))
    cases = [settle(c) for c in cases]   # last: an override can't leave a stop's row unverified
    write_checked({"environment": env, "cases": cases}, a.out)
    print(f"wrote {a.out}: {len(cases)} cases")
    return 0


# ---- render: the comparison tables -------------------------------------------------------------------------------------

SHORT = {"restart-no-full-refresh": "restart", "no-interruption": "no-int", "table-stops-full-refresh": "stops",
         "pipeline-fails": "pipe-fails", "not-applicable": "refused", "not-run": "not run"}


def combo(env: dict) -> tuple:
    return (ENGINE_ORDER.index(env["engine"]), env["mechanism"], SETUP_ORDER.get(env["setup"], 3), env["engine"], env["setup"])


def file_key_of(doc: dict) -> tuple:
    return (str(doc["environment"].get("date")), 0)


def file_key(name: str) -> tuple:
    """Run order: the date, then the run number (no suffix is run 1)."""
    m = re.search(r"-(\d{4}-\d{2}-\d{2})(?:-(\d+))?\.yaml$", name)
    return (m.group(1), int(m.group(2) or 1)) if m else ("", 0)


SHORT = {"restart-no-full-refresh": "restart", "no-interruption": "no-int", "table-stops-full-refresh": "stops",
         "pipeline-fails": "pipe-fails", "not-applicable": "refused", "not-run": "not run"}


def short_class(cls: str | None) -> str:
    if not cls:
        return ""
    if "." in cls:
        first, sub_code = cls.split(".", 1)
        return sub_code if sub_code != "UNSUPPORTED_DDL" else first.replace("INGESTION_GATEWAY_", "")
    return cls.replace("INGESTION_GATEWAY_", "")


def cell(entry: dict) -> str:
    o = entry["outcome"]
    if o == "not-run":
        cls = next((e["class"] for e in entry["events"] if e["class"]), None)
        return "not run" + (f" {short_class(cls)}" if cls else "")
    cls = next((e["class"] for e in entry["events"] if e["class"] and (
        RESTART_RE.search(e["class"]) or STOP_RE.search(e["class"]))), None)
    p = entry["post_change_row_landed"]
    landed = {True: "+", False: "-"}.get(p, "?" if p == "unverified" else "")
    if o == "not-applicable":
        landed = "n/a"
    if entry["basis"] == "partial":
        mark = "P"
    elif p == "unverified":
        mark = "U"
    elif entry["basis"] == "confounded":
        mark = "C"
    else:
        mark = "T"
    mark_text = f"n/a {mark}" if landed == "n/a" else f"{landed}{mark}"
    silent = "silent" if "silent" in (entry.get("value_check") or "") else ""
    return " ".join(x for x in [SHORT.get(o, o), short_class(cls), mark_text, silent] if x)


def note_for(entry: dict) -> str | None:
    """The long text behind a cell, or None. Identical texts share one note number."""
    if entry["outcome"] == "not-run":
        msgs = [e["message"] for e in entry["events"]]
        return "not run: " + (msgs[0] if msgs else entry.get("notes", ""))
    if entry["outcome"] == "not-applicable":
        return "the source refused the DDL: " + str(entry.get("source_accepted"))
    if entry.get("value_check") and " as " in entry["value_check"]:
        return "landed with a different value: " + entry["value_check"]
    if entry.get("value_check") and "silent" in entry["value_check"]:
        return "silent (no error): " + entry["value_check"]
    if entry["basis"] == "confounded":
        return "confounded: the batch's update failed, so this row is not attributed to the change (unverified)"
    return None


def render_md(files: list[tuple[str, dict]], detail: bool) -> str:
    if detail:
        out = ["# DDL matrix: every field, per run", ""]
        for name, doc in sorted(files, key=lambda f: combo(f[1]["environment"])):
            out += [f"## {name}", "", "```yaml", yaml.safe_dump(doc["environment"], sort_keys=False).rstrip(), "```", ""]
            for c in doc["cases"]:
                out += [f"### {c['id']}", "", "```yaml", yaml.safe_dump(c, sort_keys=False, width=200).rstrip(), "```", ""]
        return "\n".join(out) + "\n"
    cols: dict[tuple, list[dict]] = {}
    for _, doc in files:
        cols.setdefault(combo(doc["environment"]), []).append(doc)
    run_of = {id(doc): file_key(name) for name, doc in files}
    headers = []
    for key in sorted(cols):
        e = cols[key][0]["environment"]
        newest = max(cols[key], key=lambda d: file_key_of(d))["environment"]
        dp = ((newest.get("runtime") or {}).get("dp_build") or "")
        label = f"{e['engine']} {e['mechanism']} {e['setup']}" + (f" · {dp}" if dp else "") \
            + (f" ({len(cols[key])} runs)" if len(cols[key]) > 1 else "")
        headers.append((key, label))
    by_case: dict[str, dict[tuple, list[tuple]]] = {}
    for key, docs in cols.items():
        for doc in docs:
            for c in doc["cases"]:
                by_case.setdefault(c["id"], {}).setdefault(key, []).append((run_of[id(doc)], c))
    numbers: dict[str, int] = {}
    out = ["# DDL matrix results", "",
           "Generated from the run files in [results/ddl-matrix](../../../results/ddl-matrix/) by "
           "`uv run skills/lfc-schema-change-test/scripts/ddl-matrix.py render > skills/lfc-schema-change-test/references/ddl-matrix-results.md`. "
           "Don't edit by hand. The file format and the method: [ddl-matrix.md](ddl-matrix.md).", "",
           "Cell: outcome, error class, row check and basis. Outcome: `restart` (the update restarts, no full refresh), `no-int` (no "
           "interruption), `stops` (the table stops and needs a full refresh), `pipe-fails`, `refused` (the source refused the DDL), "
           "`not run`. Row: `+` landed, `-` not landed, `?` not verified. Basis: `T` tested, `C` confounded by a batch, `U` unverified, `P` partial (the run recorded only part of the case). "
           "Notes are linked from the cell.", ""]
    out += ["| Case | Types (SQL Server) | " + " | ".join(lbl for _, lbl in headers) + " |", "|" + "---|" * (len(headers) + 2)]
    for c in CAT["cases"]:
        types = (c.get("types") or {}).get("sqlserver") or next(iter((c.get("types") or {}).values()), "")
        row = [f"`{c['id']}`", types]
        for key, _ in headers:
            texts = []
            cands = by_case.get(c["id"], {}).get(key, [])
            shown = [max(cands, key=lambda rc: rc[0])[1]] if cands else []   # the newest run with this case
            for e in shown:
                t = cell(e)
                text = note_for(e)
                if text:
                    if text not in numbers:
                        numbers[text] = len(numbers) + 1
                    t += f" [n{numbers[text]}](#n{numbers[text]})"
                texts.append(t)
            row.append(" / ".join(dict.fromkeys(texts)))
        out.append("| " + " | ".join(row) + " |")
    out += ["", "A `(N runs)` column shows, for each case, the newest run that has the case; `--detail` lists every run. "
            "Each mechanism's `none` column and its `utility` column are the same cases with and without the setup script.", "",
            "## Notes", ""]
    out += [f'<a id="n{i}"></a>{i}. {text}' for text, i in sorted(numbers.items(), key=lambda kv: kv[1])]
    return "\n".join(out) + "\n"


def cmd_render(a: argparse.Namespace) -> int:
    paths = a.files or [str(p) for d in read_dirs("ddl-matrix") if d.exists() for p in sorted(d.glob("*.yaml"))]
    if not paths:
        sys.exit("no result files")
    files = []
    for p in paths:
        doc = yaml.safe_load(Path(p).read_text())
        probs = check_doc(doc, Path(p).name)
        if probs:
            sys.exit(f"{p} fails validation: {probs[0]} (run ddl-matrix-validate.py on it)")
        files.append((Path(p).name, doc))
    print(render_md(files, a.detail), end="")
    return 0


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 2
    ap = argparse.ArgumentParser(prog="ddl-matrix.py")
    sp = ap.add_subparsers(dest="cmd", required=True)
    s = sp.add_parser("sql")
    s.add_argument("engine", choices=ENGINE_ORDER); s.add_argument("mechanism")
    s.add_argument("--setup", default="none", choices=sorted(SETUP_ORDER))
    s.add_argument("--cases"); s.add_argument("--schema"); s.add_argument("--login", default="<login>")
    s.add_argument("--publication", default="<publication>"); s.add_argument("--slot", default="<slot>")
    b = sp.add_parser("bundle")
    b.add_argument("--engine", default="sqlserver", choices=ENGINE_ORDER); b.add_argument("--mechanism", default="cdc")
    b.add_argument("--connection", required=True); b.add_argument("--catalog", required=True)
    b.add_argument("--schema", required=True); b.add_argument("--source-catalog", required=True)
    b.add_argument("--source-schema", default="dbo")
    b.add_argument("--mode", default="integrated", choices=["integrated", "standard"])
    b.add_argument("--cases"); b.add_argument("--out", default="databricks.yml")
    b.add_argument("--publication", default="<publication>"); b.add_argument("--slot", default="<slot>")
    c = sp.add_parser("collect")
    c.add_argument("--pipeline"); c.add_argument("--profile"); c.add_argument("--warehouse")
    c.add_argument("--dest", help="destination catalog.schema")
    c.add_argument("--env", required=True, help="the run's environment file (see references/ddl-matrix.md)")
    c.add_argument("--since", help="UTC time of the first DDL, e.g. 2026-10-26T22:00:00Z")
    c.add_argument("--cases"); c.add_argument("--out", required=True)
    c.add_argument("--events-json"); c.add_argument("--tables-json"); c.add_argument("--save-replay")
    v = sp.add_parser("convert")
    v.add_argument("scratch"); v.add_argument("--env", required=True); v.add_argument("--out", required=True)
    v.add_argument("--set", action="append", help="KEY=VALUE for the environment block")
    r = sp.add_parser("render")
    r.add_argument("files", nargs="*"); r.add_argument("--detail", action="store_true")
    a = ap.parse_args(argv)
    if a.cmd == "collect" and not a.events_json and not (a.pipeline and a.profile and a.warehouse and a.dest):
        sys.exit("collect needs --pipeline, --profile, --warehouse and --dest, or --events-json and --tables-json")
    if a.cmd == "collect" and a.events_json and not a.tables_json:
        sys.exit("collect with --events-json also needs --tables-json")
    return {"sql": cmd_sql, "bundle": cmd_bundle, "collect": cmd_collect, "convert": cmd_convert,
            "render": cmd_render}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
