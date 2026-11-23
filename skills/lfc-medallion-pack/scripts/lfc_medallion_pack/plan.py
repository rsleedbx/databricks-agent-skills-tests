"""plan: a project's modules, each a model pack built as many LOBs, planned into what to deploy.

A project file (the customer's repo, e.g. febar.yaml) lists its modules; each is one schema of theirs (or a TPC
model), repeated across LOBs, that becomes one n:1 bronze, silver and gold:

    source:                       # the source database: one Lakebase project and database, a schema per module's LOB
      slot_prefix: febar          # replication slots and publications: <slot_prefix>_<module>_<k>
      max_slots: 10               # the source's max_replication_slots, if known: render stops past it
    limits:
      tables_per_pipeline: 250    # an ingestion pipeline's tables (Lakeflow Connect: 1,000 hard, 250 recommended)
    modules:
      bank:                       # a module: its name is in every schema of its own
        pack: tpcb                # a builtin pack, or a folder of the repo's own (packs/<name>)
        lobs: 10                  # how many LOBs (lob_001 ..), or names: [east, west]
        seed: 7                   # the 20-70-10 draw (lobs.py)
        data: true                # fill the sources (false: the tables empty, for the no-data test)
        tables_per_pipeline: 999  # optional: this module's own limit (the project's otherwise)

render() writes, under the repo:
    resources/_control.yml        the control schema: every pipeline's event log (events_<pipeline>), every gateway's
                                  staging volume, the checksums and merge watermarks: what the dashboard reads
    resources/_dashboard.yml      the dashboard (dashboards/lakehouse.lvdash.json) over the control schema's views
    resources/<module>.yml        the bundle resources: schemas (a bronze per LOB, silver, gold), one gateway
                                  and ingestion pipeline per group of LOBs, the transform pipeline, and the refresh
                                  job: ingest, merge (the YAML's dimensions, lfc-medallion's merge route), transform,
                                  verify
    marts/<pack>/*.sql            the pack's silver and gold, once (yours to change; kept on a rerun)
    source/plan.json              every module's LOBs, tiers, groups and names, for the source and verify jobs (in
                                  source/, which both bundles sync)
Source schemas are <module>_<lob> in the one database; bronze schemas <prefix>_<module>_bronze_<lob>.
"""
from __future__ import annotations

import json
import math
import os
import shutil

import yaml

from . import lobs as lobs_mod
from . import pack as pack_mod

TABLES_PER_PIPELINE = 250      # recommended; a pipeline takes at most 999 source tables (1,000 with its staging table)
HARD_TABLES = 999


MEDALLION = ("lfc-change-source", "lfc-medallion")   # the packages the jobs and pipelines install from the archive


def lob_groups(lobs: int, tables: int, limit: int) -> list[tuple[int, int]]:
    """The LOBs [first, end) of each ingestion pipeline of one class (`tables` per LOB, at most `limit` a pipeline)."""
    per = max(1, limit // tables)
    return [(k * per, min(lobs, (k + 1) * per)) for k in range(math.ceil(lobs / per))]


def load_project(path: str) -> dict:
    with open(path) as fh:
        p = yaml.safe_load(fh) or {}
    if not p.get("modules"):
        raise ValueError(f"{path}: no modules")
    p.setdefault("source", {}).setdefault("slot_prefix", "lfc")
    p.setdefault("limits", {}).setdefault("tables_per_pipeline", TABLES_PER_PIPELINE)
    return p


def module_plan(name: str, m: dict, project: dict, base: str) -> dict:
    """One module: its pack, LOBs with tiers and data settings, and its groups (one ingestion pipeline each)."""
    if not lobs_mod.NAME.match(name):
        raise ValueError(f"module {name}: lower-case letters, digits and _")
    p = pack_mod.load(m["pack"], base)
    items = lobs_mod.lobs(m.get("lobs") if isinstance(m.get("lobs"), int) else None, m.get("seed", 1),
                          m.get("lobs") if isinstance(m.get("lobs"), list) else m.get("names"))
    sf = m.get("sf")   # every LOB at this scale factor (a scale test's small data); else each tier's own
    if sf is not None and not (isinstance(sf, (int, float)) and sf > 0):
        raise ValueError(f"module {name}: sf {sf!r}: a number above 0")
    for x in items:
        t = p["tiers"][x["tier"]]
        x |= {"sf": sf if sf is not None else t.get("sf", 1), "signals": pack_mod.signals(p, x["tier"]), "source_schema": f"{name}_{x['name']}"}
    limit = int(m.get("tables_per_pipeline") or project["limits"]["tables_per_pipeline"])
    if limit > HARD_TABLES:
        raise ValueError(f"tables_per_pipeline {limit}: an ingestion pipeline takes at most {HARD_TABLES} source tables")
    sp = project["source"]["slot_prefix"]
    from lfc_medallion import split_tables   # the one rule: the merge route's, from the same roles
    facts, dims = split_tables({t: (p.get("roles") or {}).get(t, "") for t in p["tables"]})
    if m.get("split_by_role") and dims and facts:
        # facts (fact, log, queue) and dimensions (dimension, lookup, bridge) in pipelines of their own: a busy fact
        # doesn't wait on many idle dimensions; each class takes its own mode
        classes = [("facts", facts, m.get("fact_mode", "continuous")),
                   ("dimensions", dims, m.get("dimension_mode", "triggered"))]
    else:
        classes = [("all", list(p["tables"]), m.get("mode", "triggered"))]
    groups = []
    for cls, tabs, mode in classes:
        if mode not in ("triggered", "continuous"):
            raise ValueError(f"module {name}: mode {mode}: triggered or continuous")
        for b, e in lob_groups(len(items), len(tabs), limit):
            part = items[b:e]
            n = len(groups) + 1
            slot = f"{sp}_{name}_{n}"
            groups.append({"k": n, "lobs": [x["name"] for x in part], "tables": len(part) * len(tabs),
                           "table_names": tabs if cls != "all" else None, "class": cls, "mode": mode,
                           "slot": slot, "publication": f"{slot}_pub"})
    return {"module": name, "pack": p["name"], "pack_dir": p["dir"], "model": p["model"],
            "source_column": p["source_column"], "tables": p["tables"], "roles": p.get("roles", {}),
            "data": bool(m.get("data", False)), "merge_path": m.get("merge_path") or "auto",
            "lobs": items, "groups": groups, "configuration": p.get("configuration", {}),
            "readout": p.get("readout"), "marts": p["marts"], "tables_per_pipeline": limit,
            "hints": m.get("hints") or {}, "dimension_tables": dims,
            "fact_chain": _fact_chain(p, facts) if m.get("split_by_role") and dims and facts else []}


def _fact_chain(p: dict, facts: list[str]) -> list[tuple[str, str | None]]:
    """The transform's datasets a fact-only refresh must update, each with the pipeline setting naming its schema
    (None: the pipeline's own): the facts' n:1 views, then every marts view that reads them (check.downstream)."""
    from .check import downstream, written_to
    n1 = [f"all_{t}" for t in facts]
    where = written_to(p)
    return [(v, None) for v in n1] + [(v, where.get(v)) for v in downstream(p, set(n1))]


def plan(project_path: str) -> dict:
    project = load_project(project_path)
    base = os.path.dirname(os.path.abspath(project_path))
    mods = [module_plan(n, m, project, base) for n, m in project["modules"].items()]
    slots = sum(len(m["groups"]) for m in mods)
    mx = project["source"].get("max_slots")
    if mx and slots > int(mx):
        raise ValueError(f"{slots} ingestion pipelines need {slots} replication slots; the source allows {mx} "
                         "(raise max_replication_slots and max_wal_senders, or tables_per_pipeline)")
    return {"project": os.path.basename(project_path), "source": project["source"], "limits": project["limits"],
            "slots": slots, "modules": mods}


def read_plan(path: str = "plan.json") -> dict:
    """A plan render wrote (source/plan.json), as the source and verify jobs read it."""
    with open(path) as fh:
        return json.load(fh)


def _schema(name: str, comment: str) -> dict:
    return {"catalog_name": "${var.catalog}", "name": f"${{var.prefix}}_{name}", "comment": comment}


def resources(m: dict, archive: str) -> dict:
    """The bundle resources of one module (the file resources/<module>.yml)."""
    n, col = m["module"], m["source_column"]
    ctl = "${resources.schemas.control.name}"
    log = lambda key: {"catalog": "${var.catalog}", "schema": ctl, "name": f"events_{key}"}   # noqa: E731
    schemas = {f"{n}_silver": _schema(f"{n}_silver", f"Module {n}: n:1 (all_<table>: every LOB's table, with {col}) "
                                                     "and silver"),
               f"{n}_gold": _schema(f"{n}_gold", f"Module {n}: gold, across every LOB")}
    for x in m["lobs"]:
        schemas[f"{n}_bronze_{x['name']}"] = _schema(f"{n}_bronze_{x['name']}",
                                                     f"Bronze of module {n}, LOB {x['name']}, as Lakeflow Connect "
                                                     "lands it (SCD type 1)")
    pipelines = {}
    for g in m["groups"]:
        gw, ing = f"{n}_gw_{g['k']}", f"{n}_ingest_{g['k']}"
        pipelines[gw] = {   # its staging volume and event log in the control schema
            "name": f"${{var.prefix}}_{gw}", "catalog": "${var.catalog}", "schema": ctl, "serverless": True,
            "continuous": True, "event_log": log(gw),
            "gateway_definition": {"connection_name": "${var.connection}", "gateway_storage_catalog": "${var.catalog}",
                                   "gateway_storage_schema": ctl, "gateway_storage_name": f"${{var.prefix}}_{gw}"}}
        base = lambda lob: {"source_catalog": "${var.source_database}", "destination_catalog": "${var.catalog}",  # noqa: E731
                            "source_schema": f"{n}_{lob}",
                            "destination_schema": f"${{resources.schemas.{n}_bronze_{lob}.name}}",
                            "table_configuration": {"scd_type": "SCD_TYPE_1"}}
        if g.get("table_names"):   # this pipeline's share of each LOB's tables (split by role)
            objects = [{"table": base(lob) | {"source_table": t}} for lob in g["lobs"] for t in g["table_names"]]
        else:
            objects = [{"schema": base(lob)} for lob in g["lobs"]]
        pipelines[ing] = {
            "name": f"${{var.prefix}}_{ing}", "catalog": "${var.catalog}",
            "schema": f"${{resources.schemas.{n}_bronze_{g['lobs'][0]}.name}}", "serverless": True,
            "event_log": log(ing), "continuous": g.get("mode") == "continuous",
            "ingestion_definition": {
                "ingestion_gateway_id": f"${{resources.pipelines.{gw}.id}}", "source_type": "POSTGRESQL",
                "source_configurations": [{"catalog": {"source_catalog": "${var.source_database}", "postgres": {
                    "slot_config": {"slot_name": g["slot"], "publication_name": g["publication"]}}}}],
                "objects": objects}}
    conf = {"catalog": "${var.catalog}", "bronze_prefix": f"${{var.prefix}}_{n}_bronze_", "source_column": col,
            "silver_schema": f"${{resources.schemas.{n}_silver.name}}",
            "gold_schema": f"${{resources.schemas.{n}_gold.name}}"} | {k: str(v) for k, v in m["configuration"].items()}
    pipelines[f"{n}_transform"] = {
        "name": f"${{var.prefix}}_{n}_transform", "catalog": "${var.catalog}",
        "schema": f"${{resources.schemas.{n}_silver.name}}", "serverless": True, "photon": True,
        "event_log": log(f"{n}_transform"),
        "environment": {"dependencies": [f"{archive}#subdirectory=skills/{d}/scripts" for d in MEDALLION]},
        "configuration": conf,
        "libraries": [{"file": {"path": "../transform/n1.py"}}] +
                     [{"file": {"path": f"../marts/{m['pack']}/{f}"}} for f in m["marts"]]}
    ingest_tasks = [{"task_key": f"ingest_{g['k']}", "pipeline_task": {   # a continuous pipeline just runs
        "pipeline_id": f"${{resources.pipelines.{n}_ingest_{g['k']}.id}}"}} for g in m["groups"]
        if g.get("mode", "triggered") == "triggered"]
    # the merge route (lfc-medallion): merge_route classifies each table from evidence (keys, foreign keys, change
    # counters) and keeps dimensions, lookups and bridges by MERGE, facts, logs and queues in the pipeline; a module's
    # own hints (febar.yaml) override a table's role only where the evidence can't tell
    # the merge route takes the schema YAML's roles, the same ones the pipelines are split by (and a batch's
    # tables chosen by): one rule, one source of roles; a module's own hints win
    hints = json.dumps({t: r for t, r in (m.get("roles") or {}).items() if r} | (m.get("hints") or {}),
                       sort_keys=True)
    merge_task = {"task_key": "merge", "depends_on": [{"task_key": t["task_key"]} for t in ingest_tasks],
                  "environment_key": "medallion",
                  "python_wheel_task": {"package_name": "lfc-medallion", "entry_point": "lfc-medallion-merge",
                                        "named_parameters": {
                                            "catalog": "${var.catalog}", "prefix": f"${{var.prefix}}_{n}_bronze_",
                                            "target": f"${{resources.schemas.{n}_silver.name}}",
                                            "source-column": col, "hints": hints,
                                            "log": f"${{var.catalog}}.{ctl}.merge_log_{n}",
                                            # only the sources whose bronze got rows since their state are read
                                            "events": ",".join(f"${{var.catalog}}.{ctl}.events_{n}_ingest_{g['k']}"
                                                               for g in m["groups"]),
                                            # the MERGE optimizer may run a table's MERGEs on the warehouse, k at
                                            # once (lfc_medallion.paths); merge_path forces one: spark, warehouse:8
                                            "warehouse": "${var.warehouse_id}",
                                            "path": m.get("merge_path") or "auto"}}}
    if not merge_task["depends_on"]:   # every pipeline continuous: nothing to wait for
        merge_task.pop("depends_on")
    jobs = {f"{n}_refresh": {
        "name": f"${{var.prefix}}_{n}_refresh",
        "environments": [{"environment_key": "medallion", "spec": {
            "client": "2", "dependencies": [f"{archive}#subdirectory=skills/{d}/scripts" for d in MEDALLION]}}],
        "tasks": ingest_tasks + [merge_task,
            {"task_key": "transform", "depends_on": [{"task_key": "merge"}],   # waits for a facts cycle running
             "notebook_task": {"notebook_path": "../transform/refresh.py", "base_parameters": {
                 "pipeline": f"${{var.prefix}}_{n}_transform", "selection": ""}}}]},
        # the facts cycle (a split module): the fact chain of the transform alone, as often as its schedule says
        **({f"{n}_facts": {"name": f"${{var.prefix}}_{n}_facts", "max_concurrent_runs": 1, "tasks": [
            {"task_key": "refresh_facts", "notebook_task": {"notebook_path": "../transform/refresh.py",
                                                            "base_parameters": {
                                                                "pipeline": f"${{var.prefix}}_{n}_transform",
                                                                "selection": ",".join(
                                                                    f"${{var.catalog}}.{conf.get(k or '', conf['silver_schema'])}.{v}"
                                                                    for v, k in m["fact_chain"])}}}]}}
           if m.get("fact_chain") else {}),
        # verify on its own: the checksums, the CDC positions and the views, off the refresh's critical path
        f"{n}_verify": {"name": f"${{var.prefix}}_{n}_verify", "tasks": [
            {"task_key": "verify",
             "notebook_task": {"notebook_path": "../checks/verify.py", "base_parameters": {
                 "module": n, "catalog": "${var.catalog}", "prefix": "${var.prefix}", "archive": archive,
                 "connection": "${var.connection}", "database": "${var.source_database}", "control": ctl}}}]}}
    return {"resources": {"schemas": schemas, "pipelines": pipelines, "jobs": jobs}}


def render(project_path: str, archive: str, out: str | None = None) -> dict:
    """Write resources/<module>.yml, marts/<pack>/ (when missing) and source/plan.json under out (the project's
    folder)."""
    out = out or os.path.dirname(os.path.abspath(project_path))
    pl = plan(project_path)
    os.makedirs(os.path.join(out, "resources"), exist_ok=True)
    written = []
    for m in pl["modules"]:
        f = os.path.join(out, "resources", f"{m['module']}.yml")
        head = (f"# Module {m['module']}: pack {m['pack']} as {len(m['lobs'])} LOBs in {len(m['groups'])} ingestion "
                f"pipeline(s), then n:1, silver and gold.\n# Written by lfc-medallion-pack render from "
                f"{pl['project']}: change that and render again, not this file.\n")
        with open(f, "w") as fh:
            fh.write(head + yaml.safe_dump(resources(m, archive), sort_keys=False, width=120))
        written.append(f)
        md = os.path.join(out, "marts", m["pack"])
        os.makedirs(md, exist_ok=True)
        for name in m["marts"]:
            dst = os.path.join(md, name)
            if not os.path.exists(dst):
                shutil.copyfile(os.path.join(m["pack_dir"], name), dst)
                written.append(dst)
    f = os.path.join(out, "resources", "_control.yml")
    with open(f, "w") as fh:
        fh.write("# The control schema: every pipeline's event log, every gateway's staging volume, the checksums and the\n"
                 "# merge watermarks, for the dashboard. Written by lfc-medallion-pack render.\n" + yaml.safe_dump(
                     {"resources": {"schemas": {"control": _schema("control", "The lakehouse's control schema: event "
                                                                  "logs, gateway staging, checksums, watermarks")}}},
                     sort_keys=False, width=120))
    written.append(f)
    from . import dashboard   # the dashboard over the control schema's views, and its bundle resource
    os.makedirs(os.path.join(out, "dashboards"), exist_ok=True)
    f = os.path.join(out, "dashboards", "lakehouse.lvdash.json")
    with open(f, "w") as fh:
        json.dump(dashboard.build(os.path.splitext(pl["project"])[0]), fh, indent=1)
    written.append(f)
    f = os.path.join(out, "resources", "_dashboard.yml")
    with open(f, "w") as fh:
        fh.write("# The lakehouse's dashboard: rows through each stage, pipeline updates, latency, checksums (the control\n"
                 "# schema's views). Written by lfc-medallion-pack render.\n" + yaml.safe_dump({"resources": {
                     "dashboards": {"lakehouse": {
                         "display_name": "${var.prefix} lakehouse: stages, pipelines, latency, checksums",
                         "file_path": "../dashboards/lakehouse.lvdash.json", "warehouse_id": "${var.warehouse_id}",
                         "dataset_catalog": "${var.catalog}", "dataset_schema": "${resources.schemas.control.name}"}}}},
                     sort_keys=False, width=120))
    written.append(f)
    keep = dict(pl)
    keep["modules"] = [{k: v for k, v in m.items() if k != "pack_dir"} for m in pl["modules"]]
    keep["archive"] = archive   # the source job installs from it too (setup_source), so the project file names it once
    os.makedirs(os.path.join(out, "source"), exist_ok=True)
    with open(os.path.join(out, "source", "plan.json"), "w") as fh:
        json.dump(keep, fh, indent=1)
    written.append(os.path.join(out, "source", "plan.json"))
    return {"written": written, "slots": pl["slots"],
            "modules": {m["module"]: {"lobs": len(m["lobs"]), "pipelines": len(m["groups"]),
                                      "tables": sum(g["tables"] for g in m["groups"])} for m in pl["modules"]}}
