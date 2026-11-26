#!/usr/bin/env python3
"""medallion.py: the layers after the landing tables, counted per layer, as text and Mermaid; one module for Lakeflow
Connect and Zerobus.

Layer 1 is where the data lands (the Lakeflow Connect destination tables, or the table Zerobus appends to); each next
layer is one or more schemas. Each layer is counted: tables in all, materialized (streaming tables, materialized views,
Delta tables) and not (views), and by how each is written: SCD 1, SCD 2, append-only, recomputed (materialized view),
keyed (MERGE / update flows), or written outside a pipeline. The last box is reverse ETL out to a database.

How a table is written comes from its pipeline's flow_definition events (flow_type CHANGE / SNAPSHOT_CHANGE = AUTO CDC,
APPEND, MATERIALIZED_VIEW, VIEW, MERGE, ...): SCD 2 when its columns have __START_AT, else SCD 1. A table outside a
pipeline: SCD 2 by its columns, else what --written says for its layer (default: unknown).

usage: medallion.py --layer NAME=CATALOG.SCHEMA[,CATALOG.SCHEMA...][:only=t1,t2] ... [--profile P] [options]
       medallion.py --layers-json layers.json [options]      (saved layers: the output of --save)
  --layer        repeat in flow order; the first is the landing layer
  --written NAME=append|scd1|scd2|merge   how the tables of a layer written outside a pipeline are written (Zerobus
                 bronze: append)
  --reverse-etl TARGET[:METHOD[:TABLES]]  e.g. "Lakebase Postgres:synced tables:3", "SQL Server:JDBC MERGE job"
  --format text|mermaid|both   --save FILE (the counted layers as JSON, for --layers-json)
"""
import argparse
import json
import sys

import workspace_api as api  # beside this script

KINDS = ("scd1", "scd2", "append", "recomputed", "keyed", "view", "unknown")
LABEL = {"scd1": "SCD 1", "scd2": "SCD 2", "append": "append-only", "recomputed": "recomputed (MV)",
         "keyed": "keyed MERGE/update", "view": "views", "unknown": "written outside a pipeline"}
MATERIALIZED = {"STREAMING_TABLE", "MATERIALIZED_VIEW", "MANAGED", "EXTERNAL", "FOREIGN"}
WRITTEN = {"append": "append", "scd1": "scd1", "scd2": "scd2", "merge": "keyed"}   # --written values -> KINDS
FLOW_KIND = {"APPEND": "append", "COMPLETE": "recomputed", "MATERIALIZED_VIEW": "recomputed", "VIEW": "view",
             "MERGE": "keyed", "UPDATE": "keyed", "REPLACE_USING": "keyed", "REPLACE_WHERE": "keyed"}


def norm(name):
    """A table name as flow events and tables list compare: no backticks, lower case."""
    return (name or "").replace("`", "").lower()


def flow_types(pipeline_id, profile, cache, max_pages=40):
    """{output table: flow_type} from the flow_definition events of the pipeline's newest update that has them.

    The events API can't filter by event type, and an update writes its flow_definition events when it starts, so a
    busy pipeline's are many pages back: page back (newest first) to the first flow_definition, then read the rest of
    that update's events, and stop at the first event of an older update (or after max_pages pages of 250, the API's
    most per page)."""
    if pipeline_id in cache:
        return cache[pipeline_id]
    out, update, token = {}, None, None
    for _ in range(max_pages):
        q = {"max_results": 250, **({"page_token": token} if token else {})}
        try:
            ev = api.get(f"/api/2.0/pipelines/{pipeline_id}/events", profile, q)
        except api.ApiError:
            break
        done = False
        for e in ev.get("events", []):
            uid = (e.get("origin") or {}).get("update_id")
            if update is not None and uid and uid != update:
                done = True                                 # an older update: the newest one's definitions are read
                break
            fd = (e.get("details") or {}).get("flow_definition")
            if fd and fd.get("output_dataset"):
                update = update or uid
                out.setdefault(norm(fd["output_dataset"]), fd.get("flow_type"))
        token = ev.get("next_page_token")
        if done or not token:
            break
    cache[pipeline_id] = out
    return out


def kind_of(t, flows, written):
    """How one table is written: one of KINDS."""
    ttype = t.get("table_type") or ""
    if ttype in ("VIEW", "METRIC_VIEW"):
        return "view"
    scd2 = t.get("scd2")
    ft = flows.get(norm(t["full_name"])) if flows is not None else None
    if ft in ("CHANGE", "SNAPSHOT_CHANGE"):
        return "scd2" if scd2 else "scd1"
    if ft in FLOW_KIND:
        return FLOW_KIND[ft]
    if ttype == "MATERIALIZED_VIEW":
        return "recomputed"
    if t.get("lfc"):                                       # a Lakeflow Connect destination: AUTO CDC by the connector
        return "scd2" if scd2 else "scd1"
    if scd2:
        return "scd2"
    return written or "unknown"


def count_layer(name, schemas, profile, written=None, only=None, cache=None):
    """List each schema's tables and count them."""
    cache = {} if cache is None else cache
    tables, unreadable = [], []
    for cs in schemas:
        cat, sch = cs.split(".", 1)
        try:
            listed = api.tables(cat, sch, profile)
        except api.ApiError as e:                        # no access, or gone: draw the layer as not readable
            unreadable.append(f"{cs}: {str(e)[:120] or 'tables list failed'}")
            continue
        for t in listed:
            if only and t["name"] not in only:
                continue
            props = t.get("properties") or {}
            cols = [c["name"] for c in t.get("columns") or []]
            row = {"full_name": f"{cat}.{sch}.{t['name']}", "table_type": t.get("table_type"),
                   "scd2": "__START_AT" in cols and "__END_AT" in cols,
                   "lfc": any(k.startswith("__managed_ingestion.") for k in props)}
            pid = props.get("pipelines.pipelineId") or t.get("pipeline_id")
            row["kind"] = kind_of(row, flow_types(pid, profile, cache) if pid else None, written)
            tables.append(row)
    lay = summarize(name, schemas, tables)
    if unreadable:
        lay["unreadable"] = unreadable
    return lay


def summarize(name, schemas, tables):
    counts = {k: sum(1 for t in tables if t["kind"] == k) for k in KINDS}
    mat = sum(1 for t in tables if (t.get("table_type") or "") in MATERIALIZED and t["kind"] != "view")
    return {"name": name, "schemas": schemas, "tables": len(tables), "materialized": mat,
            "not_materialized": len(tables) - mat, "by_kind": {k: v for k, v in counts.items() if v}}


def landing(name, tables_kinds, table_type="STREAMING_TABLE"):
    """Layer 1 from what the ingestion already knows: [(full table name, kind)], no workspace call."""
    tables = [{"full_name": t, "table_type": table_type, "kind": k} for t, k in tables_kinds]
    schemas = sorted({t.rsplit(".", 1)[0] for t, _ in tables_kinds})
    return summarize(name, schemas, tables)


def parse_layer(spec):
    name, _, rest = spec.partition("=")
    rest, _, only = rest.partition(":only=")
    return name, [s for s in rest.split(",") if s], set(only.split(",")) if only else None


def parse_reverse(spec):
    if not spec:
        return None
    parts = spec.split(":")
    return {"target": parts[0], "method": parts[1] if len(parts) > 1 else "",
            "tables": int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None}


def n_tables(n):
    return f"{n} table" + ("" if n == 1 else "s")


def layer_line(lay):
    if lay.get("unreadable") and not lay["tables"]:
        return "not readable (" + "; ".join(lay["unreadable"]) + ")"
    return counts_line(lay) + (f"; not readable: {'; '.join(lay['unreadable'])}" if lay.get("unreadable") else "")


def counts_line(lay):
    kinds = ", ".join(f"{n} {LABEL[k]}" for k, n in lay["by_kind"].items() if k != "view")
    views = lay["by_kind"].get("view", 0)
    mat = f"{lay['materialized']} materialized" + (f" ({kinds})" if kinds else "")
    nm = f", {lay['not_materialized']} not materialized" + (f" ({views} view{'' if views == 1 else 's'})" if views else "") \
        if lay["not_materialized"] else ""
    return f"{n_tables(lay['tables'])}: {mat}{nm}"


def text(layers, reverse, indent="  "):
    if not layers:
        return []
    L = [f"{indent}medallion (layer 1 = the landing tables)"]
    w = max(len(x["name"]) for x in layers)
    for i, lay in enumerate(layers, 1):
        L.append(f"{indent}  L{i} {lay['name']:<{w}}  {', '.join(lay['schemas'])}: {layer_line(lay)}")
    if reverse:
        L.append("        |")
        n = f": {n_tables(reverse['tables'])}" if reverse["tables"] is not None else ""
        L.append(f"{indent}reverse ETL -> {reverse['target']}" + (f" ({reverse['method']})" if reverse["method"] else "") + n)
    return L


def q(s):
    """A Mermaid label: double quotes become single, and < > other than the <br/> line breaks are escaped (Mermaid
    drops what looks like an HTML tag, such as a <slot> or <catalog> placeholder)."""
    t = str(s).replace('"', "'").replace("<br/>", "\0").replace("<", "&lt;").replace(">", "&gt;").replace("\0", "<br/>")
    return '"' + t + '"'


def mermaid(layers, reverse, attach_from=None, indent="  ", first_id="L1", first_text=""):
    """(lines of a subgraph MED with one node per layer L1..Ln and attach_from -> L1, lines of the reverse ETL node RE
    and Ln -> RE). The caller places them: MED inside the workspace box, RE outside it."""
    if not layers:
        return [], []
    # first_id: the node layer 1 is drawn as (the caller's landing node, so the landing tables appear once)
    ids = [first_id] + [f"L{i}" for i in range(2, len(layers) + 1)]
    L = [f"{indent}subgraph MED[{q('medallion')}]"]
    for i, (nid, lay) in enumerate(zip(ids, layers), 1):
        kinds = "<br/>".join(f"{n} {LABEL[k]}" for k, n in lay["by_kind"].items()) or \
            ("not readable" if lay.get("unreadable") else "")
        head = f"L{i} {lay['name']}: {n_tables(lay['tables'])}" + (f"<br/>{first_text}" if i == 1 and first_text else "")
        L.append(f"{indent}  {nid}[({q(head + '<br/>' + kinds)})]")
    if len(ids) > 1:
        L.append(f"{indent}  " + " --> ".join(ids))
    L.append(f"{indent}end")
    if attach_from and attach_from != first_id:
        L.append(f"{indent}{attach_from} --> {first_id}")
    R = []
    if reverse:
        n = f"<br/>{n_tables(reverse['tables'])}" if reverse["tables"] is not None else ""
        m = f"<br/>{reverse['method']}" if reverse["method"] else ""
        R.append(f"  RE[({q('reverse ETL: ' + reverse['target'] + m + n)})]")
        R.append(f"  {ids[-1]} --> RE")
    return L, R


def collect(a):
    if a.layers_json:
        with open(a.layers_json) as f:
            return json.load(f)
    written = {}
    for w in a.written or []:
        name, _, how = w.partition("=")
        how = WRITTEN.get(how)
        if how is None:
            raise SystemExit(f"medallion: --written {w}: how must be one of {', '.join(WRITTEN)}")
        written[name] = how
    cache = {}
    out = []
    for spec in a.layer or []:
        name, schemas, only = parse_layer(spec)
        out.append(count_layer(name, schemas, a.profile, written.get(name), only, cache))
    return out


def add_args(p):
    p.add_argument("--layer", action="append", help="NAME=CATALOG.SCHEMA[,...][:only=t1,t2], in flow order")
    p.add_argument("--written", action="append", help="NAME=append|scd1|scd2|merge for tables outside a pipeline")
    p.add_argument("--layers-json", help="saved layers (from --save) instead of reading the workspace")
    p.add_argument("--reverse-etl", help="TARGET[:METHOD[:TABLES]]")


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    add_args(p)
    p.add_argument("--profile"); p.add_argument("--save")
    p.add_argument("--format", choices=["text", "mermaid", "both"], default="both")
    a = p.parse_args()
    if not (a.layer or a.layers_json):
        print(__doc__, file=sys.stderr); sys.exit(2)
    layers = collect(a)
    if a.save:
        with open(a.save, "w") as f:
            json.dump(layers, f, indent=1)
    rev = parse_reverse(a.reverse_etl)
    if a.format in ("text", "both"):
        print("\n".join(text(layers, rev, indent="")))
    if a.format in ("mermaid", "both"):
        med, re_ = mermaid(layers, rev)
        print("flowchart TD\n" + "\n".join(med + re_))


if __name__ == "__main__":
    main()
