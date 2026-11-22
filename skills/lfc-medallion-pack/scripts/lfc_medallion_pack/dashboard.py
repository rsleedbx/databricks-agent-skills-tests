"""dashboard: an AI/BI dashboard over the control schema's views (observe.py): the rows through each stage by table,
LOB, pipeline and update, each update's phases, the latency from a batch to bronze, the n:1 merge and gold, and the
checksums. It reads only the views, so it is the same for every model; render writes it with the bundle
(dashboards/lakehouse.lvdash.json, resources/_dashboard.yml). Queries name tables bare: the bundle gives the catalog
and the control schema (dataset_catalog, dataset_schema).
"""
from __future__ import annotations

ROWS = ("CASE WHEN stage = '3 n1 merge' THEN COALESCE(inserted, 0) + COALESCE(updated, 0) + COALESCE(deleted, 0) "
        "WHEN output_rows IS NOT NULL THEN output_rows ELSE COALESCE(upserted, 0) + COALESCE(deleted, 0) END")

DATASETS = {
    "ds_changes": [
        "SELECT stage, module, pipeline, update_id, at, date_trunc('HOUR', at) AS hour, ",
        "COALESCE(lob, '(all LOBs)') AS lob, table_name, kind, ",
        "COALESCE(inserted, 0) AS inserted, COALESCE(updated, 0) AS updated, COALESCE(upserted, 0) AS upserted, ",
        "COALESCE(deleted, 0) AS deleted, ",
        "CASE WHEN kind = 'snapshot' THEN COALESCE(upserted, output_rows, 0) ELSE 0 END AS snapshot_rows, ",
        f"{ROWS} AS rows, ",
        f"CASE WHEN stage = '1 source capture' THEN {ROWS} ELSE 0 END AS capture_rows, ",
        f"CASE WHEN stage = '2 bronze' THEN {ROWS} ELSE 0 END AS bronze_rows, ",
        f"CASE WHEN stage LIKE '3 n1%' THEN {ROWS} ELSE 0 END AS n1_rows, ",
        f"CASE WHEN stage IN ('4 silver', '5 gold') THEN {ROWS} ELSE 0 END AS mart_rows ",
        "FROM v_changes"],
    "ds_updates": [
        "SELECT module, pipeline, update_id, started, ended, state, seconds, ",
        "COALESCE(waiting_for_resources_s, 0) AS waiting_s, COALESCE(initializing_s, 0) AS initializing_s, ",
        "COALESCE(setting_up_tables_s, 0) AS setting_up_s, COALESCE(running_s, 0) AS running_s ",
        "FROM v_updates ORDER BY started DESC"],
    "ds_latency": [
        "SELECT module, lob, batch_at, rows, ",
        "ROUND((unix_millis(bronze_at) - unix_millis(batch_at)) / 1000.0, 1) AS to_bronze_s, ",
        "ROUND((unix_millis(merge_at) - unix_millis(batch_at)) / 1000.0, 1) AS to_merge_s, ",
        "ROUND((unix_millis(gold_at) - unix_millis(batch_at)) / 1000.0, 1) AS to_gold_s ",
        "FROM v_latency ORDER BY batch_at DESC"],
    "ds_checksums": [
        "SELECT module, lob, `table` AS table_name, status, CASE WHEN status = 'match' THEN 1 ELSE 0 END AS matched, ",
        "source_rows, bronze_rows, n1_rows, source_digest, bronze_digest, n1_digest, checked_at ",
        "FROM v_checksums ORDER BY status DESC, module, lob, table_name"],
}

THEME = {"canvasBackgroundColor": {"light": "#FCFCFC", "dark": "#1F272D"},
         "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#11171C"},
         "fontColor": {"light": "#11171C", "dark": "#E8ECF0"},
         "selectionColor": {"light": "#2272B4", "dark": "#8ACAFF"},
         "visualizationColors": ["#0072B2", "#E69F00", "#009E73", "#CC79A7", "#D55E00", "#56B4E9", "#F0E442"],
         "widgetHeaderAlignment": "LEFT"}
COUNT = {"type": "number", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}}


def _text(name: str, lines: list[str], y: int, h: int = 2) -> dict:
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": lines}},
            "position": {"x": 0, "y": y, "width": 12, "height": h}}


def _f(name: str, expr: str) -> dict:
    return {"name": name, "expression": expr}


def _widget(name: str, ds: str, fields: list[dict], spec: dict, pos: tuple, disaggregated: bool = False) -> dict:
    x, y, w, h = pos
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {
        "datasetName": ds, "fields": fields, "disaggregated": disaggregated}}], "spec": spec},
        "position": {"x": x, "y": y, "width": w, "height": h}}


def _counter(name: str, ds: str, col: str, title: str, pos: tuple, fmt: dict | None = COUNT) -> dict:
    fn = f"sum({col})"
    value = {"fieldName": fn, "displayName": title} | ({"format": fmt} if fmt else {})
    return _widget(name, ds, [_f(fn, f"SUM(`{col}`)")],
                   {"version": 2, "widgetType": "counter", "encodings": {"value": value},
                    "frame": {"showTitle": True, "title": title}}, pos)


def _filter(name: str, ds: str, col: str, title: str, pos: tuple, kind: str = "filter-multi-select") -> dict:
    x, y, w, h = pos
    q = f"{name}_q"
    return {"widget": {"name": name, "queries": [{"name": q, "query": {
        "datasetName": ds, "fields": [_f(col, f"`{col}`")], "disaggregated": False}}],
        "spec": {"version": 2, "widgetType": kind,
                 "encodings": {"fields": [{"fieldName": col, "displayName": title, "queryName": q}]},
                 "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}


def _table(name: str, ds: str, cols: list[tuple], title: str, pos: tuple, aggregate: dict | None = None) -> dict:
    """A table of `cols` [(column, title)]; aggregate {column: SUM|MAX|MIN} groups by the others."""
    aggregate = aggregate or {}
    fields, enc = [], []
    for c, t in cols:
        if c in aggregate:
            fn = f"{aggregate[c].lower()}({c})"
            fields.append(_f(fn, f"{aggregate[c]}(`{c}`)"))
        else:
            fn = c
            fields.append(_f(c, f"`{c}`"))
        enc.append({"fieldName": fn, "displayName": t})
    return _widget(name, ds, fields, {"version": 2, "widgetType": "table", "encodings": {"columns": enc},
                                      "frame": {"showTitle": True, "title": title}}, pos,
                   disaggregated=not aggregate)


def _chart(name: str, kind: str, ds: str, x: tuple, ys: list[tuple], title: str, pos: tuple,
           color: str | None = None) -> dict:
    """A line or bar chart: x (column, scale type), ys [(column, title)] summed, split by color (a column)."""
    xc, xt = x
    fields = [_f(xc, f"`{xc}`")] + [_f(f"sum({c})", f"SUM(`{c}`)") for c, _ in ys]
    enc = {"x": {"fieldName": xc, "scale": {"type": xt}, "displayName": xc}}
    if len(ys) == 1:
        enc["y"] = {"fieldName": f"sum({ys[0][0]})", "scale": {"type": "quantitative"}, "displayName": ys[0][1]}
    else:
        enc["y"] = {"scale": {"type": "quantitative"},
                    "fields": [{"fieldName": f"sum({c})", "displayName": t} for c, t in ys]}
    if color:
        fields.append(_f(color, f"`{color}`"))
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color}
    return _widget(name, ds, fields, {"version": 3, "widgetType": kind, "encodings": enc,
                                      "frame": {"showTitle": True, "title": title}}, pos)


def _page(name: str, title: str, layout: list[dict]) -> dict:
    return {"name": name, "displayName": title, "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1",
            "layout": layout}


def build(project: str) -> dict:
    """The dashboard (serialized_dashboard's JSON) for a project: four pages over the control views."""
    changes = _page("changes", "Changes", [
        _text("changes_title", [f"## {project}: rows through each stage\n", "\n",
                                "Source capture (the gateway), bronze (ingestion), n:1 (the merge route or the "
                                "pipeline), silver and gold, by table, LOB, pipeline and update."], 0),
        _filter("f_module", "ds_changes", "module", "Module", (0, 2, 3, 2)),
        _filter("f_lob", "ds_changes", "lob", "LOB", (3, 2, 3, 2)),
        _filter("f_table", "ds_changes", "table_name", "Table", (6, 2, 3, 2)),
        _filter("f_at", "ds_changes", "at", "When", (9, 2, 3, 2), "filter-date-range-picker"),
        _counter("k_capture", "ds_changes", "capture_rows", "Captured from the source", (0, 4, 3, 3)),
        _counter("k_bronze", "ds_changes", "bronze_rows", "Into bronze", (3, 4, 3, 3)),
        _counter("k_n1", "ds_changes", "n1_rows", "Into n:1 (merge and pipeline)", (6, 4, 3, 3)),
        _counter("k_snapshot", "ds_changes", "snapshot_rows", "Snapshot rows", (9, 4, 3, 3)),
        _chart("c_over_time", "line", "ds_changes", ("hour", "temporal"), [("rows", "Rows")],
               "Rows by stage, per hour", (0, 7, 12, 6), color="stage"),
        _chart("c_by_table", "bar", "ds_changes", ("table_name", "categorical"), [("rows", "Rows")],
               "Rows by table and stage", (0, 13, 6, 6), color="stage"),
        _chart("c_by_kind", "bar", "ds_changes", ("stage", "categorical"),
               [("inserted", "Inserted"), ("updated", "Updated"), ("upserted", "Upserted"), ("deleted", "Deleted"),
                ("snapshot_rows", "Snapshot")], "Inserts, updates, deletes and snapshots by stage", (6, 13, 6, 6)),
        _table("t_by_lob", "ds_changes",
               [("module", "Module"), ("lob", "LOB"), ("table_name", "Table"), ("stage", "Stage"),
                ("inserted", "Inserted"), ("updated", "Updated"), ("upserted", "Upserted"), ("deleted", "Deleted"),
                ("snapshot_rows", "Snapshot"), ("rows", "Rows")], "By LOB, table and stage", (0, 19, 12, 8),
               aggregate={c: "SUM" for c in ("inserted", "updated", "upserted", "deleted", "snapshot_rows", "rows")}),
    ])
    pipelines = _page("pipelines", "Pipelines", [
        _text("pipelines_title", ["## Pipeline updates\n", "\n",
                                  "Each update of each gateway, ingestion and transform pipeline: its state and the "
                                  "seconds in each phase."], 0),
        _filter("f_pipeline", "ds_updates", "pipeline", "Pipeline", (0, 2, 6, 2)),
        _filter("f_started", "ds_updates", "started", "Started", (6, 2, 6, 2), "filter-date-range-picker"),
        _chart("c_phases", "bar", "ds_updates", ("pipeline", "categorical"),
               [("waiting_s", "Waiting for resources"), ("initializing_s", "Initializing"),
                ("setting_up_s", "Setting up tables"), ("running_s", "Running")],
               "Seconds in each phase, all updates", (0, 4, 12, 6)),
        _table("t_updates", "ds_updates",
               [("pipeline", "Pipeline"), ("started", "Started"), ("state", "State"), ("seconds", "Seconds"),
                ("waiting_s", "Waiting"), ("initializing_s", "Initializing"), ("setting_up_s", "Setting up"),
                ("running_s", "Running")], "Updates", (0, 10, 12, 8)),
    ])
    latency = _page("latency", "Latency", [
        _text("latency_title", ["## Latency from a batch of changes\n", "\n",
                                "Seconds from each batch written to the source until its LOB's rows next reached "
                                "bronze, the n:1 merge, and gold (the transform's next completed update)."], 0),
        _filter("f_lat_lob", "ds_latency", "lob", "LOB", (0, 2, 6, 2)),
        _filter("f_lat_at", "ds_latency", "batch_at", "Batch", (6, 2, 6, 2), "filter-date-range-picker"),
        _chart("c_latency", "bar", "ds_latency", ("lob", "categorical"),
               [("to_bronze_s", "To bronze"), ("to_merge_s", "To the n:1 merge"), ("to_gold_s", "To gold")],
               "Seconds to each layer, per LOB (summed over the batches shown)", (0, 4, 12, 6)),
        _table("t_latency", "ds_latency",
               [("module", "Module"), ("lob", "LOB"), ("batch_at", "Batch"), ("rows", "Rows"),
                ("to_bronze_s", "To bronze (s)"), ("to_merge_s", "To merge (s)"), ("to_gold_s", "To gold (s)")],
               "Batches", (0, 10, 12, 8)),
    ])
    checks = _page("checksums", "Checksums", [
        _text("checks_title", ["## Checksums: source, bronze and n:1\n", "\n",
                               "Rows and an MD5 digest of every row, per table and LOB, from the latest check."], 0),
        _filter("f_ck_module", "ds_checksums", "module", "Module", (0, 2, 4, 2)),
        _filter("f_ck_status", "ds_checksums", "status", "Status", (4, 2, 4, 2)),
        _filter("f_ck_table", "ds_checksums", "table_name", "Table", (8, 2, 4, 2)),
        _counter("k_ck_matched", "ds_checksums", "matched", "Tables matching", (0, 4, 4, 3), None),
        _counter("k_ck_rows", "ds_checksums", "source_rows", "Source rows checked", (4, 4, 4, 3)),
        _counter("k_ck_n1", "ds_checksums", "n1_rows", "n:1 rows checked", (8, 4, 4, 3)),
        _table("t_checksums", "ds_checksums",
               [("module", "Module"), ("lob", "LOB"), ("table_name", "Table"), ("status", "Status"),
                ("source_rows", "Source rows"), ("bronze_rows", "Bronze rows"), ("n1_rows", "n:1 rows"),
                ("source_digest", "Source digest"), ("bronze_digest", "Bronze digest"), ("n1_digest", "n:1 digest"),
                ("checked_at", "Checked")], "Every table of every LOB", (0, 7, 12, 9)),
    ])
    return {"datasets": [{"name": n, "displayName": n[3:], "queryLines": q} for n, q in DATASETS.items()],
            "pages": [changes, pipelines, latency, checks], "uiSettings": {"theme": THEME}}
