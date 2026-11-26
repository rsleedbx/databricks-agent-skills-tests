"""canonical: a collector's raw document (collect-postgres.sql, collect-mysql.sql through mysql-normalize.jq, or
lfc-introspection's catalog reads) -> statschema's canonical schema and stats documents. to-canonical.py is its command
line; the field rules are in its header."""
from .canon import canon


def convert(raw: dict) -> tuple:
    """(schema, stats): the two canonical documents, as dicts, from a raw collector document."""
    src = raw["source_dialect"]
    schema, stats = [], []
    for t in raw["tables"]:
        pk, rows = t.get("primary_key") or [], int(t.get("row_count") or 0)
        uniq = {u[0] for u in (t.get("unique") or []) if len(u) == 1}
        cols, cstats = [], []
        for c in t["columns"]:
            col = {"name": c["name"], **canon(c["data_type"], src), "not_null": not c.get("nullable", True), "native_type": c["data_type"]}
            if c["name"] in pk: col["primary_key"] = True
            if c["name"] in uniq: col["unique"] = True
            if c.get("generated"): col["generated"] = c["generated"]   # computed by the database: stored or virtual
            if c.get("default") is not None: col["default"] = str(c["default"])   # a SQL literal (MySQL's collector writes it so)
            cols.append(col)
            nf = c.get("null_fraction") or 0.0
            s = {"name": c["name"], "null_fraction": nf, "n_distinct": c.get("n_distinct") or 0.0}
            for k in ("avg_width_bytes", "min_value", "max_value", "correlation", "histogram_bounds"):
                if c.get(k) is not None: s[k] = c[k]
            if c.get("most_common_values"):
                s["most_common_values"] = [{"value": m.get("value"), "frequency": round(m["frequency"] / max(1 - nf, 1e-9), 6)}
                                           for m in c["most_common_values"]]
            cstats.append(s)
        tbl = {"name": t["name"], "row_count": rows, "columns": cols}
        if t.get("comment"): tbl["description"] = t["comment"]
        fks = t.get("foreign_keys") or []
        if fks:
            # on_delete / on_update only where the key acts (CASCADE, SET NULL, SET DEFAULT): a CDC log may not carry what
            # the action changes (MySQL through 9.5, MariaDB), so silver applies it (zerobus-debezium-test's cdc.fk_actions)
            act = lambda f, k: {k: f[k]} if f.get(k) and f[k] not in ("NO ACTION", "RESTRICT") else {}   # noqa: E731
            tbl["fk_constraints"] = [{"columns": f["columns"], "parent_table": f["ref_table"], "parent_columns": f["ref_columns"],
                                      "fk_distribution": "uniform", **act(f, "on_delete"), **act(f, "on_update")} for f in fks]
            tbl["load_after"] = sorted({f["ref_table"] for f in fks if f["ref_table"] != t["name"]})
        schema.append(tbl)
        by = {s["name"]: s for s in cstats}
        ts = {"name": t["name"], "row_count": rows, "columns": cstats}
        if t.get("size_bytes"): ts["data_size_bytes"] = t["size_bytes"]
        ix = [{"name": f"{t['name']}_ix{i + 1}", "columns": cs, "unique": False} for i, cs in enumerate(t.get("indexes") or [])]
        ix += [{"name": f"{t['name']}_uq{i + 1}", "columns": cs, "unique": True} for i, cs in enumerate(t.get("unique") or [])]
        if ix: ts["indexes"] = ix
        if fks:                                                     # children per parent, from the FK column's distinct count
            ts["foreign_keys"] = []
            for f in fks:
                nd = (by.get(f["columns"][0]) or {}).get("n_distinct") or 0
                nd = -nd * rows if nd < 0 else nd
                ts["foreign_keys"].append({"columns": f["columns"], "parent_table": f["ref_table"], "parent_columns": f["ref_columns"],
                                           **({"avg_children_per_parent": round(rows / nd, 3)} if nd else {})})
        stats.append(ts)
    return ({"version": "1.0", "tables": schema},
            {"version": "1.0", "source_dialect": src, "values": raw.get("values", "none"), "tables": stats})


def write(schema: dict, stats: dict, schema_path: str, stats_path: str) -> None:
    """The two documents as statschema's files (schema.yaml, stats.yaml)."""
    import yaml
    for doc, path in ((schema, schema_path), (stats, stats_path)):
        with open(path, "w") as f:
            yaml.safe_dump(doc, f, sort_keys=False, allow_unicode=True)
