#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["pyyaml", "sqlglot"]
# ///
# uv run ddl-to-schema.py --dialect mysql < schema.sql > schema.yaml
# (scripts/ddl-to-schema.py is a wrapper over this file, ddl_to_schema.py; from Python: statschema.tools.ddl_to_schema(...))
# DDL in any sqlglot dialect (CREATE TABLE, COMMENT ON, ALTER TABLE ... ADD FOREIGN KEY) -> statschema's schema.yaml:
# columns in canonical types (canon.py), NOT NULL, defaults, primary, unique, and foreign keys, CHECK constraints, and
# comments. Comments that hold lfc tags stay tags: a column's become its comment (lfc:v, lfc:e, lfc:len), a table's its
# lfc_tags (lfc:rows also sets row_count), so a schema written with formulas (the TPC spec schemas) is ready for ddl.py.
# Checks and formulas are kept in MySQL syntax, the form ddl.py reads.
import argparse, os, re, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sqlglot, yaml
from sqlglot import exp
from canon import canon

ap = argparse.ArgumentParser(); ap.add_argument("--dialect", required=True); a = ap.parse_args()
D = {"postgresql": "postgres", "sqlserver": "tsql"}.get(a.dialect, a.dialect)
tables, order = {}, []
def my(e): return e.sql("mysql")                       # checks and formulas: MySQL syntax, as ddl.py expects

def fk_of(f, tname):
    ref = f.args["reference"].this                      # Schema(this=Table, expressions=[cols])
    return {"columns": [c.name for c in f.expressions], "parent_table": ref.this.name,
            "parent_columns": [c.name for c in ref.expressions], "fk_distribution": "uniform"}

for st in sqlglot.parse(sys.stdin.read(), read=D):
    if st is None: continue
    if isinstance(st, exp.Create) and st.kind == "TABLE":
        name = st.this.this.name; cols, pk, fks, checks = [], [], [], []
        for d in st.this.expressions:
            if isinstance(d, exp.ColumnDef):
                c = {"name": d.name, **canon(d.args["kind"], D)}
                for k in d.constraints:
                    kd = k.kind
                    if isinstance(kd, exp.NotNullColumnConstraint) and not kd.args.get("allow_null"): c["not_null"] = True
                    elif isinstance(kd, exp.PrimaryKeyColumnConstraint): c["primary_key"] = True; c["not_null"] = True
                    elif isinstance(kd, exp.UniqueColumnConstraint): c["unique"] = True
                    elif isinstance(kd, exp.DefaultColumnConstraint): c["default"] = kd.this.sql(D)
                    elif isinstance(kd, (exp.GeneratedAsIdentityColumnConstraint, exp.AutoIncrementColumnConstraint)): c["auto_increment"] = True
                    elif isinstance(kd, exp.CommentColumnConstraint): c["comment"] = kd.this.name
                    elif isinstance(kd, exp.CheckColumnConstraint): checks.append(my(kd.this))
                c["native_type"] = d.args["kind"].sql(D)
                cols.append(c)
            elif isinstance(d, exp.PrimaryKey): pk = [x.name for x in d.expressions]
            elif isinstance(d, exp.ForeignKey): fks.append(fk_of(d, name))
            elif isinstance(d, exp.Constraint):
                for x in d.expressions:
                    if isinstance(x, exp.ForeignKey): fks.append(fk_of(x, name))
                    elif isinstance(x, exp.PrimaryKey): pk = [y.name for y in x.expressions]
            elif isinstance(d, (exp.CheckColumnConstraint, exp.Check)): checks.append(my(d.this))
        for c in cols:
            if c["name"] in pk: c["primary_key"] = True; c["not_null"] = True
        t = {"name": name, "columns": cols}
        props = st.args.get("properties")
        for p in (props.expressions if props else []):
            if isinstance(p, exp.SchemaCommentProperty): t["_comment"] = p.this.name
        if fks: t["fk_constraints"] = fks
        if checks: t["checks"] = checks
        tables[name] = t; order.append(name)
    elif isinstance(st, exp.Comment):                   # COMMENT ON TABLE / COLUMN
        txt = st.expression.name
        if st.args.get("kind") == "TABLE": tables[st.this.name]["_comment"] = txt
        elif st.args.get("kind") == "COLUMN":
            col = st.this; t = tables[col.table]
            next(c for c in t["columns"] if c["name"] == col.name)["comment"] = txt
    elif isinstance(st, exp.Alter):                     # ALTER TABLE t ADD FOREIGN KEY ...
        for act in st.args.get("actions") or []:
            for f in act.find_all(exp.ForeignKey):
                tables[st.this.name].setdefault("fk_constraints", []).append(fk_of(f, st.this.name))

out = []
for n in order:
    t = tables[n]; cm = t.pop("_comment", None)
    if cm and "lfc:" in cm:
        t["lfc_tags"] = cm
        m = re.search(r"lfc:rows=(\d+)", cm)
        if m: t["row_count"] = int(m.group(1))
    elif cm: t["description"] = cm
    if t.get("fk_constraints"): t["load_after"] = sorted({f["parent_table"] for f in t["fk_constraints"] if f["parent_table"] != n})
    out.append(t)
yaml.safe_dump({"version": "1.0", "tables": out}, sys.stdout, sort_keys=False, allow_unicode=True, width=10**9)
print(f"{len(out)} tables", file=sys.stderr)
