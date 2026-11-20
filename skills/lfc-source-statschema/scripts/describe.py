#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["pyyaml"]
# ///
# uv run describe.py schema.yaml... [--json]
# A model at a glance: its tables by kind (fact, dimension) and role, each with its rows per scale factor or fixed rows,
# then the model's signals (patterns a model can learn that the source build writes, with their parameters). Loads the
# file with statschema's own model, as validate.py does; a table without role: is listed as unknown.
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import yaml  # noqa: E402

from statschema.core.model import ROLES  # noqa: E402
from statschema.core.schema_io import load_canonical  # noqa: E402


def num(v: float) -> str:
    return f"{int(v):,}" if float(v).is_integer() else f"{v:g}"


def describe(path: str) -> dict:
    tables = load_canonical(path)
    signals = (yaml.safe_load(open(path)) or {}).get("signals") or []
    rows = [{"table": t.name, "role": t.role or "unknown", "kind": t.kind or "unknown",
             "rows": (f"{num(t.row_count_per_sf)} x SF" if t.row_count_per_sf is not None
                      else num(t.row_count) if t.row_count is not None else "")} for t in tables]
    order = {r: i for i, r in enumerate(ROLES)}
    rows.sort(key=lambda r: (r["kind"] != "fact", order.get(r["role"], len(ROLES)), r["table"]))
    count = {k: sum(r["kind"] == k for r in rows) for k in ("fact", "dimension", "unknown")}
    return {"schema": path, "tables": len(rows), "fact": count["fact"], "dimension": count["dimension"],
            "unknown": count["unknown"], "by_table": rows, "signals": signals}


def text(d: dict) -> str:
    out = [f"{d['schema']}: {d['tables']} tables, {d['fact']} fact, {d['dimension']} dimension"
           + (f", {d['unknown']} without a role" if d["unknown"] else "")]
    for r in d["by_table"]:
        out.append(f"  {r['kind']:9} {r['role']:9} {r['table']:28} {r['rows']}")
    for s in d["signals"]:
        out.append(f"  signal {s.get('name')}: {s.get('target', '')}")
        out += [f"    cause: {c}" for c in s.get("causes") or []]
        if s.get("parameters"):
            out.append(f"    parameters: {', '.join(s['parameters'])}")
        out.append(f"    built by: {s.get('built_by', '')}")
    if not d["signals"]:
        out.append("  signals: none (every value is the spec's random one)")
    return "\n".join(out)


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--json"]
    if not args:
        sys.exit("uv run describe.py schema.yaml... [--json]")
    found = [describe(a) for a in args]
    print(json.dumps(found if len(found) > 1 else found[0], indent=1) if "--json" in sys.argv else
          "\n\n".join(text(d) for d in found))
