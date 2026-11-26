# /// script
# dependencies = ["pyyaml"]
# ///
"""plan.py: what it takes to go from the plan silver was built from (current) to an edited one (desired).

    plan.py diff <current.yaml> <desired.yaml> [--schema schema.yaml] [--activity activity.yaml] [--json]

For each table whose silver changed, an action and whether it can be done without asking:
  add      a target the table didn't have (scd1 -> <table>, scd2 -> <table>_history): a new streaming table, which reads
           the snapshot and the change log from T on the next update (refreshed, so it's rebuilt the same way each time)
  rebuild  <table> changes kind (scd1 <-> append): a full refresh of that table alone, from the snapshot and the change
           log, which holds every event from T (it's never truncated in these tests: with a retention it couldn't be)
  drop     a target no longer wanted: the pipeline stops defining it, and it stays in the catalog, no longer updated
           (seen in the Mautic test), until a DROP TABLE removes it; asks first (its readers would read stale rows,
           or lose it when dropped)
  refuse   what the pipeline can't build: scd1 or scd2 without a primary key, append together with scd1 or scd2
  warn     append for a table the activity shows updated or deleted, or whose foreign key deletes or changes its rows
           with its parent (ON DELETE CASCADE, SET NULL): those changes would be left out
  cutover / silver / reshard   the table's route changed (lfc-routing routing.transitions): its change log
           (shared <-> own, at a position: no new snapshot), its silver cell (a table built, swapped for a view, or
           into or out of a group: asks first, since a table's readers see it replaced), its shards; no refresh
A table only in desired needs its producer to capture it first (refused here); one only in current is dropped.
Prints a table, or with --json {"refresh": [...], "drop": [...], "refuse": [...], "warn": [...], "auto": bool}: the
targets to full-refresh, and whether everything can go ahead without asking. Exit 0 auto, 1 needs asking, 2 refused.
"""
import argparse
import json
import os
import sys

import yaml

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "lfc-change-source", "scripts"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "lfc-routing", "scripts"))
from lfc_routing import routing  # noqa: E402

HIST = "_history"


def load(p):
    with open(p) as f:
        return yaml.safe_load(f) or {}


def targets(table, silver):
    return {("scd1" if s == "scd1" else "append"): table for s in silver if s in ("scd1", "append")} | \
        ({"scd2": table + HIST} if "scd2" in silver else {})


def diff(cur, des, keyed, activity, acting=None):
    acts = []
    ct, dt = cur.get("tables") or {}, des.get("tables") or {}
    for t in sorted(set(ct) | set(dt)):
        a, b = list((ct.get(t) or {}).get("silver") or []), list((dt.get(t) or {}).get("silver") or [])
        if t not in ct:
            acts.append(("refuse", t, "", f"{t} is new: its producer must capture it, and its snapshot be taken, first"))
            continue
        if t not in dt:   # only in current: every target it had is dropped
            for k, tgt in sorted(targets(t, a).items()):
                acts.append(("drop", t, tgt, f"{t} is no longer in the plan: {tgt} goes" +
                             (", and its history with it" if k == "scd2" else "")))
            continue
        for kind, why in routing.transitions((ct.get(t) or {}).get("route") or {}, (dt.get(t) or {}).get("route") or {}):
            acts.append((kind, t, t, why))
        if sorted(a) == sorted(b):
            continue
        if not b or set(b) - {"scd1", "scd2", "append"} or "append" in b and len(b) > 1:
            acts.append(("refuse", t, "", f"silver {b}: scd1 and/or scd2, or append alone"))
            continue
        if keyed is not None and not keyed.get(t, True) and set(b) & {"scd1", "scd2"}:
            acts.append(("refuse", t, "", "no primary key: scd1 and scd2 need one; append only"))
            continue
        ta, tb = targets(t, a), targets(t, b)
        if ("scd1" in ta) != ("scd1" in tb) and ("append" in ta) != ("append" in tb):
            acts.append(("rebuild", t, t, f"{'+'.join(a)} -> {'+'.join(b)}: {t} changes kind, a full refresh of it alone"))
        else:
            for k in ("scd1", "append"):
                if k in tb and k not in ta:
                    acts.append(("add", t, tb[k], f"new {k} table {tb[k]}"))
                if k in ta and k not in tb and not (set(tb) & {"scd1", "append"}):
                    acts.append(("drop", t, ta[k], f"{ta[k]} is no longer wanted"))
        if "scd2" in tb and "scd2" not in ta:
            acts.append(("add", t, tb["scd2"], f"new history table {tb['scd2']}"))
        if "scd2" in ta and "scd2" not in tb:
            acts.append(("drop", t, ta["scd2"], f"{ta['scd2']} is no longer wanted: its history goes with it"))
        if "append" in b and (acting or {}).get(t):
            acts.append(("warn", t, t, f"append, but {acting[t][0]}: its parent's deletes change its rows, and silver "
                                       "would keep them"))
        act = (activity or {}).get(t) or {}
        if "append" in b and (act.get("updates") or act.get("deletes")):
            acts.append(("warn", t, t, f"append, but the source had {act.get('updates', 0)} updates and "
                                       f"{act.get('deletes', 0)} deletes: silver would leave those out"))
    return acts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["diff"]); ap.add_argument("current"); ap.add_argument("desired")
    ap.add_argument("--schema"); ap.add_argument("--activity"); ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    keyed = acting = None
    if a.schema:
        tables = load(a.schema)["tables"]
        keyed = {t["name"]: any(c.get("primary_key") for c in t["columns"]) for t in tables}
        acting = {t["name"]: [f"{','.join(f['columns'])} -> {f['parent_table']} ON DELETE {f['on_delete']}"
                              for f in t.get("fk_constraints") or []
                              if str(f.get("on_delete") or "").upper().replace("_", " ") in ("CASCADE", "SET NULL", "SET DEFAULT")]
                  for t in tables}
    acts = diff(load(a.current), load(a.desired), keyed, load(a.activity) if a.activity else None, acting)
    refuse = [x for x in acts if x[0] == "refuse"]
    ask = [x for x in acts if x[0] in ("drop", "warn", "silver")]
    if a.json:
        print(json.dumps({"refresh": sorted({x[2] for x in acts if x[0] in ("add", "rebuild")}),
                          "drop": [x[2] for x in acts if x[0] == "drop"], "refuse": [x[1] for x in refuse],
                          "warn": [x[1] for x in acts if x[0] == "warn"], "auto": not refuse and not ask,
                          "route": [x[1] for x in acts if x[0] in ("cutover", "silver", "reshard")],
                          "actions": [dict(zip(("action", "table", "target", "why"), x)) for x in acts]}))
    else:
        print("| Action | Table | Target | Why |\n|---|---|---|---|")
        for x in acts:
            print(f"| {x[0]} | `{x[1]}` | {'`' + x[2] + '`' if x[2] else ''} | {x[3]} |")
        if not acts:
            print("| none | | | the plans build the same silver |")
    sys.exit(2 if refuse else 1 if ask else 0)


if __name__ == "__main__":
    main()
