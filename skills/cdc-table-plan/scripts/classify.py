# /// script
# dependencies = ["pyyaml"]
# ///
"""classify.py: a first guess at each table's role and silver, from a canonical schema.yaml, for the customer to check.

    classify.py <schema.yaml> [--activity activity.yaml [--activity-seconds S]] [--stats stats.yaml] [--sf 1] > plan.yaml
    classify.py <schema.yaml> --truth <truth.yaml>        # compare the guess with known roles (exit 1 if any differ)
    classify.py <schema.yaml> --truth schema              # the same against the roles the schema itself carries (role:)
    classify.py <schema.yaml> --review schema             # the plan with the schema's role: lines as the corrections
    classify.py <schema.yaml> --review <review.yaml>      # the guess, corrected where a review says otherwise

The guess uses only what any schema has, documented or not: keys, foreign keys, row counts, activity and names
(decided_by: relations). For public software a review checks it against the docs or the code and corrects it
(review.yaml: <table>: {role, silver?, cite}; decided_by: docs, with the citation), and the customer has the last word
by editing the plan (decided_by: customer). A truth file is a review's roles alone (<table>: <role>); a schema whose
tables say their role (the TPC models do, from their specifications) is its own truth. The guess never reads role:.

Reads lfc-source-statschema's canonical format: keys, foreign keys (fk_constraints and columns' references), row
counts (row_count, or row_count_per_sf x --sf). --activity adds each table's inserts, updates and deletes since the
source's statistics were reset (activity.yaml, from collect-activity-*.sql), the best evidence of insert-only tables.

Roles, and the silver each gets (the plan's `silver`, which the pipeline builds; see SKILL.md):
  dimension  referenced by others, few or no parents          scd1 (current rows) and scd2 (history)
  lookup     small reference data, referenced, no parents     scd1
  bridge     a key made of two or more foreign keys            scd1
  fact       several parents, the largest of them, few or no children
             scd1; append when the activity shows no updates or deletes
  log        no primary key, or the activity shows inserts only, and named like a log
             append
  queue      inserts and deletes, few updates (activity only)  scd1
Each table gets its evidence (`why`), a confidence, and a question to ask (`ask`) where a guess rests on little.

Each table also gets a `route` (lfc-routing routing.py): its class (small_slow, small_fast, big_slow, big_fast),
its snapshot's path (log: landed into the change log; direct: read where the producer wrote it; auto: set by its real
bytes when it lands), its change log (shared, or own for big_fast and very busy tables), shards, and its silver method
(view, merge_round, merge_changed, auto_cdc). From stats.yaml's data_size_bytes (else row_count x the columns' avg_width_bytes) and the activity's changes over
--activity-seconds (how long the counts cover: since the statistics were reset). Edit it like silver; plan.py diff says
what a change of route takes.
"""
import argparse
import os
import re
import sys

import yaml

# the rules live in lfc-medallion's package (lfc_medallion.roles), so a pipeline or job can use them too; the route's
# in lfc-routing's (lfc_routing.routing)
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "lfc-medallion", "scripts"))
sys.path.insert(0, os.path.join(HERE, "..", "..", "lfc-change-source", "scripts"))
sys.path.insert(0, os.path.join(HERE, "..", "..", "lfc-routing", "scripts"))
from lfc_routing.classify import classify, review  # noqa: E402


def load(p):
    with open(p) as f:
        return yaml.safe_load(f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("schema"); ap.add_argument("--activity"); ap.add_argument("--sf", type=float, default=1)
    ap.add_argument("--stats", help="statschema's stats.yaml: each table's data_size_bytes, columns' avg_width_bytes")
    ap.add_argument("--activity-seconds", type=float, help="how many seconds the activity's counts cover")
    ap.add_argument("--truth"); ap.add_argument("--review"); ap.add_argument("--model")
    a = ap.parse_args()
    schema = load(a.schema)
    act = load(a.activity) if a.activity else None
    model = a.model or re.sub(r"\.ya?ml$", "", a.schema.rsplit("/", 1)[-1])
    # the box (lfc_routing.classify); the plan file keeps its shape: no keys, no empty problems
    p = classify(schema, load(a.stats) if a.stats else None, act, activity_seconds=a.activity_seconds, sf=a.sf,
                 model=model, with_keys=False)
    if not p["problems"]:
        p.pop("problems")
    if a.review:   # corrections from the docs or the code: role (and silver, else the role's), with the citation
        roles = {t["name"]: {"role": t["role"], "cite": "the schema's role: (its specification)"}
                 for t in schema.get("tables") or [] if t.get("role")}
        for m in review(p, roles if a.review == "schema" else load(a.review), a.review.rsplit("/", 1)[-1]):
            print(m, file=sys.stderr)
    if a.truth:
        truth = ({t["name"]: t["role"] for t in schema.get("tables") or [] if t.get("role")} if a.truth == "schema"
                 else load(a.truth))
        if not truth:
            print(f"--truth {a.truth}: no roles to compare with", file=sys.stderr)
            sys.exit(2)
        bad = 0
        for n, want in truth.items():
            got = p["tables"].get(n, {}).get("role", "missing")
            ok = got == want
            bad += not ok
            print(f"{'ok ' if ok else 'BAD'} {n:24} want {want:9} got {got:9} {'' if ok else p['tables'].get(n, {}).get('why', '')}")
        print(f"{model}: {len(truth) - bad} of {len(truth)} roles as expected")
        sys.exit(1 if bad else 0)
    sys.stdout.write("# The first guess at each table's role and silver (classify.py). Check it and edit role or silver;\n"
                     "# the pipeline builds what silver says. silver: scd1 (current rows), scd2 (history), append (inserts only).\n")
    # no anchors and aliases (*id001) for the lists tables share: every reader, yq too, sees each table's own
    class Plain(yaml.SafeDumper):
        def ignore_aliases(self, data):
            return True
    yaml.dump(p, sys.stdout, Dumper=Plain, sort_keys=False, width=140, default_flow_style=None)


if __name__ == "__main__":
    main()
