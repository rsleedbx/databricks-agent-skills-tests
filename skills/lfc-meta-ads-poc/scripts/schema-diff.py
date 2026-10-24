#!/usr/bin/env python3
"""schema-diff.py --lfc lfc.tsv [--fivetran fivetran.tsv] (--mapping map.yaml | --tables-map tables.tsv) [--out diff.tsv]

Maps the Lakeflow Connect landing to the Fivetran schema, column by column (POC objective 2).

Inputs are schema.sh exports (table, column, type, position). The pairing comes from one of:
  --mapping     a reviewed column mapping: YAML or JSON with a `mappings:` list of rows
                  {fivetran: "<table>.<column>", lfc: "<table>.<column>" or null, status: common|fivetran_only|..., method}
                (one row per Fivetran column; rows with lfc set and status other than fivetran_only are pairs).
  --tables-map  TSV of fivetran_table, lfc_table (header optional); columns pair by name, case-insensitive, camelCase
                and snake_case treated alike. references/fivetran-tables.tsv holds the standard object tables.

Prints a summary per table pair and these findings; --out writes every column as TSV (fivetran, lfc, finding, types):
  same / renamed      paired and both present (types shown when they differ: type change for downstream SQL)
  fivetran only       a Fivetran column with no LFC pair
  lfc only            an LFC column with no Fivetran pair
  mapping stale       the mapping pairs a column that the live LFC table doesn't have (connector changed, or the
                      mapping is wrong): re-review that row
  not in Fivetran     the mapping names a Fivetran column the live Fivetran table doesn't have (Fivetran schema
                      differs: column blocking, another connector version)
Tables in the mapping but not in an export are listed once and skipped (not ingested in this POC).
"""
import argparse
import csv
import json
import re
import sys
from collections import defaultdict


def norm(name):
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name).lower().replace("__", "_")


def read_tsv(path):
    cols = defaultdict(dict)  # table -> {column: type}
    with open(path, newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            cols[row["table"]][row["column"]] = row.get("type", "")
    return cols


def load_mapping(path):
    with open(path) as f:
        text = f.read()
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        try:
            import yaml
        except ImportError:
            sys.exit(f"{path}: YAML needs PyYAML; or convert it first: yq -o json {path} > map.json")
        data = yaml.safe_load(text)
    rows = data["mappings"] if isinstance(data, dict) else data
    pairs = []
    for r in rows:
        ft, lf = r.get("fivetran"), r.get("lfc")
        if not ft:
            continue
        status = r.get("status", "")
        pairs.append((ft, lf if status != "fivetran_only" else None, r.get("method", "")))
    return pairs


def tables_map(path):
    out = []
    with open(path, newline="") as f:
        for row in csv.reader(f, delimiter="\t"):
            if len(row) >= 2 and not row[0].startswith("#") and row[0] != "fivetran_table":
                out.append((row[0].strip(), row[1].strip()))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lfc", required=True)
    ap.add_argument("--fivetran")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--mapping")
    g.add_argument("--tables-map")
    ap.add_argument("--out")
    a = ap.parse_args()

    lfc = read_tsv(a.lfc)
    ft = read_tsv(a.fivetran) if a.fivetran else None
    rows = []  # (pair key, fivetran t.c, lfc t.c, finding, fivetran type, lfc type)
    skipped = set()

    if a.mapping:
        pairs = load_mapping(a.mapping)
        by_pair = defaultdict(list)
        for f_tc, l_tc, method in pairs:
            ft_t = f_tc.split(".", 1)[0]
            l_t = l_tc.split(".", 1)[0] if l_tc else None
            by_pair[ft_t].append((f_tc, l_tc, l_t, method))
        for ft_t, items in sorted(by_pair.items()):
            lfc_tables = {l_t for _, _, l_t, _ in items if l_t}
            present = [t for t in lfc_tables if t in lfc]
            if lfc_tables and not present:
                skipped.add(f"{ft_t} -> {', '.join(sorted(lfc_tables))}")
                continue
            if ft is not None and ft_t not in ft:
                skipped.add(f"{ft_t} (not in the Fivetran export)")
                continue
            key = f"{ft_t} -> {', '.join(sorted(lfc_tables)) or '-'}"
            seen_lfc = set()
            for f_tc, l_tc, l_t, method in items:
                f_c = f_tc.split(".", 1)[1]
                f_type = ft[ft_t].get(f_c, "") if ft is not None else ""
                if ft is not None and f_c not in ft[ft_t]:
                    rows.append((key, f_tc, l_tc or "", "not in Fivetran", "", ""))
                    continue
                if not l_tc:
                    rows.append((key, f_tc, "", "fivetran only", f_type, ""))
                    continue
                l_c = l_tc.split(".", 1)[1]
                seen_lfc.add((l_t, l_c))
                if l_t not in lfc or l_c not in lfc[l_t]:
                    rows.append((key, f_tc, l_tc, "mapping stale", f_type, ""))
                    continue
                finding = "renamed" if method == "rename" or norm(f_c) != norm(l_c) else "same"
                rows.append((key, f_tc, l_tc, finding, f_type, lfc[l_t][l_c]))
            for l_t in present:
                for l_c, l_type in lfc[l_t].items():
                    if (l_t, l_c) not in seen_lfc:
                        rows.append((key, "", f"{l_t}.{l_c}", "lfc only", "", l_type))
            if ft is not None:
                mapped = {f.split(".", 1)[1] for f, _, _, _ in items}
                for f_c, f_type in ft[ft_t].items():
                    if f_c not in mapped:
                        rows.append((key, f"{ft_t}.{f_c}", "", "fivetran only (not in mapping)", f_type, ""))
        if ft is not None:
            for ft_t in sorted(set(ft) - set(by_pair)):
                for f_c, f_type in ft[ft_t].items():
                    rows.append((f"{ft_t} -> (not in mapping)", f"{ft_t}.{f_c}", "", "fivetran only (not in mapping)",
                                 f_type, ""))
    else:
        if ft is None:
            sys.exit("--tables-map pairs columns by name, so it needs --fivetran too")
        for ft_t, l_t in tables_map(a.tables_map):
            if ft_t not in ft or l_t not in lfc:
                skipped.add(f"{ft_t} -> {l_t}" + (" (no Fivetran table)" if ft_t not in ft else " (no LFC table)"))
                continue
            key = f"{ft_t} -> {l_t}"
            lmap = {norm(c): c for c in lfc[l_t]}
            used = set()
            for f_c, f_type in ft[ft_t].items():
                l_c = lmap.get(norm(f_c))
                if l_c:
                    used.add(l_c)
                    rows.append((key, f"{ft_t}.{f_c}", f"{l_t}.{l_c}", "same" if l_c == f_c else "renamed",
                                 f_type, lfc[l_t][l_c]))
                else:
                    rows.append((key, f"{ft_t}.{f_c}", "", "fivetran only", f_type, ""))
            for l_c, l_type in lfc[l_t].items():
                if l_c not in used:
                    rows.append((key, "", f"{l_t}.{l_c}", "lfc only", "", l_type))

    order = ["same", "renamed", "fivetran only", "fivetran only (not in mapping)", "lfc only", "mapping stale",
             "not in Fivetran"]
    summary = defaultdict(lambda: defaultdict(int))
    type_diffs = defaultdict(int)
    for key, _, _, finding, f_type, l_type in rows:
        summary[key][finding] += 1
        if finding in ("same", "renamed") and f_type and l_type and f_type.lower() != l_type.lower():
            type_diffs[key] += 1
    used = [o for o in order if any(o in s for s in summary.values())]
    print("\t".join(["pair"] + used + (["type differs"] if ft is not None else [])))
    for key in sorted(summary):
        line = [key] + [str(summary[key].get(o, 0)) for o in used]
        if ft is not None:
            line.append(str(type_diffs[key]))
        print("\t".join(line))
    tot = defaultdict(int)
    for s in summary.values():
        for k, v in s.items():
            tot[k] += v
    print("total\t" + "\t".join(str(tot.get(o, 0)) for o in used)
          + (f"\t{sum(type_diffs.values())}" if ft is not None else ""))
    for s in sorted(skipped):
        print(f"skipped (table not ingested here): {s}", file=sys.stderr)
    stale = [r for r in rows if r[3] in ("mapping stale", "not in Fivetran")]
    for r in stale:
        print(f"{r[3]}: {r[1]} -> {r[2]}", file=sys.stderr)
    if a.out:
        with open(a.out, "w", newline="") as f:
            w = csv.writer(f, delimiter="\t")
            w.writerow(["pair", "fivetran", "lfc", "finding", "fivetran_type", "lfc_type"])
            w.writerows(sorted(rows))
    return 1 if stale else 0


if __name__ == "__main__":
    sys.exit(main())
