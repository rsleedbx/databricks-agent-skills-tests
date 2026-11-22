#!/usr/bin/env python3
"""live_scd2_refresh.py <profile> <catalog> <prefix> <target> <source> [<source> ...]: merge_all on Lakeflow Connect SCD
type 2 tables that had full refreshes, from Databricks Connect (serverless). Each source is a schema <prefix><source>
holding one table probe_<source> (lfc-downstream-history scripts/sqlserver-scd2-refresh-probe.sh, kept runs). For each:
the target and merge_all's state are seeded as of the first load (version A, the last read point), merge_all runs
across the refreshes, and the target must:
  1. keep every version the table had before its first refresh (id, value, start), none deleted;
  2. have every row the table has now (id, value, start, open or closed);
  3. have the table's open versions as its only open versions (id, value);
  4. have no two rows with one key (source, id, start);
  5. not change on a second run.
Prints one line per check and source, PASS or FAIL; exit 1 on a FAIL. The target schema is created and left in place.
"""
import sys

sys.path.insert(0, __file__.rsplit("/tests/", 1)[0] + "/scripts")
from lfc_medallion import merge

SC = "lob"   # the source column


def rows(spark, sql):
    return [tuple(r) for r in spark.sql(sql).collect()]


def main(profile, catalog, prefix, target, sources):
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.profile(profile).serverless().getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{target}`")
    failed = False

    def claim(name, s, ok, detail):
        nonlocal failed
        failed |= not ok
        print(f"{'PASS' if ok else 'FAIL'} {name} [{s}]: {detail}")

    for s in sources:
        table, out = f"probe_{s}", f"all_probe_{s}"
        src, tgt = merge._fq(catalog, prefix + s, table), merge._fq(catalog, target, out)
        spark.sql(f"DROP TABLE IF EXISTS {tgt}")
        merge._state_full(spark, catalog, target)   # makes the state table when there is none
        spark.sql(f"DELETE FROM {merge._fq(catalog, target, merge.STATE)} WHERE target_table = '{out}'")
        hist = merge.history_rows(spark, src)
        a = min(int(r.version) for r in hist if r.operation == "MERGE")   # the first load
        r1 = min(int(r.version) for r in hist if r.operation == "DLT REFRESH")
        # seed: the target and the state as of A, as a run of merge_all at A would have left them
        i = merge._inspect(spark, catalog, prefix, table, out, s, {table: ["id"]}, None, {}, True)
        merge._ensure_target(spark, tgt, False, SC, i["cols"], {})
        merge._copy(spark, tgt, i, SC, [], "seed", out, {out: {}}, "seeded", at=a)
        merge._save_states(spark, catalog, target, out, [(s, a, i["tid"], i["key"])])
        report = merge.merge_all(spark, catalog, prefix, target, [table], source_column=SC, only=[s],
                                 name="all_{table}", keys={table: ["id"]})
        print(f"merge_all [{s}]: {report}")
        key = "id, v, __START_AT.__cdc_internal_value"
        pre = rows(spark, f"SELECT {key} FROM {src} VERSION AS OF {r1 - 1}")
        got = rows(spark, f"SELECT {key} FROM {tgt}")
        missing = [x for x in set(pre) if pre.count(x) > got.count(x)]
        claim("kept-before-refresh", s, not missing,
              f"{len(pre)} versions before the refresh at {r1}, all in the target ({len(got)} rows)" if not missing
              else f"missing {missing}")
        now_rows = rows(spark, f"SELECT {key}, __END_AT IS NULL FROM {src}")
        tgt_rows = rows(spark, f"SELECT {key}, __END_AT IS NULL FROM {tgt}")
        lost = [x for x in set(now_rows) if now_rows.count(x) > tgt_rows.count(x)]
        claim("has-current", s, not lost, f"{len(now_rows)} rows of the table now, all in the target" if not lost
              else f"missing {lost}")
        open_src = sorted(rows(spark, f"SELECT id, v FROM {src} WHERE __END_AT IS NULL"))
        open_tgt = sorted(rows(spark, f"SELECT id, v FROM {tgt} WHERE __END_AT IS NULL"))
        claim("open-versions", s, open_src == open_tgt, f"target {open_tgt}, table {open_src}")
        dup = rows(spark, f"SELECT {SC}, id, to_json(__START_AT), COUNT(*) FROM {tgt} GROUP BY 1, 2, 3 HAVING COUNT(*) > 1")
        claim("one-row-per-key", s, not dup, "no key twice" if not dup else f"twice: {dup}")
        closed = rows(spark, f"SELECT COUNT(*) FROM {tgt} WHERE __END_AT.__cdc_internal_value LIKE 'lfc_medallion:%'")[0][0]
        print(f"OBSERVED closed-by-refresh [{s}]: {closed} versions closed by merge_all on a full refresh")
        before = rows(spark, f"SELECT COUNT(*), SUM(hash(*)) FROM {tgt}")
        again = merge.merge_all(spark, catalog, prefix, target, [table], source_column=SC, only=[s],
                                name="all_{table}", keys={table: ["id"]})
        after = rows(spark, f"SELECT COUNT(*), SUM(hash(*)) FROM {tgt}")
        claim("rerun-no-change", s, before == after, f"{again[out]}; rows and checksum {after}")
        print(f"ROWS [{s}]: " + " ".join(f"{r[0]}={r[1]}{'' if r[3] else '(closed)'}@{(r[2] or '')[:8] or 'snap'}"
                                         for r in sorted(rows(spark, f"SELECT id, v, to_json(__START_AT), __END_AT IS NULL "
                                                                     f"FROM {tgt} ORDER BY id, __START_AT.__cdc_timestamp_value"))))
    return 1 if failed else 0


if __name__ == "__main__":
    if len(sys.argv) < 6:
        sys.exit(__doc__)
    sys.exit(main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5:]))
