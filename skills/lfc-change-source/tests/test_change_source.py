"""Offline checks of lfc_change_source: the contract and the Lakeflow Connect bronze adapter.

    python3 -m unittest skills/lfc-change-source/tests/test_change_source.py
"""
import json
import os
import sys
import types
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
from lfc_change_source.contract import Position, Stats, last_change_sql  # noqa: E402
from lfc_change_source.lfc_bronze import Bronze  # noqa: E402

SAMPLES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "samples")
V = lambda n: Position("delta_version", str(n))  # noqa: E731


class Contract(unittest.TestCase):
    def test_position_text_round_trip(self):
        p = Position.parse("hlc:1700000000000000000.0000000001")
        self.assertEqual((p.kind, str(p)), ("hlc", "hlc:1700000000000000000.0000000001"))

    def test_last_change_per_key_by_seq(self):
        q = last_change_sql("SELECT 1", ["a", "b"])
        self.assertIn("PARTITION BY `a`, `b` ORDER BY _seq DESC", q)
        self.assertIn("WHERE `__lfc_n` = 1", q)


class LfcBronze(unittest.TestCase):
    b = Bronze("lob_001", "`c`.`s`.`account`", "account", ["a_id"], ["a_id", "a_balance"])

    def test_snapshot_pinned_and_changes_with_op_and_seq(self):
        self.assertEqual(self.b.snapshot_sql(V(8)), "SELECT `a_id`, `a_balance`, 'upsert' AS _op FROM `c`.`s`.`account` VERSION AS OF 8")
        q = self.b.changes_sql(V(3), V(8))
        self.assertIn("FROM table_changes('c.s.account', 4, 8) WHERE _change_type != 'update_preimage'", q)
        self.assertIn("CASE _change_type WHEN 'delete' THEN 'delete' ELSE 'upsert' END AS _op", q)
        self.assertIn("named_struct('v', _commit_version, 'o', CASE _change_type WHEN 'delete' THEN 0 ELSE 1 END) AS _seq", q)
        with self.assertRaises(ValueError):
            self.b.changes_sql(Position("lsn", "0/16B3748"), V(8))

    def test_commit_time_positions(self):   # read from a saved time, to the latest commit: no version lookup
        from lfc_change_source.lfc_bronze import TS, nothing_newer
        at = Position(TS, "2026-11-23T20:52:43.005Z")
        self.assertIn("TIMESTAMP AS OF TIMESTAMP'2026-11-23T20:52:43.005Z'", self.b.snapshot_sql(at))
        q = self.b.changes_sql(at, None)
        self.assertIn("table_changes('c.s.account', TIMESTAMP'2026-11-23T20:52:43.005Z' + INTERVAL 1 MICROSECOND)", q)
        self.assertIn("_commit_timestamp", q)   # what a consumer saves after reading by time
        with self.assertRaises(ValueError):
            self.b.changes_sql(at, V(9))   # one kind a range
        self.assertTrue(nothing_newer(RuntimeError("[DELTA_TIMESTAMP_GREATER_THAN_COMMIT] Timestamp ... is after")))
        self.assertFalse(nothing_newer(RuntimeError("[DELTA_CHANGE_DATA_FILE_NOT_FOUND]")))

    def test_watermark_and_stats_from_history(self):
        seen = []
        def sql(q):
            seen.append(q)
            row = (19,) if "max(version)" in q else (78, 1024, 2)
            return types.SimpleNamespace(collect=lambda: [row])
        s = types.SimpleNamespace(sql=sql)
        self.assertEqual(self.b.watermark(s), V(19))
        self.assertEqual(self.b.stats(s, V(16)), Stats(78, 1024, 2))
        self.assertIn("FROM (DESCRIBE HISTORY `c`.`s`.`account`) WHERE version > 16", seen[-1])


class CdcLog(unittest.TestCase):
    def test_changes_from_the_shared_log_by_destination_and_sequence(self):
        from lfc_change_source.cdc_log import SEQUENCES, CdcLog as Log
        src = Log("crdb1", "`m`.`zb`.`cdc_log`", "orders", "crdb1.public.orders", ["id"],
                  {"id": "BIGINT", "total": "DECIMAL(12,2)"}, seq="lsn", snapshot="SELECT * FROM `m`.`zb`.`snap`")
        q = src.changes_sql(Position("lsn", "100"), Position("lsn", "200"))
        self.assertIn("key:id::BIGINT AS `id`", q)                       # a delete has only its key
        self.assertIn("value:after:total::DECIMAL(12,2) AS `total`", q)
        self.assertIn("destination = 'crdb1.public.orders' AND operation <> 'resolved' AND value:source:lsn::BIGINT > 100 AND "
                      "value:source:lsn::BIGINT <= 200", q)
        self.assertIn("CASE operation WHEN 'delete' THEN 0 ELSE 1 END) AS _seq", q)   # a delete before an insert
        self.assertTrue(src.shared)
        self.assertIn("'upsert' AS _op FROM (SELECT * FROM `m`.`zb`.`snap`)", src.snapshot_sql())
        with self.assertRaises(ValueError):
            src.changes_sql(Position("ts_ns", "1"), None)   # another source's kind of position
        with self.assertRaises(ValueError):
            Log("x", "t", "t", "d", ["id"], {"id": "INT"}, seq="gtid")
        self.assertEqual(set(SEQUENCES), {"ts_ns", "binlog", "lsn", "scn", "ifx_lsn", "db2_lsn", "change_lsn",
                                          "gateway_cursor", "hlc"})


class GatewayStaging(unittest.TestCase):
    here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "samples", "gateway")

    def test_schema_file_to_columns_and_changes_sql(self):   # from the captured schema file
        from lfc_change_source import gateway_staging as gs
        text = open(os.path.join(self.here, "target_schema.0.json")).read()
        p = "/Volumes/c/s/staging/g/metadata/table_schemas/T_HuC79qK_50284_50300/target_schema.0.json"
        session = types.SimpleNamespace(sql=lambda q: types.SimpleNamespace(
            collect=lambda: [types.SimpleNamespace(p=p, value=text)]))
        t = gs.schemas(session, "/Volumes/c/s/staging/g")[("src_db", "lob_124", "history")]
        self.assertEqual(t["id"], "T_HuC79qK_50284_50300")
        self.assertEqual(t["keys"], ["h_t_id", "h_b_id", "h_a_id", "h_delta", "h_time", "h_filler"])   # keyless: all
        self.assertIn(("h_delta", "DECIMAL(12,2)", "C_HuC79qK_50284_50300_4"), t["columns"])
        self.assertEqual(gs.sql_type("INTEGER"), "INT")
        src = gs.Staging("lob_124", "/Volumes/c/s/staging/g", t)
        q = src.changes_sql(Position("cursor", "00000000/8F63FB10-0000000000000000000"), None)
        self.assertIn("data.`C_HuC79qK_50284_50300_4`::DECIMAL(12,2) AS `h_delta`", q)
        self.assertIn("table_id.uoid = 'T_HuC79qK_50284_50300' AND cursor.sequence > '00000000/8F63FB10-0000000000000000000'", q)
        self.assertIn("CASE operation WHEN 'DELETE' THEN 0 ELSE 1 END) AS _seq", q)   # the sample's DELETE first
        self.assertIn("/snapshot/T_HuC79qK_50284_50300/", src.snapshot_sql())
        self.assertNotIn("format => 'parquet')", src.snapshot_sql())   # the folder also holds qbc_checkpoint/ (not parquet)
        self.assertIn("pathGlobFilter => '*.parquet'", src.snapshot_sql())
        with self.assertRaises(ValueError):
            src.changes_sql(Position("lsn", "1"), None)

    def test_version_1_layout_says_why(self):   # by name, no table ids: the gateway had no applier manifest
        from lfc_change_source import gateway_staging as gs
        row = lambda p: types.SimpleNamespace(p=p, value="{}")   # noqa: E731
        s = types.SimpleNamespace(sql=lambda q: types.SimpleNamespace(collect=lambda: [
            row("/V/g/metadata/table_schemas/all_tables.json")]))
        with self.assertRaises(ValueError) as e:
            gs.schemas(s, "/V/g")
        self.assertIn("table ids off", str(e.exception))

    def test_cursor_sorts_as_text(self):   # fixed width: text order is commit order
        from lfc_change_source.gateway_staging import _cursor
        cs = [_cursor(0x9FD60E90, 0x4c8), _cursor(0x8F63FB10, 1), _cursor(0x9FD60E90, 0x4c7), _cursor(0xA0847CE0)]
        self.assertEqual(sorted(cs), [cs[1], cs[2], cs[0], cs[3]])


class Land(unittest.TestCase):
    def test_copy_into_the_shared_log_and_read_by_cursor(self):   # a gateway's files in the Debezium log's shape
        from lfc_change_source import land
        from lfc_change_source.gateway_log import from_gateway
        seen = []
        s = types.SimpleNamespace(sql=lambda q: seen.append(q) or types.SimpleNamespace(
            collect=lambda: [types.SimpleNamespace(asDict=lambda: {"num_inserted_rows": 7})]))
        self.assertEqual(land.gateway(s, "/Volumes/c/s/st/g", "`c`.`x`.`cdc_log`", "gw_1"), 7)
        self.assertIn("COPY INTO `c`.`x`.`cdc_log`", seen[0])          # each file once
        self.assertIn("PATTERN = '*/*'", seen[0])                       # cdc/<producer>/<segment>
        self.assertIn("'singleVariantColumn' = 'event'", seen[0])
        self.assertIn("WHEN 'INSERT' THEN 'create'", seen[0])           # Debezium's words
        self.assertIn("WHEN 'REPLACE' THEN 'update'", seen[0])          # change tracking's insert: never a NULL operation
        entry = {"id": "T1", "keys": ["id"], "columns": [("id", "INT", "C1"), ("v", "STRING", "C2")]}
        src = from_gateway("lob_1", "`c`.`x`.`cdc_log`", entry, "db.lob_1.t")
        q = src.changes_sql(Position("gateway_cursor", "00000000/8F63FB10-0000000000000000001"), None)
        self.assertIn("value:data.`C2`::STRING AS `v`", q)
        self.assertIn("value:cursor.sequence::STRING > '00000000/8F63FB10-0000000000000000001'", q)   # text order

    def test_gateway_snapshot_paths_log_and_direct(self):   # small: into the log; large or chosen: read in place
        from lfc_change_source import land
        from lfc_change_source.gateway_log import current_sql, from_gateway
        seen = []
        d = lambda n: types.SimpleNamespace(name=n, is_directory=True, file_size=None)   # noqa: E731
        f = lambda n, b: types.SimpleNamespace(name=n, path=n, is_directory=False, file_size=b)   # noqa: E731
        sizes = {"T1": 4096, "T2": 5 << 30, "T3": 100}

        def ls(p):
            tid = p.split("/snapshot/")[1].split("/")[0]
            if p.endswith(f"/{tid}"):
                return [d("100/"), d("250/"), d("qbc_checkpoint/")] if tid in sizes else []
            return [f("part-0.snappy.parquet", sizes[tid]), f("_SUCCESS", 0)]
        count = [types.SimpleNamespace(asDict=lambda: {"num_inserted_rows": 4})]
        s = types.SimpleNamespace(w=types.SimpleNamespace(files=types.SimpleNamespace(list_directory_contents=ls)),
                                  sql=lambda q: seen.append(q) or types.SimpleNamespace(
                                      collect=lambda: [] if "SELECT DISTINCT" in q else count))   # nothing landed yet
        entry = {"id": "T1", "keys": ["id"], "columns": [("id", "INT", "C1"), ("v", "STRING", "C2")]}
        tables = {("db", "s", "t"): entry, ("db", "s", "big"): dict(entry, id="T2"),
                  ("db", "s", "chosen"): dict(entry, id="T3"), ("db", "s", "none"): dict(entry, id="T9")}
        got = land.gateway_snapshots(s, "/V/g", "`c`.`x`.`cdc_log`", "gw_1", tables, paths={"db.s.chosen": "direct"})
        self.assertEqual({k: (v["path"], v["rows"]) for k, v in got.items()},
                         {"db.s.t": ("log", 4), "db.s.big": ("direct", None), "db.s.chosen": ("direct", None),
                          "db.s.none": (None, None)})
        self.assertEqual(got["db.s.big"]["bytes"], 5 << 30)
        self.assertEqual(len(seen), 2)                    # what's landed already (one query), then only the small one
        self.assertIn("WHERE operation = 'read' AND gateway = 'gw_1'", seen[0])
        self.assertIn("read_files('/V/g/snapshot/T1/250/', format => 'parquet', pathGlobFilter => '*.parquet')",
                      seen[1])                                             # the newest, not checkpoints
        self.assertIn("SELECT 'db.s.t', 'read',", seen[1])
        self.assertIn("struct(* EXCEPT (__sequence, __operation, _rescued_data))", seen[1])
        self.assertIn("parse_json(to_json(array(variant_get(to_variant_object(named_struct('k', `C1`)), '$.k', "
                      "'string'))))", seen[1])                              # the snapshot row's key, as a change's
        landed = {("db.s.t", 250)}
        again = land.gateway_snapshot(s, "/V/g", "`c`.`x`.`cdc_log`", "gw_1", "db.s.t", entry, landed=landed)
        self.assertEqual((again["rows"], len(seen)), (0, 2))               # in the log already: no statement
        self.assertEqual(land.snapshot_path(10, max_bytes=5), "direct")
        with self.assertRaises(ValueError):
            land.snapshot_path(1, chosen="both")
        q = from_gateway("gw_1", "`c`.`x`.`cdc_log`", entry, "db.s.t").snapshot_sql()   # no snapshot given: the log
        self.assertIn("ROW_NUMBER() OVER (PARTITION BY `id`", q)
        self.assertEqual(current_sql("gw_1", "`c`.`x`.`cdc_log`", entry, "db.s.t", "/V/g", "log"), q)
        direct = current_sql("gw_1", "`c`.`x`.`cdc_log`", entry, "db.s.t", "/V/g", "direct")
        self.assertIn("read_files('/V/g/snapshot/T1/'", direct)                # the snapshot in place
        self.assertIn("operation <> 'resolved'", direct)                         # and the log's changes

    def test_gateway_keys_one_expression_for_every_table(self):   # a map literal by table id, no branch a table
        from lfc_change_source import land
        from lfc_change_source.gateway_log import from_gateway
        entries = {("db", "s", "a"): {"id": "T1", "keys": ["id"], "columns": [("id", "INT", "C1"), ("v", "STRING", "C2")]},
                   ("db", "s", "b"): {"id": "T2", "keys": ["k2", "k1"], "columns": [("k1", "INT", "D1"), ("k2", "INT", "D2")]},
                   ("db", "s", "keyless"): {"id": "T3", "keys": [], "columns": [("x", "INT", "E1")]}}
        keys = land.gateway_keys(entries)
        self.assertEqual(keys, {"T1": ["C1"], "T2": ["D2", "D1"]})                 # key order, by column id
        q = land.gateway_key_sql(keys)
        self.assertNotIn("CASE", q)
        self.assertIn("map('T1', array('C1'), 'T2', array('D2', 'D1'))[event:table_id.uoid::STRING]", q)
        self.assertIn("variant_get(event:data, concat('$.', c), 'string')", q)
        self.assertEqual(land.gateway_key_sql(None), "CAST(NULL AS VARIANT)")
        self.assertIn("[value:table_id.uoid::STRING]", land.gateway_key_sql(keys, "value"))   # backfill_keys
        seen = []
        s = types.SimpleNamespace(sql=lambda q: seen.append(q) or types.SimpleNamespace(collect=lambda: []))
        land.gateway(s, "/V/g", "`c`.`x`.`cdc_log`", "gw_1", keys=keys)
        self.assertIn("map('T1', array('C1')", seen[0])                            # COPY INTO fills the key
        src = from_gateway("gw_1", "`c`.`x`.`cdc_log`", entries[("db", "s", "a")], "db.s.a")
        self.assertEqual(src.row_sql(), "value:data")
        self.assertEqual(src.stored_values(), {"id": "data:`C1`::INT", "v": "data:`C2`::STRING"})

    def test_changefeed_files_land_and_read_by_hlc(self):   # captured CockroachDB JSON files: tests/samples/object_store
        from lfc_change_source import land
        from lfc_change_source.changefeed_log import from_changefeed, hlc
        seen = []
        s = types.SimpleNamespace(sql=lambda q: seen.append(q) or types.SimpleNamespace(
            collect=lambda: [types.SimpleNamespace(asDict=lambda: {"num_inserted_rows": 3})]))
        land.changefeed(s, "/Volumes/c/s/l/orders", "`c`.`x`.`cdc_log`", "shop.public.orders", ["id", "k2"], "crdb1")
        self.assertIn("'shop.public.orders' AS destination", seen[0])
        self.assertIn("named_struct('id', event:key[0], 'k2', event:key[1])", seen[0])   # the key array by name
        self.assertIn("is_variant_null(event:after) THEN 'delete'", seen[0])
        self.assertIn("PATTERN = '*/*{.ndjson,.RESOLVED}'", seen[0])
        src = from_changefeed("crdb1", "`c`.`x`.`cdc_log`", "shop.public.orders", ["id"],
                              {"id": "BIGINT", "total": "DECIMAL(12,2)"})
        q = src.changes_sql(Position("hlc", hlc(5)), None)
        self.assertIn("value:updated::STRING > '0000000000000000005.0000000000'", q)
        self.assertIn("operation <> 'resolved'", q)
        self.assertIn("WHERE _op <> 'delete'", src.snapshot_sql(Position("hlc", hlc(9))))   # from the log itself
        w = types.SimpleNamespace(sql=lambda q: seen.append(q) or types.SimpleNamespace(collect=lambda: [[hlc(7)]]))
        self.assertEqual(src.watermark(w), Position("hlc", hlc(7)))
        self.assertIn("max(value:resolved::STRING)", seen[-1])                 # RESOLVED, not the newest event
        self.assertLess(hlc(9, 3), hlc(10))                                       # fixed width: text order is HLC order
        with open(os.path.join(SAMPLES, "object_store", "changefeed_orders.ndjson")) as f:
            events = [json.loads(line) for line in f]
        self.assertEqual(sorted(e["updated"] for e in events if "updated" in e),
                         [e["updated"] for e in events if "updated" in e])
        self.assertTrue(any(e.get("after") is None and "key" in e for e in events))   # a delete: no after image



class Routing(unittest.TestCase):
    def test_four_classes_by_size_and_rate(self):
        from lfc_change_source import routing
        p = routing.Policy(shared_log="`m`.`c`.`cdc_log`", fast_events_per_s=1, own_log_events_per_s=10_000)
        got = routing.plan({"db.s.region": (4096, 0.01), "db.s.carts": (64 << 20, 50.0),
                            "db.s.ledger": (5 << 40, 0.2), "db.s.orders": (5 << 40, 250_000),
                            "db.s.queue": (100, 20_000)}, p)
        self.assertEqual({t: (r.cls, r.snapshot, r.log, r.shards, r.silver) for t, r in got.items()}, {
            "db.s.region": ("small_slow", "log", "`m`.`c`.`cdc_log`", 1, "shared"),
            "db.s.carts": ("small_fast", "log", "`m`.`c`.`cdc_log`", 1, "merge_round"),   # most of its group's changes
            "db.s.ledger": ("big_slow", "direct", "`m`.`c`.`cdc_log`", 1, "merge_changed"),
            "db.s.orders": ("big_fast", "direct", "`m`.`c`.`cdc_log_orders`", 3, "auto_cdc"),   # 250,000 / 100,000
            "db.s.queue": ("small_fast", "log", "`m`.`c`.`cdc_log_queue`", 1, "merge_round")})   # busy: own log

    def test_suggest_from_statistics_says_why(self):
        from lfc_change_source import routing
        got = routing.suggest({"region": {"bytes": 2048, "events_per_s": 0.0},
                               "lineitem": {"bytes": 80 << 30, "events_per_s": 0.5},
                               "ticks": {"bytes": None, "events_per_s": 150_000.0},
                               "orders": {"bytes": None, "events_per_s": None}}, routing.Policy("shared"))
        self.assertEqual({t: (r["class"], r["snapshot"], r["log"], r["shards"], r["silver"]) for t, r in got.items()}, {
            "region": ("small_slow", "log", "shared", 1, "shared"),
            "lineitem": ("big_slow", "direct", "shared", 1, "merge_changed"),
            "ticks": ("auto_fast", "auto", "own", 2, "merge_round"),           # its size decides: small_fast or big_fast
            "orders": ("auto_slow", "auto", "shared", 1, "shared")})
        self.assertIn("80.0 GiB snapshot", got["lineitem"]["why"])
        self.assertIn("change rate unknown", got["orders"]["why"])

    def test_small_tables_in_groups_and_the_rules_out(self):
        from lfc_change_source import routing
        p = routing.Policy("`m`.`c`.`cdc_log`", shared_group_size=2, own_table={"db.s.d": "streaming reads"},
                           shared_max_reads_per_day=100)
        tables = {f"db.s.{t}": (4096, 0.0) for t in "abcdef"}
        tables["db.s.e"] = (4096, 5.0)                                      # all of its group's changes
        tables.update({f"db.s.q{i}": (4096, 1.0) for i in range(20)})      # 5% each
        got = routing.plan(tables, p, reads_per_day={"db.s.f": 500})
        self.assertEqual({t: (r.silver, r.group) for t, r in got.items() if t in ("db.s.a", "db.s.b", "db.s.c")},
                         {"db.s.a": ("shared", "0"), "db.s.b": ("shared", "0"), "db.s.c": ("shared", "1")})
        self.assertEqual((got["db.s.d"].silver, got["db.s.d"].why), ("view", "needs its own table: streaming reads"))
        self.assertEqual(got["db.s.e"].silver, "merge_round")                # over shared_max_share
        self.assertIn("20% of its group's changes", got["db.s.e"].why)
        self.assertEqual(got["db.s.f"].silver, "view")                       # read often
        self.assertEqual(got["db.s.q3"].silver, "shared")                    # fast, but 4% of the changes
        self.assertEqual(routing.log_for("db.s.x", 4096, 20_000, p).silver, "merge_round")   # its own log: no group
        tr = routing.transitions({"silver": "shared"}, {"silver": "merge_round"})
        self.assertIn("build its own table from the log once", tr[0][1])

    def test_cutover_reads_each_log_for_its_positions(self):   # shared up to P, its own after, no new snapshot
        from lfc_change_source import land, routing
        from lfc_change_source.cdc_log import CdcLog
        fq = routing.history_fq([("`m`.`c`.`cdc_log`", None), ("`m`.`c`.`cdc_log_t`", "00000000000000000042")],
                                "gateway_cursor")
        self.assertIn("FROM `m`.`c`.`cdc_log` WHERE operation IN ('resolved', 'read') OR "
                      "(value:cursor.sequence::STRING <= '00000000000000000042')", fq)
        self.assertIn("FROM `m`.`c`.`cdc_log_t` WHERE operation IN ('resolved', 'read') OR "
                      "(value:cursor.sequence::STRING > '00000000000000000042')", fq)
        self.assertEqual(routing.history_fq([("`m`.`c`.`cdc_log`", None)], "lsn"), "`m`.`c`.`cdc_log`")
        q = CdcLog("s", fq, "t", "db.s.t", ["id"], {"id": "INT"}, seq="gateway_cursor").changes_sql(None, None)
        self.assertIn(f"FROM {fq} WHERE destination = 'db.s.t'", q)
        seen = []
        s = types.SimpleNamespace(sql=lambda q: seen.append(q) or types.SimpleNamespace(collect=lambda: []))
        land.gateway(s, "/V/g", "`m`.`c`.`cdc_log_t`", "gw", only=["db.s.t"])
        land.gateway(s, "/V/g", "`m`.`c`.`cdc_log`", "gw", skip=["db.s.t"])
        self.assertIn("event:table_id.name::STRING) IN ('db.s.t')) FILEFORMAT", seen[0])
        self.assertIn("event:table_id.name::STRING) NOT IN ('db.s.t')) FILEFORMAT", seen[1])

    def test_transitions_name_what_a_route_change_takes(self):
        from lfc_change_source import routing
        same = {"class": "small_slow", "snapshot": "log", "log": "shared", "shards": 1, "silver": "view"}
        self.assertEqual(routing.transitions(same, same), [])
        hot = {"class": "big_fast", "snapshot": "direct", "log": "own", "shards": 3, "silver": "auto_cdc"}
        self.assertEqual([a for a, _ in routing.transitions(same, hot)], ["snapshot", "cutover", "silver", "reshard"])
        busy = dict(same, **{"class": "small_fast", "silver": "merge_round"})
        self.assertIn("build the table from the log once", dict(routing.transitions(same, busy))["silver"])
        self.assertIn("no new snapshot", dict(routing.transitions(same, hot))["cutover"])

    def test_chosen_by_name_and_rates_from_counts(self):
        from lfc_change_source import routing
        p = routing.Policy(shared_log="`m`.`c`.`cdc_log`", chosen={"db.s.region": "big_fast"},
                           silver_chosen={"db.s.nation": "merge_round"})
        self.assertEqual(routing.log_for("db.s.region", 10, 0, p).cls, "big_fast")
        self.assertEqual(routing.log_for("db.s.nation", 10, 0, p).silver, "merge_round")   # a view by class, chosen
        with self.assertRaises(ValueError):
            routing.classify(1, 1, routing.Policy("x", chosen={"t": "huge"}), "t")
        self.assertEqual(routing.rates({"a": 100}, {"a": 700, "b": 30}, 60), {"a": 10.0, "b": 0.5})


class Conformance(unittest.TestCase):
    def test_scenario_truth(self):
        from lfc_change_source import conformance
        self.assertEqual(conformance.expected(), {1: "a2", 2: None, 3: "c2", 4: "d", 5: None})
        self.assertLess(conformance._order({"v": "9", "o": "1"}), conformance._order({"v": "10", "o": "0"}))

    def test_every_adapter_has_the_contract_and_build(self):
        import inspect
        from lfc_change_source import ADAPTERS
        for name, mod in ADAPTERS.items():
            self.assertTrue(callable(getattr(mod, "build", None)), name)
            classes = [c for _, c in inspect.getmembers(mod, inspect.isclass) if c.__module__ == mod.__name__]
            self.assertTrue(any(all(hasattr(c, m) for m in ("snapshot_sql", "changes_sql", "watermark", "stats"))
                                for c in classes), name)


class Engine(unittest.TestCase):
    def test_phases_from_either_sdk_shape(self):   # an older SDK (a job's environment) returns ListQueriesResponse
        from lfc_change_source.engine import Warehouse
        m = types.SimpleNamespace(compilation_time_ms=2000, execution_time_ms=5000,
                                  query_compilation_start_timestamp=3000, read_bytes=10, read_files_count=1)
        q = types.SimpleNamespace(query_id="s1", is_final=True, query_start_time_ms=1000, metrics=m)
        for ret in ([q], types.SimpleNamespace(res=[q])):
            w = types.SimpleNamespace(query_history=types.SimpleNamespace(list=lambda ret=ret, **k: ret))
            self.assertEqual(Warehouse("x", w).phases(["s1"], wait_s=1)["s1"]["queue_s"], 2.0)


class Samples(unittest.TestCase):
    here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "samples")

    def test_gateway_sample_columns_map_through_its_schema(self):
        import json
        g = os.path.join(self.here, "gateway")
        schema = json.load(open(os.path.join(g, "target_schema.0.json")))
        cols = {c["uoid"]: c["name"] for c in schema["schemas"][0]["tables"][0]["columns"]}
        lines = [json.loads(x) for x in open(os.path.join(g, "cdc_history_update.jsonl")) if x.strip()]
        self.assertEqual([x["operation"] for x in lines], ["DELETE", "INSERT"])   # an update of a keyless table
        self.assertEqual(len({x["cursor"]["sequence"] for x in lines}), 1)        # at one position
        for x in lines:
            self.assertEqual(set(x["data"]) - set(cols), set())                 # every column id has a name


if __name__ == "__main__":
    unittest.main()
