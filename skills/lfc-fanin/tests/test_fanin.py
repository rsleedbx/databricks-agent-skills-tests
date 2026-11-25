"""Offline checks of lfc_fanin.fanin: names quoted part by part, what each connector type refuses, which errors
mean "not written yet", and that a connector type can be added.

    python3 -m unittest skills/lfc-fanin/tests/test_fanin.py
"""
import os
import sys
import types
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
sys.path.insert(0, os.path.join(HERE, "..", "..", "lfc-change-source", "scripts"))
from lfc_fanin import fanin  # noqa: E402

SPARK = types.SimpleNamespace(sql=lambda *a, **k: types.SimpleNamespace(collect=lambda: []))


class Names(unittest.TestCase):
    def test_quoted_part_by_part(self):
        self.assertEqual(fanin.quoted("catalog-zb.s.cdc_log"), "`catalog-zb`.`s`.`cdc_log`")
        self.assertEqual(fanin.quoted("`c`.`s`.`t`"), "`c`.`s`.`t`")

    def test_not_yet_only_a_missing_volume_folder(self):
        self.assertTrue(fanin.not_yet(Exception("[PATH_NOT_FOUND] Path does not exist: dbfs:/Volumes/c/s/v/orders")))
        self.assertFalse(fanin.not_yet(Exception("[TABLE_OR_VIEW_NOT_FOUND] `c`.`s`.`cdc_log` cannot be found")))


class Refuses(unittest.TestCase):
    def test_dbz_needs_log_volume_and_source_type(self):
        for d, why in (({}, "change_log"), ({"change_log": "c.s.l", "snapshot_volume": "/Volumes/c/s/v"}, "source_type"),
                       ({"change_log": "c.s.l", "snapshot_volume": "/tmp/x", "source_type": "SQLSERVER"}, "snapshot_volume")):
            with self.assertRaises(ValueError) as e:
                fanin.create(None, SPARK, "f", "c", "s", {"connector_type": "fanin_dbz_zbus", **d})
            self.assertIn(why, str(e.exception))

    def test_crdb_tables_with_keys_and_their_own_folders(self):
        base = {"connector_type": "fanin_crdb_cdf", "changefeed_path": "/Volumes/c/s/v"}
        t = {"source_catalog": "shop", "source_schema": "public", "source_table": "orders"}
        for objs, why in (([{"schema": {"source_catalog": "shop", "source_schema": "public"}}], "no schema objects"),
                          ([{"table": t}], "primary_keys"),
                          ([{"table": {**t, "primary_keys": ["id"]}},
                            {"table": {**t, "source_schema": "x", "primary_keys": ["id"]}}], "share a changefeed folder")):
            with self.assertRaises(ValueError) as e:
                fanin.create(None, SPARK, "f", "c", "s", {**base, "objects": objs})
            self.assertIn(why, str(e.exception))

    def test_unknown_type_and_an_added_one(self):
        with self.assertRaises(ValueError):
            fanin.create(None, SPARK, "f", "c", "s", {"connector_type": "nope"})
        seen = []
        fanin.CONNECTORS["probe"] = (lambda w, s, p, d: p["parts"].update(x=1), lambda w, s, p: seen.append(p) or {"x": 1})
        try:
            p = fanin.create(None, SPARK, "f", "c", "s", {"connector_type": "probe"})
            self.assertEqual((p["parts"], fanin.land(None, SPARK, p)), ({"x": 1}, {"x": 1}))
        finally:
            del fanin.CONNECTORS["probe"]


if __name__ == "__main__":
    unittest.main()
