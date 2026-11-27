"""Offline checks of the zerobus_debezium package: each engine's Debezium settings, setup steps and rounds, and the
application.properties text. No database, no workspace.

    python3 -m unittest skills/zerobus-debezium-test/tests/test_zerobus_debezium.py
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))

from zerobus_debezium import engines  # noqa: E402

CONN = {"host": "203.0.113.10", "port": 30021, "user": "lfc_user", "password": "p\\w"}


class EngineTest(unittest.TestCase):
    def test_every_engine_has_its_connector_and_offsets(self):
        for name, e in engines.ENGINES.items():
            p = e.source_properties(CONN, "/Volumes/c/s/v/dbz-x", "dbz-x")
            self.assertEqual(p["debezium.source.connector.class"], e.connector, name)
            self.assertEqual(p["debezium.source.offset.storage.file.filename"], "/Volumes/c/s/v/dbz-x/offsets.dat")
            self.assertEqual(p["debezium.source.topic.prefix"], "dbz_x")
            self.assertEqual(p["debezium.source.snapshot.mode"], "no_data")
            self.assertEqual(p["debezium.source.table.include.list"], e.table)

    def test_schema_history_where_asked(self):
        """A Volume refuses appends ("Illegal seek"): the notebook puts the history on local disk."""
        p = engines.get("mysql").source_properties(CONN, "/Volumes/c/s/v/x", "x", history_folder="/tmp/run")
        self.assertEqual(p["debezium.source.schema.history.internal.file.filename"], "/tmp/run/schema-history.dat")
        self.assertNotIn("debezium.source.schema.history.internal",
                         engines.get("postgres").source_properties(CONN, "/v", "x"))

    def test_rounds_cover_the_expected_events(self):
        for name, e in engines.ENGINES.items():
            for rnd in (1, 2, 3):
                sql = e.changes(rnd, 100)
                for op, i in e.expected(rnd, 100):
                    self.assertIn(str(i), sql, f"{name} round {rnd}: {op} {i}")
        self.assertIn("COMMIT", engines.get("oracle").changes(1, 100))
        self.assertTrue(engines.get("sqlserver").changes(1, 100).startswith("USE [dbz]"))

    def test_setup_steps_are_known_kinds(self):
        for name, e in engines.ENGINES.items():
            kinds = {k for k, _ in e.setup(CONN)}
            self.assertLessEqual(kinds, {"sql", "shell", "shell_detached", "sleep"}, name)
        self.assertIn("shell_detached", {k for k, _ in engines.get("db2").setup(CONN)})   # the capture agent

    def test_db2_waits_longer(self):
        self.assertGreater(engines.get("db2").lag_s, engines.get("mysql").lag_s)

    def test_unknown_engine(self):
        with self.assertRaises(ValueError):
            engines.get("sybase")


class PropertiesTest(unittest.TestCase):
    def test_render_escapes_backslashes(self):
        self.assertEqual(engines.render({"a": "p\\w", "b": 1}), "a=p\\\\w\nb=1\n")

    def test_sink_table_unquoted(self):
        s = engines.sink_properties("https://1.zerobus.r.azuredatabricks.net", "https://adb-1.azuredatabricks.net",
                                    "app", "sec", "`c`.`s`.`cdc_log`")
        self.assertEqual(s["debezium.sink.zerobus.table"], "c.s.cdc_log")
        self.assertEqual(s["debezium.sink.type"], "zerobus")


if __name__ == "__main__":
    unittest.main()
