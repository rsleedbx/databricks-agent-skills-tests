"""Offline checks of medallion.py (layer counts, classification) and of pipeline-diagram.py's medallion and Zerobus
drawing. Names are placeholders."""
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.join(HERE, "..", "scripts")
spec = importlib.util.spec_from_file_location("medallion", os.path.join(SCRIPTS, "medallion.py"))
med = importlib.util.module_from_spec(spec)
spec.loader.exec_module(med)


HERMETIC = {**os.environ, "LFC_SAAS_TABLES": os.devnull + ".none", "LFC_FACTS_DIR": os.devnull + ".none"}


def diagram(*args):
    return subprocess.run([sys.executable, os.path.join(SCRIPTS, "pipeline-diagram.py"), *args],
                          check=True, capture_output=True, text=True, env=HERMETIC).stdout


class Classify(unittest.TestCase):
    def k(self, table_type, flow=None, scd2=False, lfc=False, written=None):
        t = {"full_name": "c.s.t", "table_type": table_type, "scd2": scd2, "lfc": lfc}
        return med.kind_of(t, {"c.s.t": flow} if flow else None, written)

    def test_pipeline_flows(self):
        self.assertEqual(self.k("STREAMING_TABLE", "CHANGE"), "scd1")
        self.assertEqual(self.k("STREAMING_TABLE", "CHANGE", scd2=True), "scd2")
        self.assertEqual(self.k("STREAMING_TABLE", "SNAPSHOT_CHANGE", scd2=True), "scd2")
        self.assertEqual(self.k("STREAMING_TABLE", "APPEND"), "append")
        self.assertEqual(self.k("MATERIALIZED_VIEW", "MATERIALIZED_VIEW"), "recomputed")
        self.assertEqual(self.k("STREAMING_TABLE", "MERGE"), "keyed")

    def test_outside_a_pipeline(self):
        self.assertEqual(self.k("VIEW"), "view")
        self.assertEqual(self.k("STREAMING_TABLE", lfc=True), "scd1")         # a Lakeflow Connect destination
        self.assertEqual(self.k("MANAGED", scd2=True), "scd2")
        self.assertEqual(self.k("MANAGED", written="append"), "append")       # Zerobus bronze
        self.assertEqual(self.k("MANAGED"), "unknown")

    def test_counts(self):
        rows = [{"full_name": f"c.s.t{i}", "table_type": tt, "kind": k} for i, (tt, k) in enumerate(
            [("STREAMING_TABLE", "scd1"), ("STREAMING_TABLE", "scd2"), ("MATERIALIZED_VIEW", "recomputed"), ("VIEW", "view")])]
        lay = med.summarize("silver", ["c.s"], rows)
        self.assertEqual((lay["tables"], lay["materialized"], lay["not_materialized"]), (4, 3, 1))
        self.assertEqual(med.layer_line(lay), "4 tables: 3 materialized (1 SCD 1, 1 SCD 2, 1 recomputed (MV)), "
                                              "1 not materialized (1 view)")


class Workspace(unittest.TestCase):
    """The workspace-reading paths, on canned CLI output (med.cli patched)."""

    def setUp(self):
        self.calls = []
        self.real = med.cli

    def tearDown(self):
        med.cli = self.real

    def fake(self, responses):
        def cli(args, profile):
            self.calls.append(args)
            for prefix, value in responses:
                if args[:len(prefix)] == prefix:
                    if isinstance(value, Exception):
                        raise value
                    return value
            raise AssertionError(f"unexpected call {args}")
        med.cli = cli

    def test_flow_types_newest_first_error_and_cache(self):
        ev = {"events": [{"details": {"flow_definition": {"output_dataset": "c.s.t", "flow_type": "CHANGE"}}},
                         {"details": {"flow_definition": {"output_dataset": "c.s.t", "flow_type": "APPEND"}}},
                         {"details": {"other": {}}}]}
        self.fake([(["api", "get"], ev)])
        cache = {}
        self.assertEqual(med.flow_types("p1", None, cache), {"c.s.t": "CHANGE"})   # the newest event wins
        med.flow_types("p1", None, cache)
        self.assertEqual(len(self.calls), 1)                                       # cached per pipeline
        self.fake([(["api", "get"], subprocess.CalledProcessError(1, "databricks"))])
        self.assertEqual(med.flow_types("p2", None, {}), {})                       # an API error: no flow types

    def test_flow_types_pages_back_and_stops_at_an_older_update(self):
        fd = lambda out, ft, u: {"origin": {"update_id": u}, "details": {"flow_definition": {"output_dataset": out, "flow_type": ft}}}
        other = lambda u: {"origin": {"update_id": u}, "details": {"flow_progress": {}}}
        pages = {None: {"events": [other("u2")] * 3, "next_page_token": "t1"},           # the newest update's progress
                 "t1": {"events": [fd("c.s.a", "CHANGE", "u2"), fd("c.s.b", "APPEND", "u2"), other("u1"),
                                   fd("c.s.a", "MATERIALIZED_VIEW", "u1")], "next_page_token": "t2"},
                 "t2": {"events": [fd("c.s.z", "APPEND", "u0")]}}
        def cli(args, profile):
            self.calls.append(args)
            tok = args[2].split("page_token=")[1] if "page_token=" in args[2] else None
            return pages[tok]
        med.cli = cli
        self.assertEqual(med.flow_types("p", None, {}), {"c.s.a": "CHANGE", "c.s.b": "APPEND"})
        self.assertIn("max_results=250", self.calls[0][2])                         # the API's most per page
        self.assertEqual(len(self.calls), 2)                                       # stopped at update u1, no page t2

    def test_names_compare_without_backticks_or_case(self):
        self.assertEqual(med.kind_of({"full_name": "Main.S.T", "table_type": "STREAMING_TABLE", "scd2": False},
                                     {med.norm("`main`.`s`.`t`"): "APPEND"}, None), "append")

    def test_parse_layer_and_written(self):
        self.assertEqual(med.parse_layer("silver=c.a,c.b:only=t1,t2"), ("silver", ["c.a", "c.b"], {"t1", "t2"}))
        self.assertEqual(med.parse_layer("gold=c.g"), ("gold", ["c.g"], None))
        self.fake([(["tables", "list"], [{"name": "cdc_log", "table_type": "MANAGED", "columns": [{"name": "v"}]}])])
        import argparse
        a = argparse.Namespace(layers_json=None, layer=["bronze=c.s"], written=["bronze=append"], profile=None)
        self.assertEqual(med.collect(a)[0]["by_kind"], {"append": 1})
        a.written = ["bronze=merge"]
        self.assertEqual(med.collect(a)[0]["by_kind"], {"keyed": 1})                  # merge is counted as keyed
        a.written = ["bronze=upsert"]
        with self.assertRaises(SystemExit):
            med.collect(a)

    def test_count_layer(self):
        cols = lambda *n: [{"name": x} for x in n]
        tables = [
            {"name": "lfc_t", "table_type": "STREAMING_TABLE", "columns": cols("id"),
             "properties": {"__managed_ingestion.source.type": "cdc"}},
            {"name": "hist", "table_type": "STREAMING_TABLE", "columns": cols("id", "__START_AT", "__END_AT"),
             "properties": {"pipelines.pipelineId": "p1"}},
            {"name": "mv", "table_type": "MATERIALIZED_VIEW", "columns": cols("id"), "properties": {"pipelines.pipelineId": "p1"}},
            {"name": "v", "table_type": "VIEW", "columns": cols("id")},
            {"name": "skipped", "table_type": "MANAGED", "columns": cols("id")},
        ]
        ev = {"events": [{"details": {"flow_definition": {"output_dataset": "c.s.hist", "flow_type": "SNAPSHOT_CHANGE"}}},
                         {"details": {"flow_definition": {"output_dataset": "c.s.mv", "flow_type": "MATERIALIZED_VIEW"}}}]}
        self.fake([(["tables", "list"], tables), (["api", "get"], ev)])
        lay = med.count_layer("silver", ["c.s"], None, only={"lfc_t", "hist", "mv", "v"})
        self.assertEqual((lay["tables"], lay["materialized"], lay["not_materialized"]), (4, 3, 1))
        self.assertEqual(lay["by_kind"], {"scd1": 1, "scd2": 1, "recomputed": 1, "view": 1})
        self.assertEqual(sum(1 for c in self.calls if c[:2] == ["api", "get"]), 1)  # one events read for pipeline p1


class Unreadable(unittest.TestCase):
    def test_no_access_is_drawn_not_raised(self):
        real = med.cli
        def cli(args, profile):
            raise subprocess.CalledProcessError(1, "databricks", stderr="Error: no USE CATALOG on Catalog 'c'.\nmore")
        med.cli = cli
        try:
            lay = med.count_layer("landing", ["c.s"], None)
        finally:
            med.cli = real
        self.assertEqual(lay["tables"], 0)
        self.assertEqual(med.layer_line(lay), "not readable (c.s: Error: no USE CATALOG on Catalog 'c'.)")
        self.assertIn("not readable", med.mermaid([lay], None)[0][1])


class Drawing(unittest.TestCase):
    def setUp(self):
        self.layers = [{"name": "silver", "schemas": ["main.demo_s"], "tables": 3, "materialized": 3,
                        "not_materialized": 0, "by_kind": {"scd1": 2, "scd2": 1}},
                       {"name": "gold", "schemas": ["main.demo_g"], "tables": 2, "materialized": 1,
                        "not_materialized": 1, "by_kind": {"recomputed": 1, "view": 1}}]
        fd, self.lj = tempfile.mkstemp(suffix=".json")
        with os.fdopen(fd, "w") as f:
            json.dump(self.layers, f)

    def tearDown(self):
        os.unlink(self.lj)

    def test_lakeflow_with_layers(self):
        out = diagram("--pipeline-json", os.path.join(HERE, "integrated-pipeline.json"),
                      "--connection-json", os.path.join(HERE, "integrated-connection.json"),
                      "--layers-json", self.lj, "--reverse-etl", "Lakebase Postgres:synced tables:3")
        self.assertIn("    L1 landing", out)                                  # what landed is layer 1
        self.assertIn("    L2 silver   main.demo_s: 3 tables: 3 materialized (2 SCD 1, 1 SCD 2)", out)
        self.assertIn("1 not materialized (1 view)", out)
        self.assertIn("reverse ETL -> Lakebase Postgres (synced tables): 3 tables", out)
        self.assertIn("T --> L2 --> L3", out)                                 # the landing node is L1
        self.assertIn("L3 --> RE", out)
        self.assertEqual(out.count("    T[("), 1)                             # the landing tables drawn once

    def test_zerobus(self):
        out = diagram("--ingest", "zerobus", "--engine", "mysql", "--bronze", "main.demo_zb.cdc_log",
                      "--primary", "203.0.113.10,30001", "--firewall", "NSG", "--layers-json", self.lj)
        self.assertIn("Debezium Server (on the source host): reads the changes, pushes each to Zerobus", out)
        self.assertIn("Zerobus ingest endpoint  <-- service principal (OAuth)", out)
        self.assertIn("L1 bronze  main.demo_zb: 1 table: 1 materialized (1 append-only)", out)
        self.assertIn("CAP --> PROD", out)
        self.assertIn("PROD --- FW --- ZB", out)
        self.assertIn("change capture: binlog", out)

    def test_zerobus_capture_per_engine(self):
        def run(engine):
            return diagram("--ingest", "zerobus", "--engine", engine, "--bronze", "main.demo_zb.cdc_log")
        out = run("sqlserver")
        self.assertIn("change tables cdc.<capture instance>_CT", out)
        self.assertNotIn("utility objects", out)                          # Lakeflow Connect's, not Debezium's
        out = run("yugabytedb")
        self.assertIn("source: database host, YugabyteDB", out)
        self.assertIn("change capture: logical replication", out)

    def test_zerobus_needs_engine_and_bronze(self):
        r = subprocess.run([sys.executable, os.path.join(SCRIPTS, "pipeline-diagram.py"), "--ingest", "zerobus"],
                           capture_output=True, text=True, check=False)
        self.assertEqual(r.returncode, 2)
        self.assertIn("needs --engine and --bronze", r.stderr)



class ManagedHosts(unittest.TestCase):
    def test_service_and_cloud_from_the_host(self):
        spec = importlib.util.spec_from_file_location("diagram", os.path.join(SCRIPTS, "pipeline-diagram.py"))
        d = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(d)
        for host, want in (("srv.database.windows.net", ("azure", "Azure SQL Database")),
                           ("mi.public.abc123.database.windows.net", ("azure", "Azure SQL Managed Instance (public endpoint)")),
                           ("mi.abc123.database.windows.net", ("azure", "Azure SQL Managed Instance")),
                           ("pg.postgres.database.azure.com", ("azure", "Azure Database for PostgreSQL flexible server")),
                           ("db.cluster-abc.us-east-2.rds.amazonaws.com", ("aws", "Amazon Aurora")),
                           ("db.abc.us-east-2.rds.amazonaws.com", ("aws", "Amazon RDS")),
                           ("x.j77.aws-us-east-2.cockroachlabs.cloud", ("aws", "CockroachDB Cloud")),
                           ("203.0.113.10", None)):
            self.assertEqual(d.managed_source(host), want, host)


if __name__ == "__main__":
    unittest.main()

