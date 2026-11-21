"""Offline tests for lfc_saas_pipeline (the SDK client is faked; no workspace)."""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import lfc_saas_pipeline as p  # noqa: E402


class FakeApi:
    def __init__(self, answers):
        self.answers, self.calls = answers, []

    def do(self, method, path, body=None, query=None):
        self.calls.append((method, path, body, query))
        for (m, prefix), out in self.answers.items():
            if m == method and path.startswith(prefix):
                return out(len(self.calls)) if callable(out) else out
        return {}


class PipelineTest(unittest.TestCase):
    def use(self, answers):
        api = FakeApi(answers)
        p._client = type("W", (), {"api_client": api})()
        return api

    def tearDown(self):
        p._client = None

    def test_create_takes_the_docs_json_string_or_a_dict_and_lets_users_view(self):
        api = self.use({("POST", "/api/2.0/pipelines"): {"pipeline_id": "p1"}})
        spec = {"name": "n", "ingestion_definition": {"connection_name": "c", "objects": []}}
        self.assertEqual(p.create_pipeline(json.dumps(spec))["pipeline_id"], "p1")
        self.assertEqual(api.calls[0][:3], ("POST", "/api/2.0/pipelines", spec))
        self.assertEqual(api.calls[1], ("PATCH", "/api/2.0/permissions/pipelines/p1",
                                        {"access_control_list": [{"group_name": "users",
                                                                  "permission_level": "CAN_VIEW"}]}, None))
        api.calls.clear()
        p.create_pipeline(spec, viewers=())
        self.assertEqual(len(api.calls), 1)   # no grant

    def test_start_sends_the_docs_body_plus_validate_only_and_a_selection(self):
        api = self.use({("POST", "/api/2.0/pipelines/p1/updates"): {"update_id": "u1"}})
        p.start_pipeline("p1")
        self.assertEqual(api.calls[0][2], {"full_refresh": False, "validate_only": False, "cause": "API_CALL"})
        p.start_pipeline("p1", validate_only=True, refresh_selection=["audit_events"])
        self.assertEqual(api.calls[1][2]["refresh_selection"], ["audit_events"])
        self.assertTrue(api.calls[1][2]["validate_only"])

    def test_wait_update_returns_the_final_state(self):
        states = iter(["RUNNING", "RUNNING", "COMPLETED"])
        self.use({("GET", "/api/2.0/pipelines/p1/updates/u1"): lambda _n: {"update": {"state": next(states)}}})
        self.assertEqual(p.wait_update("p1", "u1", sleep=lambda _s: None), "COMPLETED")

    def test_list_filters_and_arg_falls_back_outside_a_notebook(self):
        api = self.use({})
        p.list_pipeline("name LIKE 'x%'")
        self.assertEqual(api.calls[0][3], {"filter": "name LIKE 'x%'"})
        self.assertEqual(p.arg("connection", "lfcsaas-1password"), "lfcsaas-1password")

    def test_each_notebook_and_its_bundle_ingest_the_same_tables_through_the_same_connection(self):
        import re
        root = Path(__file__).resolve().parents[1]
        pairs = {"1password-event-logs": "lfcsaas-1password", "marketo": "lfcsaas-marketo", "netsuite": "lfcsaas-netsuite",
                 "square": "lfcsaas-square", "wiz-audit-logs": "lfcsaas-wiz", "shopify": "lfcsaas-shopify", "zoom": "lfcsaas-zoom", "atlassian-audit-logs": "lfcsaas-atlassian-audit-logs", "openai": "lfcsaas-openai", "adobe-commerce": "lfcsaas-adobe-commerce", "meta-ads": "lfcsaas-metaads"}
        self.assertEqual(sorted(p.stem.removesuffix("-pipeline") for p in (root / "notebooks").glob("*.py")),
                         sorted(pairs))
        for slug, conn in pairs.items():
            nb = (root / f"notebooks/{slug}-pipeline.py").read_text()
            bundle = root / f"bundles/{slug}"
            yml = "".join(p.read_text() for p in sorted((bundle / "resources").glob("*.yml")))
            nb_tables = re.findall(r'"([a-z_0-9]+)"', re.search(r"for t in \(([^)]*)\)", nb).group(1))
            yml_tables = re.findall(r"source_table: ([a-z_0-9]+)", yml)
            self.assertEqual(nb_tables, yml_tables, slug)
            self.assertIn(f'arg("connection", "{conn}")', nb)
            self.assertIn(f"default: {conn}", (bundle / "databricks.yml").read_text())
            self.assertIn("group_name: users", yml)
            self.assertTrue(nb.startswith("# Databricks notebook source\n"))


if __name__ == "__main__":
    unittest.main()
