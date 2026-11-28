"""Offline tests for the pipeline tools (lfc_events, lfc_runtime, compute_report, stop_after): a fake REST answer
in place of the workspace, the summary's sections, the runtime's snapshot diff, the compute facts of an update."""
from __future__ import annotations

import sys
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "skills" / "lfc-rest" / "scripts"))
from lfc_rest import Rest  # noqa: E402

import compute_report  # noqa: E402
import lfc_events  # noqa: E402
import lfc_runtime  # noqa: E402
import stop_after  # noqa: E402

PIPE = "3f2a1b4c-0000-4000-8000-000000000001"


def fake_rest(answers):
    """A Rest whose GETs and POSTs are answered by answers(method, path, query): a dict, or a callable on the query."""
    def do(method, path, query, body, **kw):
        return answers(method, path, query or {}, body)
    return Rest(do, name="fake")


class Events(unittest.TestCase):
    def ev(self, typ, time_, **kw):
        base = {"time": time_, "type": typ, "level": kw.pop("level", "INFO"), "update": kw.pop("update", "u1"),
                "flow": kw.pop("flow", ""), "message": kw.pop("message", ""), "details": kw.pop("details", {}),
                "error_class": ""}
        return base

    def test_parse_defaults_and_flags(self):
        o = lfc_events.parse(["p", PIPE])
        self.assertEqual((o["since"], o["source"], o["max"]), ("24", "auto", "0"))
        self.assertIsNone(lfc_events.parse(["p"]))
        self.assertIsNone(lfc_events.parse(["p", PIPE, "--nope"]))
        self.assertEqual(lfc_events.parse(["p", PIPE, "--update", "ab", "--json"])["update"], "ab")

    def test_check_args_names_the_bad_one(self):
        base = lfc_events.parse(["p", PIPE])
        self.assertEqual(lfc_events.check_args({**base, "pipe": "nope"}), "pipeline id should be a UUID: nope")
        self.assertEqual(lfc_events.check_args({**base, "levels": "INFO,DEBUG"}),
                         "--levels: INFO, WARN, ERROR, comma separated")
        self.assertEqual(lfc_events.check_args(base), "")

    def test_summary_sections(self):
        evs = [
            self.ev("create_update", "2026-10-26T22:00:00.000Z", update="abcdef123",
                    details={"create_update": {"full_refresh": True}}),
            self.ev("update_progress", "2026-10-26T22:20:00.000Z", update="abcdef123",
                    details={"update_progress": {"state": "COMPLETED"}}),
            self.ev("remote_api_usage", "2026-10-26T22:01:00.000Z",
                    details={"remote_api_usage": {"api_name": "jira", "num_requests": 3}}),
            self.ev("remote_api_usage", "2026-10-26T22:02:00.000Z",
                    details={"remote_api_usage": {"api_name": "jira", "num_requests": 2}}),
            self.ev("cluster_resources", "2026-10-26T22:03:00.000Z",
                    details={"cluster_resources": {"autoscale_info": {"state": "ACTIVE"},
                                                   "task_slot_metrics": {"num_task_slots": 8}}}),
            self.ev("flow_progress", "2026-10-26T22:04:00.000Z", flow="main.s.t_cdc_flow",
                    details={"flow_progress": {"metrics": {"num_upserted_rows": 5, "backlog_bytes": 10}}}),
            self.ev("operation_progress", "2026-10-26T22:05:00.000Z",
                    details={"operation_progress": {"status": "DONE", "snapshot": {
                        "target_table_name": "main.s.orders", "snapshot_reason": "first"}}}),
            self.ev("stream_progress", "2026-10-26T22:06:00.000Z", details={"stream_progress": {
                "progress_json": '{"name": "main.s.flow", "batchDuration": 100}'}}),
            self.ev("gc_pressure", "2026-10-26T22:07:00.000Z", level="WARN", message="gc"),
        ]
        lines = lfc_events.summary(evs, "api")
        text = "\n".join(lines)
        self.assertIn("  jira: 2 events, 5 requests", text)
        self.assertIn("  2026-10-26T22:03:00.000Z  {\"autoscale\":\"ACTIVE\",\"num_task_slots\":8}", text)
        self.assertIn("  2026-10-26T22:00:00.000Z  abcdef12  full_refresh=true  COMPLETED", text)
        self.assertIn("  t_cdc_flow  upserted=5 deleted=0 output=0 max_backlog_bytes=10", text)
        self.assertIn("  2026-10-26T22:05:00.000Z  snapshot orders  DONE  first", text)
        self.assertIn("  flow  batches=1 avg_ms=100 max_ms=100", text)
        self.assertIn("  2026-10-26T22:07:00.000Z  gc_pressure  WARN  gc", text)

    def test_summary_none_lines(self):
        text = "\n".join(lfc_events.summary([], "table"))
        self.assertIn("  none (not a SaaS pipeline, or none in the window)", text)
        self.assertIn("Newest cluster_resources:\n  none (none in the window)", text)
        self.assertIn("  none with rows or backlog", text)

    def test_json_newest_first_and_cut(self):
        evs = [self.ev("flow_progress", f"2026-10-26T22:0{i}:00.000Z", message="x" * 600,
                       details={"k": "v" * 50}) for i in range(3)]
        import json
        rows = json.loads(lfc_events.json_out(evs, 2, 20))
        self.assertEqual([r["timestamp"][:16] for r in rows], ["2026-10-26T22:02", "2026-10-26T22:01"])
        self.assertEqual(len(rows[0]["message"]), 500)
        self.assertEqual(len(rows[0]["details"]), 20)

    def test_levels_are_sent_to_the_api(self):
        seen = []

        def answers(m, p, q, b):
            seen.append(q)
            return {"events": [{"timestamp": "2026-10-26T22:05:00.000Z", "event_type": "a", "level": "WARN"}]}
        got = lfc_events.Source(fake_rest(answers), PIPE, 0, "").from_api("", "WARN,ERROR", 0)
        self.assertEqual([e["level"] for e in got], ["WARN"])
        self.assertEqual(seen[0]["filter"], "level in ('WARN', 'ERROR')")

    def test_api_source_filters_update_and_window(self):
        now = datetime.now(timezone.utc)
        recent = (now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        old = (now - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        page = {"events": [
            {"timestamp": recent, "event_type": "a", "level": "INFO", "origin": {"update_id": "abc1"}},
            {"timestamp": recent, "event_type": "b", "level": "INFO", "origin": {"update_id": "def2"}},
            {"timestamp": old, "event_type": "c", "level": "INFO", "origin": {"update_id": "abc1"}},
        ]}
        api = fake_rest(lambda m, p, q, b: page)
        src = lfc_events.Source(api, PIPE, 1, "")
        got = src.from_api("abc", "", 0)
        self.assertEqual([e["type"] for e in got], ["a"])
        self.assertEqual(len(src.from_api("", "", 0)), 2)   # the window: the three days' event is out


class Runtime(unittest.TestCase):
    def test_kind_of(self):
        self.assertEqual(lfc_runtime.kind_of({"gateway_definition": {"connection_name": "c"}}), "gateway")
        self.assertEqual(lfc_runtime.kind_of({"ingestion_definition": {"ingestion_gateway_id": "x"}}),
                         "standard-applier")
        self.assertEqual(lfc_runtime.kind_of({"ingestion_definition": {"connector_type": "CDC"}}), "integrated")
        self.assertEqual(lfc_runtime.kind_of({"ingestion_definition": {"objects": []}}), "query-based")
        self.assertEqual(lfc_runtime.kind_of({}), "other")

    def test_runtime_event_picks_latest_of_the_update(self):
        evs = [
            {"event_type": "runtime_details", "timestamp": "2026-10-26T01", "origin": {"update_id": "aa11"}},
            {"event_type": "runtime_details", "timestamp": "2026-10-26T02", "origin": {"update_id": "aa22"}},
            {"event_type": "runtime_details", "timestamp": "2026-10-26T03", "origin": {"update_id": "bb33"}},
        ]
        self.assertEqual(lfc_runtime.runtime_event(evs, "aa")["timestamp"], "2026-10-26T02")
        self.assertEqual(lfc_runtime.runtime_event(evs, "")["timestamp"], "2026-10-26T03")
        self.assertEqual(lfc_runtime.runtime_event(evs, "cc"), {})

    def test_settings_text_and_diff(self):
        rt = {"details": {"runtime_details": {"runtime_configuration": {"configuration": {
            "pipelines.a": "1", "pipelines.b": {"x": 2}}}}}}
        text = lfc_runtime.settings_text(rt, "18.3", "CURRENT", "dlt:img")
        self.assertIn("_runtime.dbr\t18.3\n", text)
        self.assertIn('pipelines.b\t{"x": 2}\n', text)
        prev = {"a": "1", "b": "2", "c": "3"}
        now = {"a": "1", "b": "9", "d": "4"}
        self.assertEqual(lfc_runtime.diff_lines(prev, now),
                         ["  ~ b: 2 -> 9", "  - c (was 3)", "  + d = 4"])
        self.assertEqual(lfc_runtime.diff_lines(prev, prev), ["  no changes"])


class Compute(unittest.TestCase):
    def api(self, events, serverless=True):
        upd = {"update": {"state": "RUNNING", "config": {"serverless": serverless}}}
        pipe = {"name": "janedoe-x", "spec": {"serverless": True},
                "latest_updates": [{"update_id": "u1234567", "state": "FAILED",
                                    "creation_time": "2026-10-26T02:00:00Z"}]}

        def answers(m, p, q, b):
            if p == "/api/2.0/pipelines":
                return {"statuses": [{"pipeline_id": PIPE}]}
            if p.endswith("/events"):
                return {"events": events}
            if "/updates/" in p:
                return upd
            return pipe
        return fake_rest(answers)

    def test_update_facts_started_reached_and_error(self):
        events = [
            {"message": "Update u1234567 is INITIALIZING.", "origin": {"update_id": "u1234567"}, "level": "INFO"},
            {"message": "Flow fails", "origin": {"update_id": "u1234567"}, "level": "ERROR",
             "error": {"exceptions": [{"message": "[CONNECTION_ERROR.X] denied"}]}},
            {"message": "x", "origin": {"update_id": "other"}, "level": "ERROR",
             "error": {"exceptions": [{"message": "[NOT_MINE_ERROR]"}]}},
        ]
        ran, started, reached, err = compute_report.update_facts(self.api(events), PIPE, "u1234567", "FAILED", 0)
        self.assertEqual((ran, started, reached, err), ("serverless", "yes", "no", "CONNECTION_ERROR.X"))
        events[1]["error"]["exceptions"][0]["message"] = "[FLOW_SCHEMA_CHANGED_ERROR.X] changed"
        ran, started, reached, err = compute_report.update_facts(self.api(events), PIPE, "u1234567", "FAILED", 0)
        self.assertEqual((reached, err), ("unknown", "FLOW_SCHEMA_CHANGED_ERROR.X"))

    def test_rows_read_each_pipeline_once(self):
        events = [{"message": "Update u1234567 is RUNNING.", "origin": {"update_id": "u1234567"}, "level": "INFO"}]
        rows = compute_report.rows(self.api(events, serverless=False), "janedoe-", "")
        self.assertEqual(rows, [["janedoe-x", "pipeline", "true", "classic", "yes", "yes", "FAILED", ""]])
        self.assertEqual(compute_report.rows(self.api(events), "janedoe-", "2026-11-01T00:00:00Z"), [])

    def test_creation_time_as_ms_or_iso(self):
        ms, iso = compute_report.created_ms_and_iso(1761516300000)
        self.assertEqual((ms, iso), (1761516300000.0, "2025-10-26T22:05:00Z"))
        ms, iso = compute_report.created_ms_and_iso("2026-10-26T02:00:00Z")
        self.assertEqual(iso, "2026-10-26T02:00:00Z")
        self.assertGreater(ms, 0)
        self.assertEqual(compute_report.created_ms_and_iso(None), (0.0, ""))

    def test_aligned_pads_columns(self):
        self.assertEqual(compute_report.aligned([["a", "bb"], ["ccc", "d"]]), "a    bb\nccc  d")


class Stop(unittest.TestCase):
    def test_age_and_state(self):
        created = time.time() * 1000 - 5.5 * 60000
        api = fake_rest(lambda m, p, q, b: {"updates": [{"state": "RUNNING", "creation_time": created}]})
        state, age = stop_after.age_and_state(api, PIPE)
        self.assertEqual((state, age), ("RUNNING", 5))
        self.assertEqual(stop_after.age_and_state(fake_rest(lambda m, p, q, b: {"updates": []}), PIPE), ("", 0))

    def test_step_keeps_a_failed_lookup_pending(self):
        def boom(m, p, q, b):
            raise RuntimeError("network")
        self.assertEqual(stop_after.step(fake_rest(boom), PIPE, 30, "12:00"), (True, ""))

    def test_step_done_no_update_and_stop(self):
        def answers(created, state):
            def f(m, p, q, b):
                if m == "POST":
                    return {}
                return {"updates": [{"state": state, "creation_time": created}]}
            return f
        now_ms = time.time() * 1000
        self.assertEqual(stop_after.step(fake_rest(answers(now_ms, "COMPLETED")), PIPE, 30, "12:00"),
                         (False, f"12:00 {PIPE} COMPLETED after 0m"))
        self.assertEqual(stop_after.step(fake_rest(lambda m, p, q, b: {"updates": []}), PIPE, 30, "12:00"),
                         (False, f"12:00 {PIPE} no update"))
        old = now_ms - 40 * 60000
        self.assertEqual(stop_after.step(fake_rest(answers(old, "RUNNING")), PIPE, 30, "12:00"),
                         (False, f"12:00 {PIPE} STOPPED at 40m (was RUNNING)"))
        self.assertEqual(stop_after.step(fake_rest(answers(now_ms, "RUNNING")), PIPE, 30, "12:00"), (True, ""))


if __name__ == "__main__":
    unittest.main()
