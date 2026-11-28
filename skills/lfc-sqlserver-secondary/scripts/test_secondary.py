"""lfc-sqlserver-secondary offline: the reader's steps on a fake workspace (a Rest over a fake do) and a fake sqlcmd
(a function): the table name and the capture-instance waits, the connection switch (the password kept out of the
options it reads back), refresh refusals and bodies, the cluster log delivery, the driver log's reset line (gzipped),
the pipeline event waits, and the usage."""
from __future__ import annotations

import calendar
import contextlib
import gzip
import io
import sys
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import secondary as s
from lfc_rest import Rest


class Clock:
    """Time moves only when a wait sleeps."""

    def __init__(self):
        self.t, self.slept = 0.0, []

    def now(self):
        return self.t

    def sleep(self, secs):
        self.slept.append(secs)
        self.t += secs


class Api:
    """The workspace: routes by (method, path). A list answer is consumed in order (the last one repeats)."""

    def __init__(self, routes):
        self.routes, self.calls = dict(routes), []

    def do(self, method, path, query, body, **kw):
        self.calls.append((method, path, body))
        r = self.routes[(method, path)]
        if callable(r):
            return r(body)
        if isinstance(r, list):
            return r.pop(0) if len(r) > 1 else r[0]
        return r

    def rest(self):
        return Rest(self.do)

    def bodies(self, method, path):
        return [b for m, p, b in self.calls if (m, p) == (method, path)]


def state(*names):
    return [{"updates": [{"state": n}]} for n in names]


def reader(env, api=None, sql=None, clock=None):
    clock = clock or Clock()
    return s.Reader(env=env, api=api.rest() if api else None, sql_run=sql, sleep=clock.sleep, clock=clock.now), clock


BASE = {"CONNECTION": "conn", "DATABASE": "db", "PRIMARY": "h1,1433", "SECONDARY": "h2,1433", "SQL_USER": "u",
        "SQL_PASSWORD": "pw", "WAIT": "60"}


class Names(unittest.TestCase):
    def test_split_name(self):
        self.assertEqual(s.split_name("orders"), ("dbo", "orders"))
        self.assertEqual(s.split_name("sales.orders"), ("sales", "orders"))
        with self.assertRaises(SystemExit) as e:
            s.split_name("a.b;c")
        self.assertEqual(e.exception.code, 2)

    def test_first_row_and_cut(self):
        self.assertEqual(s.first_row("x\nNULL|c|1\nmore|1"), "NULL|c|1")
        self.assertEqual(s.first_row("no pipes"), "")
        self.assertEqual(s.field2("NULL|cap|1"), "cap")
        self.assertEqual(s.field2("plain"), "plain")


class Waits(unittest.TestCase):
    def test_wait_ci_returns_on_one_capture_instance(self):
        answers = iter(["NULL|old|2", "NULL|cap|1"])
        r, clock = reader(dict(BASE), sql=lambda node, sql: next(answers) + "\n")
        r.wait_ci("dbo.t")
        self.assertEqual(clock.slept, [10])

    def test_wait_ci_keeps_waiting_while_the_current_one_is_the_old_one(self):
        r, _ = reader(dict(BASE, WAIT="25"), sql=lambda node, sql: "NULL|cap|1\n")
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as e:
            r.wait_ci("dbo.t", "cap")   # the current one is still cap: the wait gives up
        self.assertEqual(e.exception.code, 1)

    def test_wait_ci_gives_up(self):
        r, clock = reader(dict(BASE, WAIT="25"), sql=lambda node, sql: "NULL|cap|1\n")
        with self.assertRaises(SystemExit) as e:
            r.wait_ci("dbo.t", "cap")
        self.assertEqual(e.exception.code, 1)
        self.assertEqual(sum(clock.slept), 30)

    def test_sessions_unreachable_and_reachable(self):
        def sql(node, text):
            if node == "primary":
                raise s.SqlFailed(1)
            return "2 gateway sessions|READ_ONLY\n"
        r, _ = reader(dict(BASE), sql=sql)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            r.sessions()
        self.assertEqual(buf.getvalue(), "primary: unreachable\nsecondary: 2 gateway sessions|READ_ONLY\n")

    def test_wait_event_matches_after_since(self):
        api = Api({("GET", "/api/2.0/pipelines/P1/events"): {"events": [
            {"timestamp": "2026-11-27T10:00:05.000Z", "message": "_dbo_t_snapshot_flow.x has COMPLETED"},
            {"timestamp": "2026-11-27T09:00:00.000Z", "message": "_dbo_t_snapshot_flow.y has COMPLETED"}]}})
        env = {**BASE, "PIPELINE_ID": "P1"}
        r, _ = reader(env, api=api)
        self.assertTrue(r.wait_event("has COMPLETED", "2026-11-27T10:00:00"))
        self.assertFalse(r.wait_event("never", "2026-11-27T10:00:00", quiet=True))


class Switching(unittest.TestCase):
    def test_switch_keeps_options_and_restarts_the_gateway(self):
        conn = {"name": "conn", "options": {"host": "old", "port": "1", "password": "OLD",
                                            "trustServerCertificate": "true"}}
        api = Api({
            ("GET", "/api/2.1/unity-catalog/connections/conn"): conn,
            ("PATCH", "/api/2.1/unity-catalog/connections/conn"): {},
            ("GET", "/api/2.0/pipelines/G1/updates"): state("COMPLETED", "COMPLETED", "RUNNING"),
            ("POST", "/api/2.0/pipelines/G1/stop"): {},
            ("POST", "/api/2.0/pipelines/G1/updates"): {"update_id": "u"},
        })
        env = {**BASE, "GATEWAY_ID": "G1", "RUN": "", "PIPELINE_ID": ""}
        r, _ = reader(env, api=api)
        r.switch("secondary")
        patch = api.bodies("PATCH", "/api/2.1/unity-catalog/connections/conn")[0]
        self.assertEqual(patch, {"options": {"trustServerCertificate": "true", "host": "h2", "port": "1433",
                                             "user": "u", "password": "pw"}})
        self.assertEqual(len(api.bodies("POST", "/api/2.0/pipelines/G1/updates")), 1)

    def test_switch_refuses_an_unknown_node(self):
        r, _ = reader(dict(BASE, GATEWAY_ID="G1"), api=Api({}))
        with self.assertRaises(SystemExit) as e:
            r.switch("tertiary")
        self.assertEqual(e.exception.code, 2)


class Refresh(unittest.TestCase):
    def test_continuous_pipeline_refuses_a_refresh_selection(self):
        api = Api({("GET", "/api/2.0/pipelines/P1"): {"spec": {"continuous": True}}})
        r, _ = reader(dict(BASE, PIPELINE_ID="P1"), api=api)
        with self.assertRaises(SystemExit) as e:
            r.full_refresh("dst")
        self.assertEqual(e.exception.code, 2)

    def test_full_refresh_posts_the_selection(self):
        api = Api({
            ("GET", "/api/2.0/pipelines/P1"): {"spec": {"continuous": False}},
            ("GET", "/api/2.0/pipelines/P1/updates"): state("COMPLETED"),
            ("POST", "/api/2.0/pipelines/P1/updates"): {"update_id": "u9"},
        })
        r, _ = reader(dict(BASE, PIPELINE_ID="P1"), api=api)
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            r.full_refresh("dst", "two")
        self.assertEqual(api.bodies("POST", "/api/2.0/pipelines/P1/updates"),
                         [{"full_refresh_selection": ["dst", "two"]}])
        self.assertIn("ingestion update u9: full refresh of dst two", buf.getvalue())


class LogDelivery(unittest.TestCase):
    def test_sets_the_log_destination_on_the_default_cluster_only(self):
        spec = {"name": "gw", "clusters": [{"label": "default", "num_workers": 1},
                                           {"label": "maintenance", "num_workers": 0}]}
        api = Api({("GET", "/api/2.0/pipelines/G1"): {"spec": spec},
                   ("PUT", "/api/2.0/pipelines/G1"): {}})
        r, _ = reader(dict(BASE, GATEWAY_ID="G1", LOG_VOLUME="/Volumes/c/s/v"), api=api)
        with contextlib.redirect_stderr(io.StringIO()):
            r.log_delivery()
        put = api.bodies("PUT", "/api/2.0/pipelines/G1")[0]
        self.assertEqual(put["clusters"][0]["cluster_log_conf"], {"volumes": {"destination": "/Volumes/c/s/v"}})
        self.assertNotIn("cluster_log_conf", put["clusters"][1])
        self.assertEqual(put["name"], "gw")

    def test_integrated_refuses(self):
        r, _ = reader(dict(BASE, PIPELINE_ID="P1", LOG_VOLUME="/Volumes/c/s/v"), api=Api({}))
        with self.assertRaises(SystemExit) as e:
            r.log_delivery()
        self.assertEqual(e.exception.code, 2)


class DriverLog(unittest.TestCase):
    def test_finds_the_reset_line_in_a_gzipped_log(self):
        text = ("26/11/27 09:30:00 INFO old line\n"
                "26/11/27 10:05:00 INFO Reinitialization completed successfully for abc\n")
        api = Api({
            ("GET", "/api/2.0/pipelines/G1"): {"cluster_id": "c1"},
            ("GET", "/api/2.0/fs/directories/Volumes/c/s/v/c1/driver"): {"contents": [
                {"name": "log4j-active.log.gz"}, {"name": "notes.txt"}]},
            ("GET", "/api/2.0/fs/files/Volumes/c/s/v/c1/driver/log4j-active.log.gz"): gzip.compress(text.encode()),
        })
        since = calendar.timegm((2026, 11, 27, 9, 0, 0))   # the 10:05 line is after it
        r, clock = reader(dict(BASE, GATEWAY_ID="G1", LOG_VOLUME="/Volumes/c/s/v"), api=api)
        with contextlib.redirect_stderr(io.StringIO()) as err:
            r.wait_reset(since)
        self.assertEqual(clock.slept, [])
        self.assertIn("Reinitialization completed successfully for abc", err.getvalue())

    def test_gives_up_without_a_line_after_since(self):
        api = Api({
            ("GET", "/api/2.0/pipelines/G1"): {"cluster_id": "c1"},
            ("GET", "/api/2.0/fs/directories/Volumes/c/s/v/c1/driver"): {"contents": [{"name": "log4j-a.log"}]},
            ("GET", "/api/2.0/fs/files/Volumes/c/s/v/c1/driver/log4j-a.log"): b"26/11/27 01:00:00 Reinitialization "
                                                                              b"completed successfully for old\n",
        })
        r, clock = reader(dict(BASE, GATEWAY_ID="G1", LOG_VOLUME="/Volumes/c/s/v", WAIT="60"), api=api)
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as e:
            r.wait_reset(calendar.timegm((2026, 11, 27, 2, 0, 0)))   # the 01:00 line is before it
        self.assertEqual(e.exception.code, 1)
        self.assertTrue(clock.slept)


class Usage(unittest.TestCase):
    def test_no_arguments_prints_the_usage_and_unknown_is_2(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            self.assertEqual(s.main([]), 0)
            self.assertEqual(s.main(["nope"]), 2)
        self.assertIn("secondary.py <command> [args]", buf.getvalue())

    def test_missing_setting_is_exit_2(self):
        with mock.patch.dict("os.environ", {}, clear=True), contextlib.redirect_stderr(io.StringIO()) as err:
            self.assertEqual(s.main(["ci", "dbo.t"]), 2)
        self.assertIn("set DATABASE", err.getvalue())


if __name__ == "__main__":
    unittest.main()
