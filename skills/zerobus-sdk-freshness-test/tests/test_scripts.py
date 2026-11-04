"""Offline checks of producer.py and report.py with fake SDK modules: what the run file and the report count."""
import contextlib
import importlib.util
import io
import json
import os
import shutil
import sys
import tempfile
import types
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.join(HERE, "..", "scripts")


def put(path, obj):
    with open(path, "w") as f:
        json.dump(obj, f) if not isinstance(obj, str) else f.write(obj)


def get(path):
    with open(path) as f:
        return json.load(f)


def tmpdir(test):
    d = tempfile.mkdtemp()
    test.addCleanup(shutil.rmtree, d)
    return d


def load(name, modules):
    """Import scripts/<name>.py with the given fake modules in place of the SDKs."""
    with mock.patch.dict(sys.modules, modules):
        spec = importlib.util.spec_from_file_location(name, os.path.join(SCRIPTS, name + ".py"))
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
    return m


def run_main(m, argv, modules):
    """main() with argv; returns (exit code, stdout)."""
    out = io.StringIO()
    with mock.patch.dict(sys.modules, modules), mock.patch.object(sys, "argv", argv), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
        try:
            m.main()
            code = 0
        except SystemExit as e:
            code = e.code if isinstance(e.code, int) else 1
    return code, out.getvalue()


# --- producer.py: a fake Zerobus SDK (unused with --no-zerobus) and a fake confluent_kafka

def zerobus_modules():
    shared = types.ModuleType("zerobus.sdk.shared")
    shared.AckCallback = type("AckCallback", (), {"__init__": lambda self: None})
    shared.NonRetriableException = type("NonRetriableException", (Exception,), {})
    shared.RecordType = types.SimpleNamespace(JSON="JSON")
    shared.StreamConfigurationOptions = lambda **k: k
    shared.TableProperties = lambda t: t
    sync = types.ModuleType("zerobus.sdk.sync")
    sync.ZerobusSdk = FakeZerobus
    return {"zerobus": types.ModuleType("zerobus"), "zerobus.sdk": types.ModuleType("zerobus.sdk"),
            "zerobus.sdk.shared": shared, "zerobus.sdk.sync": sync}


class FakeStream:
    flush_fails = False

    def ingest_records_nowait(self, records):
        pass

    def get_unacked_records(self):
        return []

    def flush(self):
        if FakeStream.flush_fails:
            raise RuntimeError("flush: records not acknowledged")

    def close(self):
        pass


class FakeZerobus:
    def __init__(self, endpoint, url):
        pass

    def create_stream(self, *a):
        return FakeStream()


class FakeKafka:
    produced, undelivered, fail_open = [], 0, False

    def __init__(self, conf):
        if FakeKafka.fail_open:
            raise RuntimeError("broker unreachable")

    def produce(self, topic, key, value, on_delivery):
        FakeKafka.produced.append(json.loads(value)["event_id"])

    def poll(self, t):
        return 0

    def flush(self, t):
        return FakeKafka.undelivered


class Producer(unittest.TestCase):
    def setUp(self):
        FakeKafka.produced, FakeKafka.undelivered, FakeKafka.fail_open = [], 0, False
        kmod = types.ModuleType("confluent_kafka"); kmod.Producer = FakeKafka
        self.mods = {**zerobus_modules(), "confluent_kafka": kmod}
        self.m = load("producer", self.mods)
        self.d = tmpdir(self)
        pw = os.path.join(self.d, "pw"); put(pw, "x")
        self.out = os.path.join(self.d, "run.json")
        self.argv = ["producer.py", "--no-zerobus", "--kafka-bootstrap", "b:9093", "--kafka-topic", "t", "--kafka-user", "u",
                     "--kafka-password-file", pw, "--assets", "4", "--tags", "3", "--hz", "5", "--minutes", "0.02",
                     "--streams", "2", "--dup-rate", "0.3", "--out", self.out]

    def run_json(self):
        return get(self.out)

    def test_dups_sent_counts_the_repeated_ids(self):
        code, _ = run_main(self.m, self.argv, self.mods)
        r = self.run_json()
        ids = FakeKafka.produced
        self.assertEqual(code, 0)
        self.assertEqual(r["sent"], len(ids))
        self.assertEqual(r["kafka_sent"], len(ids))
        self.assertGreater(r["dups_sent"], 0)
        self.assertEqual(r["dups_sent"], len(ids) - len(set(ids)))
        self.assertEqual(r["worker_failures"], 0)

    def test_undelivered_kafka_messages_fail_the_run(self):
        FakeKafka.undelivered = 7
        code, _ = run_main(self.m, self.argv, self.mods)
        self.assertEqual(code, 1)
        self.assertEqual(self.run_json()["kafka_errors"], 14)   # 7 per stream, 2 streams

    def test_a_failed_final_flush_fails_the_run(self):
        FakeStream.flush_fails = True
        self.addCleanup(setattr, FakeStream, "flush_fails", False)
        secret = os.path.join(self.d, "secret"); put(secret, "s")
        argv = ["producer.py", "--endpoint", "e", "--workspace-url", "https://w", "--table", "c.s.t", "--client-id", "id",
                "--secret-file", secret, "--assets", "2", "--tags", "2", "--minutes", "0.01", "--streams", "1", "--out", self.out]
        code, _ = run_main(self.m, argv, self.mods)
        self.assertEqual(code, 1)
        self.assertEqual(self.run_json()["worker_failures"], 1)

    def test_a_stream_thread_that_dies_fails_the_run(self):
        FakeKafka.fail_open = True
        code, _ = run_main(self.m, self.argv, self.mods)
        self.assertEqual(code, 1)
        self.assertEqual(self.run_json()["worker_failures"], 2)


# --- report.py and freshness_probe.py: a fake databricks.sdk whose SQL answers come from the statement text

def databricks_modules(answers, state="SUCCEEDED", cancelled=None):
    import enum
    sqlmod = types.ModuleType("databricks.sdk.service.sql")
    sqlmod.StatementState = enum.Enum("StatementState", "PENDING RUNNING SUCCEEDED FAILED")
    sqlmod.Disposition = types.SimpleNamespace(INLINE="INLINE")
    sqlmod.Format = types.SimpleNamespace(JSON_ARRAY="JSON_ARRAY")

    class Exec:
        def execute_statement(self, statement, **k):
            rows = next((v for key, v in answers if key in statement), [])
            return types.SimpleNamespace(status=types.SimpleNamespace(state=sqlmod.StatementState[state], error=None),
                                         result=types.SimpleNamespace(data_array=rows), statement_id="s")

        def get_statement(self, sid):
            return self.execute_statement("")

        def cancel_execution(self, sid):
            cancelled.append(sid)

    sdk = types.ModuleType("databricks.sdk")
    sdk.WorkspaceClient = lambda profile=None: types.SimpleNamespace(statement_execution=Exec())
    return {"databricks": types.ModuleType("databricks"), "databricks.sdk": sdk,
            "databricks.sdk.service": types.ModuleType("databricks.sdk.service"), "databricks.sdk.service.sql": sqlmod}


class Report(unittest.TestCase):
    def test_events_commits_and_phases(self):
        # 60 s run, burst 1020-1030: 10 rows/s, 100 rows/s in the burst -> 1,500 rows landed
        per_s = [[str(s), str(100 if 1020 <= s < 1030 else 10)] for s in range(1000, 1060)]
        hist = [["1", "1000", "STREAMING UPDATE", None], ["2", "1004", "STREAMING UPDATE", None],
                ["3", "1005", "OPTIMIZE", None], ["4", "1010", "STREAMING UPDATE", None]]
        answers = [("count(DISTINCT", [["1494"]]), ("DESCRIBE HISTORY", hist), ("date_trunc", per_s)]
        mods = databricks_modules(answers)
        m = load("report", mods)
        d = tmpdir(self)
        run = os.path.join(d, "run.json"); probe = os.path.join(d, "probe.json")
        put(run, {"start": 1000, "end": 1060, "burst": [1020, 1030], "sent": 1498, "dups_sent": 3})
        samples = [{"t": t, "path": "p", "ms": 1000 if t < 1020 else 5000 if t <= 1031 else 2000} for t in range(1000, 1061, 2)]
        put(probe, {"paths": ["p"], "samples": samples})
        code, out = run_main(m, ["report.py", "prof", "--warehouse", "w", "--table", "c.s.t", "--run", run,
                                 "--probe", probe, "--id-column", "value:event_id::string", "--json"], mods)
        self.assertEqual(code, 0)
        r = json.loads(out)
        self.assertEqual(r["landed"], 1500)
        self.assertEqual(r["events"], {"sent": 1495, "landed": 1494, "lost": 1, "extra_copies": 3})
        self.assertEqual(r["rate_per_s"], {"median": 10, "peak": 100, "burst_mean": 100})
        self.assertEqual(r["commits"]["write_commits"], 3)
        self.assertEqual(r["commits"]["interval_s"], {"min": 4, "p50": 5.0, "max": 6})
        f = r["freshness"]["p"]
        self.assertEqual((f["before"]["n"], f["before"]["p50_ms"]), (2, 1000))   # warm-up (15 s) left out
        self.assertEqual((f["during"]["n"], f["during"]["p50_ms"]), (6, 5000))
        self.assertEqual((f["after"]["n"], f["after"]["p50_ms"]), (15, 2000))

    def test_no_dups_sent_no_events_block(self):
        mods = databricks_modules([("DESCRIBE HISTORY", []), ("date_trunc", [["1000", "5"]])])
        m = load("report", mods)
        d = tmpdir(self); run = os.path.join(d, "run.json")
        put(run, {"start": 1000, "end": 1001, "burst": None, "sent": 5})
        code, out = run_main(m, ["report.py", "prof", "--warehouse", "w", "--table", "c.s.t", "--run", run, "--json"], mods)
        self.assertEqual(json.loads(out)["events"], None)


class Probe(unittest.TestCase):
    def test_a_stuck_statement_is_cancelled_and_the_file_written(self):
        cancelled = []
        mods = databricks_modules([], state="RUNNING", cancelled=cancelled)
        m = load("freshness_probe", mods)
        out = os.path.join(tmpdir(self), "probe.json")
        code, _ = run_main(m, ["freshness_probe.py", "prof", "--path", "p|w|c.s.t", "--seconds", "1", "--out", out], mods)
        self.assertEqual(code, 0)
        samples = get(out)["samples"]
        self.assertTrue(samples)
        self.assertTrue(all(x["ms"] is None for x in samples))
        self.assertIn("s", cancelled)


if __name__ == "__main__":
    unittest.main()
