"""lfc_schema_change offline, on a fake workspace and source: pre-flight, the pipeline specs, the update chain (a retry the
pipeline starts itself, a refused start, the deadline, integrated CDC's early stop), errors per table, expect after an
early stop, the kept record (lib.sh's keys, so kept-sweep.sh deletes it) and its lock, deletion; SQL Server set-up's
retries and drop; the source command from a contract."""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))
for _dep in ("lfc-rest", "lfc-change-source", "lfc-source-contract", "lfc-pipeline-watch"):   # as the package does
    sys.path.insert(0, str(SCRIPTS.parents[1] / _dep / "scripts"))
from lfc_rest import Rest
from lfc_schema_change import kept, scenario
from lfc_source_contract import sqlserver, srcsql

ENV = {"PROFILE": "p", "CATALOG": "main", "CONNECTION": "me-ss1", "CONN_USER": "lfc_user", "SRC_SQL": "fake",
       "PREFIX": "me", "WAREHOUSE": "w1", "DIAGRAM": "0", "RECORD": "0"}


class Clock:
    def __init__(self):
        self.t = 1790000000.0

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


class Api:
    """The REST calls the harness makes. updates: {update id: [states, polled in turn]}; chain: the update ids the
    pipeline starts after each (newest_update)."""

    def __init__(self, updates=None, follow=None, events=None, refuse_start=False):
        self.calls, self.updates, self.follow = [], dict(updates or {}), dict(follow or {})
        self.ev, self.refuse, self.host, self.started = list(events or []), refuse_start, "https://ws", []
        self.current = ""

    def call(self, m, path, body=None, **q):
        self.calls.append((m, path, body, q))
        if path.endswith("/Me"):
            return {"userName": "me@x.com"}
        if "/catalogs/" in path:
            return {"storage_root": "abfss://c@a/x"}
        if "external-locations" in path:
            raise RuntimeError("RESOURCE_DOES_NOT_EXIST")
        if "/connections/" in path:
            return {"connection_type": "SQLSERVER"}
        if m == "POST" and path == "/api/2.0/pipelines":
            return {"pipeline_id": "gw1" if "gateway_definition" in body else "ap1"}
        if m == "POST" and path.endswith("/updates"):
            if self.refuse:
                self.refuse -= 1 if self.refuse is not True else 0
                raise RuntimeError("An update is already running")
            self.started.append(body)
            self.current = next(iter(self.updates))
            return {"update_id": self.current}
        if m == "GET" and "/updates/" in path:
            u = path.rsplit("/", 1)[1]
            states = self.updates[u]
            st = states.pop(0) if len(states) > 1 else states[0]
            return {"update": {"state": st, "cause": "API_CALL" if u == "u1" else "RETRY_ON_FAILURE"}}
        if m == "GET" and path.endswith("/updates"):
            nxt = self.follow.get(self.current, self.current) or self.current
            self.current = nxt
            return {"updates": [{"update_id": nxt}]}
        if path.endswith("/events"):
            return {"events": self.ev}
        return {}

    def get(self, p, **q):
        return self.call("GET", p, **q)

    def post(self, p, b=None, **q):
        return self.call("POST", p, b, **q)

    def put(self, p, b=None, **q):
        return self.call("PUT", p, b, **q)

    def patch(self, p, b=None, **q):
        return self.call("PATCH", p, b, **q)

    def delete(self, p, **q):
        return self.call("DELETE", p, **q)

    def pages(self, path, key, limit=None, **q):
        yield from (self.call("GET", path, **q).get(key) or [])[:limit]


class Rows:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return self.rows


class Wh:
    def __init__(self, answers=None):
        self.answers, self.seen = list(answers or []), []

    def sql(self, q, raw=False):
        self.seen.append(q)
        a = self.answers.pop(0) if self.answers else [("1",)]
        if isinstance(a, Exception):
            raise a
        return Rows(a)


class Src:
    """A source command: every SQL logged; answers from a list (default "0")."""

    def __init__(self, answers=None):
        self.cmd, self.env, self.sql, self.answers = "fake", {}, [], list(answers or [])

    def __call__(self, sql, database=""):
        self.sql.append((database, sql))
        return self.answers.pop(0) if self.answers else "0"


def rest(fake):
    """A real Rest over the fake's calls (lfc_watch pages events through it)."""
    return Rest(lambda m, p, q, b, **kw: fake.call(m, p, b, **(q or {})), "fake", fake.host)


def run_of(api, env=None, wh=None, src=None, clock=None):
    clock = clock or Clock()
    return scenario.Run("addcol", env={**ENV, **(env or {})}, api=rest(api), warehouse=wh or Wh(), src=src or Src(),
                        clock=clock, sleep=clock.sleep, log=lambda *a: None)


def ev(update, flow, error_class=None, status=None):
    e = {"origin": {"update_id": update, "flow_name": flow}, "event_type": "flow_progress", "timestamp": 1790000000000}
    if error_class:
        e["error"] = {"exceptions": [{"error_class": error_class, "message": f"{error_class} happened"}]}
    if status:
        e["details"] = {"flow_progress": {"status": status}}
    return e


class Preflight(unittest.TestCase):
    def test_names_and_refusals(self):
        r = run_of(Api())
        self.assertEqual(r.db.name, "me_addcol_1790000000")
        self.assertEqual(r.schema, "me_ss1_1790000000")   # the prefix not twice, as lib.sh
        self.assertEqual(r.staging, "me_ss1_1790000000_stg")
        with self.assertRaises(scenario.Setup):
            run_of(Api(), env={"MODE": "both"})
        with self.assertRaises(scenario.Setup):
            run_of(Api(), env={"ENGINE": "postgres"})
        self.assertEqual(run_of(Api(), env={"MODE": "integrated"}).wait, 0)


class Pipelines(unittest.TestCase):
    def test_standard_makes_gateway_then_ingestion(self):
        api = Api()
        r = run_of(api)
        r.pipelines_create([r.obj("t1"), r.obj("t2", scd="SCD_TYPE_2", auto_full_refresh=True)])
        made = [b for m, p, b, _ in api.calls if m == "POST" and p == "/api/2.0/pipelines"]
        self.assertEqual(made[0]["gateway_definition"]["gateway_storage_schema"], r.staging)
        self.assertEqual(made[1]["ingestion_definition"]["ingestion_gateway_id"], "gw1")
        tc = made[1]["ingestion_definition"]["objects"][1]["table"]["table_configuration"]
        self.assertEqual(tc, {"scd_type": "SCD_TYPE_2", "auto_full_refresh_policy": {"enabled": True,
                                                                                    "min_interval_hours": 1}})
        self.assertTrue(any(p.startswith("/api/2.0/permissions/pipelines/") for _, p, _, _ in api.calls))

    def test_integrated_has_no_gateway(self):
        api = Api()
        r = run_of(api, env={"MODE": "integrated"})
        r.pipelines_create([r.obj("t1")])
        (spec,) = [b for m, p, b, _ in api.calls if m == "POST" and p == "/api/2.0/pipelines"]
        self.assertEqual(spec["ingestion_definition"]["connector_type"], "CDC")
        self.assertEqual(r.gw, "")


class Updates(unittest.TestCase):
    def test_follows_a_retry_the_pipeline_started(self):
        evs = [ev("u1", "main_s_t1_cdc_flow", "CDC_APPLIER_SCHEMA_CHANGED_DURING_STREAMING"),
               ev("u2", "main_s_t2_cdc_flow", "CDC_INCOMPATIBLE_SCHEMA_CHANGES.PRIMARY_KEY_CHANGE"), ev("u0", "old_t3_flow", "OLD")]
        api = Api(updates={"u1": ["RUNNING", "FAILED"], "u2": ["COMPLETED"]}, follow={"u1": "u2", "u2": "u2"}, events=evs)
        r = run_of(api)
        r.ap = "ap1"
        r.update(["t1"])
        self.assertEqual(api.started, [{"full_refresh_selection": ["t1"]}])
        self.assertEqual(r.state, "COMPLETED")
        self.assertEqual(r.chain, "API_CALL FAILED -> RETRY_ON_FAILURE COMPLETED")
        self.assertEqual(r.errs, ["CDC_APPLIER_SCHEMA_CHANGED_DURING_STREAMING", "CDC_INCOMPATIBLE_SCHEMA_CHANGES.PRIMARY_KEY_CHANGE"])   # not u0's
        self.assertEqual(r.errs_for("t2"), ["CDC_INCOMPATIBLE_SCHEMA_CHANGES.PRIMARY_KEY_CHANGE"])
        self.assertTrue(r.needs_refresh("t2"))
        self.assertTrue(r.has("PRIMARY_KEY_CHANGE") and r.has("PRIMARY_KEY_CHANGE", "t2") and not r.has("X", "t2"))
        self.assertFalse(r.needs_refresh("t1"))

    def test_refused_start_follows_the_running_one_then_starts_the_refresh(self):
        api = Api(updates={"u1": ["RUNNING", "COMPLETED"]}, refuse_start=1)
        api.current = "u1"
        r = run_of(api)
        r.ap = "ap1"
        self.assertEqual(r.update(["t1"]), "COMPLETED")
        self.assertEqual(api.started, [{"full_refresh_selection": ["t1"]}])   # the refresh ran after the one followed

    def test_deadline_stops_and_reports(self):
        clock = Clock()
        api = Api(updates={"u1": ["RUNNING"]})
        r = run_of(api, env={"LFC_DEADLINE_MIN": "1"}, clock=clock)
        r.ap = "ap1"
        r.update()
        self.assertIn("timed out: deadline", r.chain)
        self.assertTrue(any(p.endswith("/stop") for _, p, _, _ in api.calls))

    def test_integrated_early_stop_after_three_stable_polls_and_3_minutes(self):
        api = Api(updates={"u1": ["RUNNING"]})
        wh = Wh([[("10", "10", "5")]] * 20)
        r = run_of(api, env={"MODE": "integrated"}, wh=wh)
        r.ap = "ap1"
        r.update(fp_tables=["t1"])
        self.assertTrue(r.early)
        self.assertTrue(r.ok_state())
        self.assertIn("stopped early after", r.chain)

    def test_expect_after_an_early_stop_keeps_the_steps_errors_and_state(self):
        api = Api(updates={"u1": ["COMPLETED"]})
        r = run_of(api, wh=Wh([[("1",)], [("2",)]]))
        r.ap, r.early, r.last_events, r.errs, r.state = "ap1", True, ["kept"], ["X.PRIMARY_KEY_CHANGE"], "RUNNING"
        self.assertEqual(r.expect("2", "SELECT x"), "2")
        self.assertEqual((r.last_events, r.errs, r.state, r.early), (["kept"], ["X.PRIMARY_KEY_CHANGE"], "RUNNING", True))

    def test_refused_start_with_nothing_running_is_the_result(self):
        api = Api(updates={"u1": ["COMPLETED"]}, refuse_start=True)
        api.current = "u1"                       # the newest is finished: not this step's update
        r = run_of(api)
        r.ap = "ap1"
        self.assertEqual(r.update(), "REFUSED")
        self.assertEqual(r.errs, ["START_UPDATE_REFUSED"])


class Exits(unittest.TestCase):
    def test_a_set_up_error_before_any_claim_exits_2(self):
        class Bad:
            SHORT = "bad"

            @staticmethod
            def run(r):
                raise RuntimeError("could not set up cdc on lfcddemo: t1")
        with mock.patch.object(scenario, "Run", lambda short: run_of(Api(), env={"KEEP": "1"})), \
                mock.patch("sys.stderr"):
            self.assertEqual(scenario.main(Bad), 2)

    def test_a_grant_failure_still_returns_the_pipeline(self):
        from lfc_change_source import pipeline
        api = Api()
        orig = api.call

        def call(m, p, b=None, **q):
            if p.startswith("/api/2.0/permissions/"):
                raise RuntimeError("PERMISSION_DENIED")
            return orig(m, p, b, **q)
        api.call = call
        with mock.patch("sys.stderr"):
            self.assertEqual(pipeline.create(rest(api), {"name": "x", "ingestion_definition": {}}), "ap1")


class Kept(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.path = os.path.join(self.d, "kept.jsonl")

    def test_keep_stops_and_records_lib_sh_keys(self):
        api = Api()
        r = run_of(api)
        r.ap, r.gw, r.extra_pipelines, r.extra_schemas = "ap1", "gw1", ["dp1"], ["main.x"]
        rec = kept.keep(r, 14, self.path, log=lambda *a: None)
        stops = [p for m, p, _, _ in api.calls if p.endswith("/stop")]
        self.assertEqual(len(stops), 3)
        line = json.loads(Path(self.path).read_text())
        self.assertEqual(set(line), {"id", "kept", "delete_after", "host", "profile", "catalog", "schema", "staging",
                                     "pipelines", "stopped", "engine", "db", "src_db", "slot", "slot_dropped", "connection",
                                     "conn_user", "src_sql", "source_json", "keep_source", "extra_pipelines",
                                     "extra_schemas", "extra_sql"})
        self.assertEqual(line["pipelines"], ["ap1", "gw1"])
        self.assertEqual(line["db"], "me_addcol_1790000000")
        self.assertEqual(rec["src_sql"], "fake")
        self.assertFalse(os.path.exists(self.path + ".lock"))

    def test_a_failed_stop_is_recorded(self):
        api = Api()
        orig = api.call

        def call(m, p, b=None, **q):
            if p.endswith("/stop"):
                raise RuntimeError("invalid access token")
            return orig(m, p, b, **q)
        api.call = call
        r = run_of(api)
        r.ap = "ap1"
        self.assertFalse(kept.keep(r, 14, self.path, log=lambda *a: None)["stopped"])

    def test_lock_taken_over_when_stale(self):
        os.mkdir(self.path + ".lock")
        waits = []
        with kept.lock(self.path, take_over_s=3, sleep=waits.append):
            pass
        self.assertGreaterEqual(len(waits), 3)

    def test_delete_removes_pipelines_schemas_and_owned_database(self):
        api = Api()
        src = Src(["", "", "0"])
        r = run_of(api, env={"KEEP_DAYS": "0", "LFC_DELETE_WAIT": "0"}, src=src)
        r.ap, r.gw = "ap1", "gw1"
        with mock.patch.object(kept.time, "sleep"):
            r.cleanup()
        dels = [p for m, p, _, _ in api.calls if m == "DELETE"]
        self.assertIn("/api/2.0/pipelines/ap1", dels)
        self.assertIn(f"/api/2.1/unity-catalog/schemas/main.{r.schema}", dels)
        self.assertTrue(any("DROP DATABASE" in s for _, s in src.sql))


class SqlServer(unittest.TestCase):
    def test_track_retries_the_untracked_then_fails(self):
        src = Src(["", "t2", "", "t2", "", "t2"])
        db = sqlserver.Database(src, "d_1", "lfc_user", log=lambda *a: None, sleep=lambda s: None)
        with self.assertRaises(RuntimeError):
            db.track("cdc", ["t1", "t2"])
        execs = [s for _, s in src.sql if s.startswith("EXEC dbo.lakeflowSetupChangeDataCapture")]
        self.assertIn("lfcddemo.t1,lfcddemo.t2", execs[0])
        self.assertIn("@Tables = 'lfcddemo.t2'", execs[1])

    def test_track_ct_asks_for_ddl_objects_and_fixes_permissions(self):
        src = Src(["", "NULL", ""])
        sqlserver.Database(src, "d_1", "lfc_user", log=lambda *a: None).track("ct", ["t1"])
        self.assertIn("@CreateDdlSupportingObjects = 1", src.sql[0][1])
        self.assertIn("lakeflowFixPermissions", src.sql[-1][1])

    def test_names_checked(self):
        with self.assertRaises(ValueError):
            sqlserver.Database(Src(), "d; DROP", "u")


class Contract(unittest.TestCase):
    def test_exports_parsed(self):
        self.assertEqual(srcsql.exports("export SRC_SQL='env A=1 x.sh'\nexport CONN_USER='it'\\''s'\n"),
                         {"SRC_SQL": "env A=1 x.sh", "CONN_USER": "it's"})


if __name__ == "__main__":
    unittest.main()
