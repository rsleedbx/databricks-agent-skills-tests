"""lfc_watch offline: a fake client answers updates, events and the staging listing like the REST API and Files API."""
import io
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
from lfc_watch import core as W


def ev(ts, flow=None, status=None, level="INFO", msg="", exc=None):
    e = {"timestamp": ts, "level": level, "message": msg, "origin": {"flow_name": flow} if flow else {},
         "event_type": "flow_progress" if status else "x"}
    if status:
        e["details"] = {"flow_progress": {"status": status}}
    if exc:
        e["error"] = {"exceptions": [{"message": exc}]}
    return e


def Fake(rounds, files=None):
    """A Rest whose answers are the REST API's: updates, events (one round a check), the Files API listing."""
    i = [0]

    def do(m, path, q, body):
        upd, evs = rounds[min(i[0], len(rounds) - 1)]
        if path.endswith("/updates"):
            return {"updates": [{"state": upd}]}
        if path.endswith("/events"):
            i[0] += 1
            return {"events": evs}
        if path.startswith("/api/2.0/fs/directories"):
            return {"contents": [{"path": p, "is_directory": False, "file_size": n} for p, n in files or []]}
        return {"spec": {}}   # a pipeline that isn't a gateway
    return W.Rest(do)


def test_table_of_flow_names():
    assert W.table_of("lfcddemo.lfcddemo.intpk_cdc_flow") == ("intpk", "cdc")
    assert W.table_of("extractor_lfcddemo-azurevm-sqlserver_lfcddemo_lfcddemo_dtix_snapshot_flow") == ("dtix", "snapshot")
    assert W.table_of("extractor_x_db_s_order_lines_snapshot_flow", ["lines", "order_lines"]) == ("order_lines", "snapshot")
    assert W.table_of("something_else") is None


def test_flows_newest_state_and_errors():
    evs = [ev("2026-11-26T00:00:03Z", "c.s.intpk_cdc_flow", "RUNNING"),
           ev("2026-11-26T00:00:02Z", "c.s.intpk_cdc_flow", "STARTING"),
           ev("2026-11-26T00:00:01Z", level="ERROR", msg="update failed", exc="NO_CONNECTION_PARAMETER source_catalog")]
    state, errs = W.flows(evs)
    assert state == {"intpk": {"cdc": "RUNNING"}}
    assert errs == [(None, "NO_CONNECTION_PARAMETER source_catalog")]


def test_ready_waiting_failed():
    s = W.Status("RUNNING", {"a": {"snapshot": "COMPLETED", "cdc": "RUNNING"}, "b": {"cdc": "RUNNING"}}, wanted=("a", "b"))
    assert s.waiting() == ["b"] and not s.ready()
    s.tables["b"]["snapshot"] = "COMPLETED"
    assert s.ready()
    s.errors = [("b", "publication")]
    assert not s.ready() and s.failed() == {"b": "publication"} and s.own_error() is None


def test_watch_until_ready_one_line_a_check():
    r1 = ("RUNNING", [ev("2026-11-26T00:00:01Z", "c.s.a_cdc_flow", "RUNNING")])
    r2 = ("RUNNING", [ev("2026-11-26T00:00:02Z", "extractor_c_s_a_snapshot_flow", "COMPLETED"), *r1[1]])
    out, slept = io.StringIO(), []
    s = W.watch(Fake([r1, r2]), "p", tables=["a"], every=60, out=out, sleep=slept.append)
    assert s.ready() and slept == [60] and len(out.getvalue().splitlines()) == 2


def test_watch_fails_fast_on_the_pipelines_own_error():
    r = ("RUNNING", [ev("2026-11-26T00:00:01Z", level="ERROR", exc="User does not have CREATE permission")])
    with pytest.raises(RuntimeError, match="CREATE permission"):
        W.watch(Fake([r]), "p", tables=["a"], out=io.StringIO(), sleep=lambda s: None)


def test_watch_stalls_when_nothing_changes():
    t = [0.0]
    r = ("RUNNING", [ev("2026-11-26T00:00:01Z", "c.s.a_cdc_flow", "RUNNING")])
    with pytest.raises(TimeoutError, match="nothing changed"):
        W.watch(Fake([r]), "p", tables=["a"], every=60, stall_s=120, out=io.StringIO(),
                sleep=lambda s: t.__setitem__(0, t[0] + s), clock=lambda: t[0])


def test_staging_counts_and_growth_is_progress():
    f = Fake([("RUNNING", [])], files=[("/v/x/1", 10), ("/v/x/2", 5)])
    assert W.staging(f, "/v/x") == {"files": 2, "bytes": 15}


def test_staging_listed_only_while_a_table_waits():
    calls = []
    ready = [W.ms("2026-11-26T00:00:02Z")]
    evs = [ev("2026-11-26T00:00:02Z", "extractor_c_s_a_snapshot_flow", "COMPLETED"),
           ev("2026-11-26T00:00:01Z", "c.s.a_cdc_flow", "RUNNING")]

    def do(m, path, q, body):
        calls.append(path)
        if path.endswith("/updates"):
            return {"updates": [{"state": "RUNNING"}]}
        if path.endswith("/events"):
            return {"events": evs}
        return {"spec": {"gateway_definition": {"gateway_storage_catalog": "c", "gateway_storage_schema": "s"}}}
    s = W.check(W.Rest(do), "p", tables=["a"])
    assert s.ready() and ready and not any("/fs/" in c for c in calls)
