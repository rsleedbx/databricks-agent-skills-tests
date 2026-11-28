"""ready.py offline: a fake REST answers; the profile and catalog fall back to the saved settings; a preview that's off
is turned on only as an admin and without --no-enable (direct_cdc_connector among them)."""
import io
import os
import sys
import tempfile
from contextlib import redirect_stdout

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
import ready


def fake(admin=True, off=("direct_cdc_connector",)):
    calls, state = [], {s: s not in off for s in ready.PREVIEWS}
    state["serverless_compute"] = True

    def do(m, p, b=None):
        calls.append((m, p))
        if p.endswith("/scim/v2/Me"):
            return {"userName": "me@x", "groups": [{"display": "admins"}] if admin else []}
        if p.startswith("/api/2.1/settings/"):
            n = p.rsplit("/", 1)[1]
            if m == "PATCH":
                state[n] = b["boolean_val"]["value"]
            return {"boolean_val": {"value": state.get(n, False)}}
        if p.endswith("metastore_summary"):
            return {"global_metastore_id": "azure:centralus:x", "region": "centralus"}
        if "/catalogs/" in p:
            return {"name": "cat_x", "owner": "me@x", "storage_root": "abfss://own/"}
        if "/schemas/" in p:
            return {"owner": "me@x"}
        return {}
    return ready.Rest(lambda m, p, q, b: do(m, p, b)), calls, state


def run(argv, do):
    out = io.StringIO()
    with redirect_stdout(out):
        rc = ready.main(argv + ["--egress-file", ""], api=do)
    return rc, out.getvalue()


def test_turns_on_a_preview_that_is_off_as_admin():
    do, calls, state = fake()
    _, out = run(["p", "--catalog", "cat_x"], do)
    assert ("PATCH", "/api/2.1/settings/direct_cdc_connector") in calls and state["direct_cdc_connector"]
    assert "turned on now" in out and "5 target cat_x.default" in out


def test_no_enable_and_not_admin_change_nothing():
    for argv, admin in ((["p", "--no-enable"], True), (["p"], False)):
        do, calls, _ = fake(admin=admin)
        run(argv + ["--catalog", "cat_x"], do)
        assert not any(m == "PATCH" for m, _ in calls)


def test_saved_settings_and_none():
    with tempfile.TemporaryDirectory() as d:
        os.environ["LFC_TESTS_SETTINGS"] = os.path.join(d, "s.env")
        os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)
        assert ready.main([]) == 2                       # nothing saved, no profile, not in a notebook
        os.system(f"python3 '{ready.SETTINGS}' set DATABRICKS_PROFILE=ws-x LFC_CATALOG=cat_x > /dev/null")
        do, calls, _ = fake()
        _, out = run([], do)
        assert "== ws-x" in out and any("/catalogs/cat_x" in p for _, p in calls)
