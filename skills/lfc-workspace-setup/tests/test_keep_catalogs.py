"""keep_catalogs.py offline (the cases of the old test-keep-catalogs.sh): a fake REST for catalogs and tags."""
import io
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
import keep_catalogs as K

LIST = [{"name": "m", "catalog_type": "MANAGED_CATALOG"},
        {"name": "a-fc", "catalog_type": "FOREIGN_CATALOG", "connection_name": "a"},
        {"name": "b-fc", "catalog_type": "FOREIGN_CATALOG", "connection_name": "b"}]


def fake(tags=None, listing=LIST, list_fails=False):
    tags, calls = dict(tags or {}), []

    def do(m, p, q, b):
        if p.endswith("/catalogs"):
            if list_fails:
                raise RuntimeError("invalid access token")
            return {"catalogs": listing}
        if m == "GET":
            name = p.split("/")[-3]
            if name not in tags:
                raise RuntimeError("RESOURCE_DOES_NOT_EXIST: tag")
            return {"tag_value": tags[name]}
        calls.append((m, b["entity_type"], b["entity_name"], b["tag_value"], (q or {}).get("update_mask")))
        if b["entity_name"] == "denied":
            raise RuntimeError("PERMISSION_DENIED")
        return {}
    return K.Rest(do, "p"), calls


def run(api, cats=(), conns=(), until="2026-12-01"):
    out = io.StringIO()
    return K.keep(api, cats, conns, until, out), out.getvalue()


def test_connection_itself_and_its_foreign_catalog():
    api, calls = fake()
    n, out = run(api, conns=["a"])
    assert [(c[1], c[2]) for c in calls] == [("connections", "a"), ("catalogs", "a-fc")] and n == 0
    assert out.startswith("set     connection a RemoveAfter=2026-12-01")


def test_later_kept_earlier_updated_none_set():
    api, calls = fake({"later": "2027-01-01", "same": "2026-12-01", "early": "2026-11-01"})
    _, out = run(api, ["later", "same", "early", "fresh"])
    assert "kept    later RemoveAfter=2027-01-01" in out and "kept    same" in out
    assert "updated early RemoveAfter=2026-12-01 (was 2026-11-01)" in out and "set     fresh RemoveAfter=2026-12-01" in out
    assert calls == [("PATCH", "catalogs", "early", "2026-12-01", "tag_value"), ("POST", "catalogs", "fresh", "2026-12-01", None)]


def test_failed_create_goes_on():
    api, _ = fake()
    n, out = run(api, ["denied", "fresh"])
    assert n == 1 and "failed  denied: PERMISSION_DENIED" in out and "set     fresh" in out


def test_connection_without_foreign_catalog_and_a_failed_list():
    n, out = run(fake()[0], conns=["z"])
    assert n == 0 and "none    foreign catalogs of z" in out and "set     connection z" in out
    n, out = run(fake(list_fails=True)[0], ["fresh"], ["a"])
    assert n == 1 and "failed  catalogs list: invalid access token" in out and "set     fresh" in out


def test_month_out_and_bad_dates():
    for f, u in (("2026-01-31", "2026-02-28"), ("2028-01-31", "2028-02-29"), ("2026-12-31", "2027-01-31"),
                 ("2026-03-31", "2026-04-30"), ("2026-05-15", "2026-06-15")):
        assert K.until_date({"KEEP_FROM": f}) == u
    for e in ({"KEEP_FROM": "x"}, {"KEEP_UNTIL": "12/01/2026"}):
        assert K.main(["p", "fresh"], api=fake()[0], env=e) == 2
