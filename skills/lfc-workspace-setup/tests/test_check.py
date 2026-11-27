"""check.py offline: a fake REST; a principal not in the account stops at one FAIL; grants missing on a catalog FAIL with
the GRANT to run; owning the catalog passes them."""
import io
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
import check as C


def fake(owner="someone", privs=()):
    def do(m, p, q, b):
        if p.endswith("/account/scim/v2/Users"):
            return {"Resources": [{"id": "7", "groups": [{"display": "team"}]}]} if "me@x" in (q or {}).get("filter", "") else {}
        if p.endswith("/scim/v2/Me"):
            return {"userName": "me@x", "groups": []}
        if p.endswith("current-metastore-assignment"):
            return {"metastore_id": "m1", "workspace_id": 9}
        if "/effective-permissions/" in p:
            return {"privilege_assignments": [{"privileges": [{"privilege": x} for x in privs]}]}
        if "/catalogs/" in p:
            return {"owner": owner, "storage_root": "abfss://own/", "isolation_mode": "OPEN"}
        return {}
    return C.Rest(do, "ws")


def run(api, who, **kw):
    out = io.StringIO()
    return C.check(api, who, out=out, **kw), out.getvalue()


def test_principal_not_found_stops():
    n, out = run(fake(), "nobody@x")
    assert n == 1 and "nobody@x not found" in out and "metastore" not in out


def test_missing_grants_fail_with_the_grant():
    n, out = run(fake(privs=("USE_CATALOG",)), "me@x", catalog="c1", modes="qbc,icdc")
    assert "GRANT CREATE SCHEMA ON CATALOG `c1` TO `me@x`" in out and "FAIL  CREATE VOLUME on c1" in out and n >= 4


def test_create_schema_covers_its_own_schema():
    _, out = run(fake(privs=("USE_CATALOG", "CREATE_SCHEMA")), "me@x", catalog="c1", modes="qbc,icdc")
    assert "FAIL  USE_SCHEMA" not in out and "FAIL  CREATE_TABLE" not in out and "FAIL  CREATE VOLUME" not in out


def test_owner_through_a_group_passes_the_catalog():
    _, out = run(fake(owner="team"), "me@x", catalog="c1", modes="qbc")
    assert all(f"PASS  {p} on c1" in out for p in ("USE_CATALOG", "CREATE_SCHEMA", "USE_SCHEMA", "CREATE_TABLE"))
