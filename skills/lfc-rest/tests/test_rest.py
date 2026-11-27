"""lfc_rest offline: query args without None, paging, the Files API path, poll_until."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
from lfc_rest import Rest, not_found, target_catalog


def test_get_drops_none_and_pages_follow_the_token():
    calls = []

    def do(m, p, q, b):
        calls.append((m, p, q))
        return {"items": [1, 2], "next_page_token": "t"} if not (q or {}).get("page_token") else {"items": [3]}
    api = Rest(do)
    assert list(api.pages("/x", "items", max_results=2)) == [1, 2, 3]
    assert calls[0] == ("GET", "/x", {"max_results": 2}) and calls[1][2] == {"max_results": 2, "page_token": "t"}
    assert list(api.pages("/x", "items", limit=1)) == [1]


def test_ls_quotes_the_path():
    seen = []
    list(
        Rest(lambda m, p, q, b: seen.append(p) or {"contents": []}).ls("/Volumes/c/s/v/a b"))
    assert seen == ["/api/2.0/fs/directories/Volumes/c/s/v/a%20b"]


def test_poll_until():
    t, n = [0.0], iter([1, 2, 3])
    assert Rest.poll_until(lambda: next(n), lambda r: r == 2, sleep=lambda s: None) == 2
    with pytest.raises(TimeoutError):
        Rest.poll_until(lambda: 0, lambda r: False, every=10, timeout=20,
                        sleep=lambda s: t.__setitem__(0, t[0] + s), clock=lambda: t[0])


def test_not_found():
    assert not_found(Exception("RESOURCE_DOES_NOT_EXIST: x")) and not not_found(Exception("PERMISSION_DENIED"))


def test_a_retry_timeout_raises_the_apis_own_error():
    def do(m, p, q, b):
        try:
            raise ValueError("BadRequest: no associated worker environments")
        except ValueError as e:
            raise TimeoutError("Timed out after 0:00:05") from e
    with pytest.raises(ValueError, match="worker environments"):
        Rest(do).get("/api/2.0/clusters/list-zones")


def test_files_bytes_through_raw_and_data():
    import io
    seen = []

    def do(m, p, q, b, **kw):
        seen.append((m, p, q, kw.get("data"), kw.get("raw")))
        return {"contents": io.BytesIO(b"abc")} if m == "GET" else {}
    api = Rest(do)
    assert api.download("/Volumes/c/s/v/f.json") == b"abc"
    api.upload("/Volumes/c/s/v/f.json", b"x")
    assert seen[0] == ("GET", "/api/2.0/fs/files/Volumes/c/s/v/f.json", None, None, True)
    assert seen[1][:4] == ("PUT", "/api/2.0/fs/files/Volumes/c/s/v/f.json", {"overwrite": "true"}, b"x")


def test_target_catalog_from_the_workspace_default():
    api = lambda d: Rest(lambda m, p, q, b: {"namespace": {"value": d}} if d else {})  # noqa: E731
    assert target_catalog(api("ws_cat")) == "ws_cat" and target_catalog(api("ws_cat"), "mine") == "mine"
    for bad in ("system", ""):
        with pytest.raises(ValueError):
            target_catalog(api(bad))
