"""lfc_rest: the one way the tests repo's skills call a Databricks workspace. The SDK only signs in and carries the
request (a profile on a laptop, the notebook's own login in a workspace; its retries on 429/503, its typed errors);
every call is a REST path and its JSON, as the API documents it (the SDK's typed models drop fields, an event's
details for one).

    from lfc_rest import Rest
    api = Rest.connect("my-profile")          # in a notebook: Rest.connect()
    api.get("/api/2.0/pipelines/<id>")
    for e in api.pages("/api/2.0/pipelines/<id>/events", "events", max_results=250): ...
    api.ls("/Volumes/c/s/v/dir")              # Files API: [{"path", "is_directory", "file_size", ...}]
    api.poll_until(lambda: api.get(...), lambda r: r["state"] == "RUNNING", every=15, timeout=600)

Tests pass a fake: Rest(lambda method, path, query, body: {...}).
"""
from __future__ import annotations

import time
from urllib.parse import quote


class Rest:
    def __init__(self, do, name: str = ""):
        """do(method, path, query, body, **kw) -> the JSON answer (dict), raising on an HTTP error; kw (raw, data,
        headers) only for the Files API's bytes."""
        self._do, self.name = do, name

    @classmethod
    def connect(cls, profile: str | None = None, retry_s: int | None = None) -> Rest:
        """Signed in through the SDK: the profile, or (none) the environment's login (a notebook's own). retry_s: how
        long the SDK retries an answer it counts as passing (default 300 s; a probe whose refusal is the answer, such
        as clusters list-zones on a serverless-only workspace, wants a short one)."""
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.core import Config
        kw = {"profile": profile} if profile else {}
        if retry_s:
            kw["retry_timeout_seconds"] = retry_s
        return cls.of(WorkspaceClient(config=Config(**kw)), profile)

    @classmethod
    def of(cls, w, name: str | None = None) -> Rest:
        """A Rest on a signed-in WorkspaceClient (or a Rest, returned as is)."""
        if isinstance(w, Rest):
            return w
        return cls(lambda m, p, q, b, **kw: w.api_client.do(m, p, query=q, body=b, **kw), name or w.config.host)

    def call(self, method: str, path: str, body=None, **query):
        """The JSON answer. When the SDK gave up retrying, its last answer is raised (the API's own error), not its
        "Timed out after ..." (seen: clusters list-zones' BadRequest "no associated worker environments", retried)."""
        try:
            return self._do(method, path, {k: v for k, v in query.items() if v is not None} or None, body) or {}
        except TimeoutError as e:
            if e.__cause__ is not None:
                raise e.__cause__ from e
            raise

    def get(self, path: str, **query):
        return self.call("GET", path, **query)

    def post(self, path: str, body=None, **query):
        return self.call("POST", path, body, **query)

    def put(self, path: str, body=None, **query):
        return self.call("PUT", path, body, **query)

    def patch(self, path: str, body=None, **query):
        return self.call("PATCH", path, body, **query)

    def delete(self, path: str, **query):
        return self.call("DELETE", path, **query)

    def pages(self, path: str, key: str, limit: int | None = None, **query):
        """Each item of `key` across pages (next_page_token), up to limit."""
        n, token = 0, None
        while True:
            r = self.get(path, **query, page_token=token)
            for x in r.get(key) or []:
                yield x
                n += 1
                if limit and n >= limit:
                    return
            token = r.get("next_page_token")
            if not token:
                return

    def download(self, path: str) -> bytes:
        """A file's bytes (Files API)."""
        r = self._do("GET", f"/api/2.0/fs/files{quote(path)}", None, None, raw=True)
        c = r.get("contents") if isinstance(r, dict) else r
        return c.read() if hasattr(c, "read") else (c or b"")

    def upload(self, path: str, data: bytes, overwrite: bool = True) -> None:
        """Write a file (Files API, one PUT: files up to a few GB)."""
        self._do("PUT", f"/api/2.0/fs/files{quote(path)}", {"overwrite": "true" if overwrite else "false"}, None,
                 data=data, headers={"Content-Type": "application/octet-stream"})

    def rm(self, path: str) -> None:
        """Delete a file (Files API)."""
        self._do("DELETE", f"/api/2.0/fs/files{quote(path)}", None, None)

    def rmdir(self, directory: str) -> None:
        """Delete an empty directory (Files API)."""
        self._do("DELETE", f"/api/2.0/fs/directories{quote(directory)}", None, None)

    def ls(self, directory: str, limit: int | None = None):
        """A Files API directory's entries (one level): {"path", "is_directory", "file_size", "last_modified", "name"}."""
        return self.pages(f"/api/2.0/fs/directories{quote(directory)}", "contents", limit)

    @staticmethod
    def poll_until(fetch, done, every: float = 15, timeout: float = 600, sleep=time.sleep, clock=time.time):
        """fetch() until done(answer): the answer; TimeoutError with the last answer after timeout."""
        t0 = clock()
        while True:
            r = fetch()
            if done(r):
                return r
            if clock() - t0 > timeout:
                raise TimeoutError(f"not done after {int(timeout)} s: {str(r)[:300]}")
            sleep(every)


def not_found(e: Exception) -> bool:
    """The error is a missing resource (the SDK's NotFound, or its message)."""
    return type(e).__name__ in ("NotFound", "ResourceDoesNotExist") or any(
        x in str(e) for x in ("NOT_FOUND", "RESOURCE_DOES_NOT_EXIST", "does not exist", "404"))


def default_catalog(api) -> str:
    """The workspace's default catalog (its default-namespace setting; each workspace has its own, never assume
    "main"), or "" when none is set."""
    try:
        v = Rest.of(api).get("/api/2.0/settings/types/default_namespace_ws/names/default")
    except Exception as e:  # noqa: BLE001  a workspace that never set one answers NOT_FOUND
        if not_found(e):
            return ""
        raise
    return ((v.get("namespace") or {}).get("value")) or ""


UNWRITABLE = ("system", "hive_metastore", "samples")


def target_catalog(api, given: str | None = None) -> str:
    """The catalog to write to: `given`, else the workspace's default (each workspace has its own: never assume
    "main"). ValueError for none, or one pipelines can't write to (system, hive_metastore, samples), saying what to
    pass."""
    cat = given or default_catalog(api)
    if not cat:
        raise ValueError("no catalog given and the workspace has no default catalog: pass one (CATALOG=<catalog>)")
    if cat.lower() in UNWRITABLE:
        raise ValueError(f"catalog {cat!r}{'' if given else ' (the workspace default)'} can't be written to: "
                         "pass a writable one (CATALOG=<catalog>; lfc-workspace-setup ready.py lists them)")
    return cat
