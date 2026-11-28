#!/usr/bin/env python3
"""bin/workspace_files.py: the Workspace API's file calls (list, status, mkdirs, import, export, delete) through
lfc_rest.Rest, for the scripts that copy files into a workspace (genie_skills_sync.py, sync.py). Paths are workspace
paths such as /Workspace/... or /Users/<me>/...; a missing path raises what lfc_rest raises (check not_found(e)).

    from workspace_files import status, listing, mkdirs, put, get, delete
"""
from __future__ import annotations

import base64
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "skills", "lfc-rest", "scripts"))
from lfc_rest import Rest, not_found  # noqa: E402  (not_found: the callers' "missing" test)


def _get(api: Rest, endpoint: str, query: dict):
    """A GET with the query as a dict: the workspace calls take `path` as a query argument, and Rest.get's own first
    argument is the path, so the keyword form can't carry it."""
    return api._do("GET", endpoint, {k: v for k, v in query.items() if v is not None} or None, None)


def status(api: Rest, path: str) -> dict | None:
    """The object at path ({"path", "object_type", ...}), or None when it doesn't exist."""
    try:
        return _get(api, "/api/2.0/workspace/get-status", {"path": path})
    except Exception as e:  # noqa: BLE001  a missing path is the answer; anything else goes up
        if not_found(e):
            return None
        raise


def listing(api: Rest, path: str) -> list[dict]:
    """The objects directly under a workspace folder. Raises when the folder is missing (not_found)."""
    return _get(api, "/api/2.0/workspace/list", {"path": path}).get("objects") or []


def mkdirs(api: Rest, path: str) -> None:
    api.post("/api/2.0/workspace/mkdirs", {"path": path})


def put(api: Rest, path: str, data: bytes, fmt: str = "RAW", language: str | None = None,
        overwrite: bool = True) -> None:
    """Import data at path. fmt: RAW (a file, as written), SOURCE (a notebook from its source text)."""
    body = {"path": path, "format": fmt, "content": base64.b64encode(data).decode(), "overwrite": overwrite}
    if language:
        body["language"] = language
    api.post("/api/2.0/workspace/import", body)


def get(api: Rest, path: str, fmt: str = "SOURCE") -> bytes:
    """The object's bytes, exported in fmt (SOURCE: a notebook's source text, a file's bytes)."""
    return base64.b64decode(_get(api, "/api/2.0/workspace/export", {"path": path, "format": fmt}).get("content") or "")


def delete(api: Rest, path: str, recursive: bool = False) -> None:
    api.post("/api/2.0/workspace/delete", {"path": path, "recursive": recursive})
