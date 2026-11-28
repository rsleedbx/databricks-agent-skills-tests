#!/usr/bin/env python3
"""bin/pipeline_api.py: the Lakeflow pipeline calls the bin tools share (get, list by name, one update, the latest
updates, stop), through lfc_rest.Rest. Each returns the API's JSON as it is. Events are read by lfc_watch's events()
(skills/lfc-pipeline-watch), which pages through them.

    from pipeline_api import get, list_named, get_update, latest_updates, stop
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "skills", "lfc-rest", "scripts"))
from lfc_rest import Rest  # noqa: E402


def get(api: Rest, pipe: str) -> dict:
    """The pipeline: spec, latest_updates, state, name."""
    return api.get(f"/api/2.0/pipelines/{pipe}")


def list_named(api: Rest, prefix: str) -> list[str]:
    """The ids of the pipelines whose name starts with prefix (the API's own filter)."""
    return [s["pipeline_id"] for s in api.pages("/api/2.0/pipelines", "statuses",
                                                 filter=f"name LIKE '{prefix}%'") if s.get("pipeline_id")]


def get_update(api: Rest, pipe: str, upd: str) -> dict:
    """One update: {"update": {"state", "config": {"serverless"}, ...}}."""
    return api.get(f"/api/2.0/pipelines/{pipe}/updates/{upd}")


def latest_updates(api: Rest, pipe: str, n: int = 1) -> list[dict]:
    """The newest updates, newest first."""
    return (api.get(f"/api/2.0/pipelines/{pipe}/updates", max_results=n).get("updates") or [])[:n]


def stop(api: Rest, pipe: str) -> None:
    api.post(f"/api/2.0/pipelines/{pipe}/stop")
