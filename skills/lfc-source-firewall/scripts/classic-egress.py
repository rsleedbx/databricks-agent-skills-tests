#!/usr/bin/env python3
"""classic-egress.py [<profile>] [--warehouse <id> [--days N]] [--runs N]: the public IP a workspace's classic compute
leaves from. (Was classic-egress.sh; the same options and output.)

Classic clusters leave through the workspace's NAT, which isn't in ip-ranges.json. Two ways to find it:
  --warehouse <id>  (seconds, nothing started) read the audit log: the public source IPs of API calls that classic
                    cluster drivers made in the last N days (default 7). Needs SELECT on system.access.audit and a
                    SQL warehouse; shows every NAT IP in use, not just one. A workspace with no classic activity in
                    that window returns nothing: use the job instead.
  (default)         submit a one-off single-node notebook job that asks api.ipify.org for its address, then delete
                    the notebook. 5-10 min while the cluster starts. --runs N submits N jobs and keeps the distinct
                    IPs: a cluster in another subnet can leave from a second NAT IP.
Prints one IP per line, then a CLASSIC='<ip>/32 ...' line for fw.env:

  skills/lfc-source-firewall/scripts/classic-egress.py <PROFILE> --warehouse <WAREHOUSE_ID> | tail -1 >> fw.env

<profile> (or --profile) picks the workspace on a laptop; inside a workspace (a notebook) leave it out, and the
notebook's own login is used. Every call is REST through lfc_rest.Rest (no databricks CLI): the audit query through
the SQL statements API, the job through jobs/runs/submit, and the notebook through the workspace import and delete.

Node type for the job: m5d.large on AWS, Standard_D4ds_v5 on Azure, n2-standard-4 on GCP (NODE_TYPE overrides). The
IP is a workspace detail: keep it in private files, never in a public repository.
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "lfc-rest", "scripts"))
from lfc_rest import Rest

sys.path.insert(0, os.path.join(HERE, "..", "..", "lfc-change-source", "scripts"))
from lfc_change_source import jobs

sys.path.insert(0, os.path.join(HERE, "..", "..", "lfc-source-azure-sql", "scripts"))
from script_common import Fail

IPV4 = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+")
PRIVATE = re.compile(r"(10\.|127\.|169\.254\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.)")
NOTEBOOK_SOURCE = ('# Databricks notebook source\n'
                   'import urllib.request\n'
                   'dbutils.notebook.exit(urllib.request.urlopen("https://api.ipify.org", timeout=10).read().decode())\n')


def in_workspace() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def public_ips(texts) -> list[str]:
    """The public IPv4 addresses among the lines of texts, sorted and distinct."""
    ips = set()
    for t in texts:
        for line in str(t).splitlines():
            if IPV4.fullmatch(line) and not PRIVATE.match(line):
                ips.add(line)
    return sorted(ips)


def finish(texts) -> int:
    ips = public_ips(texts)
    if not ips:
        raise Fail(1, "no public IP found")
    print("\n".join(ips))
    print("CLASSIC='" + " ".join(f"{ip}/32" for ip in ips) + "'")
    return 0


def from_audit(api: Rest, warehouse: str, days: int) -> int:
    ws = api.get("/api/2.1/unity-catalog/current-metastore-assignment").get("workspace_id")
    sql = ("SELECT DISTINCT source_ip_address FROM system.access.audit WHERE event_date >= current_date() - "
           f"{days} AND workspace_id = {ws} AND user_agent LIKE '%Databricks-Service/driver%'")
    r = api.post("/api/2.0/sql/statements", {"warehouse_id": warehouse, "wait_timeout": "50s", "statement": sql})
    if (r.get("status") or {}).get("state") != "SUCCEEDED":
        print(json.dumps(r.get("status"), indent=2), file=sys.stderr)
        return 1
    rows = (r.get("result") or {}).get("data_array") or []
    return finish([row[0] for row in rows if row])


def spark_version(api: Rest) -> str:
    """The newest LTS runtime of the workspace's list."""
    versions = api.get("/api/2.1/clusters/spark-versions").get("versions") or []
    lts = [v for v in versions if "LTS" in (v.get("name") or "")
           and re.search(r"^[0-9]+\.[0-9]+\.x-scala", v.get("key") or "")]
    if not lts:
        raise Fail(1, "the workspace lists no LTS runtime to run the job on")
    return max(lts, key=lambda v: int(v["key"].split(".")[0]))["key"]


def node_type(host: str) -> str:
    if "azuredatabricks.net" in host:
        return os.environ.get("NODE_TYPE") or "Standard_D4ds_v5"
    if "gcp.databricks.com" in host:
        return os.environ.get("NODE_TYPE") or "n2-standard-4"
    return os.environ.get("NODE_TYPE") or "m5d.large"


def from_job(api: Rest, runs: int) -> int:
    user = api.get("/api/2.0/preview/scim/v2/Me")["userName"]
    nb = f"/Users/{user}/lfc-classic-egress-ip"
    body = {"run_name": "lfc-classic-egress-ip", "tasks": [{"task_key": "ip", "notebook_task": {"notebook_path": nb},
            "new_cluster": {"spark_version": spark_version(api), "node_type_id": node_type(api.host), "num_workers": 0,
                            "data_security_mode": "SINGLE_USER",
                            "spark_conf": {"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*]"},
                            "custom_tags": {"ResourceClass": "SingleNode"}}}]}
    jobs.upload_notebook(api, nb, NOTEBOOK_SOURCE)
    texts = []
    try:
        for _ in range(runs):
            rid = api.post("/api/2.1/jobs/runs/submit", body)["run_id"]
            st = jobs.wait(api, rid, timeout_s=1800)
            if st["result"] != "SUCCESS":
                raise Fail(1, f"run {rid} ended {st['life_cycle']} {st['result']}: {st['url']}")
            out = jobs.output(api, st["task_run_id"])
            texts.append((out.get("notebook_output") or {}).get("result"))
    finally:
        with contextlib.suppress(Exception):   # the notebook goes whether the runs worked or not (the shell's EXIT trap)
            api.post("/api/2.0/workspace/delete", {"path": nb})
    return finish(texts)


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__.strip())
        return 2
    ap = argparse.ArgumentParser(prog="classic-egress.py", add_help=False)
    ap.add_argument("profile", nargs="?", default="")
    ap.add_argument("--profile", dest="profile_opt", default="")
    ap.add_argument("--warehouse", default="")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--runs", type=int, default=1)
    a = ap.parse_args(argv)
    profile = a.profile or a.profile_opt or None
    if not profile and not in_workspace():
        print(__doc__.strip())
        return 2
    try:
        api = Rest.connect(profile)
        if a.warehouse:
            return from_audit(api, a.warehouse, a.days)
        return from_job(api, a.runs)
    except Fail as e:
        if e.text:
            print(e.text, file=sys.stderr)
        return e.rc
    except Exception as e:  # noqa: BLE001  a failed call: its message, not a traceback
        print(f"classic-egress: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
