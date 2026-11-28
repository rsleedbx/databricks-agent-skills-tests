#!/usr/bin/env python3
"""eventhubs.py up <resource group> <namespace> <region> --password-file F [--topic tag-observations] [--partitions 32]
             [--tu 2] [--max-tu 20] [--tags "Owner=<you> RemoveAfter=<date>"] [--source-json F] [--apply]
eventhubs.py down <resource group> <namespace> [--apply]
  up: an Event Hubs namespace (Standard, Kafka endpoint on, auto-inflate from --tu to --max-tu) with one event hub as
   the Kafka topic and a Listen+Send rule named kafka-test. The rule's connection string (the SASL password for user
   $ConnectionString) goes into a 0600 file, never printed, and the source contract (lfc-source-contract, engine kafka:
   bootstrap, user, topic, the password file's path; no secret) into --source-json (default <namespace>.source.json
   beside the password file) for the tests to read as SOURCE_JSON. The resource group must exist (one you choose for
   all your test resources): it's never created, re-tagged or deleted. The namespace is tagged CreatedBy=lfc-source-eventhubs;
   one of that name without the tag is left alone (up stops). AZ_SUBSCRIPTION, if set, goes on every az call.
  down: deletes the namespace (its hubs and rules) only when its CreatedBy tag says this script made it (also the tag
   lfc-kafka-freshness-test, which it used before); never the group. A failed lookup (login, subscription) exits 1, not 0.
   Prints the plan; changes nothing without --apply.
Calls az (the Azure CLI) only; Databricks is not involved. The password is written to its file, never printed.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "lfc-source-azure-sql", "scripts"))
import lfc_azure as azure
from script_common import exits

MINE = "lfc-source-eventhubs"
LEGACY = "lfc-kafka-freshness-test"   # the CreatedBy tag this script wrote before it moved here


def mine(created_by: str) -> bool:
    return created_by in (MINE, LEGACY)


def write_private(path: str, text: str) -> None:
    """Writes text to path as 0600 (the shell's umask 077 with >)."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)


def sub_args() -> list[str]:
    s = os.environ.get("AZ_SUBSCRIPTION")
    return ["--subscription", s] if s else []


def namespace_show(rg: str, ns: str):
    return azure.lookup(["eventhubs", "namespace", "show", "-g", rg, *sub_args()], ns)


@exits
def main(argv: list[str]) -> int:
    cmd, rg, ns = (argv + ["", "", ""])[:3]
    if not (cmd and rg and ns):
        print(__doc__.strip())
        return 2
    rest = argv[3:]
    if cmd == "up":
        return up(rg, ns, rest)
    if cmd == "down":
        return down(rg, ns, rest)
    print(f"up or down, not {cmd}")
    return 2


def up(rg: str, ns: str, rest: list[str]) -> int:
    loc = rest[0] if rest else ""
    if not loc:
        print("up needs <region>")
        return 2
    rest = rest[1:]
    opts = {"--password-file": "", "--topic": "tag-observations", "--partitions": "32", "--tu": "2",
            "--max-tu": "20", "--tags": "", "--source-json": ""}
    apply = False
    while rest:
        a = rest.pop(0)
        if a == "--apply":
            apply = True
        elif a in opts and rest:
            opts[a] = rest.pop(0)
        else:
            print(f"unknown {a}")
            return 2
    pwf, topic, parts = opts["--password-file"], opts["--topic"], opts["--partitions"]
    tu, maxtu, tags = opts["--tu"], opts["--max-tu"], opts["--tags"]
    if not pwf:
        print("--password-file is required")
        return 2
    sj = opts["--source-json"] or os.path.join(os.path.dirname(pwf) or ".", f"{ns}.source.json")
    print(f"plan: namespace {ns} in {rg} ({loc}), Standard, {tu}-{maxtu} TU, Kafka on; hub {topic} ({parts} partitions, "
          f"24 h); rule kafka-test (Listen, Send) -> {pwf}; contract -> {sj}")
    if not apply:
        print("plan only: run again with --apply")
        return 0

    if not azure.exists("group", "show", *sub_args(), "-n", rg, "-o", "none"):
        where = f" in subscription {os.environ['AZ_SUBSCRIPTION']}" if os.environ.get("AZ_SUBSCRIPTION") else ""
        print(f"resource group {rg} doesn't exist{where}: pass an existing group; this script never creates one")
        return 2
    status, t = namespace_show(rg, ns)
    have = status == "ok"
    by = ((t or {}).get("CreatedBy") or "") if have else ""
    if have and not mine(by):
        print(f"namespace {ns} exists and wasn't created by this script (CreatedBy={by or 'none'}): left alone; pick another name")
        return 2
    if not have:
        azure.az("eventhubs", "namespace", "create", *sub_args(), "-g", rg, "-n", ns, "-l", loc, "--sku", "Standard",
                 "--capacity", tu, "--enable-auto-inflate", "true", "--maximum-throughput-units", maxtu,
                 "--enable-kafka", "true", "--tags", f"CreatedBy={MINE}", *tags.split(), "-o", "none")
    else:
        print(f"namespace {ns}: made by this script earlier, reused")
    azure.az("eventhubs", "eventhub", "create", *sub_args(), "-g", rg, "--namespace-name", ns, "-n", topic,
             "--partition-count", parts, "--retention-time-in-hours", "24", "--cleanup-policy", "Delete", "-o", "none")
    azure.az("eventhubs", "namespace", "authorization-rule", "create", *sub_args(), "-g", rg, "--namespace-name", ns,
             "-n", "kafka-test", "--rights", "Listen", "Send", "-o", "none")
    conn = azure.az("eventhubs", "namespace", "authorization-rule", "keys", "list", *sub_args(), "-g", rg,
                    "--namespace-name", ns, "--authorization-rule-name", "kafka-test",
                    "--query", "primaryConnectionString", "-o", "tsv")
    if not conn:
        print("no connection string written")
        return 1
    write_private(pwf, conn)
    pwa = os.path.abspath(pwf)
    contract = {"contract": 1, "engine": "kafka", "name": ns, "host": f"{ns}.servicebus.windows.net", "port": 9093,
                "user": "$ConnectionString", "topic": topic, "sasl_mechanism": "PLAIN", "password_file": pwa}
    write_private(sj, json.dumps(contract, indent=2) + "\n")
    print(f"bootstrap {ns}.servicebus.windows.net:9093  user $ConnectionString  password file {pwa} "
          f"({len(conn.encode())} bytes)")
    print(f"source contract {sj}: SOURCE_JSON={sj}")
    return 0


def down(rg: str, ns: str, rest: list[str]) -> int:
    apply = rest[:1] == ["--apply"]
    print(f"plan: delete namespace {ns} in {rg} if this script made it (CreatedBy={MINE}; its hubs and rules; the group stays)")
    if not apply:
        print("plan only: run again with --apply")
        return 0
    status, t = namespace_show(rg, ns)
    if status == "missing":
        print(f"namespace {ns} not found in {rg}: nothing to delete")
        return 0
    if status == "error":
        if "ResourceGroupNotFound" in t:
            where = f" in subscription {os.environ['AZ_SUBSCRIPTION']}" if os.environ.get("AZ_SUBSCRIPTION") else ""
            print(f"resource group {rg} not found{where}: check the subscription; nothing deleted")
            return 1
        print(f"couldn't look up namespace {ns} in {rg}; nothing deleted:")
        print(t)
        return 1
    by = (t or {}).get("CreatedBy") or ""
    if not mine(by):
        print(f"namespace {ns} wasn't created by this script (CreatedBy={by or 'none'}): not deleted")
        return 2
    azure.az("eventhubs", "namespace", "delete", *sub_args(), "-g", rg, "-n", ns)
    print("deleted")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
