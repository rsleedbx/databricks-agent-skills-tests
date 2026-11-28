#!/usr/bin/env python3
"""cloud_identity.py record <subscription> | require <subscription> [tenant_id]: a cloud resource belongs to one identity:
an Azure subscription (in its tenant). One login can reach many of them, and the CLI's default is whichever was set
last, so every later call for a resource names its identity.
  record   prints the record: IDENTITY azure tenant_id=.. subscription_id=.. subscription_name=..
  require  stops (exit 1) unless this az login reaches the subscription, in that tenant when one is given
A create prints its record next to its other outputs. Keep that line's values in your private env file (SUBSCRIPTION=,
TENANT_ID=) with the resource's other settings, and read the file for every later call. Never commit it.
The shell scripts of the lfc-source-* skills source cloud-identity.sh, which calls this file.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from laptop import json_out, out_of


def record(subscription: str) -> str | None:
    """The IDENTITY line for the subscription, or None when this az login can't show it."""
    acct = json_out(["az", "account", "show", "--subscription", subscription, "-o", "json"])
    if not acct:
        return None
    return (f"IDENTITY azure tenant_id={acct['tenantId']} subscription_id={acct['id']} "
            f"subscription_name={acct['name']}")


def require(subscription: str, tenant: str = "") -> str | None:
    """None when this login reaches the subscription (in the tenant, if given); else the fix, as a message."""
    got = out_of(["az", "account", "show", "--subscription", subscription, "--query", "tenantId", "-o", "tsv"], 60)
    if not got:
        return f"subscription {subscription} isn't in this az login: az login --tenant {tenant or '<tenant_id>'}"
    if tenant and got != tenant:
        return f"subscription {subscription} is in tenant {got}, not {tenant}: az login --tenant {tenant}"
    return None


def main(argv: list[str]) -> int:
    if len(argv) < 2 or argv[0] not in ("record", "require"):
        print(__doc__.split("\n\n")[0], file=sys.stderr)
        return 2
    if argv[0] == "record":
        line = record(argv[1])
        if line is None:
            print(f"cloud_identity: az account show --subscription {argv[1]} failed", file=sys.stderr)
            return 1
        print(line)
        return 0
    problem = require(argv[1], argv[2] if len(argv) > 2 else "")
    if problem:
        print(problem, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
