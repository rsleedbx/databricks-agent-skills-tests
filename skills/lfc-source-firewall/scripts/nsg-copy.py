#!/usr/bin/env python3
"""nsg-copy.py <from-nsg> <to-nsg> <port> [--apply]: give another NSG the same sources as the region's shared NSG, for one port.
  Needs RG (both NSGs' resource group) and SUBSCRIPTION: the subscription_id from the IDENTITY line the NSG's create
  printed (az's default isn't used). Optional: TENANT_ID (checked against the login).
  Every inbound Allow rule of <from-nsg> with a source list (serverless, VPN, workspace egress, known hosts) becomes a
  rule of the same name and priority in <to-nsg>, allowing TCP <port> only (no SSH). Use it for an NSG that can't be the
  shared one, such as a SQL Managed Instance subnet's (port 3342). Prints the plan; changes nothing without --apply.
  Rerun after the shared NSG is synced: a rule whose sources, port and priority already match is left alone. Rules
  are set from the highest priority down, so a resync that moved every rule up by one slot applies without a clash.
  Calls az (the Azure CLI) only; the login check is lfc_azure's (the same as the azure-sql scripts).
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "lfc-source-azure-sql", "scripts"))
import lfc_azure as azure
from script_common import exits, need


def sources(rule: dict, drop_wildcard: bool) -> list[str]:
    """A rule's source prefixes (the list and the single one), sorted. The shared NSG's "*" is left out (the `from` side)."""
    found = list(rule.get("sourceAddressPrefixes") or []) + ([rule["sourceAddressPrefix"]] if rule.get("sourceAddressPrefix") else [])
    out = [x for x in found if x and not (drop_wildcard and x == "*")]
    return sorted(out)


def rule_named(doc: list, name: str) -> dict:
    return next((r for r in doc if r.get("name") == name), {})


@exits
def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(__doc__.strip(), file=sys.stderr)
        return 1
    frm, to, port = argv[0], argv[1], argv[2]
    apply = len(argv) > 3 and argv[3] == "--apply"
    rg = need("RG")
    sub = need("SUBSCRIPTION", "the subscription_id from the NSG IDENTITY line (the az default is not used)")
    azure.require(sub, os.environ.get("TENANT_ID", ""))
    A = azure.ident(rg, sub)
    src_doc = json.loads(azure.az("network", "nsg", "rule", "list", *A, "--nsg-name", frm, "-o", "json") or "[]")
    dst_doc = json.loads(azure.az("network", "nsg", "rule", "list", *A, "--nsg-name", to, "-o", "json") or "[]")
    # highest priority first: a rule moved up by the shared NSG's resync vacates its old slot before the next rule needs it
    inbound = [r for r in src_doc if r.get("direction") == "Inbound" and r.get("access") == "Allow"]
    for r in sorted(inbound, key=lambda r: (-int(r.get("priority") or 0), r.get("name") or "")):
        name, prio = r["name"], str(r.get("priority"))
        src = sources(r, drop_wildcard=True)   # the "*" in a source list isn't a prefix to copy
        if not src:
            print(f"skip  {name} (no source list)")
            continue
        have_rule = rule_named(dst_doc, name)
        have = sources(have_rule, drop_wildcard=False)
        have_port = have_rule.get("destinationPortRange") or ""
        have_prio = str(have_rule.get("priority") or "") if have_rule else ""
        if src == have and have_port == port and have_prio == prio:
            print(f"ok    {name} ({len(src)} sources, port {port})")
            continue
        print(f"set   {name} priority {prio}: {len(src)} sources -> TCP {port}")
        if not apply:
            continue
        # create overwrites a rule of the same name
        azure.az("network", "nsg", "rule", "create", *A, "--nsg-name", to, "-n", name, "--priority", prio,
                 "--direction", "Inbound", "--access", "Allow", "--protocol", "Tcp", "--source-port-ranges", "*",
                 "--destination-address-prefixes", "*", "--destination-port-ranges", port,
                 "--source-address-prefixes", *src, "-o", "none")
    if not apply:
        print("plan only: rerun with --apply")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
