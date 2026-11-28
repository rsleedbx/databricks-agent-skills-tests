#!/usr/bin/env python3
"""mi.py plan|create|wait|status|endpoint|stop|start|delete: an Azure SQL Managed Instance behind its own NSG.
  Needs RG (resource group), VNET (an existing VNet in RG), MI_NAME. Identity: SUBSCRIPTION, required for every command
  but plan/create (those take az's current one, pin it and print its IDENTITY line; see cli-setup cloud-identity.sh).
  Optional: TENANT_ID (checked against the login),
  LOCATION (the VNet's), SUBNET (<VNET>-mi-subnet), SUBNET_CIDR (10.10.200.0/27; /27 or larger, unused in the VNet),
  VCORES (4), STORAGE_GB (32), PRICING (Freemium: the free offer, one per subscription; else Regular), OWNER ($USER).
  create also needs ADMIN_PW_FILE (a private file, or keyring:<name>, with the sqladmin password).
  plan: read-only, says what create would make. create: the subnet's NSG (<SUBNET>-nsg) and route table
  (<SUBNET>-rt, 0.0.0.0/0 to Internet), the subnet delegated to Microsoft.Sql/managedInstances, then the instance with
  the public data endpoint (port 3342) on; returns at once. The NSG starts with no rule for 3342, so nothing outside
  the VNet gets in until lfc-source-firewall syncs the laptop and Databricks egress ranges into <SUBNET>-nsg.
  wait: polls until Succeeded (first instance in a subnet: 4-6 hours; later ones 30-60 min); exit 1 Failed, 2 timeout.
  endpoint: prints <name>.public.<dns zone>.database.windows.net,3342. stop/start: pause and resume compute (General
  Purpose). delete: the instance only (the subnet stays until Azure removes its virtual cluster, which takes hours),
  and only when its CreatedBy tag is lfc-source-azure-sql (or, made before that tag, purpose=lfc-test with this
  owner); not found exits 3, a failed lookup 1, another's 2. Tags as sqldb.py (RemoveAfter: REMOVE_AFTER_DAYS).
  Calls az (the Azure CLI) only. create passes the password to az as its one flag, as the shell script did.
"""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lfc_azure as azure
from script_common import Fail, check_password, exits, need, print_fields, read_password

MINE = "lfc-source-azure-sql"
CMDS = ("plan", "create", "wait", "status", "endpoint", "stop", "start", "delete")
WAITS = (60, 120, 300, 600, 900) + (1800,) * 9   # about 6.5 hours


def mi(A: list, name: str):
    return azure.show_json("sql", "mi", "show", *A, "-n", name)


def probe(*args: str) -> bool:
    return azure.exists(*args, "-o", "none")


@exits
def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__.strip(), file=sys.stderr)
        return 1
    cmd = argv[0]
    rg, vnet, name = need("RG"), need("VNET"), need("MI_NAME")
    subnet = os.environ.get("SUBNET") or f"{vnet}-mi-subnet"
    subnet_cidr = os.environ.get("SUBNET_CIDR") or "10.10.200.0/27"
    if cmd in ("plan", "create"):
        sub = os.environ.get("SUBSCRIPTION") or azure.default_subscription()
    else:
        sub = need("SUBSCRIPTION", "the subscription_id from the create IDENTITY line (the az default is not used)")
    azure.require(sub, os.environ.get("TENANT_ID", ""))
    A = azure.ident(rg, sub)
    nsg, rt = f"{subnet}-nsg", f"{subnet}-rt"
    if cmd not in CMDS:
        print(f"unknown {cmd}")
        return 2

    if cmd in ("plan", "create"):
        return plan_or_create(cmd, A, sub, name, vnet, subnet, subnet_cidr, nsg, rt)
    if cmd == "wait":
        for w in WAITS:
            time.sleep(w)
            d = mi(A, name)
            s = (d or {}).get("provisioningState") or "missing"
            pct = azure_pct(A, name)
            print(f"{time.strftime('%H:%M:%S')} {s} {pct}%")
            if s == "Succeeded":
                return 0
            if s in ("Failed", "Canceled", "missing"):
                return 1
        print("still provisioning: check the operation in the portal; don't delete and retry")
        return 2
    if cmd == "status":
        d = mi(A, name)
        if d is None:
            raise Fail(1, f"no instance {name}")
        print_fields({k: d.get(k) for k in ("provisioningState", "state", "vCores", "storageSizeInGb",
                                              "publicDataEndpointEnabled", "pricingModel")})
        return 0
    if cmd == "endpoint":
        f = (mi(A, name) or {}).get("fullyQualifiedDomainName") or ""
        if not f:
            raise Fail(1, f"no instance {name}")
        first, rest = f.split(".", 1)
        print(f"{first}.public.{rest},3342")
        return 0
    if cmd in ("stop", "start"):
        azure.az("sql", "mi", cmd, *A, "--mi", name, "-o", "none")
        print(f"{cmd}: {name}")
        return 0
    rc = azure.may_delete(["sql", "mi", "show", *A], name, MINE)
    if rc:
        return rc
    azure.az("sql", "mi", "delete", *A, "-n", name, "--yes", "--no-wait")
    print(f"deleting {name} (the subnet, NSG and route table stay)")
    return 0


def azure_pct(A: list, name: str) -> str:
    """The percent complete of the instance's latest operation, or "?"."""
    ops = azure.show_json("sql", "mi", "op", "list", *A, "--mi", name) or []
    pct = ops[0].get("percentComplete") if ops else None
    return "?" if pct is None else str(pct)


def plan_or_create(cmd: str, A: list, sub: str, name: str, vnet: str, subnet: str, subnet_cidr: str,
                   nsg: str, rt: str) -> int:
    V = azure.show_json("network", "vnet", "show", *A, "-n", vnet)
    if V is None:
        print(f"no VNet {vnet} in {os.environ['RG']}: ask before creating one")
        return 2
    loc = os.environ.get("LOCATION") or V.get("location")
    sn = next((s for s in V.get("subnets") or [] if s.get("name") == subnet), {})
    have_sn = sn.get("name") or ""
    if have_sn:
        subnet_cidr = sn.get("addressPrefix")
        other = ",".join((d.get("serviceName") or "") for d in sn.get("delegations") or [])
        if other and other != "Microsoft.Sql/managedInstances":
            print(f"subnet {subnet} is delegated to {other}")
            return 2
    else:
        used = ",".join(s.get("name", "") for s in V.get("subnets") or [] if s.get("addressPrefix") == subnet_cidr)
        if used:
            print(f"{subnet_cidr} is already subnet {used}: set SUBNET_CIDR")
            return 2
    print(f"location   {loc}")
    print(f"subnet     {subnet} {subnet_cidr} {'(exists)' if have_sn else '(new)'}, "
          f"delegated to Microsoft.Sql/managedInstances")
    print(f"nsg        {nsg} "
          f"{'(exists)' if probe('network', 'nsg', 'show', *A, '-n', nsg) else '(new, no 3342 rule: lfc-source-firewall adds them)'}")
    print(f"route      {rt} 0.0.0.0/0 -> Internet "
          f"{'(exists)' if probe('network', 'route-table', 'show', *A, '-n', rt) else '(new)'}")
    vcores = os.environ.get("VCORES", "4")
    print(f"instance   {name} GP_Gen5 {vcores} vCores, {os.environ.get('STORAGE_GB', '32')} GB, "
          f"pricing {os.environ.get('PRICING', 'Regular')}, public data endpoint 3342 "
          f"{'(exists)' if mi(A, name) is not None else '(new)'}")
    if cmd == "plan":
        return 0

    pwf = need("ADMIN_PW_FILE")
    check_password(pwf)
    tg = azure.tags(MINE)
    if not probe("network", "nsg", "show", *A, "-n", nsg):
        azure.az("network", "nsg", "create", *A, "-n", nsg, "-l", loc, "--tags", *tg, "-o", "none")
    if not probe("network", "route-table", "show", *A, "-n", rt):
        azure.az("network", "route-table", "create", *A, "-n", rt, "-l", loc, "--tags", *tg, "-o", "none")
        azure.az("network", "route-table", "route", "create", *A, "--route-table-name", rt, "-n", "default-internet",
                 "--address-prefix", "0.0.0.0/0", "--next-hop-type", "Internet", "-o", "none")
    if have_sn:
        azure.az("network", "vnet", "subnet", "update", *A, "--vnet-name", vnet, "-n", subnet,
                 "--network-security-group", nsg, "--route-table", rt, "--delegations", "Microsoft.Sql/managedInstances",
                 "-o", "none")
    else:
        azure.az("network", "vnet", "subnet", "create", *A, "--vnet-name", vnet, "-n", subnet,
                 "--address-prefixes", subnet_cidr, "--network-security-group", nsg, "--route-table", rt,
                 "--delegations", "Microsoft.Sql/managedInstances", "-o", "none")
    print("subnet ready")
    if mi(A, name) is not None:
        print(f"instance {name} exists: mi.py wait")
        return 0
    sid = azure.az("network", "vnet", "subnet", "show", *A, "--vnet-name", vnet, "-n", subnet, "--query", "id",
                   "-o", "tsv").strip()
    # az takes the password only as a flag: it's in this machine's process list while the command runs
    pricing = os.environ.get("PRICING", "")
    azure.az("sql", "mi", "create", *A, "-n", name, "-l", loc, "--subnet", sid, "--admin-user", "sqladmin",
             "--admin-password", read_password(pwf), "--edition", "GeneralPurpose", "--family", "Gen5",
             "--capacity", vcores, "--storage", f"{os.environ.get('STORAGE_GB', '32')}GB",
             "--license-type", "LicenseIncluded", "--public-data-endpoint-enabled", "true",
             "--minimal-tls-version", "1.2", "--backup-storage-redundancy", "Local",
             *(["--pricing-model", pricing] if pricing else []), "--tags", *tg, "--no-wait", "-o", "none")
    azure.record(sub)
    print(f"creating {name} (first instance in a subnet: 4-6 hours). Next: mi.py wait, in the background")
    print(f"then: lfc-source-firewall scripts/nsg-copy.py <shared-nsg> {nsg} 3342 --apply")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
