# Azure firewall rules

Uses `fw.env` and the parent skill's `$Q` and `$FW` (its Step 0). Start every block with `set -a; . ./fw.env`. Also needed: the resource group (`RG`) and the server or NSG.

**Resource group:** the saved `AZ_RESOURCE_GROUP` (lfc-test-settings: an existing group the user picked once; never created, re-tagged or deleted). Shared groups can hit quota or resource limits: if a create fails with a quota error there, ask for another group instead of removing other people's resources.

## Shared NSG (VMs and the shared VNet): set up once, reuse everywhere

Most test sources run on an Azure VM (often several engines in Podman on one VM), so keep **one NSG per region** and attach every VM and subnet to it. The rules are synced once; each new VM or subnet just attaches to the NSG, and doesn't need its own rules.

**Pick the NSG.** It must be in the same region as the VM or subnet it's attached to:

```bash
cat >> fw.env <<'EOF'
RG=<resource-group>
LOCATION=<region>
EOF
set -a; . ./fw.env
az network nsg list -g "$RG" -o json \
  | yq -p json -o json -r '.[] | select(.location == strenv(LOCATION)) | .name + "  rules: " + (.securityRules | length | tostring)'
```

- **One exists in the region** (in `<resource-group>`, for example `<nsg-name>` in the source's region): use it. A team NSG often already allows the serverless ranges; the coverage step below finds that.
- **None exists:** ask, then create one: `az network nsg create -g "$RG" -n lfc-test-nsg-$LOCATION -l "$LOCATION" --tags purpose=lfc-test`.

**Ports.** Podman engines on a VM are published on one range, `30000-30063`, so one set of rules covers every engine on every VM. Native installs use the engine's port (5432, 3306, 1433, 1521). SSH (22) is opened to the laptop only, never to Databricks.

**What the NSG already allows** on those ports, leaving out your own `lfc-` rules and anything open to everyone:

```bash
cat >> fw.env <<'EOF'
NSG=<nsg-name>
PORTS=30000-30063
PRIO_BASE=300
EOF
set -a; . ./fw.env
az network nsg rule list -g "$RG" --nsg-name "$NSG" -o json > nsg.json
eval "$($Q $FW/nsg_cover.sql | tee -a fw.env)"
eval "$($Q $FW/missing.sql | tee -a fw.env)"
echo "missing: $(echo $MISSING | wc -w); other rules' priorities: $USED_PRIOS"
```

Priorities must be unique within the NSG: pick a `PRIO_BASE` whose block (`PRIO_BASE` to `PRIO_BASE+20`) isn't in `USED_PRIOS`.

**Plan the `lfc-` rules.** One rule per purpose, up to 400 prefixes per rule (an NSG rule's limit), so every serverless region fits in two rules. This is just splitting a list, so it's shell:

```bash
set -a; . ./fw.env
sel() { for w in $MISSING; do case " $1 " in *" $w "*) echo "$w";; esac; done; }   # MISSING ∩ list, one per line
{ m=$(sel "$MYIP/32"); [ -z "$m" ] || echo "lfc-myip-db $((PRIO_BASE + 1)) $m"
  c=$(sel "${CLASSIC:-}"); [ -z "$c" ] || echo "lfc-classic $((PRIO_BASE + 2))" $c
  sel "$SERVERLESS" | xargs -n 400 | awk -v b="$PRIO_BASE" '{ print "lfc-serverless-" NR-1, b + 9 + NR, $0 }'
} > nsg.plan
cat nsg.plan                       # <rule> <priority> <prefixes...>: show the user before applying
```

Apply. `lfc-` rules that have nothing left to allow are deleted:

```bash
set -a; . ./fw.env
upsert() {  # <rule-name> <priority> <ports> <prefixes...>
  local n=$1 p=$2 ports=$3; shift 3
  if az network nsg rule show -g "$RG" --nsg-name "$NSG" -n "$n" -o none 2>/dev/null; then
    az network nsg rule update -g "$RG" --nsg-name "$NSG" -n "$n" --destination-port-ranges $ports --source-address-prefixes "$@" -o none
  else
    az network nsg rule create -g "$RG" --nsg-name "$NSG" -n "$n" --priority "$p" --direction Inbound --access Allow \
      --protocol Tcp --destination-port-ranges $ports --source-address-prefixes "$@" -o none
  fi
}
upsert lfc-myip-ssh "$PRIO_BASE" 22 "$MYIP/32"
for r in $LFC_RULES; do
  [ "$r" = lfc-myip-ssh ] || grep -q "^$r " nsg.plan || az network nsg rule delete -g "$RG" --nsg-name "$NSG" -n "$r"
done
while read -r name prio prefixes; do upsert "$name" "$prio" "$PORTS" $prefixes; done < nsg.plan
```

For a shared NSG serving workspaces in many regions, set `SCOPE=all` in Step 1 (about 760 CIDRs, two rules).

**Attach** the NSG to each new VM's NIC, or to the subnet the VM is in:

```bash
NSG_ID=$(az network nsg show -g "$RG" -n "$NSG" -o json | jq -r '.id')
az network nic update -g "$RG" -n <vm-nic> --network-security-group "$NSG_ID" -o json | jq -r '.networkSecurityGroup.id'
# or
az network vnet subnet update -g "$RG" --vnet-name <vnet> -n <subnet> --network-security-group "$NSG_ID" -o json | jq -r '.networkSecurityGroup.id'
```

Pass the NSG's full resource ID, and check the ID the command prints ends in `$NSG`. With the bare name, `az network nic update` exited 0 but left the VM's own NSG attached, so the new rules had no effect. A VM created with `az vm create` gets its own NSG on the NIC; attaching the shared one replaces it, so the shared NSG needs the SSH rule (`lfc-myip-ssh` above) before you attach it. Then check from the laptop: `nc -z <vm-ip> <port>`.

Check what it's attached to:

```bash
az network nsg show -g "$RG" -n "$NSG" -o json | yq -p json -o json -r '{"nics": [.networkInterfaces[]?.id], "subnets": [.subnets[]?.id]}'
```

## Why the NSG doesn't front Flexible Server or Azure SQL here

An NSG only filters traffic inside a VNet. A Flexible Server is in a VNet only with **private access** (VNet integration in a delegated subnet), and then, per Microsoft, it "has no public endpoint that's accessible through the internet" and can't use Private Link. Databricks serverless egress comes from Databricks' public ranges, so serverless can't reach such a server at all; only resources in the same or a peered VNet can (for example classic compute in a VNet-injected workspace peered to it).

So for Lakeflow Connect on serverless, create Flexible Servers and Azure SQL with **public access**, and sync the same IP lists into their **server firewall rules** (next section). Use private access behind the shared NSG only when every part of the pipeline runs on classic compute in a peered VNet.

## Flexible Server (Postgres, MySQL) and Azure SQL: server firewall rules

Server firewall rules are **start / end IP ranges**, one CIDR per rule, so each `lfc-` rule is named by `rule_name`. The flags that name the server and the rule differ between services:

| Service | `FW` (command group) | `SRV` (server flag) | `RULE` (rule-name flag) | Rule limit |
|---|---|---|---|---|
| Postgres Flexible Server | `az postgres flexible-server firewall-rule` | `--server-name` | `--name` | 500 |
| MySQL Flexible Server | `az mysql flexible-server firewall-rule` | `--name` | `--rule-name` | 250 |
| Azure SQL Database (logical server) | `az sql server firewall-rule` | `--server` | `--name` | 128 |

Find what other people's rules already allow, and your own `lfc-` rules (Postgres shown; set the three from the table for the others):

```bash
cat >> fw.env <<'EOF'
S=<server>
FW='az postgres flexible-server firewall-rule'
SRV=--server-name
RULE=--name
EOF
set -a; . ./fw.env
eval "$($FW list -g "$RG" $SRV "$S" -o json \
  | yq -p json -o shell '{"HAVE": ([.[] | select(.name | test("^lfc-") | not) | .startIpAddress + "-" + .endIpAddress] | join(" ")),
                          "LFC_RULES": ([.[] | select(.name | test("^lfc-")) | .name] | join(" "))}' \
  | tee -a fw.env)"
eval "$($Q $FW/missing.sql | tee -a fw.env)"
eval "$($Q $FW/plan.sql | tee -a fw.env)"
echo "add: $TO_ADD"; echo "drop: $TO_DROP"
```

Show the user the plan, then apply:

```bash
set -a; . ./fw.env
for r in $TO_DROP; do $FW delete -g "$RG" $SRV "$S" $RULE "$r" --yes; done
for s in $TO_ADD_SPEC; do
  IFS=, read -r _ name first last <<< "$s"
  $FW create -g "$RG" $SRV "$S" $RULE "$name" --start-ip-address "$first" --end-ip-address "$last" -o none
done
```

If a flag or `--yes` is rejected, check `$FW delete --help`: the services differ slightly.

Don't enable "Allow public access from any Azure service" (`0.0.0.0`): it opens the server to every Azure tenant, not just Databricks.

## SQL Managed Instance

The public data endpoint (port 3342) is controlled by the MI subnet's **NSG**, not by server firewall rules. Use the shared-NSG sync above on the MI subnet's NSG, with `PORTS=3342`. (MI requires its own NSG on its subnet; it can be the shared one if it's in the same region.)
