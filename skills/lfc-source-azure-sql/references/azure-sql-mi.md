# Detailed Steps: Azure SQL Managed Instance

> **Warning:** MI provisioning takes 4–6 hours for the first MI in a new subnet, then 30–60 minutes for additional ones. Do not proceed unless you can wait.

## 1-3. Subnet, NSG, route table, instance: `mi.py create`

[`../scripts/mi.py`](../scripts/mi.py) `create` runs these, in this order (Azure refuses the instance if the subnet lacks any of them):

1. `az network nsg create -n <subnet>-nsg`: the subnet's own NSG, with no rules of ours. MI adds its management rules to it (`Microsoft.Sql-managedInstances_UseOnly_*`); leave those alone.
2. `az network route-table create -n <subnet>-rt` with the route `0.0.0.0/0` to `Internet`.
3. `az network vnet subnet create --address-prefixes 10.10.200.0/27 --network-security-group <subnet>-nsg --route-table <subnet>-rt --delegations Microsoft.Sql/managedInstances` (`/27` or larger, used by nothing else).
4. `az sql mi create --edition GeneralPurpose --family Gen5 --capacity 4 --storage 32GB --license-type LicenseIncluded --public-data-endpoint-enabled true --minimal-tls-version 1.2 --backup-storage-redundancy Local --no-wait`, and `--pricing-model Freemium` for the free offer. (Older examples use `--vcores` and `--enable-minimal-tls-version`; `az` 2.87 has neither.)

`az` takes the admin password only as a flag (`--admin-password`), so it's briefly visible in this machine's process list while the command runs. Run it on a machine you control, and delete `admin.pw` once the UC connection exists.

**Port 3342 stays closed** until the NSG has an allow rule for it: the NSG's default rules deny inbound traffic from the internet. **lfc-source-firewall** adds the rules (target: `<subnet>-nsg`, port 3342), from the same IP list as every other source.

**Monitor progress** in the background (**watch-progress**): `mi.py wait` polls often at first and then less often, for about 6.5 hours, printing the state and the operation's percent complete, so a stalled create shows up early. Exit 0 Succeeded, 1 Failed or Canceled, 2 still provisioning (check the operation in the portal; don't delete and retry). Expected states: `Created` → `Accepted` → `Running` → `Succeeded`.

**The public endpoint:** `mi.py endpoint` prints `<name>.public.<dns zone>.database.windows.net,3342` (the private FQDN with `.public` after the instance name).

## 4. Verify connectivity

```bash
SQLCMDPASSWORD="$(cat admin.pw)" sqlcmd -S "$(python3 ../scripts/mi.py endpoint)" \
  -U sqladmin -Q "SELECT 1;"
```

## 5. Networking: Private endpoint (alternative to public endpoint)

If you want to reach MI from Databricks without exposing the public endpoint, use a **private endpoint** in a separate subnet. See [mi-networking.md](mi-networking.md) for full setup.
