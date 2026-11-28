# Azure SQL Managed Instance Networking Deep Dive

This reference expands on the networking setup for SQL Managed Instance (MI) — specifically the VNet, NSG, and private/public endpoint configuration. See the parent [SKILL.md](../SKILL.md) for the main provisioning steps.

## Overview: Why MI needs more network setup

A Managed Instance must live in a **dedicated `/27` subnet** (32 IP addresses) within a VNet. Azure reserves IPs for MI management, so you cannot share the subnet with other resources. Additionally, MI uses two networking paths:

1. **Private FQDN** (port 1433): only reachable within the VNet or via peering.
2. **Public endpoint** (port 3342): reachable from the internet if the NSG allows it.

This document covers the NSG (Network Security Group), route tables, and public endpoint setup.

## Network Security Group (NSG) rules

The MI subnet needs an NSG attached before the instance is created (`mi.py create` makes `<subnet>-nsg`). MI then adds the inbound and outbound rules it needs for management to that NSG (`Microsoft.Sql-managedInstances_UseOnly_*`).

**Never delete or change MI's own rules**; MI will malfunction. Our allow rules for port 3342 sit next to them and come from **lfc-source-firewall**.

### Adding a rule for the public data endpoint (port 3342)

If `--public-data-endpoint-enabled true`, the MI listens on port 3342. To reach it from outside the VNet, add an inbound NSG rule:

```bash
az network nsg rule create \
  --resource-group <resource_group> \
  --nsg-name <nsg_name> \
  --name "lfc-test-public-data-endpoint" \
  --priority 1100 \
  --protocol Tcp \
  --direction Inbound \
  --source-address-prefixes "<your_cidr>/24" \
  --source-port-ranges "*" \
  --destination-port-ranges "3342" \
  --destination-address-prefix "*" \
  --access Allow
```

Typical source CIDR (developer machine): your public IP as a `/24` or `/32`. For Databricks serverless, use the egress IP range of your workspace.

### Checking existing NSG rules

```bash
az network nsg rule list \
  --resource-group <resource_group> \
  --nsg-name <nsg_name> \
  --query "[].{Name:name, Priority:priority, Protocol:protocol, SourcePrefix:sourceAddressPrefix, DestinationPort:destinationPortRange}" \
  -o table
```

## Route tables

MI may require **user-defined route (UDR) tables** if:
- You have custom routing to on-premises networks.
- You use a VPN gateway or ExpressRoute.
- Traffic must egress through a firewall or proxy.

Default behavior (no UDR): traffic destined for the MI subnet stays within Azure; traffic to external networks uses the Internet Gateway. For Databricks serverless → MI connectivity, the default routing usually works if a private endpoint is configured or if the workspace has an egress IP in the NSG rules.

## Service Endpoints

MI subnets should have service endpoints enabled for:
- `Microsoft.Sql` (MI management)
- `Microsoft.Storage` (backup and configuration storage)

```bash
az network vnet subnet update \
  --resource-group <resource_group> \
  --vnet-name <vnet_name> \
  --name <mi_subnet_name> \
  --service-endpoints Microsoft.Sql Microsoft.Storage
```

Service endpoints are required for MI to function properly. They provide private connectivity from the subnet to Azure services without traversing the internet.

## Private endpoint for MI (alternative to public endpoint)

If you want to reach MI from Databricks without exposing a public endpoint, use a **private endpoint** in a separate subnet:

```bash
az network private-endpoint create \
  --resource-group <resource_group> \
  --name <pfx>-mssql-mi-pe-<epoch> \
  --vnet-name <vnet_name> \
  --subnet <private_endpoint_subnet_name> \
  --private-connection-resource-id "/subscriptions/<subscription_id>/resourceGroups/<resource_group>/providers/Microsoft.Sql/managedInstances/<mi_name>" \
  --group-ids managedInstance \
  --connection-name "<pfx>-mssql-mi-pe-<epoch>-conn"
```

Link the private DNS zone:

```bash
az network private-dns link vnet create \
  --resource-group <resource_group> \
  --zone-name privatelink.database.windows.net \
  --name <pfx>-mssql-mi-dns-link-<epoch> \
  --virtual-network <vnet_name> \
  --registration-enabled false
```

Create DNS A record pointing to the private endpoint's IP:

```bash
eval "$(az network private-endpoint show \
  --resource-group <resource_group> \
  --name <pfx>-mssql-mi-pe-<epoch> -o json \
  | yq -p json -o shell '{"PE_NIC_ID": (.networkInterfaces[0].id // "")}')"

eval "$(az network nic show --ids "$PE_NIC_ID" -o json \
  | yq -p json -o shell '{"PRIVATE_IP": (.ipConfigurations[0].privateIpAddress // "")}')"

az network private-dns record-set a create \
  --resource-group <resource_group> \
  --zone-name privatelink.database.windows.net \
  --name <pfx>-mssql-mi-<location>-<epoch> \
  --ttl 300

az network private-dns record-set a add-record \
  --resource-group <resource_group> \
  --zone-name privatelink.database.windows.net \
  --record-set-name <pfx>-mssql-mi-<location>-<epoch> \
  --ipv4-address "$PRIVATE_IP"
```

After the private endpoint is configured, the private FQDN (port 1433) will route through the private endpoint. You can connect from a VNet-peered Databricks workspace without exposing the public endpoint.

## Troubleshooting connectivity

### Cannot reach MI on port 3342

1. **Check NSG rule:** `az network nsg rule list --resource-group <rg> --nsg-name <nsg>`
2. **Confirm public endpoint is enabled:** `az sql mi show --resource-group <rg> --name <mi_name> --query "publicDataEndpointEnabled"`
3. **Test DNS resolution:** `nslookup <mi_public_fqdn>` (should resolve to an Azure IP)
4. **Test TCP connectivity:** `nc -zv <mi_public_fqdn> 3342` (or use Azure's connection troubleshooter)

### Cannot reach MI on port 1433 (private FQDN)

1. **Verify private endpoint:** `az network private-endpoint show --resource-group <rg> --name <pe_name>`
2. **Check VNet peering:** Databricks VNet → your resource group's VNet (both directions)
3. **Verify DNS resolution:** `nslookup <mi_private_fqdn>` from a machine in the Databricks VNet (should resolve to the private IP of the private endpoint's NIC)

### NSG rule not taking effect

- **Priority conflicts:** NSG rules are evaluated in order. A lower-priority rule (lower number) may deny traffic before your allow rule is checked. List rules and check the order.
- **Rule updates are not immediate:** After creating a rule, wait 30–60 seconds for propagation.

## References

- Azure SQL Managed Instance VNet networking: https://docs.microsoft.com/azure/azure-sql/managed-instance/connectivity-architecture-overview
- Service Endpoints: https://docs.microsoft.com/azure/virtual-network/virtual-network-service-endpoints-overview
- Network Security Groups: https://docs.microsoft.com/azure/virtual-network/network-security-groups-overview
