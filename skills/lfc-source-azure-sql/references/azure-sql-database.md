# Detailed Steps: Azure SQL Database

## 1. Create logical server

> `az` takes the admin password only as a flag (`--admin-password`), so it's briefly visible in this machine's process list while the command runs. Run it on a machine you control, and delete `admin.pw` once the UC connection exists.

```bash
umask 077
echo "Lfc1$(openssl rand -hex 12)" > admin.pw

az sql server create \
  --resource-group <resource_group> \
  --name <pfx>-mssql-<location>-<epoch> \
  --location <location> \
  --admin-user sqladmin \
  --admin-password "$(cat admin.pw)" \
  --enable-public-network true \
  --tags owner="<pfx>" purpose="lfc-test" created="$(date -u +'%Y-%m-%d')"

echo "Admin password stored in: $(pwd)/admin.pw"
```

## 2. Create database

```bash
az sql db create \
  --resource-group <resource_group> \
  --server <pfx>-mssql-<location>-<epoch> \
  --name <pfx>_mssql_db_<epoch> \
  --service-objective GP_S_Gen5_1 \
  --collation SQL_Latin1_General_CP1_CI_AS \
  --tags owner="<pfx>" purpose="lfc-test" created="$(date -u +'%Y-%m-%d')"
```

> **Note:** Collation `SQL_Latin1_General_CP1_CI_AS` is case-insensitive and accent-insensitive. For case-sensitive collation, change at database creation time.

## 3. Create private endpoint subnet

```bash
az network vnet subnet create \
  --resource-group <resource_group> \
  --vnet-name lfc-test-vnet \
  --name <pfx>-mssql-pe-subnet-<epoch> \
  --address-prefix 10.10.1.0/24
```

## 4. Create private endpoint

```bash
az network private-endpoint create \
  --resource-group <resource_group> \
  --name <pfx>-mssql-pe-<epoch> \
  --vnet-name lfc-test-vnet \
  --subnet <pfx>-mssql-pe-subnet-<epoch> \
  --private-connection-resource-id "/subscriptions/<subscription_id>/resourceGroups/<resource_group>/providers/Microsoft.Sql/servers/<pfx>-mssql-<location>-<epoch>" \
  --group-ids sqlServer \
  --connection-name "<pfx>-mssql-pe-<epoch>-conn"
```

## 5. Link private DNS zone and create A record

```bash
# Ensure the private DNS zone exists
az network private-dns zone create \
  --resource-group <resource_group> \
  --name privatelink.database.windows.net

# Link the DNS zone to the VNet
az network private-dns link vnet create \
  --resource-group <resource_group> \
  --zone-name privatelink.database.windows.net \
  --name <pfx>-mssql-dns-link-<epoch> \
  --virtual-network lfc-test-vnet \
  --registration-enabled false

# Get the private endpoint's network interface ID
eval "$(az network private-endpoint show \
  --resource-group <resource_group> \
  --name <pfx>-mssql-pe-<epoch> -o json \
  | yq -p json -o shell '{"PE_NIC_ID": (.networkInterfaces[0].id // "")}')"

# Get the private IP address
eval "$(az network nic show --ids "$PE_NIC_ID" -o json \
  | yq -p json -o shell '{"PRIVATE_IP": (.ipConfigurations[0].privateIpAddress // "")}')"

# Create the DNS A record
az network private-dns record-set a create \
  --resource-group <resource_group> \
  --zone-name privatelink.database.windows.net \
  --name <pfx>-mssql-<location>-<epoch> \
  --ttl 300

az network private-dns record-set a add-record \
  --resource-group <resource_group> \
  --zone-name privatelink.database.windows.net \
  --record-set-name <pfx>-mssql-<location>-<epoch> \
  --ipv4-address "$PRIVATE_IP"
```

## 6. Configure firewall (if public endpoint is enabled)

```bash
# Get your current public IP
MY_IP=$(curl -s https://checkip.amazonaws.com)

# Add firewall rule for your IP (SQL Database public endpoint)
az sql server firewall-rule create \
  --resource-group <resource_group> \
  --server <pfx>-mssql-<location>-<epoch> \
  --name "<pfx>-sql-my-ip-<epoch>" \
  --start-ip-address "$MY_IP" \
  --end-ip-address "$MY_IP"

# For Databricks serverless (if you have the egress CIDR range):
az sql server firewall-rule create \
  --resource-group <resource_group> \
  --server <pfx>-mssql-<location>-<epoch> \
  --name "databricks-serverless-egress-slash24" \
  --start-ip-address "198.51.100.0" \
  --end-ip-address "198.51.100.255"
```

(Replace `198.51.100.0/24` with your Databricks workspace egress IP range.)

## 7. Verify connectivity

```bash
SQLCMDPASSWORD="$(cat admin.pw)" sqlcmd -S <pfx>-mssql-<location>-<epoch>.database.windows.net,1433 \
  -U sqladmin -Q "SELECT 1;"
```
