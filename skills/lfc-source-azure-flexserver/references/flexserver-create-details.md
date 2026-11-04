# Detailed Provisioning Steps for Azure Flexible Server

See the main [SKILL.md](../SKILL.md) for prerequisites and quick start.

## Prerequisites

### 1. Azure CLI and authentication

Verify Azure CLI is installed and authenticated:

```bash
az --version
az login
az account show --query "{id:id, name:name}"
```

Save the subscription ID. Name it on every az call with `--subscription {subscription_id}` (and the tenant it's in, `az login --tenant <tenant_id>` if the login doesn't reach it). `az account set` would change the default for every later call, so don't use it.

### 2. Resource provider registration (one-time per subscription)

Check and register the required resource providers:

```bash
az provider register --namespace Microsoft.DBforMySQL
az provider register --namespace Microsoft.DBforPostgreSQL
az provider register --namespace Microsoft.Network
```

Wait for registration (typically 1–2 minutes). Check status:

```bash
az provider show -n Microsoft.DBforMySQL --query registrationState
```

### 3. Region and quota checks

Verify the target region supports the SKU you want:

```bash
# List available MySQL Flexible Server SKUs in your region
az mysql flexible-server sku list -l {location} -o table

# List available PostgreSQL Flexible Server SKUs in your region
az postgres flexible-server sku list -l {location} -o table
```

Check vCPU quota:

```bash
az vm list-usage -l {location} --query \
  "[?contains(name.value, 'vCPU')].{name:name.localizedValue, current:currentValue, limit:limit}"
```

> **Unverified:** Azure sometimes shows vCPU quota higher than actually available. If deployment fails with "quota exceeded," contact Azure support.

### 4. Credential storage

Create a secure location for the admin password (do not hardcode):

```bash
# Generate the admin password into a private file (deleted once the UC connection exists): letters and digits, 28
# characters, as lfc-source-setup's rule (base64 output can hold characters a server refuses)
umask 077; mkdir -p ~/.lfc/pw; echo "Lfc1$(openssl rand -hex 12)" > ~/.lfc/pw/flex-admin.pw
```

## Naming conventions

All resource names follow a pattern derived from your Azure identity:

```
{prefix}-{engine}-{location_slug}-{epoch}
```

- **prefix**: normalized from Azure AD UPN (e.g., `jane.doe@company.com` → `jane-doe`).
- **engine**: `mysql` or `pg`.
- **location_slug**: region with hyphens removed (e.g., `eastus` → `eastus`, `west-europe` → `westeurope`).
- **epoch**: current Unix timestamp (ensures uniqueness across redeployments).

**Example names:**

| Engine | Location | Server name | Database name |
|--------|----------|-------------|---------------|
| MySQL | eastus | `jane-doe-mysql-eastus-1778687039` | `jane_doe_mysql_db_1778687039` |
| PostgreSQL | westeurope | `jane-doe-pg-westeurope-1778687039` | `jane_doe_pg_db_1778687039` |

> **Max length:** Azure server names must be ≤63 characters (DNS label limit). If your prefix is long and epoch adds more characters, the name is truncated with a warning.

## Shared infrastructure

The following resources are **created once and shared** across all users in the same resource group and subscription:

- **Virtual Network (VNet):** `{shared_prefix}-vnet` (e.g., `lfc-test-vnet`)
  - Address space: `10.10.0.0/16`
- **Private DNS zone (MySQL):** `privatelink.mysql.database.azure.com` (fixed by Azure)
- **Private DNS zone (PostgreSQL):** `privatelink.postgres.database.azure.com` (fixed by Azure)
- **VNet links:** Each zone gets a link to the shared VNet (one link per zone, shared)

**Never deleted on teardown.** These are long-lived infrastructure that outlive individual database instances. To clean up (use with caution):

```bash
az network vnet delete -n {shared_prefix}-vnet -g {resource_group}
az network private-dns zone delete -n privatelink.mysql.database.azure.com -g {resource_group}
```

## Create the server: environment variables and steps

### Step 1: Set environment variables

```bash
export LOCATION="eastus"                           # Azure region
export RESOURCE_GROUP="<resource-group>"     # or the user's own group: they are free to choose
eval "$(az account show -o json | yq -p json -o shell '{"SUBSCRIPTION_ID": (.id // "")}')"; export SUBSCRIPTION_ID
export ENGINE="mysql"                             # or "postgres"
export VERSION="8.0.21"                              # MySQL 5.7/8.0 or PostgreSQL 12-16
export PREFIX="jane-doe"                        # Your prefix (from Azure AD UPN)
export EPOCH="$(date +%s)"                        # Unix timestamp
export SERVER_NAME="${PREFIX}-${ENGINE}-${LOCATION}-${EPOCH}"
export DB_NAME="${PREFIX}_${ENGINE}_db_${EPOCH}"
export ADMIN_USER="mysqladmin"                    # For MySQL; use "pgadmin" for PostgreSQL
export SKU="Standard_B1ms"                        # or Standard_B1s, Standard_D2ds_v4, etc.
export STORAGE_GB="32"                            # Minimum
export ENV="dev"                                  # dev, staging, or prod
```

For PostgreSQL, adjust:

```bash
export ENGINE="postgres"
export ADMIN_USER="pgadmin"
export SKU="Standard_B1ms"
```

### Step 2: Ensure resource group

```bash
az group create \
  --name "${RESOURCE_GROUP}" \
  --location "${LOCATION}"
```

### Step 3: Ensure VNet and PE subnet

```bash
VNET_NAME="lfc-test-vnet"
VNET_CIDR="10.10.0.0/16"
PE_SUBNET_NAME="${PREFIX}-${ENGINE}-pe-subnet-${EPOCH}"

# Create VNet (idempotent)
az network vnet create \
  --resource-group "${RESOURCE_GROUP}" \
  --name "${VNET_NAME}" \
  --address-prefix "${VNET_CIDR}" || true

# Create private endpoint subnet (allow PE delegation)
az network vnet subnet create \
  --resource-group "${RESOURCE_GROUP}" \
  --vnet-name "${VNET_NAME}" \
  --name "${PE_SUBNET_NAME}" \
  --address-prefix "10.10.2.0/24" \
  --disable-private-endpoint-network-policies false || true
```

### Step 4: Create Flexible Server

Read the admin password from the secure file and pass it to the create command:

```bash
ADMIN_PASSWORD="$(cat ~/.lfc/pw/flex-admin.pw)"

if [ "${ENGINE}" = "mysql" ]; then
  az mysql flexible-server create \
    --resource-group "${RESOURCE_GROUP}" \
    --name "${SERVER_NAME}" \
    --location "${LOCATION}" \
    --admin-user "${ADMIN_USER}" \
    --admin-password "${ADMIN_PASSWORD}" \
    --sku-name "${SKU}" \
    --tier "Burstable" \
    --storage-size "${STORAGE_GB}" \
    --version "${VERSION}" \
    --backup-retention "1" \
    --storage-auto-grow "Disabled" \
    --public-access "Enabled" \
    --yes

elif [ "${ENGINE}" = "postgres" ]; then
  az postgres flexible-server create \
    --resource-group "${RESOURCE_GROUP}" \
    --name "${SERVER_NAME}" \
    --location "${LOCATION}" \
    --admin-user "${ADMIN_USER}" \
    --admin-password "${ADMIN_PASSWORD}" \
    --sku-name "${SKU}" \
    --tier "Burstable" \
    --storage-size "${STORAGE_GB}" \
    --version "${VERSION}" \
    --backup-retention "1" \
    --storage-auto-grow "Disabled" \
    --public-access "Enabled" \
    --yes
fi

# Wait for server to reach Ready state (~5–15 minutes)
echo "==> Waiting for ${SERVER_NAME} to reach Ready state..."
az resource wait \
  --resource-group "${RESOURCE_GROUP}" \
  --name "${SERVER_NAME}" \
  --resource-type "Microsoft.DBfor${ENGINE^}/flexibleServers" \
  --updated
```

The admin password is in `~/.lfc/pw/flex-admin.pw` (mode 600) for the next steps; delete it once the UC connection exists.
