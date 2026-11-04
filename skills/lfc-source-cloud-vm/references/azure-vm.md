# Azure VM Creation

## Variables

```bash
SUBSCRIPTION="<your-subscription-id>"
RESOURCE_GROUP="<resource-group>"     # or the user's own group: they are free to choose
LOCATION="$AZ_REGION"   # the workspace's region, saved once (lfc-test-settings)
ZONE="1"                             # Premium SSD v2 needs a zonal VM
VM_NAME="jane-doe-mysql-vm-eastus-1779207800"
VM_SIZE="Standard_D4s_v5"            # 4 vCPU, 16 GB
SSH_KEY_PATH="$HOME/.ssh/lfc_test_rsa.pub"
OS_DISK_GB="64"
DATA_DISK_GB="128"
```

## Create VM

```bash
# every az call names its subscription: no az account set (it changes the default for every later call)

# Create resource group
az group create --subscription "$SUBSCRIPTION" --name "$RESOURCE_GROUP" --location "$LOCATION"

# Create VM with OS disk + data disk
az vm create --subscription "$SUBSCRIPTION" \
  --resource-group "$RESOURCE_GROUP" \
  --name "$VM_NAME" \
  --image Ubuntu2204 \
  --size "$VM_SIZE" \
  --os-disk-size-gb "$OS_DISK_GB" \
  --os-disk-name "${VM_NAME}-os" \
  --admin-username azureuser \
  --ssh-key-values "$SSH_KEY_PATH" \
  --public-ip-sku Standard \
  --zone "$ZONE" \
  --output table

# Data disk: Premium SSD v2 (3,000 IOPS and 125 MB/s included, set apart from size), in the VM's zone
az disk create --subscription "$SUBSCRIPTION" \
  --resource-group "$RESOURCE_GROUP" \
  --name "${VM_NAME}-data" \
  --location "$LOCATION" \
  --zone "$ZONE" \
  --sku PremiumV2_LRS \
  --size-gb "$DATA_DISK_GB" \
  --disk-iops-read-write 3000 \
  --disk-mbps-read-write 125
az vm disk attach --subscription "$SUBSCRIPTION" \
  --resource-group "$RESOURCE_GROUP" \
  --vm-name "$VM_NAME" \
  --name "${VM_NAME}-data" \
  --lun 0 \
  --caching None

# Get public IP
eval "$(az vm list-ip-addresses \
  --resource-group "$RESOURCE_GROUP" \
  --name "$VM_NAME" -o json \
  | yq -p json -o shell '{"AZURE_PUBLIC_IP": (.[0].virtualMachine.network.publicIpAddresses[0].ipAddress // "")}')"

echo "VM ready at: $AZURE_PUBLIC_IP"
```

**Why Premium SSD v2:** Premium SSD v1 (`Premium_LRS`) ties IOPS to size, and at 128 GiB it's a P10 with 500 IOPS and burst credits that make short tests look fast. The TPC SF 1 masters took up to 5.8 times as long to build on one as on a laptop, and MySQL's TPC-E 3.3 times as long as on AWS's default gp3 (3,000 IOPS). v2 gives the same 3,000 IOPS at any size, no bursting, and lets you raise IOPS or MB/s online (`az disk update --disk-iops-read-write 6000 --disk-mbps-read-write 250`). Its limits: the VM must be in a zone (centralus offers v2 and `Standard_D4s_v5` in zones 1 to 3), host caching must be `None`, and it can't be the OS disk. Measurements in [disk-io-limits.md](disk-io-limits.md). Not yet built end to end on v2: run `fio` (disk-io-limits.md) on the first VM and record it.

Next, for Podman: format the data disk (LUN 0, `/dev/disk/azure/scsi1/lun0`) as XFS with the block in [disk-setup.md](disk-setup.md), before installing anything. Don't use `/dev/sdb`: on sizes with a temporary disk (`Standard_B4ms`, the `ds` sizes) that's the temporary disk, and on `Standard_D4s_v5`, which has none, the letters can still change between boots.

## Add labels

```bash
az vm update \
  --resource-group "$RESOURCE_GROUP" \
  --name "$VM_NAME" \
  --set "tags={purpose=lfc-test,owner=jane-doe,RemoveAfter=2025-02-15}"
```
