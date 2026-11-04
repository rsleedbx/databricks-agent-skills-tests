# GCP Compute Engine VM Creation

## Variables

```bash
PROJECT="my-project"
ZONE="us-central1-a"
VM_NAME="jane-doe-mysql-vm-uscentral1-1779207800"
VM_TYPE="n1-standard-4"              # 4 vCPU, 15 GB
SSH_KEY_PATH="$HOME/.ssh/lfc_test_rsa.pub"
IMAGE_FAMILY="ubuntu-2404-lts"       # Ubuntu 24.04
OS_DISK_GB="64"
DATA_DISK_GB="128"
```

## Create VM

```bash
gcloud config set project "$PROJECT"

# Create VM with OS boot disk + data disk
gcloud compute instances create "$VM_NAME" \
  --zone="$ZONE" \
  --machine-type="$VM_TYPE" \
  --image-family="$IMAGE_FAMILY" \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size="${OS_DISK_GB}GB" \
  --boot-disk-type=pd-ssd \
  --create-disk name="${VM_NAME}-data",device-name=data,size="${DATA_DISK_GB}GB",type=pd-ssd \
  --scopes=https://www.googleapis.com/auth/cloud-platform

# Get public IP
eval "$(gcloud compute instances describe "$VM_NAME" \
  --zone="$ZONE" --format=json \
  | yq -p json -o shell '{"GCP_PUBLIC_IP": (.networkInterfaces[0].accessConfigs[0].natIP // "")}')"

echo "VM ready at: $GCP_PUBLIC_IP"
```

Next, for Podman: format the data disk (`/dev/disk/by-id/google-data`) as XFS with the block in [disk-setup.md](disk-setup.md), before installing anything.

## Add labels

```bash
gcloud compute instances add-labels "$VM_NAME" \
  --zone="$ZONE" \
  --labels=purpose=lfc-test,owner=jane-doe,remove_after=2025-02-15
```

## Add firewall tag for database access

```bash
gcloud compute instances add-tags "$VM_NAME" \
  --zone="$ZONE" \
  --tags="allow-db"
```
