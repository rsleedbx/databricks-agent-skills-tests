#!/usr/bin/env bash
# cloud-links.sh <vm public ip> [--subscription <id>] [--aws-profile <p>] [--region <aws region>] [--gcp-account <a>] [--gcp-project <p>] [--resource-id <azure id>]...
#   Clickable links, for inspection, to a test VM and what goes with it, in whichever cloud has that IP. Each cloud is
#   searched only with an identity given for it, never with the CLI's default: the identity the VM was created with (its
#   record). Azure: SUBSCRIPTION or --subscription (else every enabled subscription, by id). AWS: AWS_PROFILE or
#   --aws-profile, and AWS_REGION or --region. GCP: GCP_ACCOUNT or --gcp-account, and GCP_PROJECT or --gcp-project. A cloud
#   with no identity is skipped with the flag to give. One line per resource: "LINK <what>: <url>". Where the VM is found,
#   one "IDENTITY <cloud> ..." line: the record to keep for later calls (see lfc-source-setup cloud-identity.sh).
#     Azure: the VM, its NIC, the network security group (on the NIC or its subnet), the public IP, the OS and data disks
#     AWS:   the instance, its security groups, its EBS volumes
#     GCP:   the instance, its disks, and the VPC firewall rules
#   --resource-id adds the portal link for any other Azure resource, for example the storage account of a master
#   snapshot (bk.env SA_ID). Read-only. Exit 1 when no searched cloud had the IP.
set -uo pipefail
IP=${1:?vm public ip}; shift
SUBS=${SUBSCRIPTION:-${AZURE_SUBSCRIPTION_ID:-}}; PROF=${AWS_PROFILE:-}; REGION=${AWS_REGION:-}; GACC=${GCP_ACCOUNT:-}; GPRJ=${GCP_PROJECT:-}
EXTRA=()
while [ $# -gt 0 ]; do case $1 in
  --subscription) SUBS="$2 $SUBS"; shift 2;;
  --aws-profile) PROF=$2; shift 2;;
  --region) REGION=$2; shift 2;;
  --gcp-account) GACC=$2; shift 2;;
  --gcp-project) GPRJ=$2; shift 2;;
  --resource-id) EXTRA+=("$2"); shift 2;;
  *) echo "unknown $1" >&2; exit 2;;
esac; done
export AZURE_DEFAULTS_GROUP=
. "$(dirname "$0")/../../lfc-source-setup/scripts/cloud-identity.sh"
found=0
link() { echo "LINK $1: $2"; found=1; }
skip() { echo "skip $1: $2" >&2; }

# --- Azure: portal links are https://portal.azure.com/#@<tenant>/resource<resource id>
if command -v az >/dev/null && az account list >/dev/null 2>&1; then
  for id in ${EXTRA[@]+"${EXTRA[@]}"}; do link "Azure $(basename "$id")" "https://portal.azure.com/#@$(az account show --subscription "$(echo "$id" | awk -F/ '{print $3}')" --query tenantId -o tsv 2>/dev/null)/resource$id"; done
  for id in ${EXTRA[@]+"${EXTRA[@]}"}; do SUBS="$SUBS $(echo "$id" | awk -F/ '{print $3}')"; done
  # the given subscriptions; with none, every enabled one (a search by IP, each named by its id)
  [ -n "${SUBS// /}" ] || { echo "note: no SUBSCRIPTION given, searching every enabled subscription" >&2; SUBS=$(az account list --query "[?state=='Enabled'].id" -o tsv); }
  pipid=; cfg=; sub=
  for s in $SUBS; do
    pip=$(az network public-ip list --subscription "$s" --query "[?ipAddress=='$IP'] | [0].{id:id, cfg:ipConfiguration.id}" -o json 2>/dev/null)
    pipid=$(echo "$pip" | jq -r '.id // ""' 2>/dev/null); cfg=$(echo "$pip" | jq -r '.cfg // ""' 2>/dev/null)
    [ -n "$pipid" ] && { sub=$s; break; }
  done
  if [ -n "$pipid" ]; then
    TEN=$(az account show --subscription "$sub" --query tenantId -o tsv); AZ="https://portal.azure.com/#@$TEN/resource"
    az_record "$sub"
    link "Azure public IP $IP" "$AZ$pipid"
    nic=${cfg%/ipConfigurations/*}
    if [ -n "$nic" ]; then
      link "Azure NIC $(basename "$nic")" "$AZ$nic"
      eval "$(az network nic show --ids "$nic" -o json | yq -p json -o shell '{"VM": (.virtualMachine.id // ""), "NSG": (.networkSecurityGroup.id // ""), "SUBNET": (.ipConfigurations[0].subnet.id // "")}')"
      [ -z "$NSG" ] && [ -n "$SUBNET" ] && NSG=$(az network vnet subnet show --ids "$SUBNET" --query networkSecurityGroup.id -o tsv 2>/dev/null)
      [ -n "$NSG" ] && link "Azure network security group $(basename "$NSG")" "$AZ$NSG"
      if [ -n "$VM" ]; then
        link "Azure VM $(basename "$VM")" "$AZ$VM"
        az vm show --ids "$VM" --query "[storageProfile.osDisk.managedDisk.id, storageProfile.dataDisks[].managedDisk.id][]" -o tsv 2>/dev/null \
          | while read -r d; do [ -n "$d" ] && link "Azure disk $(basename "$d")" "$AZ$d"; done
      fi
    fi
  fi
fi

# --- AWS: the EC2 console, per region, with the profile the VM was made with
if command -v aws >/dev/null; then
  if [ -z "$PROF" ] || [ -z "$REGION" ]; then skip AWS "needs AWS_PROFILE and AWS_REGION (or --aws-profile, --region): the VM's record"
  elif acct=$(aws sts get-caller-identity --profile "$PROF" --query Account --output text 2>/dev/null); then
    j=$(aws ec2 describe-instances --profile "$PROF" --region "$REGION" --filters "Name=ip-address,Values=$IP" --query 'Reservations[0].Instances[0]' -o json 2>/dev/null)
    iid=$(echo "$j" | jq -r '.InstanceId // ""' 2>/dev/null)
    if [ -n "$iid" ] && [ "$iid" != null ]; then
      echo "IDENTITY aws profile=$PROF account_id=$acct region=$REGION"
      E="https://$REGION.console.aws.amazon.com/ec2/home?region=$REGION"
      link "AWS instance $iid" "$E#InstanceDetails:instanceId=$iid"
      echo "$j" | yq -p json -o tsv '.SecurityGroups[].GroupId' | while read -r g; do link "AWS security group $g" "$E#SecurityGroup:groupId=$g"; done
      echo "$j" | yq -p json -o tsv '.BlockDeviceMappings[].Ebs.VolumeId' | while read -r v; do link "AWS volume $v" "$E#VolumeDetails:volumeId=$v"; done
    fi
  else skip AWS "profile $PROF can't log in: aws sso login --profile $PROF"; fi
fi

# --- GCP: the Cloud console, per project, with the account and project the VM was made with
if command -v gcloud >/dev/null; then
  if [ -z "$GACC" ] || [ -z "$GPRJ" ]; then skip GCP "needs GCP_ACCOUNT and GCP_PROJECT (or --gcp-account, --gcp-project): the VM's record"
  else
    j=$(gcloud compute instances list --account "$GACC" --project "$GPRJ" --filter="networkInterfaces[0].accessConfigs[0].natIP=$IP" --format=json 2>/dev/null)
    name=$(echo "$j" | jq -r '.[0].name // ""' 2>/dev/null)
    if [ -n "$name" ] && [ "$name" != null ]; then
      echo "IDENTITY gcp account=$GACC project=$GPRJ"
      zone=$(echo "$j" | jq -r '.[0].zone' | awk -F/ '{print $NF}'); C="https://console.cloud.google.com"
      link "GCP instance $name" "$C/compute/instancesDetail/zones/$zone/instances/$name?project=$GPRJ"
      echo "$j" | yq -p json -o tsv '.[0].disks[].source' | awk -F/ '{print $NF}' | while read -r d; do link "GCP disk $d" "$C/compute/disksDetail/zones/$zone/disks/$d?project=$GPRJ"; done
      link "GCP firewall rules" "$C/net-security/firewall-manager/firewall-policies/list?project=$GPRJ"
    fi
  fi
fi
[ "$found" = 1 ] || { echo "no searched cloud has a VM with $IP (identities given above; a skipped cloud wasn't searched)"; exit 1; }
