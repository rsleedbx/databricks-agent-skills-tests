# cloud-identity.sh: sourced by the lfc-source-* cloud scripts (not run). A shim, kept on purpose: those scripts source
# this path and call az_record and az_require. The logic is in cloud_identity.py; new code calls that file directly.
#   az_record <subscription>                print the record: IDENTITY azure tenant_id=.. subscription_id=.. subscription_name=..
#   az_require <subscription> [tenant_id]   stop (exit 1) unless this az login reaches the subscription in that tenant
az_record() { python3 "$(dirname "${BASH_SOURCE[0]}")/cloud_identity.py" record "$@"; }
az_require() { python3 "$(dirname "${BASH_SOURCE[0]}")/cloud_identity.py" require "$@"; }
