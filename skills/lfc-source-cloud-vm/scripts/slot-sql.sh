#!/usr/bin/env bash
# slot-sql.sh [database]: run SQL from stdin as the engine's admin in your own database on a source VM (slot.sh,
# lfc-source-cloud-vm references/shared-vm-slots.md), through your slot key: no VM admin login needed, and only your
# own databases are reachable. The SRC_SQL of the harness for a database from slot.sh up.
#   Needs SLOT_HOST (the VM's address: host in slot.sh up's JSON) and SLOT_NAME (the database's name); optional SLOT_KEY
#   (~/.ssh/lfc_slot_ed25519). SQLCMD_FLAGS with -y (lfc-source-datagen fill.sh's "-b -y 0") asks for long values whole.
#   SLOT_OWNER (with SLOT_ADMIN_KEY, optional SLOT_ADMIN_USER, default azureuser): a database of another owner (a test run
#   for someone), through the VM admin login: sudo lfc-slot --owner <owner> sql ... instead of your slot key.
#   slot-sql.sh --env prints the host OS, the Podman version, the image and digest, and the VM instead (SRC_ENV).
set -euo pipefail
: "${SLOT_HOST:?set SLOT_HOST}" "${SLOT_NAME:?set SLOT_NAME}"
# The admin route's login shell parses the remote command line (no forced command there): plain names only, both routes.
for w in "$SLOT_NAME" ${1:+"$1"}; do
  case $w in --env) ;; *[!A-Za-z0-9_.-]*|"") echo "slot-sql: a database name is letters, digits, _ . -: $w" >&2; exit 2 ;; esac
done
if [ -n "${SLOT_OWNER:-}" ]; then
  case $SLOT_OWNER in *[!a-z0-9-]*) echo "slot-sql: SLOT_OWNER is an owner name (a-z, 0-9, -): $SLOT_OWNER" >&2; exit 2 ;; esac
  : "${SLOT_ADMIN_KEY:?SLOT_OWNER needs SLOT_ADMIN_KEY (the VM admin login)}"
  SSH=(ssh -i "$SLOT_ADMIN_KEY" -o BatchMode=yes -o ConnectTimeout=15 "${SLOT_ADMIN_USER:-azureuser}@$SLOT_HOST"
       sudo /usr/local/sbin/lfc-slot --owner "$SLOT_OWNER")
else
  SSH=(ssh -i "${SLOT_KEY:-$HOME/.ssh/lfc_slot_ed25519}" -o BatchMode=yes -o ConnectTimeout=15 "slot@$SLOT_HOST")
fi
if [ "${1:-}" = --env ]; then
  exec "${SSH[@]}" info "$SLOT_NAME"
fi
WIDE=""; case ${SQLCMD_FLAGS:-} in *-y*) WIDE=--wide ;; esac
# ssh exits 255 when it can't connect (a dropped VPN, the laptop's IP changing): retry with the same input, so a step
# isn't silently skipped. Any other exit status is the SQL's own and is returned as is.
IN=$(cat); rc=255
for wait in 5 15 30 60 120; do
  rc=0; printf '%s\n' "$IN" | "${SSH[@]}" sql "$SLOT_NAME" ${1:+"$1"} ${WIDE:+"$WIDE"} || rc=$?
  [ $rc -ne 255 ] && exit $rc
  echo "ssh to $SLOT_HOST failed; retrying in ${wait}s" >&2; sleep "$wait"
done
exit $rc
