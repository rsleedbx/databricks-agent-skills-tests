#!/usr/bin/env bash
# slot-ssh.sh <owner>: the forced command of one person's SSH key on a shared VM, so the key can run slot.sh only, and
# only as that owner. In ~slot/.ssh/authorized_keys, one line per person:
#   command="/usr/local/sbin/lfc-slot-ssh alice",restrict ssh-ed25519 AAAA... alice
# and in /etc/sudoers.d/lfc-slot: slot ALL=(root) NOPASSWD: /usr/local/sbin/lfc-slot
# The words of the command the person typed (ssh slot@vm up postgres pg1 --from x) become slot.sh's arguments; nothing
# reaches a shell.
set -euo pipefail
read -r -a ARGS <<<"${SSH_ORIGINAL_COMMAND:-ls}"
for a in "${ARGS[@]}"; do [[ $a =~ ^[A-Za-z0-9._-]+$ ]] || { echo "slot: bad argument: $a" >&2; exit 2; }; done
[ "${ARGS[0]}" = --owner ] && { echo "slot: the owner comes from your key" >&2; exit 2; }
exec sudo -n /usr/local/sbin/lfc-slot --owner "$1" "${ARGS[@]}"
