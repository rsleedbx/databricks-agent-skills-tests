#!/usr/bin/env bash
# slot-vm-setup.sh: as root on a new Ubuntu 24.04 VM, make it a slot.sh host with its own JuiceFS volume:
#   podman, fuse3, jq and the JuiceFS client; the data disk as XFS at /var/lib/containers (disk-setup.sh); Redis for the
#   metadata (127.0.0.1 only, unless META_URL names another store); the volume formatted if it's new, mounted at /mnt/jfs at boot (lfc-jfs.service); slot.sh,
#   slot-ssh.sh and the login user `slot` (references/shared-vm-slots.md). Safe to rerun.
# Before it runs: /etc/juicefs/azure.env (mode 600) with AZURE_STORAGE_CONNECTION_STRING for the volume's container
#   (references/juicefs-branches.md, "Access"). Next to it: disk-setup.sh, slot.sh, slot-ssh.sh.
# Settings (environment): VOLUME (the JuiceFS volume name), BUCKET (https://<container>.core.windows.net), HOST (the
#   address slot.sh prints), DISK (default /dev/disk/azure/scsi1/lun0), CACHE_MB (default half the data disk),
#   SLOT_SETTINGS (more lines for /etc/lfc-slot.env, e.g. MAX_PER_OWNER=0), META_URL (a metadata store JuiceFS
#   supports, without the password, e.g. postgres://<role>@<host>:5432/<database>?sslmode=require for a managed
#   Postgres; its password goes in azure.env as META_PASSWORD; default: the local Redis).
#   KIND=personal: slot.sh's state goes on the volume at /mnt/jfs/.state (it comes back with the data when the VM is
#   rebuilt), its lock stays on the VM's disk (LOCK=/var/lib/lfc/lock), and one vCPU per running database
#   (CPUS_PER_DB=1). Any other value: as before.
#   RESTORE_DUMP=<gzipped juicefs dump>: when the metadata store is empty, load this dump into it instead of formatting
#   the volume (a VM rebuilt over the data its volume has in object storage). Without it, an empty store is formatted.
#   JFS_VERSION=<x.y.z>: install this JuiceFS release (when the client isn't installed yet). Unset: the latest release.
#   STORAGE=local: no JuiceFS and no object storage while the VM runs (a cloud without credentials for the VM, e.g. AWS
#   with no instance profile): the databases live on the data disk itself, ROOT=/var/lib/containers (masters/, dbs/,
#   .state/, .secrets/ for the admin env files), and slot.sh clones a master with `cp --reflink`. VOLUME, BUCKET and
#   azure.env aren't used. A disk from the old layout (lfc-data/masters, lfc-data/secrets) is moved into place. rclone
#   and zstd are installed for the copy to object storage the admin runs before the VM is deleted.
#   MASTERS_URL=https://<account>.blob.core.windows.net/<container> with MASTERS (all, a comma list, or none for the admin envs
#   only) and MASTERS_SAS_FILE (a read SAS in a root-only file, mode 600; deleted when this ends): the masters masters-export.sh put
#   there go into ROOT/masters, and every admin env in its manifest into SECRETS (mode 600). `slot-vm-setup.sh masters` with those
#   settings adds them to a VM set up already; on a JuiceFS VM, the same settings with no VOLUME do that too (the volume isn't touched).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
STORAGE=${STORAGE:-juicefs}
case $STORAGE in juicefs|local) ;; *) echo "STORAGE: juicefs or local, not $STORAGE"; exit 1 ;; esac
die() { echo "slot-vm-setup: $*" >&2; exit 1; }
private_file() { local m; m=$(stat -c %a "$1" 2>/dev/null || stat -f %Lp "$1"); case $m in *00) return 0 ;; esac; return 1; }   # no group or other bits

# ---- masters from object storage (masters-export.sh writes them) ----
# a setting from /etc/lfc-slot.env (as slot.sh reads it), else the environment, else the default
lfc_setting() { local v; v=$(sed -n "s/^$1=//p" /etc/lfc-slot.env 2>/dev/null | tail -1); echo "${v:-${!1:-$2}}"; }
masters_paths() {   # MROOT (its masters/ holds the masters) and MSECRETS: slot.sh's ROOT and SECRETS for this storage mode
  if [ "$STORAGE" = local ]; then
    MROOT=$(lfc_setting ROOT /var/lib/containers); MSECRETS=$(lfc_setting SECRETS /var/lib/containers/.secrets)
  else
    MROOT=$(lfc_setting ROOT /mnt/jfs); MSECRETS=$(lfc_setting SECRETS /root/lfc-src)
    findmnt -t fuse.juicefs "$MROOT" >/dev/null 2>&1 || die "$MROOT isn't mounted (lfc-jfs.service): no masters are taken yet"
  fi
  install -d "$MROOT/masters"; install -d -m 700 "$MSECRETS"
}
sha_stdin() { if command -v sha256sum >/dev/null; then sha256sum | cut -c1-64; else shasum -a 256 | cut -c1-64; fi; }
url_cfg() { printf 'url = "%s"\n' "$1"; }   # the URL, with the SAS, reaches curl through -K (a pipe): never on its command line
bl() { local b=$1 q=$2; shift 2; curl -sS --retry 3 -K <(url_cfg "$MU/$b?${q:+$q&}$MSAS") "$@"; }   # bl <blob> <query> curl args...
masters_import() {   # MASTERS, MASTERS_URL, MASTERS_SAS_FILE: the masters and admin envs of the container's manifest
  local n e want f chosen="" seen="" names rc got sha dir stg h code
  : "${MASTERS:?MASTERS=all or a comma list of masters}" "${MASTERS_SAS_FILE:?MASTERS_SAS_FILE=<root-only file with the container SAS>}"
  MU=${MASTERS_URL:?MASTERS_URL=https://<account>.blob.core.windows.net/<container>}; MU=${MU%/}
  [ -f "$MASTERS_SAS_FILE" ] || die "no $MASTERS_SAS_FILE"
  trap 'rm -f "$MASTERS_SAS_FILE"; [ -z "${MT:-}" ] || rm -rf "$MT"; [ -z "${T:-}" ] || rm -rf "$T"' EXIT   # the SAS goes when this ends
  private_file "$MASTERS_SAS_FILE" || die "$MASTERS_SAS_FILE: chmod 600 it (no group or other access)"
  MSAS=$(tr -d ' \r\n' < "$MASTERS_SAS_FILE"); MSAS=${MSAS#\?}
  [ -n "$MSAS" ] || die "$MASTERS_SAS_FILE is empty"
  masters_paths
  MT=$(mktemp -d)
  code=$(curl -sS --retry 3 -K <(url_cfg "$MU/manifest.json?$MSAS") -o "$MT/manifest.json" -w '%{http_code}') || die "no connection to $MU"
  [ "$code" = 200 ] || die "manifest.json: HTTP $code (the SAS needs read)"
  jq -e '.masters and .envs' "$MT/manifest.json" >/dev/null || die "manifest.json has no masters or envs"
  if [ "$MASTERS" = none ]; then names=""
  elif [ "$MASTERS" = all ]; then names=$(jq -r '.masters | keys[]' "$MT/manifest.json")
  else names=$(printf '%s\n' "$MASTERS" | tr ',' '\n'); fi
  for n in $names; do
    [[ $n =~ ^[A-Za-z0-9._-]+$ ]] || die "bad master name: $n"
    jq -e --arg n "$n" '.masters[$n]' "$MT/manifest.json" >/dev/null || die "$n isn't in the manifest"
    chosen="$chosen $n"
  done
  [ "$MASTERS" = none ] || [ -n "$chosen" ] || die "no masters in the manifest"
  # the admin envs of every engine in the manifest, with or without masters (a resume or a create without a master needs them).
  # A different file where one should go would make a clone's password differ from its master's: stop before anything is written
  seen=$(jq -r '.envs[].engine' "$MT/manifest.json" | sort -u | tr '\n' ' ')
  for e in $seen; do
    want=$(jq -r --arg e "$e" '[.envs[] | select(.engine == $e) | .sha256][0] // empty' "$MT/manifest.json")
    f=$MSECRETS/$e.env
    if [ -e "$f" ]; then [ "$(sha_stdin < "$f")" = "$want" ] || die "$f exists with other content: move it aside first"; fi
  done
  for e in $seen; do
    f=$MSECRETS/$e.env; [ -e "$f" ] && continue
    want=$(jq -r --arg e "$e" '[.envs[] | select(.engine == $e) | .sha256][0] // empty' "$MT/manifest.json")
    bl "$e.env" "" -fsS -o "$MT/$e.env" || die "$e.env: download failed"
    [ "$(sha_stdin < "$MT/$e.env")" = "$want" ] || die "$e.env: sha256 doesn't match the manifest"
    install -m 600 "$MT/$e.env" "$f"
  done
  [ -z "$chosen" ] || command -v zstd >/dev/null || DEBIAN_FRONTEND=noninteractive apt-get install -y -qq zstd >/dev/null
  for n in $chosen; do
    dir=$MROOT/masters/$n; e=$(jq -r --arg n "$n" '.masters[$n].engine' "$MT/manifest.json")
    if [ -d "$dir" ]; then jq -cn --arg m "$n" --arg e "$e" '{master: $m, engine: $e, state: "exists"}'; continue; fi
    sha=$(jq -r --arg n "$n" '.masters[$n].sha256' "$MT/manifest.json")
    # the blob streams through sha256 and zstd into a staging folder: it becomes the master only when the sum matches
    stg=$MROOT/masters/.incoming-$n; rm -rf "$stg"; install -d "$stg"; h=$MT/$n.sha
    set +e
    bl "$n.tar.zst" "" -fsS | tee >(sha_stdin > "$h") | zstd -dc -q | tar -x --numeric-owner -C "$stg"
    rc=("${PIPESTATUS[@]}")
    set -e
    wait   # the checksum is written by its own process: done when this returns
    got=$(cut -c1-64 "$h" 2>/dev/null || true)
    if [ "${rc[0]}" != 0 ] || [ "${rc[1]}" != 0 ] || [ "${rc[2]}" != 0 ] || [ "${rc[3]}" != 0 ] || [ "$got" != "$sha" ] || [ ! -d "$stg/$n" ]; then
      rm -rf "${stg:?}"; die "$n: the download didn't verify (sha256 ${got:-none}, manifest $sha, statuses ${rc[*]}): the partial master is removed"
    fi
    mv "$stg/$n" "$dir"; rm -rf "${stg:?}"
    echo "$e" > "$MROOT/masters/$n.engine"
    jq -cn --arg m "$n" --arg e "$e" '{master: $m, engine: $e, state: "imported"}'
  done
}
# masters only, into an existing setup: `slot-vm-setup.sh masters`, or on a JuiceFS VM the same settings with no VOLUME (the volume
# is then never formatted or re-created: the masters and envs are fetched and the mount must be there)
if [ "${1:-}" = masters ] || { [ -n "${MASTERS_URL:-}" ] && [ -z "${VOLUME:-}" ] && [ "$STORAGE" = juicefs ]; }; then masters_import; exit 0; fi

: "${HOST:?HOST=<public address>}"
DISK=${DISK:-/dev/disk/azure/scsi1/lun0}
PERSONAL=0; [ "${KIND:-}" = personal ] && PERSONAL=1
if [ "$STORAGE" = juicefs ]; then
  : "${VOLUME:?VOLUME=<volume name>}" "${BUCKET:?BUCKET=https://<container>.core.windows.net}"
  [ -s /etc/juicefs/azure.env ] || { echo "no /etc/juicefs/azure.env: write the storage connection string first"; exit 1; }
  chmod 600 /etc/juicefs/azure.env
fi
step() { echo "== $*"; }

step "packages (after cloud-init's own)"
cloud-init status --wait >/dev/null 2>&1 || true
export DEBIAN_FRONTEND=noninteractive
command -v podman >/dev/null && command -v jq >/dev/null && dpkg -s fuse3 >/dev/null 2>&1 \
  || { apt-get update -qq && apt-get install -y -qq podman fuse3 jq xfsprogs >/dev/null; }
if [ "$STORAGE" = local ]; then
  command -v rclone >/dev/null && command -v zstd >/dev/null || apt-get install -y -qq rclone zstd >/dev/null
elif ! command -v juicefs >/dev/null; then
  if [ -n "${JFS_VERSION:-}" ]; then V="v${JFS_VERSION#v}"; else V=$(curl -fsSL https://api.github.com/repos/juicedata/juicefs/releases/latest | jq -r .tag_name); fi
  T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT
  (cd "$T" && curl -fsSLO "https://github.com/juicedata/juicefs/releases/download/$V/juicefs-${V#v}-linux-amd64.tar.gz" \
    && curl -fsSLO "https://github.com/juicedata/juicefs/releases/download/$V/checksums.txt" \
    && grep " juicefs-${V#v}-linux-amd64.tar.gz" checksums.txt | sha256sum -c - >/dev/null \
    && tar xzf "juicefs-${V#v}-linux-amd64.tar.gz" juicefs && install juicefs /usr/local/bin/)
fi
[ "$STORAGE" = local ] || juicefs version | head -1

step "data disk at /var/lib/containers"
DISK=$DISK MNT=/var/lib/containers bash "$here/disk-setup.sh"

if [ "$STORAGE" = local ]; then
step "databases on the data disk (no JuiceFS)"
R=/var/lib/containers
xfs_growfs $R >/dev/null   # a disk made larger than the snapshot it came from
install -d -m 755 $R/masters $R/dbs; install -d -m 700 $R/.secrets
for old in masters secrets; do   # a disk from the old layout: same filesystem, so a rename
  [ -d $R/lfc-data/$old ] || continue
  dst=$R/masters; [ $old = secrets ] && dst=$R/.secrets
  for d in $R/lfc-data/$old/*; do [ -e "$d" ] && { [ -e "$dst/$(basename "$d")" ] || mv "$d" "$dst/"; }; done
  rmdir $R/lfc-data/$old 2>/dev/null || true
done
rmdir $R/lfc-data 2>/dev/null || true
[ $PERSONAL = 0 ] || install -d -m 700 $R/.state
systemctl enable --now podman-restart.service >/dev/null 2>&1
else
J=/var/lib/containers/jfs; install -d $J/cache $J/redis /mnt/jfs
CACHE_MB=${CACHE_MB:-$(( $(df -m --output=size /var/lib/containers | tail -1) / 2 ))}

if [ -z "${META_URL:-}" ]; then
step "metadata (Redis)"
podman container exists jfs-redis || podman run -d --name jfs-redis --restart=always -p 127.0.0.1:6379:6379 \
  -v $J/redis:/data docker.io/library/redis:7 redis-server --appendonly yes --appendfsync everysec >/dev/null
systemctl enable --now podman-restart.service >/dev/null 2>&1
for _ in $(seq 1 30); do podman exec jfs-redis redis-cli ping 2>/dev/null | grep -q PONG && break; sleep 2; done
META=redis://127.0.0.1:6379/1
WAIT="ExecStartPre=/bin/sh -c 'for i in \$(seq 1 60); do podman exec jfs-redis redis-cli ping 2>/dev/null | grep -q PONG && exit 0; sleep 2; done; exit 1'"
else
  step "metadata ($(sed -E 's|://.*@|://...@|' <<<"$META_URL"))"
  META=$META_URL WAIT=""
fi
if ! ( set -a; . /etc/juicefs/azure.env; set +a; juicefs status "$META" >/dev/null 2>&1 ); then
  if [ -n "${RESTORE_DUMP:-}" ]; then   # an empty store with the volume's dump: load it, never format over its data
    [ -s "$RESTORE_DUMP" ] || { echo "no dump at $RESTORE_DUMP"; exit 1; }
    T=$(mktemp); zcat "$RESTORE_DUMP" > "$T"; juicefs load "$META" "$T" >/dev/null; rm -f "$T"
    echo "loaded the metadata of $VOLUME from $RESTORE_DUMP"
  else
    ( set -a; . /etc/juicefs/azure.env; set +a; juicefs format --storage wasb --bucket "$BUCKET" --trash-days 0 "$META" "$VOLUME" >/dev/null )
    echo "formatted $VOLUME"
  fi
fi

step "mount at boot (lfc-jfs.service)"
cat > /etc/systemd/system/lfc-jfs.service <<EOF
[Unit]
Description=JuiceFS volume $VOLUME at /mnt/jfs
Wants=network-online.target podman-restart.service
After=network-online.target podman-restart.service

[Service]
Type=simple
EnvironmentFile=/etc/juicefs/azure.env
$WAIT
ExecStart=/usr/local/bin/juicefs mount --writeback --cache-dir $J/cache --cache-size $CACHE_MB -o allow_other --log /var/log/juicefs.log "$META" /mnt/jfs
ExecStop=/usr/local/bin/juicefs umount /mnt/jfs
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload; systemctl enable lfc-jfs >/dev/null 2>&1; systemctl restart lfc-jfs
for _ in $(seq 1 30); do findmnt -t fuse.juicefs /mnt/jfs >/dev/null && break; sleep 2; done
findmnt -t fuse.juicefs /mnt/jfs >/dev/null || { echo "FAIL: /mnt/jfs isn't mounted: journalctl -u lfc-jfs"; exit 1; }
install -d /mnt/jfs/masters /mnt/jfs/dbs
[ $PERSONAL = 0 ] || install -d -m 700 /mnt/jfs/.state
fi

step "slot.sh and the slot user"
install -m 755 "$here/slot.sh" /usr/local/sbin/lfc-slot
install -m 755 "$here/slot-ssh.sh" /usr/local/sbin/lfc-slot-ssh
if [ "$STORAGE" = local ]; then D=/var/lib/containers; else D=/mnt/jfs; fi
{ printf 'HOST=%s\n' "$HOST"; [ -z "${SLOT_SETTINGS:-}" ] || printf '%s\n' "$SLOT_SETTINGS"
  [ "$STORAGE" = juicefs ] || printf 'ROOT=%s\nSECRETS=%s/.secrets\n' $D $D
  [ $PERSONAL = 0 ] || printf 'STATE=%s/.state\nLOCK=/var/lib/lfc/lock\nCPUS_PER_DB=1\n' $D; } > /etc/lfc-slot.env
id slot >/dev/null 2>&1 || useradd -m -s /bin/bash slot
echo 'slot ALL=(root) NOPASSWD: /usr/local/sbin/lfc-slot' > /etc/sudoers.d/lfc-slot; chmod 440 /etc/sudoers.d/lfc-slot
visudo -cq
install -d -o slot -g slot -m 700 ~slot/.ssh
touch ~slot/.ssh/authorized_keys; chown slot:slot ~slot/.ssh/authorized_keys; chmod 600 ~slot/.ssh/authorized_keys
if [ -n "${MASTERS_URL:-}" ]; then step "masters from object storage"; masters_import; fi
if [ "$STORAGE" = local ]; then echo "OK: databases on the data disk at $D (no JuiceFS), lfc-slot installed, HOST=$HOST"
else echo "OK: $VOLUME mounted at /mnt/jfs (cache $CACHE_MB MiB), lfc-slot installed, HOST=$HOST"; fi
