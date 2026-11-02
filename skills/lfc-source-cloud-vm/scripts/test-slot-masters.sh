#!/usr/bin/env bash
# test-slot-masters.sh: offline checks of masters-export.sh, slot-vm-setup.sh masters, and the admin password change of a
#   database cloned from a master in slot.sh. Stubs only: a local Blob-style HTTP server (Put Blob, Put Block, Put Block List,
#   Get, Head), a fake podman that keeps each container's password and logs its calls, a curl that logs its argv (the SAS must
#   not be on it), and fake timeout, ss, findmnt, juicefs and flock. No VM, no containers, no object storage. One PASS/FAIL line
#   per check; exit 1 on any FAIL.
# bash32: exempt (runs the VM scripts with the bash on PATH)
# shellcheck disable=SC2034   # variables are read by the check strings that ok() evals
set -uo pipefail
S=$(cd "$(dirname "$0")" && pwd); T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT; fail=0
ok() { if eval "$2"; then echo "PASS $1"; else echo "FAIL $1"; fail=1; fi; }
SAS_SIG=TESTSIG
mkdir -p "$T/bin" "$T/fp/bin" "$T/fp/run" "$T/fp/db" "$T/fp/envs" "$T/fp/eng" "$T/blob"
REAL_CURL=$(command -v curl)
export REAL_CURL ARGV_LOG=$T/argv.log PATH="$T/bin:$PATH"
cat > "$T/bin/curl" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$ARGV_LOG"
exec "$REAL_CURL" "$@"
EOF
chmod +x "$T/bin/curl"

# ---- a Blob-style server: blobs are files under $T/blob/box; the requests are logged; CORRUPT names get one byte flipped
cat > "$T/server.py" <<'PY'
import hashlib, http.server, os, re, shutil, sys, urllib.parse
ROOT, PORT, CORRUPT = sys.argv[1], int(sys.argv[2]), set(sys.argv[3:])
os.makedirs(os.path.join(ROOT, 'box'), exist_ok=True)
def safe(bid):
    return bid.replace('/', '_').replace('+', '-')
class H(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self, *a):
        pass
    def target(self):
        u = urllib.parse.urlsplit(self.path)
        with open(os.path.join(ROOT, 'requests.log'), 'a') as f:
            f.write('%s %s\n' % (self.command, self.path))
        return u, os.path.join(ROOT, u.path.lstrip('/'))   # /box/<blob>
    def reply(self, code, length=0):
        self.send_response(code); self.send_header('Content-Length', str(length)); self.end_headers()
    def do_GET(self):
        u, p = self.target()
        if not os.path.isfile(p):
            return self.reply(404)
        data = open(p, 'rb').read()
        if os.path.basename(p) in CORRUPT and len(data) > 20:
            data = data[:10] + bytes([data[10] ^ 0xff]) + data[11:]
        self.reply(200, len(data)); self.wfile.write(data)
    def do_HEAD(self):
        u, p = self.target()
        if not os.path.isfile(p):
            return self.reply(404)
        self.reply(200, os.path.getsize(p))
    def do_PUT(self):
        u, p = self.target()
        q = urllib.parse.parse_qs(u.query)
        if self.headers.get('Transfer-Encoding') or self.headers.get('Content-Length') is None:
            return self.reply(411)                     # the uploads must say how long they are
        body = self.rfile.read(int(self.headers['Content-Length']))
        if q.get('comp') == ['block']:
            d = p + '.blocks'; os.makedirs(d, exist_ok=True)
            open(os.path.join(d, safe(q['blockid'][0])), 'wb').write(body)
            return self.reply(201)
        if q.get('comp') == ['blocklist']:
            d = p + '.blocks'
            with open(p, 'wb') as out:
                for bid in re.findall(r'<Latest>(.*?)</Latest>', body.decode()):
                    out.write(open(os.path.join(d, safe(bid)), 'rb').read())
            shutil.rmtree(d, ignore_errors=True)
            return self.reply(201)
        if self.headers.get('x-ms-blob-type') is None:
            return self.reply(400)
        open(p, 'wb').write(body)
        self.reply(201)
http.server.ThreadingHTTPServer(('127.0.0.1', PORT), H).serve_forever()
PY
PORT=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
serve() {   # serve [corrupt blob names]: (re)start the server on $PORT, and wait until it answers
  [ -z "${SRV:-}" ] || { kill "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; }
  python3 -I "$T/server.py" "$T/blob" "$PORT" "$@" > "$T/server.out" 2>&1 &
  SRV=$!
  for _ in $(seq 1 100); do curl -s -o /dev/null "http://127.0.0.1:$PORT/" && break; sleep 0.1; done
}
serve
trap 'kill "${SRV:-0}" 2>/dev/null; rm -rf "${T:?}"' EXIT

# ---- fixtures: two masters with their engines, the admin envs, the SAS (mode 600: the export refuses anything else)
mkdir -p "$T/root/masters/alpha/data" "$T/root/masters/beta" "$T/secrets"
head -c 3145728 /dev/urandom > "$T/root/masters/alpha/data/rand.bin"      # over 2 MiB: the export's block path (MASTERS_PUT_BLOB_MAX_MB=2)
echo hello > "$T/root/masters/alpha/PG_VERSION"; ln -s PG_VERSION "$T/root/masters/alpha/link"
echo x > "$T/root/masters/beta/f"
echo postgres > "$T/root/masters/alpha.engine"; echo mysql > "$T/root/masters/beta.engine"
printf 'POSTGRES_PASSWORD=Lfc1aaaa0000000000000001\n' > "$T/secrets/postgres.env"
printf 'MYSQL_ROOT_PASSWORD=Lfc1bbbb0000000000000002\n' > "$T/secrets/mysql.env"
chmod 600 "$T/secrets/"*.env
printf 'sv=2023-11-03&sig=%s\n' "$SAS_SIG" > "$T/sas"; chmod 600 "$T/sas"
MU="http://127.0.0.1:$PORT/box"
export_run() {   # the masters of $MASTERS (default all) exported to the box, blocks of 1 MiB past 2 MiB
  env ROOT="$T/root" SECRETS="$T/secrets" EXPORT_TMP="$T/tmp" MASTERS_URL="$MU" MASTERS_SAS_FILE="$T/sas" \
    MASTERS_BLOCK_MB=1 MASTERS_PUT_BLOB_MAX_MB=2 MASTERS="${MASTERS:-all}" bash "$S/masters-export.sh"
}
import_run() {   # import_run <root> <secrets> [masters]: a VM's masters and envs from the box; the SAS is copied (the import deletes it)
  cp "$T/sas" "$T/sas.use"; chmod 600 "$T/sas.use"
  env STORAGE=local ROOT="$1" SECRETS="$2" MASTERS_URL="$MU" MASTERS_SAS_FILE="$T/sas.use" MASTERS="${3:-all}" \
    bash "$S/slot-vm-setup.sh" masters
}
private600() { [ "$(stat -c %a "$1" 2>/dev/null || stat -f %Lp "$1")" = 600 ]; }

echo "== masters-export.sh and slot-vm-setup.sh masters"
: > "$T/argv.log"
out1=$(export_run 2>"$T/exp1.err"); rc1=$?
ok "export: both masters uploaded, rc 0 ($(tr '\n' ' ' < "$T/exp1.err" | cut -c1-200))" '[ $rc1 = 0 ] && [ "$(grep -c "\"state\":\"uploaded\"" <<<"$out1")" = 2 ]'
ok "export: the manifest has the engine, bytes, sha256 and file count of each master" \
  '[ "$(jq -r .masters.alpha.engine "$T/blob/box/manifest.json")" = postgres ] && [ "$(jq -r .masters.beta.engine "$T/blob/box/manifest.json")" = mysql ] && [ "$(jq .masters.alpha.files "$T/blob/box/manifest.json")" = 2 ]'
ok "export: a stored archive's sha256 and size are the manifest's (the block path reassembled it)" \
  '[ "$(sha256sum < "$T/blob/box/alpha.tar.zst" 2>/dev/null | cut -c1-64 || shasum -a 256 < "$T/blob/box/alpha.tar.zst" | cut -c1-64)" = "$(jq -r .masters.alpha.sha256 "$T/blob/box/manifest.json")" ] || [ "$(shasum -a 256 < "$T/blob/box/alpha.tar.zst" | cut -c1-64)" = "$(jq -r .masters.alpha.sha256 "$T/blob/box/manifest.json")" ]'
ok "export: the archive is zstd of a tar that holds the master, symlink and all" \
  '[ "$(zstd -dc "$T/blob/box/alpha.tar.zst" | tar -t | grep -c "^alpha/")" -ge 3 ] && zstd -dc "$T/blob/box/alpha.tar.zst" | tar -t | grep -qx "alpha/link"'
ok "export: the big master went in blocks, the small one in one Put Blob" \
  'grep -q "PUT /box/alpha.tar.zst?comp=block" "$T/blob/requests.log" && ! grep -q "PUT /box/beta.tar.zst?comp=block" "$T/blob/requests.log"'
ok "export: each engine's admin env is in the manifest with its sha256, and in the box" \
  '[ "$(jq -r ".envs[] | select(.engine == \"postgres\") | .sha256" "$T/blob/box/manifest.json")" = "$(shasum -a 256 "$T/secrets/postgres.env" | cut -c1-64)" ] && cmp -s "$T/secrets/postgres.env" "$T/blob/box/postgres.env"'
ok "export: the temporary archive is gone afterwards" '[ -z "$(ls -A "$T/tmp" 2>/dev/null | grep -v "^run\\.")" ] && [ -z "$(find "$T/tmp" -name "*.tar.zst" 2>/dev/null)" ]'
mark=$(wc -l < "$T/blob/requests.log")
out2=$(export_run 2>/dev/null); rc2=$?
ok "export again: both skipped (same sha256), rc 0" '[ $rc2 = 0 ] && [ "$(grep -c "\"state\":\"skipped\"" <<<"$out2")" = 2 ]'
ok "export again: no archive or env uploaded (only the manifest)" \
  '! tail -n +$((mark + 1)) "$T/blob/requests.log" | grep -q "PUT /box/[a-z]*\.\(tar\.zst\|env\)"'
ok "the SAS never reached a command line (curl's argv has its config, not the URL)" \
  '! grep -q "$SAS_SIG" "$ARGV_LOG" && grep -q -- "-K" "$ARGV_LOG"'
printf 'sv=2023-11-03&sig=%s\n' "$SAS_SIG" > "$T/sas.open"; chmod 644 "$T/sas.open"
err=$(env ROOT="$T/root" SECRETS="$T/secrets" MASTERS_URL="$MU" MASTERS_SAS_FILE="$T/sas.open" bash "$S/masters-export.sh" 2>&1 >/dev/null); rc=$?
ok "export refuses a SAS file that others can read" '[ $rc != 0 ] && [[ $err == *"chmod 600"* ]]'

import_run "$T/vm1" "$T/vm1s" > "$T/imp1.out" 2>"$T/imp1.err"; rc=$?
ok "import: both masters imported, rc 0 ($(tr '\n' ' ' < "$T/imp1.err" | cut -c1-160))" '[ $rc = 0 ] && [ "$(grep -c "\"state\":\"imported\"" "$T/imp1.out")" = 2 ]'
ok "import: the masters are the exported trees (symlink and all), and their engine files" \
  'diff -r "$T/root/masters/alpha" "$T/vm1/masters/alpha" && diff -r "$T/root/masters/beta" "$T/vm1/masters/beta" && [ "$(cat "$T/vm1/masters/alpha.engine")" = postgres ]'
ok "import: the admin envs are in SECRETS, mode 600, with the master's content" \
  'cmp -s "$T/secrets/postgres.env" "$T/vm1s/postgres.env" && cmp -s "$T/secrets/mysql.env" "$T/vm1s/mysql.env" && private600 "$T/vm1s/postgres.env"'
ok "import: no staging folder is left, and the SAS file is deleted" '[ -z "$(ls -A "$T/vm1/masters" | grep incoming)" ] && [ ! -e "$T/sas.use" ]'
ok "import: the SAS never reached a command line" '! grep -q "$SAS_SIG" "$ARGV_LOG"'
import_run "$T/vm1" "$T/vm1s" > "$T/imp2.out" 2>/dev/null; rc=$?
ok "import again (adding masters later): both exist, nothing changed, rc 0" '[ $rc = 0 ] && [ "$(grep -c "\"state\":\"exists\"" "$T/imp2.out")" = 2 ]'
import_run "$T/vm1" "$T/vm1s" nope > /dev/null 2>"$T/imp3.err"; rc=$?
ok "import of a name the manifest doesn't have: stops, rc 1" '[ $rc != 0 ] && grep -q "isn.t in the manifest" "$T/imp3.err"'

import_run "$T/vm4" "$T/vm4s" none > "$T/imp6.out" 2>"$T/imp6.err"; rc=$?
ok "import with MASTERS=none: no master, but every admin env of the manifest comes (mode 600), rc 0" \
  '[ $rc = 0 ] && [ -z "$(ls -A "$T/vm4/masters")" ] && cmp -s "$T/secrets/postgres.env" "$T/vm4s/postgres.env" && cmp -s "$T/secrets/mysql.env" "$T/vm4s/mysql.env" && private600 "$T/vm4s/mysql.env"'

mkdir -p "$T/vm2s"; printf 'POSTGRES_PASSWORD=Lfc1cccc0000000000000003\n' > "$T/vm2s/postgres.env"; chmod 600 "$T/vm2s/postgres.env"
import_run "$T/vm2" "$T/vm2s" > /dev/null 2>"$T/imp4.err"; rc=$?
ok "import: an admin env with other content is never overwritten: stops before any master" \
  '[ $rc != 0 ] && [[ $(cat "$T/imp4.err") == *"exists with other content"* ]] && [ ! -e "$T/vm2/masters/alpha" ] && cmp -s "$T/vm2s/postgres.env" <(printf "POSTGRES_PASSWORD=Lfc1cccc0000000000000003\n")'

serve beta.tar.zst
import_run "$T/vm3" "$T/vm3s" beta > /dev/null 2>"$T/imp5.err"; rc=$?
ok "import: a download that doesn't match its sha256 is removed and fails" \
  '[ $rc != 0 ] && [[ $(cat "$T/imp5.err") == *"partial master is removed"* ]] && [ ! -e "$T/vm3/masters/beta" ] && [ -z "$(ls -A "$T/vm3/masters" | grep incoming)" ]'
ok "import: the mismatched run still deletes the SAS file" '[ ! -e "$T/sas.use" ]'
serve

cp "$T/sas" "$T/sas.u6"; chmod 600 "$T/sas.u6"
env -u MASTERS STORAGE=local ROOT="$T/vm6" SECRETS="$T/vm6s" MASTERS_URL="$MU" MASTERS_SAS_FILE="$T/sas.u6" bash "$S/slot-vm-setup.sh" masters > "$T/imp9.out" 2>"$T/imp9.err"; rc=$?
ok "import with MASTERS unset: every master of the manifest (as masters-export.sh's default), rc 0" \
  '[ $rc = 0 ] && [ "$(grep -c "\"state\":\"imported\"" "$T/imp9.out")" = 2 ] && diff -r "$T/root/masters/alpha" "$T/vm6/masters/alpha" >/dev/null'

# a checksum process that writes its sum only after the download ends (a slow sha256sum stub): the sum must be complete when it's read
mkdir -p "$T/slow/bin"; REAL_SUM=$(if command -v sha256sum >/dev/null; then command -v sha256sum; else echo "/usr/bin/shasum -a 256"; fi)   # the absolute path: the stub shadows the name
cat > "$T/slow/bin/sha256sum" <<EOF
#!/usr/bin/env bash
tmp=\$(mktemp); cat > "\$tmp"; sleep 1; $REAL_SUM < "\$tmp"; rm -f "\$tmp"
EOF
chmod +x "$T/slow/bin/sha256sum"
cp "$T/sas" "$T/sas.slow"; chmod 600 "$T/sas.slow"
env PATH="$T/slow/bin:$PATH" STORAGE=local ROOT="$T/vm7" SECRETS="$T/vm7s" MASTERS_URL="$MU" MASTERS_SAS_FILE="$T/sas.slow" MASTERS=beta \
  bash "$S/slot-vm-setup.sh" masters > "$T/slow.out" 2>"$T/slow.err"; rc=$?
ok "a slow checksum (its sum written after the download ends) still verifies: beta imported, rc 0 ($(tr '\n' ' ' < "$T/slow.err" | cut -c1-160))" \
  '[ $rc = 0 ] && [ "$(grep -c "\"state\":\"imported\"" "$T/slow.out")" = 1 ] && diff -r "$T/root/masters/beta" "$T/vm7/masters/beta" >/dev/null'

# a full setup with MASTERS_URL: the SAS file is checked before any setup step (nothing printed, nothing run)
out=$(env -u MASTERS_SAS_FILE STORAGE=local HOST=192.0.2.9 MASTERS_URL="$MU" bash "$S/slot-vm-setup.sh" 2>"$T/early.err"); rc=$?
ok "a full setup with MASTERS_URL and no MASTERS_SAS_FILE: stops before the first step, rc 1, names the setting" \
  '[ $rc != 0 ] && [ -z "$out" ] && [[ $(cat "$T/early.err") == *MASTERS_SAS_FILE* ]]'
printf 'sv=x\n' > "$T/sas.open2"; chmod 644 "$T/sas.open2"
out=$(env STORAGE=local HOST=192.0.2.9 MASTERS_URL="$MU" MASTERS_SAS_FILE="$T/sas.open2" bash "$S/slot-vm-setup.sh" 2>"$T/early2.err"); rc=$?
ok "a full setup with a SAS file others can read: stops before the first step, rc 1" \
  '[ $rc != 0 ] && [ -z "$out" ] && [[ $(cat "$T/early2.err") == *"chmod 600"* ]]'

# ---- slot.sh: the admin password of a clone changes once, at its first start
# the fake podman: a container runs while $FT/run/<name> exists; its env file at start is kept in $FT/envs/<name>; its engine
# (the lfc.engine label) in $FT/eng/<name>; its admin password is in $FT/db/<name> (the one in its env file at its first
# start). Each engine's change is checked the way slot.sh's rotate_change sends it: the old password from the stdin the
# command reads (Oracle's CONNECT, Informix's NUL-terminated unix_chkpwd input, Db2's connect file, MYSQL_PWD for the client
# the engine names); the new one is recorded from the ALTER or chpasswd that carries it. The admin helpers that read the
# container's env check it. FT_FAIL=1 fails every ALTER. Logins are logged as auth-ok in $FT/calls, with every call.
cat > "$T/fp/bin/podman" <<'EOF'
#!/usr/bin/env bash
echo "$*" >> "$FT/calls"
case $1 in
  container) [ "$2" = exists ] && [ -f "$FT/run/$3" ] ;;
  inspect) n=${!#}; [ -f "$FT/run/$n" ] || exit 1
           case "$*" in *Running*) echo true ;; *StartedAt*) echo 2026-10-31T10:00:00Z ;; *) exit 1 ;; esac ;;
  ps) ls "$FT/run" ;;
  logs) echo "DATABASE IS READY TO USE"; echo "(*) Setup has completed" ;;   # the readiness lines of Oracle and Db2
  rm|stop) n=${!#}; rm -f "$FT/run/$n" ;;
  run) n=""; ef=""; eng=""
       while [ $# -gt 0 ]; do
         case $1 in --name) n=$2; shift 2 ;; --env-file) ef=$2; shift 2 ;;
           --label) case $2 in lfc.engine=*) eng=${2#lfc.engine=} ;; esac; shift 2 ;; *) shift ;; esac
       done
       [ -n "$n" ] || exit 1
       cp "$ef" "$FT/envs/$n"; date > "$FT/run/$n"; [ -z "$eng" ] || echo "$eng" > "$FT/eng/$n"
       # the admin password at a first start: the env file's, except Informix, whose OS password the image sets to its own default
       # (slot.sh's prep sets the env file's on each start, after rotate_now: the check of a clone's old one comes first)
       [ -f "$FT/db/$n" ] || { if [ "$eng" = informix ]; then echo image-default > "$FT/db/$n"; else grep -m1 'PASSWORD=' "$ef" | cut -d= -f2 > "$FT/db/$n" || : > "$FT/db/$n"; fi; }
       ;;
  exec) shift
        while [[ ${1:-} == -* ]]; do case $1 in -u|-e) shift ;; esac; shift; done
        n=$1; shift; [ -f "$FT/run/$n" ] || { echo "no container $n" >&2; exit 1; }
        cmd="$*"; eng=$(cat "$FT/eng/$n" 2>/dev/null || true); in=$(mktemp "$FT/in.XXXXXX"); cat > "$in"
        first=$(head -1 "$in"); db=$(cat "$FT/db/$n")
        bad() { echo "$1" >&2; rm -f "$in"; exit 1; }
        login() { [ "$1" = "$db" ] || bad "auth failed for $n"; echo "auth-ok $n" >> "$FT/calls"; }
        setpw() { [ -n "$1" ] || bad "no new password for $n"; [ -z "${FT_FAIL:-}" ] || bad "ALTER failed (test)"; echo "$1" > "$FT/db/$n"; }
        envof() { sed -n "s/^$1=//p" "$FT/envs/$n" | head -1; }
        case $cmd in
          # postgres: the old password is the first stdin line, read into PGPASSWORD for psql; ALTER USER sets the new one
          *'read -r p; PGPASSWORD=$p'*) login "$first"
            setpw "$(sed -n "s/^ALTER USER postgres WITH PASSWORD '\([^']*\)';\$/\1/p" "$in")" ;;
          # mysql and mariadb: the old password is the first line, MYSQL_PWD for the client the engine names; ALTER sets all roots
          *'read -r p; MYSQL_PWD=$p exec '*) [[ $cmd == *"exec $eng -h127.0.0.1"* ]] || bad "$eng: the change runs the wrong client: $cmd"
            login "$first"
            grep -q 'SELECT host' "$in" && printf '%%\nlocalhost\n'
            if grep -q ALTER "$in"; then
              pws=$(sed -n "s/^ALTER USER 'root'@'[^']*' IDENTIFIED BY '\([^']*\)';\$/\1/p" "$in" | sort -u)
              [ "$(printf '%s\n' "$pws" | wc -l | tr -d ' ')" = 1 ] || bad "root accounts got different passwords"
              setpw "$pws"
            fi ;;
          # Db2: the old password is the first line, printed into the connect file; then chpasswd sets db2inst1's OS password
          *'read -r p; f=$(mktemp); printf "connect to LFCDB user db2inst1 using %s;'*)
            [[ $cmd == *'"$p"'* ]] || bad "db2: the connect doesn't pass the password"
            login "$first" ;;
          *'read -r l; echo "$l" | chpasswd'*) [ "${first%%:*}" = db2inst1 ] || bad "chpasswd for ${first%%:*}"
            setpw "${first#db2inst1:}" ;;
          # sqlserver: the old sa password is the first line, SQLCMDPASSWORD for sqlcmd; ALTER LOGIN sets the new one
          *'SQLCMDPASSWORD=$p'*) login "$first"
            setpw "$(sed -n "s/^ALTER LOGIN sa WITH PASSWORD = N'\([^']*\)';\$/\1/p" "$in")" ;;
          # Oracle: the sqlplus script's CONNECT carries the old sys password in double quotes; sys and system get the new one
          *'sqlplus -s -L /nolog'*) login "$(sed -n 's/^CONNECT sys\/"\([^"]*\)"@.*AS SYSDBA$/\1/p' "$in")"
            new=$(sed -n 's/^ALTER USER sys IDENTIFIED BY "\([^"]*\)";$/\1/p' "$in")
            [ -n "$new" ] && [ "$new" = "$(sed -n 's/^ALTER USER system IDENTIFIED BY "\([^"]*\)";$/\1/p' "$in")" ] \
              || bad "sys and system: no new password, or two different ones"
            setpw "$new" ;;
          # Informix: unix_chkpwd reads the old OS password of informix, NUL-terminated; chpasswd sets the new one
          'unix_chkpwd informix nullok') [ "$(tail -c1 "$in" | od -An -tx1 | tr -d ' \n')" = 00 ] || bad "unix_chkpwd input isn't NUL-terminated"
            old=$(tr -d '\0' < "$in")
            [ "$(wc -c < "$in" | tr -d ' ')" = $(( ${#old} + 1 )) ] || bad "unix_chkpwd input isn't one NUL-terminated password"
            login "$old" ;;
          chpasswd) while IFS= read -r l; do case $l in informix:*) setpw "${l#informix:}" ;; esac; done < "$in" ;;
          # the admin helpers of sql read the admin password from the container's own env: a container not started again fails here
          *'MYSQL_PWD="$MYSQL_ROOT_PASSWORD"'*) [ "$(envof MYSQL_ROOT_PASSWORD)" = "$db" ] || bad "$n: its env has an old MYSQL_ROOT_PASSWORD" ;;
          *'MYSQL_PWD="$MARIADB_ROOT_PASSWORD"'*) [ "$(envof MARIADB_ROOT_PASSWORD)" = "$db" ] || bad "$n: its env has an old MARIADB_ROOT_PASSWORD" ;;
          *'-P "$MSSQL_SA_PASSWORD"'*) [ "$(envof MSSQL_SA_PASSWORD)" = "$db" ] || bad "$n: its env has an old MSSQL_SA_PASSWORD" ;;
          *) ;;
        esac
        rm -f "$in"; exit 0 ;;
  *) echo "fake podman: $*" >&2; exit 1 ;;
esac
EOF
cat > "$T/fp/bin/timeout" <<'EOF'
#!/usr/bin/env bash
shift; exec "$@"
EOF
printf '#!/usr/bin/env bash\nexit 0\n' > "$T/fp/bin/ss"
printf '#!/usr/bin/env bash\necho fuse.juicefs\n' > "$T/fp/bin/findmnt"
cat > "$T/fp/bin/juicefs" <<'EOF'
#!/usr/bin/env bash
echo "$*" >> "$FT/juicefs.log"
[ "${1:-}" = clone ] && cp -R "$3" "$4"
exit 0
EOF
printf '#!/usr/bin/env bash\nexit 0\n' > "$T/fp/bin/flock"
chmod +x "$T/fp/bin/"*
# a JuiceFS VM with only the masters' settings (no VOLUME): the masters and envs come, the volume is not formatted
: > "$T/fp/calls"; : > "$T/fp/juicefs.log"; cp "$T/sas" "$T/sas.implicit"; chmod 600 "$T/sas.implicit"
env FT="$T/fp" PATH="$T/fp/bin:$T/bin:$PATH" STORAGE=juicefs ROOT="$T/vm5" SECRETS="$T/vm5s" MASTERS_URL="$MU" \
  MASTERS_SAS_FILE="$T/sas.implicit" MASTERS=none bash "$S/slot-vm-setup.sh" > /dev/null 2>"$T/imp8.err"; rc=$?
ok "no VOLUME on a JuiceFS VM: only the envs are fetched, rc 0; no juicefs format, no podman, no redis" \
  '[ $rc = 0 ] && cmp -s "$T/secrets/postgres.env" "$T/vm5s/postgres.env" && [ ! -s "$T/fp/juicefs.log" ] && [ ! -s "$T/fp/calls" ]'
mkdir -p "$T/fp/secrets" "$T/fp/root/masters/pgm" "$T/fp/root/masters/mym" "$T/fp/root/masters/sqm" "$T/fp/root/masters/crm"
printf 'POSTGRES_PASSWORD=Lfc1a1a1a1a1a1a1a1a1a1a1a1\n' > "$T/fp/secrets/postgres.env"
printf 'MYSQL_ROOT_PASSWORD=Lfc1b2b2b2b2b2b2b2b2b2b2b2\n' > "$T/fp/secrets/mysql.env"
printf 'ACCEPT_EULA=Y\nMSSQL_SA_PASSWORD=Lfc1c3c3c3c3c3c3c3c3c3c3c3\n' > "$T/fp/secrets/sqlserver.env"
: > "$T/fp/secrets/cockroachdb.env"
slot() {
  FT=$T/fp PATH="$T/fp/bin:$T/bin:$PATH" ROOT=$T/fp/root STATE=$T/fp/state LOCK=$T/fp/state/.lock LIMITS=$T/fp/no-limits \
    MAX_RUNNING=99 MAX_PER_OWNER=0 HOST=192.0.2.1 SECRETS=$T/fp/secrets bash "$S/slot.sh" --owner alice "$@"
}
calls() { cat "$T/fp/calls" 2>/dev/null || true; }
ST=$T/fp/state/alice
env_of() { sed -n "s/^$2=//p" "$1" | head -1; }

echo "== slot.sh: the admin password of a database cloned from a master"
: > "$T/fp/calls"
out=$(slot up postgres pg1 --from pgm 2>"$T/pg1.err"); rc=$?
ok "postgres clone: up answers with a login (lfc_user) and rc 0" '[ $rc = 0 ] && [ "$(jq -r .user <<<"$out")" = lfc_user ]'
ok "postgres clone: the admin password changed once, from the master's (logged in with it first)" \
  '[ "$(grep -c "^auth-ok lfc-slot-alice-pg1$" "$T/fp/calls")" = 1 ] && [ ! -e "$ST/pg1.rotate" ]'
ok "postgres clone: the env file has a new admin password, the database has the same one" \
  '[ "$(env_of "$ST/pg1.env" POSTGRES_PASSWORD)" != "$(env_of "$T/fp/secrets/postgres.env" POSTGRES_PASSWORD)" ] && [ "$(env_of "$ST/pg1.env" POSTGRES_PASSWORD)" = "$(cat "$T/fp/db/lfc-slot-alice-pg1")" ]'
ok "postgres clone: no container restart (its admin helpers use the socket, the image reads its env at init only)" \
  '[ "$(grep -c "^run " "$T/fp/calls")" = 1 ]'
ok "the master's env file is untouched" 'cmp -s "$T/fp/secrets/postgres.env" "$T/fp/secrets/postgres.env" && grep -q "Lfc1a1a1a1a1a1a1a1a1a1a1a1" "$T/fp/secrets/postgres.env"'
: > "$T/fp/calls"; slot up postgres pg1 >/dev/null 2>&1
ok "postgres: up again doesn't change the password a second time" '[ "$(grep -c auth-ok "$T/fp/calls")" = 0 ]'

: > "$T/fp/calls"; out=$(slot up mysql my1 --from mym 2>"$T/my1.err"); rc=$?
ok "mysql clone: the admin password changed (logged in with the master's first), and the container started again" \
  '[ $rc = 0 ] && [ "$(grep -c "^auth-ok lfc-slot-alice-my1$" "$T/fp/calls")" -ge 1 ] && [ "$(grep -c "^run " "$T/fp/calls")" = 2 ] && [ ! -e "$ST/my1.rotate" ]'
ok "mysql clone: the restart comes after the change (its admin helpers read MYSQL_ROOT_PASSWORD from the container's env)" \
  '[ "$(grep -n "^auth-ok lfc-slot-alice-my1$" "$T/fp/calls" | tail -1 | cut -d: -f1)" -lt "$(grep -n "^run .*--name lfc-slot-alice-my1" "$T/fp/calls" | tail -1 | cut -d: -f1)" ]'
ok "mysql clone: the running container got the new value" '[ "$(env_of "$T/fp/envs/lfc-slot-alice-my1" MYSQL_ROOT_PASSWORD)" = "$(env_of "$ST/my1.env" MYSQL_ROOT_PASSWORD)" ]'

: > "$T/fp/calls"; : > "$T/fp/calls"; out=$(slot up sqlserver ss1 --from sqm 2>"$T/ss1.err"); rc=$?
ok "sqlserver clone: the sa password changed, and the container started again with it" \
  '[ $rc = 0 ] && [ "$(grep -c "^auth-ok lfc-slot-alice-ss1$" "$T/fp/calls")" = 1 ] && [ "$(grep -c "^run " "$T/fp/calls")" = 2 ] && [ ! -e "$ST/ss1.rotate" ]'

# Oracle, Db2, Informix and MariaDB: an admin env as slot.sh's up writes it, and a master of each
mkdir -p "$T/fp/root/masters/"{orm,db2m,ifm,mdm}
printf 'ORACLE_PASSWORD=Lfc1d1d1d1d1d1d1d1d1d1d1d1\n' > "$T/fp/secrets/oracle.env"
printf 'LICENSE=accept\nDB2INSTANCE=db2inst1\nDB2INST1_PASSWORD=Lfc1e5e5e5e5e5e5e5e5e5e5e5\nDBNAME=LFCDB\n' > "$T/fp/secrets/db2.env"
printf 'LICENSE=accept\nINFORMIX_PASSWORD=Lfc1f6f6f6f6f6f6f6f6f6f6f6\n' > "$T/fp/secrets/informix.env"
printf 'MARIADB_ROOT_PASSWORD=Lfc1a7a7a7a7a7a7a7a7a7a7a7\n' > "$T/fp/secrets/mariadb.env"
# engine, clone name, master, admin key, restarted after the change (1 = the restart list of rotate_now)
for spec in "oracle ora1 orm ORACLE_PASSWORD 0" "informix ifx1 ifm INFORMIX_PASSWORD 0" "db2 db21 db2m DB2INST1_PASSWORD 1" "mariadb mdb1 mdm MARIADB_ROOT_PASSWORD 1"; do
  read -r e n m k restarted <<<"$spec"; c=lfc-slot-alice-$n
  : > "$T/fp/calls"; out=$(slot up "$e" "$n" --from "$m" 2>"$T/$n.err"); rc=$?
  mp=$(env_of "$T/fp/secrets/$e.env" "$k"); ne=$(env_of "$ST/$n.env" "$k"); dbpw=$(cat "$T/fp/db/$c" 2>/dev/null || true)
  envpw=$(env_of "$T/fp/envs/$c" "$k"); runs=$(grep -c -- "--name $c " "$T/fp/calls" || true)
  ok "$e clone: up answers with lfc_user and rc 0 ($(tr '\n' ' ' < "$T/$n.err" | cut -c1-120))" '[ $rc = 0 ] && [ "$(jq -r .user <<<"$out")" = lfc_user ]'
  ok "$e clone: logged in with the master's password before the change" '[ "$(grep -c "^auth-ok $c\$" "$T/fp/calls")" -ge 1 ]'
  ok "$e clone: the admin env has a new password (not the master's), and the database has the same one" \
    '[ -n "$ne" ] && [ "$ne" != "$mp" ] && [ "$ne" = "$dbpw" ]'
  ok "$e clone: no marker left" '[ ! -e "$ST/$n.rotate" ]'
  if [ "$restarted" = 1 ]; then
    ok "$e clone: started again after the change (its helpers read the new env), the change logged in before the start" \
      '[ "$runs" = 2 ] && [ "$envpw" = "$ne" ] && [ "$(grep -n "^auth-ok $c\$" "$T/fp/calls" | tail -1 | cut -d: -f1)" -lt "$(grep -n -- "--name $c " "$T/fp/calls" | tail -1 | cut -d: -f1)" ]'
  else
    ok "$e clone: not started again (the change needs no restart)" '[ "$runs" = 1 ]'
  fi
done

: > "$T/fp/calls"; out=$(slot up cockroachdb cr1 --from crm 2>"$T/cr1.err"); rc=$?
ok "cockroachdb clone: insecure, nothing to change: no login, no ALTER, marker gone, rc 0" \
  '[ $rc = 0 ] && [ ! -e "$ST/cr1.rotate" ] && [ "$(grep -c auth-ok "$T/fp/calls")" = 0 ] && [[ $(cat "$T/cr1.err") == *insecure* ]]'

: > "$T/fp/calls"; slot up postgres pg2 --empty >/dev/null 2>&1
ok "an empty database gets no marker and no change (its password is new already)" \
  '[ ! -e "$ST/pg2.rotate" ] && [ "$(grep -c auth-ok "$T/fp/calls")" = 0 ]'

before=$(env_of "$ST/pg1.env" POSTGRES_PASSWORD)
: > "$T/fp/calls"; FT_FAIL=1 slot up postgres pg4 --from pgm > "$T/pg4.out" 2>"$T/pg4.err"; rc=$?
ok "a failed change: up still answers (rc 0) and says the password is still the master's" \
  '[ $rc = 0 ] && [[ $(cat "$T/pg4.err") == *"still the master"* ]] && [ "$(jq -r .user "$T/pg4.out")" = lfc_user ]'
ok "a failed change keeps the working password (the env file is the master's) and the marker for the next up" \
  '[ "$(env_of "$ST/pg4.env" POSTGRES_PASSWORD)" = "$(env_of "$T/fp/secrets/postgres.env" POSTGRES_PASSWORD)" ] && [ -e "$ST/pg4.rotate" ] && [ -z "$(ls -A "$ST" | grep "\.XXXXXX")" ]'
: > "$T/fp/calls"; slot up postgres pg4 > /dev/null 2>"$T/pg4b.err"; rc=$?
ok "the next up changes it (the database still runs on its password: the change is tried again)" \
  '[ $rc = 0 ] && [ ! -e "$ST/pg4.rotate" ] && [ "$(env_of "$ST/pg4.env" POSTGRES_PASSWORD)" = "$(cat "$T/fp/db/lfc-slot-alice-pg4")" ] && [ "$(env_of "$ST/pg4.env" POSTGRES_PASSWORD)" != "$(env_of "$T/fp/secrets/postgres.env" POSTGRES_PASSWORD)" ]'
slot rm pg4 >/dev/null 2>&1; ok "rm: the marker goes with the database" '[ ! -e "$ST/pg4.rotate" ] && [ ! -e "$ST/pg4.env" ]'

: > "$T/fp/calls"; slot clone pg1 pg1c > /dev/null 2>"$T/clone.err"; rc=$?
ok "clone: the copy gets a marker of its own (a clone of a rotated database changes too)" \
  '[ $rc = 0 ] && [ -e "$ST/pg1c.rotate" ] && [ "$(env_of "$ST/pg1c.env" POSTGRES_PASSWORD)" = "$(env_of "$ST/pg1.env" POSTGRES_PASSWORD)" ]'
: > "$T/fp/calls"; slot up postgres pg1c > /dev/null 2>"$T/pg1c.err"; rc=$?
ok "the clone's first up changes its password: new value, not the source's" \
  '[ $rc = 0 ] && [ "$(grep -c "^auth-ok lfc-slot-alice-pg1c$" "$T/fp/calls")" = 1 ] && [ ! -e "$ST/pg1c.rotate" ] && [ "$(env_of "$ST/pg1c.env" POSTGRES_PASSWORD)" != "$(env_of "$ST/pg1.env" POSTGRES_PASSWORD)" ]'

echo "== bash syntax and the repo's lint"
ok "masters-export.sh, slot-vm-setup.sh, slot.sh: bash -n" 'bash -n "$S/masters-export.sh" && bash -n "$S/slot-vm-setup.sh" && bash -n "$S/slot.sh"'
exit $fail
