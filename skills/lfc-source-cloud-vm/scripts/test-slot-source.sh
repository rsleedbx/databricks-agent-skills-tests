#!/usr/bin/env bash
# test-slot-source.sh: offline checks of slot-source.sh, the source contract (lfc-source-contract) for a slot database:
# the file with and without a connection, the refusal that keeps a password off stdout, the contract validating with
# source-env.sh, and its SRC_SQL reaching the database through slot-sql.sh and a fake ssh to the forced command.
# Prints "all passed" or the failures; exit 1 on any failure.
#   bash skills/lfc-source-cloud-vm/scripts/test-slot-source.sh
set -uo pipefail
SLOT=$(cd "$(dirname "$0")" && pwd); CS=$(cd "$SLOT/../../lfc-source-contract/scripts" && pwd)
FX=$CS/fixtures; SE=$CS/source-env.sh
T=$(mktemp -d); fails=0
ok() { printf "  ok    %s\n" "$1"; }
no() { printf "  FAIL  %s\n" "$1"; fails=$((fails + 1)); }
check() { local name=$1; shift; if "$@" > "$T/out" 2>&1; then ok "$name"; else no "$name"; sed 's/^/        /' "$T/out" | tail -5; fi; }
# a fake ssh: logs its arguments and its stdin
FB=$T/bin; mkdir -p "$FB"
cat > "$FB/ssh" <<'FAKE'
#!/usr/bin/env bash
{ echo "ARGV $(basename "$0") $*"; echo "STDIN"; cat; } >> "$FAKE_LOG"
exit 0
FAKE
chmod +x "$FB/ssh"

echo "== slot-source.sh -> source-env.sh -> slot-sql.sh -> ssh"
UP='{"contract":1,"name":"pg1","engine":"postgres","host":"203.0.113.10","port":30000,"user":"lfc_user","password":"Lfc1secretpw","container":"lfc-slot-me-pg1","state":"running"}'
slot_with_connection() {   # no password in the file, mode 600, the connection and facts in it
  printf '%s\n' "$UP" | SLOT_HOST=203.0.113.10 SLOT_KEY="$T/key" "$SLOT/slot-source.sh" --connection me_pg1 --facts "$FX/facts.json" --out "$T/slot.json" > /dev/null || return 1
  ! grep -q Lfc1secretpw "$T/slot.json" && find "$T/slot.json" -perm 600 | grep -q . && grep -q '"connection":"me_pg1"' "$T/slot.json" && grep -q '"facts":' "$T/slot.json"
}
check "slot-source with a connection: no password in the file, mode 600, connection and facts in it" slot_with_connection
check "slot-source's contract validates with source-env.sh" "$SE" "$T/slot.json"
slot_sql_cmd() {   # the sql_cmd names the database and the key, and runs slot-sql.sh
  eval "$(exports_of_path "$T/slot.json")"
  case $SRC_SQL in *"SLOT_NAME=pg1"*"SLOT_KEY=$T/key"*"/slot-sql.sh") ;; *) return 1 ;; esac
}
exports_of_path() { "$SE" "$1" 2> /dev/null; }
check "slot-source: the sql_cmd is slot-sql.sh with the database's name and the key" slot_sql_cmd
slot_without_connection() {   # the password goes to --out's file, and only the file's path is printed
  printf '%s\n' "$UP" | SLOT_HOST=203.0.113.10 "$SLOT/slot-source.sh" --out "$T/slot-pw.json" > "$T/printed" || return 1
  [ "$(cat "$T/printed")" = "$T/slot-pw.json" ] && grep -q Lfc1secretpw "$T/slot-pw.json" && ! grep -q Lfc1secretpw "$T/printed"
}
check "slot-source without a connection: the password is in --out's file, not printed" slot_without_connection
slot_refused() {   # without --out, a contract that holds the password is refused: nothing is printed
  printf '%s\n' "$UP" | SLOT_HOST=203.0.113.10 "$SLOT/slot-source.sh" > "$T/out2" 2> /dev/null; [ $? = 2 ] && [ ! -s "$T/out2" ]
}
check "slot-source without --out or --connection: refused, nothing printed" slot_refused
slot_through_ssh() {   # the contract's SRC_SQL reaches the database through the forced command (fake ssh)
  : > "$T/log.slot"; export PATH=$FB:$PATH FAKE_LOG=$T/log.slot; eval "$(exports_of_path "$T/slot.json")"
  printf 'SELECT 1;\n' | $SRC_SQL mydb > /dev/null 2>&1
  grep -q '^ARGV ssh .*slot@203.0.113.10 sql pg1 mydb' "$T/log.slot" && grep -q 'SELECT 1' "$T/log.slot"
}
check "slot-source: SRC_SQL reaches the database through ssh (slot-sql.sh, fake ssh)" slot_through_ssh
slot_for_owner() {   # another owner's database: sql_cmd goes through the VM admin login as that owner
  printf '%s\n' "$UP" | SLOT_HOST=203.0.113.10 SLOT_KEY="$T/key" SLOT_OWNER=shreya-sharma-chen SLOT_ADMIN_KEY="$T/admin" \
    "$SLOT/slot-source.sh" --connection c1 --out "$T/slot-owner.json" > /dev/null || return 1
  : > "$T/log.owner"; export PATH=$FB:$PATH FAKE_LOG=$T/log.owner; eval "$(exports_of_path "$T/slot-owner.json")"
  printf 'SELECT 2;\n' | $SRC_SQL mydb > /dev/null 2>&1
  grep -q "^ARGV ssh -i $T/admin .*azureuser@203.0.113.10 sudo /usr/local/sbin/lfc-slot --owner shreya-sharma-chen sql pg1 mydb" "$T/log.owner"
}
check "slot-source for another owner: SRC_SQL goes through the admin login as that owner" slot_for_owner
bad_owner() { SLOT_HOST=h SLOT_NAME=pg1 SLOT_OWNER='x;y' SLOT_ADMIN_KEY=k "$SLOT/slot-sql.sh" < /dev/null > /dev/null 2>&1; [ $? = 2 ]; }
check "slot-sql: an owner name with other characters is refused" bad_owner
shell_words() {   # names that the admin login's shell would parse are refused before ssh runs
  : > "$T/log.sh"; export PATH=$FB:$PATH FAKE_LOG=$T/log.sh
  for w in 'a;b' 'a b' '$(id)'; do
    SLOT_HOST=h SLOT_NAME="$w" SLOT_OWNER=o SLOT_ADMIN_KEY=k "$SLOT/slot-sql.sh" < /dev/null > /dev/null 2>&1; [ $? = 2 ] || return 1
    SLOT_HOST=h SLOT_NAME=pg1 SLOT_OWNER=o SLOT_ADMIN_KEY=k "$SLOT/slot-sql.sh" "$w" < /dev/null > /dev/null 2>&1; [ $? = 2 ] || return 1
  done
  [ ! -s "$T/log.sh" ]
}
check "slot-sql: a database name with shell characters is refused, no ssh" shell_words

rm -rf "${T:?}"
echo; [ $fails -eq 0 ] && echo "all passed" || echo "$fails failed"; exit $((fails > 0))
