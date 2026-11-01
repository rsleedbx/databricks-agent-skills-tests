#!/usr/bin/env bash
# test-slot-limits.sh: checks slot.sh's per-owner limits against a fake podman and flock (no VM, no containers): the
#   LIMITS lookup (owner line, default line, a key missing from the owner's line, a key that starts like another, no
#   file), `up` at the limit stopping without parking, and `up --park` parking just enough, the earliest started
#   first. One PASS/FAIL line per check; exit 1 on any FAIL.
# bash32: exempt (slot.sh runs on the Linux VM; this runs it with the bash on PATH)
set -uo pipefail
S=$(cd "$(dirname "$0")" && pwd); T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT; fail=0
ok() { if eval "$2"; then echo "PASS $1"; else echo "FAIL $1"; fail=1; fi; }
mkdir -p "$T/bin" "$T/run" "$T/root" "$T/state"
# the fake podman: a running container is a file $T/run/<name> holding its start time; stop/rm remove it and are logged
cat > "$T/bin/podman" <<'EOF'
#!/usr/bin/env bash
case "$1" in
  inspect) f=$FT/run/$4; [ -f "$f" ] || exit 1
           case "$3" in *Running*) echo true ;; *StartedAt*) cat "$f" ;; *) exit 1 ;; esac ;;
  container) [ -f "$FT/run/$3" ] ;;
  stop) echo "stop ${*: -1}" >> "$FT/calls"; rm -f "$FT/run/${*: -1}" ;;
  rm) rm -f "$FT/run/${*: -1}" ;;
  ps) ls "$FT/run" ;;
  *) echo "fake podman: $*" >&2; exit 1 ;;
esac
EOF
printf '#!/usr/bin/env bash\nexit 0\n' > "$T/bin/flock"
chmod +x "$T/bin/podman" "$T/bin/flock"
slot() { FT=$T PATH="$T/bin:$PATH" ROOT=$T/root STATE=$T/state LOCK=$T/lock LIMITS=$T/limits MAX_RUNNING=99 \
           HOST=192.0.2.1 bash "$S/slot.sh" --owner "$@"; }
db() {   # db <owner> <name> [<start time>]: a postgres database record, running when a start time is given
  install -d "$T/state/$1"; printf '{"engine": "postgres", "port": 30000}\n' > "$T/state/$1/$2.json"
  [ -z "${3:-}" ] || echo "$3" > "$T/run/lfc-slot-$1-$2"
}

printf 'default max_running=1 hours_per_week=29\nalice max_running=2 max_running_x=9\nbob hours_per_week=10\n' > "$T/limits"
ok "owner line wins; a longer key isn't read" '[ "$(slot alice limits)" = "{\"owner\":\"alice\",\"max_running\":2,\"hours_per_week\":29}" ]'
ok "key missing from the owner line: the default line" '[ "$(slot bob limits)" = "{\"owner\":\"bob\",\"max_running\":1,\"hours_per_week\":10}" ]'
ok "owner not listed: the default line" '[ "$(slot carol limits)" = "{\"owner\":\"carol\",\"max_running\":1,\"hours_per_week\":29}" ]'
mv "$T/limits" "$T/limits.off"
ok "no file: MAX_PER_OWNER, hours not set" '[ "$(MAX_PER_OWNER=3 slot carol limits)" = "{\"owner\":\"carol\",\"max_running\":3,\"hours_per_week\":null}" ]'
mv "$T/limits.off" "$T/limits"

# alice: max_running=2, three running (b started first, then a, then c), and a parked one to start
db alice a 2026-10-31T10:00:00Z; db alice b 2026-10-31T09:00:00Z; db alice c 2026-10-31T11:00:00Z; db alice new
: > "$T/calls"; err=$(slot alice up postgres new 2>&1 >/dev/null); rc=$?
ok "at the limit: stops, names them, parks nothing" '[ $rc = 1 ] && [[ $err == *"you have 3 of 2 running databases on this VM (b a c)"* ]] && [ ! -s "$T/calls" ]'
: > "$T/calls"; err=$(slot alice up postgres new --park 2>&1 >/dev/null)
ok "--park: parks the two that started first, no more" '[ "$(paste -sd, - < "$T/calls")" = "stop lfc-slot-alice-b,stop lfc-slot-alice-a" ] && [ -f "$T/run/lfc-slot-alice-c" ]'
ok "--park says so" '[[ $err == *"parking your running database b first (2 at a time for you on this VM)"* ]]'
# a running database of someone else doesn't count, and one under the limit starts without parking
db bob x 2026-10-31T08:00:00Z; db carol y; rm -f "$T"/run/lfc-slot-alice-*
: > "$T/calls"; slot carol up postgres y >/dev/null 2>&1
ok "under the limit: nothing parked (others' databases don't count)" '[ ! -s "$T/calls" ]'
printf 'default max_running=x\n' > "$T/limits"
err=$(slot carol up postgres y 2>&1 >/dev/null); rc=$?
ok "a bad max_running: stops with the file named" '[ $rc = 1 ] && [[ $err == *"max_running for carol in $T/limits isn'"'"'t a number: x"* ]]'
exit $fail
