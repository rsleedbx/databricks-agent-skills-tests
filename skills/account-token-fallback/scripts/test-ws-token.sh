#!/usr/bin/env bash
# test-ws-token.sh: checks ws-token.sh's TO (its timeout: perl alarm, as timeout(1) isn't on macOS), the line from the
#   script itself: a command's exit status passes through, and a command that hangs is stopped. Then ws-token.sh itself
#   with a fake databricks and az on PATH: a deleted workspace (exit 3, no login asked), an expired account login (named
#   first), an account token the workspace takes. One PASS/FAIL line per check; exit 1 on any FAIL.
set -uo pipefail
S=$(cd "$(dirname "$0")" && pwd); fail=0
ok() { if eval "$2"; then echo "PASS $1"; else echo "FAIL $1"; fail=1; fi; }
eval "$(grep -E '^TO=\(' "$S/ws-token.sh")"
[ ${#TO[@]} -gt 0 ] || { echo "FAIL no TO=( line in ws-token.sh"; exit 1; }
"${TO[@]}" 5 true; rc=$?
ok "exit 0 passes through" '[ $rc = 0 ]'
"${TO[@]}" 5 sh -c 'exit 3'; rc=$?
ok "exit 3 passes through" '[ $rc = 3 ]'
s=$SECONDS; "${TO[@]}" 1 sleep 5 2>/dev/null; rc=$?
ok "a hung command is stopped after 1 s" '[ $rc != 0 ] && [ $((SECONDS - s)) -le 3 ]'
"${TO[@]}" 5 no-such-command-x 2>/dev/null; rc=$?
ok "a missing command fails" '[ $rc != 0 ]'

# ---- ws-token.sh with fakes: FK_ME=<what the workspace says to a token config> (ok, or an error line); FK_AZ, FK_ACCT:
# the tokens az and the account login hand out (empty: none)
T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT; mkdir -p "$T/bin"
cat > "$T/bin/databricks" <<'FK'
#!/usr/bin/env bash
case "$1 $2" in
  "auth profiles") printf '{"profiles":[{"name":"ws1","host":"https://adb-1.2.azuredatabricks.net","account_id":"acct-1"},{"name":"acct-azure-1","host":"https://accounts.azuredatabricks.net","account_id":"acct-1"}]}\n' ;;
  "auth token") printf '{"access_token":"%s"}\n' "${FK_ACCT:-}" ;;
  "current-user me")
    [ -n "${DATABRICKS_CONFIG_FILE:-}" ] || { echo "Error: refresh token is invalid" >&2; exit 1; }   # its own login: expired
    if [ "${FK_ME:-}" = ok ] && grep -q '^token = good' "$DATABRICKS_CONFIG_FILE"; then echo '{"userName":"u"}'; exit 0; fi
    echo "${FK_ME:-Error: invalid access token}" >&2; exit 1 ;;
  *) echo "fake databricks: $*" >&2; exit 2 ;;
esac
FK
cat > "$T/bin/az" <<'FK'
#!/usr/bin/env bash
[ "$1 $2" = "account get-access-token" ] && [ -n "${FK_AZ:-}" ] && echo "$FK_AZ"
FK
chmod +x "$T/bin/databricks" "$T/bin/az"
wst() { PATH="$T/bin:$PATH" bash "$S/ws-token.sh" ws1 --cfg "$T/ws1.cfg" > "$T/out" 2> "$T/err"; }
FK_AZ=bad FK_ACCT=good FK_ME="Error: Did not find workspace with specified org ID: 1" wst; rc=$?
ok "a deleted workspace: exit 3, says it no longer exists, asks for no login" \
  '[ $rc = 3 ] && grep -q "no longer exists" "$T/err" && ! grep -q "auth login" "$T/err" && [ ! -e "$T/ws1.cfg" ]'
FK_AZ=bad FK_ACCT= FK_ME="Error: invalid access token" wst; rc=$?
ok "an expired account login too: exit 1, the account login is named first" \
  '[ $rc = 1 ] && grep -q "so has the account login that covers it: log in to the account .*auth login --profile acct-azure-1; or to this workspace only" "$T/err"'
FK_AZ=bad FK_ACCT=good FK_ME=ok wst; rc=$?
ok "an account token the workspace takes: exit 0, the config file printed and mode 600" \
  '[ $rc = 0 ] && [ "$(cat "$T/out")" = "$T/ws1.cfg" ] && [ "$(stat -f %Lp "$T/ws1.cfg" 2>/dev/null || stat -c %a "$T/ws1.cfg")" = 600 ]'
rm -f "$T/ws1.cfg"; FK_AZ= FK_ACCT=good FK_ME="Error: dial tcp: lookup adb-1.2.azuredatabricks.net: no such host" wst; rc=$?
ok "a host that no longer resolves: exit 3" '[ $rc = 3 ] && grep -q "no longer exists" "$T/err"'
exit $fail
