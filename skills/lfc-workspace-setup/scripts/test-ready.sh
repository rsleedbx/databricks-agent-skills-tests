#!/usr/bin/env bash
# test-ready.sh: ready.sh's profile and catalog fallback to the saved settings (lfc-test-settings), offline: a fake
#   databricks on PATH logs every call (and answers {}), LFC_TESTS_SETTINGS points at a temporary file. With a
#   saved DATABRICKS_PROFILE and LFC_CATALOG, every call names that profile and the catalog is checked; a profile on the
#   command line wins; with neither, ready.sh exits 2 with the setup hint and calls nothing. PASS/FAIL per check.
set -uo pipefail
S=$(cd "$(dirname "$0")" && pwd); ST=$S/../../lfc-test-settings/scripts/settings.sh; fail=0
ok() { if eval "$2"; then echo "PASS $1"; else echo "FAIL $1"; fail=1; fi; }
T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT; mkdir -p "$T/bin"
printf '#!/usr/bin/env bash\necho "$*" >> "$FD_LOG"\necho "{}"\n' > "$T/bin/databricks"; chmod +x "$T/bin/databricks"   # answers {} to everything
export LFC_TESTS_SETTINGS=$T/settings.env FD_LOG=$T/log
rd() { : > "$T/log"; PATH="$T/bin:$PATH" bash "$S/ready.sh" "$@" --no-enable > "$T/out" 2>&1; }

rd; rc=$?
ok "nothing saved, no profile given: exit 2, the setup hint, no CLI call" '[ $rc = 2 ] && grep -q "no DATABRICKS_PROFILE saved" "$T/out" && [ ! -s "$T/log" ]'
bash "$ST" set DATABRICKS_PROFILE=ws-x LFC_CATALOG=cat_x
rd
ok "saved profile: every databricks call names --profile ws-x" '[ -s "$T/log" ] && ! grep -v -- "--profile ws-x" "$T/log" | grep -q .'
ok "saved catalog: the catalog checked is cat_x" 'grep -q "cat_x" "$T/log" "$T/out"'
rd ws-y --catalog cat_y
ok "a profile and catalog on the command line win over the saved ones" '! grep -v -- "--profile ws-y" "$T/log" | grep -q . && grep -q "cat_y" "$T/log" "$T/out" && ! grep -q "cat_x" "$T/log"'
exit $fail
