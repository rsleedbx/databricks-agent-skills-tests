#!/usr/bin/env bash
# test-scripts.sh: connection.sh and pipeline.sh offline, with a fake databricks on PATH (it logs every call and the
#   request body; FD_CONN=1: the connection exists). Checks that a Kafka source contract (SOURCE_JSON or --source-json)
#   gives the bootstrap, user, password file and topic; that the password goes into the request body, never the
#   command line; that a contract of another engine is refused; that nothing is created without --apply. One PASS/FAIL
#   line per check; exit 1 on any FAIL.
set -uo pipefail
S=$(cd "$(dirname "$0")" && pwd); fail=0
ok() { if eval "$2"; then echo "PASS $1"; else echo "FAIL $1"; fail=1; fi; }
T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT; mkdir -p "$T/bin"
cat > "$T/bin/databricks" <<'FD'
#!/usr/bin/env bash
echo "$*" >> "$FD_LOG"
case "$*" in
  "connections get"*) [ -n "${FD_CONN:-}" ] && echo '{"connection_type":"KAFKA","options":{"bootstrap_servers":"x:9093"}}' || exit 1 ;;
  "connections create"*) for x; do case $x in @*) cp "${x#@}" "$FD_BODY" ;; esac; done; echo '{"name":"c1","connection_type":"KAFKA","options":{"bootstrap_servers":"b"}}' ;;
  "pipelines create"*) echo '{"pipeline_id":"p1"}' ;;
esac
FD
chmod +x "$T/bin/databricks"
printf 'Endpoint=sb://ns1/;SharedAccessKeyName=r;SharedAccessKey=fakekey\n' > "$T/ns1.pw"; chmod 600 "$T/ns1.pw"
cat > "$T/ns1.source.json" <<J
{"contract": 1, "engine": "kafka", "name": "ns1", "host": "ns1.servicebus.windows.net", "port": 9093,
 "user": "\$ConnectionString", "topic": "t1", "password_file": "ns1.pw"}
J
printf '{"contract": 1, "engine": "postgres", "host": "h", "port": 5432, "user": "u"}\n' > "$T/pg.json"
run() { : > "$T/log"; FD_LOG=$T/log FD_BODY=$T/body DIAGRAM=0 PATH="$T/bin:$PATH" bash "$@" > "$T/out" 2>&1; }

SOURCE_JSON=$T/ns1.source.json run "$S/connection.sh" prof c1; rc=$?
ok "connection.sh, plan from SOURCE_JSON: bootstrap and user from the contract, nothing created" \
  '[ $rc = 0 ] && grep -q "bootstrap ns1.servicebus.windows.net:9093, SASL PLAIN, user \$ConnectionString, password from $T/ns1.pw" "$T/out" && ! grep -q create "$T/log"'
run "$S/connection.sh" prof c1 --source-json "$T/ns1.source.json" --apply; rc=$?
ok "connection.sh --apply: the password in the request body, not on any command line" \
  '[ $rc = 0 ] && [ "$(jq -r .options.password "$T/body")" = "$(cat "$T/ns1.pw")" ] && [ "$(jq -r .options.bootstrap_servers "$T/body")" = ns1.servicebus.windows.net:9093 ] && ! grep -q fakekey "$T/log" "$T/out"'
FD_CONN=1 run "$S/connection.sh" prof c1 --source-json "$T/ns1.source.json" --apply; rc=$?
ok "connection.sh: an existing connection is left as it is" '[ $rc = 0 ] && grep -q "left as it is" "$T/out" && ! grep -q "connections create" "$T/log"'
run "$S/connection.sh" prof c1 --source-json "$T/pg.json"; rc=$?
ok "connection.sh: a contract that isn't kafka is refused" '[ $rc = 2 ] && grep -q "not kafka" "$T/out"'
SOURCE_JSON=$T/ns1.source.json run "$S/pipeline.sh" prof --connection c1 --table a.b.c; rc=$?
ok "pipeline.sh, plan: the topic from the contract, nothing created" \
  '[ $rc = 0 ] && sed "/^plan only/d" "$T/out" | jq -e ".ingestion_definition.objects[0].table.connector_options.kafka_options.topics == [\"t1\"]" > /dev/null && ! grep -q create "$T/log"'
run "$S/pipeline.sh" prof --connection c1 --topic other --source-json "$T/ns1.source.json" --table a.b.c; rc=$?
ok "pipeline.sh: --topic wins over the contract" '[ $rc = 0 ] && sed "/^plan only/d" "$T/out" | jq -e ".ingestion_definition.objects[0].table.connector_options.kafka_options.topics == [\"other\"]" > /dev/null'
run "$S/pipeline.sh" prof --connection c1 --source-json "$T/ns1.source.json" --table a.b.c --apply; rc=$?
ok "pipeline.sh --apply: created, its id printed" '[ $rc = 0 ] && grep -q "^pipeline p1$" "$T/out" && grep -q "pipelines create" "$T/log"'
exit $fail
