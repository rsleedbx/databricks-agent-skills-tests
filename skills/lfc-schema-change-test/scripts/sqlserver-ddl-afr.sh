#!/usr/bin/env bash
# sqlserver-ddl-afr.sh: which SQL Server DDL needs a full refresh, and does auto_full_refresh_policy do it on its own?
# Answers a field question for SQL Server. For each DDL below there are three tables: CDC without the policy (<op>_off),
# CDC with it (<op>_on) and change tracking without it (<op>_ct; not for pk, as SQL Server won't drop the primary key
# of a change-tracked table). Each gets the DDL and one new row; then the run reads each table's error, lets the policy
# work on the _on tables, and full-refreshes the others that asked for it.
#
#   widen    ALTER COLUMN v VARCHAR(20) -> VARCHAR(50)
#   narrow   ALTER COLUMN v VARCHAR(20) -> VARCHAR(10)
#   rename   sp_rename v -> v2 (on the CDC tables with CDC turned off and on around it, which SQL Server requires)
#   dropcol  DROP COLUMN x
#   pk       primary key (id) -> (id, k), with CDC turned off and on around it (SQL Server refuses it otherwise)
#
#   Needs and options: as sqlserver-add-column.sh (PROFILE CONNECTION CATALOG SRC_SQL CONN_USER; SRC_ENV, MODE, KEEP, OUT)
set -uo pipefail
. "$(dirname "$0")/lib.sh"
lfc_init ddlafr
OPS="widen narrow rename dropcol pk"
TABLES=""; for o in $OPS; do TABLES+=" ${o}_off ${o}_on"; [ $o = pk ] || TABLES+=" ${o}_ct"; done
FP_TABLES=$TABLES

{ for t in $TABLES; do echo "CREATE TABLE lfcddemo.$t (id INT NOT NULL, k INT NOT NULL DEFAULT 0, v VARCHAR(20), x INT, CONSTRAINT pk_$t PRIMARY KEY (id));"; done; } | src_db
for t in $TABLES; do fill $t 10; done
src_util
for t in $TABLES; do case $t in *_ct) track ct $t;; *) track cdc $t;; esac; done

OBJS=""; for t in $TABLES; do case $t in *_on) OBJS+="${OBJS:+,}$(obj $t "$AFR")";; *) OBJS+="${OBJS:+,}$(obj $t)";; esac; done
pipelines_create "$OBJS"
allrows() { q "SELECT $(for t in $TABLES; do printf '(SELECT COUNT(*) FROM %s.%s),' "$T" "$t"; done | sed 's/,$//')" 2>&1 | tr '\t' ' '; }
WANT=$(for t in $TABLES; do printf '10 '; done | sed 's/ $//')
update; R=$(allrows); [ "$R" = "$WANT" ] || { update; R=$(allrows); }
[ "$R" = "$WANT" ] || { echo "baseline rows wrong: $R; $ERRS"; exit 1; }

# --- the DDL, then one row each ------------------------------------------------------------------------------------
ddl() {   # ddl <op> <table>
  case $1 in
    widen)   echo "ALTER TABLE lfcddemo.$2 ALTER COLUMN v VARCHAR(50);";;
    narrow)  echo "ALTER TABLE lfcddemo.$2 ALTER COLUMN v VARCHAR(10);";;
    # sp_rename of a CDC-captured column is refused (Msg 4928): on the CDC tables, CDC off around it too
    rename)  [[ $2 == *_ct ]] || echo "EXEC sys.sp_cdc_disable_table @source_schema = 'lfcddemo', @source_name = '$2', @capture_instance = 'all';"
             echo "EXEC sp_rename 'lfcddemo.$2.v', 'v2', 'COLUMN';";;
    dropcol) echo "ALTER TABLE lfcddemo.$2 DROP COLUMN x;";;
    # SQL Server refuses to drop the index CDC uses (Msg 22979): CDC off, the new key, then CDC on again (below)
    pk)      printf "EXEC sys.sp_cdc_disable_table @source_schema = 'lfcddemo', @source_name = '%s', @capture_instance = 'all';\nALTER TABLE lfcddemo.%s DROP CONSTRAINT pk_%s;\nALTER TABLE lfcddemo.%s ADD CONSTRAINT pk_%s PRIMARY KEY (id, k);\n" "$2" "$2" "$2" "$2" "$2";;
  esac
}
for o in $OPS; do for t in ${o}_off ${o}_on ${o}_ct; do
  [ $t = pk_ct ] && continue
  out=$( { ddl $o $t; echo GO; case $t in (pk_o*|rename_o*) ;; (*) echo "INSERT INTO lfcddemo.$t (id) VALUES (11);";; esac; } | src 2>&1) || echo "source DDL on $t failed: $(echo "$out" | head -2)"
done; done
track cdc pk_off pk_on rename_off rename_on
# their row after CDC is on again: a change made while CDC was off is never captured
for t in pk_off pk_on rename_off rename_on; do echo "INSERT INTO lfcddemo.$t (id) VALUES (11);" | src >/dev/null; done
# CDC capture instances keep their old column list after an ALTER; the utility objects' DDL trigger (1.7) refreshes them
for _ in 1 2 3 4; do
  sleep "$WAIT"; update
  n=0; for t in $TABLES; do [ -n "$(errs_for $t)" ] || [ "$(q "SELECT COUNT(*) FROM $T.$t" 2>&1)" = 11 ] && n=$((n + 1)); done
  [ $n -eq $(echo $TABLES | wc -w) ] && break
done
for t in $TABLES; do printf -v "ERR_$t" '%s' "$(errs_for $t)"; done   # one ERR_<table> each (bash 3.2: no declare -A)
err() { local v=ERR_$1; printf '%s' "${!v:-}"; }
cols() { q "SELECT CONCAT_WS(',', COLLECT_LIST(CONCAT(column_name, ':', data_type))) FROM \`$CATALOG\`.information_schema.columns WHERE table_schema = '$S' AND table_name = '$1'" 2>&1; }
for t in $TABLES; do
  e=$(err $t); observe "$t-after-ddl" "errors: ${e:-none}$( [ -n "${e// /}" ] && echo "; $(msgs_for $t | head -1 | cut -c1-160)"); rows $(q "SELECT COUNT(*) FROM $T.$t" 2>&1); columns $(cols $t)"
done

# --- the policy: the _on tables recover with no refresh requested (up to 3 more updates) ----------------------------------
for _ in 1 2 3; do
  done_n=0
  for o in $OPS; do [ "$(q "SELECT COUNT(*) FROM $T.${o}_on" 2>&1)" = 11 ] && done_n=$((done_n + 1)); done
  [ $done_n -eq $(echo $OPS | wc -w) ] && break; update
done
for o in $OPS; do
  r=$(q "SELECT COUNT(*) FROM $T.${o}_on" 2>&1) e=$(err ${o}_off)
  if [ -z "${e// /}" ]; then
    observe "$o-inline" "no error without the policy (${o}_off rows $(q "SELECT COUNT(*) FROM $T.${o}_off" 2>&1), columns $(cols ${o}_off)); with it ${o}_on rows $r"
  elif [[ "$e" =~ CDC_INCOMPATIBLE_SCHEMA_CHANGES|PRIMARY_KEY_CHANGE|DDL_NOT_SUPPORTED|UNTRACEABLE|CDC_LOG_TRUNCATED ]]; then
    [ "$r" = 11 ] && claim "$o-afr-recovers" PASS "${o}_off asked for a full refresh (${e% }); ${o}_on recovered on its own: 11 rows, columns $(cols ${o}_on)" \
      || claim "$o-afr-recovers" FAIL "${o}_off: ${e% }; ${o}_on rows $r, errors $(errs_for ${o}_on)"
  else
    observe "$o-other-error" "${o}_off: ${e% } (not a full-refresh error); ${o}_on rows $r"
  fi
done

# --- a requested full refresh recovers the others ------------------------------------------------------------------
FR=""; for t in $TABLES; do [[ $t == *_on ]] && continue; [ "$(q "SELECT COUNT(*) FROM $T.$t" 2>&1)" = 11 ] || FR+="${FR:+,}$t"; done
if [ -n "$FR" ]; then
  update "$FR"
  bad=""; for t in ${FR//,/ }; do [ "$(q "SELECT COUNT(*) FROM $T.$t" 2>&1)" = 11 ] || bad+=" $t"; done
  [ -z "$bad" ] && claim full-refresh-recovers PASS "full refresh of $FR: 11 rows each" || claim full-refresh-recovers FAIL "still short after refreshing $FR:$bad; errors: ${ERRS:-none}"
fi

result_md sqlserver-ddl-afr
exit $FAILED
