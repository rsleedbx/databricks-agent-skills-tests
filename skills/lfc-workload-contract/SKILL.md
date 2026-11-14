---
name: lfc-workload-contract
description: "The workload contract for Lakeflow Connect tests: a statschema model file (a TPC model or a customer's shape, format 1) and one small versioned workload.json that names its DDL, the tables, and the commands that create and fill copies of the model, so a test builds its source tables without naming the shape or fill scripts by path. Use when a test needs a model's tables on a source database (WORKLOAD_JSON), when writing or checking a model file or a workload file, or when changing what a test reads from a workload."
---

# Workload contract (contract 1)

A Lakeflow Connect test that needs many tables of a model (a TPC schema, or a customer's shape) takes them through a workload file: `WORKLOAD_JSON=<file>` on the script. The file is written by the shape layer's producer (the `workload.sh` in lfc-source-datagen). The test runs its commands from the file and never names the statschema or datagen scripts, so either side can move its files without touching the other.

This is the same pattern as the source contract ([lfc-source-contract](../lfc-source-contract/SKILL.md)): a versioned JSON file, a validator that prints exports, and a producer that writes the paths so consumers don't.

## The model file (format 1)

A statschema model file: the shaped schema that `ddl.py` turns into DDL. Format 1 is what a consumer may rely on, and it's the part of statschema's `schema.yaml` shape that every producer writes:

| Field | Required | Meaning |
|---|---|---|
| `version` | yes | the string `"1.0"` (quoted) |
| `tables[].name` | yes | a non-empty string, unique in the file |
| `tables[].row_count` or `row_count_per_sf` | exactly one | a whole number, 0 or more (fixed size), or a number above 0 (rows per scale unit) |
| `tables[].columns[].name` | yes | unique in its table |
| `tables[].columns[].type` | yes | statschema's canonical type (a non-empty string) |
| `tables[].columns[].primary_key` | no | `true` or `false`; the key is the columns with `true`, none for a keyless table |
| `tables[].builtin_generator` | no | a string |

Other keys are ignored. Tables may sit at indent 0 or 2, and columns in block or flow style. Block scalars, anchors, tags and tabs are refused. The shaped file (`shape.py`'s output) keeps `version` and writes `row_count`, and it passes.

[`scripts/validate-model.py`](scripts/validate-model.py) `<model.yaml>...` checks files: one line per problem, `<file>: <problem>` (a parse error gives the line), exit 1; nothing and exit 0 when they hold. Stdlib only.

## The workload file (contract 1)

```json
{"contract": 1, "model": "tpcc", "scale": 0.01, "dialect": "tsql",
 "tables": ["district", "customer", "warehouse", "item", "stock", "order_line", "new_order", "orders", "history"],
 "ddl": "/abs/work/tpcc-sf0.01/model.sql",
 "fill_cmd": "bash /abs/datagen/copies.sh ms",
 "bundle_cmd": "bash /abs/datagen/bundle.sh"}
```

| Field | Required | Meaning | Export |
|---|---|---|---|
| `contract` | yes | `1` | |
| `model` | yes | a lowercase name; copies are named `<model>_NNNN` | `WL_MODEL` |
| `scale` | yes | a number above 0 (the scale factor the model was shaped at) | `WL_SCALE` |
| `dialect` | yes | the DDL's dialect (`tsql` or `postgres`: the fill command's engine) | `WL_DIALECT` |
| `tables` | yes | the model's table names as the DDL creates them; each a plain identifier | `WL_TABLES` (space-separated), `WL_NTABLES` |
| `ddl` | yes | absolute path of the DDL file, one copy's `CREATE TABLE` statements with unqualified names | `WL_DDL` |
| `fill_cmd` | yes | the fill command (below): words separated by one space | `WL_FILL` |
| `bundle_cmd` | no | the bundle command (below) | `WL_BUNDLE` (unset when absent) |
| `verify` | no | absolute path of the model's rule checks for the dialect (below) | `WL_VERIFY` (unset when absent) |

Paths are absolute, and a command's words are letters, digits, `_ . / -` and single spaces, so no quoting is needed. A workload file holds no secret: a key named like a password, secret or token is refused.

### The fill command

```bash
MSSQL='<client command>' $WL_FILL "$WL_DDL" "$WL_MODEL" <first copy> <last copy> [<parallel>]
```

It creates copies `<model>_<first>` .. `<model>_<last>` (four digits) in the target database, each with the DDL's tables, and fills them. A copy that exists is reused and a copy already filled is resumed, so a rerun continues. The client is the command the fill calls for SQL: for `tsql` it reads SQL on stdin, connects to the target database, and prints rows with `-y 0` (the variable is `MSSQL`); for `postgres` it reads SQL on stdin, connects to the target database, and keeps psql's own output (`-At`, `|` between fields; the variable is `PSQL`). `<parallel>` (default 4) is how many copies fill at once.

Its output: one line per copy, `done <schema> <seconds>s` or `failed <schema> <reason>`, then the last line `all done` (exit 0) or `finished with failures` (exit 1). A consumer reads only those.

### The verify file

```bash
{ echo "SET search_path = <model>_NNNN;"; cat "$WL_VERIFY"; } | <client>
```

SQL for the dialect with unqualified table names, run in one copy's schema. Each row that starts with `bad` is a check: its label, then counts, which must all be 0. Other rows (row counts, key ranges) are for a reader. `workload.sh --spec` writes it for TPC-H on `postgres` (the spec's keys, prices, lines per order, and the dates across tables).

### The bundle command

```bash
$WL_BUNDLE /path/out.tgz
```

It writes one portable archive. Unpacked into a folder, every file named in `fill_cmd` is at the top of that folder under its base name, so the fill runs there as `$WL_FILL_IN_BUNDLE`. The DDL file is not in the bundle: copy it beside the bundle and pass its base name. The consumer supplies the client (for `tsql`, an executable that reads SQL on stdin, named as the bundle's `MSSQL` client).

`WL_FILL_IN_BUNDLE` is `fill_cmd` with each absolute word reduced to its base name. `workload-env.sh` computes it.

## Producers and consumers

- **Producer**: the shape layer's `workload.sh` in lfc-source-datagen. It runs statschema's `shape.py` and `ddl.py`, checks the model file with `validate-model.py`, and writes `model.yaml`, `model.sql` and `workload.json` for one model at one scale. It checks its own workload file with `workload-env.sh` before it returns. The fill and bundle commands it writes are datagen's `copies.sh` and `bundle.sh`.
- **Consumers**: the Lakeflow tests that need many tables. The first is lfc-schema-change-test's `sqlserver-scale.sh` (`WORKLOAD_JSON`); it runs the fill locally with `BUILD_HERE=1`, or bundles it and runs it on the source VM. `postgres-tpch-cdc.sh` fills one copy of the spec TPC-H on a PostgreSQL source and runs `verify` before and after its change rounds.
- A consumer may use only the exports above. It doesn't read the model file, the DDL's contents, or the fill's options.

## What a producer guarantees

- The model file holds format 1, and the DDL creates exactly the `tables` named.
- The workload file passes `workload-env.sh`: the paths exist when it is written, and the fill command runs as the output rules above say.
- No password, token or connection string is in it.

## What stays internal

Not part of the contract, and free to change within contract 1: the DDL's statements and types for the dialect, the planner's options (`ROWS`, `ORDER`, `DATA`, `PLAIN_EVERY`, `BLOCK`, `LOB_LEN`, `SESSIONS` in lfc-source-datagen's `fill.sh`), the planner files, how `shape.py` chooses each column's rule, and the capped-DML flags.

## Version rule

Consumers read contract 1 only. `workload-env.sh` exits 2 with one line on any other `contract` value, and on a missing, wrong or unsafe field, or a file that isn't there. A producer that writes a new version updates its consumers first. A change a reader can't ignore is a new version; an optional field is not.

## Scripts

- [scripts/workload-env.sh](scripts/workload-env.sh) `<workload.json>`: validates the file and prints the exports. Use `eval "$(scripts/workload-env.sh workload.json)"`.
- [scripts/validate-model.py](scripts/validate-model.py) `<model.yaml>...`: the model format 1 check.
- [scripts/test-workload-env.sh](scripts/test-workload-env.sh): offline checks on [scripts/fixtures/](scripts/fixtures/) (the valid file, each refusal, the exports, no secret printed), and it runs [scripts/test-validate-model.sh](scripts/test-validate-model.sh) (the model fixtures: both indentations, each refusal).

## Example

```bash
WORK=$(mktemp -d)   # a producer's output folder: the shape layer's workload.sh writes it
bash <datagen>/workload.sh --model tpcb --sf 0.01 --dialect tsql --out "$WORK"  # the producer; prints $WORK/workload.json
eval "$(skills/lfc-workload-contract/scripts/workload-env.sh "$WORK/workload.json")"
echo "$WL_NTABLES tables: $WL_TABLES"
MSSQL="$SRC_SQL $DB" $WL_FILL "$WL_DDL" "$WL_MODEL" 1 10 4   # copies tpcb_0001 .. tpcb_0010 in $DB
```

## Errors

| Message | Cause | Fix |
|---|---|---|
| `missing <field>` | the file lacks a required field | add it (table above) |
| `contract <n> is ... reads contract 1 only` | another version | use a reader for `<n>`, or a producer that writes 1 |
| `<field> must be ...` | a value out of range or of the wrong type | correct it (table above) |
| `is a secret: a workload file holds no secrets` | a key named like a password | remove it; the source's secret stays in its own contract |
| `... is not a file` | the DDL or a command's file is missing | rebuild the workload, or copy the file back |
| `model.yaml breaks model format 1` (from the producer) | the shaped model is not format 1 | fix the schema or the producer; see `validate-model.py`'s lines |
