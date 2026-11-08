---
name: lfc-test-results
description: Record and recall Lakeflow Connect test results (source setup, UC connections, query-based and CDC ingestion, branch layers, performance) so anyone can see what was tested, where, with which skill versions, and what happened. Use after any test run, and before testing something, to check what's already known.
parent: lfc-source-setup
---

# Test results: record and recall

Every test run leaves two things in this repo's `results/`, so the next person (or agent) can find out what's been tested without rerunning it. The space of tests (hosts, engines and versions, storage, workspaces, modes, workloads, sizes, change rates, credentials, network paths) and what's planned are in [references/test-matrix.md](references/test-matrix.md).

| File | What | Who reads it |
|---|---|---|
| `results/runs/<run>.jsonl` | One JSON record per test cell (engine × mode × host × setup), one file per run, append-only | `recall.py`, `jq`, DuckDB, agents |
| `results/<date>-<topic>.md` | The write-up of one run: setup, a results table, what broke, which skill fixes came out of it | people |

Installed as a plugin (`lfc-tests`), the copy is replaced on every update, so new records and findings go to `~/.lfc/tests/results/` (or `$LFC_TESTS_RESULTS`) instead, and `recall.py`, `finding.py` and `ddl-matrix.py render` read the shipped results plus those ([scripts/results_dir.py](scripts/results_dir.py)).

The repo is **public**: records and write-ups hold outcomes, counts, versions and error text, never IPs, hostnames, IDs, emails, catalog or workspace names, or credentials. `record.py` refuses a record that looks like it holds any of these.

## Before testing: recall

```bash
S=skills/lfc-test-results/scripts
uv run $S/recall.py                                   # current status: the latest result per test x engine x host
uv run $S/recall.py --engine oracle                   # everything about one engine, newest first
uv run $S/recall.py --test icdc,scdc --outcome blocked,fail --all
uv run $S/recall.py --grep "not supported in your workspace"
uv run $S/recall.py --coverage                        # test x engine grid of the newest outcomes
uv run $S/coverage_page.py --write docs/lakeflow-connect-database.md   # after adding records: regenerate the by-database table
uv run $S/recall.py --combos --test zerobus           # per route and topology: engine x model grid of the newest outcomes
uv run $S/recall.py --workspace classic --storage juicefs --all
```

Then read the run's write-up (`--run` names it) for the details. If a cell already passed with the current skill commits (`skills` in the record), don't rerun it unless the user asks or the docs changed.

The same for a change: when earlier records, the vendor's docs or a skill already show how it will behave (a setting the docs define, a fix shaped like one that passed), don't propose a long live run for it. Make it, run the offline checks, and say in the commit or PR what wasn't run. Record nothing for it, since no run happened; the next run that exercises it records the outcome. Propose a run only when nothing covers the behavior, or the user asks.

`jq` and DuckDB work on the file directly:

```bash
jq -c 'select(.subject.engine == "mysql" and .test == "icdc")' results/runs/*.jsonl
duckdb -c "SELECT test, subject.engine, outcome, count(*) FROM read_json('results/runs/*.jsonl') GROUP BY ALL ORDER BY ALL"
```

## Before reading a pipeline result

- **Count rows only once the gateway's snapshot is done.** An ingestion update can report COMPLETED while the gateway is still snapshotting, or while a gateway flow has failed. In the 2026-10-25 round that happened in 7 runs, and a second plain update then completed the rows. So wait until the gateway's events (`bin/lfc-events.sh <profile> <gateway-id>`) show `SNAPSHOT_COMPLETED` for each table before comparing source and destination counts. If you count earlier, say so in `result`.
- **A gateway snapshots only after its ingestion pipeline's first update starts.** A gateway started on its own finds the tables (Schema Exploration) and then waits; start the ingestion update before waiting for `SNAPSHOT_COMPLETED`.
- **Count the destination tables only.** The staging schema's tables and the shared `cdc_staging_table` aren't source tables; leave them out of row counts and table counts.
- **Read every page of events.** The events API returns at most 250 per page, newest first; follow `next_page_token` before concluding an event never came.
- **Integrated CDC on MySQL: the extractor's snapshot flow is the signal.** The per-table operation event "Snapshot of <table> has COMPLETED" doesn't come (it stays at IN_PROGRESS), and the applier's snapshot flows report COMPLETED only when the update ends. A `flow_progress` COMPLETED on `extractor_..._<table>_snapshot_flow` means the table's snapshot is read. Tested 2026-10-26, MySQL 8.4.13.
- **Change tracking keeps one change per key between reads.** Updating the same row again and again shows one staged change per read, not one per update. To measure how often a table is read, or whether changes are lost, write a new key per change.
- **Give every wait a deadline.** A loop that waits for an update or an event gives up after 20 minutes and records "timed out"; a batch with a hard stop sets `LFC_DEADLINE_MIN` (`skills/lfc-schema-change-test/scripts/lib.sh`), so `update` stops the pipeline at the deadline and reports it.
- **Read the gateway's events as well as the ingestion pipeline's.** Source-side errors (archive logs gone, slot invalidated, capture instance missing) appear on the gateway; the ingestion update can show none.
- **Record the build:** `bin/lfc-runtime.sh --summary <profile> <pipeline-id>` for the gateway and the ingestion pipeline goes in `subject` or `setup.runtime`. A result without it can't be compared with the next release.
- **Check each setup step took effect before reading the result.** A grant, revoke or source change that failed silently makes the case invalid, not a finding. Read the state back: effective permissions, the capture instance, the slot.

## After testing: record

Record every cell you ran, including the ones that stopped (`expected-stop`) or were refused by the workspace (`blocked`). A record is one line of JSON:

| Field | Required | Content |
|---|---|---|
| `date` | yes | `YYYY-MM-DD` of the run |
| `run` | yes | the run's name, the same as its write-up: `<date>-<topic>` |
| `test` | yes | `connection`, `qbc`, `fcqbc` (QBC via foreign catalog), `icdc` (integrated CDC), `scdc` (standard CDC), `dml`, `source-setup`, `rclone-pull`, `reflink-isolation`, `juicefs-clone`, `disk-restore`, `snapshot-read`, `write-tps`, `load`, `zerobus` (change capture through Zerobus into bronze, silver, gold; `cdc` in the 2026-10-22 records), `other` |
| `subject` | yes | `{"engine": "oracle", "version": "23ai Free 23.26"}`, plus `image`, `edition` where they matter |
| `outcome` | yes | `pass`, `fail`, `blocked` (the workspace or environment refused it), `expected-stop` (the skill correctly stopped), `partial` |
| `result` | yes | what happened, with counts: `snapshot 10/10; 3 inserts, 2 updates, 2 deletes applied (11/11)` |
| `host` | | where the source ran: `azure-vm-podman`, `azure-flexserver`, `aws-rds`, `local-podman`, `lakebase` |
| `workspace` | | `{"cloud": "azure", "region": "centralus", "env": "fevm", "compute": "serverless-only", "catalog": "own managed storage"}`; descriptions, never names or IDs |
| `setup` | | the rest of the test's setup: `storage`, `network`, `credentials`, `workload` (`{"name": "tpch", "sf": 1}`), `dml` (`{"rate_per_s": 100}`), `tool` (the script that ran it, e.g. `lfc-source-cloud-vm scripts/tpc-master.sh`), `contributor` (a handle). New keys are allowed; `record.py` notes them, and they get documented in [test-matrix.md](references/test-matrix.md) |
| `compute` | pipeline tests | what compute the test asked for and got, so a classic default, a start that never happened, and a firewall block stay apart: `serverless` (`unset`, `true`, `false`: what the run sent), `ran_on` (`serverless`, `classic`, `not-started`, `unknown`: the latest update's `config.serverless`), `started` (the compute came up: `INITIALIZING` or `Extractor STARTED`), `reached_source` (it then connected), `blocked_by` (`compute-not-allowed`, `firewall`, `feature-gate`, `other`; needed when `started` or `reached_source` is false), and on a rerun `switched_from` + `switch_reason`. `gateway`: the same for a standard CDC gateway. Read them with `bin/compute_report.sh <profile> <name-prefix>`. Example: `{"serverless": "unset", "ran_on": "classic", "started": true, "reached_source": false, "blocked_by": "firewall"}` |
| `metrics` | | numbers: `{"rows": 5000000, "clone_s": 0.1, "tps": 234}` |
| `error` | non-pass | the exact error text, with names and IDs replaced (`<connection>`, `<pipeline-id>`) |
| `skills` | auto | both repos' commits; `record.py` fills it in from the checkouts next to each other |
| `docs` | | the skill files the test exercised or fixed |
| `notes` | | anything else: a workaround, a doc defect found |

```bash
cat > /tmp/rec.jsonl <<'EOF'
{"date": "2026-10-16", "run": "2026-10-16-azvm-sources", "test": "icdc", "host": "azure-vm-podman",
 "subject": {"engine": "oracle", "version": "23ai Free 23.26"}, "workspace": {"cloud": "azure", "compute": "serverless-only"},
 "outcome": "pass", "result": "snapshot 10/10; 3 inserts, 2 updates, 2 deletes applied (11/11)"}
EOF
uv run $S/record.py < /tmp/rec.jsonl            # validates the batch, appends it to results/runs/<run>.jsonl, or prints what's wrong and appends nothing
uv run $S/record.py --check                     # re-validate the whole file (bin/test_scripts.sh runs this)
```

Write each record on one line in the file you pipe in (or pass a JSON array). Then write or extend `results/<run>.md`: the environment (cloud, region, workspace type, catalog type, VM size), a results table, what broke and how it was fixed, and the skill fixes. Commit both with the skill fixes they led to.

## Findings: what a combination meets

A run record says what was tested and how it came out. A finding says what the test turned up that a real deployment of the same combination would also meet, so nobody has to find it twice: a source's behavior, a connector's, a Databricks limit or bug, a gap in the pattern (bronze, silver, gold), or a defect in the test tooling that made a check wrong. Not typos. Record each as it's found, also when it's fixed the same day.

One file per finding, `results/findings/<id>.json`, through [scripts/finding.py](scripts/finding.py):

```bash
S=skills/lfc-test-results/scripts
uv run $S/finding.py --source mysql --route debezium-zerobus --topology n:1   # before building a combination: what's known
uv run $S/finding.py --source sqlserver --route lfc-icdc --long                # with the effect and the fix
uv run $S/finding.py add < finding.json                                        # a new one (--update to change one)
uv run $S/finding.py --check && uv run $S/finding.py --md > docs/findings.md   # validate, then regenerate the table
```

| Field | Content |
|---|---|
| `id`, `title` | kebab-case id (the file name); a title short enough for one table line |
| `kind` | `source-behavior`, `connector-behavior`, `databricks-limit`, `databricks-bug`, `pattern`, `tooling` |
| `status` | `fixed`, `workaround`, `by-design`, `documented-limit`, `open` |
| `applies` | the combination, each a list (`*`: any): `source` (the records' engine names), `route` (`lfc-qbc`, `lfc-icdc`, `lfc-scdc`, `debezium-zerobus`, `kafka-connect-zerobus`), `layer` (`source`, `capture`, `producer`, `bronze`, `silver`, `gold`, `test`), `topology` (`1:1`, `n:1` many sources into one target, `1:n` one source into many) |
| `found` | `date`, and `by`: the test and its settings, or the run's name |
| `symptom`, `cause`, `effect`, `fix` | what was seen (with the error text), why, what a customer would see, and the fix or workaround |
| `versions`, `not_affected`, `where`, `refs`, `notes` | optional: versions it applies to, sources checked and not affected, the scripts or docs that handle it, public documentation |

A lookup leaves out findings only about the test itself (layer `test`) unless `--with-test` or `--layer test` is given. The same public-repo rules apply as for records: `finding.py` refuses what `record.py` refuses.

## Contributing in parallel

Each person or agent runs their own setup and records under their own run name, so contributions never touch the same file:

1. `recall.py --coverage` and [test-matrix.md](references/test-matrix.md): pick a gap, or a setup nobody has (a different VM, engine version, network path, workspace).
2. Run it with the skills, fixing any skill that's wrong along the way.
3. Record with `record.py` under a new run name (`<date>-<topic>`, add your handle to the topic if two people might pick the same one); write `results/<run>.md`.
4. One pull request: the run's two files, the skill fixes, and any new dimension values in test-matrix.md. `bin/test_scripts.sh` must pass; it validates every record and scans the write-ups for anything internal.

Never edit another run's file. If a result turns out wrong, record a new run that says so.

## Rules

- **Placeholders, not names.** IPs as `203.0.113.x`, `<connection>`, `<catalog>`, `<pipeline-id>`, `<workspace>`. Describe a catalog by its kind ("Default Storage", "own managed storage"), not its name.
- **Exact errors.** Copy the product's error text, after replacing names and IDs; that's what the next person will search for.
- **One record per cell per run.** A rerun after a fix is a new record with a new date or run name; don't edit old ones (the files are append-only, and `recall.py` shows the newest).
- **Say what wasn't tested.** A cell that was skipped, or tested only partly, is a `partial` or `blocked` record with a note, not a silent gap.
- **Results come from real runs only.** Never record a predicted or expected outcome as a result.
