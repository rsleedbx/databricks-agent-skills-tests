# Running the tests

Setup, the test kinds, and how results are recorded. Back to the [README](../README.md).

## Layout

```
run.sh                 # runner: one headless `claude -p` session per prompt, multi-turn via `tag+|`
summarize.py           # skills loaded, reference files read, answer preview
env.sh                 # sources env.local.sh, sets SKILLS_REPO / OMNIGENT_WS / PIPELINE_WS
env.example.sh         # copy to env.local.sh (gitignored) and fill in
sync.sh                # workspace file bridge for sandboxes without git push
bin/run_matrix.sh      # the whole Lakeflow Connect matrix, one batch per mode, then a compute readback
bin/run_batch.sh       # run several prompt files in parallel, each in its own scratch dir
bin/compute_report.sh  # per pipeline: serverless asked, ran on, started, reached the source
bin/stop_after.sh      # stop pipeline updates that run past N minutes
bin/test_scripts.sh    # regression tests for the skills' scripts (offline, or on Postgres and MySQL)
bin/check_links.py     # every relative link in every skill
pr-notes/              # PR descriptions with the test matrix, one per skills-repo branch
prompts/
  lakeflow-connect.txt # routing / guidance prompts (no resources created)
  podman-desktop-remove.txt # laptop disk: Podman Desktop and its VM (report only, nothing removed)
  lfc-ui-capture.txt   # UI captures: is the Chrome tool attached to your Chrome (nothing created)
  lfc-matrix/          # Lakeflow Connect deploy tests: 5 sources x 4 modes, plus DML (MATRIX.md)
results/               # runs/<run>.jsonl records and <date>-<topic>.md write-ups
skills/                # test-only skills: source setup, data generation, firewall, results
```

## Setup

Check out this repo next to the skills repo (or set `SKILLS_REPO`):

```
~/github/databricks-agent-skills
~/github/databricks-agent-skills-tests
```

```bash
cp env.example.sh env.local.sh    # fill in profiles, catalog, connections (never committed)
source env.sh
```

When the workspace profile's OAuth login has expired, `run.sh` mints a token from your account login for the same account and keeps going ([account-token-fallback](../skills/account-token-fallback/SKILL.md)); log in to the account once with `databricks auth login --host <accounts-url> --account-id <id> --profile <name>`.

`run.sh` copies the skills repo's `skills/` into the Claude plugin cache before each run, so
sessions test your local edits. Sessions run in a scratch `WORK_DIR` under `/tmp`, outside both
repos, so skills load from the plugin cache and generated bundles stay out of git.
`PIPELINE_WS` is passed to Claude as `DATABRICKS_CONFIG_PROFILE`, so the Databricks skills
target that workspace. Sessions run with `--verbose --output-format stream-json`
(`stream-json` fails without `--verbose`).

## Script tests

The skills' scripts (planners, `python/fill.py`, the statschema pipeline, the firewall DuckDB queries) have their own regression tests:

```bash
bin/test_scripts.sh                    # offline: syntax, links, the six TPC schemas through shape.py and ddl.py, round trips
PSQL="podman exec -i dg-pg psql -U postgres" MYSQL="podman exec -i dg-my mysql -uroot" bin/test_scripts.sh   # and fill on both engines
bin/check_links.py                     # every relative link in every skill
```

A check runs only when what it depends on changed since it last passed on this machine
([bin/test_gate.py](../bin/test_gate.py), with [bin/test_skip](../bin/test_skip/scripts/test_skip.py)). Its
fingerprint is its command plus the files it depends on: the repo paths it names, a skill widened to the skills and bin
files its code references, a bin tool to the whole repo. The checks that pass files through the run's temp folder
run or skip together. A check that names no repo path always runs, as do the `bash -n` loops (milliseconds each).
Passes are kept outside the repo (one ledger per checkout in `~/.cache/test-skip/`; `TEST_SKIP_DIR` moves it), so CI and a fresh
checkout run everything. `TEST_SKIP_AGAIN=1 bin/test_scripts.sh` runs every check. On 2026-11-20 a run with nothing
changed took 33 s instead of 3 min 15 s.

The live runs skip what didn't change too:
- `run.sh` (and so `bin/run_batch.sh` and `bin/run_matrix.sh`): a tag and its follow-up turns run only when their
  prompts (as filled), the workspace profile or the skills repo's `skills/` changed since the tag last passed on this
  machine (its session exited 0). A skipped tag leaves no new `<tag>.json` in the results.
- [lfc-saas-pipeline/scripts/run_tests.py](../skills/lfc-saas-pipeline/scripts/run_tests.py): a connector's bundle and
  notebook run only when its notebook, bundle, the package, its UC connection's `updated_at` or the workspace changed.
`TEST_SKIP_AGAIN=1` (or `run_tests.py --again`) runs everything.

With databases, it fills TPC-C and the TPC-H spec schema on each engine (schemas named `test_*`) and checks every table's target, the spec's rules, and the audit. Run it after changing a skill's scripts.

## Schema-change tests

Each script answers one schema-evolution question by running it on a fresh source database, prints one PASS or FAIL row per claim, and deletes everything it created. First: does `ADD COLUMN` on SQL Server need a full refresh (about 25 minutes). See [lfc-schema-change-test](../skills/lfc-schema-change-test/SKILL.md) for what it needs and the command; it also has a prompt to hand the rerun to a coding agent.

## Guidance tests

No resources created:

```bash
./run.sh "$SKILLS_REPO" prompts/lakeflow-connect.txt
./summarize.py /tmp/lakeflow-connect-results
```

## Deploy tests: the Lakeflow Connect matrix

**These create real, billable resources in `PIPELINE_WS`.** Each prompt file is two turns: a
plain request (the skill must ask before creating anything), then the answers and go-ahead.
`${VAR}` placeholders come from `env.local.sh`.

Run the whole matrix with `bin/run_matrix.sh` ([MATRIX.md](../MATRIX.md) has every cell and its pass criteria). It runs one mode per batch, so tests on the same connection don't race for the same default pipeline name (the skill names pipelines `<firstname><lastname>-<connection>`, then `-2`, `-3`; standard CDC uses `-gw` / `-ap`), and ends by reading back what compute each pipeline ran on:

```bash
LFC_ENV=<name> source env.sh
bin/run_matrix.sh                                  # qbc -> fcqbc -> icdc -> scdc -> dml
bin/run_matrix.sh icdc scdc                        # only some modes
LFC_SERVERLESS=unset bin/run_matrix.sh icdc        # take the product's compute default instead of serverless: true
bin/run_batch.sh prompts/lfc-matrix/qbc-*.txt      # one batch by hand
```

The prompts ask for `serverless: true` by default (`LFC_SERVERLESS`), because the test firewall allows only the serverless ranges; see "Workspace requirements" in MATRIX.md. Check the workspace first with the `ready.py` check (databricks-lakeflow-connect `scripts/`): previews, compute, target catalog, grants, egress ranges.

Integrated CDC updates can run a long time on an idle source. Stop them after 30 minutes:

```bash
bin/stop_after.sh 30 <pipeline-id> <pipeline-id>
```

What to check in each result (`/tmp/<name>-results/p1.json`, or the session's final answer):

- **Pipeline names** follow the convention, and nothing was created before the go-ahead.
- **The rows-per-run report** matches the source: snapshot counts equal source row counts
  (distinct rows for tables without a primary key under CDC).
- **Expected failures leave nothing behind**, especially Postgres replication slots and
  publications.
- **No secrets on disk:** the Lakebase demo writes `conn.json` with a generated password and must
  delete it.

### Clean up

After a deploy test: `databricks bundle destroy` in each `/tmp/<name>-work`, stop any standard
CDC gateway (`databricks pipelines stop <gateway-id>`), and drop any Postgres slot a test
created. The skill's own cleanup steps cover this; check they ran.

## Prompt file format

One `tag|prompt` per line. A `tag+|text` line is a follow-up turn: it resumes the session started
by `tag` and appends to the same `/tmp/<name>-results/<tag>.json`.

```
p1|Set up a Lakeflow Connect query-based ingestion pipeline from the existing connection ${LFC_PG_CONNECTION} in the workspace for Databricks CLI profile ${PIPELINE_WS}.
p1+|Use the source tables intpk and dtix ... Yes, create everything, run it once, then show the rows per table per run report.
```

## Permissions for headless sessions

Headless sessions can't answer permission prompts. `bin/run_batch.sh` passes a scoped allowlist
through `CLAUDE_ALLOWED_TOOLS`, as a single `--allowedTools` argument so rules with spaces
survive. That covers the Databricks CLI, file tools, `jq`, `WebFetch` (skills check current docs),
and `rm` of `conn.json` by every path a session might use (`/tmp` resolves to `/private/tmp` on
macOS). Don't put rules with spaces in `CLAUDE_ARGS`: it's split on spaces.

## Workspace file bridge

When editing in a sandbox (Omnigent, Solution Builder) with no git push access, `sync.sh`
uploads and downloads individual files through a Databricks workspace (`push` / `pull` for skill
files, `push-tests` / `pull-tests` for this repo).

- `workspace import --format SOURCE` stores every file as a notebook with a
  `# Databricks notebook source` first line; `sync.sh pull` strips it (`tail -n +2`).
- Don't send a `git format-patch` through the bridge: the notebook round-trip shifts context lines
  and `git apply` fails. Send the changed files.
- Keep shell scripts sent this way ASCII (`--`, not an em-dash): the round-trip can mangle
  other characters.
- After pulling skill files: `python3 scripts/skills.py generate`, then `validate`, in the
  skills repo.

## Test results

Every run is recorded in [`results/`](../results/): `runs/<run>.jsonl` holds one record per test cell (what, where, outcome, counts, the exact error, both repos' commits), and `results/<date>-<topic>.md` is the write-up. See what's been tested before running anything:

```bash
uv run skills/lfc-test-results/scripts/recall.py                    # latest result per test x engine x host
uv run skills/lfc-test-results/scripts/recall.py --engine oracle --all
```

Record new results with `skills/lfc-test-results/scripts/record.py`, which refuses anything that looks internal (this repo is public). The [lfc-test-results](../skills/lfc-test-results/SKILL.md) skill has the format and rules.
