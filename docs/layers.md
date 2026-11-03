# Layers

This repo is one repository for now, and it stays one while the skills change quickly. Its skills and bin scripts are sorted into layers so that a later split into separate repositories is a mechanical step (paths only, no code changes), and so that the dependencies between layers point one way. [`layers.yaml`](../layers.yaml) says which skill or bin script is in which layer, and which layers each may use. [`bin/check_layers.py`](../bin/check_layers.py) checks every reference in the repo against it, and [`bin/test_scripts.sh`](../bin/test_scripts.sh) runs the check.

## The rule

A skill or bin script may reference another one only if the other one's layer is the same as its own, or is in its layer's `may_use`. Dependencies point down:

```
           repo (tooling: bin/, contributor skills; may use every layer, nothing uses it)
             |
   lakeflow -----> contract ----> common
      |               ^             ^
      +-----------> sources ---------+   sources also uses shape
                        |
                      shape --------> contract, common
```

| Layer | May use | What it holds | Skills and bin scripts |
|---|---|---|---|
| **common** | nothing | Helpers any layer may use: credentials, CLI JSON, long waits, token and tool checks, the rules for writing skills and branches | `account-token-fallback`, `options-first`, `podman-desktop-remove`, `secret-handling`, `cli-setup`, `shell-json-scripting`, `watch-progress`, `skill-token-budget`, `git-topic-branches` |
| **shape** | contract, common | Data shaped like a customer's or a benchmark's, engine-agnostic: the schema and statistics, and the data generated inside any database. Its producer writes the workload file | `lfc-source-statschema`, `lfc-source-datagen` |
| **contract** | common | The contracts between a producer and its consumers: the source contract (what a source provides), and the workload contract (a model's tables and the commands that build them) | `lfc-source-contract`, `lfc-workload-contract` |
| **sources** | shape, contract, common | Source databases on demand: provisioning, reaching and opening them | `lfc-source-podman`, `lfc-source-cloud-vm`, `lfc-source-azure-sql`, `lfc-source-azure-flexserver`, `lfc-source-gcp-cloudsql`, `lfc-source-aws-rds`, `lfc-source-aws-aurora`, `lfc-source-firewall`, `lfc-source-eventhubs` |
| **lakeflow** | contract, common | Lakeflow Connect tests and demos, and the prep and checks around them | every other skill, and the `bin/` scripts `lfc-events.sh`, `lfc-runtime.sh`, `compute_report.sh`, `stop_after.sh` |
| **repo** | all | This repo's own tooling: checks, the test runners, the plugin hook | `public-skill-conventions` (contributor skill), `bin/` checks and runners (`check_links.py`, `check_layers.py`, `lint_scripts.py`, `scripts_index.py`, `run_batch.sh`, `run_matrix.sh`, `plugin-session.sh`, `test_*`) |

Within a layer any reference is allowed. A reference is a path to another skill or bin script: `skills/<x>/...`, `../<x>/...`, `$SK/<x>`, `<x>/scripts/...` written bare, or `bin/<file>`. A name in plain prose is not a reference.

### Why each skill is where it is

One line per skill: what it does, which decides its layer.

| Skill | Layer | Why |
|---|---|---|
| `account-token-fallback` | common | Gets a workspace token from a login the user already has; no database. |
| `options-first` | common | Lists and ranks the ways to do a task; a method, no tooling. |
| `podman-desktop-remove` | common | Removes Podman Desktop from a Mac so its VM stops filling the disk; host cleanup, no database. |
| `secret-handling` | common | Reads passwords and keys without printing them; every layer reads credentials. Also holds `pw.sh` and `pw.py` (the password reference used by source and Lakeflow scripts). |
| `cli-setup` | common | Checks the command-line tools are installed and records a cloud identity (`check-tools.sh`, `cloud-identity.sh`), which sources and Lakeflow scripts both source. Moved here from lfc-source-setup. |
| `shell-json-scripting` | common | How to read CLI JSON in shell (jq, yq, DuckDB); used by every script. |
| `watch-progress` | common | Waits on a long job with backoff; generic. |
| `skill-token-budget` | common | How to write any skill so it loads few tokens; the rule applies to every layer. Its check commands are repo tooling (baseline). |
| `git-topic-branches` | common | The branch rule for contributors; a workflow, no code. |
| `lfc-source-statschema` | shape | Builds a source shaped like a customer's or a TPC schema from statschema's format; engine-agnostic. |
| `lfc-source-datagen` | shape | Fills any database with generated rows, from the database itself; engine SQL only. Its `workload.sh` writes a model's workload file (`workload.json`) for the tests. |
| `lfc-source-contract` | contract | The contract between a source and its consumers. |
| `lfc-workload-contract` | contract | The contract between a model (statschema's format) and the tests that build its tables: the model file's format and the workload file. A validator and an export script; no database. |
| `lfc-source-podman` | sources | Runs source databases in Podman containers. |
| `lfc-source-cloud-vm` | sources | Provisions databases on cloud VMs (`slot.sh`, VM setup, masters). |
| `lfc-source-azure-sql` | sources | Provisions Azure SQL Database and Managed Instance. |
| `lfc-source-azure-flexserver` | sources | Provisions Azure Database for MySQL and PostgreSQL Flexible Server. |
| `lfc-source-gcp-cloudsql` | sources | Provisions Cloud SQL databases. |
| `lfc-source-aws-rds` | sources | Provisions RDS databases. |
| `lfc-source-aws-aurora` | sources | Provisions Aurora clusters. |
| `lfc-source-eventhubs` | sources | Provisions an Azure Event Hubs namespace as a Kafka broker and writes its source contract (engine `kafka`); `lfc-kafka-freshness-test` reads it as `SOURCE_JSON`. Moved here from that skill. |
| `lfc-source-firewall` | sources | Opens a source's firewall to the laptop and the workspace's egress; reaches the database's network. |
| `lfc-source-setup` | lakeflow | Lakeflow source prep: utility objects, CDC and change tracking, grants (per engine). Its per-engine pages are what the source skills point to (baseline). |
| `lfc-source-log-state` | lakeflow | Reads where a SQL Server source's change tracking and CDC stand, for a Lakeflow pipeline. |
| `lfc-source-sizing` | lakeflow | How much of a source Lakeflow Connect moves. |
| `lfc-schema-change-test` | lakeflow | Runs Lakeflow schema-change tests end to end. Its many-table build takes a workload file (contract); the cloud VM's container script is sources: baseline. |
| `lfc-downstream-history` | lakeflow | Keeps history downstream of a Lakeflow destination. |
| `lfc-connection-sync` | lakeflow | Creates the same Unity Catalog connections in several workspaces. |
| `lfc-workspace-setup` | lakeflow | Prepares a workspace for Lakeflow tests (previews, catalogs). |
| `lfc-meta-ads` | lakeflow | Lakeflow Connect Meta Ads pipelines. |
| `lfc-saas-answer` | lakeflow | How a SaaS connector behaves when the source can't be stood up. |
| `lfc-test-results` | lakeflow | Records and recalls Lakeflow test results. |
| `lfc-ui-capture` | lakeflow | Captures the Lakeflow wizard as masked screenshots. |
| `cdc-table-plan` | lakeflow | Table roles and silver layout for a CDC plan. |
| `zerobus-workspace-setup` | lakeflow | Zerobus previews in a workspace. |
| `zerobus-debezium-test` | lakeflow | Debezium to Zerobus change-data tests (it provisions a source: baseline). |
| `mautic-source` | lakeflow | A real application's database as a CDC test source (the Lakeflow test side). |
| `lfc-pipeline-diagram`, `lfc-sqlserver-secondary` | lakeflow | Pending on open branches; listed in `layers.yaml` so they classify when they land. |
| `public-skill-conventions` | repo | Conventions for the public skills, checked by its own script. |
| `bin/lfc-events.sh`, `lfc-runtime.sh`, `compute_report.sh`, `stop_after.sh` | lakeflow | Pipeline events, runtime and reports, stopping updates: Lakeflow only. |
| `bin/check_*.py`, `lint_scripts.py`, `scripts_index.py`, `test_*`, `run_batch.sh`, `run_matrix.sh`, `plugin-session.sh` | repo | The repo's checks, runners and plugin hook. |

## Moved in this change

| From | To | Why |
|---|---|---|
| `lfc-source-setup/scripts/pw.sh`, `pw.py` | `secret-handling/scripts/` | Password reference used by sources, Lakeflow and connection scripts; a secret-handling helper. |
| `lfc-source-setup/scripts/cloud-identity.sh`, `check-tools.sh` | `cli-setup/scripts/` (new common skill) | Used by sources and by lfc-source-setup alike; neither is about Lakeflow or a particular engine. |

Nothing was moved that the internal tool uses. The internal tool uses `skills/account-token-fallback/scripts/ws-token.sh` (common, unchanged) and `skills/lfc-source-cloud-vm/scripts` (sources, unchanged).

## The baseline

[`layers-baseline.txt`](../layers-baseline.txt) lists the references that break the rule today: `<from> -> <to>: <file>:<pattern>`, one per line. The check fails on any reference that breaks the rule and has no line there, and on any line that matches no reference, so the baseline only shrinks: delete a line when its reference goes. Today it holds 45 references in 37 lines (of 175 references in all; 130 allowed).

| Pair (layer) | References | Main offenders | What it would take |
|---|---|---|---|
| sources -> lakeflow | 19 | The source skills link to `lfc-source-setup`'s per-engine pages (Lakeflow prep: CDC, grants) as the next step, and `lfc-source-firewall` cites a Lakeflow script and test matrix in its references. | Split `lfc-source-setup` into engine provisioning (sources) and Lakeflow prep (lakeflow); the source skills then link only to their own layer. |
| lakeflow -> shape | 5 | `cdc-table-plan` uses statschema's collection SQL; `zerobus-debezium-test` uses statschema and datagen. (`lfc-schema-change-test` takes a workload file instead.) | Lakeflow tests take a built source (a schema file or a database) from the shape layer's output rather than calling its scripts. |
| lakeflow -> sources | 8 | `lfc-schema-change-test` calls the cloud VM's container script; `zerobus-debezium-test` runs the VM, Podman and cloud-links scripts (it provisions a CockroachDB source). | Move the provisioning to the caller's configuration, or let the test skill own a source-side wrapper in sources. |
| common -> repo | 7 | `skill-token-budget` and `account-token-fallback` name the repo's checks and runner in their text. | Keep as contributor guidance, or reword to the repo's CONTRIBUTING; this is the only edge a split must rewrite in a common skill. |
| lakeflow -> repo | 4 | `lfc-test-results` and `lfc-workspace-setup` name `bin/test_scripts.sh` and `bin/run_batch.sh`. | Same as above: repo tooling is referenced by name, not run by path. |
| shape -> lakeflow | 2 | `lfc-source-statschema` points to `lfc-source-sizing`'s workload patterns; `lfc-source-datagen` points to `lfc-source-setup`'s Postgres page. | Drop the two pointers, or move those pages into shape. |

The lists above are a snapshot; `python3 bin/check_layers.py --baseline` prints the current lines, and `--report` the table by pair.

## The check

```bash
python3 bin/check_layers.py                  # the check (test_scripts.sh runs it)
python3 bin/check_layers.py --report         # the table by layer pair, and each skill-level edge
python3 bin/check_layers.py --baseline       # the current disallowed references, as baseline lines
python3 bin/check_layers.py --paths sources   # the paths of one or more layers, for a split
python3 -m unittest -q bin/test_check_layers.py
```

A new skill must be added to `layers.yaml`, under the layer it belongs to, or the check fails and says so. A new bin script goes under `bin:` of its layer. A name that another branch will add goes under `pending:`; it classifies whether or not it's on main yet. The check doesn't read the YAML with a library: it reads the subset `layers.yaml` uses, so it runs on a stock Python.

## statschema

[`lfc-source-statschema`](../skills/lfc-source-statschema/SKILL.md) is a fork of statschema, and the fork is ahead of its upstream. Its SKILL.md has the table of what changed (the collectors, the one DDL emitter, generation inside the database, the replay). The model lives in its `scripts/statschema/` folder, so nothing is installed from git. Upstreaming those changes to the upstream repository is a later step; this change does not de-vendor or edit the code. In the layers it sits in shape, and a split takes the code with it.

## Splitting later

The split is three repositories: **shared** (common, contract, shape: everything the other two use), **sources**, and **lakeflow**. Each keeps the paths of its layers, and each runs the same check on its own `layers.yaml`.

It is ready when the baseline has no line across the split. Today 34 references cross it (sources to lakeflow 19, lakeflow to shape 5, lakeflow to sources 8, shape to lakeflow 2), so those go first. The common-to-repo and lakeflow-to-repo lines stay, for each repo's own tooling. The references that stay allowed (sources to shape, contract and common; shape to contract and common; lakeflow to contract and common) become dependencies on the shared repo: each consumer pins a revision of it, by checkout or by the plugin's dependency, and the internal tool does the same.

```bash
# one clone per target repo; each keeps only its layers' paths
git clone --no-local <this repo> ../lfc-tests-sources && cd ../lfc-tests-sources
python3 bin/check_layers.py --paths sources > /tmp/paths-sources.txt
git filter-repo --paths-from-file /tmp/paths-sources.txt      # plus the root files that repo needs (README, LICENSE, ...)

git clone --no-local <this repo> ../lfc-tests-lakeflow && cd ../lfc-tests-lakeflow
python3 bin/check_layers.py --paths lakeflow > /tmp/paths-lakeflow.txt
git filter-repo --paths-from-file /tmp/paths-lakeflow.txt

git clone --no-local <this repo> ../lfc-tests-shared && cd ../lfc-tests-shared
python3 bin/check_layers.py --paths shape,contract,common > /tmp/paths-shared.txt
git filter-repo --paths-from-file /tmp/paths-shared.txt
```

Run `git filter-repo --help` on the machine that does it to confirm the `--paths-from-file` form. Then in each repo: drop the other layers from `layers.yaml`, run `python3 bin/check_layers.py`, and the check must pass with the baseline lines that stay in that repo (common-to-repo and lakeflow-to-repo). Each repo then needs its own copy of `bin/check_layers.py` and `bin/test_scripts.sh`, and the repo-level files (README, CONTRIBUTING, SCRIPTS.md, `results/`) are assigned by hand: `SCRIPTS.md` is regenerated per repo by `bin/scripts_index.py`.

What the internal tool would change, when the split happens (it calls this repo by path, through its layout settings and `$LFC_TESTS`):

| Path it uses today | Layer | After the split |
|---|---|---|
| `skills/account-token-fallback/scripts/ws-token.sh` | common | the shared repo |
| `skills/lfc-source-cloud-vm/scripts` (copied to the VM) | sources | the sources repo; its path setting moves to that repo |
| `skills/lfc-source-log-state/scripts/sqlserver-log-state.sh` (in its contributor skill) | lakeflow | the lakeflow repo |
| `skills/lfc-schema-change-test/scripts/lib.sh` (in its docs) | lakeflow | the lakeflow repo |
| `$T/skills/skill-token-budget`, `$T/skills/public-skill-conventions` (in its docs; these live under `.claude/skills/` here) | repo | the repo that keeps them; also fix the path in its docs |
| the repo list in its layout and plugin-repos scripts | all | one entry per new repo, with its own variable |

The plugin, if the packaging answer below allows one plugin per layer, becomes one plugin per repo with the same skills, so users install what they need.

## Plugin packaging

Checked on 2026-11-02 against Claude Code's plugin documentation (code.claude.com/docs/en/plugins/reference, plugin-marketplaces, plugins/marketplace-reference, plugins/loading, plugins/host-marketplace, plugins/cli-reference) and the installed CLI (`claude plugin validate`). The docs are read through a summarizing fetch, so the quotes are the summaries' wording, not raw HTML.

**Answer: one marketplace can list several plugins from the same repo, each with a subset of the skills, but only in two shapes, and neither is in use.**

- **Shape A: one root plugin, entry-level `skills`.** A marketplace entry whose `source` is `"./"` and that lists `skills` loads only those folders; the default `skills/` scan is skipped. No file moves. But every `"./"` entry reads the same root `plugin.json`, so a second per-layer plugin would share the name `lfc-tests`. The docs don't say what two entries sharing one manifest do, and `claude plugin validate` passes a two-entry fixture without loading it. Untested, so not added.
- **Shape B: one folder per plugin.** Each plugin has its own folder with its own `skills/`. Skill folders have to sit inside that folder (the docs reject `../` paths and paths that leave the plugin root), or be symlinks. The symlink rules differ between two pages of the docs. This needs file moves, so it's not done.

What was done: `lfc-tests` is unchanged (same name, all skills, same marketplace entry), and no per-layer plugin was added. `claude plugin validate .` passes with two warnings that were there before this change: no marketplace description, and no plugin icon.

What a per-layer plugin would need, in order:

1. Shape B folders, `plugins/<layer>/` each with `.claude-plugin/plugin.json`, the layer's skills moved into `plugins/<layer>/skills/`, and the layer's `bin/` scripts available to it. The `SessionStart` hook (`bin/plugin-session.sh`) would need a copy in each plugin, since its path must stay inside the plugin root. That is the move the change above avoids.
2. A marketplace entry per plugin with `source: "./plugins/<layer>"`, and an entry name equal to its manifest name.
3. `claude plugin validate .` for the marketplace, then `claude plugin validate ./plugins/<layer>` for each plugin (the marketplace check doesn't open the plugins' skill files).
4. An install test in a clean profile: that each plugin lists only its skills, and that `lfc-tests` still lists all of them.

Once the layers are separate repositories (above), each repository is its own marketplace with one plugin, and this question goes away.
