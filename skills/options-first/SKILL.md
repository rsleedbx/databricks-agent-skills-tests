---
name: options-first
description: "Before building or testing a solution, list every way to do it, rank the ways by how simple they are to set up and operate, check what existing skills already have for each, and test the simplest first through the events that break it. Records the comparison as options.yaml next to the skill, with scripts to render, check and index all of them. Use whenever a question has more than one possible answer: a workaround, a setup, a pipeline design, a data path, a tool choice."
---

# Options first

Most questions have several answers. The one that gets built first is often the one that came to mind first, not the simplest one to set up and operate. A customer runs it for years, so operating effort counts as much as setup.

## Steps

1. **List the options** before building any. Include the ones that need nothing built (a setting, a built-in mode, the docs' own answer) and the ones a docs page says aren't supported (mark them so). Ask: what would the product team, the asker and a skill in this repo each try?
2. **Search the repo's skills for each option** (`git grep -il '<feature>' -- skills`): an option a skill already tests, or a harness that can be extended, costs little to try. Read its trade-offs file if it has one.
3. **Score each option**, 1 (nothing to build or watch) to 5 (custom code you own):
   - **setup:** what has to be created (pipelines, jobs, code lines, permissions);
   - **operate:** what someone has to do while it runs (scans, monitoring, recovery steps, cost);
   - **breaks:** the events that can break it (a full refresh, a schema change, a restart, a gap, retention running out, a rerun) and the recovery for each.
4. **Write `options.yaml`** next to the skill that answers the question (format: run `scripts/options.py` with no arguments) and render it: `uv run scripts/options.py render <skill>/options.yaml`.
5. **Test from the top of the ranking**, each option through its break events, not only the happy path. Stop at the first one that passes; record the rest as `untested` with what testing them would take. If the simplest fails, write why in its row and test the next.
6. **Answer with the pick first**, then the alternatives and their status, so the reader sees the simple way and what it gives up.
7. **Update the file** when an option's status changes (a new test, a docs change, a new feature). `uv run scripts/options.py index` lists every topic's choices and pick across the repo, so the next question on the topic starts from it.

## Scripts

| Script | Use |
|---|---|
| [scripts/options.py](scripts/options.py) | `render` one options.yaml as a ranked table with the pick; `check` files; `index` all of them |
| [scripts/test-options.sh](scripts/test-options.sh) | Checks options.py on a sample file |

Ranking: usable status first (`tested`, `partly-tested`, `docs`, `untested`, `not-supported` last), then setup plus operate effort, then the number of break events. The pick is the first `tested` or `docs` option.

## Example

**lfc-downstream-history** `options.yaml`: keeping a Lakeflow Connect table's history when the connector writes SCD type 1 only. Six options, from the connector's own setting to a hand-written MERGE. Each has its break events, and the two pipelines that were built have a harness that tests them through a full refresh.
