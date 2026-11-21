# Contributing

Results, fixes and new tests are welcome. The repo works like a shared notebook ([How to use this repo](README.md#how-to-use-this-repo)): each run adds its own records, so people testing in parallel never edit the same file.

## How a change gets in

1. Fork the repo, and make one topic branch per change from `main` ([git-topic-branches](.claude/skills/git-topic-branches/SKILL.md)).
2. Run the checks your change touches: `bash bin/test_scripts.sh`, `uv run bin/check_links.py`, and `uv run bin/scripts_index.py` when you add or change a script ([docs/running-tests.md](docs/running-tests.md)).
3. Open a pull request to `main`. The maintainer reviews and merges it; while there's one maintainer, the maintainer's own changes are merged after the checks above pass, and a second reviewer is added when there is one.

## The reuse hook

With Claude Code, the repo's [.claude/settings.json](.claude/settings.json) runs `bin/repo_reuse_hook` before an edit: when the edit adds a function like one the repo already has, it asks, naming the existing function. Use or extend that one unless the job differs. It compares only with this repo's code and sends nothing anywhere. It's a vendored copy ([.vendored.json](.vendored.json)), so change it upstream, not here. If you run a reuse hook of your own across your repos, set `REUSE_FIRST_GLOBAL=1` in your Claude Code settings' `env` and this one stays silent.

## Adding test results

Record each result with `record.py` ([lfc-test-results](skills/lfc-test-results/SKILL.md)). With the skills installed as a plugin, your records are in `~/.lfc/tests/results/`: copy the run files into `results/runs/` of your fork and open a pull request.

## Layers

Every skill and bin script is in a layer ([docs/layers.md](docs/layers.md)). A new one goes into [layers.yaml](layers.yaml) under the layer it belongs to, and a reference may only point to a layer that layer may use: `bin/check_layers.py` (run by `bin/test_scripts.sh`) fails otherwise. A fix that removes a reference from [layers-baseline.txt](layers-baseline.txt) is welcome; adding a line to it needs a reason in the pull request.

## What can't go in

This repo is public. No customer data, internal information, IPs, hostnames, IDs, emails, workspace or catalog names, or credentials, in code, results, docs or commit messages ([SECURITY.md](SECURITY.md)).

## Questions and access

Open an issue. Maintainers: the repository owner ([@rsleedbx](https://github.com/rsleedbx)).
