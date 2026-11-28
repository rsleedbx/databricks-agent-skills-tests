# Working in this repository

For coding agents opened in a clone of this repo (the plugin's SessionStart hook, `bin/plugin_session.py`, says the same
to plugin users).

- **Python and REST, so a skill runs on the laptop and in a workspace (Genie Code, a notebook, a job).** The bash
  scripts are being converted to Python: write new code in Python, change the `.py` when a `.sh` has one next to it,
  and never turn a converted `.py` back into `.sh`. While the conversion runs, some pages still name a `.sh`: point
  them at the `.py`, don't fall back. Databricks calls go through `lfc_rest.Rest` ([lfc-rest](skills/lfc-rest/SKILL.md));
  the SDK only signs in, and there are no `databricks ...` subprocesses (the CLI doesn't run on notebook compute).
  `--profile <name>` picks the workspace on the laptop and is never chosen for the user; in a workspace leave it out,
  and `Rest.connect()` signs in as the workspace's own identity. Each script has `main(argv)`, so a notebook or Genie
  Code calls `main([...])` without a shell.
- **Ask each setup question once.** Before asking which cloud account, subscription, region, resource group,
  workspace, catalog or source host to use, read the user's saved answers:
  `eval "$(bash skills/lfc-test-settings/scripts/settings.sh env)"`. `settings.sh require <keys>` prints only the keys
  still missing: ask for those in one message, each with a recommendation, and `settings.sh set` them. With nothing
  saved yet, `settings.sh setup` lists what the CLIs already know first ([lfc-test-settings](skills/lfc-test-settings/SKILL.md)).
- **State the defaults first.** `bash skills/lfc-test-settings/scripts/settings.sh defaults` prints what the skills do
  unless the user says otherwise (a cloud VM of their own, the workspace's cloud and region, how the firewall is opened,
  serverless in tests, tags and cleanup): tell the user before the first test. Keep code and
  [defaults.yaml](defaults.yaml) in step: `bin/check_defaults.py` checks them.
- **Tools:** `bash skills/cli-setup/scripts/check-tools.sh local` (or `aws`, `azure`, `gcp`) names what's missing, with
  the install command; install nothing without asking.
- **Logins:** an expired workspace login often still works through **account-token-fallback** (`ws-token.sh`); ask for
  a login only when that fails, and then for the account login first.
- **No long live run for a change already backed by evidence.** When earlier runs (`recall.py` in
  [lfc-test-results](skills/lfc-test-results/SKILL.md)), the vendor's docs or a skill already show how it behaves, make
  the change, run the offline checks (`python3 bin/test_scripts.py`) and say what wasn't run; the next run that needs it
  shows the effect, and it's fixed then. Suggest a live run only when nothing covers the behavior, or the user asks.
- **A measurement run must be able to change a decision.** Before it, write down what it should show (from the
  model, the docs and the runs before it), which decision changes if it doesn't, and why the docs, the system tables
  and earlier runs can't settle it. No decision at stake: don't run it. Record the prediction with the result.
- **Before creating anything,** show what will be created (a diagram for a pipeline: **lfc-pipeline-diagram**) and wait
  for one yes. Clean up only what the run created.
- **Scripts, not instructions:** each skill runs the scripts in its own `scripts/` folder; run one with no arguments for
  its usage. [SCRIPTS.md](SCRIPTS.md) lists them all.
