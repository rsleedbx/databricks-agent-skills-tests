# Working in this repository

For coding agents opened in a clone of this repo (the plugin's SessionStart hook, `bin/plugin-session.sh`, says the same
to plugin users).

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
  the change, run the offline checks (`bash bin/test_scripts.sh`) and say what wasn't run; the next run that needs it
  shows the effect, and it's fixed then. Suggest a live run only when nothing covers the behavior, or the user asks.
- **Before creating anything,** show what will be created (a diagram for a pipeline: **lfc-pipeline-diagram**) and wait
  for one yes. Clean up only what the run created.
- **Scripts, not instructions:** each skill runs the scripts in its own `scripts/` folder; run one with no arguments for
  its usage. [SCRIPTS.md](SCRIPTS.md) lists them all.
