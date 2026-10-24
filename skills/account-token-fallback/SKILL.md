---
name: account-token-fallback
description: "Keep using Databricks workspace CLI profiles after their OAuth logins expire, with a token from a login you already have (the Azure CLI on Azure, or the account login that covers the same account), instead of logging in to each workspace again. Also checks every profile in ~/.databrickscfg at once (which log in, which a fallback covers, which need a login) and adds account profiles one standard way. Use when a workspace profile fails to authenticate (token refresh, invalid refresh token, 401), before a long test run, or when setting up profiles for many workspaces."
---

# Expired workspace logins: check, fall back, add accounts

Each workspace profile has its own OAuth login, and logins expire. Another login can often stand in:

- **Azure:** an Entra ID token from the Azure CLI (`az login`) works for every Azure Databricks workspace in that tenant, with no account profile at all.
- **AWS, GCP and Azure:** an account login (a profile on `accounts.*`) mints a short-lived token that the account's workspaces accept, unless the workspace takes only its own login (seen on field sandbox `fe-sandbox-*` workspaces).

Three scripts in [`scripts/`](scripts/):

```bash
S=skills/account-token-fallback/scripts
bash $S/profiles-check.sh --verify                 # every workspace profile: ok, covered, or needs-login, with the fix
CFG=$(bash $S/ws-token.sh <profile>) && [ -n "$CFG" ] && export DATABRICKS_CONFIG_FILE=$CFG    # one profile
CFG=$(bash $S/ws-token.sh new-ws --host https://<new-workspace>) && export DATABRICKS_CONFIG_FILE=$CFG   # no profile yet
bash $S/add-account.sh <account-id> <aws|azure|gcp>   # add an account profile, the standard way
```

## Check every profile: `profiles-check.sh`

One `databricks auth profiles -o json` (it validates every profile, a few seconds for dozens), then one line per workspace profile: cloud, account, whether its login is valid, a status, and the next step. Without `--verify` the status is a guess from the profiles alone: `ok`, `try` (expired, and a valid account profile or the Azure CLI may cover it), `account-expired`, `no-account-profile`, or `no-account-id`. Accounts that need a login follow, on stderr. **With `--verify` it tries each fallback** through `ws-token.sh`, so every expired profile ends as `covered` or `needs-login` with the reason. Use `--verify` before trusting the list: a valid account login doesn't mean the workspace accepts its token, and a workspace that no longer exists fails the same way. It changes nothing in `~/.databrickscfg` and deletes every token file it makes.

## Fall back for one profile: `ws-token.sh`

- **Its own login works:** prints nothing; keep using `~/.databrickscfg`.
- **It has expired:** on Azure, tries the Azure CLI token (and, when the workspace is in another Entra tenant, which the error names, that tenant if `az` is logged in to it); then each account profile with the same `account_id`. The first token the workspace accepts goes into a one-profile config file (mode 600, under `$TMPDIR`) named after the workspace profile, and the script prints its path. With `DATABRICKS_CONFIG_FILE` pointing at it, `--profile <profile>` keeps working, so skills and test sessions that pass `--profile` need no change.
- **Nothing works:** exits 1 and says which login to run.
- **A workspace you have no profile for yet** (one just created in an account you're logged in to): `ws-token.sh <name> --host <workspace URL> [--account-id <id>]` mints the same way and writes the config file with `<name>` as its profile. Without `--account-id` it tries each account profile on the workspace's cloud until the workspace accepts a token.

`run.sh` calls it before the first session, and `bin/run_matrix.sh` again before the compute readback, so a matrix run doesn't stop at an expired workspace login.

## Add an account profile: `add-account.sh`

The standard name is **`account-<cloud>-<account-id>`**, logged in to the cloud's account host (`https://accounts.cloud.databricks.com`, `https://accounts.azuredatabricks.net`, `https://accounts.gcp.databricks.com`) with `--account-id`. If a valid account profile for that account already exists, whatever its name, it's reused and nothing is added. If only expired ones exist, it logs in to one of them again rather than adding a duplicate. Existing profiles keep their names, since other tools may refer to them; the standard name is for accounts that have none yet. The login opens a browser, so run it in your own terminal (in Claude Code: `! bash .../add-account.sh <id> <cloud>`). `--dry-run` shows the command. The account ID of a workspace profile is its `account_id` in `~/.databrickscfg`, which `databricks auth login` writes; `profiles-check.sh` prints the ones that need an account profile.

## When a login never finishes

`databricks auth login` (and `add-account.sh`) opens a browser, which returns to a local port, `127.0.0.1:8020` by default, when you sign in. If another program holds that port, the browser says you're signed in but the command keeps waiting, and the profile isn't saved. Check with `lsof -nP -iTCP -sTCP:LISTEN | grep ':80[23][0-9]'`.

- **Seen holding it:** another program on the laptop, restarted within a second by its launch agent when stopped, holding 8020, 8021 and 8022; and old `databricks auth login` processes, from the CLI or an IDE extension, left waiting for weeks. Stop the stale logins.
- **Another port:** the CLI takes `DATABRICKS_OAUTH_CALLBACK_PORT`, and since v0.265.0 falls back through ports from 8020 on its own, but a program holding several ports in a row defeats that. `add-account.sh` picks the first free port from 8021 to 8030 itself, and leaves the program alone (tested 2026-10: an AWS account login completed on port 8023 while another program held 8020 to 8022). With `--free-port` it stops the program instead and starts the login right away, before the program comes back; ask the user first, since it can belong to another app.
- **Finish the sign-in in the newest tab.** A tab from a login that was stopped returns to nothing: the browser says you're signed in, and nothing is saved.
- **Check which CLI runs:** `which -a databricks`. An older second copy (seen: v0.209.1 in `/usr/local/bin` next to v1.21.0 from Homebrew) has no port fallback.

## Rules

- A minted token lasts about an hour. Rerun `ws-token.sh` when calls start failing, or before a long run.
- Never print a token or copy a config file elsewhere; delete it when done. It holds a live credential.
- **The scripts never read the CLI's config file themselves.** Profile fields (host, account_id, which profiles are account profiles) come from `databricks auth profiles --skip-validate -o json`, and the CLI reads the file. Security hooks in some agent setups block any command that names the file, and a parser of our own would drift from the CLI's rules. `DATABRICKS_CONFIG_FILE_SOURCE` still points the scripts at another real config file.
- An account login acts as you: it doesn't need account admin, and it gives no more than your own workspace permissions.
- Tested 2026-10 against 30 workspace profiles on AWS, Azure and GCP (`profiles-check.sh --verify`, 41 s): 8 logged in on their own; the Azure CLI covered 4 expired Azure profiles, one of them in a second Entra tenant; an account login covered an AWS field sandbox workspace when forced (not account admin). Field sandbox `fe-sandbox-*` workspaces refused account tokens, a deleted workspace failed the same way, and most of the rest needed an account login that had itself expired.
