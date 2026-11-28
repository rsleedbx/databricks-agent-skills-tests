---
name: secret-handling
description: "Use passwords, tokens and keys from files (source database logins, UC connection passwords, cloud keys, credential JSON) in scripts and agent sessions without printing them: read only named fields, pass a secret into one command's environment, check a value without showing it, keep files owner-only, block commands that would print a secret file, and what to do after a leak. Use whenever a command or script reads a credential file, creates a connection with a password, or a secret may have been printed."
---

# Secrets in scripts and agent sessions

Anything a command prints in an agent session goes into the model's context and into the session transcript on disk (Claude Code: `~/.claude/projects/<dir>/<session>.jsonl`). It can't be taken back: a printed secret is a leaked secret, and the fix is to rotate it.

## Rules

1. **Never print a secret file, not even a filtered view of it.** A filter that drops keys named `password` or `secret` misses nested and oddly named fields (`.dba.password`, `admin_pw`, a token inside a URL). Read the fields you need **by name**: `yq -r '.host_fqdn' creds.json`.
2. **A secret goes only into the one command that uses it**, through its environment or standard input, never as an argument (arguments show in `ps`) and never echoed:
   - `python3 scripts/secret_run.py <file> <field|-> <ENV> -- <command>`: `python3 scripts/secret_run.py creds.json .dba.password SQLCMDPASSWORD -- sqlcmd -S host,1433 -U admin -Q "SELECT 1"`.
   - Clients that read a password from the environment: SQL Server `sqlcmd` (`SQLCMDPASSWORD`), PostgreSQL `psql` (`PGPASSWORD`), MySQL `mysql` (`MYSQL_PWD`).
   - A JSON body with a password (a UC connection): build it with `PW=$(cat f) yq -n '.options.password = strenv(PW) | ...' > body.json`, `chmod 600`, pass it with `--json @body.json`, delete it after.
   - A password a script needs: a `*_PW_FILE` variable naming a private file (`LOGIN_PW_FILE=~/.secrets/login.pw`), read inside the script.
3. **Check a secret without showing it:** its length (`${#V}`), a hash prefix (`printf %s "$V" | shasum -a 256 | cut -c1-12`), or compare two copies (`[ "$A" = "$B" ]`). `yq` without `-r` quotes strings that contain special characters, so a comparison against unquoted text fails: use `yq -r`.
4. **Keep secret files owner-only** (`600`, their directories `700`): `python3 scripts/secret_perms.py <dir>` lists the others; `--fix` sets them.
5. **Never commit a secret, never add one to memory, notes or a results file.** Secret scanners in pre-commit hooks catch known formats only, not a random database password.

## Scripts

| Script | Use |
|---|---|
| [scripts/secret_run.py](scripts/secret_run.py) | Run one command with a secret from a JSON/YAML field or a password file in an env var |
| [scripts/secret_find.py](scripts/secret_find.py) | Where a secret's value appears (files, and git commits on any branch), without printing it |
| [scripts/secret_perms.py](scripts/secret_perms.py) | Secret files another local user can read; `--fix` |
| [scripts/block-secret-prints.py](scripts/block-secret-prints.py) | Claude Code hook that refuses a Bash command printing a secret file |
| [tests/test_secret_scripts.py](tests/test_secret_scripts.py) | Checks all four on a fake secret: `python3 -m unittest -q tests/test_secret_scripts.py` |

Run any script with no arguments for its usage (`python3 scripts/<name>.py`).

## Block printing in Claude Code

`block-secret-prints.py` is a PreToolUse hook: it refuses a Bash command in which a printing tool (`cat`, `head`, `yq`, `jq`, `python`, `sed`, `grep` without `-l`, …) reads a file matching a secret pattern, including inside `ssh host '...'` and `bash -c '...'`. Capturing into a variable (`PW=$(yq -r .password f) cmd`), writing the file, and the scripts above stay allowed. Add to `~/.claude/settings.json`:

```json
{"hooks": {"PreToolUse": [{"matcher": "Bash", "hooks": [{"type": "command",
  "command": "python3 /path/to/skills/secret-handling/scripts/block-secret-prints.py"}]}]}}
```

Patterns: the defaults (`*.pw`, `*credentials*.json*`, `*credentials*.y*ml*` (backups included), `*.pem`, `*.key`, `*_rsa`, `*_ed25519`, `.databrickscfg`, `.pgpass`, `.my.cnf`, `*.env`, `.netrc`), replaced by `~/.config/secret-handling/paths` (one glob per line; a glob with `/` matches the whole path, `~` expanded) or `SECRET_PATHS` (colon-separated). Add the directories your own tools keep credentials in. It is a guard against accidents, not against a determined reader: `X=$(cat f); echo "$X"` still prints.

## After a leak

1. **Rotate the secret** at its source (the database login, the token, the key). The old value is out; nothing else undoes that.
2. **Find every place it went:** `python3 scripts/secret_find.py <file> <field> ~/.claude "$TMPDIR" <repos...>` lists files and commits that hold it. A commit pushed to a shared remote needs the remote's owner (history rewrite, cache purge) besides the rotation.
3. **Update every copy of the new value:** the credential file, secret scopes, UC connections that use the login, CI variables.
4. **Fix the step that printed it** (a field read by name, `secret_run.py`, the hook) so the next run can't.
