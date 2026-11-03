---
name: lfc-test-settings
description: "The settings a Lakeflow Connect test skill would otherwise ask for on every run (the Azure subscription and tenant, AWS profile, region, resource group, Databricks workspace profile, target catalog, owner tag), asked once and kept per user in ~/.lfc/tests/settings.env, then read by every later run. Use at the start of any test that creates cloud or workspace resources, before asking the user anything."
parent: lfc-source-setup
---

# Test settings: ask once, then read

A test that creates resources needs the same answers each time: which subscription, which region, which workspace.
They go in one file per user, outside the repo (a plugin update replaces the repo's copy):
`~/.lfc/tests/settings.env` (or `$LFC_TESTS_SETTINGS`), mode 600. [`scripts/settings.sh`](scripts/settings.sh) reads
and writes it; run it with no arguments for its usage.

## The first run: setup, then fast

What changes rarely (the Azure and AWS accounts, the shared workspaces, local Podman or a VM) is found once:
`bash scripts/settings.sh setup` lists it all in one parallel pass (about 6 s): the Azure subscriptions, the AWS
profiles and their accounts (from the config, no network), every Databricks profile and whether its login works, and
local Podman, next to what's saved. Ask every missing question at once, with a recommendation for each, `set` the
answers, then `settings.sh checked`.

Every later run only reads the file: `require` makes no CLI call and asks nothing. Look a value up again only when a
command using it fails (a subscription gone, a workspace deleted: **account-token-fallback**'s `ws-token.sh` exits 3
for a workspace that no longer exists), then `set` the new one and `checked`. `require` notes when the settings were
last checked over 30 days ago.

## At the start of a test

1. **Check what's missing** for the keys the test uses: `bash scripts/settings.sh require AZ_SUBSCRIPTION AZ_REGION DATABRICKS_PROFILE`.
   It prints the missing keys only, and exits 1 if there are any. Many missing (a first run): `setup` first.
2. **For each missing key, show what's there and recommend one**, then ask once. Don't ask an open question:
   - `AZ_SUBSCRIPTION`, `AZ_TENANT`: `settings.sh azure` lists the subscriptions this `az` login reaches, with the CLI's
     default marked. Recommend the one the user's earlier resources were made in (their records), not the CLI default.
     A subscription in a tenant the login doesn't reach needs `az login --tenant <tenant id>`.
   - `AWS_PROFILE`, `AWS_REGION`: `aws configure list-profiles`; `aws sts get-caller-identity --profile <p>` for the account.
   - `DATABRICKS_PROFILE`, `WORKSPACES`: show `settings.sh workspaces` (the user's `databricks auth profiles`, numbered by
     cloud, with each login's state) and ask which ones they work in often; save those as `WORKSPACES` and the one used
     most as `DATABRICKS_PROFILE`. Don't pick for them from other records. A profile whose login expired often still works
     through **account-token-fallback** (`ws-token.sh`: the Azure CLI on Azure, or the account login); ask for a login
     only when that fails, and then for the account's login first (it covers every workspace in the account).
   - `SOURCE_HOST`: `podman` (local, **lfc-source-podman**) or `vm` (a cloud VM, **lfc-source-cloud-vm**), and `SOURCE_VM`
     for the VM's name.
   - `LFC_CATALOG`: the workspace's default catalog first (`databricks settings default-namespace get`), checked as in
     **lfc-workspace-setup**.
   - `AZ_RESOURCE_GROUP`: the one existing group every Azure test resource goes in. `setup` suggests the user's own
     groups in the subscription (tagged with them as Owner or Creator); ask which one, and save it. A subscription
     allows only so many groups, so skills never create, re-tag or delete one.
3. **Save the answers**: `bash scripts/settings.sh set AZ_SUBSCRIPTION=<id> AZ_TENANT=<id> AZ_REGION=centralus ...`.
4. **Read them** for every command, and pass the identity explicitly (`--subscription "$AZ_SUBSCRIPTION"`,
   `--profile "$DATABRICKS_PROFILE"`): never rely on the CLI's current default, which is whatever was set last.

   ```bash
   eval "$(bash scripts/settings.sh env)"
   az eventhubs namespace list --subscription "$AZ_SUBSCRIPTION" -g "$AZ_RESOURCE_GROUP" -o table
   ```

**Cleanup removes only what the skill created.** A create tags what it makes (`CreatedBy=<skill>`, with `Owner` and
`RemoveAfter`) or records it in its own state file; a teardown deletes a resource only when that tag or record says
this skill made it, and never the group or anything that was there before.

Change one with `set`, drop one with `unset`; `show` prints them all. Only names and ids belong here: passwords and
keys stay in their own 0600 files, named by a setting if needed (`KAFKA_PASSWORD_FILE=~/.lfc/kafka/<ns>.pw`).

## Keys

| Key | What | Example |
|---|---|---|
| `AZ_SUBSCRIPTION`, `AZ_TENANT` | Azure subscription id and its tenant | `<subscription id>`, `<tenant id>` |
| `AZ_REGION` | Azure region for test resources: the workspace's region | `centralus` |
| `AZ_RESOURCE_GROUP` | the existing group every Azure test resource goes in | `<resource group>` |
| `AWS_PROFILE`, `AWS_ACCOUNT`, `AWS_REGION` | AWS CLI profile, its account and region | `<profile>`, `<account id>`, `us-east-2` |
| `DATABRICKS_PROFILE` | the workspace the tests use by default | `<profile>` |
| `WORKSPACES` | the shared workspaces to pick from, by profile, comma-separated | `<profile>,<profile>` |
| `SOURCE_HOST`, `SOURCE_VM` | where source databases run: `podman` (local) or `vm`, and the VM | `podman`, `<vm name>` |
| `LFC_CATALOG` | the target catalog there | `<catalog>` |
| `OWNER` | the owner tag and name prefix | `<firstname>.<lastname>` |
| `REMOVE_AFTER_DAYS` | days until a test resource's `RemoveAfter` tag | `7` |
| `SETTINGS_CHECKED` | when `setup` last ran (`settings.sh checked`) | `2026-11-02` |

A skill that needs another key documents it here and uses the same file.
