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

## At the start of a test

1. **Check what's missing** for the keys the test uses: `bash scripts/settings.sh require AZ_SUBSCRIPTION AZ_REGION DATABRICKS_PROFILE`.
   It prints the missing keys only, and exits 1 if there are any.
2. **For each missing key, show what's there and recommend one**, then ask once. Don't ask an open question:
   - `AZ_SUBSCRIPTION`, `AZ_TENANT`: `settings.sh azure` lists the subscriptions this `az` login reaches, with the CLI's
     default marked. Recommend the one the user's earlier resources were made in (their records), not the CLI default.
     A subscription in a tenant the login doesn't reach needs `az login --tenant <tenant id>`.
   - `AWS_PROFILE`, `AWS_REGION`: `aws configure list-profiles`; `aws sts get-caller-identity --profile <p>` for the account.
   - `DATABRICKS_PROFILE`: `databricks auth profiles`; a profile whose login expired needs `databricks auth login --profile <p>`.
   - `LFC_CATALOG`: the workspace's default catalog first (`databricks settings default-namespace get`), checked as in
     **lfc-workspace-setup**.
   - `AZ_RESOURCE_GROUP`: a group of the user's own for test resources, made for it; never an existing shared one
     (a create can change a group's tags).
3. **Save the answers**: `bash scripts/settings.sh set AZ_SUBSCRIPTION=<id> AZ_TENANT=<id> AZ_REGION=centralus ...`.
4. **Read them** for every command, and pass the identity explicitly (`--subscription "$AZ_SUBSCRIPTION"`,
   `--profile "$DATABRICKS_PROFILE"`): never rely on the CLI's current default, which is whatever was set last.

   ```bash
   eval "$(bash scripts/settings.sh env)"
   az eventhubs namespace list --subscription "$AZ_SUBSCRIPTION" -g "$AZ_RESOURCE_GROUP" -o table
   ```

Change one with `set`, drop one with `unset`; `show` prints them all. Only names and ids belong here: passwords and
keys stay in their own 0600 files, named by a setting if needed (`KAFKA_PASSWORD_FILE=~/.lfc/kafka/<ns>.pw`).

## Keys

| Key | What | Example |
|---|---|---|
| `AZ_SUBSCRIPTION`, `AZ_TENANT` | Azure subscription id and its tenant | `<subscription id>`, `<tenant id>` |
| `AZ_REGION` | Azure region for test resources: the workspace's region | `centralus` |
| `AZ_RESOURCE_GROUP` | the user's own group for test resources | `lfc-tests-<owner>` |
| `AWS_PROFILE`, `AWS_REGION` | AWS CLI profile and region | `<profile>`, `us-east-2` |
| `DATABRICKS_PROFILE` | the workspace the tests use | `<profile>` |
| `LFC_CATALOG` | the target catalog there | `<catalog>` |
| `OWNER` | the owner tag and name prefix | `<firstname>.<lastname>` |
| `REMOVE_AFTER_DAYS` | days until a test resource's `RemoveAfter` tag | `7` |

A skill that needs another key documents it here and uses the same file.
