---
name: lfc-saas-pipeline
description: "Create a Lakeflow Connect SaaS ingestion pipeline two ways from one definition: a bundle (deploy from the CLI or the workspace) and a small notebook that pip-installs the docs notebook's helper functions (create, edit, start, wait) and keeps only the spec cell. Use when a SaaS connector needs a repeatable deployment, or a test needs the REST API from Python (validate-only updates, refreshing selected tables) that a bundle can't express. First connector: 1Password Event Logs."
parent: lfc-saas-answer
---

# SaaS ingestion pipelines: a bundle and a notebook

The docs' notebook for each SaaS connector has two cells of helper functions marked "Do NOT modify" and one spec
cell. Here the helpers are a package, `scripts/lfc_saas_pipeline`, so a notebook installs it and keeps only the
spec cell; the same pipeline is also a bundle. Both need a UC connection to the vendor first (the connector's
connection page); each defaults to `lfcsaas-<connector>`.

| Connector | Notebook | Bundle | Connection | Tables |
|---|---|---|---|---|
| 1Password Event Logs | [notebook](notebooks/1password-event-logs-pipeline.py) | [bundle](bundles/1password-event-logs) | `lfcsaas-1password` | audit_events, item_usages, sign_in_attempts |
| Marketo | [notebook](notebooks/marketo-pipeline.py) | [bundle](bundles/marketo) | `lfcsaas-marketo` | leads, static_lists |
| NetSuite | [notebook](notebooks/netsuite-pipeline.py) | [bundle](bundles/netsuite) | `lfcsaas-netsuite` | transaction; needs NetSuite's JDBC driver in a volume (`netsuite_jar_path`) |
| Square | [notebook](notebooks/square-pipeline.py) | [bundle](bundles/square) | `lfcsaas-square` | customers, orders, payments |
| Wiz Audit Logs | [notebook](notebooks/wiz-audit-logs-pipeline.py) | [bundle](bundles/wiz-audit-logs) | `lfcsaas-wiz` | audit_log_entries, vulnerability_findings |
| Shopify | [notebook](notebooks/shopify-pipeline.py) | [bundle](bundles/shopify) | `lfcsaas-shopify` | customers, orders, products |
| Zoom | [notebook](notebooks/zoom-pipeline.py) | [bundle](bundles/zoom) | `lfcsaas-zoom` | users, meetings; no public docs page yet |
| Atlassian Audit Logs | [notebook](notebooks/atlassian-audit-logs-pipeline.py) | [bundle](bundles/atlassian-audit-logs) | `lfcsaas-atlassian-audit-logs` | audit_logs |
| OpenAI | [notebook](notebooks/openai-pipeline.py) | [bundle](bundles/openai) | `lfcsaas-openai` | audit_logs, users, usage_completions |
| Adobe Commerce | [notebook](notebooks/adobe-commerce-pipeline.py) | [bundle](bundles/adobe-commerce) | `lfcsaas-adobe-commerce` | products, customers, orders; no public docs page yet |
| GitHub | [notebook](notebooks/github-pipeline.py) | [bundle](bundles/github) | `lfcsaas-github` | repositories, pull_requests; sign in to GitHub to make the connection; `org` required (`run_tests.py --var org=...`) |
| Meta Ads | [notebook](notebooks/meta-ads-pipeline.py) | [bundle](bundles/meta-ads) | `lfcsaas-metaads` | campaigns, ad_sets, ads, ad_insights; sign in to Meta to make the connection; `account_id` (`act_<id>`) required (`run_tests.py --var account_id=...`) |

| Way | Files | When |
|---|---|---|
| Bundle | [bundles/](bundles) (one folder per connector) | a repeatable deployment: the schema and the pipeline, CAN_VIEW for `users`; `-t scheduled` adds a job (paused) that runs it daily |
| Notebook | [notebooks/](notebooks) (one per connector) | the docs' notebook, short: run all, or run it as a job with parameters |
| Python (REST API) | `lfc_saas_pipeline` | tests from a notebook or a laptop: validate-only updates, `refresh_selection`, waiting for an update, editing or deleting a pipeline |

## Deploy the bundle

From the CLI, in the bundle's folder (variables `connection`, `catalog`, `schema`; defaults `lfcsaas-1password`,
`main`, `onepassword_event_logs`):

```bash
databricks bundle deploy --profile <p> [--var schema=<schema>]
databricks bundle run onepassword_event_logs --profile <p>
```

From the workspace: put the folder in a Git folder, open `databricks.yml`, click the deployments icon, **Deploy**,
then run the pipeline from the **Bundle resources** pane. The default `dev` target (for tests) is the schema and the
pipeline, in development mode, so resource names get a `dev_<user>_` prefix. The `scheduled` target
(`databricks bundle deploy -t scheduled`) adds a daily job (paused until you unpause it), with shared names; it's
left out of the default because every job counts toward the workspace's job quota.

## Run the notebook

Import [the notebook](notebooks/1password-event-logs-pipeline.py) (Workspace, Import, File), then **Run all**. Its
first cell installs the package from this repo:

```
%pip install "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip#subdirectory=skills/lfc-saas-pipeline/scripts"
```

A branch or tag name in place of `main` pins a version. The spec cell's `connection`, `catalog`, `schema` and `name`
are `arg(name, default)`: a job's parameters of those names override the defaults, so the same notebook runs
unattended.

## The package

Same names and arguments as the docs' cells: `create_pipeline(pipeline_definition)`, `edit_pipeline(id, ...)`,
`delete_pipeline(id)`, `list_pipeline(filter)`, `get_pipeline(id)`, `start_pipeline(id, full_refresh=False)`. A
definition may be the docs' JSON string or a dict. Each prints the response as the docs' cells do, returns it, and
raises on an error. `create_pipeline` adds CAN_VIEW for `users` (`viewers=()` for none); `start_pipeline` also takes
`validate_only` and `refresh_selection`; `wait_update(id, update_id)` returns the update's final state. It signs in
through the Databricks SDK: by itself in a notebook, with `DATABRICKS_CONFIG_PROFILE` elsewhere.

Offline tests: `python3 -m unittest skills/lfc-saas-pipeline/tests/test_lfc_saas_pipeline.py`.

## Adding a connector

Copy the bundle folder and the notebook, and change the connector's source schema and tables (its reference page's
supported tables), the connection default, and the names. The package doesn't change.
