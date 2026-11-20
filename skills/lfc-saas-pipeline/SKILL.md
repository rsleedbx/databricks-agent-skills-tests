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

| Way | Files | When |
|---|---|---|
| Bundle | [bundles/](bundles) (one folder per connector) | a repeatable deployment: the schema, the pipeline, a job (paused) that runs it daily, CAN_VIEW for `users` |
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
then run the pipeline from the **Bundle resources** pane. The `dev` target is in development mode, so resource names
get a `dev_<user>_` prefix; add a target without `mode: development` for shared names.

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
