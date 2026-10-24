---
name: lfc-saas-answer
description: "Tooling for working out how a Lakeflow Connect SaaS connector behaves when the source can't be stood up for a test: an index of every connector's Databricks doc pages, release status and published pipeline notebook, built from the public docs sitemap; vendor facts (API allocations, deletes, timestamps, permissions) checked word for word against the vendor's public pages; and what to read in the customer's own org and pipeline. Salesforce facts first."
---

# Lakeflow Connect SaaS connectors: sources and checked vendor facts

A database source can be stood up, filled and tested ([lfc-source-setup](../lfc-source-setup/SKILL.md)). A SaaS source mostly can't: there is no container for Salesforce or Workday, and a customer's org is theirs. So how a SaaS connector behaves is worked out from evidence, in this order:

1. **The Databricks docs** for the connector: concepts, connection, pipeline, limits, FAQ, troubleshoot, reference. [references/connectors.md](references/connectors.md) lists them for every connector.
2. **The vendor's public docs**, for what the Databricks docs assume: API allocations, how deletes and modification timestamps work, permissions. These facts live in `references/vendor/<connector>.yaml`, one row per fact with the page and an exact quote, and every row is checked against its page (below).
3. **What the customer can check in their own org**: the vendor's usage pages and response headers, the pipeline's event log ([bin/lfc-events.sh](../../bin/lfc-events.sh)), the destination table.
4. **A test**, where the vendor offers a free developer org or sandbox (Salesforce Developer Edition, ServiceNow Personal Developer Instance, HubSpot developer test accounts). Start the pipeline from the notebook the docs publish for the connector.

Everything here comes from public pages: the Databricks docs and their sitemap, the vendors' docs, and what a pipeline and the vendor's org show.

## Every connector in the docs

[references/connectors.md](references/connectors.md) lists every connector with an `ingestion/lakeflow-connect/<connector>-pipeline` page: its release status (the badge under the page title), its other doc pages, and the ready-to-run pipeline notebook the docs publish for it (import it with the page's *Open in Databricks* button). It is generated from the public docs sitemap:

```bash
bash skills/lfc-saas-answer/scripts/connector-index.sh            # rewrite references/connectors.md
bash skills/lfc-saas-answer/scripts/connector-index.sh --check    # what changed since, exit 1 if anything
```

Rerun it after a docs release.

## Vendor facts, checked against their pages

| Connector | Vendor facts |
|---|---|
| Salesforce | [vendor/salesforce.yaml](references/vendor/salesforce.yaml) |

The rows use the format of [lfc-source-sizing's limits](../lfc-source-sizing/references/limits.md) (product, limit, value, kind, url, quote, ask, checked), plus `text_url` when the page renders with JavaScript and its text comes from a docs API (Salesforce). Check every row before relying on it:

```bash
python3 skills/lfc-source-sizing/scripts/limits-check.py skills/lfc-saas-answer/references/vendor/*.yaml
python3 skills/lfc-source-sizing/scripts/limits-check.py --find <text_url or url> "<words>"   # page text to quote from
```

Copy quotes from `--find` output, never from a summary: a paraphrased quote fails the check, and that's the point. A row whose page moved or changed fails too; fix the row, don't drop the check.

## Add a connector's vendor facts

1. Find its row in [connectors.md](references/connectors.md) and read its FAQ and limits pages.
2. For each behavior those pages rest on (allocations, deletes, timestamps, pagination, permissions, token lifetime), find the vendor page, add the fact to `references/vendor/<connector>.yaml` with `--find`, and run the check.
3. Add the connector's row to the table above.

## Wording: neutral, facts only

Customers often run Lakeflow Connect next to other ingestion tools, and which ones they keep is their call. Every skill, script and result here describes another tool only by what it lands (tables, columns, types, rows, behavior) and maps Lakeflow Connect to it: "map to the Fivetran schema", not "replace Fivetran". No "replace", "migrate off", "instead of", "win", or one product "against" or "vs" another, no comparison with another vendor's price, and no advice on which components to keep.

## Connector POC kits

| Connector | Kit |
|---|---|
| Meta Ads | [lfc-meta-ads-poc](../lfc-meta-ads-poc/SKILL.md): pipelines, custom report probe, schedule, SLA and rate-limit report, mapping to the Fivetran schema, row counts, cost |
