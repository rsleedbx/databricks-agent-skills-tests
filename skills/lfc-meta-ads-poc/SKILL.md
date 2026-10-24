---
name: lfc-meta-ads-poc
description: "Scripts for a Lakeflow Connect Meta Ads proof of concept: create the pipelines (objects, ad_insights, prebuilt and custom reports, SCD type 2), probe custom report definitions before using them, schedule them for a morning SLA, report freshness, failures and Meta rate-limit errors per update and per day, map the landed tables and columns to the Fivetran schema and count rows on both landings, and read the DBU cost from system billing. Tested end to end on a workspace with a Meta Ads connection."
parent: lfc-saas-answer
---

# Meta Ads POC with Lakeflow Connect

A Meta Ads POC usually has four objectives. Each one has a script here. Where the customer already lands Meta data with Fivetran, the scripts map the Lakeflow Connect tables to that schema.

| Objective | Evidence the POC needs | Script |
|---|---|---|
| 1. Ingest objects, prebuilt and custom reports, daily by the SLA | pipelines that complete; the time each day's last good update ended | [pipeline.sh](scripts/pipeline.sh), [probe.sh](scripts/probe.sh), [schedule.sh](scripts/schedule.sh), [run.sh](scripts/run.sh), [report.sh](scripts/report.sh) |
| 2. Map to the Fivetran schema | tables, columns and types on both landings, through a reviewed column mapping; rows per day | [schema.sh](scripts/schema.sh), [schema-diff.py](scripts/schema-diff.py), [fivetran-doc-schema.py](scripts/fivetran-doc-schema.py), [row-parity.sh](scripts/row-parity.sh) |
| 3. Cost | DBUs per day split into free tier and billed, at list price | [cost.sh](scripts/cost.sh) |
| 4. Rate limits and reliability | error classes per update (`META_ADS_RATE_LIMIT_EXCEEDED`, `META_ADS_REQUESTED_DATA_LIMIT_EXCEEDED`, ...), failed and retried updates | [report.sh](scripts/report.sh) |

Every script takes the CLI profile first, prints its usage when run without arguments, and prints the workspace link of every pipeline, job and schema it creates. They need the Databricks CLI, `jq`, and `python3` for the two Python scripts. The SQL ones use a SQL warehouse (`--warehouse <id>`; otherwise the first running serverless one).

## What Lakeflow Connect does for you, and what you specify

| Lakeflow Connect does | You specify |
|---|---|
| The OAuth app and token refresh for the Meta connection | The Meta user who signs in, with access to the ad accounts (a user of its own, so the POC doesn't share production API quota) |
| Incremental reads: `updated_time` cursor for objects, `date_stop` plus a lookback (7 days by default) for insights and reports; full refresh for objects without an update column | Which tables, per pipeline; `start_date` for insights (36 months back by default) |
| Retries on Meta rate limits, waiting the interval Meta returns (capped at two minutes, up to three retries) | How many objects per pipeline and how often it runs (no more than hourly in the Beta) |
| Types mapped to Delta types; SCD type 1 tables, or type 2 for `ads`, `ad_sets`, `campaigns` | `scd_type: SCD_TYPE_2` where history is needed |
| The prebuilt reports' level, breakdowns, attribution | For custom reports: level, breakdowns, action breakdowns, time increment, attribution windows, action report time |
| Retrying a failed update once by itself (cause `RETRY_ON_FAILURE`) | The job schedule, failure emails |

Docs: [connector overview](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/meta-ads), [pipeline](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/meta-ads-pipeline), [reference](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/meta-ads-reference), [limits](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/meta-ads-limits), [FAQ](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/meta-ads-faq).

## Before you start

1. A workspace admin turns on the Beta preview (Settings, Previews, Lakeflow Connect for Meta Ads).
2. Someone with `CREATE CONNECTION` creates the Meta Ads connection in Catalog Explorer (External data, Connections, Meta Ads) and signs in with the POC's Meta user. Pipelines can't be created in the UI during the Beta; the connection can, and these scripts do the rest.
3. The ad account id with its `act_` prefix (Meta Business Manager, or the account switcher in Ads Manager). It is the `source_schema` of every table.
4. `USE CATALOG` and `CREATE SCHEMA` on the destination catalog; `SELECT` on `system.billing` for the cost.

```bash
S=skills/lfc-meta-ads-poc/scripts
P=<profile>; CONN=<connection>; ACCT=act_<id>; CAT=<catalog>; SCH=<schema>
```

## 1. Try the custom reports first

Meta accepts only some breakdown combinations together. A custom report that asks for another one fails the table with `[META_ADS_SOURCE_API_ERROR] An error occurred in the Meta Ads API call. Facebook trace id ...`, nothing about the breakdowns, and the whole update is FAILED. So write each Fivetran custom report as a `custom_report_options` file (examples in [references](references/)) and probe them together in a throwaway pipeline over the last 7 days:

```bash
$S/probe.sh $P --connection $CONN --account $ACCT --catalog $CAT --schema ${SCH}_probe report1.json report2.json ...
#   cr_report1  COMPLETED  rows=1234
#   cr_report2  FAILED  META_ADS_SOURCE_API_ERROR  Meta trace id <trace-id>
```

[references/meta-breakdowns.tsv](references/meta-breakdowns.tsv) lists the combinations Meta publishes and those the prebuilt reports use; `pipeline.sh` warns about any other. `country` with `publisher_platform` is one Meta doesn't list, and it fails: split it into two reports (`custom-report-country.json`, `custom-report-platform.json`) or pick a listed set.

A custom report's metrics are fixed: `impressions`, `clicks`, `spend`, `reach`, `actions`, `action_values`, plus the ids, the dates, `insight_id`, `breakdown_hash` and the breakdown columns. `custom_report_options` has no field list, so a Fivetran custom report with other fields (video metrics, ROAS, `cpm`) needs a prebuilt report that has them (the reference lists each one's metric set), `ad_insights`, or a gap noted for objective 2.

## 2. Create the pipelines

Two pipelines, each under 10 objects (fewer objects per pipeline is the docs' advice against rate limits): one for the objects, one for insights and reports.

```bash
$S/pipeline.sh $P --connection $CONN --account $ACCT --catalog $CAT --schema $SCH --name ${SCH}_objects \
  --scd2 ads,ad_sets,campaigns          # default tables: the 8 documented objects
$S/pipeline.sh $P --connection $CONN --account $ACCT --catalog $CAT --schema $SCH --name ${SCH}_insights \
  --tables ad_insights,basic_campaign_report,delivery_platform_report,demographics_country_report \
  --custom-report custom_report_country=$S/../references/custom-report-country.json \
  --custom-report custom_report_platform=$S/../references/custom-report-platform.json \
  --start-date 2026-06-01 --lookback 7
```

Add `ad_accounts` to `--tables` for the ad account details (Fivetran's `account_history`): it isn't in the docs' object list, and it lists every ad account the connection's Meta user can see, so the script reads it from source schema `default` (with the `act_` id as its source schema the table fails with `TABLE_OR_VIEW_NOT_FOUND`).

`--lookback` sets `custom_insights_lookback_window` on every insights table, and where a table has no `action_attribution_windows` it also sets Meta's attribution to `<N>d_click,1d_view` (the reference's "attribution fallback"). Without either, Meta uses the ad account's default attribution. Set `action_attribution_windows` in each custom report to match the Fivetran report.

`--dry-run` prints the JSON instead. Rerunning with the same `--name` updates the pipeline. The create API takes the fields at the top level (`name`, `catalog`, `schema`, `ingestion_definition`); a body nested under `"pipeline": {...}` (the bundle layout) is refused with `unknown field: pipeline` and `name must be set`.

## 3. Run, schedule, watch

```bash
$S/run.sh $P <pipeline-id>                      # one update, waits, then the report below; exit 1 unless COMPLETED
$S/run.sh $P <pipeline-id> --full-refresh ads   # full refresh of some tables (or all, without a list)
$S/schedule.sh $P <pipeline-id> --email you@example.com          # daily 06:00 America/New_York; --cron, --tz, --paused
$S/report.sh $P <pipeline-id> --days 7 --sla 08:00 --tz America/New_York --tables
```

`report.sh` prints one line per update (start, end, minutes, state, cause, error classes with counts), each table's last status and rows written (`-` when the event has no row metric, which is common in the API's events; `--counts` adds each table's current row count from SQL), and the SLA per day: `met (completed at 05:58)` when an update completed by the SLA time that day, `MISSED` otherwise. Error classes come from the bracketed codes in the update's WARN and ERROR events, so Meta's rate-limit codes show the moment they occur; a rate limit that the connector's retries absorbed leaves no error.

After a failed update the pipeline starts a retry of its own (cause `RETRY_ON_FAILURE`); `run.sh` says so. A table that fails on every run (a rejected custom report) fails both, every scheduled run: fix or remove it.

## 4. Map to the Fivetran schema, count rows

```bash
$S/schema.sh $P $CAT.$SCH > lfc.tsv                           # the LFC landing
$S/schema.sh $P <fivetran_catalog>.<fivetran_schema> > ft.tsv  # the Fivetran landing, if it is in the same metastore
python3 $S/fivetran-doc-schema.py > ft-doc.tsv                 # or Fivetran's documented schema, until you have theirs
python3 $S/schema-diff.py --lfc lfc.tsv --fivetran ft.tsv --mapping <reviewed-mapping.yaml> --out diff.tsv
python3 $S/schema-diff.py --lfc lfc.tsv --fivetran ft.tsv --tables-map $S/../references/fivetran-tables.tsv --out diff.tsv
```

With a reviewed column mapping (rows `{fivetran: table.column, lfc: table.column or null, status, method}`) the diff counts per table pair: same, renamed, Fivetran only, LFC only, type differs; and it flags mapping rows the live tables contradict (`mapping stale`, `not in Fivetran`, exit 1). Without one, `--tables-map` pairs the standard tables and matches columns by name. Things to expect:

- Fivetran keeps `*_history` tables (a row per version) and child tables for nested fields (`ad_conversion`, `creative_*`, `lookalike_*`); LFC keeps nested fields as arrays or strings in the parent table.
- Ids: Fivetran lands `id`, `account_id`, `campaign_id` as `bigint`; LFC as `string`. Joins between the two need a cast.
- SCD type 2 tables get `__START_AT` and `__END_AT`.

Rows and metrics per day, on closed days (outside the lookback):

```bash
$S/row-parity.sh $P --fivetran <ft_cat>.<ft_schema> --lfc $CAT.$SCH --from 2026-09-01 --to 2026-09-30 \
  --report <fivetran_report>=custom_report_country --object ad_history=ads --object campaign_history=campaigns
```

Each day shows rows, `spend` and `impressions` on both landings and the Lakeflow Connect figure as a percentage of the Fivetran one; a pair is OK within 99 to 101 % (`--target`, set it to the POC's own target). Objects compare distinct ids, since Fivetran's history tables hold several rows per object.

## 5. Cost

```bash
$S/cost.sh $P <objects-pipeline-id>,<insights-pipeline-id> --since 2026-10-01
```

DBUs per day and SKU, the totals split into the free-tier SKU (`LAKEFLOW_CONNECT_FREE_USAGE`) and billed DBUs at list price, the daily average and 30 times it. The free tier is a daily allowance per workspace shared by every managed connector there, so a pipeline's free share depends on what else runs in the workspace; the last section shows the workspace's free-tier DBUs per day and how many pipelines shared them. Billing rows arrive hours after a run.

## 6. Clean up

```bash
$S/cleanup.sh $P --job <job-id> --pipeline <id> --pipeline <id> --drop-schema $CAT.$SCH   # asks first; --yes to skip
```

## How it was tested

AWS workspace (us-west-2), serverless, Unity Catalog; runtime DBR 18.3, image `dlt:18.3.8` (`dlt-release-dp-20261015`); a Meta Ads connection (OAuth U2M) to a test ad account with no campaigns, so every table landed empty and row counts could only be checked on mock tables.

| Case | Result |
|---|---|
| 8 documented objects, `ads`/`ad_sets`/`campaigns` as SCD type 2 | Tested end to end: completed; SCD type 2 tables have `__START_AT`, `__END_AT` |
| `ad_insights` + 3 prebuilt reports + 2 custom reports (`country`; `publisher_platform`) | Tested end to end: completed |
| Custom report `country` + `publisher_platform` (with and without attribution windows) | Tested: table FAILED, `META_ADS_SOURCE_API_ERROR`; the update FAILED and a `RETRY_ON_FAILURE` update followed |
| Custom reports: no breakdown; `country`; `publisher_platform`; `publisher_platform` + `platform_position`; campaign level `age` + `gender` with `1d_click` | Tested: completed |
| Custom report with `hourly_stats_aggregated_by_advertiser_time_zone` | Tested: completed; whether rows come back hourly needs an account with spend |
| `ad_accounts` as a source table (not in the docs' list) | Tested: with the ad account as source schema, `TABLE_OR_VIEW_NOT_FOUND`; with source schema `default`, completed, one row per ad account the Meta user sees |
| Pipeline create body nested under `pipeline` | Tested: refused |
| Job from `schedule.sh`, run once | Tested: update with cause `JOB_TASK`, completed |
| `schema-diff.py` with a reviewed mapping, against the live LFC landing (with `ad_accounts`) and Fivetran's documented schema | Tested: no stale rows; type differences on ids; 2 Fivetran tables the mapping doesn't cover |
| `row-parity.sh` | Tested on mock tables: a day at 98 % flagged, exit 1 |
| `cost.sh` | Tested on a pipeline with billing history; new pipelines show nothing until billing arrives |

Not tested: data from an ad account with spend (row counts, lookback re-reads, a real rate limit), the Fivetran side from a live Fivetran landing.

## Trade-offs

- **Two pipelines, not one.** Objects and insights fail, retry and get scheduled apart, and each stays under 10 objects; the cost is two pipelines to schedule and watch.
- **Probe in a throwaway pipeline.** A rejected custom report is found in a minute without failing the POC pipeline; the probe creates and deletes a pipeline each time.
- **Errors from bracketed codes in event messages.** No connector internals needed, and new codes show up without a script change; a message without a bracketed code is counted only through its state.
- **SLA per day, not per update.** A schedule that runs more often than daily is judged on whether the morning data was there; a late failed update after a good one doesn't count against the day.
- **Cost at list price.** Discounts and commitments differ per customer; list price keeps the comparison reproducible, and the account team applies the contract rate.
