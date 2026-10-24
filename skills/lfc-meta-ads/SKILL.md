---
name: lfc-meta-ads
description: "Scripts for Lakeflow Connect Meta Ads pipelines: create them (objects, ad_insights, prebuilt and custom reports, SCD type 2, ad_accounts), try custom report definitions in a throwaway pipeline, schedule them, list each update's timing, table status and error classes, map the landed tables and columns to the Fivetran schema, count rows and metrics per day on two landings, read the DBU cost from system billing, and remove what they created. Tested on a workspace with a Meta Ads connection."
parent: lfc-saas-answer
---

# Meta Ads pipelines with Lakeflow Connect

Each section below is one task with its script; use the ones you need, in any order.

| Task | Script |
|---|---|
| [Try custom report definitions](#try-custom-report-definitions) | [probe.sh](scripts/probe.sh) |
| [Create a pipeline](#create-a-pipeline) | [pipeline.sh](scripts/pipeline.sh) |
| [Run, schedule](#run-and-schedule) | [run.sh](scripts/run.sh), [schedule.sh](scripts/schedule.sh) |
| [List updates: timing, tables, errors](#list-updates-timing-tables-errors) | [report.sh](scripts/report.sh) |
| [Map to the Fivetran schema](#map-to-the-fivetran-schema) | [schema.sh](scripts/schema.sh), [schema-diff.py](scripts/schema-diff.py), [fivetran-doc-schema.py](scripts/fivetran-doc-schema.py) |
| [Count rows and metrics on two landings](#count-rows-and-metrics-on-two-landings) | [row-parity.sh](scripts/row-parity.sh) |
| [Cost](#cost) | [cost.sh](scripts/cost.sh) |
| [Clean up](#clean-up) | [cleanup.sh](scripts/cleanup.sh) |

Every script takes the CLI profile first, prints its usage when run without arguments, and prints the workspace link of every pipeline, job and schema it creates. They need the Databricks CLI, `jq`, and `python3` for the two Python scripts. The SQL ones use a SQL warehouse (`--warehouse <id>`; otherwise the first running serverless one).

## What Lakeflow Connect does, and what you specify

| Lakeflow Connect does | You specify |
|---|---|
| The OAuth app and token refresh for the Meta connection | The Meta user who signs in, with access to the ad accounts |
| Incremental reads: `updated_time` cursor for objects, `date_stop` plus a lookback (7 days by default) for insights and reports; full refresh for objects without an update column | Which tables, per pipeline; `start_date` for insights (36 months back by default) |
| Retries on Meta rate limits, waiting the interval Meta returns (capped at two minutes, up to three retries) | How many objects per pipeline (at most 250) and how often it runs (the docs advise no more than hourly in the Beta) |
| Types mapped to Delta types; SCD type 1 tables, or type 2 for `ads`, `ad_sets`, `campaigns` | `scd_type: SCD_TYPE_2` where history is needed |
| The prebuilt reports' level, breakdowns, attribution | For custom reports: level, breakdowns, action breakdowns, time increment, attribution windows, action report time |
| Retrying a failed update by itself (cause `RETRY_ON_FAILURE`) | The job schedule, failure emails |

Docs: [connector overview](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/meta-ads), [pipeline](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/meta-ads-pipeline), [reference](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/meta-ads-reference), [limits](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/meta-ads-limits), [FAQ](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/meta-ads-faq).

## Before you start

1. A workspace admin turns on the Beta preview (Settings, Previews, Lakeflow Connect for Meta Ads).
2. Someone with `CREATE CONNECTION` creates the Meta Ads connection in Catalog Explorer (External data, Connections, Meta Ads) and signs in with a Meta user that can see the ad accounts. Pipelines can't be created in the UI during the Beta; the connection can, and these scripts do the rest.
3. The ad account id with its `act_` prefix (Meta Business Manager, or the account switcher in Ads Manager). It is the `source_schema` of every table except `ad_accounts`.
4. `USE CATALOG` and `CREATE SCHEMA` on the destination catalog; `SELECT` on `system.billing` for the cost.

```bash
S=skills/lfc-meta-ads/scripts
P=<profile>; CONN=<connection>; ACCT=act_<id>; CAT=<catalog>; SCH=<schema>
```

## Try custom report definitions

Meta accepts only some breakdown combinations together. A custom report that asks for another one fails its table with `[META_ADS_SOURCE_API_ERROR] An error occurred in the Meta Ads API call. Facebook trace id ...`, which doesn't name the breakdowns, and the update is FAILED. `probe.sh` runs one or more `custom_report_options` files (examples in [references](references/)) in a throwaway pipeline over the last 7 days, each in its own table, then deletes it:

```bash
$S/probe.sh $P --connection $CONN --account $ACCT --catalog $CAT --schema ${SCH}_probe report1.json report2.json ...
#   cr_report1  COMPLETED  rows=-
#   cr_report2  FAILED  META_ADS_SOURCE_API_ERROR  Meta trace id <trace-id>
```

[references/meta-breakdowns.tsv](references/meta-breakdowns.tsv) lists the combinations Meta publishes and those the prebuilt reports use; `pipeline.sh` warns about any other. `country` with `publisher_platform` is not in Meta's list, and it failed; each of them alone completed.

A custom report's columns are fixed: `impressions`, `clicks`, `spend`, `reach`, `actions`, `action_values`, plus the ids, the dates, `insight_id`, `breakdown_hash` and the breakdown columns. `custom_report_options` has no field list; other metrics (video, ROAS, `cpm`) are in the prebuilt reports (the reference lists each one's metric set) or `ad_insights`.

## Create a pipeline

```bash
$S/pipeline.sh $P --connection $CONN --account $ACCT --catalog $CAT --schema $SCH --name <name> \
  --scd2 ads,ad_sets,campaigns                       # default tables: the 8 documented objects
$S/pipeline.sh $P --connection $CONN --account $ACCT --catalog $CAT --schema $SCH --name <name> \
  --tables ad_insights,basic_campaign_report,delivery_platform_report \
  --custom-report custom_report_country=$S/../references/custom-report-country.json \
  --start-date <YYYY-MM-DD> --lookback <days>
```

- **Objects and insights** can share one pipeline or use separate ones. Tables in separate pipelines fail, retry and run on their own schedules; the docs suggest fewer objects per pipeline when Meta rate-limits.
- **`ad_accounts`** (every ad account the connection's Meta user can see) isn't in the docs' object list. The script reads it from source schema `default`; with the `act_` id as its source schema it fails with `TABLE_OR_VIEW_NOT_FOUND`.
- **`--lookback`** sets `custom_insights_lookback_window` on every insights table. Where a table has no `action_attribution_windows`, it also sets Meta's attribution to `<N>d_click,1d_view` (the reference's "attribution fallback"). With neither, Meta uses the ad account's default attribution.
- **`--dry-run`** prints the JSON instead. Rerunning with the same `--name` updates the pipeline.
- **The create API** takes the fields at the top level (`name`, `catalog`, `schema`, `ingestion_definition`). A body nested under `"pipeline": {...}` (the bundle layout) is refused with `unknown field: pipeline` and `name must be set`.

## Run and schedule

```bash
$S/run.sh $P <pipeline-id>                      # one update, waits, then report.sh for it; exit 1 unless COMPLETED
$S/run.sh $P <pipeline-id> --full-refresh ads   # full refresh of some tables (or all, without a list)
$S/schedule.sh $P <pipeline-id> --cron "0 0 6 * * ?" --tz <zone> --email <address>   # --paused to create it paused
```

After a failed update the pipeline starts a retry of its own (cause `RETRY_ON_FAILURE`); `run.sh` says so. A table that fails on every update fails the retry and every scheduled run too.

## List updates: timing, tables, errors

```bash
$S/report.sh $P <pipeline-id> --days 7 --tz <zone> --tables [--counts] [--by HH:MM]
```

- **One line per update:** start, end, minutes, state, cause, and error classes with counts. The error classes are the bracketed codes in the update's WARN and ERROR events (`META_ADS_RATE_LIMIT_EXCEEDED`, `META_ADS_REQUESTED_DATA_LIMIT_EXCEEDED`, ...). A rate limit the connector's retries absorbed leaves no error event.
- **`--tables`:** each table's last status and rows written. `-` means the event has no row metric, which is common in the API's events; `--counts` adds each table's current row count from SQL.
- **`--by HH:MM`:** one line per day, saying whether an update completed by that time of day.

## Map to the Fivetran schema

```bash
$S/schema.sh $P $CAT.$SCH > lfc.tsv                           # the Lakeflow Connect landing
$S/schema.sh $P <fivetran_catalog>.<fivetran_schema> > ft.tsv  # a Fivetran landing in the same metastore
python3 $S/fivetran-doc-schema.py > ft-doc.tsv                 # or Fivetran's documented schema
python3 $S/schema-diff.py --lfc lfc.tsv --fivetran ft.tsv --mapping <column-mapping.yaml> --out diff.tsv
python3 $S/schema-diff.py --lfc lfc.tsv --fivetran ft.tsv --tables-map $S/../references/fivetran-tables.tsv --out diff.tsv
```

With a column mapping (rows `{fivetran: table.column, lfc: table.column or null, status, method}`), the diff counts per table pair: same, renamed, Fivetran only, LFC only, type differs. It flags mapping rows the live tables contradict (`mapping stale`, `not in Fivetran`, exit 1). Without one, `--tables-map` pairs the standard tables and matches columns by name. What the test landings showed:

- Fivetran keeps `*_history` tables (a row per version) and child tables for nested fields (`ad_conversion`, `creative_*`, `lookalike_*`); Lakeflow Connect keeps nested fields as arrays or strings in the parent table.
- Ids: Fivetran's schema has `id`, `account_id`, `campaign_id` as `bigint`; Lakeflow Connect lands them as `string`. A join across the two needs a cast.
- SCD type 2 tables get `__START_AT` and `__END_AT`.

## Count rows and metrics on two landings

```bash
$S/row-parity.sh $P --fivetran <ft_cat>.<ft_schema> --lfc $CAT.$SCH --from <YYYY-MM-DD> --to <YYYY-MM-DD> \
  --report <fivetran_report>=<lfc_report> --object ad_history=ads --object campaign_history=campaigns [--target <pct>]
```

For each day in the range: rows, `spend` and `impressions` (`--metrics`) on both landings, and the Lakeflow Connect figure as a percentage of the other. Objects count distinct ids, since history tables hold several rows per object. With `--target`, each pair is marked OK or UNDER TARGET, and the exit code is 1 when one is under. Days inside the lookback can still change on both landings.

## Cost

```bash
$S/cost.sh $P <pipeline-id>[,<pipeline-id>...] --since <YYYY-MM-DD>
```

- **DBUs per day and SKU.** The totals are split into the free-tier SKU (`LAKEFLOW_CONNECT_FREE_USAGE`) and billed DBUs at list price, with the daily average and 30 times it.
- **The free tier** is a daily allowance per workspace, shared by every managed connector there. The last section shows the workspace's free-tier DBUs per day and how many pipelines shared them.
- **Billing rows arrive hours after a run.**

## Clean up

```bash
$S/cleanup.sh $P --job <job-id> --pipeline <id> --pipeline <id> --drop-schema $CAT.$SCH   # asks first; --yes to skip
```

## How it was tested

The test environment:

- AWS workspace (us-west-2), serverless, Unity Catalog.
- Runtime DBR 18.3, image `dlt:18.3.8` (`dlt-release-dp-20261015`).
- A Meta Ads connection (OAuth U2M) to a test ad account with no campaigns. Every table landed empty, so row counts were checked on mock tables only.

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
| `schema-diff.py` with a column mapping, against the live landing (with `ad_accounts`) and Fivetran's documented schema | Tested: no stale rows; type differences on ids; 2 Fivetran tables the mapping doesn't cover |
| `row-parity.sh` | Tested on mock tables: a day at 98 % marked UNDER TARGET with `--target 99`, exit 1 |
| `cost.sh` | Tested on a pipeline with billing history; new pipelines show nothing until billing arrives |

Not tested: data from an ad account with spend (row counts, lookback re-reads, a real rate limit), a live Fivetran landing.

## Trade-offs

- **Probe in a throwaway pipeline.** A rejected custom report shows up in a minute, and no other pipeline's update fails; the cost is creating and deleting a pipeline for each probe.
- **Errors from bracketed codes in event messages.** No connector internals are needed, and new codes show up without a script change; a message without a bracketed code is counted only through its update's state.
- **`--by` per day, not per update.** With more than one update a day, the line says whether any completed by that time; a later failed update doesn't change it.
- **Cost at list price.** The USD figure is DBUs times `system.billing.list_prices`.
