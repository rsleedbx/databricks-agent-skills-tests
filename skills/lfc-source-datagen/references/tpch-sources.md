# Any TPC model as several business sources

[`lfc_source_datagen.sources`](../scripts/lfc_source_datagen/sources.py) builds any TPC model (TPC-B, C, H, DS, E, DI) as n business sources in one PostgreSQL database: one schema per line of business (or acquired company), each at its own scale, with the same tables and repeating keys (each copy starts at 1). That is the source of an n:1 test: Lakeflow Connect ingests each schema, and **lfc-medallion** stacks them into one table per table with a `_source` column. [`lfc_source_datagen.tpch_sources`](../scripts/lfc_source_datagen/tpch_sources.py) adds TPC-H's own steps: the spec's words and lead times a model can learn; [`lfc_source_datagen.tpcc_sources`](../scripts/lfc_source_datagen/tpcc_sources.py) adds TPC-C's delivery times; [`lfc_source_datagen.tpcds_sources`](../scripts/lfc_source_datagen/tpcds_sources.py) TPC-DS's returns and web ship dates.

It reuses the skill's parts and the source contract's:
- the DDL from `tpc_schema` (the spec's TPC-H and TPC-B) or statschema's `shape()` and `ddl()`;
- the fill from `python/fill.py`'s `fill()`;
- the checks from `tpc/<model>-verify.sql`;
- the publication and slot from `lfc_source_contract.cdc`;
- the writes in a notebook through `lfc_source_contract.jdbc`.

Per source, in copy `<model>_<n>`, each step safe to repeat:
1. the model's DDL at the source's scale, all or nothing (a guarded `DO` block);
2. `SELECT` for the reading login;
3. the fill, whose INSERTs size themselves from the key already there;
4. the spec's checks where the model has them, once (a schema comment marks them);
5. the model's own steps (TPC-H: `tpch-labels.sql`, then `tpch-lead-times.sql` when the source has lead times; TPC-C: `tpcc-delivery.sql` when it has delivery times; TPC-DS: `tpcds-returns.sql` and `tpcds-ship.sql`, each when the source has it);
6. last, the source's name, so a schema with the name is a finished source and a rerun after a failure resumes in the copy.

Then one publication over every source.

From a notebook or job, with no password or driver in the code: writes go through the writer's TYPE JDBC connection, reads through the regular POSTGRESQL one (**lfc-source-contract**'s `lakebase.setup(..., writer=, jdbc_connection=)` makes both):

```python
%pip install "https://github.com/rsleedbx/databricks-agent-skills-tests/archive/refs/heads/main.zip#subdirectory=skills/lfc-source-datagen/scripts"

from lfc_source_contract import cdc
from lfc_source_contract.jdbc import dbapi
from lfc_source_datagen.sources import build
from lfc_source_datagen.tpch_sources import tpch_sources
connect = lambda: dbapi(spark, "orders_jdbc", read_connection="orders_pg", database="orders")
display(tpch_sources(connect, {"express": (0.05, "1,0,1,1,5,0,0,1,2"), "grocery": (0.1, "30,2,20,10,5,1,3,9,5"),
                               "aerospace": (0.03, "365,60,260,90,5,3,14,45,30")}, login="lfc_cdc", labels="anonymous"))
display(build(connect, "tpcc", {"east": 0.01, "west": 0.02}, login="lfc_cdc"))     # any other model
cdc.slot(connect().cursor(), "orders_slot")      # after the gateway and ingestion pipeline exist; drop=True when done
```

From the laptop: `connector("orders", lakebase="<project>", profile="<profile>")` (you, the project owner) or `connector("orders", host=..., password=...)` in place of `connect`. Or the commands: `lfc-tpch-sources --lakebase <project> --profile <profile> [--login R] [--labels anonymous] orders express:0.05:1,0,1,1,5,0,0,1,2 ...`, `lfc-tpcc-sources ... orders local:1:3,0,2,5,2,1,2,12,3,2 ...`, `lfc-tpcds-sources ... retail mass:1:40,80,40,15,1,30/6,1,3,2,5,3,80,2,3 ...`, `lfc-sources --model tpcds ...`, and `--slot <slot> [--drop]` for the slot. As a job: a `python_wheel_task` with those entry points and the archive URL in the environment's dependencies.

It fills with the skill's Python planner, which writes the same SQL as the SQL planners apart from its scramble constants. So every column a formula defines (keys, prices, quantities, dates, and so the lead times and labels) is the one `fill.sh` writes, and the filler text columns differ (`references/python-drivers.md`).

## Text values

The generic fill writes numbers into TPC-H's text columns. A person, Genie or a model reads words, so `tpch-labels.sql` writes the spec's:

- region and nation names, each nation in its spec region;
- market segments, order priorities, ship modes and ship instructions;
- `Supplier#000000001`-style names.

Each value comes from the row's key, so a rerun gives the same values. `--labels anonymous` writes `Nation 01` … `Nation 25` and `Region 1` … `Region 5` with the same map. Use it with lead times, where some nations are made slow: with real country names, that reads as a claim about real countries.

## Lead times

The spec draws a line's dates apart: commit 30 to 90 days after the order, receipt 1 to 30 days after ship. Lateness (receipt after commit) is then random, and nothing can learn it. `tpch-lead-times.sql` gives each source one business's committed lead time and three causes:

| Part | Days |
|---|---|
| commit | order + `commit` (the promise) |
| ship | order + `handle_min` + 0 … `handle_span` − 1, plus `slow_nation` when the supplier's nation key is a multiple of `slow_every` |
| receipt | ship + transit by ship mode (`air` for AIR and REG AIR, `truck` for TRUCK, MAIL and FOB, `sea` for RAIL and SHIP) + 0 … `jitter` − 1 |

The values are deterministic (from the keys). The spec's date rules no longer hold afterwards, so the script checks them before. The order of the nine numbers in `<name>:<sf>:<lead times>` is `commit,handle_min,handle_span,slow_nation,slow_every,air,truck,sea,jitter`.

Three profiles, measured as % of lines late:

| Source | Lead times | Slow nations vs other | AIR / TRUCK / SHIP |
|---|---|---|---|
| express (next-day) | `1,0,1,1,5,0,0,1,2` | 64.2 vs 14.2 | 20.8 / 20.7 / 70.8 |
| grocery (a month) | `30,2,20,10,5,1,3,9,5` | 36.2 vs 2.8 | 8.4 / 12.5 / 30.1 |
| aerospace (a year) | `365,60,260,90,5,3,14,45,30` | 30.1 vs 1.4 | 9.9 / 11.8 / 19.6 |

These numbers were measured on Lakebase (PostgreSQL 17) on 2026-11-14 at scale factors 0.05, 0.1 and 0.03. A rerun on PostgreSQL 14 at 0.03 on 2026-11-15 gave express 64.5 vs 14.4 and grocery 37.6 vs 2.5. A source without lead times keeps the spec's dates: 62.6% late, at random.

## TPC-C delivery times

In the spec a delivered line's `ol_delivery_d` is its order's `o_entry_d`: nothing is ever late, and TPC-C has no promised date. `tpcc-delivery.sql` gives each source one business's promise and four causes of an order's delivery time:

| Part | Days |
|---|---|
| handling | `handle_min` + 0 … `handle_span` − 1 |
| a slow district | `slow_district` when `o_d_id` is a multiple of `slow_every` |
| the carrier's tier | (`o_carrier_id` − 1) % 3 × `carrier_step` (three tiers of the ten carriers) |
| a credit hold | `credit_hold` when the customer's `c_credit` is BC (one in ten) |
| a backorder | `backorder` when a line's item has under `low_stock` in `stock` at its supplying warehouse |
| jitter | 0 … `jitter` − 1 |

An order is late when its delivery is more than `promise` days after its entry. Orders still in `new_order` stay open (no carrier, no delivery), as in the spec. The values come from the keys, so a rerun gives the same dates. With no promise column, `order_line`'s comment keeps the signal and its parameters as JSON: the summary reads the promise from it, and a report can name the signal from the data. The order of the ten numbers in `<name>:<sf>:<delivery>` is `promise,handle_min,handle_span,slow_every,slow_district,carrier_step,credit_hold,low_stock,backorder,jitter`.

Three profiles, as % of delivered orders late (21,000 per source at scale factor 1):

| Source | Delivery | All | Slow district vs other | Carrier tier 0 / 1 / 2 | Bad credit vs good | Backorder vs none |
|---|---|---|---|---|---|---|
| local (next day) | `3,0,2,5,2,1,2,12,3,2` | 39.9 | 74.5 vs 31.2 | 24.1 / 30.4 / 70.3 | 77.1 vs 35.7 | 85.8 vs 29.7 |
| regional (a week) | `7,1,4,5,4,2,5,12,6,3` | 47.6 | 84.0 vs 38.5 | 31.4 / 45.2 / 71.5 | 94.7 vs 42.3 | 97.6 vs 36.5 |
| national (three weeks) | `21,3,10,5,10,5,10,12,14,5` | 38.5 | 78.0 vs 28.6 | 22.9 / 37.3 / 60.5 | 76.7 vs 34.2 | 90.8 vs 27.0 |

Measured on PostgreSQL 14 on 2026-11-20. These numbers needed `order_line.ol_i_id` to point at an item (`fk_order_line_item` in statschema's `tpcc.yaml`); before it, the fill drew any integer there and only 25,007 of 300,000 lines had stock to check.

## TPC-DS returns and ship dates

The fill draws TPC-DS's returns and ship dates on their own: no `store_returns` row names a sale, and a web line can ship before it sold. `lfc_source_datagen.tpcds_sources` gives one source two signals, as one retailer has both, in `<name>:<sf>:<returns>/<ship>` (either half empty for none). Each signal's table comment keeps its name and parameters as JSON.

**Returns** (`tpcds-returns.sql`, signal `return_propensity`). The step rebuilds `store_returns` from `store_sales`, one row per returned line, with the line's item and ticket, so a return joins its sale. A line comes back when a hash of its keys % 1000 is under a per-mille rate:

| Part | Per mille |
|---|---|
| base | `base` |
| a fit category (`i_category` Clothing, Shoes) | `fit` |
| an active-discount promotion (`p_discount_active` Y) | `promo` |
| the household's income tier (`income_band` 1-5, 6-10, 11-15, 16-20: 0 … 3) | tier × `income_step` |

Whether a line comes back is a draw, so a model learns the rate per category, promotion and income, not each line.
- The return comes `lag_min` + 0 … `lag_span` − 1 days after the sale, with part of the line's quantity.
- Its reason follows the cause: 'Did not fit' for the fit categories, 'Changed mind' for a discounted sale, else any reason.
- The six numbers, in order: `base,fit,promo,income_step,lag_min,lag_span`.

**Ship dates** (`tpcds-ship.sql`, signal `web_ship_delay`). A web line ships this many days after its sale:

| Part | Days |
|---|---|
| handling | `handle_min` + 0 … `handle_span` − 1 |
| the ship mode's tier (`sm_type`: OVERNIGHT, NEXT DAY AIR, EXPRESS 0; TWO DAY, PRIORITY 1; GROUND, SURFACE, LIBRARY 2) | tier × `mode_step` |
| a slow warehouse (`ws_warehouse_sk` a multiple of `slow_every`) | `slow_wh` |
| a bulk line (`ws_quantity` of `bulk_qty` or more) | `bulk` |
| jitter | 0 … `jitter` − 1 |

- A line is late when it ships more than `promise` days after its sale.
- The days are date-key differences, since `d_date_sk` is a day number (1 is 1900-01-02, and `date_dim` is its calendar).
- The nine numbers, in order: `promise,handle_min,handle_span,mode_step,slow_every,slow_wh,bulk_qty,bulk,jitter`.

Three sources at scale factor 0.1, each with 288,040 store lines and 71,938 web lines:

| Source | Returns | Returned: all | Fit vs other | Promotion vs none | Income tier 0 → 3 |
|---|---|---|---|---|---|
| mass | `40,80,40,15,1,30` | 9.8 | 16.2 vs 8.3 | 11.8 vs 7.9 | 7.7 → 12.0 |
| fashion | `60,250,60,30,1,30` | 18.5 | 38.5 vs 13.5 | 21.3 vs 15.6 | 14.2 → 22.8 |
| hardlines | `20,30,20,5,1,14` | 4.4 | 6.8 vs 3.8 | 5.3 vs 3.4 | 3.6 → 5.1 |

| Source | Ship | Late: all | Mode tier 0 / 1 / 2 | Slow warehouse vs other | Bulk vs other |
|---|---|---|---|---|---|
| mass (standard) | `6,1,3,2,5,3,80,2,3` | 40.8 | 10.8 / 35.2 / 79.0 | 75.0 vs 32.2 | 63.8 vs 34.6 |
| fashion (express) | `2,0,2,1,5,2,80,1,2` | 49.7 | 20.2 / 48.5 / 84.2 | 91.9 vs 39.1 | 71.0 vs 44.0 |
| hardlines (marketplace) | `12,2,6,3,5,5,60,4,5` | 44.5 | 17.8 / 44.3 / 75.1 | 79.5 vs 35.7 | 66.1 vs 29.5 |

Measured on PostgreSQL 14 on 2026-11-20. Every return names its sale, comes on or after it and is no larger; no web line ships before it sold. The other channels' returns (`catalog_returns`, `web_returns`) are still drawn on their own.

## Publication

On PostgreSQL 15 and later, the publication is `FOR TABLES IN SCHEMA` the sources' schemas, so a table added later is in it. Before 15, that form doesn't exist, so it lists the tables there now by name. A rerun with other sources `ALTER`s the publication to the new list.
