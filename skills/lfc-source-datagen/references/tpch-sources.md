# Any TPC model as several business sources

[`lfc_source_datagen.sources`](../scripts/lfc_source_datagen/sources.py) builds any TPC model (TPC-B, C, H, DS, E, DI) as n business sources in one PostgreSQL database: one schema per line of business (or acquired company), each at its own scale, with the same tables and repeating keys (each copy starts at 1). That is the source of an n:1 test: Lakeflow Connect ingests each schema, and **lfc-medallion** stacks them into one table per table with a `_source` column. [`lfc_source_datagen.tpch_sources`](../scripts/lfc_source_datagen/tpch_sources.py) adds TPC-H's own steps: the spec's words and lead times a model can learn. Other models' signals come from their schema: [`lfc_source_datagen.signals`](../scripts/lfc_source_datagen/signals.py) reads each `signals:` block in statschema's `schemas/<model>.yaml` and runs it. A block names its SQL step (`tpc/<file>`), the table whose comment keeps the parameters, which parameters must be 1 or more, and a summary query. The parameters go into that table's comment as `lfc:signal=<JSON>`, after the fill's tags (`lfc:rows`, `lfc:n`, `lfc:by`), so a later fill of the table, such as an append, still works. A model has one signal per table. A new signal is a SQL file and a YAML block, with no Python. `lfc-sources --model <model>` runs them, for example TPC-C's delivery times and TPC-DS's returns and web ship dates. `lfc-tpcc-sources` and `lfc-tpcds-sources` stay as the same command.

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

An order is late when its delivery is more than `promise` days after its entry. Orders still in `new_order` stay open (no carrier, no delivery), as in the spec. The values come from the keys, so a rerun gives the same dates. With no promise column, `order_line`'s comment keeps the signal and its parameters (`lfc:signal=<JSON>`): the summary reads the promise from it, and a report can name the signal from the data. The order of the ten numbers in `<name>:<sf>:<delivery>` is `promise,handle_min,handle_span,slow_every,slow_district,carrier_step,credit_hold,low_stock,backorder,jitter`.

Three profiles, as % of delivered orders late (21,000 per source at scale factor 1):

| Source | Delivery | All | Slow district vs other | Carrier tier 0 / 1 / 2 | Bad credit vs good | Backorder vs none |
|---|---|---|---|---|---|---|
| local (next day) | `3,0,2,5,2,1,2,12,3,2` | 39.9 | 74.5 vs 31.2 | 24.1 / 30.4 / 70.3 | 77.1 vs 35.7 | 85.8 vs 29.7 |
| regional (a week) | `7,1,4,5,4,2,5,12,6,3` | 47.6 | 84.0 vs 38.5 | 31.4 / 45.2 / 71.5 | 94.7 vs 42.3 | 97.6 vs 36.5 |
| national (three weeks) | `21,3,10,5,10,5,10,12,14,5` | 38.5 | 78.0 vs 28.6 | 22.9 / 37.3 / 60.5 | 76.7 vs 34.2 | 90.8 vs 27.0 |

Measured on PostgreSQL 14 on 2026-11-20. These numbers needed `order_line.ol_i_id` to point at an item (`fk_order_line_item` in statschema's `tpcc.yaml`); before it, the fill drew any integer there and only 25,007 of 300,000 lines had stock to check.

## TPC-DS returns and ship dates

The fill draws TPC-DS's returns and ship dates on their own: no `store_returns` row names a sale, and a web line can ship before it sold. `lfc_source_datagen.tpcds_sources` gives one source two signals, as one retailer has both, in `<name>:<sf>:<returns>/<ship>` (either half empty for none). Each signal's table comment keeps its name and parameters (`lfc:signal=<JSON>`).

**Returns** (`tpcds-returns.sql`, signal `return_propensity`). The step rebuilds the returns of all three channels from their sales: `store_returns` from `store_sales` (item and ticket), `catalog_returns` and `web_returns` from `catalog_sales` and `web_sales` (item and order number), one row per returned line, so a return joins its sale. A line comes back when a hash of its keys % 1000 is under a per-mille rate, the same in every channel, each drawing from its own hash:

| Part | Per mille |
|---|---|
| base | `base` |
| a fit category (`i_category` Clothing, Shoes) | `fit` |
| an active-discount promotion (`p_discount_active` Y) | `promo` |
| the household's income tier (`income_band` 1-5, 6-10, 11-15, 16-20: 0 … 3; catalog and web: the bill-to household) | tier × `income_step` |

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

Measured on PostgreSQL 14 on 2026-11-20. Every return names its sale, comes on or after it and is no larger; no web line ships before it sold.

A catalog or web return is refunded to the sale's bill-to customer and returned by its ship-to one. `cs_promo_sk` and `ws_promo_sk` are foreign keys to `promotion`, as in the spec, so the promotion cause applies in every channel. The three channels with `mass`'s numbers at scale factor 0.1, as % of lines returned:

| Channel | Lines | All | Fit vs other | Promotion vs none | Income tier 0 → 3 |
|---|---|---|---|---|---|
| store | 288,140 | 9.8 | 16.2 vs 8.2 | 11.6 vs 8.1 | 8.3 → 11.4 |
| catalog | 144,255 | 9.8 | 16.2 vs 8.3 | 11.8 vs 7.8 | 7.6 → 12.1 |
| web | 72,038 | 9.8 | 16.3 vs 8.2 | 11.8 vs 7.9 | 7.5 → 12.0 |

## TPC-E trade failures

The fill gives every trade status CMPT and draws its bid and history times on its own, so nothing predicts a cancelled or a slow trade. `tpce-trades.sql` (signal `trade_failure`, from `tpce.yaml`'s `signals:`) gives each source one brokerage's handling. A trade fails when it's cancelled or completes more than `promise` minutes after it was submitted.

| Part | Minutes, or how often cancelled |
|---|---|
| a limit order's bid (`TLB`, `TLS`) | 0 … `spread` per mille from the security's `last_trade.lt_price`, below to buy, above to sell; over `far`, cancelled `cancel_far` percent of the time |
| any trade cancelled | `cancel_base` per mille |
| handling | `handle_min` + 0 … `handle_span` − 1 |
| a limit order waiting | `limit_wait` |
| a slow broker (`customer_account.ca_b_id` a multiple of `slow_every`) | `slow_broker` |
| the customer's tier | (`c_tier` − 1) × `tier_step` |
| a margin trade (`t_is_cash` 0) | `margin` |
| jitter | 0 … `jitter` − 1 |

- `trade_history` is rebuilt to match: SBMT at `t_dts`, PNDG a second later for a limit order, then CMPT or CNCL.
- `settlement` keeps the completed trades. Its cash type comes from `t_is_cash`, and its due date is two days after completion.
- `holding` and `holding_history` drop the rows of cancelled trades (a cancelled trade opens no holding and changes none), and `cash_transaction` keeps only completed cash trades, as the settlement's cash type says.
- The thirteen numbers, in order: `promise,handle_min,handle_span,limit_wait,slow_every,slow_broker,tier_step,margin,jitter,spread,far,cancel_far,cancel_base`.

Three sources at scale factor 0.05 (166,938 trades each), `lfc-sources --model tpce`, as % of trades failed:

| Source | Numbers | All | Cancelled | Limit far / near / market | Slow broker vs other | Tier 1 / 2 / 3 | Margin vs cash |
|---|---|---|---|---|---|---|---|
| online (8 min) | `8,0,3,3,5,4,1,2,3,50,30,60,5` | 25.0 | 9.9 | 74.5 / 34.6 / 8.2 | 61.1 vs 16.0 | 18.3 / 24.1 / 32.7 | 32.3 vs 17.7 |
| full_service (26 min) | `26,2,10,8,5,15,4,6,5,80,40,50,3` | 33.4 | 10.1 | 72.0 / 44.1 / 17.1 | 85.4 vs 20.4 | 23.4 / 31.9 / 44.8 | 41.3 vs 25.5 |
| institutional (75 min) | `75,5,30,20,5,40,10,15,10,100,50,40,2` | 26.6 | 8.1 | 60.1 / 33.2 / 13.4 | 77.8 vs 13.8 | 18.3 / 26.6 / 35.0 | 32.8 vs 20.5 |

Measured on PostgreSQL 14 on 2026-11-20. In each source no history row comes before its trade, no cancelled trade keeps a settlement, every completed one has one, and every settlement's cash type matches `t_is_cash`.

## TPC-B unusual withdrawals

The fill's `h_delta` is the row number divided by 100, so no amount means anything. `tpcb-withdrawals.sql` (signal `unusual_withdrawal`, from `tpcb.yaml`'s `signals:`) gives each source one bank's history. A row is a large withdrawal at a per-mille rate:

| Part | Per mille |
|---|---|
| base | `base` |
| a risky teller (`h_t_id` a multiple of `teller_every`) | `teller` |
| a watch-listed account (`h_a_id` a multiple of `account_every`) | `account` |
| a night hour of `h_time` (from `night_from`, before `night_to`, wrapping past midnight) | `night` |

- A large withdrawal is −`big_min` … −(`big_min` + `big_span` − 1) dollars, and every other row is ±1 … `normal_max`. A row is unusual when `h_delta` ≤ −`big_min`, so keep `normal_max` below `big_min`.
- Every account's, teller's and branch's balance becomes the sum of its deltas, so the three sums equal the history's, as in the spec.
- The branch isn't a cause: TPC-B has one per scale-factor unit, so it rarely varies in a test source.
- `history` has no key. The planner reads its row number from `h_time` (one second per row), and the step reads it there too, so the amounts can be rewritten and a later append still works.
- The eleven numbers, in order: `base,teller_every,teller,account_every,account,night_from,night_to,night,normal_max,big_min,big_span`.

Three sources at scale factor 1 (100,000 history rows each), `lfc-sources --model tpcb`, as % of rows unusual:

| Source | Numbers | All | Risky teller vs other | Watched account vs other | Night vs day | None of the three |
|---|---|---|---|---|---|---|
| retail | `5,5,60,50,150,22,6,40,500,5000,20000` | 3.0 | 9.0 vs 2.2 | 20.6 vs 2.9 | 5.5 vs 1.2 | 0.5 |
| cash_heavy | `10,5,120,20,100,20,4,60,2000,10000,40000` | 5.2 | 18.0 vs 3.5 | 22.9 vs 4.7 | 9.8 vs 1.8 | 1.0 |
| online | `2,5,10,100,300,0,5,30,300,3000,10000` | 1.2 | 2.5 vs 1.0 | 30.6 vs 1.1 | 3.3 vs 0.5 | 0.2 |

Measured on PostgreSQL 14 on 2026-11-20. In each source the account, teller and branch sums equal the history's, and no normal row is larger than `normal_max`. An append fill after the signal added 1,000 history rows that continue the row numbers.

## TPC-DI account closings

The fill draws an account's `Status` (6% Inactive) and every cash balance on its own, so nothing predicts a closing. `tpcdi-account-close.sql` (signal `account_close`, from `tpcdi.yaml`'s `signals:`) gives each source one broker-dealer's book.
- **Cash:** each account's cash moves by its own drift, −`drift_span` … `drift_span` dollars from one `FactCashBalances` row to the next by date.
- **Falling:** an account is falling when its drift is below −`fall_at` and it has two or more balances, so the fall shows in the data.
- **Closing:** an account closes (`Status` Inactive) at a per-mille rate:

| Part | Per mille |
|---|---|
| base | `base` |
| the customer's tier (`DimCustomer.Tier`; tier 3 closes most) | (`Tier` − 1) × `tier_step` |
| a slow broker (`SK_BrokerID` a multiple of `broker_every`) | `broker` |
| a falling balance | `falling` |

- `FactCashBalances` has 3,000 rows per scale factor, two balances for each account, so every account shows a trend.
- A closing changes `Status` in place; it doesn't add an SCD version.
- The seven numbers, in order: `base,tier_step,broker_every,broker,falling,drift_span,fall_at`.

Three sources at scale factor 5 (7,500 accounts each), `lfc-sources --model tpcdi`, as % of accounts closed:

| Source | Numbers | All | Tier 1 / 2 / 3 | Slow broker vs other | Falling vs other | Falling |
|---|---|---|---|---|---|---|
| retail | `20,30,10,120,250,500,200` | 13.5 | 10.6 / 12.6 / 16.4 | 25.5 vs 12.7 | 31.2 vs 5.9 | 30.0 |
| wealth | `8,15,10,60,150,500,250` | 6.5 | 5.2 / 6.2 / 7.9 | 12.5 vs 6.2 | 17.9 vs 2.8 | 24.9 |
| discount | `40,40,5,100,300,300,100` | 19.8 | 16.3 / 19.1 / 23.3 | 25.2 vs 19.0 | 40.5 vs 9.6 | 33.3 |

Measured on PostgreSQL 14 on 2026-11-20. The closing draw has its own hash, so the cash shows nothing of it: in `retail` the closed rate is 13.1 to 13.9% in every quintile of the first balance. `DimAccount`'s comment keeps its fill tags with the signal after them, and an append fill after the signal added 100 accounts.

## Appending to a source

`lfc-sources --model <model> --append SF <database> <source> ...` grows existing sources by SF scale factors and re-applies their signals with the parameters kept in their tables' comments, so the new rows follow them too.
- **Growth:** each table gets its rows per scale factor (`lfc:sf_rows`, as built) times SF, so the tables keep their ratios.
- **Fixed tables:** a fixed-size table gets none (TPC-DS `income_band`, `reason`, `date_dim`; TPC-H `nation`, `region`), and neither does a code table whose key values are all used (`lfc:fixed`).
- **Rebuilt tables:** a table a signal rebuilds (its YAML block's `rebuilds:`) is left to the signal.
- **Too small an SF:** one that would give a scaling table less than one row is refused, with the smallest that works. A parent that doesn't grow would leave the children keyed under it pointing past it. TPC-B grows by whole branches (`--append 1`). TPC-C grows by whole warehouses, so a source built at 0.05 grows by 0.05.
- **Whole steps:** the SF must be a whole number of steps of the smallest scaling table (1 ÷ its rows per scale factor). TPC-B 1.4 would give 1 branch but 14 tellers, so it's refused.
- **Rounds:** the growth runs as the fill's own rounds, to each table's rows at the start plus its share, so a statement still has at most `--rows` rows and the rounds go on until every table reaches its target. A second `--append` measures from the rows there then and grows again, so after a failed one, check the tables' counts before running it again.
- **Parents of new rows:** a foreign key outside the key has a formula built from its parent's rows at build, so the new rows point at the parents that were there then (new history at the original tellers and accounts, new orders at the original customers). New parents get children only through their keys: a new branch's accounts, a new warehouse's districts and orders.
- **`--append 0`:** it only re-applies.

Each step changes only rows whose value differs, using `IS DISTINCT FROM`, deleting unwanted rows and inserting missing ones. On unchanged rows it changes none, so under CDC an append shows the new rows and what they change. The summary row starts with the number of rows the signals changed.

Measured on PostgreSQL 14 on 2026-11-20, one source per model. In each, the append ran without errors, the fixed-size tables kept their rows, the signals' rates held, and a rerun (`--append 0`) changed 0 rows.

| Model (built at) | --append | Grew | Fixed, unchanged |
|---|---|---|---|
| TPC-C (0.05) | 0.05 | every table but `item` doubled: one more warehouse, district, its customers and orders | `item` |
| TPC-B (1) | 1 | one more branch, its 10 tellers, 100,000 accounts and 100,000 history rows (0.01 is refused) | — |
| TPC-H (0.05) | 0.01 | supplier, part, partsupp, customer, orders, lineitem by 20% | `nation`, `region` |
| TPC-DS (0.02) | 0.01 | the sales, returns, `inventory`, `item`, `customer` and the other scaling tables by 50% | 11 tables: `date_dim`, `time_dim`, the demographics, `income_band`, `reason`, `ship_mode`, `store`, `warehouse`, `call_center`, `web_site` |
| TPC-E (0.01) | 0.005 | 22 tables by 50%, `trade` 33,388 → 50,082 | 9 tables: `zip_code`, `taxrate`, the code tables, `industry`, `commission_rate` |
| TPC-DI (1) | 0.5 | 13 tables by 50% | `DimDate`, `DimTime`, `Industry`, `TaxRate`, `StatusType`, `TradeType` |

## Publication

On PostgreSQL 15 and later, the publication is `FOR TABLES IN SCHEMA` the sources' schemas, so a table added later is in it. Before 15, that form doesn't exist, so it lists the tables there now by name. A rerun with other sources `ALTER`s the publication to the new list.
