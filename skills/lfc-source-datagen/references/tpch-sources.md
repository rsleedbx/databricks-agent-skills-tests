# TPC-H as several business sources

[`scripts/tpc/tpch-sources.sh`](../scripts/tpc/tpch-sources.sh) builds TPC-H as n business sources in one Postgres database: one schema per line of business (or acquired company), each at its own scale, with the same tables and repeating keys (each copy starts at 1). That is the source of an n:1 test: Lakeflow Connect ingests each schema, and **lfc-medallion** stacks them into one table per table with a `_source` column.

Per source it:

1. builds spec TPC-H at the source's scale (`workload.sh --model tpch --spec`), fills it as copy N, and checks it against the spec (`tpch-verify.sql`);
2. renames the copy to the source's name;
3. writes the spec's words into the text columns (`tpch-labels.sql`);
4. optionally sets lead times a model can learn (`tpch-lead-times.sql`);
5. grants the contract's login `SELECT` on the schema.

Then it creates one publication over every source. The slot is a separate step (`tpch-sources.sh slot`), run after the pipelines exist, so nothing holds WAL before a gateway reads it.

```bash
SOURCE_JSON=src.json bash scripts/tpc/tpch-sources.sh --labels anonymous orders_db \
  express:0.05:1,0,1,1,5,0,0,1,2  grocery:0.1:30,2,20,10,5,1,3,9,5  aerospace:0.03:365,60,260,90,5,3,14,45,30
# after the gateway and ingestion pipeline exist:
SOURCE_JSON=src.json bash scripts/tpc/tpch-sources.sh slot orders_db orders_slot
# when the gateway is stopped for good:
SOURCE_JSON=src.json bash scripts/tpc/tpch-sources.sh slot orders_db orders_slot --drop
```

The contract (**lfc-source-contract**) gives the administrator's `sql_cmd` (it creates the database and the publication; for Lakebase, `lakebase-sql.sh`) and the login (`user`) the grants go to. The scale factor is at least 0.03 (TPC-H needs 230 suppliers). A schema that already exists is kept as it is.

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

## Publication

On PostgreSQL 15 and later, the publication is `FOR TABLES IN SCHEMA` the sources' schemas, so a table added later is in it. Before 15, that form doesn't exist, so it lists the tables there now by name. A rerun with other sources `ALTER`s the publication to the new list.
