# How classify.py guesses a table's role, and how well

A change log carries every table the same way; silver doesn't have to. `classify.py` guesses, for each table, whether it
needs its current rows (SCD type 1), its history too (SCD type 2), or only its rows appended (no MERGE). A customer's
schema usually comes without documentation, so the first guess uses only what any schema has: keys, foreign keys, row
counts, column names and types, and the database's activity counts when it keeps them. For public software, a review
then checks the guess against the docs or the code (`--review`), and the customer has the last word (they edit
`plan.yaml`).

This page is the record of what the rules are, what they get right, what they get wrong and what that costs, and
which signals from published work and database catalogs could be added next. Update it with each change to the rules.

## What a wrong guess costs

The misses aren't equal. Sort them by what the pipeline then does:

| The guess | The truth | Silver then | Cost |
|---|---|---|---|
| append | updated or deleted | keeps each row as inserted | **changes lost**: updates and deletes never reach silver (bronze still has them; a rebuild as scd1 gets them back) |
| scd1 only | a dimension | no history table | **history lost** from then on (rebuildable from bronze, which keeps every event from T) |
| scd1 + scd2 | not a dimension | an extra history table | extra storage and work, nothing lost |
| scd1 | append-only | a MERGE where an append would do | extra work, nothing lost |
| another role, same silver | | the same tables | nothing |

So a guess of `append` needs the most evidence, and where it rests on little the plan asks (`ask:`) instead.

## The rules

In the order `classify.py` applies them. Each runs over the foreign-key graph; rules 5 and 7 depend on the neighbours'
roles, so the roles are computed in passes until nothing changes.

| # | Rule | Gives | Why | Misfires on |
|---|---|---|---|---|
| 1 | no primary key | log, append | nothing to merge on | none seen |
| 2 | named like a queue (`queue`), nothing below it | queue, scd1 | inserted, then deleted when processed | none seen |
| 3 | a fixed size in a scaled model, at most 4 columns besides keys, parents only lookups | lookup, scd1 | code tables: a code, a name, a description | TPC-DS `household_demographics` (a narrow dimension); misses `ship_mode`, TPC-E `exchange` (5+ columns) |
| 4 | a 1:1 extension of its only parent (its key is the foreign key to it) | the parent's role | the same entity, split | TPC-E `last_trade` (a fact on a dimension's key) |
| 5 | a key made of its foreign keys, few columns of its own or rows below it | bridge, scd1 | a many-to-many link | TPC-E `holding_summary` (a fact with one quantity) |
| 6 | the header of fact rows: a child's key holds its foreign key to this table, every child a fact or a log | fact | order -> order lines (TPC's detail rows carry the header's key) | ORM schemas, where every table has its own `id`: the signal is gone |
| 7 | named like a log (log, history, audit, events, stats, hits), no modified column: nothing below it, a log; else a fact | log append / fact scd1 | event tables in app schemas | Mautic `page_hits`, `dynamic_content_stats`: named like logs, but updated |
| 8 | exactly two parents, its own `id`, nothing below, a few plain columns | bridge, scd1 | an ORM's link table | none seen |
| 9 | several parents, at least as large as the largest, nothing below; or named `fact` | fact, scd1 | the star's centre | TPC-E `trade_request` (a queue) |
| 10 | one parent, nothing below, dated when added (`date_added`, `created_at`), no modified column, no name column | fact, scd1 | an event row (an opt-out, a download) | none seen |
| 11 | otherwise: referenced by others, or one parent at most | dimension, scd1 + scd2 | an entity others point at | TPC-C `new_order` (a queue) |

With **activity** (`--activity`: inserts, updates and deletes since the statistics were reset), a refinement after the
rules: a fact or log with inserts only and nothing referencing it becomes a log (append); one updated or deleted
becomes a fact (scd1); inserts and deletes with few updates, a queue. Activity never turns a dimension into a log: a
quiet dimension gets inserts only too, and a fixture load is inserts only. A fact others reference stays scd1 when the
window shows inserts only (an e-mail sent, opened later).

Then a constraint, which isn't a guess: a table with a primary key and a foreign key whose `ON DELETE` acts on it
(CASCADE, SET NULL, SET DEFAULT; the collector records `on_delete`) is never `[append]`. Its parent's deletes delete
or change its rows, so it gets `[scd1]`, and the role it was guessed stays. In Mautic this moves 19 tables named like
logs (`lead_event_log`, `page_hits`, the `*_stats` tables) to scd1. On MySQL through 9.5 those changes aren't in the
binlog either: silver applies them itself (`zerobus-debezium-test`, `cdc.fk_actions`).

## How well, relations only

Against roles from the specifications (each TPC table's `role:` in its schema) and from Mautic's code (`truth/mautic.yaml`) (25 tables whose writes were traced). The
silver column counts what the pipeline builds, which is what matters; the history column, whether a table got history
when it should.

| Schema | Tables | Role | Silver | History |
|---|---|---|---|---|
| TPC-H | 8 | 8 | 8 | 8 |
| TPC-B | 4 | 4 | 4 | 4 |
| TPC-DI | 18 | 18 | 18 | 18 |
| TPC-C | 9 | 8 | 8 | 8 |
| TPC-DS | 24 | 22 | 22 | 22 |
| TPC-E | 32 | 27 | 29 | 29 |
| **TPC** | **95** | **87 (92%)** | **89 (94%)** | **89** |
| Mautic 7.2.1 | 25 | 22 (88%) | 22 | 25 |

With the activity of two rounds of Mautic's own traffic, Mautic gets 24 of 25. The misses, by cost:

- **Changes lost:** Mautic `page_hits` and `dynamic_content_stats`, named like logs, guessed append. Mautic updates every
  `page_hits` row once after inserting it (40 inserts, 40 updates in two rounds); the code reading had said
  append-only, and the measured activity corrected it. `dynamic_content_stats` gets no traffic in the test, so it stays
  a question.
- **History lost:** TPC-DS `household_demographics` (narrow, fixed size, nothing below it: shaped like a lookup).
- **Extra cost only:** TPC-C `new_order` and TPC-E `trade_request` (queues, known only from activity), TPC-E
  `last_trade`, `account_permission`, `exchange`, TPC-DS `ship_mode` (an extra history table); Mautic `form_submissions`
  (a MERGE where an append would do).
- **Same silver:** TPC-E `holding_summary` (bridge for fact).

**This is fit accuracy, not a blind test.** The rules were tuned on these schemas. Mautic shows the gap on a schema the
rules hadn't seen: 19 of 25 on the first pass, 22 to 23 after the ORM signals (rules 7, 8, 10, the name and modified
columns) were added. A blind measure needs another application nobody tuned on (see below).

## Signals not used yet

From published work on deriving a dimensional model from an operational schema, and from the databases' own
catalogs. Each source was opened and the quoted text read there.

| Signal | Indicates | Source | Needs | Would help with |
|---|---|---|---|---|
| **The number of numeric fields** that aren't keys | the more, the more likely an event or fact: "the more numeric fields in an entity the more likely it is that the entity is an event or fact" | Phipps & Davis (below) | the schema | `holding_summary`, `last_trade` (quantities, prices); rule 5 and 9 count only DECIMAL and DOUBLE today, not integer quantities |
| **An event at a point in time with measurements** (transaction entities); entities related to them one-to-many (component); chains of one-to-many above those (classification) | facts; dimensions; hierarchies and lookups: "It describes an event that happens at a point in time", "It contains measurements or quantities that may be summarised" | Moody & Kortink (below) | the schema | rule 10 is the point-in-time half; a classification by chain depth could replace rule 3's size test, which fails in a collected schema (no scale factor) |
| **Relationship cardinality** | "the cardinality of a relationship determines how useful a related entity is to the schema being created" | Phipps & Davis | statistics: a foreign key column's distinct count against the table's rows | facts have many rows per parent; a 1:1 extension has one: `last_trade` against `security` |
| **The queries**: which tables are summed and counted, which only joined, filtered and grouped by | facts and dimensions as the workload uses them; Phipps & Davis evaluate candidate schemas with "queries to guide selection of candidate schemas most likely to meet user needs" | Phipps & Davis | the top queries (statschema's `collect-queries-*.sql` already collects them) | every case where structure is ambiguous; the strongest signal not used |
| **The physical order of a column's values** | a key or creation time near +1: rows arrive in that order and stay (an append-only table looks like this): "Statistical correlation between physical row ordering and logical ordering of the column values. This ranges from -1 to +1." | PostgreSQL, `pg_stats.correlation` | PostgreSQL statistics (collected with `values=ranges`) | append vs updated, without waiting for activity |
| **Updated, deleted and HOT-updated rows** | in-place changes; the share HOT-updated says the changes touch no indexed column | PostgreSQL, `pg_stat_user_tables`: `n_tup_upd` "Total number of rows updated", `n_tup_del`, `n_tup_hot_upd` "Number of rows HOT updated" | PostgreSQL statistics | `collect-activity-postgres.sql` reads the first two; the HOT share isn't used |
| **When a table was last written** | a quiet table (static reference data) against a busy one: "When the table was last updated. For some storage engines, this value is NULL." | MySQL, `information_schema.TABLES.UPDATE_TIME` | MySQL's catalog | lookup vs dimension, where size can't say |
| **Soft-delete and validity columns** (`deleted_at`, `is_deleted`, `valid_from`/`valid_to`) | a table that keeps its own history (scd1 is enough), or deletes by update | none: the test's own idea, to check | the schema | avoids history of history |
| **Names, read for their meaning** (a lexical method; today an LLM) | a table's subject: "leads", "page_hits", "segments" | SAMSTAR, a lexical method (below; not read past its title) | the schema | rules 2, 7, 9 read names by pattern only; reading them for meaning must say so in `why` |

Not read: Jensen, Holmgren and Pedersen discover multidimensional structure from relational data itself (the
publisher's page didn't open); Romero and Abelló survey the methods, data-driven ones included.

## What to try next

1. **The queries** (`collect-queries-*.sql` output into `classify.py --queries`): summed columns make facts, grouped-by
   tables dimensions. It's the one signal that sees how the customer uses the tables.
2. ~~Integer quantities as measures~~ (after Phipps & Davis): tried, no gain. Counting integer columns named like
   quantities (qty, count, amount, price, total, balance, points) as measures changed nothing alone, because rule 5
   takes a link with one column of its own first. Letting a measure override rule 5 made TPC-E `holding_summary` a
   fact (right) and `customer_taxrate` one too (wrong: a link whose one column is a rate): 27 of 32 either way (TPC-E had 32 tables then; `settlement` came later). One
   numeric column doesn't tell a fact's quantity from a link's attribute; the queries (1) or the activity can.
3. **Foreign key fan-out from statistics** (distinct count of the foreign key column against rows): 1:1 extensions and
   facts apart.
4. **`pg_stats.correlation` and `UPDATE_TIME`** where the collectors can get them, for append and quiet tables
   without a traffic window.
5. **A blind test**: a second application's schema (a public one with its own ORM, not Mautic), its truth from its
   code, classified before any rule is changed for it; the number goes in this page as the first unfitted one.

## Sources

- Cassandra Phipps, Karen C. Davis. "Automating Data Warehouse Conceptual Schema Design and Evaluation". DMDW 2002,
  CEUR-WS Vol-58. <https://ceur-ws.org/Vol-58/phipps-davis.pdf>
- Daniel L. Moody, Mark A.R. Kortink. "From enterprise models to dimensional models: a methodology for data warehouse
  and data mart design". DMDW 2000, CEUR-WS Vol-28. <https://ceur-ws.org/Vol-28/paper5.pdf>
- Il-Yeol Song, Ritu Khare, Bing Dai. "SAMSTAR: a semi-automated lexical method for generating star schemas from an
  entity-relationship diagram". DOLAP 2007. <https://doi.org/10.1145/1317331.1317334>
- Mikael R. Jensen, Thomas Holmgren, Torben Bach Pedersen. "Discovering Multidimensional Structure in Relational Data".
  DaWaK 2004, LNCS. <https://doi.org/10.1007/978-3-540-30076-2_14>
- Oscar Romero, Alberto Abelló. "A Survey of Multidimensional Modeling Methodologies". International Journal of Data
  Warehousing and Mining, 2009. <https://doi.org/10.4018/jdwm.2009040101>
- PostgreSQL: [`pg_stats`](https://www.postgresql.org/docs/current/view-pg-stats.html),
  [`pg_stat_user_tables`](https://www.postgresql.org/docs/current/monitoring-stats.html).
- MySQL 8.4: [`INFORMATION_SCHEMA.TABLES`](https://dev.mysql.com/doc/refman/8.4/en/information-schema-tables-table.html).
