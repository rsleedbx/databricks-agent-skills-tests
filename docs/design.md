# FlowThrough: from source changes to gold

FlowThrough is intelligent, low latency, high throughput, scales to 10,000s of tables and supports n:1 fan-in. Best of
all, there is never a full table refresh, by design.

- **Intelligent:** table relationships and database statistics decide whether each table is a dimension, a fact or
  realtime, and give it the snapshot and silver that fit. [design-intelligent.md](design-intelligent.md)
- **Latency:** Zerobus writes each change straight into the log, or a gateway's files are copied in once each; the
  busiest tables are kept by AUTO CDC in a continuous pipeline. [design-transport.md](design-transport.md)
- **Throughput:** one append-only log per transport, clustered by table; a table busy enough to slow the others gets
  a log of its own, sharded by key. [design-transport.md](design-transport.md)
- **Scale:** small tables share one table of changes and one MERGE a round, with a view each; big ones get a MERGE of
  their own or AUTO CDC. [design-scale.md](design-scale.md)
- **Fan-in:** each source (a line of business, a tenant, a region) into one table each, whatever transport or silver
  method it came through. [design-fanin.md](design-fanin.md)
- **No full refresh:** the source is read once a table; a schema change is an event at its position, a suspect part is
  re-read alone, and a checkpoint resnapshots from silver. [design-no-full-refresh.md](design-no-full-refresh.md)

FlowThrough is a technique, not a package: six layers, each with many sources or ways in, and no layer depends on
another's choices.

```mermaid
flowchart LR
  src[("Source<br/>databases")]
  subgraph PL ["Plan"]
    direction LR
    intro["Workspace: introspection<br/>schema + statistics"]
    plan[("Plan contract<br/>schema + stats tables,<br/>routing's decisions")]
    intro --> plan
  end
  subgraph SN ["Snapshot"]
    direction LR
    sw["Workspace: gateway,<br/>QBC, spark.read.jdbc"]
    so["Outside: the customer's<br/>own export (files)"]
    snap[("Snapshot contract<br/>a Delta or streaming<br/>table per source table")]
    sw --> snap
    so --> snap
  end
  subgraph LG ["Append-only log"]
    direction LR
    lw["Workspace: Lakeflow Connect<br/>gateway, Debezium + Zerobus"]
    lo["Outside: Debezium + Zerobus"]
    log[("Log contract<br/>one or more logs,<br/>changes only")]
    lw --> log
    lo --> log
  end
  silver["Silver<br/>snapshot + the<br/>log's later changes"]
  n1["n:1<br/>many sources'<br/>tables into one"]
  gold["Gold<br/>marts"]
  src --> intro
  src --> sw
  src --> so
  src --> lw
  src --> lo
  plan --> silver
  snap --> silver
  log --> silver
  silver --> n1 --> gold
```

## Three contracts

FlowThrough meets every source at three contracts, staged in Delta, and the process (silver, n:1, gold) reads only them:

| Contract | What it holds | Staged as | Today |
|---|---|---|---|
| Plan | the source's schema and statistics, and each table's route | Delta tables | lfc-introspection's `introspection_schema` and `introspection_stats`; lfc-routing's `routing_decisions` |
| Snapshot | every table's rows once, after its log has started | a Delta or streaming table per source table | lfc-change-source `SNAPSHOTS` (`qbc`, `jdbc`), `intake` |
| Append-only log | every table's changes, into one log or several | Delta tables, changes only | lfc-change-source's adapters (`cdc_log`, `gateway_log`, ...) |

Snapshots land per table, by any mix of methods; changes land into one or more logs, by any mix of transports. Silver
never knows which method or transport a table came through.

## n:1 consolidation

The same business data is scattered across an enterprise's databases. Each database can come through a different
transport, as long as it meets the contracts: MySQL through a Lakeflow Connect gateway and Db2 through Debezium and
Zerobus, say, consolidated into one silver, n:1 and gold.

## Transport

A transport lands contract data from a source into FlowThrough's staging. The ones tested for FlowThrough:

| Contract | Outside Databricks | In the Databricks workspace |
|---|---|---|
| Plan | | introspection: the source's schema and statistics |
| Snapshot | the customer's own export (files, landed by `intake.files`) | Lakeflow Connect gateway, QBC, `spark.read.jdbc` |
| Append-only log | Debezium + Zerobus | Lakeflow Connect gateway, Debezium + Zerobus |

## Layers

| Layer | What it does | Detail | Skill |
|---|---|---|---|
| 1. Routing | first, the plan: the schema, the statistics and the tables in scope decide each table's CDC log, snapshot method and first silver cell; then every round, its measured rate moves it | [design-intelligent.md](design-intelligent.md) | lfc-routing `routing`, lfc-log-silver `flowthrough` |
| 2. CDC | every source's changes (Zerobus, a gateway, a changefeed, ...) into one shape: an append-only change log, changes only | [design-transport.md](design-transport.md) | lfc-change-source |
| 3. Snapshot | every table's rows once, by the method routing chose (a direct JDBC read, QBC, a gateway's own, the customer's files), into a Delta table of its own, after its CDC has started | [design-no-full-refresh.md](design-no-full-refresh.md) | lfc-change-source (`qbc`, `jdbc_job`, `intake`) |
| 4. Silver | combines them: the snapshot table and the log's changes after it, kept by the cell routing chose (view, shared group, MERGE, AUTO CDC) | [design-scale.md](design-scale.md) | lfc-log-silver `silver` |
| 5. n:1 | many sources' tables into one | [design-fanin.md](design-fanin.md) | lfc-medallion |
| 6. Gold | marts on the n:1 tables | [design-fanin.md](design-fanin.md) | lfc-medallion-pack |

So the cases add up instead of multiplying: a new CDC source is one row in layer 2, a new snapshot source one row in
layer 3, a new kind of table one cell in silver.

Where the code doesn't meet this design yet, and what has been tested: [design-audit.md](design-audit.md).
