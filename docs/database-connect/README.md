# Database connectors: changes into one change log

Patterned on the Lakeflow Connect [managed database connectors](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/cdc-overview)
overview. A database's changes reach Databricks by one of two routes, and both end in the same place: an
append-only change log (one Delta table for every source table), then silver tables kept from it. Back to the
[README](../../README.md).

| Route | What reads the database | Where changes land | When to use it |
|---|---|---|---|
| Debezium through Zerobus | Debezium Server, run by you, next to the database | Zerobus writes each change into the change log | a database Lakeflow Connect doesn't cover, or the change log itself as bronze |
| Lakeflow Connect gateway | the ingestion gateway (a Databricks pipeline) | its staging volume, landed into the change log (lfc-change-source `land.gateway`) | databases Lakeflow Connect supports; many sources into one set of tables |

## Supported databases

| Database | Debezium through Zerobus (each tested end to end through the fan-in) | Lakeflow Connect gateway |
|---|---|---|
| Microsoft SQL Server | [create an ingestion pipeline](sqlserver/debezium-zerobus.md) | [create an ingestion pipeline](sqlserver/lakeflow-connect-gateway.md) |
| PostgreSQL | [create an ingestion pipeline](postgresql/debezium-zerobus.md) | Lakeflow Connect's own: [lakeflow-connect-database.md](../lakeflow-connect-database.md) |
| CockroachDB | [create an ingestion pipeline](cockroachdb/debezium-zerobus.md) | not supported by Lakeflow Connect |
| Informix | [create an ingestion pipeline](informix/debezium-zerobus.md) | not supported by Lakeflow Connect |

SQL Server on an Always On availability group: [sqlserver/always-on.md](sqlserver/always-on.md). The other
databases tested through Zerobus are in [lakeflow-connect-zerobus.md](../lakeflow-connect-zerobus.md); through a
gateway, in [lakeflow-connect-database.md](../lakeflow-connect-database.md).

## Connector components

| Component | Debezium through Zerobus | Lakeflow Connect gateway |
|---|---|---|
| Connection | a Unity Catalog connection to read the source's schema (`remote_query`) | the Unity Catalog connection the gateway uses |
| Reader | Debezium Server, outside Databricks, outbound 443 only | the ingestion gateway, a continuous pipeline |
| Staging | none: Zerobus writes the log directly | the gateway's staging volume |
| Change log | one append-only Delta table (`destination`, `operation`, `key`, `value`), clustered by destination | the same table, with the staging files landed into it |
| Silver | lfc-log-silver: each table a view, a MERGE, AUTO CDC, or a group's shared table | the same |

The tests fill their sources with the TPC models; each model's tables and keys: [tpc-models.md](../tpc-models.md).

Every route gives the change log the same columns, so silver doesn't know which route a table came from. The four
layers, transport to gold: [design.md](../design.md).

## Network connectivity

- **Debezium through Zerobus:** Debezium Server reaches the database on its port, and Zerobus on 443. Nothing in
  Databricks connects to the database except the schema reads.
- **Lakeflow Connect gateway:** the gateway's compute connects to the database (serverless: allow the workspace's
  serverless egress; classic: the workspace's network). See the Lakeflow Connect
  [network requirements](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/cdc-overview).
