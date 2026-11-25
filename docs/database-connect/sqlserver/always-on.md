# SQL Server on an Always On availability group

How each route behaves when the database is in an availability group, and how to keep reads off the primary. Back to
[database connectors](../README.md).

## Debezium through Zerobus

Debezium connects to one node. CDC is enabled on the primary, and its capture and cleanup jobs run there; the
availability group copies the change tables to the secondaries. Point Debezium at the listener (it follows a
failover) or at the primary. Not tested here on a secondary.

## Lakeflow Connect gateway

The Lakeflow Connect docs support the primary, or the listener that reaches it
([SQL Server limits](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-limits)). When the
reads must stay off the primary, this is what we saw with the gateway on a readable secondary (SQL Server 2022, a
two-node availability group, utility objects 1.7):

| Step | On the readable secondary |
|---|---|
| Snapshot and CDC changes | work: reads only |
| Change tracking | not possible: SQL Server refuses change tracking reads on a secondary |
| `ALTER TABLE ... ADD` a column | the table stops: the gateway's follow-up writes (its capture instance switch) fail on a read-only database |
| Data freshness | minutes of lag (a periodic write fails and is retried) |

**What fixes a stopped table:** the gateway itself has to finish the capture instance switch, on the primary. Move
the gateway to the primary while the switch is pending, then back:

1. Keep two Unity Catalog connections with the same login, one to each node, and point the gateway at the primary's
   (the gateway's `connection_name`).
2. Wait until the table has one capture instance again (`SELECT capture_instance FROM cdc.change_tables WHERE
   source_object_id = OBJECT_ID('<schema>.<table>')`); in our test it took about 80 seconds after the `ALTER`.
3. Point the gateway back at the secondary's connection.

Don't finish the switch from outside the gateway (running the utility objects' merge or disable procedures yourself):
in our test the gateway kept failing, then found the old capture instance gone and asked for a full refresh of the
table.

The skill [lfc-sqlserver-secondary](../../../skills/lfc-sqlserver-secondary/SKILL.md) does the move, the waits and the
checks (`scripts/secondary.sh`), and the login and grants every node needs.
