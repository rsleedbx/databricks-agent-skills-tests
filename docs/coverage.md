# What's been tested

Each line comes from `results/runs/*.jsonl`. Check the current state with `uv run skills/lfc-test-results/scripts/recall.py --coverage`, and see the plan in [test-matrix.md](../skills/lfc-test-results/references/test-matrix.md#planned). Back to the [README](../README.md).

What the tests turned up for each combination (source behaviors, connector behaviors, Databricks limits, gaps in the pattern), and the fix or workaround for each, is in [findings.md](findings.md).

## Test axes: what's been tested and works

From `results/runs/*.jsonl`, 2026-10. Each line is a value that passed at least once; a value that's missing hasn't been tested yet, which doesn't mean it fails. Blocked results aren't listed: a mode the workspace hasn't enabled, a missing grant, or a firewall that doesn't allow the compute's IP all work once fixed.

| Axis | Tested and works |
|---|---|
| Where the source runs | Azure managed databases (Azure SQL Database; Flexible Server for MySQL and PostgreSQL); Lakebase; Podman on an AWS VM and on an Azure VM; Podman in a laptop Lima VM (data generation only) |
| Database and version | Lakeflow Connect: Lakebase, MySQL 8.4, Oracle 23ai Free (the same image reports itself as 26ai Free 23.26), PostgreSQL 16, SQL Server 2022. Zerobus (Debezium or Kafka Connect): CockroachDB 25.4, Db2 11.5, Informix 14.10, MySQL 8.4, Oracle, PostgreSQL 16, SQL Server 2022, YugabyteDB 2026.1 |
| Workspace | AWS and Azure field sandboxes (FEVM), each serverless-only and with classic compute; target catalog with its own managed storage (every mode) or on Default Storage (query-based only) |
| Compute | `serverless: true` on every pipeline and gateway. Left unset: query-based ran serverless; integrated CDC and the gateway ran classic on a classic workspace (Azure), which needs the classic egress IP in the source's firewall: that path isn't tested yet |
| Network path | public IP, firewall open to the workspace's serverless egress ranges; also cross-cloud (an AWS workspace reading the Azure VM) |
| Credentials | password stored in the UC connection |
| Storage under the database | local XFS; XFS reflink clones of a master; JuiceFS branches (object storage); rclone copies of a master |
| Data | seed tables (`intpk`, `dtix`); TPC-B, -C, -H, -DS, -E and -DI at SF 0.1 and SF 1; pgbench |
| Data generation | the CLI planners (psql, mysql, sqlcmd, sqlplus) and the Python planner through pip drivers, on six databases |


## Lakeflow Connect modes, by database

See the table in the [README](../README.md#tested-end-to-end-lakeflow-connect-modes-by-database).

**Doesn't work, by design** (the product refuses it; as of 2026-10):

- CDC (integrated or standard) into a catalog on Default Storage: `Default Storage for Unity Catalog is not supported on LakeFlow Connect pipelines`. Query-based works there.
- Classic compute, including a classic gateway, on a serverless-only workspace: `You must use serverless compute in this workspace`.
- `system` and `hive_metastore` as the target catalog.
- Modes the product doesn't offer for a source (integrated CDC from PostgreSQL or Lakebase; standard CDC from Oracle): the skill should say so and create nothing, which the matrix checks.

GCP, private network paths (private network gateway, PrivateLink), secrets by reference, other database versions, managed services on AWS and GCP, and larger data volumes aren't tested yet: see the plan in [test-matrix.md](../skills/lfc-test-results/references/test-matrix.md#planned).
