# The Lakeflow Connect test space

What a test can vary, what's been covered, and what's planned. A test result picks one value from each row; record the values in the result's `host`, `subject`, `workspace` and `setup` fields, so `recall.py` can show which combinations are known. Anyone can add a value nobody listed: record it, then add it here in the same pull request.

Check what's covered with `uv run scripts/recall.py --coverage` (test × engine) and `--all` with filters (`--workspace`, `--storage`, `--host`, `--engine`).

## Dimensions

| Dimension | Record it in | Values | Covered so far |
|---|---|---|---|
| Where the source runs | `host` | `azure-vm-podman`, `aws-vm-podman`, `gcp-vm-podman`, `local-podman`, managed services (`azure-flexserver`, `azure-sql`, `aws-rds`, `aws-aurora`, `gcp-cloudsql`), `lakebase` | Azure VM, AWS VM, Lakebase |
| Engine and version | `subject.engine`, `subject.version`, `subject.image` | MySQL, PostgreSQL, SQL Server, Oracle (the four with Lakeflow Connect database connectors); versions for compatibility tests, e.g. MySQL 5.7 / 8.0 / 8.4, Postgres 13-17, SQL Server 2017-2025, Oracle 19c / 21c / 23ai | one version of each |
| Storage under the database | `setup.storage` | `xfs` (local disk), `xfs-reflink` (clone of a master), `juicefs` (branch of a master in object storage), `rclone-pull` (master copied down from object storage) | all four, Postgres + 5 engines |
| Workspace | `workspace.cloud`, `workspace.env`, `workspace.compute`, `workspace.catalog` | cloud `aws` / `azure` / `gcp`; env `fevm` (field sandbox) / `dogfood`; compute `serverless-only` / `classic`; catalog `Default Storage` / `own managed storage` | Azure FEVM serverless-only; AWS FEVM classic |
| Compute | `compute` (`serverless`, `ran_on`, `started`, `reached_source`, `blocked_by`, `switched_from`; `gateway` for standard CDC) | set `unset` / `true` / `false` (`LFC_SERVERLESS`), times what it ran on, serverless / classic, on a `serverless-only` or `classic` workspace | Azure classic: unset → classic, started, source not reached (firewall); rerun with true → serverless, pass. Unset on a serverless-only workspace not yet recorded per pipeline |
| Lakeflow Connect mode | `test` | `connection`, `qbc` (query-based through the connection), `fcqbc` (through a foreign catalog), `icdc` (integrated CDC), `scdc` (standard CDC: gateway + applier), `dml` | all modes; some refused by the workspace |
| Workload | `setup.workload` | `{"name": "seed"}` (intpk / dtix), `{"name": "tpch", "sf": 1}`, TPC-C, TPC-DS, TPC-E, TPC-DI, TPC-B (`lfc-source-datagen`, `lfc-source-statschema`) | seed tables only |
| Snapshot size | `metrics.rows`, `metrics.bytes` | from the workload: scale factor 1, 2, 4, ... | 10 rows (ingestion), 5M-30M rows (branch layers) |
| Change rate | `setup.dml` | `{"rate_per_s": 100, "mix": "insert:update:delete=3:2:2", "minutes": 10}` | a single burst of 7 changes per table |
| Credentials on the connection | `setup.credentials` | `password` (in the connection), `uc-secret-akv` (Azure Key Vault secret, referenced from a UC connection), `uc-secret-aws-sm` (AWS Secrets Manager, referenced) | password only |
| Network path | `setup.network` | `public-ip` (firewall to serverless and classic egress), `private-network-gateway` ([announcement](https://www.databricks.com/blog/introducing-private-network-gateway)), `privatelink`, `vpc-peering` | public IP only |
| Change-log CDC (Zerobus) | `setup.producer`, `setup.bronze`, `setup.silver` | producer `debezium-server-zerobus` / `kafka-connect-zerobus`; bronze `shared` / `fanout` / `per-table`; silver `pipeline-auto-cdc` / `merge-job` / `none` (`zerobus-debezium-test`) | CockroachDB, empty TPC-mix tables: pipeline to 1000, MERGE to 4000; 8000 over UC's tables per schema; one producer to about 100-250 tables |
| Kafka into Delta (Lakeflow Connect) | `test: load`, `setup.route`, `setup.broker`, `setup.pipeline`, `setup.runtime`, `compute` | route `lfc-kafka`; broker Azure Event Hubs (Kafka endpoint; throughput units, partitions); pipeline serverless, continuous, channel `PREVIEW`, value `VARIANT` (`lfc-kafka-freshness-test`) | Azure FEVM centralus: one 15-minute run with a 10x burst, Zerobus and Kafka from one producer |
| Who ran it | `setup.contributor` | a GitHub handle or team name, never an email | |

## Planned

In rough order; each line becomes records when it runs.

1. **Preloaded masters at TPC-H (and TPC-C) SF 1, 2 and 4** for each engine, kept in object storage, so snapshot tests don't load data first (`lfc-source-datagen`, `lfc-source-cloud-vm` master snapshots).
2. **Sizing guidance:** for each size, snapshot and CDC throughput on `xfs`, `juicefs` and `rclone-pull`, to say what size can stay on a branch, what should be copied down, and how long a copy takes.
3. **Change-rate tests:** CDC lag and pipeline duration at fixed DML rates on each engine.
4. **Secrets by reference:** UC connections whose password comes from an Azure Key Vault secret and an AWS Secrets Manager secret, on each engine.
5. **Private network gateway** instead of public IPs, on AWS and Azure. Needs an account where the tester is account admin (Premium), or an admin to set up the NCC: delegate a `/28` in the test VNet (same region and Entra ID tenant as the workspace), create the gateway, give the VM a DNS name (private DNS zone) or use `ALL_TRAFFIC`, and allow only the gateway subnet in the VM's NSG. Then run the matrix with `LFC_SERVERLESS=true` and record `setup.network: private-network-gateway`. On AWS (Private Preview, through the account team), the same, plus PrivateLink to an NLB as `privatelink`. See lfc-source-firewall, "Private paths instead of allowlisting".
6. **The default on a classic workspace, reaching the source:** add the classic egress IP to the firewall (lfc-source-firewall, "Classic compute egress"), run the matrix with `LFC_SERVERLESS=unset`, and record what each pipeline ran on and whether it reached the source.
7. **Version compatibility:** the supported versions of each engine in Podman (JuiceFS branches make many versions cheap to keep), published as a version × mode grid.
8. **Dogfood workspaces** (AWS, Azure) alongside FEVM, classic and serverless-only.

## Known so far

- Workspaces refuse some modes until the account team enables them: the error is `Ingestion source '<ENGINE>' is not supported in your workspace`. Record it as `blocked` with the exact text.
- CDC needs a catalog with its own managed storage; query-based doesn't.
- A serverless-only workspace rejects classic gateways; `serverless: true` works for SQL Server standard CDC.
- On a workspace with classic compute, `serverless` left unset put integrated CDC (MySQL, SQL Server) and the gateway on classic (Azure, 2026-10). With the test firewall allowing only the serverless ranges, they started and then couldn't reach the source; with `serverless: true` they passed. Query-based ran serverless.
- Classic took about 8 minutes to start; serverless 10 s to 1 min 20 s.
- Sandbox workspaces can disappear within hours: read back compute and row counts right after a run (`run_matrix.sh` now does the compute part).
