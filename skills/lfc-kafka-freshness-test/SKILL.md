---
name: lfc-kafka-freshness-test
description: "Measure the Lakeflow Connect Kafka connector (Beta) from a Kafka topic into a Delta table: from a Kafka broker given as a source contract (SOURCE_JSON, engine kafka; lfc-source-eventhubs makes one on Azure), create a Unity Catalog KAFKA connection and a serverless continuous ingestion pipeline (value as VARIANT), send simulated events (optionally the same events to Zerobus at the same moments), then report rows sent vs landed, the per-second rate and burst seen in the data, the table's commits, and freshness before, during and after a burst. Use to measure event-to-queryable time or burst behavior through Kafka, or to compare it with a direct Zerobus producer on identical events."
parent: zerobus-sdk-freshness-test
---

# Kafka (Event Hubs) -> Lakeflow Connect -> Delta: freshness and bursts

The producer, the freshness probe and the report come from **zerobus-sdk-freshness-test**: `producer.py` sends to a
Kafka topic with `--kafka-*` (and to Zerobus at the same moments unless `--no-zerobus`), and `freshness_probe.py` and
`report.py` read the event time from inside the VARIANT value. The broker is a source: it comes through the source
contract (**lfc-source-contract**, engine `kafka`: bootstrap, SASL user, topic, the password file), which
**lfc-source-eventhubs** writes for an Azure Event Hubs namespace. This skill adds the connection and the pipeline:

| Script | Does | Changes anything |
|---|---|---|
| [`scripts/connection.sh`](scripts/connection.sh) | A UC connection of type `KAFKA` (`bootstrap_servers`, `sasl_mechanism`, `user`, `password`) from `SOURCE_JSON` (or `--bootstrap`, `--user`, `--password-file`); the password from the file into a 0600 request body, deleted after | Only with `--apply` |
| [`scripts/pipeline.sh`](scripts/pipeline.sh) | A serverless, continuous ingestion pipeline on channel `PREVIEW` into one table, the topic from `SOURCE_JSON` (or `--topic`): key `STRING`, value `VARIANT`; then the diagram of what was made (**lfc-pipeline-diagram**) | Only with `--apply` |
| [`scripts/test-scripts.sh`](scripts/test-scripts.sh) | Both against a fake `databricks`: the contract's fields, the password only in the request body, a non-Kafka contract refused, nothing created without `--apply` | No |

Run any script with no arguments for its usage. Any Kafka broker with SASL PLAIN works: write its contract by hand
(the format is in **lfc-source-contract**), or pass the flags.

## Workflow

0. **Plan and draw it.** Read the saved answers first (**lfc-test-settings**: `eval "$(bash skills/lfc-test-settings/scripts/settings.sh env)"`,
   then `settings.sh require DATABRICKS_PROFILE LFC_CATALOG` lists only what to ask). Pass the cloud identity on every call (`--profile` on each Databricks call; the broker's
   skill does the same for its cloud). An expired workspace login often still works through
   **account-token-fallback** (`ws-token.sh`). Draw the plan before creating anything: `pipeline.sh` without `--apply`
   prints the spec; give it to **lfc-pipeline-diagram**'s `pipeline-diagram.py --pipeline-json` with a `KAFKA`
   connection JSON. The probe needs a SQL warehouse: create a Small serverless one if the workspace has none (tagged,
   auto-stop), and delete it at the end.
1. **Broker:** a source contract for it: on Azure, **lfc-source-eventhubs** (`eventhubs.sh up ... --apply` writes
   `<namespace>.source.json`; size it for the burst there). `export SOURCE_JSON=<that file>`.
2. **Connection:** `connection.sh <PROFILE> <name> --apply` (the bootstrap, user and password file from `SOURCE_JSON`).
3. **Pipeline:** create the schema first, then
   `pipeline.sh <PROFILE> --connection <name> --table <catalog>.<schema>.<table> --apply` (the topic from `SOURCE_JSON`).
   A continuous pipeline starts on its own; wait for its update to be `RUNNING` before sending (`starting_offset`
   `latest` skips anything sent earlier).
4. **Probe, then producer** (from **zerobus-sdk-freshness-test**):

   ```bash
   Z=skills/zerobus-sdk-freshness-test/scripts; P=<PROFILE>; T=<catalog>.<schema>.<table>
   eval "$(bash skills/lfc-source-contract/scripts/source-env.sh "$SOURCE_JSON")"   # SRC_HOST, SRC_PORT, CONN_USER, SRC_TOPIC, SRC_PASSWORD_FILE
   uv run $Z/freshness_probe.py $P --seconds 1200 --out probe.json \
     --path "lfc_kafka|<warehouse-id>|$T|timestamp_micros(value:producer_time::bigint)" \
     --path "zerobus|<warehouse-id>|<zerobus table>" &
   uv run $Z/producer.py --kafka-bootstrap "$SRC_HOST:$SRC_PORT" --kafka-topic "$SRC_TOPIC" \
     --kafka-user "$CONN_USER" --kafka-password-file "$SRC_PASSWORD_FILE" <the Zerobus options, or --no-zerobus> \
     --minutes 15 --streams 10 --burst-at 5 --burst-minutes 2 --burst-x 10 --out run.json
   ```

5. **Report** each table with the same run file:

   ```bash
   uv run $Z/report.py $P --warehouse <warehouse-id> --table $T --run run.json --probe probe.json \
     --column 'timestamp_micros(value:producer_time::bigint)' --id-column 'value:event_id::string'
   ```

6. **Tear down only what this run made**, after recording (step 7): stop and delete the pipeline (it runs until
   stopped), drop the schema, delete the connection and the probe's warehouse if the run created it, then the broker
   with its own skill (**lfc-source-eventhubs**: `eventhubs.sh down`, which checks the `CreatedBy` tag), and the
   password file.
7. **Record** with **lfc-test-results** `record.py`: `test: load`, `setup.route: lfc-kafka`, `compute` as for any
   pipeline test, and the runtime from `bin/lfc-runtime.sh --summary <PROFILE> <pipeline-id>`.

## Reading the results

- **Freshness** is the warehouse's clock minus the newest `producer_time` in the table, as in
  **zerobus-sdk-freshness-test**. Through Kafka it also includes the broker and the pipeline's own commits.
- **Commits:** a pipeline writes `STREAMING UPDATE` commits, and `OPTIMIZE` commits of its own between them;
  `report.py` counts both and times the `STREAMING UPDATE` ones.
- **Same events on two paths:** with Zerobus and Kafka from one `producer.py`, both tables get each event with the same
  `event_id` and `producer_time`, so the counts and freshness compare directly. One process writing both reaches a
  lower burst rate than one writing only Zerobus; record the rate the data shows.

## Common Issues

| Symptom | Likely cause | Fix |
|---|---|---|
| `JSON transformer options were specified but no parsing configuration was provided. Specify at least one of 'schema', 'schema_file_path', or 'schema_evolution_mode', or omit json_options entirely to parse the value as a Variant column.` | `value_transformer` has `json_options`; seen with `{"schema_evolution_mode": "rescue"}` and no `schema` | Omit `json_options`: the value lands as `VARIANT`, as `pipeline.sh` does. A typed value with a `schema` wasn't tried here |
| The table stays empty while the producer sends | The pipeline started after the producer, with `starting_offset` `latest` | Start the producer once the update is `RUNNING`, or use `earliest` |
| The pipeline's flow fails: `[STREAM_FAILED] ... terminated with exception: Failed to create new KafkaAdminClient SQLSTATE: XXKST` | The connection's bootstrap server can't be reached (seen with a namespace that doesn't exist) | Check `bootstrap_servers` and port 9093 from serverless: `databricks connections get <name>` |
| The producer prints `Kafka messages not delivered` and exits 1 | The broker refused or never answered: bootstrap, port 9093, user or password | Check the connection string file and the namespace's network rules |

## Tested

`recall.py --test load --all`:

- `results/2026-11-03-lfc-kafka-contract.md`: the workflow above as written, the broker from **lfc-source-eventhubs**'s
  contract (`SOURCE_JSON`) for the connection, the pipeline and the producer: 4 minutes at 3,000/s with a 10x burst
  (29,789/s); every distinct event landed once (0 lost, 0 extra copies); freshness p50 3.9 s, p90 5.9 s.
- `results/2026-11-03-lfc-kafka-laptop.md`: the scripts as committed here, end to end on Azure (centralus), producer on
  a laptop, Kafka only: 3,000/s for 15 minutes with a 10x burst that reached 29,938/s on average; freshness p50 3.9 s,
  p90 5.3 s; 330 more rows than sent, each an extra copy (no event id more than twice).
  That run's producer didn't yet count the duplicates it sends on purpose (`dups_sent`), so loss isn't ruled out exactly. Teardown
  removed only what the run made.
- `results/2026-10-29-lfc-kafka-freshness.md`: an earlier producer in a Databricks job, writing Zerobus and Kafka from
  one process: the burst reached about 21,000/s.
