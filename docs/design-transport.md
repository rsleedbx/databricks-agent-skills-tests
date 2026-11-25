# FlowThrough: CDC (transport), latency and throughput

Layer 1a, CDC ([design.md](design.md)), turns every transport into one shape: change events in an append-only change log
(`destination`, `operation`, `key`, `value`), one row an event, the event kept as it came (VARIANT). Nothing after this
layer knows which transport a table came from; each adapter passes one conformance scenario (lfc-change-source). The
log carries changes only: each table's snapshot is a table of its own ([design-no-full-refresh.md](design-no-full-refresh.md)).

| Transport | Changes arrive as | Position | Into the log by | Adapter |
|---|---|---|---|---|
| Lakeflow Connect ingestion pipeline | the pipeline's own streaming tables (SCD type 1 or 2): already one source's silver, so straight to layer 3 | Delta version or commit time | none | `lfc_bronze` |
| Lakeflow Connect gateway, staging read in place | JSON change files in the gateway's staging volume, every table mixed | the gateway's cursor | not landed: read where it is | `gateway_staging` |
| Lakeflow Connect gateway, staging landed | the same files | the gateway's cursor | `COPY INTO` (polled) or Auto Loader (continuous) | `gateway_log` (`land.gateway`, `autoloader`) |
| Debezium Server through Zerobus | rows Zerobus writes into the log | the source's own (LSN, binlog position, SCN, ...) | Zerobus: no landing compute | `cdc_log` |
| CockroachDB changefeed (`format='json'`, `initial_scan = 'no'`) | JSON files in cloud storage or a Volume, a folder a table | the HLC | `COPY INTO` (`land.changefeed`) | `changefeed_log` |

## Latency

A change reaches silver after the log's latency plus its silver cell's: Zerobus writes each event as it comes, with no
landing compute; a gateway's change files are copied once each, polled or by a continuous Auto Loader stream (seconds
per file). The busiest tables are kept by AUTO CDC flows in a continuous pipeline, so their silver follows the log by a
micro-batch; the rest catch up each round ([design-scale.md](design-scale.md)).

## Throughput

One log per transport, clustered by table, so a reader of one table skips the others' files. A busy table makes every
other table's read skip its events (clustering helps only once OPTIMIZE has rewritten the new files), so a table gets
a log of its own once it's big and fast, or once its rate passes `own_log_events_per_s`; past what one stream
carries (`shard_events_per_s`, Zerobus's documented 100,000 records a second per stream), its log is sharded by key
hash. Split by transport, not inside a log: a gateway's change files mix all its tables, so such a table goes on a
gateway (or a Debezium target table) of its own, and each landing stays a copy of whole files.

Moving a table between logs is a cut-over at a position P: its events after P land in the new log, and readers read
the old log up to P and the new one after it (`routing.history_fq`); no new snapshot.

What limits it: a gateway's tables (adding tables slows as it holds more), its change capture's cadence, landing (one
`COPY INTO` per gateway shares the change files), and Zerobus's per-stream and per-table rates. Measured values for
these are kept with their configuration in internal notes, since they change with the warehouse, the gateway and the
source.
