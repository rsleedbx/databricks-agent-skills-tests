"""lfc_change_source: the change-source contract (contract.py) and its adapters, one per way changes arrive.

ADAPTERS: each adapter module by name. A module has its source class (snapshot_sql, changes_sql, watermark, stats,
shared) and build(session, schema, snapshot, steps, columns, keys) for the conformance scenario (conformance.py).
SNAPSHOTS: each snapshot method by name, chosen apart from the transport (docs/design-no-full-refresh.md): qbc, a
query-based pipeline; the others are a transport's own (a gateway's staging, a changefeed's initial scan, in the log).
Whatever the method, a snapshot read in place reaches silver as a Delta table with the source's column names and types.
routing.py says which log and snapshot path each table takes, by its class (dimension, big_slow, big_fast).
"""
from . import cdc_log, changefeed_log, gateway_log, gateway_staging, lfc_bronze, qbc

ADAPTERS = {"lfc_bronze": lfc_bronze, "cdc_log": cdc_log, "gateway_staging": gateway_staging,
            "gateway_log": gateway_log,   # a gateway's change files landed into the change log (land.gateway)
            "changefeed_log": changefeed_log}   # a CockroachDB changefeed's files landed (land.changefeed)

SNAPSHOTS = {"qbc": qbc}   # qbc.snapshot(w, ...) takes it; qbc.snapshot_table(catalog, schema, table) names it
