"""lfc_change_source: the change-source contract (contract.py) and its adapters, one per way changes arrive.

ADAPTERS: each adapter module by name. A module has its source class (snapshot_sql, changes_sql, watermark, stats,
shared) and build(session, schema, snapshot, steps, columns, keys) for the conformance scenario (conformance.py).
SNAPSHOTS: each snapshot method by name, chosen apart from the transport (docs/design-no-full-refresh.md): qbc, a
query-based pipeline; jdbc, Spark's jdbc source in a serverless job (an engine with no query-based connector); the
others are a transport's own (a gateway's staging, a changefeed's initial scan, in the log). Each has snapshot() and
snapshot_table().
Whatever the method, a snapshot read in place reaches silver as a Delta table with the source's column names and types.
Which log, snapshot method and silver cell each table takes is routing's decision (lfc-routing).
"""
from . import cdc_log, changefeed_log, gateway_log, gateway_staging, jdbc_job, lfc_bronze, qbc

ADAPTERS = {"lfc_bronze": lfc_bronze, "cdc_log": cdc_log, "gateway_staging": gateway_staging,
            "gateway_log": gateway_log,   # a gateway's change files landed into the change log (land.gateway)
            "changefeed_log": changefeed_log}   # a CockroachDB changefeed's files landed (land.changefeed)

SNAPSHOTS = {"qbc": qbc, "jdbc": jdbc_job}   # .snapshot(...) takes it; .snapshot_table(catalog, schema, table) names it
