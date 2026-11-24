"""lfc_change_source: the change-source contract (contract.py) and its adapters, one per way changes arrive.

ADAPTERS: each adapter module by name. A module has its source class (snapshot_sql, changes_sql, watermark, stats,
shared) and build(session, schema, snapshot, steps, columns, keys) for the conformance scenario (conformance.py).
"""
from . import cdc_log, gateway_log, gateway_staging, lfc_bronze

ADAPTERS = {"lfc_bronze": lfc_bronze, "cdc_log": cdc_log, "gateway_staging": gateway_staging,
            "gateway_log": gateway_log}   # a gateway's change files landed into the change log (land.gateway)
