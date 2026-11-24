"""lfc_change_source: the change-source contract (contract.py) and its adapters, one per way changes arrive.

ADAPTERS: each adapter module by name. A module has its source class (snapshot_sql, changes_sql, watermark, stats,
shared) and build(session, schema, snapshot, steps, columns, keys) for the conformance scenario (conformance.py).
"""
from types import SimpleNamespace

from . import cdc_log, gateway_staging, lfc_bronze

ADAPTERS = {"lfc_bronze": lfc_bronze, "cdc_log": cdc_log, "gateway_staging": gateway_staging,
            # a gateway's change files landed into a change log (land.gateway), read as a change log
            "gateway_log": SimpleNamespace(__name__="gateway_log", build=cdc_log.build_gateway, CdcLog=cdc_log.CdcLog)}
