# A Lakeflow pipeline source: a gateway's change files into a change log, continuously (lfc_change_source.autoloader).
# Configuration: cdc.root (the gateway's staging folder, /Volumes/...), cdc.log (the log table, quoted; it must exist:
# land.ensure_log), cdc.gateway (its name in the log's gateway column), cdc.package (the folder holding
# lfc_change_source, a workspace path).
import sys

from pyspark import pipelines as dp

sys.path.insert(0, spark.conf.get("cdc.package"))  # noqa: F821  spark: the pipeline's session
from lfc_change_source import autoloader  # noqa: E402

dp.create_sink("change_log", "delta", {"tableName": spark.conf.get("cdc.log")})  # noqa: F821


@dp.append_flow(target="change_log", name="gateway_changes")
def gateway_changes():
    return autoloader.gateway_stream(spark, spark.conf.get("cdc.root"), spark.conf.get("cdc.gateway"))  # noqa: F821
