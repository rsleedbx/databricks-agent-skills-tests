# A Lakeflow pipeline source: a gateway's change files into a change log, continuously (lfc_change_source.autoloader).
# Configuration: cdc.root (the gateway's staging folder, /Volumes/...), cdc.log (the log table, quoted; it must exist:
# land.ensure_log), cdc.gateway (its name in the log's gateway column), cdc.package (the folder holding
# lfc_change_source, a workspace path). Each row's key is filled in from the gateway's schema files, read when the
# pipeline starts (land.gateway_keys): restart it after adding tables, or their rows land with no key.
import sys

from pyspark import pipelines as dp

sys.path.insert(0, spark.conf.get("cdc.package"))  # noqa: F821  spark: the pipeline's session
from lfc_change_source import autoloader, land  # noqa: E402
from lfc_change_source import gateway_staging as gs  # noqa: E402

dp.create_sink("change_log", "delta", {"tableName": spark.conf.get("cdc.log")})  # noqa: F821


@dp.append_flow(target="change_log", name="gateway_changes")
def gateway_changes():
    root = spark.conf.get("cdc.root")  # noqa: F821
    keys = land.gateway_keys(gs.schemas(spark, root))  # noqa: F821
    return autoloader.gateway_stream(spark, root, spark.conf.get("cdc.gateway"), keys=keys)  # noqa: F821
