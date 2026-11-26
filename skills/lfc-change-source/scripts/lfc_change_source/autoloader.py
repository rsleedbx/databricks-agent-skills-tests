"""autoloader: a gateway's change files into a change log as a stream (Auto Loader), instead of COPY INTO run again
and again: each file read once where the gateway wrote it, each event as land.gateway makes it (the same
destination and operation SQL). On serverless compute a stream runs continuously only in a pipeline (serverless jobs
take Trigger.AvailableNow alone), so pipelines/gateway_autoloader.py runs it as a continuous pipeline's append flow
into a Delta sink (the log).

    from lfc_change_source import autoloader
    df = autoloader.gateway_stream(spark, "/Volumes/.../<gateway id>", "gw_1")   # destination, operation, key, value, ...
"""
from __future__ import annotations

from .land import GATEWAY_DESTINATION, GATEWAY_OPERATION, gateway_key_sql

COLUMNS = ("destination", "operation", "key", "value", "gateway", "file")   # the log's, land.ensure_log


def gateway_stream(spark, root: str, name: str, schema_location: str | None = None, keys: dict | None = None):
    """A streaming DataFrame of the gateway's change events from <root>/cdc/, in the change log's columns (keys:
    land.gateway_keys, to fill each row's key)."""
    from pyspark.sql import functions as F
    reader = (spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")
              .option("singleVariantColumn", "event"))
    if schema_location:
        reader = reader.option("cloudFiles.schemaLocation", schema_location)
    return (reader.load(f"{root}/cdc/")
            .select(F.expr(GATEWAY_DESTINATION).alias("destination"), F.expr(GATEWAY_OPERATION).alias("operation"),
                    F.expr(gateway_key_sql(keys)).alias("key"), F.col("event").alias("value"),
                    F.lit(name).alias("gateway"), F.col("_metadata.file_path").alias("file")))
