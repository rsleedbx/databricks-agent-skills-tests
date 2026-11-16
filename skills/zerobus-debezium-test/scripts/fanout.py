# fanout.py: one streaming read of the shared change log, written out per table or per shard, so each silver table
# reads only its own events. Producers keep writing the one shared log, which needs no DDL for a new table or a new
# column: a table that appears gets its bronze table here, created on its first events, and nothing upstream stops.
# Rows are copied as they are (the log's own shape); silver-from-schema.py normalizes them.
#
#   fanout.py --source <catalog.schema.cdc_log> --target '<catalog.schema>.cdc_log_{table}' | '<...>.cdc_log_s{shard}'
#             [--shards K] [--tables t1,t2] --checkpoint /Volumes/<catalog>/<schema>/<volume>/_fanout [--once]
#
# --tables: only those tables, so tables move out of the shared log one at a time (the hot ones first) while the rest
# stay in it; silver-from-schema.py's cdc.bronze_table.<table> points a moved table at its own. A new checkpoint
# starts at the log's beginning, so the moved table's bronze holds its history from T (as long as the log is kept).
#
# A serverless job task (spark_python_task). --once processes what's there and stops (availableNow); without it the
# stream runs continuously. Exactly once per target: each micro-batch writes each target with txnAppId (the target)
# and txnVersion (the batch id), so a retried batch is skipped by Delta. The shard rule is silver-from-schema.py's.
# Each target re-reads the micro-batch (serverless has no cache), so a micro-batch costs one read per target it
# touches: {table} suits up to hundreds of tables; for 10,000s, {shard} with K about the number of silver pipelines.
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ap = argparse.ArgumentParser()
ap.add_argument("--source", required=True)
ap.add_argument("--target", required=True)
ap.add_argument("--shards", type=int, default=1)
ap.add_argument("--checkpoint", required=True)
ap.add_argument("--once", action="store_true")
ap.add_argument("--tables", default="")
a = ap.parse_args()
spark = SparkSession.builder.getOrCreate()


def write(batch, batch_id):
    b = batch.withColumn("_table", F.substring_index("destination", ".", -1))
    if a.tables:
        b = b.where(F.col("_table").isin(a.tables.split(",")))
    if "{shard}" in a.target:
        # Spark's crc32 is zlib's (CRC-32, unsigned), so this is canonical_sql.shard_of
        b = b.withColumn("_part", (F.crc32(F.col("_table").cast("binary")) % a.shards).cast("string"))
        name = lambda p: a.target.format(shard=p)   # noqa: E731
    else:
        b = b.withColumn("_part", F.col("_table"))
        name = lambda p: a.target.format(table=p)   # noqa: E731
    # no persist: serverless doesn't cache, so each target re-reads the micro-batch (from the log's new files)
    for p in [r[0] for r in b.select("_part").distinct().collect()]:
        target = name(p)
        (b.where(F.col("_part") == p).drop("_table", "_part").write.format("delta").mode("append")
         .option("txnAppId", f"fanout:{target}").option("txnVersion", batch_id).saveAsTable(target))


q = (spark.readStream.table(a.source).writeStream.foreachBatch(write)
     .option("checkpointLocation", a.checkpoint))
q = q.trigger(availableNow=True) if a.once else q.trigger(processingTime="10 seconds")
q.start().awaitTermination()
