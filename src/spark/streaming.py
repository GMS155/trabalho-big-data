import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from schemas.vehicle import RAW_SCHEMA, rename_columns

logger = logging.getLogger(__name__)

HDFS_INCOMING   = "hdfs://namenode:9000/incoming"
HDFS_PROCESSED  = "hdfs://namenode:9000/processed"
HDFS_CHECKPOINT = "hdfs://namenode:9000/checkpoint/streaming"


def _clean(df: DataFrame) -> DataFrame:
    df = rename_columns(df)
    df = df.filter(
        F.col("energy_consumption").isNotNull()
        & F.col("speed_kmh").isNotNull()
        & (F.col("speed_kmh") >= 0)
    )
    return df


def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
    count = batch_df.count()
    if count == 0:
        logger.info("Batch %s is empty – skipping.", batch_id)
        return
    logger.info("Batch %s → %s rows → writing to %s", batch_id, count, HDFS_PROCESSED)
    (
        batch_df
        .write
        .mode("append")
        .parquet(HDFS_PROCESSED)
    )


def start_streaming(spark: SparkSession) -> None:

    stream_df = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .option("maxFilesPerTrigger", 1)   # one file per micro-batch
        .schema(RAW_SCHEMA)
        .load(HDFS_INCOMING)
    )

    cleaned_df = _clean(stream_df)

    query = (
        cleaned_df.writeStream
        .foreachBatch(_write_batch)
        .option("checkpointLocation", HDFS_CHECKPOINT)
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("Streaming query active (id=%s).", query.id)
    query.awaitTermination()
