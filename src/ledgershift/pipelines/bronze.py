"""Append-only bronze ingestion preserving source lineage."""
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, lit


def ingest_json(
    spark: SparkSession,
    source: str | Path,
    target: str | Path,
    batch_id: str,
) -> None:
    frame = spark.read.json(str(source))
    (
        frame.withColumn("_ingest_ts", lit(datetime.now(UTC).isoformat()))
        .withColumn("_source_file", input_file_name())
        .withColumn("_batch_id", lit(batch_id))
        .write.format("delta")
        .mode("append")
        .save(str(target))
    )
