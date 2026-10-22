"""Incremental CDC application with stale-event protection and batch idempotency.

The batch ledger is intentionally separate from the business table. Replaying a completed
batch is a no-op; target MERGE predicates also make event-level replays safe. Run one writer
per target/ledger pair to avoid concurrent batch-ledger races.
"""
from __future__ import annotations

from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


def latest_cdc_events(events: DataFrame, key: str) -> DataFrame:
    """Return one event per key, choosing the highest LSN deterministically."""
    required = {key, "op", "lsn"}
    missing = sorted(required.difference(events.columns))
    if missing:
        raise ValueError("CDC events missing required columns: " + ", ".join(missing))
    invalid_op = events.filter(~col("op").isin("I", "U", "D"))
    if invalid_op.limit(1).count():
        raise ValueError("CDC op must be one of I, U or D")
    if events.filter(col(key).isNull() | col("lsn").isNull()).limit(1).count():
        raise ValueError("CDC key and lsn must be non-null")
    ordering = Window.partitionBy(key).orderBy(col("lsn").desc())
    return (events.withColumn("_cdc_rank", row_number().over(ordering))
            .filter(col("_cdc_rank") == 1).drop("_cdc_rank"))


def merge_cdc(
    spark: SparkSession,
    target_path: str | Path,
    events: DataFrame,
    batch_id: str,
    ledger_path: str | Path,
    key: str = "order_id",
) -> dict[str, int | str | bool]:
    """Apply one CDC batch to an existing Delta target, once per batch ID.

    The target must already exist as a Delta table and contain the key and lsn columns.
    A single writer is required because Delta cannot atomically commit two independent tables.
    """
    if not batch_id or not batch_id.strip():
        raise ValueError("batch_id must be a non-empty string")
    target = str(target_path)
    ledger = str(ledger_path)
    if not DeltaTable.isDeltaTable(spark, target):
        raise ValueError(f"CDC target is not an existing Delta table: {target}")
    if not DeltaTable.isDeltaTable(spark, ledger):
        (spark.createDataFrame([], "batch_id STRING")
         .write.format("delta").mode("overwrite").save(ledger))
    ledger_table = DeltaTable.forPath(spark, ledger)
    if ledger_table.toDF().filter(col("batch_id") == batch_id).limit(1).count():
        return {"batch_id": batch_id, "already_processed": True, "events_received": 0}

    staged = latest_cdc_events(events, key)
    received = staged.count()
    target_table = DeltaTable.forPath(spark, target)
    target_columns = target_table.toDF().columns
    missing_target_values = sorted(set(target_columns).difference(staged.columns))
    if missing_target_values:
        raise ValueError(
            "CDC events missing target columns: " + ", ".join(missing_target_values)
        )
    # CDC operation is control metadata, not a business-table column. Explicit mappings
    # prevent Delta updateAll/insertAll from trying to write the op column into the target.
    update_values = {name: f"s.{name}" for name in target_columns if name != key}
    insert_values = {name: f"s.{name}" for name in target_columns}
    (target_table.alias("t")
     .merge(staged.alias("s"), f"t.{key} = s.{key}")
     .whenMatchedDelete(condition="s.op = 'D' AND s.lsn >= t.lsn")
     .whenMatchedUpdate(condition="s.op <> 'D' AND s.lsn > t.lsn", set=update_values)
     .whenNotMatchedInsert(condition="s.op <> 'D'", values=insert_values)
     .execute())

    # Append only after the target merge succeeds. Replays remain event-safe if a process
    # crashes between the merge and ledger write; the single-writer constraint prevents races.
    spark.createDataFrame([(batch_id,)], "batch_id STRING").write.format("delta").mode("append").save(ledger)
    return {"batch_id": batch_id, "already_processed": False, "events_received": received}
