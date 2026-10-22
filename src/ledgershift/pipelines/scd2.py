"""Slowly changing dimension type 2 (SCD2) helpers for customer history."""
from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, sha2, struct, to_json, to_timestamp


def with_attribute_hash(frame: DataFrame, tracked_columns: Sequence[str]) -> DataFrame:
    """Add a stable hash for tracked attributes, normalizing nulls explicitly."""
    missing = sorted(set(tracked_columns).difference(frame.columns))
    if missing:
        raise ValueError("tracked columns missing: " + ", ".join(missing))
    if not tracked_columns:
        raise ValueError("tracked_columns must contain at least one column")
    # JSON struct serialization preserves null positions and avoids delimiter collisions.
    values = [col(name).alias(name) for name in tracked_columns]
    return frame.withColumn("_attribute_hash", sha2(to_json(struct(*values)), 256))


def stage_scd2(
    current: DataFrame,
    incoming: DataFrame,
    key: str,
    tracked_columns: Sequence[str],
    effective_at: str,
) -> tuple[DataFrame, DataFrame]:
    """Return keys to expire and incoming versions to insert.

    Current must contain only current dimension rows. The caller persists the two outputs
    according to its storage strategy; this function is deterministic and has no side effects.
    """
    for label, frame in (("current", current), ("incoming", incoming)):
        missing = sorted({key, *tracked_columns}.difference(frame.columns))
        if missing:
            raise ValueError(f"{label} dimension missing columns: " + ", ".join(missing))
    if key in tracked_columns:
        raise ValueError("the business key cannot be a tracked attribute")

    current_hashed = with_attribute_hash(current, tracked_columns).alias("c")
    incoming_hashed = with_attribute_hash(incoming, tracked_columns).alias("i")
    joined = incoming_hashed.join(current_hashed, col(f"i.{key}") == col(f"c.{key}"), "left")
    changed = col(f"c.{key}").isNotNull() & (
        col("i._attribute_hash") != col("c._attribute_hash")
    )
    is_new = col(f"c.{key}").isNull()
    changed_or_new = joined.filter(changed | is_new)

    expire_keys = (joined.filter(changed).select(col(f"c.{key}").alias(key))
                   .dropDuplicates([key]))
    insert_columns = list(incoming.columns)
    new_versions = changed_or_new.select(
        *[col(f"i.{name}").alias(name) for name in insert_columns],
        to_timestamp(lit(effective_at)).alias("valid_from"),
        lit(None).cast("timestamp").alias("valid_to"),
        lit(True).alias("is_current"),
        col("i._attribute_hash").alias("_attribute_hash"),
    )
    return expire_keys, new_versions
