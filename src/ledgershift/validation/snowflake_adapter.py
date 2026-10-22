"""Read Snowflake serving tables into Spark for reconciliation."""
from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import DataFrame, SparkSession

from ledgershift.snowflake.load import snowflake_options, validate_table_identifier


def read_snowflake_table(
    spark: SparkSession,
    table: str,
    *,
    options: Mapping[str, str] | None = None,
) -> DataFrame:
    """Read an existing Snowflake table through the Spark connector."""
    validate_table_identifier(table)
    connector_options = dict(options) if options is not None else snowflake_options()
    return (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**connector_options)
        .option("dbtable", table)
        .load()
    )
