"""Load validated gold DataFrames into Snowflake using the Spark connector.

Credentials are read from environment variables and are never logged.
The caller supplies DataFrames explicitly so this module does not own pipeline
or catalog naming policy.
"""
from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

_REQUIRED = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
             "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA")


def snowflake_options(env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Build connector options and fail early when required settings are absent."""
    source = os.environ if env is None else env
    missing = [key for key in _REQUIRED if not source.get(key)]
    if missing:
        raise ValueError("Missing required Snowflake settings: " + ", ".join(missing))

    return {
        "sfURL": source["SNOWFLAKE_ACCOUNT"],
        "sfUser": source["SNOWFLAKE_USER"],
        "sfPassword": source["SNOWFLAKE_PASSWORD"],
        "sfWarehouse": source["SNOWFLAKE_WAREHOUSE"],
        "sfDatabase": source["SNOWFLAKE_DATABASE"],
        "sfSchema": source["SNOWFLAKE_SCHEMA"],
        "sfRole": source.get("SNOWFLAKE_ROLE", ""),
    }


def load_dataframe(
    dataframe: Any,
    table: str,
    *,
    mode: str = "append",
    options: Mapping[str, str] | None = None,
) -> None:
    """Write a Spark DataFrame to Snowflake; only append/overwrite are allowed.

    For repeatable production loads, use a run-scoped staging table followed by
    a transactional Snowflake MERGE keyed by the source business key. Direct
    append is intentionally not described as idempotent.
    """
    if not table or not all(part.replace("_", "").isalnum() for part in table.split(".")):
        raise ValueError("table must be a simple Snowflake identifier, optionally schema-qualified")
    if mode not in {"append", "overwrite"}:
        raise ValueError("mode must be 'append' or 'overwrite'")

    connector_options = dict(options) if options is not None else snowflake_options()
    writer = (
        dataframe.write.format("net.snowflake.spark.snowflake")
        .options(**connector_options)
        .option("dbtable", table)
        .mode(mode)
    )
    writer.save()
