"""Snowflake load helpers with identifier validation and retry-safe staging/MERGE support."""
from __future__ import annotations

import os
import re
from collections.abc import Mapping
from typing import Any

_REQUIRED = (
    "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA",
)
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def snowflake_options(env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Build connector options, failing before any network operation if config is incomplete."""
    source = os.environ if env is None else env
    missing = [key for key in _REQUIRED if not source.get(key)]
    if missing:
        raise ValueError("Missing required Snowflake settings: " + ", ".join(missing))
    account = source["SNOWFLAKE_ACCOUNT"].strip()
    if not account or any(char.isspace() for char in account):
        raise ValueError("SNOWFLAKE_ACCOUNT must be a non-empty account identifier or URL")
    options = {
        "sfURL": account,
        "sfUser": source["SNOWFLAKE_USER"],
        "sfPassword": source["SNOWFLAKE_PASSWORD"],
        "sfWarehouse": source["SNOWFLAKE_WAREHOUSE"],
        "sfDatabase": source["SNOWFLAKE_DATABASE"],
        "sfSchema": source["SNOWFLAKE_SCHEMA"],
    }
    role = source.get("SNOWFLAKE_ROLE")
    if role:
        options["sfRole"] = role
    return options


def validate_table_identifier(table: str) -> tuple[str, ...]:
    """Accept one-, two-, or three-part unquoted SQL identifiers."""
    parts = tuple(part.strip() for part in table.split("."))
    identifier = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
    if not 1 <= len(parts) <= 3 or any(not identifier.fullmatch(part) for part in parts):
        raise ValueError("table must be a valid one-, two-, or three-part Snowflake identifier")
    return parts


def load_dataframe(
    dataframe: Any,
    table: str,
    *,
    mode: str = "append",
    options: Mapping[str, str] | None = None,
) -> None:
    """Write a Spark DataFrame to Snowflake.

    Append is not idempotent. Use stage-and-MERGE for retry-safe key-based ingestion.
    """
    validate_table_identifier(table)
    if mode not in {"append", "overwrite"}:
        raise ValueError("mode must be 'append' or 'overwrite'")
    connector_options = dict(options) if options is not None else snowflake_options()
    (dataframe.write.format("net.snowflake.spark.snowflake")
        .options(**connector_options)
        .option("dbtable", table)
        .mode(mode)
        .save())


def load_dataframe_via_stage(
    dataframe: Any,
    target_table: str,
    *,
    keys: tuple[str, ...],
    batch_id: str,
    options: Mapping[str, str] | None = None,
    stage_table: str | None = None,
) -> None:
    """Overwrite a run-scoped staging table and MERGE rows by business key.

    Serialize writers to a target; the caller provides a deterministic batch ID.
    """
    if not batch_id or not batch_id.strip():
        raise ValueError("batch_id must be non-empty")
    if not keys or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", key) for key in keys):
        raise ValueError("keys must contain valid column identifiers")
    target_parts = validate_table_identifier(target_table)
    stage_parts = validate_table_identifier(stage_table or f"{target_parts[-1]}_STAGE")
    if len(stage_parts) != len(target_parts):
        raise ValueError("stage_table must have the same qualification depth as target_table")
    if not hasattr(dataframe, "columns") or not hasattr(dataframe, "withColumn"):
        raise TypeError("dataframe must be a Spark DataFrame")
    missing_keys = sorted(set(keys).difference(dataframe.columns))
    if missing_keys:
        raise ValueError("DataFrame is missing merge key columns: " + ", ".join(missing_keys))

    from pyspark.sql.functions import lit

    source = dataframe if "SOURCE_BATCH_ID" in dataframe.columns else dataframe.withColumn(
        "SOURCE_BATCH_ID", lit(batch_id)
    )
    connector_options = dict(options) if options is not None else snowflake_options()
    (source.write.format("net.snowflake.spark.snowflake")
        .options(**connector_options)
        .option("dbtable", ".".join(stage_parts))
        .mode("overwrite")
        .save())

    try:
        import snowflake.connector
    except ImportError as exc:
        raise RuntimeError(
            "snowflake-connector-python is required to execute staged MERGE"
        ) from exc

    env = os.environ
    connection_args = {
        "account": env.get("SNOWFLAKE_ACCOUNT"),
        "user": env.get("SNOWFLAKE_USER"),
        "password": env.get("SNOWFLAKE_PASSWORD"),
        "warehouse": env.get("SNOWFLAKE_WAREHOUSE"),
        "database": env.get("SNOWFLAKE_DATABASE"),
        "schema": env.get("SNOWFLAKE_SCHEMA"),
    }
    if env.get("SNOWFLAKE_ROLE"):
        connection_args["role"] = env["SNOWFLAKE_ROLE"]
    if not all(connection_args.get(name) for name in ("account", "user", "password", "warehouse", "database", "schema")):
        raise ValueError("Snowflake connection environment is incomplete for MERGE execution")
    connection = snowflake.connector.connect(**connection_args)
    quote = lambda parts: ".".join(f'"{part.upper()}"' for part in parts)
    target_sql, stage_sql = quote(target_parts), quote(stage_parts)
    on_clause = " AND ".join(f't."{key.upper()}" = s."{key.upper()}"' for key in keys)
    cursor = connection.cursor()
    try:
        cursor.execute(
            f"MERGE INTO {target_sql} t USING {stage_sql} s ON {on_clause} "
            "WHEN MATCHED THEN UPDATE ALL BY NAME "
            "WHEN NOT MATCHED THEN INSERT ALL BY NAME"
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()
