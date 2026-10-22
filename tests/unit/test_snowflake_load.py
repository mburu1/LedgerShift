from __future__ import annotations

import pytest

from ledgershift.snowflake.load import load_dataframe, snowflake_options, validate_table_identifier


def valid_env() -> dict[str, str]:
    return {
        "SNOWFLAKE_ACCOUNT": "example.snowflakecomputing.com",
        "SNOWFLAKE_USER": "loader",
        "SNOWFLAKE_PASSWORD": "not-a-real-password",
        "SNOWFLAKE_WAREHOUSE": "LOAD_WH",
        "SNOWFLAKE_DATABASE": "LEDGERSHIFT",
        "SNOWFLAKE_SCHEMA": "MART",
    }


def test_snowflake_options_maps_required_values() -> None:
    options = snowflake_options(valid_env())
    assert options["sfURL"] == "example.snowflakecomputing.com"
    assert options["sfDatabase"] == "LEDGERSHIFT"
    assert options["sfSchema"] == "MART"
    assert options["sfPassword"] == "not-a-real-password"
    assert "sfRole" not in options


def test_snowflake_options_reports_missing_keys() -> None:
    with pytest.raises(ValueError, match="SNOWFLAKE_PASSWORD"):
        snowflake_options({"SNOWFLAKE_ACCOUNT": "example.snowflakecomputing.com"})


@pytest.mark.parametrize("table", ["", "MART.BAD-NAME", "MART;DROP TABLE X", "A..B", "A.B.C.D"])
def test_load_rejects_invalid_table_names_before_touching_dataframe(table: str) -> None:
    with pytest.raises(ValueError):
        load_dataframe(object(), table, options={})


@pytest.mark.parametrize("table", ["FACT_ORDERS", "MART.FACT_ORDERS", "DB.MART.FACT_ORDERS"])
def test_table_identifier_accepts_valid_parts(table: str) -> None:
    assert len(validate_table_identifier(table)) in (1, 2, 3)


def test_load_rejects_unsupported_mode_before_touching_dataframe() -> None:
    with pytest.raises(ValueError, match="mode"):
        load_dataframe(object(), "MART.FACT_ORDERS", mode="ignore", options={})
