"""Unit tests for SCD2 change staging."""
import pytest
from pyspark.sql import SparkSession

from ledgershift.pipelines.scd2 import stage_scd2


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]")
               .appName("ledgershift-scd2-tests")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    yield session
    session.stop()


def test_stage_scd2_expires_changed_and_inserts_changed_or_new(spark):
    current = spark.createDataFrame([
        (1, "Ada", "KE"),
        (2, "Kim", "UG"),
    ], "customer_id INT, name STRING, country STRING")
    incoming = spark.createDataFrame([
        (1, "Ada", "KE"),
        (2, "Kim", "KE"),
        (3, "Lee", "TZ"),
    ], "customer_id INT, name STRING, country STRING")

    expire, insert = stage_scd2(
        current, incoming, "customer_id", ["name", "country"], "2026-10-22T12:00:00Z"
    )
    assert [row.customer_id for row in expire.collect()] == [2]
    inserted = {row.customer_id: row for row in insert.collect()}
    assert set(inserted) == {2, 3}
    assert all(row.is_current for row in inserted.values())
    assert all(row.valid_to is None for row in inserted.values())
    assert all(row.valid_from is not None for row in inserted.values())


def test_stage_scd2_requires_tracked_attributes(spark):
    current = spark.createDataFrame([(1, "Ada")], "customer_id INT, name STRING")
    with pytest.raises(ValueError, match="tracked_columns"):
        stage_scd2(current, current, "customer_id", [], "2026-10-22T12:00:00Z")
