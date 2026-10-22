"""Unit tests for deterministic CDC staging and event validation."""
import pytest
from pyspark.sql import SparkSession

from ledgershift.pipelines.cdc import latest_cdc_events


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]")
               .appName("ledgershift-cdc-tests")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    yield session
    session.stop()


def test_latest_cdc_event_wins_per_key(spark):
    frame = spark.createDataFrame([
        (1, "I", 10, "old"),
        (1, "U", 12, "new"),
        (2, "D", 11, "deleted"),
    ], "order_id INT, op STRING, lsn LONG, payload STRING")
    rows = {row.order_id: row for row in latest_cdc_events(frame, "order_id").collect()}
    assert rows[1].lsn == 12
    assert rows[1].payload == "new"
    assert rows[2].op == "D"


def test_cdc_rejects_unknown_operation(spark):
    frame = spark.createDataFrame([(1, "X", 1)], "order_id INT, op STRING, lsn LONG")
    with pytest.raises(ValueError, match="I, U or D"):
        latest_cdc_events(frame, "order_id")


def test_cdc_rejects_null_key_or_lsn(spark):
    frame = spark.createDataFrame([(None, "I", 1)], "order_id INT, op STRING, lsn LONG")
    with pytest.raises(ValueError, match="non-null"):
        latest_cdc_events(frame, "order_id")
