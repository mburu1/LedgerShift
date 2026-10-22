import pytest
from pyspark.sql import SparkSession

from ledgershift.validation.checks import (
    compare_aggregates,
    compare_column_checksums,
    compare_profiles,
    compare_referential_integrity,
    compare_row_counts,
    compare_sampled_rows,
    summarize,
)


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]")
               .appName("ledgershift-validation-tests")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    yield session
    session.stop()


def test_row_count_mismatch_fails():
    result = compare_row_counts("orders", 10, 9)
    assert result.status == "FAIL"


def test_aggregate_comparison_detects_missing_and_unexpected_metrics():
    results = compare_aggregates("orders", {"sum_amount": 100, "count": 4},
                                {"sum_amount": 100, "extra": 1})
    assert all(item.status == "FAIL" for item in results)
    assert {item.name for item in results} == {
        "aggregate:sum_amount", "aggregate:count", "aggregate:extra"
    }


def test_profiles_allow_numeric_tolerance_but_detect_drift():
    results = compare_profiles(
        "customers",
        {"email": {"null_count": 0, "distinct_count": 10}},
        {"email": {"null_count": 0, "distinct_count": 11}},
        tolerance=0.5,
    )
    assert [item.status for item in results] == ["PASS", "FAIL"]


def test_column_checksums_detect_tampered_values(spark):
    source = spark.createDataFrame([(1, "10.00"), (2, "20.00")],
                                   "order_id INT, amount STRING")
    target = spark.createDataFrame([(1, "10.00"), (2, "99.00")],
                                   "order_id INT, amount STRING")
    results = compare_column_checksums(source, target, "orders", ["amount"])
    assert any(result.status == "FAIL" for result in results)


def test_referential_integrity_detects_orphans(spark):
    orders = spark.createDataFrame([(1,), (2,)], "order_id INT")
    payments = spark.createDataFrame([(1,), (3,)], "order_id INT")
    result = compare_referential_integrity(payments, orders, "payments", "order_id", "order_id")
    assert result.status == "FAIL"
    assert result.actual == 1


def test_sampled_row_diff_detects_tampering(spark):
    source = spark.createDataFrame([(1, "paid")], "order_id INT, status STRING")
    target = spark.createDataFrame([(1, "refunded")], "order_id INT, status STRING")
    # One bucket ensures the single row is included.
    result = compare_sampled_rows(source, target, "orders", "order_id", ["status"], 1, 0)
    assert result.status == "FAIL"
    assert result.actual == 1


def test_empty_summary_fails_closed():
    assert summarize([])["verdict"] == "FAIL"
