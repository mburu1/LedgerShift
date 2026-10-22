from ledgershift.validation.checks import (
    compare_aggregates,
    compare_profiles,
    compare_row_counts,
    summarize,
)


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


def test_empty_summary_fails_closed():
    assert summarize([])["verdict"] == "FAIL"
