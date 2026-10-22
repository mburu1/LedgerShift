"""Reusable deterministic validation checks."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class CheckResult:
    name: str
    table: str
    status: str
    expected: Any
    actual: Any
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _result(name: str, table: str, expected: Any, actual: Any, detail: str = "") -> CheckResult:
    passed = expected == actual
    return CheckResult(name, table, "PASS" if passed else "FAIL", expected, actual,
                       detail or ("values match" if passed else "values differ"))


def compare_row_counts(table: str, expected: int, actual: int) -> CheckResult:
    return _result("row_count", table, expected, actual,
                   "counts match" if expected == actual else "row-count mismatch")


def compare_aggregates(table: str, expected: Mapping[str, Any],
                       actual: Mapping[str, Any]) -> list[CheckResult]:
    """Compare named aggregates and report missing or unexpected metrics."""
    results = []
    for metric in sorted(set(expected) | set(actual)):
        left, right = expected.get(metric), actual.get(metric)
        if metric not in expected:
            results.append(CheckResult("aggregate:" + metric, table, "FAIL", None, right,
                                       "unexpected metric"))
        elif metric not in actual:
            results.append(CheckResult("aggregate:" + metric, table, "FAIL", left, None,
                                       "missing metric"))
        else:
            results.append(_result("aggregate:" + metric, table, left, right))
    return results


def compare_profiles(table: str, expected: Mapping[str, Mapping[str, Any]],
                     actual: Mapping[str, Mapping[str, Any]],
                     tolerance: float = 0.0) -> list[CheckResult]:
    """Compare per-column profiles; numeric values honor the configured tolerance."""
    if tolerance < 0:
        raise ValueError("tolerance cannot be negative")
    results: list[CheckResult] = []
    for column in sorted(set(expected) | set(actual)):
        if column not in expected or column not in actual:
            results.append(CheckResult("column_profile", table, "FAIL",
                                       expected.get(column), actual.get(column),
                                       "column profile missing"))
            continue
        for metric in sorted(set(expected[column]) | set(actual[column])):
            left, right = expected[column].get(metric), actual[column].get(metric)
            if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                passed = abs(left - right) <= tolerance
            else:
                passed = left == right
            results.append(CheckResult(
                "profile:" + column + ":" + metric, table, "PASS" if passed else "FAIL",
                left, right, "within tolerance" if passed else "profile mismatch"))
    return results


def summarize(results: Iterable[CheckResult]) -> dict[str, Any]:
    checks = list(results)
    return {
        "verdict": "PASS" if checks and all(r.status == "PASS" for r in checks) else "FAIL",
        "total": len(checks),
        "passed": sum(r.status == "PASS" for r in checks),
        "failed": sum(r.status != "PASS" for r in checks),
        "checks": [r.as_dict() for r in checks],
    }
