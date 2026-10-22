"""Reusable deterministic validation checks."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, pmod, sum as spark_sum, xxhash64


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
        if metric not in expected or metric not in actual:
            reason = "unexpected metric" if metric not in expected else "missing metric"
            results.append(
                CheckResult("aggregate:" + metric, table, "FAIL", left, right, reason)
            )
        elif left != right:
            results.append(
                CheckResult("aggregate:" + metric, table, "FAIL", left, right, "values differ")
            )
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
            if metric not in expected[column] or metric not in actual[column]:
                passed = False
            elif isinstance(left, (int, float)) and isinstance(right, (int, float)):
                passed = abs(left - right) <= tolerance
            else:
                passed = left == right
            results.append(
                CheckResult(
                    "profile:" + column + ":" + metric,
                    table,
                    "PASS" if passed else "FAIL",
                    left,
                    right,
                    "within tolerance" if passed else "profile mismatch",
                )
            )
    return results


def compare_column_checksums(
    source: DataFrame, target: DataFrame, table: str, columns: Iterable[str]
) -> list[CheckResult]:
    """Compare null-aware row counts and stable xxhash64 sums for each requested column."""
    results: list[CheckResult] = []
    source_columns, target_columns = set(source.columns), set(target.columns)
    for name in sorted(set(columns)):
        if name not in source_columns or name not in target_columns:
            results.append(CheckResult(
                "column_checksum", table, "FAIL",
                name in source_columns, name in target_columns,
                "column missing from source or target",
            ))
            continue

        def profile(frame: DataFrame, column_name: str) -> dict[str, Any]:
            value = col(column_name).cast("string")
            row = frame.agg(
                count("*").alias("rows"),
                count(value).alias("non_null"),
                countDistinct(value).alias("distinct"),
                spark_sum(xxhash64(value).cast("decimal(38,0)")).alias("checksum"),
            ).first()
            return row.asDict()

        left, right = profile(source, name), profile(target, name)
        for metric in ("rows", "non_null", "distinct", "checksum"):
            results.append(_result(
                f"column:{name}:{metric}", table, left[metric], right[metric]
            ))
    return results


def compare_referential_integrity(
    child: DataFrame, parent: DataFrame, table: str, foreign_key: str, parent_key: str
) -> CheckResult:
    """Fail when non-null child foreign keys do not resolve to a parent key."""
    if foreign_key not in child.columns:
        raise ValueError(f"child table missing foreign key column: {foreign_key}")
    if parent_key not in parent.columns:
        raise ValueError(f"parent table missing key column: {parent_key}")
    parent_keys = parent.select(col(parent_key).alias("_parent_key")).dropDuplicates()
    orphans = (child.filter(col(foreign_key).isNotNull())
               .join(parent_keys, col(foreign_key) == col("_parent_key"), "left_anti")
               .count())
    return _result("referential_integrity", table, 0, orphans,
                   "all foreign keys resolve" if orphans == 0 else f"{orphans} orphan rows")


def compare_sampled_rows(
    source: DataFrame,
    target: DataFrame,
    table: str,
    key: str,
    columns: Iterable[str],
    buckets: int = 100,
    bucket: int = 0,
) -> CheckResult:
    """Compare a deterministic hash bucket of rows and report differing-key counts."""
    if buckets < 1 or not 0 <= bucket < buckets:
        raise ValueError("bucket must be within the positive bucket count")
    selected = sorted(set(columns).difference({key}))
    for label, frame in (("source", source), ("target", target)):
        missing = sorted({key, *selected}.difference(frame.columns))
        if missing:
            raise ValueError(f"{label} table missing columns: " + ", ".join(missing))
    left = source.filter(pmod(xxhash64(col(key)), buckets) == bucket).select(key, *selected)
    right = target.filter(pmod(xxhash64(col(key)), buckets) == bucket).select(key, *selected)
    l_alias, r_alias = left.alias("l"), right.alias("r")
    joined = l_alias.join(r_alias, col(f"l.{key}") == col(f"r.{key}"), "full_outer")
    missing_key = col(f"l.{key}").isNull() | col(f"r.{key}").isNull()
    changed = missing_key
    for name in selected:
        changed = changed | ~col(f"l.{name}").eqNullSafe(col(f"r.{name}"))
    mismatches = joined.filter(changed).count()
    return _result("sampled_row_diff", table, 0, mismatches,
                   "sampled rows match" if mismatches == 0 else f"{mismatches} sampled mismatches")


def summarize(results: Iterable[CheckResult]) -> dict[str, Any]:
    checks = list(results)
    return {
        "verdict": "PASS" if checks and all(r.status == "PASS" for r in checks) else "FAIL",
        "total": len(checks),
        "passed": sum(r.status == "PASS" for r in checks),
        "failed": sum(r.status != "PASS" for r in checks),
        "checks": [r.as_dict() for r in checks],
    }
