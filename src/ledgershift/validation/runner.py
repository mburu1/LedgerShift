"""Run fail-closed file/Delta or Delta-to-Snowflake reconciliation checks."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from ledgershift.validation.checks import (
    CheckResult, compare_aggregates, compare_column_checksums, compare_profiles,
    compare_row_counts, compare_sampled_rows, summarize,
)
from ledgershift.validation.report import write_report


def _read_file_source(spark, path: Path):
    if not path.exists():
        raise FileNotFoundError(f"validation input does not exist: {path}")
    if path.is_dir() and (path / "_delta_log").is_dir():
        return spark.read.format("delta").load(str(path))
    if path.is_file() and path.suffix.lower() == ".parquet":
        return spark.read.parquet(str(path))
    if path.is_dir():
        files = sorted(path.rglob("*.jsonl"))
        if files:
            return spark.read.json([str(item) for item in files])
        raise ValueError(f"directory has neither Delta metadata nor JSONL records: {path}")
    if path.is_file() and path.suffix.lower() == ".jsonl":
        return spark.read.json(str(path))
    raise ValueError(f"unsupported validation input: {path}")


def _metrics(frame) -> dict[str, object]:
    from pyspark.sql.functions import col, count, countDistinct, min as spark_min, max as spark_max, sum as spark_sum

    columns = set(frame.columns)
    row = frame.agg(count("*").alias("row_count")).first()
    result: dict[str, object] = {"row_count": row["row_count"]}
    key = next((name for name in ("order_id", "payment_id", "customer_id") if name in columns), None)
    if key:
        result[f"distinct_{key}"] = frame.agg(countDistinct(col(key)).alias("n")).first()["n"]
    amount = next((name for name in ("amount", "gross_revenue", "total_amount") if name in columns), None)
    if amount:
        result[f"sum_{amount}"] = frame.agg(
            spark_sum(col(amount).cast("decimal(38, 4)")).alias("n")
        ).first()["n"]
    date_column = next((name for name in ("order_date", "order_ts", "revenue_date") if name in columns), None)
    if date_column:
        stats = frame.agg(
            spark_min(col(date_column).cast("string")).alias("min_date"),
            spark_max(col(date_column).cast("string")).alias("max_date"),
        ).first()
        result[f"min_{date_column}"], result[f"max_{date_column}"] = stats["min_date"], stats["max_date"]
    return result


def _compare_dataframes(source, target, table: str, key: str | None, buckets: int):
    source_metrics, target_metrics = _metrics(source), _metrics(target)
    results = [compare_row_counts(table, source_metrics["row_count"], target_metrics["row_count"])]
    results.extend(compare_aggregates(table, source_metrics, target_metrics))
    shared = sorted(set(source.columns) & set(target.columns))
    if not shared:
        results.append(CheckResult("schema_overlap", table, "FAIL", "at least one shared column", 0,
                                   "source and target have no columns in common"))
        return results

    results.extend(compare_column_checksums(source, target, table, shared))
    source_profile, target_profile = {}, {}
    from pyspark.sql.functions import col, count, countDistinct
    for column in shared:
        def profile(frame):
            return frame.agg(
                count("*").alias("row_count"),
                count(col(column)).alias("non_null_count"),
                countDistinct(col(column)).alias("distinct_count"),
            ).first().asDict()
        source_profile[column], target_profile[column] = profile(source), profile(target)
    results.extend(compare_profiles(table, source_profile, target_profile))
    resolved_key = key or next((name for name in ("order_id", "payment_id", "customer_id") if name in shared), None)
    if resolved_key:
        if resolved_key not in source.columns or resolved_key not in target.columns:
            results.append(CheckResult("sampled_row_diff", table, "FAIL", resolved_key, None,
                                       "business key is absent from one side"))
        else:
            non_key = [name for name in shared if name != resolved_key]
            results.append(compare_sampled_rows(source, target, table, resolved_key, non_key, buckets, 0))
    else:
        results.append(CheckResult("sampled_row_diff", table, "FAIL", "business key", None,
                                   "No shared business key; pass --key"))
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True, help="Delta path, Parquet, or JSONL source")
    parser.add_argument("--target", type=Path, default=Path("."), help="File target path; ignored for Snowflake")
    parser.add_argument("--target-format", choices=("file", "snowflake"), default="file")
    parser.add_argument("--target-table", help="Snowflake table when --target-format=snowflake")
    parser.add_argument("--key", default=None, help="Business key for sampled row differences")
    parser.add_argument("--buckets", type=int, default=100)
    parser.add_argument("--report", type=Path, default=Path("docs/validation/report.json"))
    parser.add_argument("--markdown-report", type=Path, default=Path("docs/validation/report.md"))
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()
    if args.buckets < 1:
        parser.error("--buckets must be positive")
    if args.target_format == "snowflake" and not args.target_table:
        parser.error("--target-table is required with --target-format=snowflake")

    from ledgershift.spark import get_spark
    spark = get_spark("ledgershift-validation")
    try:
        source = _read_file_source(spark, args.source)
        if args.target_format == "snowflake":
            from ledgershift.validation.snowflake_adapter import read_snowflake_table
            target = read_snowflake_table(spark, args.target_table)
            target_name = args.target_table
        else:
            target = _read_file_source(spark, args.target)
            target_name = str(args.target)
        results = _compare_dataframes(source, target, target_name, args.key, args.buckets)
        report = write_report(summarize(results), args.report, args.markdown_report, args.run_id)
        print(json.dumps(report, indent=2, default=str))
        if report["verdict"] != "PASS":
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
