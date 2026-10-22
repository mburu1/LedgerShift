"""Compare source and target JSONL row counts and fail closed."""
import argparse
import json
from pathlib import Path

from ledgershift.validation.checks import compare_row_counts, summarize
from ledgershift.validation.report import write_report


def count_lines(path: Path) -> int:
    """Count non-empty JSONL records in a file or a directory tree."""
    if path.is_file():
        files = [path]
    elif path.is_dir():
        files = sorted(path.rglob("*.jsonl"))
    else:
        raise FileNotFoundError(f"validation input does not exist: {path}")
    return sum(1 for file in files for line in file.open(encoding="utf-8") if line.strip())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--report", type=Path, default=Path("docs/validation/report.json"))
    parser.add_argument("--markdown-report", type=Path,
                        default=Path("docs/validation/report.md"))
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    summary = summarize([
        compare_row_counts("source_to_target", count_lines(args.source), count_lines(args.target))
    ])
    report = write_report(summary, args.report, args.markdown_report, args.run_id)
    print(json.dumps(report, indent=2, default=str))
    if report["verdict"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
