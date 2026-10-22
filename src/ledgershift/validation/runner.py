"""Compare row counts between JSONL source and target files; fail closed."""
import argparse
import json
from pathlib import Path
from ledgershift.validation.checks import compare_row_counts, summarize

def count_lines(path: Path) -> int:
    files = sorted(path.glob("*.jsonl")) if path.is_dir() else [path]
    return sum(1 for file in files for line in file.open(encoding="utf-8") if line.strip())

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--report", type=Path, default=Path("docs/validation/report.json"))
    args = parser.parse_args()
    report = summarize([compare_row_counts("source_to_target", count_lines(args.source), count_lines(args.target))])
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    if report["verdict"] != "PASS":
        raise SystemExit(1)

if __name__ == "__main__":
    main()
