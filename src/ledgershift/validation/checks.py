"""Reusable deterministic validation checks."""
from collections.abc import Iterable
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
    def as_dict(self) -> dict:
        return asdict(self)

def compare_row_counts(table: str, expected: int, actual: int) -> CheckResult:
    passed = expected == actual
    return CheckResult("row_count", table, "PASS" if passed else "FAIL", expected, actual, "counts match" if passed else "row-count mismatch")

def summarize(results: Iterable[CheckResult]) -> dict:
    checks = list(results)
    return {"verdict": "PASS" if checks and all(r.status == "PASS" for r in checks) else "FAIL", "checks": [r.as_dict() for r in checks]}
