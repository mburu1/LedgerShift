"""Render machine-readable and human-readable validation evidence."""
from __future__ import annotations

import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _git_sha() -> str:
    if value := os.getenv("GITHUB_SHA"):
        return value
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def write_report(
    summary: dict[str, Any],
    json_path: str | Path = "docs/validation/report.json",
    markdown_path: str | Path = "docs/validation/report.md",
    run_id: str | None = None,
    fault_reconciliation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Write a timestamped validation report; never imply approval automatically."""
    report = {
        "run_id": run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "git_sha": _git_sha(),
        **summary,
    }
    if fault_reconciliation is not None:
        report["fault_reconciliation"] = fault_reconciliation

    json_file, markdown_file = Path(json_path), Path(markdown_path)
    json_file.parent.mkdir(parents=True, exist_ok=True)
    markdown_file.parent.mkdir(parents=True, exist_ok=True)
    json_file.write_text(
        json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8"
    )

    lines = [
        "# LedgerShift validation report",
        "",
        f"- **Run ID:** {report['run_id']}",
        f"- **Timestamp (UTC):** {report['timestamp_utc']}",
        f"- **Git SHA:** {report['git_sha']}",
        f"- **Verdict:** {report['verdict']}",
        f"- **Checks:** {report['passed']} passed / {report['failed']} failed "
        f"({report['total']} total)",
        "",
        "| Check | Table | Status | Expected | Actual | Detail |",
        "|---|---|---|---|---|---|",
    ]
    for check in report.get("checks", []):
        values = [
            check.get(field, "")
            for field in ("name", "table", "status", "expected", "actual", "detail")
        ]
        escaped = [
            str(value).replace("|", "\\|").replace("\n", " ") for value in values
        ]
        lines.append("| " + " | ".join(escaped) + " |")
    if fault_reconciliation is not None:
        rendered_faults = json.dumps(fault_reconciliation, indent=2, default=str)
        lines.extend(["", "## Fault reconciliation", "", rendered_faults])
    lines.extend(
        [
            "",
            "## Sign-off",
            "",
            "- **Automated verdict:** " + report["verdict"],
            "- **Human approval:** pending reviewer sign-off",
            "",
        ]
    )
    markdown_file.write_text("\n".join(lines), encoding="utf-8")
    return report
