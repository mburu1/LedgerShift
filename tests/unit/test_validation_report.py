"""Tests for durable validation report artifacts."""
import json

from ledgershift.validation.checks import compare_row_counts, summarize
from ledgershift.validation.report import write_report


def test_report_writes_json_and_markdown_with_pending_human_signoff(tmp_path):
    summary = summarize([compare_row_counts("orders", 3, 3)])
    json_path = tmp_path / "report.json"
    markdown_path = tmp_path / "report.md"

    report = write_report(summary, json_path, markdown_path, run_id="test-run")

    saved = json.loads(json_path.read_text(encoding="utf-8"))
    markdown = markdown_path.read_text(encoding="utf-8")
    assert report["verdict"] == "PASS"
    assert saved["run_id"] == "test-run"
    assert saved["git_sha"]
    assert "Human approval:** pending reviewer sign-off" in markdown
    assert "| row_count | orders | PASS | 3 | 3 | counts match |" in markdown


def test_report_preserves_failed_verdict(tmp_path):
    summary = summarize([compare_row_counts("orders", 3, 2)])
    report = write_report(summary, tmp_path / "report.json", tmp_path / "report.md")
    assert report["verdict"] == "FAIL"
    assert report["failed"] == 1
