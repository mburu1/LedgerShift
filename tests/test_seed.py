import json

import pytest

from ledgershift.generator.seed import generate


def test_generate_writes_reproducible_snapshot(tmp_path):
    manifest = generate(tmp_path, customers=5, orders=20, seed=7)
    snapshot = tmp_path / "source/snapshot/orders.jsonl"
    rows = [json.loads(line) for line in snapshot.read_text().splitlines()]
    assert manifest["input_rows"] == 20
    assert len(rows) == manifest["output_rows"]
    assert (tmp_path / "source/faults_manifest.json").exists()


def test_generate_rejects_invalid_counts(tmp_path):
    with pytest.raises(ValueError):
        generate(tmp_path, customers=0, orders=1, seed=1)
