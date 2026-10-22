from pathlib import Path
import pytest
from ledgershift.validation.runner import _read_file_source


def test_read_source_fails_for_missing_path(tmp_path: Path):
    with pytest.raises(FileNotFoundError, match="does not exist"):
        _read_file_source(None, tmp_path / "missing")
