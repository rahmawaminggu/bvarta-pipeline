"""Unit tests for the --date incremental filter in Bronze."""

import os
import pytest

from pipeline.bronze import _resolve_input_paths


class TestResolvePaths:
    def test_no_filter_returns_all_jsonl(self, tmp_path):
        (tmp_path / "day_2025-01-01.jsonl").write_text("")
        (tmp_path / "day_2025-01-02.jsonl").write_text("")
        (tmp_path / "other.txt").write_text("")
        paths = _resolve_input_paths(str(tmp_path), None)
        assert len(paths) == 2
        assert all(p.endswith(".jsonl") for p in paths)

    def test_date_filter_returns_single_file(self, tmp_path):
        (tmp_path / "day_2025-01-01.jsonl").write_text("")
        (tmp_path / "day_2025-01-02.jsonl").write_text("")
        paths = _resolve_input_paths(str(tmp_path), "2025-01-01")
        assert len(paths) == 1
        assert paths[0].endswith("day_2025-01-01.jsonl")

    def test_date_filter_missing_file_raises(self, tmp_path):
        (tmp_path / "day_2025-01-01.jsonl").write_text("")
        with pytest.raises(FileNotFoundError, match="2025-01-03"):
            _resolve_input_paths(str(tmp_path), "2025-01-03")

    def test_no_jsonl_files_raises(self, tmp_path):
        (tmp_path / "readme.txt").write_text("")
        with pytest.raises(FileNotFoundError):
            _resolve_input_paths(str(tmp_path), None)

    def test_paths_are_sorted(self, tmp_path):
        (tmp_path / "day_2025-01-03.jsonl").write_text("")
        (tmp_path / "day_2025-01-01.jsonl").write_text("")
        (tmp_path / "day_2025-01-02.jsonl").write_text("")
        paths = _resolve_input_paths(str(tmp_path), None)
        basenames = [os.path.basename(p) for p in paths]
        assert basenames == sorted(basenames)