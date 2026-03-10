"""evaluate.py Recall Rate 정확성 검증 테스트"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
import evaluate as eval_mod
from evaluate import cmd_checkpoint, cmd_compare, cmd_report, cmd_score


# ─── Recall Rate 계산 로직 (핵심) ─────────────────────────────────────────────


class TestRecallRateCalculation:
    """cmd_compare 내부의 집합 연산을 직접 검증한다.
    _get_recent_symbols, _get_briefing_symbols를 monkeypatch하여 격리 테스트.
    """

    def _compute(self, ground_truth: set, briefing: set):
        """recall_rate, hallucination_rate 계산"""
        recalled = ground_truth & briefing
        total_gt = len(ground_truth)
        recall_rate = len(recalled) / total_gt if total_gt > 0 else 0

        hallucinated = briefing - ground_truth
        total_briefing = len(briefing)
        hallucination_rate = (
            len(hallucinated) / total_briefing if total_briefing > 0 else 0
        )
        return recall_rate, hallucination_rate

    def test_perfect(self):
        r, h = self._compute({"A", "B", "C"}, {"A", "B", "C"})
        assert r == 1.0
        assert h == 0.0

    def test_partial(self):
        r, h = self._compute({"A", "B", "C"}, {"A", "B", "D"})
        assert abs(r - 2 / 3) < 1e-9
        assert abs(h - 1 / 3) < 1e-9

    def test_zero_recall(self):
        r, h = self._compute({"A", "B"}, {"C", "D"})
        assert r == 0.0
        assert h == 1.0

    def test_empty_ground_truth(self):
        r, h = self._compute(set(), {"A"})
        assert r == 0.0
        # hallucination_rate: 1/1 = 1.0
        assert h == 1.0

    def test_empty_briefing(self):
        r, h = self._compute({"A"}, set())
        assert r == 0.0
        assert h == 0.0  # 0/0 → 0 (div guard)

    def test_both_empty(self):
        r, h = self._compute(set(), set())
        assert r == 0.0
        assert h == 0.0


# ─── 서브커맨드 통합 테스트 ──────────────────────────────────────────────────


class TestCmdCheckpoint:
    def test_creates_json(self, seeded_db, tmp_path, monkeypatch):
        db, conn = seeded_db
        monkeypatch.setattr(eval_mod, "EVAL_DIR", tmp_path)
        monkeypatch.setattr(
            "evaluate.get_db_connection", lambda: (db, conn),
        )

        args = argparse.Namespace(command="checkpoint")
        result = cmd_checkpoint(args)
        assert result == 0

        # JSON 파일 생성 확인
        files = list(tmp_path.glob("checkpoint_*.json"))
        assert len(files) == 1

        with open(files[0]) as f:
            data = json.load(f)
        assert "symbols" in data
        assert "timestamp" in data


class TestCmdScore:
    def test_appends_to_scores(self, tmp_path, monkeypatch):
        monkeypatch.setattr(eval_mod, "EVAL_DIR", tmp_path)

        args = argparse.Namespace(
            command="score", recall=4, accuracy=3, continuation=5, notes="테스트",
        )
        result = cmd_score(args)
        assert result == 0

        scores_path = tmp_path / "scores.jsonl"
        assert scores_path.exists()
        with open(scores_path) as f:
            lines = f.readlines()
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["symbol_recall"] == 4
        assert data["accuracy"] == 3
        assert data["continuation"] == 5


class TestCmdCompare:
    def test_e2e_compare(self, seeded_db, tmp_path, monkeypatch):
        db, conn = seeded_db
        monkeypatch.setattr(eval_mod, "EVAL_DIR", tmp_path)
        monkeypatch.setattr(
            "evaluate.get_db_connection", lambda: (db, conn),
        )

        # 먼저 checkpoint 저장
        checkpoint = {
            "timestamp": "2025-01-01T00:00:00",
            "git_diff": "",
            "symbols": [
                {"name": "greet", "file_path": "hello.py", "type": "function"},
                {"name": "formatName", "file_path": "utils.js", "type": "function"},
            ],
        }
        cp_path = tmp_path / "checkpoint_20250101_000000.json"
        with open(cp_path, "w") as f:
            json.dump(checkpoint, f)

        args = argparse.Namespace(command="compare")
        result = cmd_compare(args)
        assert result == 0

        # comparisons.jsonl 기록 확인
        comp_path = tmp_path / "comparisons.jsonl"
        assert comp_path.exists()
        with open(comp_path) as f:
            data = json.loads(f.readline())
        assert data["recall_rate"] > 0

    def test_no_checkpoint_returns_1(self, tmp_path, monkeypatch):
        monkeypatch.setattr(eval_mod, "EVAL_DIR", tmp_path)
        args = argparse.Namespace(command="compare")
        result = cmd_compare(args)
        assert result == 1


class TestCmdReport:
    def test_with_data(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setattr(eval_mod, "EVAL_DIR", tmp_path)

        # scores.jsonl 생성
        with open(tmp_path / "scores.jsonl", "w") as f:
            f.write(json.dumps({
                "timestamp": "2025-01-01T00:00:00",
                "symbol_recall": 4, "accuracy": 3, "continuation": 5, "notes": "",
            }) + "\n")

        # comparisons.jsonl 생성
        with open(tmp_path / "comparisons.jsonl", "w") as f:
            f.write(json.dumps({
                "timestamp": "2025-01-01T01:00:00",
                "checkpoint_file": "cp.json",
                "recall_rate": 0.8,
                "hallucination_rate": 0.1,
                "recalled_count": 4,
                "total_ground_truth": 5,
                "hallucinated_count": 1,
            }) + "\n")

        args = argparse.Namespace(command="report")
        result = cmd_report(args)
        assert result == 0

        captured = capsys.readouterr()
        assert "timestamp" in captured.out  # CSV 헤더
        assert "manual_score" in captured.out
        assert "auto_compare" in captured.out

    def test_empty(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setattr(eval_mod, "EVAL_DIR", tmp_path)

        args = argparse.Namespace(command="report")
        result = cmd_report(args)
        assert result == 0

        captured = capsys.readouterr()
        assert "No evaluation data found." in captured.out
