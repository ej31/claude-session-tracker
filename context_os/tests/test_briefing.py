"""context_briefing.py 품질 검증 테스트"""
from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from context_briefing import (
    _format_turns_section,
    _get_recent_turns,
    _get_session_symbols,
    format_briefing,
    main,
)


# ─── 포맷팅 단위 테스트 ──────────────────────────────────────────────────────


class TestFormatTurnsSection:
    def test_basic_format(self):
        turns = [
            {"id": "t1", "timestamp": "2025-01-01T10:00:00", "type": "response", "summary": "요약1"},
            {"id": "t2", "timestamp": "2025-01-01T11:00:00", "type": "edit", "summary": "요약2"},
            {"id": "t3", "timestamp": "2025-01-01T12:00:00", "type": "response", "summary": "요약3"},
        ]
        lines = _format_turns_section(turns)
        assert lines[0] == "## 최근 작업 내역"
        assert len(lines) == 4  # 헤더 + 3 항목

    def test_type_labels(self):
        turns = [
            {"id": "t1", "timestamp": "", "type": "response", "summary": "s1"},
            {"id": "t2", "timestamp": "", "type": "edit", "summary": "s2"},
        ]
        lines = _format_turns_section(turns)
        assert "[응답]" in lines[1]
        assert "[수정]" in lines[2]

    def test_max_5_turns(self):
        turns = [
            {"id": f"t{i}", "timestamp": "", "type": "response", "summary": f"s{i}"}
            for i in range(10)
        ]
        lines = _format_turns_section(turns)
        # 헤더(1) + 최대 5개 항목
        assert len(lines) == 6


# ─── 브리핑 품질 테스트 (seeded_db) ───────────────────────────────────────────


class TestBriefingContainsCorrectSymbols:
    def test_symbols_in_briefing(self, seeded_db):
        _, conn = seeded_db
        turns = _get_recent_turns(conn, "test-session")
        symbols = _get_session_symbols(conn, "test-session")
        briefing = format_briefing("test-session", turns, symbols, conn)

        assert "greet" in briefing
        assert "formatName" in briefing


class TestBriefingDependencyChain:
    def test_chain_in_briefing(self, seeded_db):
        _, conn = seeded_db
        turns = _get_recent_turns(conn, "test-session")
        symbols = _get_session_symbols(conn, "test-session")
        briefing = format_briefing("test-session", turns, symbols, conn)

        # greet → formatName CALLS 관계가 있으므로 호출 대상에 포함
        assert "호출 대상" in briefing


class TestBriefingNoData:
    def test_empty_session(self, seeded_db):
        _, conn = seeded_db
        briefing = format_briefing("nonexistent-session", [], [], conn)
        assert "맥락 데이터가 없습니다" in briefing


class TestBriefingFormatConsistency:
    def test_starts_with_header(self, seeded_db):
        _, conn = seeded_db
        turns = _get_recent_turns(conn, "test-session")
        symbols = _get_session_symbols(conn, "test-session")
        briefing = format_briefing("test-session", turns, symbols, conn)

        assert briefing.startswith("[Context OS 브리핑]")
        assert "**Branch**" in briefing

    def test_empty_session_still_has_header(self, seeded_db):
        _, conn = seeded_db
        briefing = format_briefing("empty", [], [], conn)
        assert briefing.startswith("[Context OS 브리핑]")


class TestBriefingSymbolCallsIncluded:
    def test_calls_section(self, seeded_db):
        _, conn = seeded_db
        turns = _get_recent_turns(conn, "test-session")
        symbols = _get_session_symbols(conn, "test-session")
        briefing = format_briefing("test-session", turns, symbols, conn)

        # greet에 CALLS 관계(→ formatName)가 존재하므로 호출 대상 섹션 포함
        assert "호출 대상" in briefing


class TestBriefingRecentCommitIncluded:
    def test_commit_line(self, seeded_db):
        _, conn = seeded_db
        turns = _get_recent_turns(conn, "test-session")
        symbols = _get_session_symbols(conn, "test-session")
        briefing = format_briefing("test-session", turns, symbols, conn)

        # greet은 abc123 커밋이 수정. 최근 수정 라인이 포함되어야 함
        assert "최근 수정" in briefing


class TestBriefingE2E:
    def test_main_stdout(self, seeded_db, monkeypatch, capsys):
        db, conn = seeded_db
        input_data = {"session_id": "test-session"}
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(input_data)))
        monkeypatch.setattr(
            "context_briefing.get_db_connection", lambda: (db, conn),
        )

        result = main()
        assert result == 0

        captured = capsys.readouterr()
        assert "[Context OS 브리핑]" in captured.out
        assert "greet" in captured.out
