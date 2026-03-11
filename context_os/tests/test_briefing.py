"""context_briefing.py 품질 검증 테스트"""
from __future__ import annotations

import io
import json
import sys
from contextlib import nullcontext
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from context_briefing import (
    _format_turns_section,
    _get_recent_turns,
    _get_session_symbols,
    _get_touched_files,
    format_briefing,
    main,
)


class TestFormatTurnsSection:
    def test_basic_format(self):
        turns = [
            {"id": "t1", "timestamp": "2025-01-01T10:00:00", "type": "response", "summary": "요약1"},
            {"id": "t2", "timestamp": "2025-01-01T11:00:00", "type": "edit", "summary": "요약2"},
            {"id": "t3", "timestamp": "2025-01-01T12:00:00", "type": "response", "summary": "요약3"},
        ]
        lines = _format_turns_section(turns)
        assert lines[0] == "## 최근 작업 내역"
        assert len(lines) == 4


class TestBriefingSections:
    def test_symbols_and_files_in_briefing(self, seeded_db):
        _, conn = seeded_db
        turns = _get_recent_turns(conn, "test-session")
        files = _get_touched_files(conn, "test-session")
        symbols = _get_session_symbols(conn, "test-session")
        briefing = format_briefing(
            "test-session",
            turns,
            files,
            symbols,
            conn,
            scope_meta={"branch": "main", "worktree_root": "/tmp/repo"},
        )

        assert "formatName" in briefing
        assert "utils.js" in briefing
        assert "## 최근 작업 파일" in briefing

    def test_fail_closed_omits_symbol_section(self, seeded_db):
        _, conn = seeded_db
        turns = _get_recent_turns(conn, "test-session")
        files = _get_touched_files(conn, "test-session")
        briefing = format_briefing(
            "test-session",
            turns,
            files,
            [],
            conn,
            scope_meta={"branch": "main", "worktree_root": "/tmp/repo"},
            graph_verified=False,
        )
        assert "symbol/dependency 맥락 주입을 생략" in briefing
        assert "## 안전하게 확인된 심볼" not in briefing

    def test_inactive_symbol_is_filtered(self, seeded_db):
        _, conn = seeded_db
        conn.execute(
            "MATCH (s:Symbol {id: 'utils.js:formatName:2'}) SET s.is_active = false"
        )
        symbols = _get_session_symbols(conn, "test-session")
        names = {sym["name"] for sym in symbols}
        assert "formatName" not in names

    def test_empty_session(self, seeded_db):
        _, conn = seeded_db
        briefing = format_briefing("nonexistent-session", [], [], [], conn)
        assert "맥락 데이터가 없습니다" in briefing


class TestBriefingE2E:
    def test_main_stdout(self, seeded_db, monkeypatch, capsys):
        db, conn = seeded_db
        input_data = {"session_id": "test-session", "cwd": "/tmp/repo"}
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(input_data)))
        monkeypatch.setattr("context_briefing.scope_lock", lambda cwd: nullcontext())
        monkeypatch.setattr("context_briefing.ensure_scope_current", lambda cwd, include_git_history=True: True)
        monkeypatch.setattr("context_briefing.get_db_connection", lambda **kwargs: (db, conn))
        monkeypatch.setattr(
            "context_briefing.load_scope_meta",
            lambda cwd=None: {"branch": "main", "worktree_root": "/tmp/repo"},
        )

        result = main()
        assert result == 0

        captured = capsys.readouterr()
        assert "[Context OS 브리핑]" in captured.out
        assert "utils.js" in captured.out

    def test_scope_missing_prints_fail_closed_message(self, monkeypatch, capsys):
        input_data = {"session_id": "test-session", "cwd": "/tmp/repo"}
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(input_data)))
        monkeypatch.setattr("context_briefing.scope_lock", lambda cwd: nullcontext())
        monkeypatch.setattr("context_briefing.ensure_scope_current", lambda cwd, include_git_history=True: False)
        monkeypatch.setattr(
            "context_briefing.load_scope_meta",
            lambda cwd=None: {"branch": "main", "worktree_root": "/tmp/repo"},
        )
        monkeypatch.setattr("context_briefing.get_db_connection", lambda **kwargs: (_ for _ in ()).throw(FileNotFoundError()))

        result = main()
        assert result == 0

        captured = capsys.readouterr()
        assert "symbol/dependency 맥락 주입을 생략" in captured.out
