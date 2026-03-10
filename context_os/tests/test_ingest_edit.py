"""kuzu_ingest_edit.py 단위/통합 테스트"""
from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from kuzu_ingest_edit import (
    _find_affected_symbols,
    _resolve_relative_path,
    main,
)


# ─── 단위 테스트: _resolve_relative_path ──────────────────────────────────────


class TestResolveRelativePath:
    def test_absolute_to_relative(self):
        result = _resolve_relative_path("/home/proj/src/main.py", "/home/proj")
        assert result == "src/main.py"

    def test_already_relative(self):
        result = _resolve_relative_path("src/main.py", "/home/proj")
        # cwd로 시작하지 않으므로 fallback → 파일명만 반환
        assert result == "main.py"

    def test_no_cwd(self):
        result = _resolve_relative_path("/abs/path/file.py", "")
        assert result == "file.py"

    def test_trailing_slash_handling(self):
        result = _resolve_relative_path("/home/proj/src/main.py", "/home/proj/")
        # cwd가 /로 끝나더라도 startswith로 매칭
        assert "main.py" in result


# ─── DB 통합 테스트 ──────────────────────────────────────────────────────────


class TestFindAffectedSymbols:
    def test_existing_file(self, seeded_db):
        _, conn = seeded_db
        result = _find_affected_symbols(conn, "hello.py")
        names = {s["name"] for s in result}
        assert "greet" in names
        assert "farewell" in names
        assert "Greeter" in names

    def test_no_match(self, seeded_db):
        _, conn = seeded_db
        result = _find_affected_symbols(conn, "nonexistent.py")
        assert result == []


class TestIngestEditE2E:
    def test_edit_tool(self, seeded_db, monkeypatch):
        db, conn = seeded_db
        input_data = {
            "tool_name": "Edit",
            "session_id": "test-session",
            "tool_input": {"file_path": "/tmp/repo/hello.py"},
            "cwd": "/tmp/repo",
        }
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(input_data)))
        monkeypatch.setattr(
            "kuzu_ingest_edit.get_db_connection", lambda: (db, conn),
        )
        monkeypatch.setattr(
            "kuzu_ingest_edit.get_or_create_session",
            lambda conn, sid, cwd: sid,
        )

        result = main()
        assert result == 0

        # MODIFIED_BY 관계 확인 (edit turn → hello.py 심볼)
        res = conn.execute(
            "MATCH (t:Turn {type: 'edit'})-[:MODIFIED_BY]->(s:Symbol) "
            "WHERE s.file_path = 'hello.py' "
            "RETURN count(*)"
        )
        assert res.get_next()[0] >= 1

    def test_non_edit_tool_early_return(self, seeded_db, monkeypatch):
        db, conn = seeded_db
        input_data = {
            "tool_name": "Read",
            "session_id": "test-session",
            "tool_input": {"file_path": "/tmp/repo/hello.py"},
            "cwd": "/tmp/repo",
        }
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(input_data)))

        result = main()
        assert result == 0

    def test_write_tool(self, seeded_db, monkeypatch):
        db, conn = seeded_db
        input_data = {
            "tool_name": "Write",
            "session_id": "test-session",
            "tool_input": {"file_path": "/tmp/repo/hello.py"},
            "cwd": "/tmp/repo",
        }
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(input_data)))
        monkeypatch.setattr(
            "kuzu_ingest_edit.get_db_connection", lambda: (db, conn),
        )
        monkeypatch.setattr(
            "kuzu_ingest_edit.get_or_create_session",
            lambda conn, sid, cwd: sid,
        )

        result = main()
        assert result == 0

        # Write도 Edit과 동일하게 MODIFIED_BY 관계 생성
        res = conn.execute(
            "MATCH (t:Turn {type: 'edit'})-[:MODIFIED_BY]->(s:Symbol) "
            "RETURN count(*)"
        )
        assert res.get_next()[0] >= 1
