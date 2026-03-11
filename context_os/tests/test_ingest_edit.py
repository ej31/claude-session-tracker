"""kuzu_ingest_edit.py 단위/통합 테스트"""
from __future__ import annotations

import io
import json
import sys
from contextlib import nullcontext
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from kuzu_ingest_edit import (
    _find_affected_symbols,
    _resolve_relative_path,
    main,
)


class TestResolveRelativePath:
    def test_absolute_to_relative(self):
        result = _resolve_relative_path("/home/proj/src/main.py", "/home/proj")
        assert result == "src/main.py"

    def test_already_relative(self):
        result = _resolve_relative_path("src/main.py", "/home/proj")
        assert result == "src/main.py"

    def test_no_cwd(self):
        result = _resolve_relative_path("/abs/path/file.py", "")
        assert result == "file.py"

    def test_trailing_slash_handling(self):
        result = _resolve_relative_path("/home/proj/src/main.py", "/home/proj/")
        assert result == "src/main.py"


class TestFindAffectedSymbols:
    def test_existing_file(self, seeded_db):
        _, conn = seeded_db
        result = _find_affected_symbols(conn, "hello.py")
        names = {s["name"] for s in result}
        assert {"greet", "farewell", "Greeter"} <= names

    def test_no_match(self, seeded_db):
        _, conn = seeded_db
        result = _find_affected_symbols(conn, "nonexistent.py")
        assert result == []


class TestIngestEditE2E:
    def test_edit_tool_creates_touched_file_only_for_ambiguous_file(self, seeded_db, monkeypatch):
        db, conn = seeded_db
        input_data = {
            "tool_name": "Edit",
            "session_id": "test-session",
            "tool_input": {"file_path": "/tmp/repo/hello.py"},
            "cwd": "/tmp/repo",
        }
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(input_data)))
        monkeypatch.setattr("kuzu_ingest_edit.scope_lock", lambda cwd: nullcontext())
        monkeypatch.setattr("kuzu_ingest_edit.ensure_scope_current", lambda cwd, include_git_history=False: True)
        monkeypatch.setattr("kuzu_ingest_edit.get_db_connection", lambda **kwargs: (db, conn))
        monkeypatch.setattr("kuzu_ingest_edit.get_or_create_session", lambda conn, sid, cwd: sid)

        result = main()
        assert result == 0

        res = conn.execute(
            "MATCH (t:Turn {type: 'edit'})-[:TOUCHED_FILE]->(f:File) "
            "WHERE f.path = 'hello.py' RETURN count(*)"
        )
        assert res.get_next()[0] >= 1

        res = conn.execute(
            "MATCH (t:Turn {type: 'edit'})-[:MODIFIED_BY]->(s:Symbol) "
            "WHERE s.file_path = 'hello.py' RETURN count(*)"
        )
        assert res.get_next()[0] == 0

    def test_write_tool_keeps_file_fact(self, seeded_db, monkeypatch):
        db, conn = seeded_db
        input_data = {
            "tool_name": "Write",
            "session_id": "test-session",
            "tool_input": {"file_path": "/tmp/repo/hello.py"},
            "cwd": "/tmp/repo",
        }
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(input_data)))
        monkeypatch.setattr("kuzu_ingest_edit.scope_lock", lambda cwd: nullcontext())
        monkeypatch.setattr("kuzu_ingest_edit.ensure_scope_current", lambda cwd, include_git_history=False: True)
        monkeypatch.setattr("kuzu_ingest_edit.get_db_connection", lambda **kwargs: (db, conn))
        monkeypatch.setattr("kuzu_ingest_edit.get_or_create_session", lambda conn, sid, cwd: sid)

        result = main()
        assert result == 0

        res = conn.execute(
            "MATCH (t:Turn {type: 'edit'})-[:TOUCHED_FILE]->(f:File) "
            "WHERE f.path = 'hello.py' RETURN count(*)"
        )
        assert res.get_next()[0] >= 1

    def test_unique_symbol_file_creates_modified_by(self, seeded_db, monkeypatch):
        db, conn = seeded_db
        conn.execute(
            "MATCH (s:Symbol) WHERE s.file_path = 'hello.py' AND s.name <> 'greet' "
            "SET s.is_active = false"
        )

        input_data = {
            "tool_name": "Edit",
            "session_id": "test-session",
            "tool_input": {"file_path": "/tmp/repo/hello.py"},
            "cwd": "/tmp/repo",
        }
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(input_data)))
        monkeypatch.setattr("kuzu_ingest_edit.scope_lock", lambda cwd: nullcontext())
        monkeypatch.setattr("kuzu_ingest_edit.ensure_scope_current", lambda cwd, include_git_history=False: True)
        monkeypatch.setattr("kuzu_ingest_edit.get_db_connection", lambda **kwargs: (db, conn))
        monkeypatch.setattr("kuzu_ingest_edit.get_or_create_session", lambda conn, sid, cwd: sid)

        result = main()
        assert result == 0

        res = conn.execute(
            "MATCH (t:Turn {type: 'edit'})-[:MODIFIED_BY]->(s:Symbol) "
            "WHERE s.name = 'greet' RETURN count(*)"
        )
        assert res.get_next()[0] >= 1

    def test_non_edit_tool_early_return(self, seeded_db, monkeypatch):
        input_data = {
            "tool_name": "Read",
            "session_id": "test-session",
            "tool_input": {"file_path": "/tmp/repo/hello.py"},
            "cwd": "/tmp/repo",
        }
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(input_data)))
        result = main()
        assert result == 0
