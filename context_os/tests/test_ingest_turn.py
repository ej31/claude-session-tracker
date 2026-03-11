"""kuzu_ingest_turn.py 단위/통합 테스트"""
from __future__ import annotations

import io
import json
import sys
from contextlib import nullcontext
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from build_context_os import get_active_symbol_index
from kuzu_ingest_turn import (
    _create_about_relations,
    _extract_code_blocks,
    _find_mentioned_symbols,
    _load_known_names,
    _resolve_explicit_symbol_ids,
    main,
)


class TestFindMentionedSymbols:
    def test_function_like_match_only(self):
        result = _find_mentioned_symbols("greet() 호출", {"greet", "farewell"})
        assert result == ["greet"]

    def test_plain_text_is_ignored(self):
        result = _find_mentioned_symbols("greet 함수 수정", {"greet"})
        assert result == []


class TestExtractCodeBlocks:
    def test_two_blocks(self):
        text = "설명\n```python\nprint('hello')\n```\n중간\n```js\nconsole.log('hi')\n```"
        result = _extract_code_blocks(text)
        assert len(result) == 2

    def test_no_blocks(self):
        assert _extract_code_blocks("일반 텍스트") == []


class TestLoadKnownNames:
    def test_returns_active_symbol_names(self, seeded_db):
        _, conn = seeded_db
        names = _load_known_names(conn)
        assert {"greet", "farewell", "Greeter", "formatName", "capitalize"} <= names


class TestCreateAboutRelations:
    def test_creates_relations(self, seeded_db):
        _, conn = seeded_db
        conn.execute(
            "CREATE (t:Turn {id: 'about-test-turn', session_id: 'test-session', "
            "timestamp: '2025-01-01T12:00:00', type: 'response', summary: 'test', ref_url: ''})"
        )
        count = _create_about_relations(conn, "about-test-turn", ["hello.py:greet:1"])
        assert count == 1

        result = conn.execute(
            "MATCH (t:Turn {id: 'about-test-turn'})-[:ABOUT]->(s:Symbol) RETURN s.name"
        )
        assert result.get_next()[0] == "greet"


class TestResolveExplicitSymbolIds:
    def test_inline_code_resolves_unique_symbol(self, seeded_db):
        _, conn = seeded_db
        index = get_active_symbol_index(conn)
        symbol_ids = _resolve_explicit_symbol_ids("`greet` 를 봐", index)
        assert symbol_ids == ["hello.py:greet:1"]

    def test_ambiguous_name_is_skipped(self, seeded_db):
        _, conn = seeded_db
        conn.execute(
            "CREATE (s:Symbol {id: 'dup.py:greet:1', name: 'greet', type: 'function', "
            "file_path: 'dup.py', start_line: 1, end_line: 2, code: 'def greet(): pass', "
            "intent: '', is_active: true})"
        )
        index = get_active_symbol_index(conn)
        symbol_ids = _resolve_explicit_symbol_ids("greet()", index)
        assert symbol_ids == []


class TestIngestTurnE2E:
    def test_main_creates_turn_and_about_for_explicit_code(self, seeded_db, monkeypatch):
        db, conn = seeded_db
        input_data = {
            "session_id": "test-session",
            "last_assistant_message": "`greet` 함수를 수정했습니다",
            "cwd": "/tmp/repo",
        }

        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(input_data)))
        monkeypatch.setattr("kuzu_ingest_turn.scope_lock", lambda cwd: nullcontext())
        monkeypatch.setattr("kuzu_ingest_turn.ensure_scope_current", lambda cwd, include_git_history=False: True)
        monkeypatch.setattr("kuzu_ingest_turn.get_db_connection", lambda **kwargs: (db, conn))
        monkeypatch.setattr("kuzu_ingest_turn.get_or_create_session", lambda conn, sid, cwd: sid)

        result = main()
        assert result == 0

        res = conn.execute(
            "MATCH (t:Turn) WHERE t.type = 'response' AND t.session_id = 'test-session' "
            "RETURN count(t)"
        )
        assert res.get_next()[0] >= 2

        res = conn.execute(
            "MATCH (t:Turn {type: 'response'})-[:ABOUT]->(s:Symbol) "
            "WHERE s.name = 'greet' RETURN count(*)"
        )
        assert res.get_next()[0] >= 1

    def test_plain_text_does_not_create_about(self, seeded_db, monkeypatch):
        db, conn = seeded_db
        input_data = {
            "session_id": "test-session",
            "last_assistant_message": "greet 함수를 수정했습니다",
            "cwd": "/tmp/repo",
        }

        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(input_data)))
        monkeypatch.setattr("kuzu_ingest_turn.scope_lock", lambda cwd: nullcontext())
        monkeypatch.setattr("kuzu_ingest_turn.ensure_scope_current", lambda cwd, include_git_history=False: True)
        monkeypatch.setattr("kuzu_ingest_turn.get_db_connection", lambda **kwargs: (db, conn))
        monkeypatch.setattr("kuzu_ingest_turn.get_or_create_session", lambda conn, sid, cwd: sid)

        result = main()
        assert result == 0

        res = conn.execute(
            "MATCH (t:Turn {type: 'response'})-[:ABOUT]->(s:Symbol) "
            "WHERE t.summary = 'greet 함수를 수정했습니다' AND s.name = 'greet' "
            "RETURN count(*)"
        )
        assert res.get_next()[0] == 0

    def test_empty_message_early_return(self, seeded_db, monkeypatch):
        input_data = {
            "session_id": "test-session",
            "last_assistant_message": "",
            "cwd": "/tmp/repo",
        }
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(input_data)))
        result = main()
        assert result == 0
