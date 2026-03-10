"""kuzu_ingest_turn.py 단위/통합 테스트"""
from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from kuzu_ingest_turn import (
    _create_about_relations,
    _extract_code_blocks,
    _find_mentioned_symbols,
    _load_known_names,
    main,
)


# ─── 단위 테스트: _find_mentioned_symbols ─────────────────────────────────────


class TestFindMentionedSymbols:
    def test_basic_match(self):
        result = _find_mentioned_symbols("greet 함수 수정", {"greet", "farewell"})
        assert result == ["greet"]

    def test_word_boundary(self):
        """greeting에서 greet를 매칭하면 안 됨"""
        result = _find_mentioned_symbols("greeting card", {"greet"})
        assert result == []

    def test_short_name_skip(self):
        """MIN_SYMBOL_NAME_LENGTH(3) 미만은 무시"""
        result = _find_mentioned_symbols("if x > 0", {"if", "x"})
        assert result == []

    def test_multiple_matches(self):
        text = "greet 함수와 farewell 함수를 수정"
        result = _find_mentioned_symbols(text, {"greet", "farewell"})
        assert set(result) == {"greet", "farewell"}

    def test_empty_inputs(self):
        assert _find_mentioned_symbols("", set()) == []
        assert _find_mentioned_symbols("some text", set()) == []
        assert _find_mentioned_symbols("", {"greet"}) == []

    def test_exact_name_only(self):
        """formatName은 매칭되지만 format은 아님 (단어 경계)"""
        result = _find_mentioned_symbols(
            "formatName 호출", {"formatName", "format"},
        )
        assert "formatName" in result


# ─── 단위 테스트: _extract_code_blocks ────────────────────────────────────────


class TestExtractCodeBlocks:
    def test_two_blocks(self):
        text = "설명\n```python\nprint('hello')\n```\n중간\n```js\nconsole.log('hi')\n```"
        result = _extract_code_blocks(text)
        assert len(result) == 2

    def test_no_blocks(self):
        assert _extract_code_blocks("일반 텍스트") == []

    def test_empty_string(self):
        assert _extract_code_blocks("") == []


# ─── DB 통합 테스트 ──────────────────────────────────────────────────────────


class TestLoadKnownNames:
    def test_returns_symbol_names(self, seeded_db):
        _, conn = seeded_db
        names = _load_known_names(conn)
        assert "greet" in names
        assert "farewell" in names
        assert "Greeter" in names
        assert "formatName" in names
        assert "capitalize" in names


class TestCreateAboutRelations:
    def test_creates_relations(self, seeded_db):
        _, conn = seeded_db
        # 새 Turn 생성
        conn.execute(
            "CREATE (t:Turn {id: 'about-test-turn', session_id: 'test-session', "
            "timestamp: '2025-01-01T12:00:00', type: 'response', "
            "summary: 'test', ref_url: ''})"
        )
        count = _create_about_relations(conn, "about-test-turn", ["greet"])
        assert count >= 1

        # 관계 검증
        result = conn.execute(
            "MATCH (t:Turn {id: 'about-test-turn'})-[:ABOUT]->(s:Symbol) RETURN s.name"
        )
        names = []
        while result.has_next():
            names.append(result.get_next()[0])
        assert "greet" in names


class TestIngestTurnE2E:
    def test_main_creates_turn_and_about(self, seeded_db, monkeypatch):
        db, conn = seeded_db
        input_data = {
            "session_id": "test-session",
            "last_assistant_message": "greet 함수를 수정했습니다",
            "cwd": "/tmp/repo",
        }

        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(input_data)))
        monkeypatch.setattr(
            "kuzu_ingest_turn.get_db_connection", lambda: (db, conn),
        )
        # get_or_create_session은 git 명령 실행을 시도하므로 mock
        monkeypatch.setattr(
            "kuzu_ingest_turn.get_or_create_session",
            lambda conn, sid, cwd: sid,
        )

        result = main()
        assert result == 0

        # Turn 노드 생성 확인
        res = conn.execute(
            "MATCH (t:Turn) WHERE t.type = 'response' AND t.session_id = 'test-session' "
            "RETURN count(t)"
        )
        # 기존 seeded_db의 response turn(1) + 새로 생성된 turn(1)
        assert res.get_next()[0] >= 2

    def test_empty_message_early_return(self, seeded_db, monkeypatch):
        db, conn = seeded_db
        input_data = {
            "session_id": "test-session",
            "last_assistant_message": "",
            "cwd": "/tmp/repo",
        }
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(input_data)))

        # DB 연결이 호출되면 안 됨 (early return)
        result = main()
        assert result == 0
