"""build_context_os.py 단위/통합 테스트"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from build_context_os import (
    extract_calls,
    extract_symbols,
    generate_mock_intent,
    get_dependency_chain,
    get_session_context,
    get_symbol_impact,
    init_db,
    overlay_git_history,
)
from tests.conftest import skip_no_ast_grep


# ─── 단위 테스트: generate_mock_intent ────────────────────────────────────────


class TestGenerateMockIntent:
    def test_python_docstring(self):
        code = 'def foo():\n    """도움말"""\n    pass'
        assert generate_mock_intent("foo", code) == "도움말"

    def test_no_docstring_snake_case(self):
        code = "def my_func(): pass"
        assert generate_mock_intent("my_func", code) == "my func"

    def test_jsdoc(self):
        code = "/** 설명 */\nfunction fn() {}"
        assert generate_mock_intent("fn", code) == "설명"

    def test_multiline_docstring_first_line(self):
        code = 'def bar():\n    """첫 줄\n    두 번째 줄\n    """\n    pass'
        assert generate_mock_intent("bar", code) == "첫 줄"

    def test_empty_code(self):
        assert generate_mock_intent("some_func", "") == "some func"


# ─── DB 쿼리 함수 테스트 (seeded_db) ─────────────────────────────────────────


class TestGetSymbolImpact:
    def test_existing_commit(self, seeded_db):
        _, conn = seeded_db
        result = get_symbol_impact(conn, "abc123")
        assert len(result) == 1
        assert result[0]["name"] == "greet"

    def test_no_match(self, seeded_db):
        _, conn = seeded_db
        result = get_symbol_impact(conn, "nonexistent")
        assert result == []


class TestGetDependencyChain:
    def test_with_deps(self, seeded_db):
        _, conn = seeded_db
        result = get_dependency_chain(conn, "Greeter")
        dep_names = {r["name"] for r in result}
        assert "greet" in dep_names
        assert "formatName" in dep_names

    def test_no_deps(self, seeded_db):
        _, conn = seeded_db
        result = get_dependency_chain(conn, "capitalize")
        assert result == []


class TestGetSessionContext:
    def test_existing_session(self, seeded_db):
        _, conn = seeded_db
        ctx = get_session_context(conn, "test-session")
        assert len(ctx["turns"]) == 2
        assert len(ctx["symbols"]) == 2

    def test_nonexistent_session(self, seeded_db):
        _, conn = seeded_db
        ctx = get_session_context(conn, "nonexistent")
        assert ctx["turns"] == []
        assert ctx["symbols"] == []


class TestInitDbIdempotent:
    def test_double_init(self, tmp_path):
        db_path = str(tmp_path / "idempotent_db")
        db1, conn1 = init_db(db_path)
        # 두 번째 호출도 에러 없이 완료되어야 함
        db2, conn2 = init_db(db_path)
        # 스키마가 여전히 동작하는지 검증
        conn2.execute("MATCH (s:Symbol) RETURN count(s)")


# ─── 통합 테스트: ast-grep 필요 ──────────────────────────────────────────────


@skip_no_ast_grep
class TestExtractSymbolsIntegration:
    def test_extract_from_tmp_repo(self, tmp_repo, tmp_db):
        _, conn = tmp_db
        repo_id = "test/integration"
        conn.execute(
            "CREATE (r:Repository {id: $id, url: '', local_path: $lp, name: 'repo'})",
            parameters={"id": repo_id, "lp": str(tmp_repo)},
        )
        known = extract_symbols(str(tmp_repo), conn, repo_id)
        names = {s["name"] for s in known.values()}
        assert "greet" in names
        assert "farewell" in names
        assert "Greeter" in names


@skip_no_ast_grep
class TestExtractCallsIntegration:
    def test_calls_relation(self, tmp_repo, tmp_db):
        _, conn = tmp_db
        repo_id = "test/calls"
        conn.execute(
            "CREATE (r:Repository {id: $id, url: '', local_path: $lp, name: 'repo'})",
            parameters={"id": repo_id, "lp": str(tmp_repo)},
        )
        known = extract_symbols(str(tmp_repo), conn, repo_id)
        extract_calls(str(tmp_repo), conn, known)

        # Greeter → greet CALLS 관계 존재 확인
        result = conn.execute(
            "MATCH (a:Symbol)-[:CALLS]->(b:Symbol) "
            "WHERE a.name = 'Greeter' OR a.name = 'say_hello' "
            "RETURN b.name"
        )
        callee_names = set()
        while result.has_next():
            callee_names.add(result.get_next()[0])
        assert "greet" in callee_names


@skip_no_ast_grep
class TestOverlayGitHistory:
    def test_commits_and_modifies(self, tmp_repo, tmp_db):
        _, conn = tmp_db
        repo_id = "test/git"
        conn.execute(
            "CREATE (r:Repository {id: $id, url: '', local_path: $lp, name: 'repo'})",
            parameters={"id": repo_id, "lp": str(tmp_repo)},
        )
        known = extract_symbols(str(tmp_repo), conn, repo_id)
        overlay_git_history(str(tmp_repo), conn, known, repo_id)

        # Commit 노드 생성 확인
        result = conn.execute("MATCH (c:Commit) RETURN count(c)")
        count = result.get_next()[0]
        assert count >= 2

        # MODIFIES 관계 존재 확인
        result = conn.execute("MATCH ()-[:MODIFIES]->() RETURN count(*)")
        mod_count = result.get_next()[0]
        assert mod_count > 0


@skip_no_ast_grep
class TestFullPipelineE2E:
    def test_full_pipeline(self, tmp_repo, tmp_path):
        """build_context_os 전체 파이프라인을 E2E로 실행"""
        db_path = str(tmp_path / "e2e_db")
        db, conn = init_db(db_path)

        repo_id = "test/e2e"
        conn.execute(
            "CREATE (r:Repository {id: $id, url: '', local_path: $lp, name: 'repo'})",
            parameters={"id": repo_id, "lp": str(tmp_repo)},
        )

        known = extract_symbols(str(tmp_repo), conn, repo_id)
        assert len(known) >= 3  # greet, farewell, Greeter 최소

        extract_calls(str(tmp_repo), conn, known)
        overlay_git_history(str(tmp_repo), conn, known, repo_id)

        # 노드 카운트 검증
        for table in ["File", "Symbol", "Commit"]:
            result = conn.execute(f"MATCH (n:{table}) RETURN count(n)")
            assert result.get_next()[0] > 0

        # 관계 카운트 검증
        for rel in ["HAS_FILE", "CONTAINS", "HAS_COMMIT"]:
            result = conn.execute(f"MATCH ()-[:{rel}]->() RETURN count(*)")
            assert result.get_next()[0] > 0
