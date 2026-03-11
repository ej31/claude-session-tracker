"""build_context_os.py 단위/통합 테스트"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from build_context_os import (
    build_context_os,
    extract_calls,
    extract_symbols,
    generate_mock_intent,
    get_dependency_chain,
    get_scope_db_path,
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


@skip_no_ast_grep
class TestScopedSafety:
    def test_rebuild_marks_removed_symbol_inactive(self, tmp_repo):
        build_context_os(str(tmp_repo), include_git_history=False)
        db_path = get_scope_db_path(str(tmp_repo))

        hello_py = tmp_repo / "hello.py"
        hello_py.write_text(
            'def greet(name):\n'
            '    return f"Hello, {name}"\n'
            '\n'
            'class Greeter:\n'
            '    def say_hello(self):\n'
            '        return greet("world")\n',
        )

        build_context_os(str(tmp_repo), include_git_history=False)
        _, conn = init_db(db_path)

        result = conn.execute(
            "MATCH (s:Symbol {id: 'hello.py:farewell:5'}) RETURN s.is_active"
        )
        assert result.has_next()
        assert result.get_next()[0] is False

    def test_worktree_scopes_do_not_mix_symbols(self, tmp_repo, tmp_path):
        subprocess.run(
            ["git", "branch", "feature-worktree"],
            cwd=tmp_repo,
            capture_output=True,
            check=True,
        )
        feature_dir = tmp_path / "feature-worktree"
        subprocess.run(
            ["git", "worktree", "add", str(feature_dir), "feature-worktree"],
            cwd=tmp_repo,
            capture_output=True,
            check=True,
        )

        feature_hello = feature_dir / "hello.py"
        feature_hello.write_text(
            'def feature_only(name):\n'
            '    return f"Feature, {name}"\n'
            '\n'
            'class Greeter:\n'
            '    def say_hello(self):\n'
            '        return feature_only("world")\n',
        )

        build_context_os(str(tmp_repo), include_git_history=False)
        build_context_os(str(feature_dir), include_git_history=False)

        base_db_path = get_scope_db_path(str(tmp_repo))
        feature_db_path = get_scope_db_path(str(feature_dir))
        assert base_db_path != feature_db_path

        _, base_conn = init_db(base_db_path)
        _, feature_conn = init_db(feature_db_path)

        base_result = base_conn.execute(
            "MATCH (s:Symbol) WHERE s.is_active = true RETURN DISTINCT s.name"
        )
        base_names = set()
        while base_result.has_next():
            base_names.add(base_result.get_next()[0])

        feature_result = feature_conn.execute(
            "MATCH (s:Symbol) WHERE s.is_active = true RETURN DISTINCT s.name"
        )
        feature_names = set()
        while feature_result.has_next():
            feature_names.add(feature_result.get_next()[0])

        assert "greet" in base_names
        assert "feature_only" not in base_names
        assert "feature_only" in feature_names
        assert "greet" not in feature_names
