"""Context OS 테스트 공통 fixtures

모든 테스트는 tmp_path 기반 임시 Kùzu DB로 격리된다.
"""
from __future__ import annotations

import subprocess
import sys
from datetime import datetime
from pathlib import Path

import kuzu
import pytest

# 소스 모듈 import 경로 설정
sys.path.insert(0, str(Path(__file__).parent.parent))
from build_context_os import SCHEMA_STATEMENTS, init_db


# ─── ast-grep 설치 여부 ──────────────────────────────────────────────────────

def _has_ast_grep() -> bool:
    """ast-grep(sg) CLI 설치 여부 확인"""
    try:
        result = subprocess.run(
            ["sg", "--version"], capture_output=True, timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


HAS_AST_GREP = _has_ast_grep()
skip_no_ast_grep = pytest.mark.skipif(
    not HAS_AST_GREP, reason="ast-grep(sg) 미설치",
)


# ─── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_db(tmp_path):
    """임시 디렉토리에 Kùzu DB + 스키마 초기화.
    반환: (db, conn)
    """
    db_path = str(tmp_path / "test_db")
    db, conn = init_db(db_path)
    return db, conn


@pytest.fixture
def tmp_repo(tmp_path):
    """tmp_path에 git init + 샘플 Python/JS 파일 + 2 커밋.
    반환: Path (레포 루트)
    """
    repo_dir = tmp_path / "sample_repo"
    repo_dir.mkdir()

    # git init
    subprocess.run(["git", "init"], cwd=repo_dir, capture_output=True, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"],
        cwd=repo_dir, capture_output=True, check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"],
        cwd=repo_dir, capture_output=True, check=True,
    )

    # hello.py - greet, farewell 함수 + Greeter 클래스 (greet 호출)
    hello_py = repo_dir / "hello.py"
    hello_py.write_text(
        'def greet(name):\n'
        '    """인사 함수"""\n'
        '    return f"Hello, {name}"\n'
        '\n'
        'def farewell(name):\n'
        '    """작별 함수"""\n'
        '    return f"Goodbye, {name}"\n'
        '\n'
        'class Greeter:\n'
        '    def say_hello(self):\n'
        '        return greet("world")\n',
    )

    subprocess.run(["git", "add", "."], cwd=repo_dir, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "-m", "feat: add hello.py"],
        cwd=repo_dir, capture_output=True, check=True,
    )

    # utils.js - formatName, capitalize 함수
    utils_js = repo_dir / "utils.js"
    utils_js.write_text(
        '/** 이름 포맷팅 */\n'
        'function formatName(n) {\n'
        '  return n.trim();\n'
        '}\n'
        '\n'
        'const capitalize = (s) => {\n'
        '  return s.charAt(0).toUpperCase() + s.slice(1);\n'
        '}\n',
    )

    subprocess.run(["git", "add", "."], cwd=repo_dir, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "-m", "feat: add utils.js"],
        cwd=repo_dir, capture_output=True, check=True,
    )

    return repo_dir


@pytest.fixture
def seeded_db(tmp_db):
    """tmp_db에 Repository/File/Symbol/Commit/Turn/관계를 미리 적재.
    반환: (db, conn)
    """
    db, conn = tmp_db

    # Repository
    conn.execute(
        "CREATE (r:Repository {id: 'test/repo', url: 'https://github.com/test/repo', "
        "local_path: '/tmp/repo', name: 'repo'})"
    )

    # Files
    conn.execute("CREATE (f:File {path: 'hello.py', language: 'python', size: 200})")
    conn.execute("CREATE (f:File {path: 'utils.js', language: 'javascript', size: 150})")

    # Repository → HAS_FILE
    conn.execute(
        "MATCH (r:Repository {id: 'test/repo'}), (f:File {path: 'hello.py'}) "
        "CREATE (r)-[:HAS_FILE]->(f)"
    )
    conn.execute(
        "MATCH (r:Repository {id: 'test/repo'}), (f:File {path: 'utils.js'}) "
        "CREATE (r)-[:HAS_FILE]->(f)"
    )

    # Symbols (5개)
    symbols = [
        ("hello.py:greet:1", "greet", "function", "hello.py", 1, 3,
         'def greet(name):\n    """인사 함수"""\n    return f"Hello, {name}"', "인사 함수"),
        ("hello.py:farewell:5", "farewell", "function", "hello.py", 5, 7,
         'def farewell(name):\n    """작별 함수"""\n    return f"Goodbye, {name}"', "작별 함수"),
        ("hello.py:Greeter:9", "Greeter", "class", "hello.py", 9, 11,
         'class Greeter:\n    def say_hello(self):\n        return greet("world")', ""),
        ("utils.js:formatName:2", "formatName", "function", "utils.js", 2, 4,
         'function formatName(n) {\n  return n.trim();\n}', "이름 포맷팅"),
        ("utils.js:capitalize:6", "capitalize", "function", "utils.js", 6, 8,
         'const capitalize = (s) => {\n  return s.charAt(0).toUpperCase() + s.slice(1);\n}', ""),
    ]
    for sid, name, stype, fp, sl, el, code, intent in symbols:
        conn.execute(
            "CREATE (s:Symbol {id: $id, name: $name, type: $type, "
            "file_path: $fp, start_line: $sl, end_line: $el, code: $code, intent: $intent})",
            parameters={
                "id": sid, "name": name, "type": stype,
                "fp": fp, "sl": sl, "el": el, "code": code, "intent": intent,
            },
        )

    # File → CONTAINS → Symbol
    for sid, _, _, fp, *_ in symbols:
        conn.execute(
            "MATCH (f:File {path: $fp}), (s:Symbol {id: $sid}) "
            "CREATE (f)-[:CONTAINS]->(s)",
            parameters={"fp": fp, "sid": sid},
        )

    # CALLS: Greeter → greet, greet → formatName
    conn.execute(
        "MATCH (a:Symbol {id: 'hello.py:Greeter:9'}), (b:Symbol {id: 'hello.py:greet:1'}) "
        "CREATE (a)-[:CALLS]->(b)"
    )
    conn.execute(
        "MATCH (a:Symbol {id: 'hello.py:greet:1'}), (b:Symbol {id: 'utils.js:formatName:2'}) "
        "CREATE (a)-[:CALLS]->(b)"
    )

    # Commits
    conn.execute(
        "CREATE (c:Commit {hash: 'abc123', message: 'feat: add hello.py', "
        "author: 'Test', date: '2025-01-01T00:00:00'})"
    )
    conn.execute(
        "CREATE (c:Commit {hash: 'def456', message: 'feat: add utils.js', "
        "author: 'Test', date: '2025-01-02T00:00:00'})"
    )

    # Repository → HAS_COMMIT
    conn.execute(
        "MATCH (r:Repository {id: 'test/repo'}), (c:Commit {hash: 'abc123'}) "
        "CREATE (r)-[:HAS_COMMIT]->(c)"
    )
    conn.execute(
        "MATCH (r:Repository {id: 'test/repo'}), (c:Commit {hash: 'def456'}) "
        "CREATE (r)-[:HAS_COMMIT]->(c)"
    )

    # MODIFIES: abc123 → greet, def456 → formatName
    conn.execute(
        "MATCH (c:Commit {hash: 'abc123'}), (s:Symbol {id: 'hello.py:greet:1'}) "
        "CREATE (c)-[:MODIFIES {changed_lines: '1,2,3'}]->(s)"
    )
    conn.execute(
        "MATCH (c:Commit {hash: 'def456'}), (s:Symbol {id: 'utils.js:formatName:2'}) "
        "CREATE (c)-[:MODIFIES {changed_lines: '2,3,4'}]->(s)"
    )

    # Session
    conn.execute(
        "CREATE (s:Session {id: 'test-session', started_at: '2025-01-01T00:00:00', "
        "cwd: '/tmp/repo', branch: 'main'})"
    )

    # Session → IN_REPO
    conn.execute(
        "MATCH (s:Session {id: 'test-session'}), (r:Repository {id: 'test/repo'}) "
        "CREATE (s)-[:IN_REPO]->(r)"
    )

    # Turns
    ts1 = "2025-01-01T10:00:00"
    ts2 = "2025-01-01T11:00:00"
    conn.execute(
        "CREATE (t:Turn {id: $id, session_id: 'test-session', "
        "timestamp: $ts, type: 'response', summary: 'greet 함수 설명', ref_url: ''})",
        parameters={"id": f"test-session:{ts1}", "ts": ts1},
    )
    conn.execute(
        "CREATE (t:Turn {id: $id, session_id: 'test-session', "
        "timestamp: $ts, type: 'edit', summary: 'Edit: utils.js', ref_url: ''})",
        parameters={"id": f"test-session:{ts2}", "ts": ts2},
    )

    # Session → HAS_TURN
    conn.execute(
        "MATCH (s:Session {id: 'test-session'}), (t:Turn {id: $tid}) "
        "CREATE (s)-[:HAS_TURN]->(t)",
        parameters={"tid": f"test-session:{ts1}"},
    )
    conn.execute(
        "MATCH (s:Session {id: 'test-session'}), (t:Turn {id: $tid}) "
        "CREATE (s)-[:HAS_TURN]->(t)",
        parameters={"tid": f"test-session:{ts2}"},
    )

    # ABOUT: response turn → greet
    conn.execute(
        "MATCH (t:Turn {id: $tid}), (s:Symbol {id: 'hello.py:greet:1'}) "
        "CREATE (t)-[:ABOUT]->(s)",
        parameters={"tid": f"test-session:{ts1}"},
    )

    # MODIFIED_BY: edit turn → formatName
    conn.execute(
        "MATCH (t:Turn {id: $tid}), (s:Symbol {id: 'utils.js:formatName:2'}) "
        "CREATE (t)-[:MODIFIED_BY]->(s)",
        parameters={"tid": f"test-session:{ts2}"},
    )

    return db, conn
