#!/usr/bin/env python3
"""Context OS - 코드 구조 + Git 히스토리 그래프 DB 구축 파이프라인

타겟 레포를 클론/지정하고, ast-grep + GitPython으로 코드 구조와
Git 히스토리를 Kùzu 그래프 DB에 적재한다.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

try:
    import kuzu
    from git import Repo
except ImportError as e:
    print(
        f"필수 패키지 미설치: {e}\npip install -r requirements.txt",
        file=sys.stderr,
    )
    sys.exit(1)

# ─── 상수 ─────────────────────────────────────────────────────────────────────

LOG_FILE = Path("~/.claude/hooks/hooks.log").expanduser()
DEFAULT_DB_DIR = Path("~/.claude/context_os/db").expanduser()
MAX_CODE_LENGTH = 5000
MAX_COMMITS = 50

LANGUAGE_MAP = {
    ".py": "python",
    ".js": "javascript",
    ".mjs": "javascript",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".jsx": "javascript",
}

SKIP_DIRS = {
    ".git", "node_modules", "__pycache__", ".venv", "venv",
    "dist", "build", ".tox", ".eggs", ".mypy_cache",
}

# ast-grep 심볼 추출 패턴 (언어별)
SYMBOL_PATTERNS = {
    "python": [
        ("def $NAME($$$)", "function"),
        ("class $NAME($$$)", "class"),
        ("class $NAME", "class"),
    ],
    "javascript": [
        ("function $NAME($$$) { $$$ }", "function"),
        ("const $NAME = ($$$) => { $$$ }", "function"),
        ("const $NAME = ($$$) => $$$", "function"),
        ("class $NAME { $$$ }", "class"),
    ],
    "typescript": [
        ("function $NAME($$$) { $$$ }", "function"),
        ("const $NAME = ($$$) => { $$$ }", "function"),
        ("const $NAME = ($$$) => $$$", "function"),
        ("class $NAME { $$$ }", "class"),
    ],
}

# 함수 호출 패턴 (CALLS 관계 추출용)
CALL_PATTERNS = {
    "python": "$FUNC($$$)",
    "javascript": "$FUNC($$$)",
    "typescript": "$FUNC($$$)",
}

# CALLS 관계에서 무시할 내장 함수/키워드
BUILTIN_SKIP = {
    "print", "len", "str", "int", "float", "list", "dict", "set", "tuple",
    "range", "enumerate", "zip", "map", "filter", "sorted", "reversed",
    "isinstance", "issubclass", "hasattr", "getattr", "setattr", "delattr",
    "super", "type", "id", "hash", "repr", "bool", "bytes", "open",
    "console", "require", "import", "exports", "module",
    "parseInt", "parseFloat", "setTimeout", "setInterval",
    "JSON", "Math", "Object", "Array", "String", "Number", "Boolean",
    "Promise", "Error", "TypeError", "ReferenceError",
}


# ─── 로거 ─────────────────────────────────────────────────────────────────────

def setup_logger(name: str) -> logging.Logger:
    """stderr + 파일 동시 출력 로거 생성"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(logging.Formatter(f"[{name}] %(levelname)s: %(message)s"))
    logger.addHandler(sh)

    return logger


log = setup_logger("context-os")


# ─── Kùzu 스키마 ──────────────────────────────────────────────────────────────

SCHEMA_STATEMENTS = [
    # Node tables
    (
        "CREATE NODE TABLE IF NOT EXISTS Repository"
        "(id STRING, url STRING, local_path STRING, name STRING,"
        " PRIMARY KEY (id))"
    ),
    (
        "CREATE NODE TABLE IF NOT EXISTS Session"
        "(id STRING, started_at STRING, cwd STRING, branch STRING,"
        " PRIMARY KEY (id))"
    ),
    (
        "CREATE NODE TABLE IF NOT EXISTS File"
        "(path STRING, language STRING, size INT64, PRIMARY KEY (path))"
    ),
    (
        "CREATE NODE TABLE IF NOT EXISTS Symbol"
        "(id STRING, name STRING, type STRING, file_path STRING,"
        " start_line INT64, end_line INT64, code STRING, intent STRING,"
        " PRIMARY KEY (id))"
    ),
    (
        "CREATE NODE TABLE IF NOT EXISTS Commit"
        "(hash STRING, message STRING, author STRING, date STRING,"
        " PRIMARY KEY (hash))"
    ),
    (
        "CREATE NODE TABLE IF NOT EXISTS Turn"
        "(id STRING, session_id STRING, timestamp STRING, type STRING,"
        " summary STRING, ref_url STRING, PRIMARY KEY (id))"
    ),
    # Rel tables — 그래프 계층: Repository → File/Commit, Session → Turn → Symbol
    "CREATE REL TABLE IF NOT EXISTS HAS_FILE(FROM Repository TO File)",
    "CREATE REL TABLE IF NOT EXISTS HAS_COMMIT(FROM Repository TO Commit)",
    "CREATE REL TABLE IF NOT EXISTS IN_REPO(FROM Session TO Repository)",
    "CREATE REL TABLE IF NOT EXISTS HAS_TURN(FROM Session TO Turn)",
    "CREATE REL TABLE IF NOT EXISTS CONTAINS(FROM File TO Symbol)",
    "CREATE REL TABLE IF NOT EXISTS CALLS(FROM Symbol TO Symbol)",
    "CREATE REL TABLE IF NOT EXISTS MODIFIES(FROM Commit TO Symbol, changed_lines STRING)",
    "CREATE REL TABLE IF NOT EXISTS ABOUT(FROM Turn TO Symbol)",
    "CREATE REL TABLE IF NOT EXISTS MODIFIED_BY(FROM Turn TO Symbol)",
    "CREATE REL TABLE IF NOT EXISTS LED_TO(FROM Turn TO Commit)",
]


# ─── DB 초기화 / 연결 ────────────────────────────────────────────────────────

def init_db(db_path: str) -> Tuple[kuzu.Database, kuzu.Connection]:
    """Kùzu DB 초기화 및 스키마 생성"""
    db_dir = Path(db_path)
    # 부모 디렉토리만 생성 (kuzu가 DB 경로를 직접 관리)
    db_dir.parent.mkdir(parents=True, exist_ok=True)
    db = kuzu.Database(str(db_dir))
    conn = kuzu.Connection(db)
    for stmt in SCHEMA_STATEMENTS:
        try:
            conn.execute(stmt)
        except RuntimeError as e:
            if "already exists" not in str(e).lower():
                raise
    log.info(f"DB 초기화 완료: {db_path}")
    return db, conn


def get_db_connection(
    db_path: Optional[str] = None,
) -> Tuple[kuzu.Database, kuzu.Connection]:
    """기존 DB에 연결 (db_path 미지정 시 환경변수 → 기본 경로 순서)"""
    path = db_path or os.environ.get("CONTEXT_OS_DB_PATH") or str(DEFAULT_DB_DIR)
    if not Path(path).exists():
        raise FileNotFoundError(
            f"DB가 존재하지 않습니다: {path}. build_context_os.py를 먼저 실행하세요."
        )
    db = kuzu.Database(path)
    conn = kuzu.Connection(db)
    return db, conn


# ─── Repository / Session 헬퍼 ───────────────────────────────────────────────

def generate_repo_id(repo_input: str) -> str:
    """레포 URL 또는 경로에서 고유 ID 생성 (예: 'ej31/claude-session-tracker')"""
    # URL 형태
    if repo_input.startswith(("http://", "https://", "git@")):
        url = repo_input.rstrip("/").rstrip(".git")
        if "github.com" in url:
            parts = url.split("/")
            return f"{parts[-2]}/{parts[-1]}"
        return url

    # 로컬 경로 → git remote에서 추출 시도
    try:
        result = subprocess.run(
            ["git", "-C", repo_input, "remote", "get-url", "origin"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            url = result.stdout.strip().rstrip(".git")
            if "github.com" in url:
                if url.startswith("https://"):
                    parts = url.split("/")
                    return f"{parts[-2]}/{parts[-1]}"
                if ":" in url:
                    return url.split(":")[-1]
    except Exception:
        pass

    return Path(repo_input).name


def resolve_repo_id(cwd: str) -> Optional[str]:
    """cwd의 git remote에서 repo_id 추출 (hook에서 사용)"""
    try:
        result = subprocess.run(
            ["git", "-C", cwd, "remote", "get-url", "origin"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            return generate_repo_id(result.stdout.strip())
    except Exception:
        pass
    return Path(cwd).name


def _detect_branch(cwd: str) -> str:
    """cwd의 현재 git branch 이름 반환"""
    try:
        result = subprocess.run(
            ["git", "-C", cwd, "branch", "--show-current"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    return ""


def _migrate_repository(conn: kuzu.Connection, old_id: str, new_id: str) -> None:
    """fallback repo_id(디렉토리명) → remote 기반 ID로 노드 프로퍼티 업데이트.
    Kuzu 내부 ID는 변경되지 않으므로 기존 관계(HAS_FILE, HAS_COMMIT, IN_REPO)가 모두 유지된다."""
    result = conn.execute(
        "MATCH (r:Repository {id: $oid}) RETURN r.id",
        parameters={"oid": old_id},
    )
    if not result.has_next():
        return

    conn.execute(
        "MATCH (r:Repository {id: $oid}) SET r.id = $nid, r.name = $nid",
        parameters={"oid": old_id, "nid": new_id},
    )
    log.info(f"Repository 마이그레이션 완료: '{old_id}' → '{new_id}'")


def get_or_create_session(
    conn: kuzu.Connection, session_id: str, cwd: str,
) -> str:
    """Session 노드 생성/조회 후 Repository와 연결. session_id 반환"""
    from datetime import datetime

    branch = _detect_branch(cwd)
    conn.execute(
        "MERGE (s:Session {id: $id}) "
        "SET s.started_at = $ts, s.cwd = $cwd, s.branch = $branch",
        parameters={
            "id": session_id,
            "ts": datetime.now().isoformat(),
            "cwd": cwd,
            "branch": branch,
        },
    )

    # Repository가 DB에 존재하면 Session → Repository 연결
    repo_id = resolve_repo_id(cwd)
    if repo_id:
        # fallback → remote 마이그레이션 감지
        dir_name = Path(cwd).name
        if "/" in repo_id and dir_name != repo_id:
            _migrate_repository(conn, old_id=dir_name, new_id=repo_id)

        result = conn.execute(
            "MATCH (r:Repository {id: $rid}) RETURN r.id",
            parameters={"rid": repo_id},
        )
        if result.has_next():
            conn.execute(
                "MATCH (s:Session {id: $sid}), (r:Repository {id: $rid}) "
                "MERGE (s)-[:IN_REPO]->(r)",
                parameters={"sid": session_id, "rid": repo_id},
            )

    return session_id


# ─── ast-grep 실행 헬퍼 ──────────────────────────────────────────────────────

def run_ast_grep(pattern: str, lang: str, cwd: str) -> List[dict]:
    """ast-grep 패턴 실행 후 JSON 결과 반환"""
    try:
        result = subprocess.run(
            ["sg", "--pattern", pattern, "--lang", lang, "--json"],
            capture_output=True,
            text=True,
            cwd=cwd,
            timeout=60,
        )
        if not result.stdout.strip():
            return []
        return json.loads(result.stdout)
    except FileNotFoundError:
        log.error("ast-grep(sg)가 설치되지 않았습니다. brew install ast-grep")
        raise
    except json.JSONDecodeError as e:
        log.warning(f"ast-grep JSON 파싱 실패 (pattern={pattern}): {e}")
        return []
    except subprocess.TimeoutExpired:
        log.warning(f"ast-grep 타임아웃 (pattern={pattern})")
        return []


def make_symbol_id(file_path: str, name: str, start_line: int) -> str:
    """심볼 고유 ID 생성 (file_path:name:start_line)"""
    return f"{file_path}:{name}:{start_line}"


# ─── Step 1: 레포 설정 ───────────────────────────────────────────────────────

def clone_or_use_repo(repo_path: str, target_dir: Optional[str] = None) -> str:
    """레포 클론 또는 기존 로컬 경로 사용"""
    if repo_path.startswith(("http://", "https://", "git@")):
        clone_dir = target_dir or tempfile.mkdtemp(prefix="context_os_")
        log.info(f"레포 클론: {repo_path} → {clone_dir}")
        Repo.clone_from(repo_path, clone_dir)
        return clone_dir

    local_path = Path(repo_path).expanduser().resolve()
    if not local_path.exists():
        raise FileNotFoundError(f"경로가 존재하지 않습니다: {repo_path}")
    log.info(f"로컬 레포 사용: {local_path}")
    return str(local_path)


def discover_files(repo_dir: str) -> List[dict]:
    """레포 내 소스 파일 탐색 및 언어 감지"""
    files = []
    repo_path = Path(repo_dir)
    for file_path in repo_path.rglob("*"):
        if any(skip in file_path.parts for skip in SKIP_DIRS):
            continue
        if not file_path.is_file():
            continue
        lang = LANGUAGE_MAP.get(file_path.suffix)
        if not lang:
            continue
        rel_path = str(file_path.relative_to(repo_path))
        files.append({
            "path": rel_path,
            "language": lang,
            "size": file_path.stat().st_size,
        })
    log.info(f"파일 탐색 완료: {len(files)}개 소스 파일")
    return files


# ─── Step 2: 심볼 추출 ───────────────────────────────────────────────────────

def extract_symbols(
    repo_dir: str, conn: kuzu.Connection, repo_id: str,
) -> Dict[str, dict]:
    """ast-grep으로 심볼 추출 후 DB에 적재"""
    files = discover_files(repo_dir)
    known_symbols: Dict[str, dict] = {}
    languages_found: Set[str] = {f["language"] for f in files}

    # File 노드 적재 + Repository → HAS_FILE 관계
    for f in files:
        conn.execute(
            "MERGE (file:File {path: $path}) "
            "SET file.language = $lang, file.size = $size",
            parameters={"path": f["path"], "lang": f["language"], "size": f["size"]},
        )
        conn.execute(
            "MATCH (r:Repository {id: $rid}), (f:File {path: $fp}) "
            "MERGE (r)-[:HAS_FILE]->(f)",
            parameters={"rid": repo_id, "fp": f["path"]},
        )

    # 언어별 심볼 패턴 실행
    for lang in languages_found:
        patterns = SYMBOL_PATTERNS.get(lang, [])
        for pattern, sym_type in patterns:
            matches = run_ast_grep(pattern, lang, repo_dir)
            for m in matches:
                sym = _parse_symbol_match(m, sym_type)
                if not sym or sym["id"] in known_symbols:
                    continue
                known_symbols[sym["id"]] = sym
                _upsert_symbol(conn, sym)

    log.info(f"심볼 추출 완료: {len(known_symbols)}개")
    return known_symbols


def _parse_symbol_match(match: dict, sym_type: str) -> Optional[dict]:
    """ast-grep 매치 결과를 심볼 dict로 변환"""
    meta = match.get("metaVariables", {})
    name_var = meta.get("single", {}).get("NAME", {})
    name = name_var.get("text")
    if not name:
        return None

    file_path = match.get("file", "")
    rng = match.get("range", {})
    start_line = rng.get("start", {}).get("line", 0) + 1  # 0-indexed → 1-indexed
    end_line = rng.get("end", {}).get("line", 0) + 1
    code = match.get("text", "")[:MAX_CODE_LENGTH]
    sid = make_symbol_id(file_path, name, start_line)

    return {
        "id": sid,
        "name": name,
        "type": sym_type,
        "file_path": file_path,
        "start_line": start_line,
        "end_line": end_line,
        "code": code,
    }


def _upsert_symbol(conn: kuzu.Connection, sym: dict) -> None:
    """심볼 노드 + CONTAINS 관계 적재"""
    conn.execute(
        "MERGE (s:Symbol {id: $id}) "
        "SET s.name = $name, s.type = $type, s.file_path = $fp, "
        "s.start_line = $sl, s.end_line = $el, s.code = $code",
        parameters={
            "id": sym["id"], "name": sym["name"], "type": sym["type"],
            "fp": sym["file_path"], "sl": sym["start_line"],
            "el": sym["end_line"], "code": sym["code"],
        },
    )
    conn.execute(
        "MATCH (f:File {path: $fp}), (s:Symbol {id: $sid}) "
        "MERGE (f)-[:CONTAINS]->(s)",
        parameters={"fp": sym["file_path"], "sid": sym["id"]},
    )


# ─── Step 3: CALLS 관계 추출 ─────────────────────────────────────────────────

def extract_calls(
    repo_dir: str,
    conn: kuzu.Connection,
    known_symbols: Dict[str, dict],
) -> None:
    """함수 호출 패턴 매칭으로 CALLS 관계 적재"""
    name_to_symbols = _build_name_index(known_symbols)
    file_symbols = _build_file_symbol_index(known_symbols)
    languages_present = _collect_languages(known_symbols)

    calls_count = 0
    for lang in languages_present:
        pattern = CALL_PATTERNS.get(lang)
        if not pattern:
            continue
        matches = run_ast_grep(pattern, lang, repo_dir)
        for m in matches:
            count = _process_call_match(m, name_to_symbols, file_symbols, conn)
            calls_count += count

    log.info(f"CALLS 관계 추출 완료: {calls_count}개")


def _build_name_index(known_symbols: Dict[str, dict]) -> Dict[str, List[dict]]:
    """이름 → 심볼 목록 매핑"""
    index: Dict[str, List[dict]] = {}
    for sym in known_symbols.values():
        index.setdefault(sym["name"], []).append(sym)
    return index


def _build_file_symbol_index(known_symbols: Dict[str, dict]) -> Dict[str, List[dict]]:
    """파일별 심볼 범위 인덱스 (start_line 정렬)"""
    index: Dict[str, List[dict]] = {}
    for sym in known_symbols.values():
        index.setdefault(sym["file_path"], []).append(sym)
    for syms in index.values():
        syms.sort(key=lambda s: s["start_line"])
    return index


def _collect_languages(known_symbols: Dict[str, dict]) -> Set[str]:
    """known_symbols에서 사용된 언어 집합"""
    langs = set()
    for sym in known_symbols.values():
        lang = LANGUAGE_MAP.get(Path(sym["file_path"]).suffix, "")
        if lang:
            langs.add(lang)
    return langs


def _process_call_match(
    match: dict,
    name_to_symbols: Dict[str, List[dict]],
    file_symbols: Dict[str, List[dict]],
    conn: kuzu.Connection,
) -> int:
    """단일 함수 호출 매치를 처리하고 생성된 CALLS 수 반환"""
    callee_name = _extract_callee_name(match)
    if not callee_name or callee_name in BUILTIN_SKIP:
        return 0
    if callee_name not in name_to_symbols:
        return 0

    file_path = match.get("file", "")
    call_line = match.get("range", {}).get("start", {}).get("line", 0) + 1
    caller = _find_enclosing_symbol(file_path, call_line, file_symbols)
    if not caller:
        return 0

    count = 0
    for callee_sym in name_to_symbols[callee_name]:
        if callee_sym["id"] == caller["id"]:
            continue
        conn.execute(
            "MATCH (a:Symbol {id: $caller}), (b:Symbol {id: $callee}) "
            "MERGE (a)-[:CALLS]->(b)",
            parameters={"caller": caller["id"], "callee": callee_sym["id"]},
        )
        count += 1
    return count


def _extract_callee_name(match: dict) -> Optional[str]:
    """함수 호출 매치에서 callee 이름 추출 (self.method → method)"""
    meta = match.get("metaVariables", {})
    func_var = meta.get("single", {}).get("FUNC", {})
    func_text = func_var.get("text", "")
    if not func_text:
        return None
    return func_text.rsplit(".", 1)[-1]


def _find_enclosing_symbol(
    file_path: str, line: int, file_symbols: Dict[str, List[dict]],
) -> Optional[dict]:
    """주어진 파일/줄 번호를 감싸는 가장 안쪽 심볼 찾기"""
    symbols = file_symbols.get(file_path, [])
    result = None
    for sym in symbols:
        if sym["start_line"] <= line <= sym["end_line"]:
            if result is None:
                result = sym
            elif (sym["end_line"] - sym["start_line"]) < (
                result["end_line"] - result["start_line"]
            ):
                result = sym
    return result


# ─── Step 4: Git History Overlay ──────────────────────────────────────────────

def overlay_git_history(
    repo_dir: str,
    conn: kuzu.Connection,
    known_symbols: Dict[str, dict],
    repo_id: str,
    max_commits: int = MAX_COMMITS,
) -> None:
    """Git 커밋 히스토리에서 Commit 노드 + MODIFIES 관계 적재"""
    file_symbols = _build_file_symbol_index(known_symbols)
    repo = Repo(repo_dir)

    if repo.bare:
        log.warning("Bare 레포는 Git 히스토리 분석을 건너뜁니다")
        return

    commits_count = 0
    modifies_count = 0

    for commit in repo.iter_commits(max_count=max_commits):
        commit_hash = _upsert_commit(conn, commit)
        # Repository → HAS_COMMIT 관계
        conn.execute(
            "MATCH (r:Repository {id: $rid}), (c:Commit {hash: $hash}) "
            "MERGE (r)-[:HAS_COMMIT]->(c)",
            parameters={"rid": repo_id, "hash": commit_hash},
        )
        commits_count += 1

        parent = commit.parents[0] if commit.parents else None
        try:
            diffs = (
                commit.diff(parent, create_patch=True)
                if parent
                else commit.diff(None, create_patch=True)
            )
        except Exception as e:
            log.debug(f"커밋 {commit.hexsha[:12]} diff 실패: {e}")
            continue

        modifies_count += _process_commit_diffs(
            conn, commit.hexsha[:12], diffs, file_symbols,
        )

    log.info(
        f"Git 히스토리 분석 완료: {commits_count}개 커밋, "
        f"{modifies_count}개 MODIFIES 관계"
    )


def _upsert_commit(conn: kuzu.Connection, commit) -> str:
    """Commit 노드 적재. commit_hash 반환"""
    commit_hash = commit.hexsha[:12]
    conn.execute(
        "MERGE (c:Commit {hash: $hash}) "
        "SET c.message = $msg, c.author = $author, c.date = $date",
        parameters={
            "hash": commit_hash,
            "msg": commit.message.strip().split("\n")[0][:200],
            "author": str(commit.author),
            "date": commit.committed_datetime.isoformat(),
        },
    )
    return commit_hash


def _process_commit_diffs(
    conn: kuzu.Connection,
    commit_hash: str,
    diffs,
    file_symbols: Dict[str, List[dict]],
) -> int:
    """커밋의 diff를 분석하고 MODIFIES 관계를 생성. 생성 수 반환"""
    modifies_count = 0
    for diff_item in diffs:
        file_path = diff_item.b_path or diff_item.a_path
        if not file_path:
            continue

        changed_lines = _parse_diff_lines(diff_item)
        if not changed_lines:
            continue

        symbols = file_symbols.get(file_path, [])
        for sym in symbols:
            overlapping = [
                l for l in changed_lines
                if sym["start_line"] <= l <= sym["end_line"]
            ]
            if not overlapping:
                continue
            conn.execute(
                "MATCH (c:Commit {hash: $hash}), (s:Symbol {id: $sid}) "
                "MERGE (c)-[r:MODIFIES]->(s) "
                "SET r.changed_lines = $lines",
                parameters={
                    "hash": commit_hash,
                    "sid": sym["id"],
                    "lines": ",".join(str(l) for l in overlapping),
                },
            )
            modifies_count += 1
    return modifies_count


def _parse_diff_lines(diff_item) -> List[int]:
    """unified diff에서 변경된 줄 번호 추출 (new file 기준)"""
    try:
        patch = diff_item.diff
        if isinstance(patch, bytes):
            patch = patch.decode("utf-8", errors="replace")
    except Exception:
        return []

    lines = []
    current_line = 0
    for line in patch.split("\n"):
        if line.startswith("@@"):
            match = re.search(r"\+(\d+)", line)
            if match:
                current_line = int(match.group(1))
        elif line.startswith("+") and not line.startswith("+++"):
            lines.append(current_line)
            current_line += 1
        elif line.startswith("-") and not line.startswith("---"):
            pass  # 삭제된 줄은 카운트하지 않음
        else:
            current_line += 1
    return lines


# ─── Step 5: Intent 추출 (Mock) ──────────────────────────────────────────────

def generate_mock_intent(func_name: str, code_snippet: str) -> str:
    """함수명 + docstring 기반 휴리스틱 intent 생성 (외부 AI SDK 미사용)"""
    # Python docstring
    docstring = re.search(r'"""(.*?)"""', code_snippet, re.DOTALL)
    if not docstring:
        docstring = re.search(r"'''(.*?)'''", code_snippet, re.DOTALL)
    if docstring:
        first_line = docstring.group(1).strip().split("\n")[0]
        if first_line:
            return first_line

    # JSDoc 주석
    jsdoc = re.search(r"/\*\*\s*\n?\s*\*?\s*(.*?)[\n*]", code_snippet)
    if jsdoc:
        text = jsdoc.group(1).strip()
        if text:
            return text

    # 함수명에서 추론 (snake_case → 공백)
    return func_name.replace("_", " ").strip()


def update_intents(conn: kuzu.Connection, known_symbols: Dict[str, dict]) -> None:
    """함수 심볼의 intent 속성 업데이트"""
    updated = 0
    for sym in known_symbols.values():
        if sym["type"] != "function":
            continue
        intent = generate_mock_intent(sym["name"], sym.get("code", ""))
        conn.execute(
            "MATCH (s:Symbol {id: $id}) SET s.intent = $intent",
            parameters={"id": sym["id"], "intent": intent},
        )
        updated += 1
    log.info(f"Intent 업데이트 완료: {updated}개 함수")


# ─── Step 6: 검색 함수 ───────────────────────────────────────────────────────

def get_symbol_impact(conn: kuzu.Connection, commit_hash: str) -> List[dict]:
    """커밋이 영향을 미친 심볼 목록 조회"""
    result = conn.execute(
        "MATCH (c:Commit {hash: $hash})-[r:MODIFIES]->(s:Symbol) "
        "RETURN s.name, s.type, s.file_path, s.start_line, r.changed_lines",
        parameters={"hash": commit_hash},
    )
    rows = []
    while result.has_next():
        row = result.get_next()
        rows.append({
            "name": row[0], "type": row[1], "file_path": row[2],
            "start_line": row[3], "changed_lines": row[4],
        })
    return rows


def get_dependency_chain(conn: kuzu.Connection, symbol_name: str) -> List[dict]:
    """심볼의 호출 체인 조회 (2-depth)"""
    result = conn.execute(
        "MATCH (s:Symbol {name: $name})-[:CALLS*1..2]->(dep:Symbol) "
        "RETURN DISTINCT dep.name, dep.type, dep.file_path, dep.intent",
        parameters={"name": symbol_name},
    )
    rows = []
    while result.has_next():
        row = result.get_next()
        rows.append({
            "name": row[0], "type": row[1],
            "file_path": row[2], "intent": row[3],
        })
    return rows


def get_session_context(conn: kuzu.Connection, session_id: str) -> dict:
    """세션 맥락 복원 (Turn + 관련 심볼)"""
    turn_result = conn.execute(
        "MATCH (t:Turn {session_id: $sid}) "
        "RETURN t.id, t.timestamp, t.type, t.summary "
        "ORDER BY t.timestamp DESC",
        parameters={"sid": session_id},
    )
    turns = []
    while turn_result.has_next():
        row = turn_result.get_next()
        turns.append({
            "id": row[0], "timestamp": row[1],
            "type": row[2], "summary": row[3],
        })

    sym_result = conn.execute(
        "MATCH (t:Turn {session_id: $sid})-[:ABOUT|MODIFIED_BY]->(s:Symbol) "
        "RETURN DISTINCT s.name, s.type, s.file_path, s.intent",
        parameters={"sid": session_id},
    )
    symbols = []
    while sym_result.has_next():
        row = sym_result.get_next()
        symbols.append({
            "name": row[0], "type": row[1],
            "file_path": row[2], "intent": row[3],
        })

    return {"turns": turns, "symbols": symbols}


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="Context OS 그래프 DB 구축")
    parser.add_argument("--repo", required=True, help="타겟 레포 URL 또는 로컬 경로")
    parser.add_argument("--db", default=str(DEFAULT_DB_DIR), help="Kùzu DB 경로")
    parser.add_argument(
        "--max-commits", type=int, default=MAX_COMMITS,
        help="분석할 최대 커밋 수",
    )
    args = parser.parse_args()

    log.info(f"파이프라인 시작: repo={args.repo}, db={args.db}")

    # Step 1: 레포 설정 & DB 초기화
    repo_dir = clone_or_use_repo(args.repo)
    db, conn = init_db(args.db)

    # Repository 노드 생성
    repo_id = generate_repo_id(args.repo)
    repo_name = Path(repo_dir).name
    conn.execute(
        "MERGE (r:Repository {id: $id}) "
        "SET r.url = $url, r.local_path = $lp, r.name = $name",
        parameters={
            "id": repo_id, "url": args.repo,
            "lp": repo_dir, "name": repo_name,
        },
    )
    log.info(f"Repository 노드 생성: {repo_id}")

    # Step 2: 심볼 추출
    known_symbols = extract_symbols(repo_dir, conn, repo_id)

    # Step 3: CALLS 관계 추출
    extract_calls(repo_dir, conn, known_symbols)

    # Step 4: Git 히스토리 분석
    overlay_git_history(
        repo_dir, conn, known_symbols, repo_id, max_commits=args.max_commits,
    )

    # Step 5: Intent 생성
    update_intents(conn, known_symbols)

    log.info(f"파이프라인 완료: {len(known_symbols)}개 심볼 적재")
    return 0


if __name__ == "__main__":
    sys.exit(main())
