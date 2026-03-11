#!/usr/bin/env python3
"""Context OS - code graph and session context pipeline.

The graph is scoped per worktree. Safety wins over recall:
- each worktree gets its own Kuzu DB
- active code symbols are refreshed from the current checkout
- if freshness cannot be proven, callers should fail closed
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

try:
    import kuzu
    from git import Repo
except ImportError as e:
    print(
        f"필수 패키지 미설치: {e}\npip install -r requirements.txt",
        file=sys.stderr,
    )
    sys.exit(1)


LOG_FILE = Path("~/.claude/hooks/hooks.log").expanduser()
CONTEXT_OS_ROOT = Path("~/.claude/context_os").expanduser()
SCOPES_DIR = CONTEXT_OS_ROOT / "scopes"
DEFAULT_DB_DIR = CONTEXT_OS_ROOT / "db"
SCHEMA_VERSION = 2
MAX_CODE_LENGTH = 5000
MAX_COMMITS = 50
LOCK_TIMEOUT_SECS = 300
LOCK_STALE_SECS = 600

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

CALL_PATTERNS = {
    "python": "$FUNC($$$)",
    "javascript": "$FUNC($$$)",
    "typescript": "$FUNC($$$)",
}

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


SCHEMA_STATEMENTS = [
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
        "(path STRING, language STRING, size INT64, is_active BOOL,"
        " PRIMARY KEY (path))"
    ),
    (
        "CREATE NODE TABLE IF NOT EXISTS Symbol"
        "(id STRING, name STRING, type STRING, file_path STRING,"
        " start_line INT64, end_line INT64, code STRING, intent STRING,"
        " is_active BOOL, PRIMARY KEY (id))"
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
    "CREATE REL TABLE IF NOT EXISTS TOUCHED_FILE(FROM Turn TO File)",
]


def setup_logger(name: str) -> logging.Logger:
    """Create a logger that writes to stderr and the shared hook log."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")

    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(LOG_FILE)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except OSError as e:
        # 테스트/샌드박스에서는 홈 디렉터리 로그 파일 생성이 막힐 수 있다.
        logger.debug("파일 로거 비활성화: %s", e)

    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(logging.Formatter(f"[{name}] %(levelname)s: %(message)s"))
    logger.addHandler(sh)

    return logger


log = setup_logger("context-os")


def init_db(db_path: str) -> Tuple[kuzu.Database, kuzu.Connection]:
    """Initialize a Kuzu DB and ensure the schema exists."""
    db_dir = Path(db_path)
    db_dir.parent.mkdir(parents=True, exist_ok=True)
    db = kuzu.Database(str(db_dir))
    conn = kuzu.Connection(db)
    for stmt in SCHEMA_STATEMENTS:
        try:
            conn.execute(stmt)
        except RuntimeError as e:
            if "already exists" not in str(e).lower():
                raise
    log.info("DB 초기화 완료: %s", db_path)
    return db, conn


def _run_git(cwd: str, *args: str, timeout: int = 5) -> Optional[str]:
    try:
        result = subprocess.run(
            ["git", "-C", cwd, *args],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except Exception as e:
        log.debug("git 명령 실패 (%s): %s", " ".join(args), e)
        return None

    if result.returncode != 0:
        return None
    text = result.stdout.strip()
    return text or None


def resolve_worktree_root(path: str) -> str:
    """Return the worktree root for a path, or the resolved path if not in git."""
    target = Path(path or os.getcwd()).expanduser().resolve()
    cwd = str(target if target.is_dir() else target.parent)
    root = _run_git(cwd, "rev-parse", "--show-toplevel")
    return str(Path(root).resolve()) if root else cwd


def resolve_repo_root(path: str) -> str:
    """Return the common repository working root for a git worktree."""
    worktree_root = resolve_worktree_root(path)
    common_dir = _run_git(worktree_root, "rev-parse", "--git-common-dir")
    if not common_dir:
        return worktree_root

    common_path = Path(common_dir)
    if not common_path.is_absolute():
        common_path = (Path(worktree_root) / common_path).resolve()
    return str(common_path.parent)


def _detect_branch(cwd: str) -> str:
    return _run_git(cwd, "branch", "--show-current") or ""


def _detect_head(cwd: str) -> str:
    return _run_git(cwd, "rev-parse", "HEAD") or ""


def _normalize_repo_url(value: str) -> str:
    url = value.rstrip("/")
    if url.endswith(".git"):
        url = url[:-4]
    return url


def generate_repo_id(repo_input: str) -> str:
    """Generate a stable repository id, usually owner/repo."""
    if repo_input.startswith(("http://", "https://", "git@")):
        url = _normalize_repo_url(repo_input)
        if "github.com" in url:
            parts = url.split("/")
            return f"{parts[-2]}/{parts[-1]}"
        if ":" in url and url.startswith("git@"):
            return url.split(":", 1)[1]
        return url

    resolved = Path(repo_input).expanduser().resolve()
    remote = _run_git(str(resolved), "remote", "get-url", "origin")
    if remote:
        return generate_repo_id(remote)
    return resolved.name


def resolve_repo_id(cwd: str) -> str:
    remote = _run_git(cwd, "remote", "get-url", "origin")
    if remote:
        return generate_repo_id(remote)
    return Path(resolve_worktree_root(cwd)).name


def _scope_id_for_root(worktree_root: str) -> str:
    return hashlib.sha256(worktree_root.encode("utf-8")).hexdigest()[:16]


def get_scope_dir(cwd: Optional[str] = None) -> Path:
    worktree_root = resolve_worktree_root(cwd or os.getcwd())
    return SCOPES_DIR / _scope_id_for_root(worktree_root)


def get_scope_db_path(cwd: Optional[str] = None) -> str:
    return str(get_scope_dir(cwd) / "db")


def get_scope_meta_path(cwd: Optional[str] = None) -> Path:
    return get_scope_dir(cwd) / "meta.json"


def get_scope_lock_path(cwd: Optional[str] = None) -> Path:
    return get_scope_dir(cwd) / "lock"


def load_scope_meta(cwd: Optional[str] = None) -> Optional[dict]:
    meta_path = get_scope_meta_path(cwd)
    if not meta_path.exists():
        return None
    with open(meta_path) as f:
        return json.load(f)


def _write_scope_meta(cwd: str, data: dict) -> None:
    meta_path = get_scope_meta_path(cwd)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    with open(meta_path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _iter_source_paths(repo_dir: str) -> Iterable[Path]:
    repo_path = Path(repo_dir)
    for file_path in sorted(repo_path.rglob("*")):
        if any(skip in file_path.parts for skip in SKIP_DIRS):
            continue
        if not file_path.is_file():
            continue
        if file_path.suffix not in LANGUAGE_MAP:
            continue
        yield file_path


def discover_files(repo_dir: str) -> List[dict]:
    """List supported source files within a repository."""
    repo_path = Path(repo_dir)
    files = []
    for file_path in _iter_source_paths(repo_dir):
        rel_path = file_path.relative_to(repo_path).as_posix()
        files.append({
            "path": rel_path,
            "language": LANGUAGE_MAP[file_path.suffix],
            "size": file_path.stat().st_size,
        })
    log.info("파일 탐색 완료: %s개 소스 파일", len(files))
    return files


def compute_source_fingerprint(repo_dir: str) -> str:
    """Hash current source file paths and contents for strong freshness checks."""
    repo_path = Path(repo_dir)
    hasher = hashlib.sha256()
    for file_path in _iter_source_paths(repo_dir):
        rel_path = file_path.relative_to(repo_path).as_posix()
        hasher.update(rel_path.encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(file_path.read_bytes())
        hasher.update(b"\0")
    return hasher.hexdigest()


def _current_scope_snapshot(cwd: str) -> dict:
    worktree_root = resolve_worktree_root(cwd)
    return {
        "schema_version": SCHEMA_VERSION,
        "worktree_root": worktree_root,
        "repo_root": resolve_repo_root(cwd),
        "repo_id": resolve_repo_id(cwd),
        "branch": _detect_branch(worktree_root),
        "head": _detect_head(worktree_root),
        "source_fingerprint": compute_source_fingerprint(worktree_root),
    }


def _persist_scope_meta(repo_dir: str, include_git_history: bool) -> dict:
    snapshot = _current_scope_snapshot(repo_dir)
    snapshot["last_synced_at"] = datetime.now().isoformat()
    snapshot["last_sync_kind"] = "full" if include_git_history else "code"
    _write_scope_meta(repo_dir, snapshot)
    return snapshot


def is_scope_fresh(cwd: str) -> bool:
    meta = load_scope_meta(cwd)
    db_path = Path(get_scope_db_path(cwd))
    if not meta or not db_path.exists():
        return False
    if meta.get("schema_version") != SCHEMA_VERSION:
        return False

    current = _current_scope_snapshot(cwd)
    for key in (
        "schema_version",
        "worktree_root",
        "repo_root",
        "repo_id",
        "branch",
        "head",
        "source_fingerprint",
    ):
        if current.get(key) != meta.get(key):
            return False
    return True


@contextmanager
def scope_lock(cwd: str, timeout_secs: int = LOCK_TIMEOUT_SECS):
    """Cross-process scope lock using exclusive lock-file creation."""
    lock_path = get_scope_lock_path(cwd)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.time() + timeout_secs
    fd = None

    while time.time() < deadline:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
            payload = {"pid": os.getpid(), "started_at": time.time()}
            os.write(fd, json.dumps(payload).encode("utf-8"))
            break
        except FileExistsError:
            try:
                age = time.time() - lock_path.stat().st_mtime
            except FileNotFoundError:
                continue
            if age > LOCK_STALE_SECS:
                try:
                    lock_path.unlink()
                    continue
                except FileNotFoundError:
                    continue
            time.sleep(0.1)

    if fd is None:
        raise TimeoutError(f"Context OS lock timeout: {lock_path}")

    try:
        yield
    finally:
        try:
            os.close(fd)
        finally:
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass


def get_db_connection(
    db_path: Optional[str] = None,
    cwd: Optional[str] = None,
) -> Tuple[kuzu.Database, kuzu.Connection]:
    """Connect to the scoped Context OS DB."""
    path = db_path or os.environ.get("CONTEXT_OS_DB_PATH")
    if not path:
        scope_cwd = cwd or os.environ.get("CONTEXT_OS_WORKTREE_ROOT") or os.getcwd()
        path = get_scope_db_path(scope_cwd)
    if not Path(path).exists():
        raise FileNotFoundError(
            f"DB가 존재하지 않습니다: {path}. build_context_os.py를 먼저 실행하세요."
        )
    db = kuzu.Database(path)
    conn = kuzu.Connection(db)
    return db, conn


def clone_or_use_repo(repo_path: str, target_dir: Optional[str] = None) -> str:
    """Clone a remote repo or return a local repo path."""
    if repo_path.startswith(("http://", "https://", "git@")):
        clone_dir = target_dir or tempfile.mkdtemp(prefix="context_os_")
        log.info("레포 클론: %s → %s", repo_path, clone_dir)
        Repo.clone_from(repo_path, clone_dir)
        return clone_dir

    local_path = Path(repo_path).expanduser().resolve()
    if not local_path.exists():
        raise FileNotFoundError(f"경로가 존재하지 않습니다: {repo_path}")
    log.info("로컬 레포 사용: %s", local_path)
    return str(local_path)


def get_or_create_session(conn: kuzu.Connection, session_id: str, cwd: str) -> str:
    """Create or update the Session node and connect it to the current repo."""
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

    repo_id = resolve_repo_id(cwd)
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


def run_ast_grep(pattern: str, lang: str, cwd: str) -> List[dict]:
    """Run ast-grep and return JSON results."""
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
        log.warning("ast-grep JSON 파싱 실패 (pattern=%s): %s", pattern, e)
        return []
    except subprocess.TimeoutExpired:
        log.warning("ast-grep 타임아웃 (pattern=%s)", pattern)
        return []


def make_symbol_id(file_path: str, name: str, start_line: int) -> str:
    return f"{file_path}:{name}:{start_line}"


def _ensure_repository(
    conn: kuzu.Connection,
    repo_id: str,
    repo_input: str,
    repo_dir: str,
) -> None:
    conn.execute(
        "MERGE (r:Repository {id: $id}) "
        "SET r.url = $url, r.local_path = $lp, r.name = $name",
        parameters={
            "id": repo_id,
            "url": repo_input,
            "lp": resolve_worktree_root(repo_dir),
            "name": Path(resolve_repo_root(repo_dir)).name,
        },
    )


def _deactivate_code_graph(conn: kuzu.Connection) -> None:
    conn.execute("MATCH (f:File) SET f.is_active = false")
    conn.execute("MATCH (s:Symbol) SET s.is_active = false")
    conn.execute("MATCH ()-[r:CALLS]->() DELETE r")


def _reset_commit_graph(conn: kuzu.Connection) -> None:
    conn.execute("MATCH (c:Commit) DETACH DELETE c")


def extract_symbols(
    repo_dir: str,
    conn: kuzu.Connection,
    repo_id: str,
) -> Dict[str, dict]:
    """Extract current code symbols and reactivate them."""
    files = discover_files(repo_dir)
    known_symbols: Dict[str, dict] = {}
    languages_found: Set[str] = {f["language"] for f in files}

    for file_info in files:
        conn.execute(
            "MERGE (file:File {path: $path}) "
            "SET file.language = $lang, file.size = $size, file.is_active = true",
            parameters={
                "path": file_info["path"],
                "lang": file_info["language"],
                "size": file_info["size"],
            },
        )
        conn.execute(
            "MATCH (r:Repository {id: $rid}), (f:File {path: $fp}) "
            "MERGE (r)-[:HAS_FILE]->(f)",
            parameters={"rid": repo_id, "fp": file_info["path"]},
        )

    for lang in languages_found:
        patterns = SYMBOL_PATTERNS.get(lang, [])
        for pattern, sym_type in patterns:
            matches = run_ast_grep(pattern, lang, repo_dir)
            for match in matches:
                sym = _parse_symbol_match(match, sym_type)
                if not sym or sym["id"] in known_symbols:
                    continue
                known_symbols[sym["id"]] = sym
                _upsert_symbol(conn, sym)

    log.info("심볼 추출 완료: %s개", len(known_symbols))
    return known_symbols


def _parse_symbol_match(match: dict, sym_type: str) -> Optional[dict]:
    meta = match.get("metaVariables", {})
    name_var = meta.get("single", {}).get("NAME", {})
    name = name_var.get("text")
    if not name:
        return None

    file_path = Path(match.get("file", "")).as_posix()
    rng = match.get("range", {})
    start_line = rng.get("start", {}).get("line", 0) + 1
    end_line = rng.get("end", {}).get("line", 0) + 1
    code = match.get("text", "")[:MAX_CODE_LENGTH]
    return {
        "id": make_symbol_id(file_path, name, start_line),
        "name": name,
        "type": sym_type,
        "file_path": file_path,
        "start_line": start_line,
        "end_line": end_line,
        "code": code,
    }


def _upsert_symbol(conn: kuzu.Connection, sym: dict) -> None:
    conn.execute(
        "MERGE (s:Symbol {id: $id}) "
        "SET s.name = $name, s.type = $type, s.file_path = $fp, "
        "s.start_line = $sl, s.end_line = $el, s.code = $code, s.is_active = true",
        parameters={
            "id": sym["id"],
            "name": sym["name"],
            "type": sym["type"],
            "fp": sym["file_path"],
            "sl": sym["start_line"],
            "el": sym["end_line"],
            "code": sym["code"],
        },
    )
    conn.execute(
        "MATCH (f:File {path: $fp}), (s:Symbol {id: $sid}) "
        "MERGE (f)-[:CONTAINS]->(s)",
        parameters={"fp": sym["file_path"], "sid": sym["id"]},
    )


def extract_calls(
    repo_dir: str,
    conn: kuzu.Connection,
    known_symbols: Dict[str, dict],
) -> None:
    name_to_symbols = _build_name_index(known_symbols)
    file_symbols = _build_file_symbol_index(known_symbols)
    languages_present = _collect_languages(known_symbols)

    calls_count = 0
    for lang in languages_present:
        pattern = CALL_PATTERNS.get(lang)
        if not pattern:
            continue
        matches = run_ast_grep(pattern, lang, repo_dir)
        for match in matches:
            calls_count += _process_call_match(
                match, name_to_symbols, file_symbols, conn,
            )

    log.info("CALLS 관계 추출 완료: %s개", calls_count)


def _build_name_index(known_symbols: Dict[str, dict]) -> Dict[str, List[dict]]:
    index: Dict[str, List[dict]] = {}
    for sym in known_symbols.values():
        index.setdefault(sym["name"], []).append(sym)
    return index


def _build_file_symbol_index(known_symbols: Dict[str, dict]) -> Dict[str, List[dict]]:
    index: Dict[str, List[dict]] = {}
    for sym in known_symbols.values():
        index.setdefault(sym["file_path"], []).append(sym)
    for symbols in index.values():
        symbols.sort(key=lambda item: item["start_line"])
    return index


def _collect_languages(known_symbols: Dict[str, dict]) -> Set[str]:
    langs = set()
    for sym in known_symbols.values():
        lang = LANGUAGE_MAP.get(Path(sym["file_path"]).suffix)
        if lang:
            langs.add(lang)
    return langs


def _process_call_match(
    match: dict,
    name_to_symbols: Dict[str, List[dict]],
    file_symbols: Dict[str, List[dict]],
    conn: kuzu.Connection,
) -> int:
    callee_name = _extract_callee_name(match)
    if not callee_name or callee_name in BUILTIN_SKIP:
        return 0
    if callee_name not in name_to_symbols:
        return 0

    file_path = Path(match.get("file", "")).as_posix()
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
    meta = match.get("metaVariables", {})
    func_var = meta.get("single", {}).get("FUNC", {})
    func_text = func_var.get("text", "")
    if not func_text:
        return None
    return func_text.rsplit(".", 1)[-1]


def _find_enclosing_symbol(
    file_path: str,
    line: int,
    file_symbols: Dict[str, List[dict]],
) -> Optional[dict]:
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


def overlay_git_history(
    repo_dir: str,
    conn: kuzu.Connection,
    known_symbols: Dict[str, dict],
    repo_id: str,
    max_commits: int = MAX_COMMITS,
) -> None:
    file_symbols = _build_file_symbol_index(known_symbols)
    repo = Repo(repo_dir)
    if repo.bare:
        log.warning("Bare 레포는 Git 히스토리 분석을 건너뜁니다")
        return

    commits_count = 0
    modifies_count = 0
    for commit in repo.iter_commits(max_count=max_commits):
        commit_hash = _upsert_commit(conn, commit)
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
            log.debug("커밋 %s diff 실패: %s", commit.hexsha[:12], e)
            continue

        modifies_count += _process_commit_diffs(
            conn, commit.hexsha[:12], diffs, file_symbols,
        )

    log.info(
        "Git 히스토리 분석 완료: %s개 커밋, %s개 MODIFIES 관계",
        commits_count,
        modifies_count,
    )


def _upsert_commit(conn: kuzu.Connection, commit) -> str:
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
    modifies_count = 0
    for diff_item in diffs:
        file_path = diff_item.b_path or diff_item.a_path
        if not file_path:
            continue

        changed_lines = _parse_diff_lines(diff_item)
        if not changed_lines:
            continue

        symbols = file_symbols.get(Path(file_path).as_posix(), [])
        for sym in symbols:
            overlapping = [
                line for line in changed_lines
                if sym["start_line"] <= line <= sym["end_line"]
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
                    "lines": ",".join(str(line) for line in overlapping),
                },
            )
            modifies_count += 1
    return modifies_count


def _parse_diff_lines(diff_item) -> List[int]:
    try:
        patch = diff_item.diff
        if isinstance(patch, bytes):
            patch = patch.decode("utf-8", errors="replace")
    except Exception as e:
        log.debug("diff 파싱 실패 (_parse_diff_lines): %s", e)
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
            continue
        else:
            current_line += 1
    return lines


def generate_mock_intent(func_name: str, code_snippet: str) -> str:
    docstring = re.search(r'"""(.*?)"""', code_snippet, re.DOTALL)
    if not docstring:
        docstring = re.search(r"'''(.*?)'''", code_snippet, re.DOTALL)
    if docstring:
        first_line = docstring.group(1).strip().split("\n")[0]
        if first_line:
            return first_line

    jsdoc = re.search(r"/\*\*\s*\n?\s*\*?\s*(.*?)[\n*]", code_snippet)
    if jsdoc:
        text = jsdoc.group(1).strip()
        if text:
            return text

    return func_name.replace("_", " ").strip()


def update_intents(conn: kuzu.Connection, known_symbols: Dict[str, dict]) -> None:
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
    log.info("Intent 업데이트 완료: %s개 함수", updated)


def sync_repository_graph(
    repo_dir: str,
    conn: kuzu.Connection,
    repo_input: str,
    include_git_history: bool = True,
    max_commits: int = MAX_COMMITS,
) -> Dict[str, dict]:
    repo_id = generate_repo_id(repo_input)
    _ensure_repository(conn, repo_id, repo_input, repo_dir)
    _deactivate_code_graph(conn)

    known_symbols = extract_symbols(repo_dir, conn, repo_id)
    extract_calls(repo_dir, conn, known_symbols)
    update_intents(conn, known_symbols)

    if include_git_history:
        _reset_commit_graph(conn)
        overlay_git_history(repo_dir, conn, known_symbols, repo_id, max_commits)

    _persist_scope_meta(repo_dir, include_git_history=include_git_history)
    return known_symbols


def build_context_os(
    repo_path: str,
    db_path: Optional[str] = None,
    max_commits: int = MAX_COMMITS,
    include_git_history: bool = True,
) -> Tuple[kuzu.Database, kuzu.Connection, Dict[str, dict]]:
    repo_dir = clone_or_use_repo(repo_path)
    resolved_db_path = db_path or get_scope_db_path(repo_dir)

    meta = load_scope_meta(repo_dir)
    if meta and meta.get("schema_version") != SCHEMA_VERSION:
        db_dir = Path(resolved_db_path)
        if db_dir.exists():
            shutil.rmtree(db_dir)

    db, conn = init_db(resolved_db_path)
    known_symbols = sync_repository_graph(
        repo_dir,
        conn,
        repo_path,
        include_git_history=include_git_history,
        max_commits=max_commits,
    )
    return db, conn, known_symbols


def ensure_scope_current(
    cwd: str,
    include_git_history: bool = True,
    max_commits: int = MAX_COMMITS,
) -> bool:
    worktree_root = resolve_worktree_root(cwd)
    meta = load_scope_meta(worktree_root)
    db_path = Path(get_scope_db_path(worktree_root))

    if meta and meta.get("schema_version") != SCHEMA_VERSION and db_path.exists():
        shutil.rmtree(db_path)

    if db_path.exists() and is_scope_fresh(worktree_root):
        return True

    try:
        build_context_os(
            worktree_root,
            db_path=str(db_path),
            max_commits=max_commits,
            include_git_history=include_git_history,
        )
        return True
    except Exception as e:
        log.error("Scope 재동기화 실패: %s", e)
        return False


def get_active_symbols_by_file(conn: kuzu.Connection, file_path: str) -> List[dict]:
    result = conn.execute(
        "MATCH (s:Symbol) WHERE s.file_path = $fp AND s.is_active = true "
        "RETURN s.id, s.name, s.type, s.start_line, s.end_line",
        parameters={"fp": file_path},
    )
    symbols = []
    while result.has_next():
        row = result.get_next()
        symbols.append({
            "id": row[0],
            "name": row[1],
            "type": row[2],
            "start_line": row[3],
            "end_line": row[4],
        })
    return symbols


def get_active_symbol_index(conn: kuzu.Connection) -> Dict[str, List[dict]]:
    result = conn.execute(
        "MATCH (s:Symbol) WHERE s.is_active = true "
        "RETURN s.id, s.name, s.file_path, s.type"
    )
    index: Dict[str, List[dict]] = {}
    while result.has_next():
        row = result.get_next()
        index.setdefault(row[1], []).append({
            "id": row[0],
            "name": row[1],
            "file_path": row[2],
            "type": row[3],
        })
    return index


def get_symbol_impact(conn: kuzu.Connection, commit_hash: str) -> List[dict]:
    result = conn.execute(
        "MATCH (c:Commit {hash: $hash})-[r:MODIFIES]->(s:Symbol) "
        "RETURN s.name, s.type, s.file_path, s.start_line, r.changed_lines",
        parameters={"hash": commit_hash},
    )
    rows = []
    while result.has_next():
        row = result.get_next()
        rows.append({
            "name": row[0],
            "type": row[1],
            "file_path": row[2],
            "start_line": row[3],
            "changed_lines": row[4],
        })
    return rows


def get_dependency_chain(conn: kuzu.Connection, symbol_name: str) -> List[dict]:
    result = conn.execute(
        "MATCH (s:Symbol)-[:CALLS*1..2]->(dep:Symbol) "
        "WHERE s.name = $name AND s.is_active = true AND dep.is_active = true "
        "RETURN DISTINCT dep.name, dep.type, dep.file_path, dep.intent",
        parameters={"name": symbol_name},
    )
    rows = []
    while result.has_next():
        row = result.get_next()
        rows.append({
            "name": row[0],
            "type": row[1],
            "file_path": row[2],
            "intent": row[3],
        })
    return rows


def get_session_context(conn: kuzu.Connection, session_id: str) -> dict:
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
            "id": row[0],
            "timestamp": row[1],
            "type": row[2],
            "summary": row[3],
        })

    sym_result = conn.execute(
        "MATCH (t:Turn {session_id: $sid})-[:ABOUT|MODIFIED_BY]->(s:Symbol) "
        "WHERE s.is_active = true "
        "RETURN DISTINCT s.name, s.type, s.file_path, s.intent",
        parameters={"sid": session_id},
    )
    symbols = []
    while sym_result.has_next():
        row = sym_result.get_next()
        symbols.append({
            "name": row[0],
            "type": row[1],
            "file_path": row[2],
            "intent": row[3],
        })

    return {"turns": turns, "symbols": symbols}


def main() -> int:
    parser = argparse.ArgumentParser(description="Context OS 그래프 DB 구축")
    parser.add_argument("--repo", required=True, help="타겟 레포 URL 또는 로컬 경로")
    parser.add_argument("--db", default=None, help="Kùzu DB 경로")
    parser.add_argument(
        "--max-commits",
        type=int,
        default=MAX_COMMITS,
        help="분석할 최대 커밋 수",
    )
    parser.add_argument(
        "--code-only",
        action="store_true",
        help="코드 그래프만 동기화하고 git history는 건너뜀",
    )
    args = parser.parse_args()

    log.info("파이프라인 시작: repo=%s db=%s", args.repo, args.db or "(scoped)")
    build_context_os(
        args.repo,
        db_path=args.db,
        max_commits=args.max_commits,
        include_git_history=not args.code_only,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
