from __future__ import annotations

import hashlib
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path


SUPPORTED_SUFFIXES = {
    "",
    ".py", ".js", ".jsx", ".ts", ".tsx", ".mjs",
    ".c", ".h", ".S", ".asm", ".mak", ".txt", ".md", ".sh",
}


def _stable_digest(value: str, length: int = 16) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:length]


def _run_git(cwd: str, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", cwd, *args],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def resolve_worktree_root(path: str) -> str:
    target = Path(path or os.getcwd()).expanduser().resolve()
    cwd = str(target if target.is_dir() else target.parent)
    root = _run_git(cwd, "rev-parse", "--show-toplevel")
    return str(Path(root).resolve()) if root else cwd


def detect_head(path: str) -> str:
    return _run_git(resolve_worktree_root(path), "rev-parse", "HEAD")


def detect_branch(path: str) -> str:
    return _run_git(resolve_worktree_root(path), "branch", "--show-current")


def detect_remote(path: str) -> str:
    return _run_git(resolve_worktree_root(path), "remote", "get-url", "origin")


def is_worktree_clean(path: str) -> bool:
    return not _run_git(resolve_worktree_root(path), "status", "--porcelain")


def normalize_remote(value: str) -> str:
    cleaned = value.strip().rstrip("/")
    if cleaned.endswith(".git"):
        cleaned = cleaned[:-4]
    if cleaned.startswith("git@"):
        host, remainder = cleaned.split(":", 1)
        return f"ssh://{host[4:]}/{remainder}"
    return cleaned


def make_repo_id(path: str) -> str:
    remote = normalize_remote(detect_remote(path))
    if remote:
        parts = remote.split("/")
        if len(parts) >= 2:
            return f"{parts[-2]}/{parts[-1]}"
        return remote
    return Path(resolve_worktree_root(path)).name


def make_ingest_batch_id(repo_id: str, started_at: datetime | None = None) -> str:
    moment = started_at or datetime.utcnow()
    seed = f"{repo_id}:{moment.isoformat()}"
    return f"{moment.strftime('%Y%m%d%H%M%S')}-{_stable_digest(seed, 8)}"


def make_developer_id(email_norm: str, name: str) -> str:
    basis = (email_norm or name or "unknown").strip().lower()
    return f"dev_{_stable_digest(basis, 12)}"


def make_file_id(repo_id: str, canonical_path: str) -> str:
    return f"file_{_stable_digest(f'{repo_id}:{canonical_path}', 16)}"


def make_file_state_id(file_id: str, blob_sha: str, path_at_commit: str = "") -> str:
    return f"{file_id}@{_stable_digest(f'{blob_sha}:{path_at_commit}', 16)}"


def make_symbol_id(repo_id: str, file_id: str, kind: str, canonical_name: str) -> str:
    seed = f"{repo_id}:{file_id}:{kind}:{canonical_name}"
    return f"sym_{_stable_digest(seed, 16)}"


def make_symbol_state_id(
    symbol_id: str,
    signature_hash: str,
    body_hash: str,
    file_state_id: str = "",
) -> str:
    return f"{symbol_id}@{_stable_digest(f'{signature_hash}:{body_hash}:{file_state_id}', 24)}"


def make_chunk_namespace(symbol_state_id: str | None, file_state_id: str) -> str:
    return symbol_state_id or file_state_id


def make_chunk_id(commit_sha: str, chunk_namespace: str, ordinal: int) -> str:
    return f"{commit_sha}:{chunk_namespace}:{ordinal}"


def hash_content(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def iter_source_paths(path: str):
    root = Path(resolve_worktree_root(path))
    for file_path in sorted(root.rglob("*")):
        if not file_path.is_file():
            continue
        if ".git" in file_path.parts or "__pycache__" in file_path.parts:
            continue
        if file_path.suffix not in SUPPORTED_SUFFIXES:
            continue
        yield file_path


def compute_source_fingerprint(path: str) -> str:
    root = Path(resolve_worktree_root(path))
    hasher = hashlib.sha256()
    for file_path in iter_source_paths(path):
        rel_path = file_path.relative_to(root).as_posix()
        hasher.update(rel_path.encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(file_path.read_bytes())
        hasher.update(b"\0")
    return hasher.hexdigest()


def shard_for_timestamp(repo_id: str, timestamp: str) -> str:
    dt = datetime.fromisoformat(timestamp)
    quarter = ((dt.month - 1) // 3) + 1
    repo_slug = re.sub(r"[^a-zA-Z0-9]+", "_", repo_id).strip("_").lower() or "repo"
    return f"history_chunks__{repo_slug}__{dt.year}q{quarter}"


def head_table_name(repo_id: str) -> str:
    repo_slug = re.sub(r"[^a-zA-Z0-9]+", "_", repo_id).strip("_").lower() or "repo"
    return f"head_chunks__{repo_slug}"
