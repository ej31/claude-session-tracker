#!/usr/bin/env python3
"""Claude Code GitHub Projects hook 공통 유틸리티"""
from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple

LOG_FILE = Path("~/.claude/hooks/hooks.log").expanduser()
_CONFIG_ENV = Path("~/.claude/hooks/config.env").expanduser()
STATE_DIR = Path("~/.claude/hooks/state").expanduser()
RUNTIME_STATUS_FILE = Path("~/.claude/hooks/runtime_status.json").expanduser()
CLAUDE_CODE_LABEL = "claude-code"
PROJECT_STATUS_MARKER = "<!-- claude-session-tracker:project-status -->"


# ─── 로거 ────────────────────────────────────────────────────────────────────

def setup_logger(name: str) -> logging.Logger:
    """stderr + 파일 동시 출력 로거 생성"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")

    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(logging.Formatter(f"[{name}] %(levelname)s: %(message)s"))
    logger.addHandler(sh)

    return logger


# ─── 환경변수 로드 ────────────────────────────────────────────────────────────

def _load_single(path: str) -> None:
    env_path = Path(path).expanduser()
    if not env_path.exists():
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if not (value.startswith('"') or value.startswith("'")):
                value = value.split("#")[0].strip()
            value = value.strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def load_env_file(path: str = "~/.keys/.env") -> None:
    """env 파일 + hook config.env 로드 (이미 설정된 값은 덮어쓰지 않음)"""
    _load_single(path)
    _load_single(str(_CONFIG_ENV))


# ─── 설정값 접근 (환경변수 우선) ─────────────────────────────────────────────

def _require(key: str) -> str:
    value = os.environ.get(key, "")
    if not value:
        raise RuntimeError(
            f"환경변수 {key}가 없습니다. `npx claude-session-tracker`로 재설치해주세요."
        )
    return value


def _project_id() -> str:
    return _require("GITHUB_PROJECT_ID")


def _status_field_id() -> str:
    return _require("GITHUB_STATUS_FIELD_ID")


def _status_option(name: str) -> str:
    key_map = {
        "registered": "GITHUB_STATUS_REGISTERED",
        "responding": "GITHUB_STATUS_RESPONDING",
        "waiting": "GITHUB_STATUS_WAITING",
        "closed": "GITHUB_STATUS_CLOSED",
    }
    return _require(key_map[name])


def _created_field_id() -> Optional[str]:
    return os.environ.get("GITHUB_CREATED_FIELD_ID") or None


def _last_active_field_id() -> Optional[str]:
    return os.environ.get("GITHUB_LAST_ACTIVE_FIELD_ID") or None


_COMMENT_LABELS = {
    "en": {"prompt": "Prompt", "response": "Response"},
    "ko": {"prompt": "프롬프트", "response": "답변"},
    "ja": {"prompt": "プロンプト", "response": "回答"},
    "zh": {"prompt": "提示词", "response": "回答"},
}


def _comment_labels() -> dict:
    lang = os.environ.get("CST_LANG", "en")
    return _COMMENT_LABELS.get(lang, _COMMENT_LABELS["en"])


def _project_url() -> Optional[str]:
    owner = os.environ.get("GITHUB_PROJECT_OWNER")
    number = os.environ.get("GITHUB_PROJECT_NUMBER")
    if not owner or not number:
        return None
    return f"https://github.com/users/{owner}/projects/{number}"


def _done_timeout() -> int:
    return int(os.environ.get("DONE_TIMEOUT_SECS", "1800"))


def _notes_repo() -> str:
    return _require("NOTES_REPO")


# ─── 파일 / JSON 헬퍼 ────────────────────────────────────────────────────────

def _read_json(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_runtime_status() -> Optional[dict]:
    try:
        return _read_json(RUNTIME_STATUS_FILE)
    except Exception:
        return None


def save_runtime_status(data: dict) -> None:
    _write_json(RUNTIME_STATUS_FILE, data)


def clear_runtime_status() -> None:
    try:
        RUNTIME_STATUS_FILE.unlink()
    except FileNotFoundError:
        pass


# ─── GraphQL / gh 헬퍼 ───────────────────────────────────────────────────────

def graphql_request(query: str, variables: dict) -> dict:
    """gh CLI를 통해 GitHub GraphQL API 요청"""
    _log = setup_logger("graphql")
    payload = json.dumps({"query": query, "variables": variables})
    _log.debug(f"GraphQL 요청: vars={list(variables.keys())}")
    result = subprocess.run(
        ["gh", "api", "graphql", "--input", "-"],
        input=payload,
        capture_output=True,
        text=True,
        timeout=15,
    )
    if result.stdout.strip():
        data = json.loads(result.stdout)
        if "errors" in data:
            _log.warning(f"GraphQL 부분 오류: {data['errors']}")
        return data
    _log.error(f"gh api 실패 (returncode={result.returncode}): {result.stderr.strip()}")
    raise RuntimeError(f"gh api 실패: {result.stderr.strip()}")


def is_repo_private(repo: str) -> bool:
    result = subprocess.run(
        ["gh", "api", f"repos/{repo}", "--jq", ".private"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"repo 조회 실패: {repo}")

    value = result.stdout.strip().lower()
    if value == "true":
        return True
    if value == "false":
        return False
    raise RuntimeError(f"repo visibility 응답 해석 실패: {value}")


def get_tracker_project_status_update() -> Optional[dict]:
    query = """
    query($projectId: ID!) {
      node(id: $projectId) {
        ... on ProjectV2 {
          statusUpdates(first: 20, orderBy: { field: CREATED_AT, direction: DESC }) {
            nodes {
              id
              status
              body
              updatedAt
            }
          }
        }
      }
    }
    """
    result = graphql_request(query, {"projectId": _project_id()})
    nodes = result.get("data", {}).get("node", {}).get("statusUpdates", {}).get("nodes", [])
    for node in nodes:
        body = node.get("body") or ""
        if PROJECT_STATUS_MARKER in body:
            return node
    return None


def is_tracker_board_off_track() -> bool:
    status_update = get_tracker_project_status_update()
    if not status_update:
        return False
    return status_update.get("status") == "OFF_TRACK"


# ─── 상태 파일 ───────────────────────────────────────────────────────────────

def get_state_path(session_id: str) -> Path:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    return STATE_DIR / f"{session_id}.json"


def load_state(session_id: str) -> Optional[dict]:
    try:
        return _read_json(get_state_path(session_id))
    except Exception:
        return None


def save_state(session_id: str, state: dict) -> None:
    _write_json(get_state_path(session_id), state)


def is_tracking_paused(state: Optional[dict]) -> bool:
    return bool(state and state.get("tracking_paused"))


# ─── GitHub Projects ─────────────────────────────────────────────────────────

def set_item_status(item_id: str, status_name: str) -> None:
    mutation = """
    mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $optionId: String!) {
      updateProjectV2ItemFieldValue(input: {
        projectId: $projectId
        itemId: $itemId
        fieldId: $fieldId
        value: { singleSelectOptionId: $optionId }
      }) {
        projectV2Item { id }
      }
    }
    """
    graphql_request(
        mutation,
        {
            "projectId": _project_id(),
            "itemId": item_id,
            "fieldId": _status_field_id(),
            "optionId": _status_option(status_name),
        },
    )


def set_item_date_field(item_id: str, field_id: str, date_str: str) -> None:
    mutation = """
    mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $date: Date!) {
      updateProjectV2ItemFieldValue(input: {
        projectId: $projectId
        itemId: $itemId
        fieldId: $fieldId
        value: { date: $date }
      }) {
        projectV2Item { id }
      }
    }
    """
    graphql_request(
        mutation,
        {
            "projectId": _project_id(),
            "itemId": item_id,
            "fieldId": field_id,
            "date": date_str,
        },
    )


def update_issue_title(repo: str, issue_number: int, title: str) -> None:
    _log = setup_logger("update-title")
    result = subprocess.run(
        [
            "gh",
            "api",
            f"repos/{repo}/issues/{issue_number}",
            "--method",
            "PATCH",
            "--field",
            f"title={title}",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    if result.returncode != 0:
        raise RuntimeError(f"제목 업데이트 실패: {result.stderr.strip()}")
    _log.debug(f"제목 업데이트 완료: {repo}#{issue_number} → {title}")


def add_issue_comment(repo: str, issue_number: int, body: str) -> None:
    _log = setup_logger("issue-comment")
    result = subprocess.run(
        ["gh", "issue", "comment", str(issue_number), "--repo", repo, "--body", body],
        capture_output=True,
        text=True,
        timeout=15,
    )
    if result.returncode != 0:
        raise RuntimeError(f"댓글 추가 실패: {result.stderr.strip()}")
    _log.debug(f"댓글 추가 완료: {repo}#{issue_number}")


def create_repo_issue_and_add_to_project(repo: str, title: str, body: str) -> Tuple[str, int]:
    ensure_label(repo, CLAUDE_CODE_LABEL)

    me = subprocess.run(
        ["gh", "api", "user", "--jq", ".login"],
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()

    result = subprocess.run(
        [
            "gh",
            "api",
            f"repos/{repo}/issues",
            "--method",
            "POST",
            "--field",
            f"title={title}",
            "--field",
            f"body={body}",
            "--field",
            f"labels[]={CLAUDE_CODE_LABEL}",
            *(["--field", f"assignees[]={me}"] if me else []),
            "--jq",
            ".node_id, .number",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Issue 생성 실패: {result.stderr.strip()}")

    lines = result.stdout.strip().splitlines()
    issue_node_id = lines[0].strip('"')
    issue_number = int(lines[1])

    mutation = """
    mutation($projectId: ID!, $contentId: ID!) {
      addProjectV2ItemById(input: {
        projectId: $projectId
        contentId: $contentId
      }) {
        item { id }
      }
    }
    """
    add_result = graphql_request(
        mutation,
        {"projectId": _project_id(), "contentId": issue_node_id},
    )
    item_id = add_result["data"]["addProjectV2ItemById"]["item"]["id"]
    return item_id, issue_number


# ─── Git / Label ─────────────────────────────────────────────────────────────

def get_git_repo(cwd: str) -> Optional[str]:
    """cwd의 git remote origin에서 'owner/repo' 반환. 없으면 None"""
    try:
        result = subprocess.run(
            ["git", "-C", cwd, "remote", "get-url", "origin"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return None
        url = result.stdout.strip().rstrip(".git")
        if "github.com" not in url:
            return None
        if url.startswith("https://"):
            parts = url.split("/")
            return f"{parts[-2]}/{parts[-1]}"
        if ":" in url:
            return url.split(":")[-1]
    except Exception:
        return None
    return None


def ensure_label(repo: str, label: str) -> None:
    result = subprocess.run(
        ["gh", "label", "list", "--repo", repo, "--search", label, "--json", "name"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode == 0:
        existing = [item["name"] for item in json.loads(result.stdout or "[]")]
        if label in existing:
            return
    subprocess.run(
        [
            "gh",
            "label",
            "create",
            label,
            "--repo",
            repo,
            "--color",
            "0075ca",
            "--description",
            "Claude Code session",
            "--force",
        ],
        capture_output=True,
        timeout=10,
    )


# ─── 상태 탐색 / 타이머 ──────────────────────────────────────────────────────

def cancel_timer(state: dict) -> None:
    pid = state.get("timer_pid")
    if not pid:
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        pass


def _normalize_cwd(cwd: str) -> str:
    try:
        return str(Path(cwd).expanduser().resolve())
    except Exception:
        return cwd


def is_resume(transcript_path: str) -> bool:
    try:
        with open(transcript_path, encoding="utf-8") as f:
            first = json.loads(f.readline())
        return first.get("type") == "file-history-snapshot"
    except Exception:
        return False


def find_active_state_by_cwd(cwd: str) -> Optional[Tuple[dict, str]]:
    if not STATE_DIR.exists():
        return None
    normalized_cwd = _normalize_cwd(cwd)
    candidates = []
    for file in STATE_DIR.glob("*.json"):
        try:
            with open(file, encoding="utf-8") as fp:
                state = json.load(fp)
            if _normalize_cwd(state.get("cwd", "")) == normalized_cwd and state.get("status") != "closed":
                candidates.append((file.stat().st_mtime, state, file.stem))
        except Exception:
            continue
    if not candidates:
        return None
    _, state, session_id = max(candidates)
    return state, session_id
