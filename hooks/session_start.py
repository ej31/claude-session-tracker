#!/usr/bin/env python3
"""
Claude Code SessionStart hook
세션 시작 시 GitHub Projects에 item 생성 + "세션 등록 됨"
resume이면 기존 item을 찾아서 재활성화
"""
from __future__ import annotations

import json
import logging
import socket
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from github_utils import (
    _notes_repo,
    cancel_timer,
    create_repo_issue_and_add_to_project,
    find_active_state_by_cwd,
    get_git_repo,
    is_resume,
    load_env_file,
    load_state,
    save_state,
    set_item_status,
    setup_logger,
)

logger = setup_logger("session-start")


def get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "unknown"


def main() -> int:
    load_env_file()

    try:
        input_data = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        logger.error(f"stdin JSON 파싱 실패: {e}")
        return 0

    session_id = input_data.get("session_id", "")
    cwd = input_data.get("cwd", "")
    transcript_path = input_data.get("transcript_path", "")

    if not session_id:
        return 0

    # 이미 이 session_id로 상태 파일이 있으면 skip
    if load_state(session_id):
        logger.info(f"이미 등록된 세션: {session_id[:8]}…")
        return 0

    # resume 감지: 기존 item 재활성화
    if is_resume(transcript_path):
        result = find_active_state_by_cwd(cwd)
        if result:
            old_state, old_session_id = result
            cancel_timer(old_state)
            old_state["session_id"] = session_id
            old_state.pop("timer_pid", None)
            save_state(session_id, old_state)
            set_item_status(old_state["item_id"], "registered")
            logger.info(
                f"Resume: 기존 item 재활성화 {old_state['item_id']} "
                f"(이전 세션: {old_session_id[:8]}…)"
            )
            return 0
        logger.info("Resume이지만 기존 item 없음 → 새 item 생성")

    # 신규 item 생성
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    local_ip = get_local_ip()
    title = f"[{timestamp}] Claude Code 세션 — {Path(cwd).name}"
    body = (
        f"**세션 ID:** `{session_id}`  \n"
        f"**시각:** {timestamp}  \n"
        f"**IP:** `{local_ip}`  \n"
        f"**작업 디렉토리:** `{cwd}`  \n"
        f"**Transcript:** `{transcript_path}`  \n"
    )

    repo = get_git_repo(cwd)
    item_id = None
    issue_number = None

    try:
        notes_repo = _notes_repo()
        target_repo = repo if repo else notes_repo
        if not repo:
            logger.info(f"git remote 없음 → {notes_repo}에 Issue 생성")
        else:
            logger.info(f"GitHub repo 감지: {repo} → Issue 생성")
        item_id, issue_number = create_repo_issue_and_add_to_project(
            target_repo, title, body
        )
        if not repo:
            repo = notes_repo

        set_item_status(item_id, "registered")

        save_state(session_id, {
            "session_id": session_id,
            "item_id": item_id,
            "cwd": cwd,
            "repo": repo,
            "issue_number": issue_number,
            "status": "registered",
            "created_at": datetime.now().isoformat(),
        })
        logger.info(f"item 생성 완료: {item_id} repo={repo}")
    except Exception as e:
        logger.error(f"item 생성 실패: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
