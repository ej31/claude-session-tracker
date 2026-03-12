#!/usr/bin/env python3
"""
Claude Code SessionStart hook
세션 시작 시 GitHub Projects에 item 생성 + "세션 등록 됨"
resume이면 기존 item을 찾아서 재활성화
"""
from __future__ import annotations

import json
import logging
import os
import socket
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from cst_github_utils import (
    _created_field_id,
    _notes_repo,
    clear_runtime_status,
    cancel_timer,
    create_repo_issue_and_add_to_project,
    find_active_state_by_cwd,
    get_git_repo,
    get_tracker_project_status_update,
    is_tracker_board_off_track,
    is_repo_private,
    is_resume,
    load_env_file,
    load_state,
    save_runtime_status,
    save_state,
    set_item_date_field,
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

    try:
        notes_repo = _notes_repo()
        if not is_repo_private(notes_repo):
            save_runtime_status({
                "status": "blocked",
                "reason": "notes_repo_public",
                "repo": notes_repo,
                "cwd": cwd,
                "checked_at": datetime.now().isoformat(),
            })
            logger.error(f"공개 저장소는 추적할 수 없음: {notes_repo}")
            print(
                f"Tracking is disabled because {notes_repo} is public. "
                "Please configure a private NOTES_REPO before starting a tracked session."
            )
            return 0
    except Exception as e:
        save_runtime_status({
            "status": "blocked",
            "reason": "notes_repo_check_failed",
            "repo": os.environ.get("NOTES_REPO", ""),
            "cwd": cwd,
            "checked_at": datetime.now().isoformat(),
            "error": str(e),
        })
        logger.error(f"NOTES_REPO 검사 실패: {e}")
        print(
            "Tracking is disabled because the configured NOTES_REPO could not be verified. "
            "Run `claude-session-tracker doctor` and fix the repository visibility check first."
        )
        return 0

    try:
        if is_tracker_board_off_track():
            status_update = get_tracker_project_status_update() or {}
            save_runtime_status({
                "status": "blocked",
                "reason": "project_off_track",
                "cwd": cwd,
                "checked_at": datetime.now().isoformat(),
                "status_update_id": status_update.get("id"),
            })
            logger.info("project board가 OFF_TRACK 이므로 세션 등록 생략")
            print(
                "Tracking is disabled because the configured project board is currently OFF_TRACK. "
                "Move it back to ON_TRACK before starting a tracked session."
            )
            return 0
    except Exception as e:
        logger.error(f"project status 확인 실패: {e}")

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
            old_repo = old_state.get("repo")
            old_issue = old_state.get("issue_number")
            if old_repo and old_issue:
                issue_url = f"https://github.com/{old_repo}/issues/{old_issue}"
                clear_runtime_status()
                print(
                    f"This session is being tracked at {issue_url} (resumed) — "
                    f"Please inform the user that this conversation is being recorded at this URL. "
                    f"Every token counts."
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

    git_repo = get_git_repo(cwd)  # 제목 prefix용 (이슈 생성 위치와 무관)
    item_id = None
    issue_number = None

    try:
        # 이슈는 항상 notes_repo에 생성, git_repo는 제목 prefix에만 사용
        if git_repo:
            logger.info(f"GitHub repo 감지: {git_repo} → {notes_repo}에 Issue 생성")
        else:
            logger.info(f"git remote 없음 → {notes_repo}에 Issue 생성")
        item_id, issue_number = create_repo_issue_and_add_to_project(
            notes_repo, title, body
        )

        set_item_status(item_id, "registered")

        save_state(session_id, {
            "session_id": session_id,
            "item_id": item_id,
            "cwd": cwd,
            "repo": notes_repo,
            "issue_number": issue_number,
            "status": "registered",
            "created_at": datetime.now().isoformat(),
        })
        logger.info(f"item 생성 완료: {item_id} repo={notes_repo}")

        # Created 날짜 설정
        created_fid = _created_field_id()
        if created_fid and item_id:
            try:
                today = datetime.now().strftime("%Y-%m-%d")
                set_item_date_field(item_id, created_fid, today)
                logger.info(f"Created 필드 설정: {today}")
            except Exception as e:
                logger.error(f"Created 필드 설정 실패: {e}")

        # 사용자에게 이슈 URL 안내 (stdout → Claude가 system-reminder로 수신)
        issue_url = f"https://github.com/{notes_repo}/issues/{issue_number}"
        clear_runtime_status()
        print(
            f"This session is being tracked at {issue_url} — "
            f"Please inform the user that this conversation is being recorded at this URL. "
            f"Every token counts."
        )
    except Exception as e:
        logger.error(f"item 생성 실패: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
