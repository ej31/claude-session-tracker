#!/usr/bin/env python3
"""
Claude Code Stop hook
응답 완료 시 "프롬프트 입력 대기"로 변경 + 30분 타이머 시작
30분 내 새 입력 없으면 mark_done.py가 "세션 종료"로 변경
"""
from __future__ import annotations

import json
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from cst_github_utils import (
    _comment_labels,
    _last_active_field_id,
    add_issue_comment,
    cancel_timer,
    clear_runtime_status,
    get_tracker_project_status_update,
    is_tracker_board_inactive,
    is_tracking_paused,
    load_env_file,
    load_state,
    save_runtime_status,
    save_state,
    set_item_date_field,
    set_item_status,
    setup_logger,
)

logger = setup_logger("session-stop")

MARK_DONE_SCRIPT = str(Path(__file__).parent / "cst_mark_done.py")


def main() -> int:
    load_env_file()

    try:
        input_data = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        logger.error(f"stdin JSON 파싱 실패: {e}")
        return 0

    session_id = input_data.get("session_id", "")
    if not session_id:
        return 0

    state = load_state(session_id)
    if not state:
        return 0

    item_id = state.get("item_id")
    if not item_id:
        return 0

    if is_tracking_paused(state):
        logger.info(f"tracking paused → 응답 후처리 생략: {session_id[:8]}…")
        cancel_timer(state)
        state.pop("timer_pid", None)
        save_state(session_id, state)
        return 0

    try:
        if is_tracker_board_inactive():
            status_update = get_tracker_project_status_update() or {}
            save_runtime_status({
                "status": "blocked",
                "reason": "project_inactive",
                "cwd": state.get("cwd", ""),
                "checked_at": datetime.now().isoformat(),
                "status_update_id": status_update.get("id"),
            })
            cancel_timer(state)
            state.pop("timer_pid", None)
            save_state(session_id, state)
            logger.info(f"project board INACTIVE → 응답 후처리 생략: {session_id[:8]}…")
            return 0
        clear_runtime_status()
    except Exception as e:
        logger.error(f"project status 확인 실패: {e}")

    try:
        # 기존 타이머 취소
        cancel_timer(state)

        # "프롬프트 입력 대기"로 변경
        set_item_status(item_id, "waiting")
        state["status"] = "waiting"

        # 30분 후 "세션 종료" 타이머 시작
        proc = subprocess.Popen(
            ["python3", MARK_DONE_SCRIPT, session_id],
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        state["timer_pid"] = proc.pid
        save_state(session_id, state)
        logger.info(
            f"상태 변경 → 프롬프트 입력 대기 | "
            f"타이머 시작: pid={proc.pid} session={session_id[:8]}…"
        )
    except Exception as e:
        logger.error(f"상태 변경 실패: {e}")

    # Last Active 날짜 갱신
    last_active_fid = _last_active_field_id()
    if last_active_fid and item_id:
        try:
            from datetime import datetime
            today = datetime.now().strftime("%Y-%m-%d")
            set_item_date_field(item_id, last_active_fid, today)
        except Exception as e:
            logger.error(f"Last Active 필드 갱신 실패: {e}")

    # 답변을 이슈 댓글로 저장
    last_message = input_data.get("last_assistant_message", "").strip()
    repo = state.get("repo")
    issue_number = state.get("issue_number")
    if repo and issue_number and last_message:
        try:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            label = _comment_labels()["response"]
            comment_body = f"**[{timestamp}] {label}**\n\n{last_message}"
            add_issue_comment(repo, issue_number, comment_body)
            logger.info(f"답변 댓글 저장: {repo}#{issue_number}")
        except Exception as e:
            logger.error(f"답변 댓글 저장 실패: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
