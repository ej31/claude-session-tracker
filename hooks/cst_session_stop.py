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
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from cst_github_utils import (
    add_issue_comment,
    cancel_timer,
    load_env_file,
    load_state,
    save_state,
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

    # 답변을 이슈 댓글로 저장
    last_message = input_data.get("last_assistant_message", "").strip()
    repo = state.get("repo")
    issue_number = state.get("issue_number")
    if repo and issue_number and last_message:
        try:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            comment_body = f"**[{timestamp}] 답변**\n\n{last_message}"
            add_issue_comment(repo, issue_number, comment_body)
            logger.info(f"답변 댓글 저장: {repo}#{issue_number}")
        except Exception as e:
            logger.error(f"답변 댓글 저장 실패: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
