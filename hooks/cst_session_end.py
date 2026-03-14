#!/usr/bin/env python3
"""
Claude Code SessionEnd hook
세션이 완전히 종료될 때 실행됨
타이머 취소 + GitHub Projects status를 "closed"로 변경 + Issue close
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from cst_github_utils import (
    cancel_timer,
    close_issue,
    is_tracking_paused,
    load_env_file,
    load_state,
    save_state,
    set_item_status,
    setup_logger,
)

logger = setup_logger("session-end")


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

    # 대기 중인 타이머가 있으면 취소 (세션이 이미 종료되므로 불필요)
    cancel_timer(state)
    state.pop("timer_pid", None)

    # 이미 closed 상태면 중복 처리 방지
    if state.get("status") == "closed":
        save_state(session_id, state)
        return 0

    if is_tracking_paused(state):
        logger.info(f"tracking paused → 세션 종료 처리 생략: {session_id[:8]}…")
        save_state(session_id, state)
        return 0

    # GitHub Projects status를 "closed"로 변경
    try:
        set_item_status(item_id, "closed")
        state["status"] = "closed"
        logger.info(f"세션 종료 → status closed: item={item_id} session={session_id[:8]}…")
    except Exception as e:
        logger.error(f"status 변경 실패: {e}")

    # GitHub Issue close
    repo = state.get("repo")
    issue_number = state.get("issue_number")
    if repo and issue_number:
        try:
            close_issue(repo, issue_number)
            logger.info(f"Issue close 완료: {repo}#{issue_number}")
        except Exception as e:
            logger.error(f"Issue close 실패: {e}")

    save_state(session_id, state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
