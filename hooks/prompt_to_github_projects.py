#!/usr/bin/env python3
"""
Claude Code UserPromptSubmit hook
프롬프트 수신 시 item 상태를 "답변 중"으로 변경
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from github_utils import (
    add_issue_comment,
    cancel_timer,
    load_env_file,
    load_state,
    save_state,
    set_item_status,
    setup_logger,
    update_issue_title,
)

logger = setup_logger("prompt-to-github")


def main() -> int:
    load_env_file()

    try:
        input_data = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        logger.error(f"stdin JSON 파싱 실패: {e}")
        return 0

    session_id = input_data.get("session_id", "unknown")
    prompt_text = input_data.get("prompt", "").strip()

    state = load_state(session_id)
    if not state:
        logger.warning(f"상태 파일 없음: {session_id[:8]}… (SessionStart hook 확인 필요)")
        return 0

    item_id = state.get("item_id")
    if not item_id:
        return 0

    try:
        # 예약된 타이머 취소 (아직 응답 처리 중)
        cancel_timer(state)
        state.pop("timer_pid", None)

        set_item_status(item_id, "responding")
        state["status"] = "responding"
        save_state(session_id, state)
        logger.info(f"상태 변경 → 답변 중: {item_id}")
    except Exception as e:
        logger.error(f"상태 변경 실패: {e}")

    # 최신 프롬프트를 이슈 제목으로 업데이트
    repo = state.get("repo")
    issue_number = state.get("issue_number")
    if repo and issue_number and prompt_text:
        try:
            project_name = Path(state.get("cwd", "")).name
            if len(project_name) > 20:
                project_name = project_name[:20] + "..."
            title = f"[{project_name}] {prompt_text}"[:80]
            update_issue_title(repo, issue_number, title)
            logger.info(f"이슈 제목 업데이트: {title}")
        except Exception as e:
            logger.error(f"이슈 제목 업데이트 실패: {e}")

    # 프롬프트를 이슈 댓글로 저장
    if repo and issue_number and prompt_text:
        try:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            comment_body = f"**[{timestamp}] 프롬프트**\n\n{prompt_text}"
            add_issue_comment(repo, issue_number, comment_body)
            logger.info(f"프롬프트 댓글 저장: {repo}#{issue_number}")
        except Exception as e:
            logger.error(f"프롬프트 댓글 저장 실패: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
