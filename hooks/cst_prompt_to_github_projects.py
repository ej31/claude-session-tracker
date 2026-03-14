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
from cst_github_utils import (
    _comment_labels,
    _last_active_field_id,
    _project_url,
    add_issue_comment,
    cancel_timer,
    clear_runtime_status,
    get_git_repo,
    get_tracker_project_status_update,
    is_tracker_board_off_track,
    is_tracking_paused,
    save_runtime_status,
    load_env_file,
    load_state,
    save_state,
    set_item_date_field,
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

    # SessionStart(async)가 아직 완료되지 않았을 수 있으므로 재시도
    import time

    state = None
    max_retries = 5
    for attempt in range(max_retries):
        state = load_state(session_id)
        if state:
            break
        if attempt < max_retries - 1:
            logger.info(f"상태 파일 대기 중: {session_id[:8]}… (시도 {attempt + 1}/{max_retries})")
            time.sleep(1)

    if not state:
        logger.warning(f"상태 파일 없음: {session_id[:8]}… (SessionStart hook 확인 필요)")
        return 0

    item_id = state.get("item_id")
    if not item_id:
        return 0

    if is_tracking_paused(state):
        logger.info(f"tracking paused → 프롬프트 처리 생략: {session_id[:8]}…")
        return 0

    try:
        if is_tracker_board_off_track():
            status_update = get_tracker_project_status_update() or {}
            save_runtime_status({
                "status": "blocked",
                "reason": "project_off_track",
                "cwd": state.get("cwd", ""),
                "checked_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "status_update_id": status_update.get("id"),
            })
            cancel_timer(state)
            state.pop("timer_pid", None)
            save_state(session_id, state)
            logger.info(f"project board OFF_TRACK → 프롬프트 처리 생략: {session_id[:8]}…")
            return 0
        clear_runtime_status()
    except Exception as e:
        logger.error(f"project status 확인 실패: {e}")

    # 첫 프롬프트 시 GitHub Project URL 안내
    if not state.get("first_prompt_notified"):
        project_url = _project_url()
        if project_url:
            print(
                f"현재 이 대화는 {project_url} 에 기록되고 있습니다. "
                f"Please inform the user of this tracking URL naturally at the start of your response."
            )
        state["first_prompt_notified"] = True
        save_state(session_id, state)

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

    # Last Active 날짜 갱신
    last_active_fid = _last_active_field_id()
    if last_active_fid and item_id:
        try:
            from datetime import datetime
            today = datetime.now().strftime("%Y-%m-%d")
            set_item_date_field(item_id, last_active_fid, today)
        except Exception as e:
            logger.error(f"Last Active 필드 갱신 실패: {e}")

    # 최신 프롬프트를 이슈 제목으로 업데이트
    repo = state.get("repo")
    issue_number = state.get("issue_number")
    if repo and issue_number and prompt_text:
        try:
            cwd = state.get("cwd", "")
            git_repo = get_git_repo(cwd) if cwd else None
            if git_repo:
                # GitHub repo인 경우: [owner/repo] 형식
                prefix = git_repo
            else:
                # git repo가 아닌 경우: 폴더 이름
                prefix = Path(cwd).name
            if len(prefix) > 30:
                prefix = prefix[:30] + "..."
            title = f"[{prefix}] {prompt_text}"[:80]
            update_issue_title(repo, issue_number, title)
            logger.info(f"이슈 제목 업데이트: {title}")
        except Exception as e:
            logger.error(f"이슈 제목 업데이트 실패: {e}")

    # 프롬프트를 이슈 댓글로 저장
    if repo and issue_number and prompt_text:
        try:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            label = _comment_labels()["prompt"]
            comment_body = f"**[{timestamp}] {label}**\n\n{prompt_text}"
            add_issue_comment(repo, issue_number, comment_body)
            logger.info(f"프롬프트 댓글 저장: {repo}#{issue_number}")
        except Exception as e:
            logger.error(f"프롬프트 댓글 저장 실패: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
