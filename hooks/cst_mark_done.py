#!/usr/bin/env python3
"""
백그라운드 타이머 프로세스
DONE_TIMEOUT_SECS 후 GitHub Projects item을 "세션 종료"로 변경
session_stop.py가 Popen으로 실행하며, 새 입력이 오면 SIGTERM으로 취소됨
"""
from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from cst_github_utils import (
    _done_timeout,
    check_project_board_status,
    close_issue,
    is_tracking_paused,
    load_env_file,
    load_state,
    save_state,
    set_item_status,
)

LOG_FILE = Path("~/.claude/hooks/mark_done.log").expanduser()
# 로그 디렉토리가 없거나 쓸 수 없어도 타이머 프로세스가 죽으면 안 되므로
# 파일 로깅 실패 시 stderr 전용으로 폴백한다
try:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s [mark_done] %(levelname)s: %(message)s",
    )
except OSError:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [mark_done] %(levelname)s: %(message)s",
    )
logger = logging.getLogger(__name__)


def main() -> int:
    if len(sys.argv) < 2:
        logger.error("인자 없음: session_id 필요")
        return 1

    session_id = sys.argv[1]
    load_env_file()
    timeout_secs = _done_timeout()
    logger.info(f"타이머 시작: {session_id[:8]}… ({timeout_secs}초 후 세션 종료)")

    time.sleep(timeout_secs)

    state = load_state(session_id)
    if not state:
        logger.info(f"상태 파일 없음, 종료: {session_id[:8]}…")
        return 0

    item_id = state.get("item_id")
    if not item_id:
        return 0

    if is_tracking_paused(state):
        logger.info(f"paused 상태라 자동 종료 생략: {session_id[:8]}…")
        state.pop("timer_pid", None)
        save_state(session_id, state)
        return 0

    board_reason = check_project_board_status(state.get("cwd", ""), logger)
    if board_reason:
        state.pop("timer_pid", None)
        save_state(session_id, state)
        logger.info(f"project board {board_reason} → 자동 종료 생략: {session_id[:8]}…")
        return 0

    try:
        set_item_status(item_id, "closed")
        state["status"] = "closed"
        state.pop("timer_pid", None)
        save_state(session_id, state)
        logger.info(f"세션 종료 처리 완료: item={item_id} session={session_id[:8]}…")
    except Exception as e:
        logger.error(f"세션 종료 처리 실패: {e}")

    # GitHub Issue close
    repo = state.get("repo")
    issue_number = state.get("issue_number")
    if repo and issue_number:
        try:
            close_issue(repo, issue_number)
            logger.info(f"Issue close 완료: {repo}#{issue_number}")
        except Exception as e:
            logger.error(f"Issue close 실패: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
