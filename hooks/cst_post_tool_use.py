#!/usr/bin/env python3
"""
Claude Code PostToolUse hook
AskUserQuestion 도구 응답을 이슈 댓글로 저장
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from cst_github_utils import (
    add_issue_comment,
    load_env_file,
    load_state,
    setup_logger,
)

logger = setup_logger("post-tool-use")


def main() -> int:
    load_env_file()

    try:
        input_data = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        logger.error(f"stdin JSON 파싱 실패: {e}")
        return 0

    # AskUserQuestion 도구만 처리
    if input_data.get("tool_name") != "AskUserQuestion":
        return 0

    session_id = input_data.get("session_id", "")
    state = load_state(session_id)
    if not state:
        return 0

    repo = state.get("repo")
    issue_number = state.get("issue_number")
    if not repo or not issue_number:
        return 0

    # tool_response에서 선택값 추출
    tool_response = input_data.get("tool_response", {})
    answers = tool_response.get("answers", {})
    if not answers:
        return 0

    # 질문 목록 (tool_input에서)
    questions = input_data.get("tool_input", {}).get("questions", [])
    question_map = {q["question"]: q for q in questions}

    lines = []
    for question, answer in answers.items():
        q_info = question_map.get(question, {})
        # 선택한 옵션의 description 찾기
        description = ""
        for opt in q_info.get("options", []):
            if opt.get("label") == answer:
                description = opt.get("description", "")
                break
        if description:
            lines.append(f"- **{question}** → {answer} _({description})_")
        else:
            lines.append(f"- **{question}** → {answer}")

    if not lines:
        return 0

    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        comment_body = f"**[{timestamp}] 선택**\n\n" + "\n".join(lines)
        add_issue_comment(repo, issue_number, comment_body)
        logger.info(f"선택 댓글 저장: {repo}#{issue_number} answers={answers}")
    except Exception as e:
        logger.error(f"선택 댓글 저장 실패: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
