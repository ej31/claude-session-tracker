#!/usr/bin/env python3
"""Context OS - Stop hook

Claude Code 응답 완료 시 Turn 데이터를 Kùzu에 적재한다.
응답 텍스트에서 known_symbols를 매칭하여 ABOUT 관계를 생성한다.
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from build_context_os import get_db_connection, get_or_create_session, setup_logger

logger = setup_logger("context-os-turn")

MIN_SYMBOL_NAME_LENGTH = 3


def _load_known_names(conn) -> set:
    """DB에서 모든 심볼 이름 조회"""
    result = conn.execute("MATCH (s:Symbol) RETURN DISTINCT s.name")
    names = set()
    while result.has_next():
        names.add(result.get_next()[0])
    return names


def _find_mentioned_symbols(text: str, known_names: set) -> list:
    """텍스트에서 언급된 심볼 이름 탐색 (단어 경계 매칭)"""
    mentioned = []
    for name in known_names:
        if len(name) < MIN_SYMBOL_NAME_LENGTH:
            continue
        pattern = rf"\b{re.escape(name)}\b"
        if re.search(pattern, text):
            mentioned.append(name)
    return mentioned


def _extract_code_blocks(text: str) -> list:
    """마크다운 코드 블록 내용 추출"""
    return re.findall(r"```[\w]*\n(.*?)```", text, re.DOTALL)


def _create_about_relations(conn, turn_id: str, symbol_names: list) -> int:
    """Turn → Symbol ABOUT 관계 생성. 생성 수 반환"""
    count = 0
    for name in symbol_names:
        result = conn.execute(
            "MATCH (s:Symbol {name: $name}) RETURN s.id",
            parameters={"name": name},
        )
        while result.has_next():
            sid = result.get_next()[0]
            conn.execute(
                "MATCH (t:Turn {id: $tid}), (s:Symbol {id: $sid}) "
                "MERGE (t)-[:ABOUT]->(s)",
                parameters={"tid": turn_id, "sid": sid},
            )
            count += 1
    return count


def main() -> int:
    try:
        input_data = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        logger.error(f"stdin JSON 파싱 실패: {e}")
        return 0

    session_id = input_data.get("session_id", "")
    last_message = input_data.get("last_assistant_message", "")
    cwd = input_data.get("cwd", "")
    if not session_id or not last_message:
        return 0

    try:
        db, conn = get_db_connection()
    except FileNotFoundError:
        logger.debug("Context OS DB 없음, 건너뜀")
        return 0
    except Exception as e:
        logger.error(f"DB 연결 실패: {e}")
        return 0

    # Session 노드 생성/조회 + Repository 연결
    get_or_create_session(conn, session_id, cwd)

    # Turn 노드 생성
    timestamp = datetime.now().isoformat()
    turn_id = f"{session_id}:{timestamp}"
    summary = last_message[:200].replace("\n", " ")

    conn.execute(
        "MERGE (t:Turn {id: $id}) SET t.session_id = $sid, t.timestamp = $ts, "
        "t.type = $type, t.summary = $summary",
        parameters={
            "id": turn_id,
            "sid": session_id,
            "ts": timestamp,
            "type": "response",
            "summary": summary,
        },
    )

    # Session → HAS_TURN 관계
    conn.execute(
        "MATCH (s:Session {id: $sid}), (t:Turn {id: $tid}) "
        "MERGE (s)-[:HAS_TURN]->(t)",
        parameters={"sid": session_id, "tid": turn_id},
    )

    # 전략 1: 응답 텍스트에서 심볼 이름 매칭
    known_names = _load_known_names(conn)
    mentioned = _find_mentioned_symbols(last_message, known_names)

    # 전략 2: 코드 블록 내에서도 심볼 탐색
    for block in _extract_code_blocks(last_message):
        block_mentioned = _find_mentioned_symbols(block, known_names)
        mentioned.extend(block_mentioned)

    # 중복 제거 후 ABOUT 관계 생성
    unique_mentioned = list(set(mentioned))
    about_count = _create_about_relations(conn, turn_id, unique_mentioned)

    logger.info(
        f"Turn 적재 완료: {turn_id[:40]}…, "
        f"ABOUT 관계 {about_count}개"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
