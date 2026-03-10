#!/usr/bin/env python3
"""Context OS - PostToolUse hook

Edit/Write 도구 사용 시 수정된 심볼을 추적하여
Turn → Symbol MODIFIED_BY 관계를 생성한다.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from build_context_os import get_db_connection, get_or_create_session, setup_logger

logger = setup_logger("context-os-edit")


def _find_affected_symbols(conn, file_path: str) -> list:
    """해당 파일에 속한 모든 심볼 조회"""
    result = conn.execute(
        "MATCH (s:Symbol) WHERE s.file_path = $fp "
        "RETURN s.id, s.name",
        parameters={"fp": file_path},
    )
    symbols = []
    while result.has_next():
        row = result.get_next()
        symbols.append({"id": row[0], "name": row[1]})
    return symbols


def _resolve_relative_path(file_path: str, cwd: str) -> str:
    """절대 경로를 cwd 기준 상대 경로로 변환"""
    if cwd and file_path.startswith(cwd):
        return file_path[len(cwd):].lstrip("/")
    return Path(file_path).name


def main() -> int:
    try:
        input_data = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        logger.error(f"stdin JSON 파싱 실패: {e}")
        return 0

    tool_name = input_data.get("tool_name", "")
    if tool_name not in ("Edit", "Write"):
        return 0

    session_id = input_data.get("session_id", "")
    tool_input = input_data.get("tool_input", {})
    file_path = tool_input.get("file_path", "")
    if not session_id or not file_path:
        return 0

    cwd = input_data.get("cwd", "")

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

    # 절대 경로 → 상대 경로 변환
    rel_path = _resolve_relative_path(file_path, cwd)

    # 해당 파일의 심볼 조회
    affected = _find_affected_symbols(conn, rel_path)
    if not affected:
        logger.debug(f"수정 파일에 매칭되는 심볼 없음: {rel_path}")
        return 0

    # Turn 노드 생성 (edit 타입)
    timestamp = datetime.now().isoformat()
    turn_id = f"{session_id}:{timestamp}"

    conn.execute(
        "MERGE (t:Turn {id: $id}) SET t.session_id = $sid, t.timestamp = $ts, "
        "t.type = $type, t.summary = $summary",
        parameters={
            "id": turn_id,
            "sid": session_id,
            "ts": timestamp,
            "type": "edit",
            "summary": f"{tool_name}: {rel_path}",
        },
    )

    # Session → HAS_TURN 관계
    conn.execute(
        "MATCH (s:Session {id: $sid}), (t:Turn {id: $tid}) "
        "MERGE (s)-[:HAS_TURN]->(t)",
        parameters={"sid": session_id, "tid": turn_id},
    )

    # MODIFIED_BY 관계 생성
    for sym in affected:
        conn.execute(
            "MATCH (t:Turn {id: $tid}), (s:Symbol {id: $sid}) "
            "MERGE (t)-[:MODIFIED_BY]->(s)",
            parameters={"tid": turn_id, "sid": sym["id"]},
        )

    logger.info(
        f"Edit 추적 완료: {rel_path}, "
        f"영향받은 심볼 {len(affected)}개"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
