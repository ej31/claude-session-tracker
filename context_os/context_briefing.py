#!/usr/bin/env python3
"""Context OS - SessionStart(compact) hook

compact 발생 시 구조화된 맥락 브리핑을 생성하여 stdout으로 출력한다.
Claude Code가 system-reminder로 수신하여 맥락을 복원한다.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from build_context_os import get_db_connection, setup_logger

logger = setup_logger("context-os-briefing")

MAX_RECENT_TURNS = 10
MAX_SYMBOLS = 20
MAX_DEPS_DISPLAY = 5
MAX_COMMITS_DISPLAY = 1
MAX_CHAIN_DISPLAY = 3


def _get_recent_turns(conn, session_id: str) -> list:
    """세션의 최근 Turn 조회 (Session → HAS_TURN 경유)"""
    result = conn.execute(
        "MATCH (s:Session {id: $sid})-[:HAS_TURN]->(t:Turn) "
        "RETURN t.id, t.timestamp, t.type, t.summary "
        "ORDER BY t.timestamp DESC LIMIT 10",
        parameters={"sid": session_id},
    )
    turns = []
    while result.has_next():
        row = result.get_next()
        turns.append({
            "id": row[0], "timestamp": row[1],
            "type": row[2], "summary": row[3],
        })
    return turns


def _get_session_symbols(conn, session_id: str) -> list:
    """세션에서 다룬 심볼 조회 (Session → HAS_TURN → Turn → ABOUT/MODIFIED_BY 경유)"""
    result = conn.execute(
        "MATCH (ses:Session {id: $sid})-[:HAS_TURN]->(t:Turn)"
        "-[:ABOUT|MODIFIED_BY]->(s:Symbol) "
        "RETURN DISTINCT s.id, s.name, s.type, s.file_path, s.intent, s.start_line "
        "LIMIT 20",
        parameters={"sid": session_id},
    )
    symbols = []
    while result.has_next():
        row = result.get_next()
        symbols.append({
            "id": row[0], "name": row[1], "type": row[2],
            "file_path": row[3], "intent": row[4], "start_line": row[5],
        })
    return symbols


def _get_symbol_calls(conn, symbol_id: str) -> list:
    """심볼의 호출 대상 조회"""
    result = conn.execute(
        "MATCH (s:Symbol {id: $sid})-[:CALLS]->(dep:Symbol) "
        "RETURN dep.name, dep.file_path",
        parameters={"sid": symbol_id},
    )
    deps = []
    while result.has_next():
        row = result.get_next()
        deps.append({"name": row[0], "file_path": row[1]})
    return deps


def _get_recent_commits_for_symbol(conn, symbol_id: str) -> list:
    """심볼을 수정한 최근 커밋 조회"""
    result = conn.execute(
        "MATCH (c:Commit)-[:MODIFIES]->(s:Symbol {id: $sid}) "
        "RETURN c.hash, c.message "
        "ORDER BY c.date DESC LIMIT 3",
        parameters={"sid": symbol_id},
    )
    commits = []
    while result.has_next():
        row = result.get_next()
        commits.append({"hash": row[0], "message": row[1]})
    return commits


def _format_symbol_section(symbols: list, conn) -> list:
    """심볼 섹션 포맷팅"""
    lines = ["## 최근 작업 심볼"]
    for sym in symbols:
        location = f"{sym['file_path']}:{sym['start_line']}"
        intent_part = f" — {sym['intent']}" if sym.get("intent") else ""
        lines.append(f"- {sym['name']}() [{location}]{intent_part}")

        # 호출 대상
        calls = _get_symbol_calls(conn, sym["id"])
        if calls:
            call_names = ", ".join(
                c["name"] + "()" for c in calls[:MAX_DEPS_DISPLAY]
            )
            lines.append(f"  - 호출 대상: {call_names}")

        # 최근 커밋
        commits = _get_recent_commits_for_symbol(conn, sym["id"])
        for c in commits[:MAX_COMMITS_DISPLAY]:
            lines.append(f"  - 최근 수정: commit {c['hash']} \"{c['message']}\"")

    return lines


def _format_turns_section(turns: list) -> list:
    """Turn 내역 섹션 포맷팅"""
    type_labels = {"response": "응답", "edit": "수정"}
    lines = ["## 최근 작업 내역"]
    for i, turn in enumerate(turns[:5], 1):
        label = type_labels.get(turn["type"], turn["type"])
        lines.append(f"- Turn #{i} [{label}]: {turn['summary']}")
    return lines


def _format_dependency_section(symbols: list, conn) -> list:
    """의존성 체인 섹션 포맷팅"""
    lines = ["## 의존성 체인"]
    for sym in symbols[:MAX_CHAIN_DISPLAY]:
        calls = _get_symbol_calls(conn, sym["id"])
        if not calls:
            continue
        chain = f"{sym['name']}()"
        for dep in calls[:3]:
            chain += f" → {dep['name']}()"
        lines.append(f"- {chain}")
    return lines


def _get_session_info(conn, session_id: str) -> dict:
    """Session 메타데이터 조회 (branch, cwd, repo)"""
    result = conn.execute(
        "MATCH (s:Session {id: $sid}) "
        "RETURN s.cwd, s.branch",
        parameters={"sid": session_id},
    )
    if result.has_next():
        row = result.get_next()
        return {"cwd": row[0], "branch": row[1]}
    return {}


def format_briefing(
    session_id: str, turns: list, symbols: list, conn,
) -> str:
    """구조화된 브리핑 텍스트 생성"""
    session_info = _get_session_info(conn, session_id)
    branch = session_info.get("branch", "")

    lines = ["[Context OS 브리핑] compact 후 맥락 복원", ""]

    if branch:
        lines.append(f"**Branch**: `{branch}`")
        lines.append("")

    if symbols:
        lines.extend(_format_symbol_section(symbols, conn))
        lines.append("")

    if turns:
        lines.extend(_format_turns_section(turns))
        lines.append("")

    if symbols:
        dep_lines = _format_dependency_section(symbols, conn)
        if len(dep_lines) > 1:  # 헤더만 있는 경우 제외
            lines.extend(dep_lines)
            lines.append("")

    if not symbols and not turns:
        lines.append("(이 세션에 대한 맥락 데이터가 없습니다)")

    return "\n".join(lines)


def main() -> int:
    try:
        input_data = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        logger.error(f"stdin JSON 파싱 실패: {e}")
        return 0

    session_id = input_data.get("session_id", "")
    if not session_id:
        logger.debug("session_id 없음, 건너뜀")
        return 0

    try:
        db, conn = get_db_connection()
    except FileNotFoundError:
        logger.debug("Context OS DB 없음, 건너뜀")
        return 0
    except Exception as e:
        logger.error(f"DB 연결 실패: {e}")
        return 0

    turns = _get_recent_turns(conn, session_id)
    symbols = _get_session_symbols(conn, session_id)

    briefing = format_briefing(session_id, turns, symbols, conn)
    print(briefing)

    logger.info(
        f"브리핑 생성 완료: session={session_id[:8]}…, "
        f"심볼 {len(symbols)}개, Turn {len(turns)}개"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
