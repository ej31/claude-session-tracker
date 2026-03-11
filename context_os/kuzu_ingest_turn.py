#!/usr/bin/env python3
"""Context OS - Stop hook.

Only explicit code mentions are promoted to ABOUT relations, and only when the
name resolves to exactly one active symbol in the current worktree scope.
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from build_context_os import (
    ensure_scope_current,
    get_active_symbol_index,
    get_db_connection,
    get_or_create_session,
    setup_logger,
    scope_lock,
)

logger = setup_logger("context-os-turn")

IDENT_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\b")
CALL_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")


def _load_known_names(conn) -> set:
    """Return currently active symbol names."""
    return set(get_active_symbol_index(conn).keys())


def _find_mentioned_symbols(text: str, known_names: set) -> list:
    """Return only explicit function-like mentions such as greet()."""
    mentioned = []
    for name in CALL_RE.findall(text):
        if name in known_names:
            mentioned.append(name)
    return mentioned


def _extract_code_blocks(text: str) -> list:
    return re.findall(r"```[\w]*\n(.*?)```", text, re.DOTALL)


def _extract_inline_code(text: str) -> list:
    return re.findall(r"`([^`\n]+)`", text)


def _collect_explicit_candidates(text: str) -> list:
    candidates = []

    for name in CALL_RE.findall(text):
        candidates.append(name)

    for snippet in _extract_inline_code(text):
        candidates.extend(IDENT_RE.findall(snippet))
        candidates.extend(CALL_RE.findall(snippet))

    for block in _extract_code_blocks(text):
        candidates.extend(IDENT_RE.findall(block))
        candidates.extend(CALL_RE.findall(block))

    # preserve order, remove duplicates
    seen = set()
    ordered = []
    for name in candidates:
        if name in seen:
            continue
        seen.add(name)
        ordered.append(name)
    return ordered


def _create_about_relations(conn, turn_id: str, symbol_ids: list) -> int:
    count = 0
    for symbol_id in symbol_ids:
        conn.execute(
            "MATCH (t:Turn {id: $tid}), (s:Symbol {id: $sid}) "
            "MERGE (t)-[:ABOUT]->(s)",
            parameters={"tid": turn_id, "sid": symbol_id},
        )
        count += 1
    return count


def _resolve_explicit_symbol_ids(text: str, active_index: dict) -> list:
    resolved_ids = []
    for name in _collect_explicit_candidates(text):
        matches = active_index.get(name, [])
        if len(matches) == 1:
            resolved_ids.append(matches[0]["id"])
    return resolved_ids


def main() -> int:
    try:
        input_data = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        logger.error("stdin JSON 파싱 실패: %s", e)
        return 0

    session_id = input_data.get("session_id", "")
    last_message = input_data.get("last_assistant_message", "")
    cwd = input_data.get("cwd", "")
    if not session_id or not last_message or not cwd:
        return 0

    try:
        with scope_lock(cwd):
            graph_ready = ensure_scope_current(cwd, include_git_history=False)

            try:
                _, conn = get_db_connection(cwd=cwd)
            except FileNotFoundError:
                logger.debug("Context OS DB 없음, 건너뜀")
                return 0

            get_or_create_session(conn, session_id, cwd)

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
            conn.execute(
                "MATCH (s:Session {id: $sid}), (t:Turn {id: $tid}) "
                "MERGE (s)-[:HAS_TURN]->(t)",
                parameters={"sid": session_id, "tid": turn_id},
            )

            if not graph_ready:
                logger.warning("graph freshness 미검증 상태라 ABOUT 생략: %s", turn_id[:40])
                return 0

            active_index = get_active_symbol_index(conn)
            symbol_ids = _resolve_explicit_symbol_ids(last_message, active_index)
            about_count = _create_about_relations(conn, turn_id, symbol_ids)
            logger.info(
                "Turn 적재 완료: %s…, ABOUT 관계 %s개",
                turn_id[:40],
                about_count,
            )
    except Exception as e:
        logger.error("Turn 적재 실패: %s", e)

    return 0


if __name__ == "__main__":
    sys.exit(main())
