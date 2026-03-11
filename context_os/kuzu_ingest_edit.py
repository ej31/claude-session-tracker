#!/usr/bin/env python3
"""Context OS - PostToolUse hook.

Edit/Write events refresh the current worktree graph and then attach only facts
that can be proven safely:
- always record the touched file when it exists in the active graph
- only record MODIFIED_BY when the file maps to exactly one active symbol
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from build_context_os import (
    ensure_scope_current,
    get_active_symbols_by_file,
    get_db_connection,
    get_or_create_session,
    setup_logger,
    scope_lock,
)

logger = setup_logger("context-os-edit")


def _find_affected_symbols(conn, file_path: str) -> list:
    """Return active symbols for a file in the current scope."""
    return get_active_symbols_by_file(conn, file_path)


def _resolve_relative_path(file_path: str, cwd: str) -> str:
    """Normalize a path to a worktree-relative path when possible."""
    path = Path(file_path)
    if not path.is_absolute():
        return path.as_posix()

    if cwd:
        try:
            return path.resolve().relative_to(Path(cwd).resolve()).as_posix()
        except ValueError:
            return path.name
    return path.name


def _attach_touched_file(conn, turn_id: str, file_path: str) -> bool:
    result = conn.execute(
        "MATCH (f:File {path: $fp}) WHERE f.is_active = true RETURN f.path",
        parameters={"fp": file_path},
    )
    if not result.has_next():
        return False

    conn.execute(
        "MATCH (t:Turn {id: $tid}), (f:File {path: $fp}) "
        "MERGE (t)-[:TOUCHED_FILE]->(f)",
        parameters={"tid": turn_id, "fp": file_path},
    )
    return True


def main() -> int:
    try:
        input_data = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        logger.error("stdin JSON 파싱 실패: %s", e)
        return 0

    tool_name = input_data.get("tool_name", "")
    if tool_name not in ("Edit", "Write"):
        return 0

    session_id = input_data.get("session_id", "")
    tool_input = input_data.get("tool_input", {})
    file_path = tool_input.get("file_path", "")
    cwd = input_data.get("cwd", "")
    if not session_id or not file_path or not cwd:
        return 0

    rel_path = _resolve_relative_path(file_path, cwd)

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
            conn.execute(
                "MATCH (s:Session {id: $sid}), (t:Turn {id: $tid}) "
                "MERGE (s)-[:HAS_TURN]->(t)",
                parameters={"sid": session_id, "tid": turn_id},
            )

            touched = _attach_touched_file(conn, turn_id, rel_path)
            if not graph_ready:
                logger.warning("graph freshness 미검증 상태라 MODIFIED_BY 생략: %s", rel_path)
                return 0

            affected = _find_affected_symbols(conn, rel_path)
            if len(affected) == 1:
                conn.execute(
                    "MATCH (t:Turn {id: $tid}), (s:Symbol {id: $sid}) "
                    "MERGE (t)-[:MODIFIED_BY]->(s)",
                    parameters={"tid": turn_id, "sid": affected[0]["id"]},
                )
                logger.info(
                    "Edit 추적 완료: %s, TOUCHED_FILE=%s, MODIFIED_BY=%s",
                    rel_path,
                    touched,
                    affected[0]["name"],
                )
            else:
                logger.info(
                    "Edit 추적 완료: %s, TOUCHED_FILE=%s, MODIFIED_BY 생략(활성 심볼 %s개)",
                    rel_path,
                    touched,
                    len(affected),
                )
    except Exception as e:
        logger.error("Edit 추적 실패: %s", e)

    return 0


if __name__ == "__main__":
    sys.exit(main())
