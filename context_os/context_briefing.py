#!/usr/bin/env python3
"""Context OS compact-time briefing.

If the graph cannot be proven fresh, the briefing fails closed and omits
symbol-derived context instead of risking stale injection.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from build_context_os import (
    get_db_connection,
    is_scope_fresh,
    load_scope_meta,
    setup_logger,
    scope_lock,
)

logger = setup_logger("context-os-briefing")

MAX_RECENT_TURNS = 10
MAX_SYMBOLS = 20
MAX_FILES = 10
MAX_DEPS_DISPLAY = 5
MAX_COMMITS_DISPLAY = 1
MAX_CHAIN_DISPLAY = 3


def _get_recent_turns(conn, session_id: str) -> list:
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
            "id": row[0],
            "timestamp": row[1],
            "type": row[2],
            "summary": row[3],
        })
    return turns


def _get_touched_files(conn, session_id: str) -> list:
    result = conn.execute(
        "MATCH (ses:Session {id: $sid})-[:HAS_TURN]->(t:Turn)-[:TOUCHED_FILE]->(f:File) "
        "WHERE f.is_active = true "
        "WITH f, max(t.timestamp) AS latest "
        "RETURN f.path, latest ORDER BY latest DESC LIMIT 10",
        parameters={"sid": session_id},
    )
    files = []
    while result.has_next():
        row = result.get_next()
        files.append({"path": row[0], "latest": row[1]})
    return files


def _get_session_symbols(conn, session_id: str) -> list:
    result = conn.execute(
        "MATCH (ses:Session {id: $sid})-[:HAS_TURN]->(t:Turn)-[:ABOUT|MODIFIED_BY]->(s:Symbol) "
        "WHERE s.is_active = true "
        "WITH s, max(t.timestamp) AS latest "
        "RETURN s.id, s.name, s.type, s.file_path, s.intent, s.start_line "
        "ORDER BY latest DESC LIMIT 20",
        parameters={"sid": session_id},
    )
    symbols = []
    while result.has_next():
        row = result.get_next()
        symbols.append({
            "id": row[0],
            "name": row[1],
            "type": row[2],
            "file_path": row[3],
            "intent": row[4],
            "start_line": row[5],
        })
    return symbols


def _get_symbol_calls(conn, symbol_id: str) -> list:
    result = conn.execute(
        "MATCH (s:Symbol {id: $sid})-[:CALLS]->(dep:Symbol) "
        "WHERE s.is_active = true AND dep.is_active = true "
        "RETURN dep.name, dep.file_path",
        parameters={"sid": symbol_id},
    )
    deps = []
    while result.has_next():
        row = result.get_next()
        deps.append({"name": row[0], "file_path": row[1]})
    return deps


def _get_recent_commits_for_symbol(conn, symbol_id: str) -> list:
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


def _format_turns_section(turns: list) -> list:
    type_labels = {"response": "응답", "edit": "수정"}
    lines = ["## 최근 작업 내역"]
    for idx, turn in enumerate(turns[:5], 1):
        label = type_labels.get(turn["type"], turn["type"])
        lines.append(f"- Turn #{idx} [{label}]: {turn['summary']}")
    return lines


def _format_files_section(files: list) -> list:
    lines = ["## 최근 작업 파일"]
    for file_info in files[:MAX_FILES]:
        lines.append(f"- {file_info['path']}")
    return lines


def _format_symbol_section(symbols: list, conn) -> list:
    lines = ["## 안전하게 확인된 심볼"]
    for sym in symbols[:MAX_SYMBOLS]:
        location = f"{sym['file_path']}:{sym['start_line']}"
        intent_part = f" — {sym['intent']}" if sym.get("intent") else ""
        lines.append(f"- {sym['name']}() [{location}]{intent_part}")

        calls = _get_symbol_calls(conn, sym["id"])
        if calls:
            call_names = ", ".join(
                f"{dep['name']}()" for dep in calls[:MAX_DEPS_DISPLAY]
            )
            lines.append(f"  - 호출 대상: {call_names}")

        commits = _get_recent_commits_for_symbol(conn, sym["id"])
        for commit in commits[:MAX_COMMITS_DISPLAY]:
            lines.append(
                f"  - 최근 수정: commit {commit['hash']} \"{commit['message']}\""
            )
    return lines


def _format_dependency_section(symbols: list, conn) -> list:
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


def format_briefing(
    session_id: str,
    turns: list,
    files: list,
    symbols: list,
    conn,
    scope_meta: dict | None = None,
    graph_verified: bool = True,
) -> str:
    lines = ["[Context OS 브리핑] compact 후 맥락 복원", ""]

    if scope_meta:
        branch = scope_meta.get("branch") or ""
        worktree = scope_meta.get("worktree_root") or ""
        if branch:
            lines.append(f"**Branch**: `{branch}`")
        if worktree:
            lines.append(f"**Worktree**: `{worktree}`")
        if branch or worktree:
            lines.append("")

    if turns:
        lines.extend(_format_turns_section(turns))
        lines.append("")

    if files:
        lines.extend(_format_files_section(files))
        lines.append("")

    if graph_verified and symbols:
        lines.extend(_format_symbol_section(symbols, conn))
        lines.append("")

        dep_lines = _format_dependency_section(symbols, conn)
        if len(dep_lines) > 1:
            lines.extend(dep_lines)
            lines.append("")
    elif not graph_verified:
        lines.append(
            "(현재 graph freshness를 보장할 수 없어 symbol/dependency 맥락 주입을 생략합니다)"
        )

    if not turns and not files and not symbols and graph_verified:
        lines.append("(이 세션에 대한 맥락 데이터가 없습니다)")

    return "\n".join(lines).rstrip()


def main() -> int:
    try:
        input_data = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        logger.error("stdin JSON 파싱 실패: %s", e)
        return 0

    session_id = input_data.get("session_id", "")
    if not session_id:
        logger.debug("session_id 없음, 건너뜀")
        return 0

    cwd = input_data.get("cwd", "") or os.getcwd()

    turns = []
    files = []
    symbols = []
    scope_meta = load_scope_meta(cwd) or {}
    graph_verified = False

    try:
        with scope_lock(cwd):
            # Compact-time briefing is on a user-visible path, so fail closed
            # instead of triggering a synchronous rebuild when the scope is stale.
            graph_verified = is_scope_fresh(cwd)
            scope_meta = load_scope_meta(cwd) or scope_meta

            try:
                _, conn = get_db_connection(cwd=cwd)
            except FileNotFoundError:
                conn = None

            if conn is not None:
                turns = _get_recent_turns(conn, session_id)
                files = _get_touched_files(conn, session_id)
                if graph_verified:
                    symbols = _get_session_symbols(conn, session_id)
    except Exception as e:
        logger.error("브리핑 생성 중 예외: %s", e)

    print(format_briefing(
        session_id,
        turns,
        files,
        symbols,
        conn if "conn" in locals() and conn is not None else None,
        scope_meta=scope_meta,
        graph_verified=graph_verified,
    ))

    logger.info(
        "브리핑 생성 완료: session=%s…, verified=%s, files=%s, symbols=%s, turns=%s",
        session_id[:8],
        graph_verified,
        len(files),
        len(symbols),
        len(turns),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
