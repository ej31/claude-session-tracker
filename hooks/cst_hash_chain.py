#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""세션 댓글 무결성 검증을 위한 hash chain 모듈

각 댓글은 이전 hash를 참조하는 SHA-256 체인으로 연결된다.
hash는 순수 content(질문/응답 텍스트)만 포함하며,
타임스탬프, 라벨 등 메타데이터는 제외한다.

댓글 본문에 HTML 주석으로 hash를 삽입하여
데이터와 검증 수단이 같은 위치에 존재하도록 보장한다.

댓글 구조 (GitHub에 저장되는 형태)

    <!-- integrity: {hash} | prev: {prev} -->
    <!-- meta: {timestamp} {label} -->
    {순수 content}

meta 주석은 검증 시 제거되며, hash 계산에 포함되지 않는다.
"""
from __future__ import annotations

import hashlib
import re
from typing import Optional, Tuple

GENESIS_HASH = "0" * 64
HASH_PATTERN = re.compile(
    r"<!-- integrity: (?P<hash>[0-9a-f]{64}) \| prev: (?P<prev>[0-9a-f]{64}) -->"
)
META_PATTERN = re.compile(r"<!-- meta: .+? -->")


def compute_hash(content: str, prev_hash: str) -> str:
    """content + prev_hash로 SHA-256 hash를 계산한다.

    Args:
        content: 순수 텍스트 (질문 또는 응답 본문, 메타데이터 제외)
        prev_hash: 이전 댓글의 hash (첫 댓글이면 GENESIS_HASH)

    Returns:
        64자리 hex SHA-256 hash
    """
    payload = f"{prev_hash}{content}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_hash_comment(current_hash: str, prev_hash: str) -> str:
    """댓글 본문에 삽입할 integrity HTML 주석 문자열을 생성한다."""
    return f"<!-- integrity: {current_hash} | prev: {prev_hash} -->"


def build_meta_comment(timestamp: str, label: str) -> str:
    """display 메타데이터를 HTML 주석으로 감싸 반환한다."""
    return f"<!-- meta: {timestamp} {label} -->"


def stamp_comment(
    content: str, prev_hash: str, timestamp: str, label: str
) -> Tuple[str, str]:
    """content에 hash chain + meta 주석을 붙인 댓글 본문과 현재 hash를 반환한다.

    hash는 순수 content만으로 계산한다. timestamp/label은 meta 주석에만 기록되며
    hash 계산에 포함되지 않는다.

    Args:
        content: 순수 텍스트 (hash 계산 대상)
        prev_hash: 이전 댓글의 hash
        timestamp: 표시용 타임스탬프 (예: "2026-03-15 10:00:00")
        label: 표시용 라벨 (예: "Prompt", "Response")

    Returns:
        (완성된 댓글 본문, 현재 hash)
    """
    current_hash = compute_hash(content, prev_hash)
    integrity_tag = build_hash_comment(current_hash, prev_hash)
    meta_tag = build_meta_comment(timestamp, label)
    comment_body = f"{integrity_tag}\n{meta_tag}\n{content}"
    return comment_body, current_hash


def extract_hash(comment_body: str) -> Optional[Tuple[str, str]]:
    """댓글 본문에서 (current_hash, prev_hash) 튜플을 추출한다.

    hash 주석이 없으면 None을 반환한다.
    """
    match = HASH_PATTERN.search(comment_body)
    if not match:
        return None
    return match.group("hash"), match.group("prev")


def extract_content(comment_body: str) -> str:
    """댓글 본문에서 integrity/meta 주석을 제거하고 순수 content만 반환한다."""
    result = HASH_PATTERN.sub("", comment_body)
    result = META_PATTERN.sub("", result)
    return result.strip()


def verify_comment(comment_body: str, expected_prev_hash: str) -> bool:
    """단일 댓글의 hash 무결성을 검증한다.

    Args:
        comment_body: hash 주석이 포함된 댓글 전문
        expected_prev_hash: 이 댓글의 prev가 일치해야 하는 hash

    Returns:
        hash chain이 유효하면 True
    """
    hashes = extract_hash(comment_body)
    if not hashes:
        return False

    current_hash, prev_hash = hashes
    if prev_hash != expected_prev_hash:
        return False

    content = extract_content(comment_body)
    recomputed = compute_hash(content, prev_hash)
    return recomputed == current_hash


def verify_chain(comment_bodies: list[str]) -> Tuple[bool, int]:
    """댓글 목록 전체의 hash chain을 검증한다.

    Args:
        comment_bodies: 시간순 정렬된 댓글 본문 리스트

    Returns:
        (전체 유효 여부, 첫 번째 실패 인덱스). 전부 유효하면 (True, -1)
    """
    prev_hash = GENESIS_HASH
    for idx, body in enumerate(comment_bodies):
        hashes = extract_hash(body)
        if not hashes:
            return False, idx
        current_hash, recorded_prev = hashes
        if recorded_prev != prev_hash:
            return False, idx
        content = extract_content(body)
        recomputed = compute_hash(content, recorded_prev)
        if recomputed != current_hash:
            return False, idx
        prev_hash = current_hash
    return True, -1
