from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DeveloperFact:
    developer_id: str
    name: str
    email_norm: str


@dataclass
class CommitFact:
    commit_sha: str
    tree_sha: str
    authored_at: str
    committed_at: str
    message: str
    parent_shas: list[str]
    parent_count: int
    ingest_batch_id: str
    developer_id: str


@dataclass
class FileFact:
    file_id: str
    repo_id: str
    canonical_path: str
    first_seen_commit: str
    last_seen_commit: str
    status: str = "active"


@dataclass
class FileStateFact:
    file_state_id: str
    file_id: str
    path_at_commit: str
    blob_sha: str
    language: str
    size_bytes: int
    parse_status: str
    valid_from_commit: str
    valid_to_commit: str = ""
    content: str = ""
    content_hash: str = ""
    change_kind: str = "unchanged"


@dataclass
class SymbolFact:
    symbol_id: str
    repo_id: str
    file_id: str
    kind: str
    fq_name: str
    canonical_name: str
    first_seen_commit: str
    last_seen_commit: str


@dataclass
class SymbolStateFact:
    symbol_state_id: str
    symbol_id: str
    file_state_id: str
    signature_hash: str
    body_hash: str
    ast_hash: str
    start_line: int
    end_line: int
    valid_from_commit: str
    valid_to_commit: str = ""
    change_kind: str = "unchanged"
    content: str = ""
    content_hash: str = ""
    kind: str = ""
    canonical_name: str = ""
    fq_name: str = ""


@dataclass
class CallFact:
    caller_symbol_state_id: str
    callee_symbol_id: str
    callee_symbol_state_id: str
    valid_from_commit: str
    valid_to_commit: str = ""


@dataclass
class ChunkFact:
    chunk_id: str
    snapshot_commit_sha: str
    snapshot_seq: int
    repo_id: str
    file_id: str
    file_state_id: str
    path_at_commit: str
    symbol_id: str
    symbol_state_id: str
    chunk_scope: str
    ordinal: int
    start_line: int
    end_line: int
    language: str
    content: str
    content_hash: str
    search_text: str
    intent_text: str
    alias_text: str
    symbol_text: str
    doc_text: str
    surrogate_version: str
    surrogate_source: str
    fts_ready: bool
    author_ts: str
    branch_hint: str
    is_head: bool
    ingest_batch_id: str
    embedding_model: str = ""
    vector: list[float] = field(default_factory=list)


@dataclass
class FileTouchFact:
    commit_sha: str
    file_id: str
    file_state_id: str
    change_type: str
    additions: int
    deletions: int
    similarity_score: float
    path_at_commit: str


@dataclass
class SymbolTouchFact:
    commit_sha: str
    symbol_state_id: str
    changed_lines: list[int]
    diff_hash: str
    change_kind: str


@dataclass
class ParsedSymbol:
    kind: str
    canonical_name: str
    fq_name: str
    start_line: int
    end_line: int
    signature: str
    body: str
    doc_intent: str
    calls: list[str] = field(default_factory=list)


@dataclass
class ParsedFile:
    parse_status: str
    symbols: list[ParsedSymbol]


@dataclass
class CommitBundle:
    repo_id: str
    developer: DeveloperFact
    commit: CommitFact
    file_facts: list[FileFact]
    file_states: list[FileStateFact]
    symbol_facts: list[SymbolFact]
    symbol_states: list[SymbolStateFact]
    calls: list[CallFact]
    chunks: list[ChunkFact]
    file_touches: list[FileTouchFact]
    symbol_touches: list[SymbolTouchFact]
    current_path_to_file_id: dict[str, str]
    latest_file_states: dict[str, FileStateFact]
    latest_symbol_states: dict[str, SymbolStateFact]
    current_symbol_ids_by_name: dict[str, list[str]]
    closed_file_state_ids: list[str] = field(default_factory=list)
    closed_symbol_state_ids: list[str] = field(default_factory=list)
