from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import lancedb
import pyarrow as pa

from history_config import LanceOptimizePolicy
from history_ids import head_table_name, shard_for_timestamp
from history_ir import ChunkFact


def _quote(value: str) -> str:
    return value.replace("'", "''")


class LanceHistoryStore:
    def __init__(self, db_dir: str, optimize_policy: LanceOptimizePolicy | None = None):
        self.db_dir = Path(db_dir)
        self.db_dir.mkdir(parents=True, exist_ok=True)
        self.db = lancedb.connect(str(self.db_dir))
        self.optimize_policy = optimize_policy or LanceOptimizePolicy()
        self._tables: dict[str, object] = {}

    def _list_table_names(self) -> list[str]:
        listing = self.db.list_tables()
        if hasattr(listing, "tables"):
            return list(listing.tables)
        names = []
        for item in listing:
            if isinstance(item, str):
                names.append(item)
            elif isinstance(item, tuple) and len(item) == 2 and item[0] == "tables":
                names.extend(item[1])
        return names

    @property
    def chunk_schema(self) -> pa.Schema:
        return pa.schema([
            pa.field("chunk_id", pa.string()),
            pa.field("snapshot_commit_sha", pa.string()),
            pa.field("snapshot_seq", pa.int64()),
            pa.field("repo_id", pa.string()),
            pa.field("file_id", pa.string()),
            pa.field("file_state_id", pa.string()),
            pa.field("path_at_commit", pa.string()),
            pa.field("symbol_id", pa.string()),
            pa.field("symbol_state_id", pa.string()),
            pa.field("chunk_scope", pa.string()),
            pa.field("ordinal", pa.int64()),
            pa.field("start_line", pa.int64()),
            pa.field("end_line", pa.int64()),
            pa.field("language", pa.string()),
            pa.field("content", pa.string()),
            pa.field("content_hash", pa.string()),
            pa.field("search_text", pa.string()),
            pa.field("intent_text", pa.string()),
            pa.field("alias_text", pa.string()),
            pa.field("symbol_text", pa.string()),
            pa.field("doc_text", pa.string()),
            pa.field("surrogate_version", pa.string()),
            pa.field("surrogate_source", pa.string()),
            pa.field("fts_ready", pa.bool_()),
            pa.field("author_ts", pa.string()),
            pa.field("branch_hint", pa.string()),
            pa.field("is_head", pa.bool_()),
            pa.field("ingest_batch_id", pa.string()),
            # Read-only compatibility placeholders for prior vector schema.
            pa.field("embedding_model", pa.string()),
            pa.field("vector", pa.list_(pa.float32())),
        ])

    def _open_or_create(self, name: str, schema: pa.Schema):
        if name in self._tables:
            return self._tables[name]
        try:
            table = self.db.create_table(name, schema=schema, exist_ok=True)
        except Exception:
            table = self.db.open_table(name)
        self._tables[name] = table
        return table

    def _head_table(self, repo_id: str):
        return self._open_or_create(head_table_name(repo_id), self.chunk_schema)

    def _history_table(self, repo_id: str, committed_at: str):
        return self._open_or_create(shard_for_timestamp(repo_id, committed_at), self.chunk_schema)

    def upsert_snapshot(self, repo_id: str, commit_sha: str, committed_at: str, chunks: list[ChunkFact]) -> tuple[str, int]:
        history_table = self._history_table(repo_id, committed_at)
        history_table.delete(f"snapshot_commit_sha = '{_quote(commit_sha)}'")
        history_table.add([self._chunk_to_row(chunk) for chunk in chunks])
        head_table = self._head_table(repo_id)
        head_table.delete(f"repo_id = '{_quote(repo_id)}'")
        head_table.add([self._chunk_to_row(chunk) for chunk in chunks])
        return history_table.name, history_table.version

    def optimize_tables(self, repo_id: str, committed_at: str) -> None:
        for table in (self._head_table(repo_id), self._history_table(repo_id, committed_at)):
            self._ensure_scalar_indices(table)
            self._ensure_fts_index(table)
            try:
                table.optimize(
                    cleanup_older_than=timedelta(days=self.optimize_policy.cleanup_older_than_days),
                )
            except Exception:
                pass

    def query_chunks(self, repo_id: str, query_text: str, limit: int = 10, where: str = "") -> list[dict]:
        rows: list[dict] = []
        seen = set()
        table_names = [name for name in self._list_table_names() if name.startswith("history_chunks__")]
        table_names.append(head_table_name(repo_id))
        for table_name in table_names:
            if table_name not in self._list_table_names():
                continue
            table = self.db.open_table(table_name)
            builder = table.search(query_text, query_type="fts", fts_columns="search_text")
            if where:
                builder = builder.where(where, prefilter=True)
            for row in builder.limit(limit).to_list():
                key = row["chunk_id"]
                if key in seen:
                    continue
                seen.add(key)
                rows.append(row)
        rows.sort(key=lambda row: row.get("_score", 0.0), reverse=True)
        return rows[:limit]

    def snapshot_rows(self, repo_id: str, commit_sha: str) -> list[dict]:
        table_names = [name for name in self._list_table_names() if name.startswith("history_chunks__")]
        rows: list[dict] = []
        for table_name in table_names:
            table = self.db.open_table(table_name)
            builder = table.search(None).where(
                f"repo_id = '{_quote(repo_id)}' AND snapshot_commit_sha = '{_quote(commit_sha)}'"
            )
            rows.extend(builder.to_list())
        return rows

    def describe_storage(self) -> dict:
        table_names = self._list_table_names()
        return {
            "tables": table_names,
            "head_tables": [name for name in table_names if name.startswith("head_chunks__")],
            "history_tables": [name for name in table_names if name.startswith("history_chunks__")],
        }

    def rebuild_surrogates(self, repo_id: str) -> None:
        for table_name in self._list_table_names():
            if not (table_name.startswith("history_chunks__") or table_name == head_table_name(repo_id)):
                continue
            table = self.db.open_table(table_name)
            self._ensure_scalar_indices(table)
            self._ensure_fts_index(table, replace=True)

    def _ensure_scalar_indices(self, table) -> None:
        for column, index_type in (
            ("snapshot_commit_sha", "BTREE"),
            ("file_id", "BTREE"),
            ("symbol_id", "BTREE"),
            ("symbol_state_id", "BTREE"),
            ("path_at_commit", "BTREE"),
            ("author_ts", "BTREE"),
            ("language", "BITMAP"),
            ("chunk_scope", "BITMAP"),
            ("branch_hint", "BITMAP"),
            ("is_head", "BITMAP"),
            ("fts_ready", "BITMAP"),
        ):
            try:
                table.create_scalar_index(column, index_type=index_type)
            except Exception:
                continue

    def _ensure_fts_index(self, table, replace: bool = True) -> None:
        try:
            if table.count_rows() < self.optimize_policy.min_rows_for_fts_index:
                return
            table.create_fts_index(
                "search_text",
                replace=replace,
                lower_case=True,
                remove_stop_words=True,
                ascii_folding=True,
            )
        except Exception:
            pass

    def _chunk_to_row(self, chunk: ChunkFact) -> dict:
        return {
            "chunk_id": chunk.chunk_id,
            "snapshot_commit_sha": chunk.snapshot_commit_sha,
            "snapshot_seq": chunk.snapshot_seq,
            "repo_id": chunk.repo_id,
            "file_id": chunk.file_id,
            "file_state_id": chunk.file_state_id,
            "path_at_commit": chunk.path_at_commit,
            "symbol_id": chunk.symbol_id,
            "symbol_state_id": chunk.symbol_state_id,
            "chunk_scope": chunk.chunk_scope,
            "ordinal": chunk.ordinal,
            "start_line": chunk.start_line,
            "end_line": chunk.end_line,
            "language": chunk.language,
            "content": chunk.content,
            "content_hash": chunk.content_hash,
            "search_text": chunk.search_text,
            "intent_text": chunk.intent_text,
            "alias_text": chunk.alias_text,
            "symbol_text": chunk.symbol_text,
            "doc_text": chunk.doc_text,
            "surrogate_version": chunk.surrogate_version,
            "surrogate_source": chunk.surrogate_source,
            "fts_ready": chunk.fts_ready,
            "author_ts": chunk.author_ts,
            "branch_hint": chunk.branch_hint,
            "is_head": chunk.is_head,
            "ingest_batch_id": chunk.ingest_batch_id,
            "embedding_model": chunk.embedding_model,
            "vector": chunk.vector,
        }
