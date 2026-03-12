from __future__ import annotations

import csv
import tempfile
from pathlib import Path

import kuzu

from history_config import KuzuBulkIngestPolicy
from history_ir import CommitBundle, FileFact, FileStateFact, SymbolFact, SymbolStateFact


SCHEMA_STATEMENTS = [
    (
        "CREATE NODE TABLE IF NOT EXISTS Repository("
        "repo_id STRING, normalized_remote STRING, local_root STRING, default_branch STRING, created_at STRING, "
        "PRIMARY KEY(repo_id))"
    ),
    (
        "CREATE NODE TABLE IF NOT EXISTS Developer("
        "developer_id STRING, name STRING, email_norm STRING, PRIMARY KEY(developer_id))"
    ),
    (
        "CREATE NODE TABLE IF NOT EXISTS Commit("
        "commit_sha STRING, tree_sha STRING, authored_at STRING, committed_at STRING, message STRING, "
        "parent_count INT64, ingest_batch_id STRING, PRIMARY KEY(commit_sha))"
    ),
    (
        "CREATE NODE TABLE IF NOT EXISTS File("
        "file_id STRING, repo_id STRING, canonical_path STRING, first_seen_commit STRING, "
        "last_seen_commit STRING, status STRING, PRIMARY KEY(file_id))"
    ),
    (
        "CREATE NODE TABLE IF NOT EXISTS FileState("
        "file_state_id STRING, file_id STRING, path_at_commit STRING, blob_sha STRING, language STRING, "
        "size_bytes INT64, parse_status STRING, valid_from_commit STRING, valid_to_commit STRING, change_kind STRING, "
        "PRIMARY KEY(file_state_id))"
    ),
    (
        "CREATE NODE TABLE IF NOT EXISTS Symbol("
        "symbol_id STRING, repo_id STRING, file_id STRING, kind STRING, fq_name STRING, canonical_name STRING, "
        "first_seen_commit STRING, last_seen_commit STRING, PRIMARY KEY(symbol_id))"
    ),
    (
        "CREATE NODE TABLE IF NOT EXISTS SymbolState("
        "symbol_state_id STRING, symbol_id STRING, file_state_id STRING, signature_hash STRING, body_hash STRING, "
        "ast_hash STRING, start_line INT64, end_line INT64, valid_from_commit STRING, valid_to_commit STRING, "
        "change_kind STRING, content STRING, canonical_name STRING, fq_name STRING, kind STRING, "
        "PRIMARY KEY(symbol_state_id))"
    ),
    "CREATE REL TABLE IF NOT EXISTS AUTHORED(FROM Developer TO Commit)",
    "CREATE REL TABLE IF NOT EXISTS PARENT_OF(FROM Commit TO Commit)",
    "CREATE REL TABLE IF NOT EXISTS TOUCHES(FROM Commit TO FileState, change_type STRING, additions INT64, deletions INT64, similarity_score DOUBLE)",
    "CREATE REL TABLE IF NOT EXISTS OF_FILE(FROM FileState TO File)",
    "CREATE REL TABLE IF NOT EXISTS DECLARES(FROM FileState TO SymbolState)",
    "CREATE REL TABLE IF NOT EXISTS OF_SYMBOL(FROM SymbolState TO Symbol)",
    "CREATE REL TABLE IF NOT EXISTS MODIFIES(FROM Commit TO SymbolState, changed_lines STRING, diff_hash STRING, change_kind STRING)",
    "CREATE REL TABLE IF NOT EXISTS CALLS(FROM SymbolState TO SymbolState, valid_from_commit STRING, valid_to_commit STRING)",
    "CREATE REL TABLE IF NOT EXISTS PRECEDES_FILE(FROM FileState TO FileState)",
    "CREATE REL TABLE IF NOT EXISTS PRECEDES_SYMBOL(FROM SymbolState TO SymbolState)",
]


class TemporalGraphStore:
    def __init__(
        self,
        db_path: str,
        buffer_pool_size: int = 0,
        max_num_threads: int = 0,
        checkpoint_threshold: int = -1,
        bulk_ingest_policy: KuzuBulkIngestPolicy | None = None,
    ):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.bulk_ingest_policy = bulk_ingest_policy or KuzuBulkIngestPolicy()
        self.db = kuzu.Database(
            str(self.db_path),
            buffer_pool_size=buffer_pool_size,
            max_num_threads=max_num_threads,
            checkpoint_threshold=checkpoint_threshold,
        )
        self.conn = kuzu.Connection(self.db)
        for stmt in SCHEMA_STATEMENTS:
            self.conn.execute(stmt)

    def ingest_repository(self, repo_id: str, normalized_remote: str, local_root: str, default_branch: str, created_at: str) -> None:
        self.conn.execute(
            """
            MERGE (r:Repository {repo_id: $repo_id})
            SET r.normalized_remote = $normalized_remote,
                r.local_root = $local_root,
                r.default_branch = $default_branch,
                r.created_at = $created_at
            """,
            parameters={
                "repo_id": repo_id,
                "normalized_remote": normalized_remote,
                "local_root": local_root,
                "default_branch": default_branch,
                "created_at": created_at,
            },
        )

    def ingest_commit_bundle(
        self,
        bundle: CommitBundle,
        *,
        staging_dir: str | None = None,
        force_copy_from: bool | None = None,
    ) -> None:
        self._upsert_developer(bundle)
        self._upsert_commit(bundle)
        if self._should_use_copy_from(bundle, force_copy_from=force_copy_from):
            self._bulk_copy_bundle(bundle, staging_dir=staging_dir)
        else:
            self._upsert_file_facts(bundle)
            self._upsert_file_states(bundle)
            self._upsert_symbol_facts(bundle)
            self._upsert_symbol_states(bundle)
            self._upsert_file_touches(bundle)
            self._upsert_symbol_touches(bundle)
            self._upsert_calls(bundle)
        self._close_states(bundle)
        self._link_precedes(bundle)

    def export_snapshot_staging(self, bundle: CommitBundle, staging_dir: str) -> dict[str, str]:
        target = Path(staging_dir)
        target.mkdir(parents=True, exist_ok=True)
        paths = {
            "files": target / "files.csv",
            "file_states": target / "file_states.csv",
            "symbols": target / "symbols.csv",
            "symbol_states": target / "symbol_states.csv",
            "file_touches": target / "file_touches.csv",
            "symbol_touches": target / "symbol_touches.csv",
            "of_file": target / "of_file.csv",
            "declares": target / "declares.csv",
            "of_symbol": target / "of_symbol.csv",
            "calls": target / "calls.csv",
        }
        self._write_csv(paths["files"], bundle.file_facts, ["file_id", "repo_id", "canonical_path", "first_seen_commit", "last_seen_commit", "status"])
        self._write_csv(paths["file_states"], bundle.file_states, ["file_state_id", "file_id", "path_at_commit", "blob_sha", "language", "size_bytes", "parse_status", "valid_from_commit", "valid_to_commit", "change_kind"])
        self._write_csv(paths["symbols"], bundle.symbol_facts, ["symbol_id", "repo_id", "file_id", "kind", "fq_name", "canonical_name", "first_seen_commit", "last_seen_commit"])
        self._write_csv(paths["symbol_states"], bundle.symbol_states, ["symbol_state_id", "symbol_id", "file_state_id", "signature_hash", "body_hash", "ast_hash", "start_line", "end_line", "valid_from_commit", "valid_to_commit", "change_kind", "content", "canonical_name", "fq_name", "kind"])
        self._write_rel_csv(paths["file_touches"], [
            {
                "from": row.commit_sha,
                "to": row.file_state_id,
                "change_type": row.change_type,
                "additions": row.additions,
                "deletions": row.deletions,
                "similarity_score": row.similarity_score,
            }
            for row in bundle.file_touches
        ])
        self._write_rel_csv(paths["symbol_touches"], [
            {
                "from": row.commit_sha,
                "to": row.symbol_state_id,
                "changed_lines": ",".join(str(line) for line in row.changed_lines),
                "diff_hash": row.diff_hash,
                "change_kind": row.change_kind,
            }
            for row in bundle.symbol_touches
        ])
        self._write_rel_csv(paths["of_file"], [
            {"from": row.file_state_id, "to": row.file_id}
            for row in bundle.file_states
        ])
        self._write_rel_csv(paths["declares"], [
            {"from": row.file_state_id, "to": row.symbol_state_id}
            for row in bundle.symbol_states
        ])
        self._write_rel_csv(paths["of_symbol"], [
            {"from": row.symbol_state_id, "to": row.symbol_id}
            for row in bundle.symbol_states
        ])
        self._write_rel_csv(paths["calls"], [
            {
                "from": row.caller_symbol_state_id,
                "to": row.callee_symbol_state_id,
                "valid_from_commit": row.valid_from_commit,
                "valid_to_commit": row.valid_to_commit,
            }
            for row in bundle.calls
        ])
        return {name: str(path) for name, path in paths.items()}

    def get_latest_file_state(self, file_id: str) -> dict | None:
        result = self.conn.execute(
            """
            MATCH (fs:FileState {file_id: $file_id})
            WHERE fs.valid_to_commit = ''
            RETURN fs.file_state_id, fs.path_at_commit, fs.blob_sha
            """,
            parameters={"file_id": file_id},
        )
        if not result.has_next():
            return None
        row = result.get_next()
        return {"file_state_id": row[0], "path_at_commit": row[1], "blob_sha": row[2]}

    def developer_for_commit(self, commit_sha: str) -> dict | None:
        result = self.conn.execute(
            """
            MATCH (d:Developer)-[:AUTHORED]->(c:Commit {commit_sha: $commit_sha})
            RETURN d.developer_id, d.name, d.email_norm
            """,
            parameters={"commit_sha": commit_sha},
        )
        if not result.has_next():
            return None
        row = result.get_next()
        return {"developer_id": row[0], "name": row[1], "email_norm": row[2]}

    def latest_symbol_by_name(self, canonical_name: str) -> list[dict]:
        result = self.conn.execute(
            """
            MATCH (ss:SymbolState)-[:OF_SYMBOL]->(s:Symbol)
            WHERE s.canonical_name = $canonical_name AND ss.valid_to_commit = ''
            RETURN s.symbol_id, ss.symbol_state_id, ss.signature_hash, ss.body_hash, ss.content
            """,
            parameters={"canonical_name": canonical_name},
        )
        rows = []
        while result.has_next():
            row = result.get_next()
            rows.append({
                "symbol_id": row[0],
                "symbol_state_id": row[1],
                "signature_hash": row[2],
                "body_hash": row[3],
                "content": row[4],
            })
        return rows

    def symbol_state_history(self, symbol_id: str) -> list[dict]:
        result = self.conn.execute(
            """
            MATCH (ss:SymbolState {symbol_id: $symbol_id}), (c:Commit {commit_sha: ss.valid_from_commit})
            RETURN ss.symbol_state_id, ss.signature_hash, ss.body_hash, ss.valid_from_commit, ss.content, c.committed_at
            ORDER BY c.committed_at ASC
            """,
            parameters={"symbol_id": symbol_id},
        )
        rows = []
        while result.has_next():
            row = result.get_next()
            rows.append({
                "symbol_state_id": row[0],
                "signature_hash": row[1],
                "body_hash": row[2],
                "valid_from_commit": row[3],
                "content": row[4],
                "committed_at": row[5],
            })
        return rows

    def callers_of_symbol_state(self, symbol_state_id: str) -> list[dict]:
        result = self.conn.execute(
            """
            MATCH (caller:SymbolState)-[:CALLS]->(callee:SymbolState {symbol_state_id: $symbol_state_id})
            RETURN caller.symbol_state_id, caller.symbol_id, caller.canonical_name, caller.content
            """,
            parameters={"symbol_state_id": symbol_state_id},
        )
        rows = []
        while result.has_next():
            row = result.get_next()
            rows.append({
                "caller_symbol_state_id": row[0],
                "caller_symbol_id": row[1],
                "caller_name": row[2],
                "content": row[3],
            })
        return rows

    def touched_symbol_states(self, commit_sha: str) -> list[dict]:
        result = self.conn.execute(
            """
            MATCH (c:Commit {commit_sha: $commit_sha})-[r:MODIFIES]->(ss:SymbolState)
            RETURN ss.symbol_state_id, ss.symbol_id, ss.content, r.change_kind
            """,
            parameters={"commit_sha": commit_sha},
        )
        rows = []
        while result.has_next():
            row = result.get_next()
            rows.append({
                "symbol_state_id": row[0],
                "symbol_id": row[1],
                "content": row[2],
                "change_kind": row[3],
            })
        return rows

    def touch_stats(self, commit_sha: str) -> dict:
        file_result = self.conn.execute(
            "MATCH (c:Commit {commit_sha: $commit_sha})-[:TOUCHES]->(fs:FileState) RETURN count(fs)",
            parameters={"commit_sha": commit_sha},
        )
        symbol_result = self.conn.execute(
            "MATCH (c:Commit {commit_sha: $commit_sha})-[:MODIFIES]->(ss:SymbolState) RETURN count(ss)",
            parameters={"commit_sha": commit_sha},
        )
        files = file_result.get_next()[0] if file_result.has_next() else 0
        symbols = symbol_result.get_next()[0] if symbol_result.has_next() else 0
        return {"files": files, "symbols": symbols}

    def load_repository_state(self, repo_id: str) -> dict:
        return {
            "file_facts": self._load_file_facts(repo_id),
            "symbol_facts": self._load_symbol_facts(repo_id),
            "latest_file_states": self._load_latest_file_states(repo_id),
            "latest_symbol_states": self._load_latest_symbol_states(repo_id),
        }

    def explain(self, query: str, parameters: dict | None = None) -> list[list]:
        result = self.conn.execute(f"EXPLAIN {query}", parameters=parameters or {})
        rows = []
        while result.has_next():
            rows.append(result.get_next())
        return rows

    def profile(self, query: str, parameters: dict | None = None) -> list[list]:
        result = self.conn.execute(f"PROFILE {query}", parameters=parameters or {})
        rows = []
        while result.has_next():
            rows.append(result.get_next())
        return rows

    def _upsert_developer(self, bundle: CommitBundle) -> None:
        self.conn.execute(
            """
            MERGE (d:Developer {developer_id: $developer_id})
            SET d.name = $name, d.email_norm = $email_norm
            """,
            parameters={
                "developer_id": bundle.developer.developer_id,
                "name": bundle.developer.name,
                "email_norm": bundle.developer.email_norm,
            },
        )

    def _upsert_commit(self, bundle: CommitBundle) -> None:
        self.conn.execute(
            """
            MERGE (c:Commit {commit_sha: $commit_sha})
            SET c.tree_sha = $tree_sha,
                c.authored_at = $authored_at,
                c.committed_at = $committed_at,
                c.message = $message,
                c.parent_count = $parent_count,
                c.ingest_batch_id = $ingest_batch_id
            """,
            parameters={
                "commit_sha": bundle.commit.commit_sha,
                "tree_sha": bundle.commit.tree_sha,
                "authored_at": bundle.commit.authored_at,
                "committed_at": bundle.commit.committed_at,
                "message": bundle.commit.message,
                "parent_count": bundle.commit.parent_count,
                "ingest_batch_id": bundle.commit.ingest_batch_id,
            },
        )
        self.conn.execute(
            """
            MATCH (d:Developer {developer_id: $developer_id}), (c:Commit {commit_sha: $commit_sha})
            MERGE (d)-[:AUTHORED]->(c)
            """,
            parameters={
                "developer_id": bundle.developer.developer_id,
                "commit_sha": bundle.commit.commit_sha,
            },
        )
        for parent_sha in bundle.commit.parent_shas:
            self.conn.execute(
                """
                MERGE (p:Commit {commit_sha: $parent_sha})
                SET p.parent_count = COALESCE(p.parent_count, 0)
                """,
                parameters={"parent_sha": parent_sha},
            )
            self.conn.execute(
                """
                MATCH (p:Commit {commit_sha: $parent_sha}), (c:Commit {commit_sha: $commit_sha})
                MERGE (p)-[:PARENT_OF]->(c)
                """,
                parameters={"parent_sha": parent_sha, "commit_sha": bundle.commit.commit_sha},
            )

    def _upsert_file_facts(self, bundle: CommitBundle) -> None:
        for file_fact in bundle.file_facts:
            self.conn.execute(
                """
                MERGE (f:File {file_id: $file_id})
                SET f.repo_id = $repo_id,
                    f.canonical_path = $canonical_path,
                    f.first_seen_commit = $first_seen_commit,
                    f.last_seen_commit = $last_seen_commit,
                    f.status = $status
                """,
                parameters=file_fact.__dict__,
            )

    def _upsert_file_states(self, bundle: CommitBundle) -> None:
        for file_state in bundle.file_states:
            self.conn.execute(
                """
                MERGE (fs:FileState {file_state_id: $file_state_id})
                SET fs.file_id = $file_id,
                    fs.path_at_commit = $path_at_commit,
                    fs.blob_sha = $blob_sha,
                    fs.language = $language,
                    fs.size_bytes = $size_bytes,
                    fs.parse_status = $parse_status,
                    fs.valid_from_commit = $valid_from_commit,
                    fs.valid_to_commit = $valid_to_commit,
                    fs.change_kind = $change_kind
                """,
                parameters={
                    "file_state_id": file_state.file_state_id,
                    "file_id": file_state.file_id,
                    "path_at_commit": file_state.path_at_commit,
                    "blob_sha": file_state.blob_sha,
                    "language": file_state.language,
                    "size_bytes": file_state.size_bytes,
                    "parse_status": file_state.parse_status,
                    "valid_from_commit": file_state.valid_from_commit,
                    "valid_to_commit": file_state.valid_to_commit,
                    "change_kind": file_state.change_kind,
                },
            )
            self.conn.execute(
                """
                MATCH (fs:FileState {file_state_id: $file_state_id}), (f:File {file_id: $file_id})
                MERGE (fs)-[:OF_FILE]->(f)
                """,
                parameters={"file_state_id": file_state.file_state_id, "file_id": file_state.file_id},
            )

    def _upsert_symbol_facts(self, bundle: CommitBundle) -> None:
        for symbol_fact in bundle.symbol_facts:
            self.conn.execute(
                """
                MERGE (s:Symbol {symbol_id: $symbol_id})
                SET s.repo_id = $repo_id,
                    s.file_id = $file_id,
                    s.kind = $kind,
                    s.fq_name = $fq_name,
                    s.canonical_name = $canonical_name,
                    s.first_seen_commit = $first_seen_commit,
                    s.last_seen_commit = $last_seen_commit
                """,
                parameters=symbol_fact.__dict__,
            )

    def _upsert_symbol_states(self, bundle: CommitBundle) -> None:
        for symbol_state in bundle.symbol_states:
            self.conn.execute(
                """
                MERGE (ss:SymbolState {symbol_state_id: $symbol_state_id})
                SET ss.symbol_id = $symbol_id,
                    ss.file_state_id = $file_state_id,
                    ss.signature_hash = $signature_hash,
                    ss.body_hash = $body_hash,
                    ss.ast_hash = $ast_hash,
                    ss.start_line = $start_line,
                    ss.end_line = $end_line,
                    ss.valid_from_commit = $valid_from_commit,
                    ss.valid_to_commit = $valid_to_commit,
                    ss.change_kind = $change_kind,
                    ss.content = $content,
                    ss.canonical_name = $canonical_name,
                    ss.fq_name = $fq_name,
                    ss.kind = $kind
                """,
                parameters={
                    "symbol_state_id": symbol_state.symbol_state_id,
                    "symbol_id": symbol_state.symbol_id,
                    "file_state_id": symbol_state.file_state_id,
                    "signature_hash": symbol_state.signature_hash,
                    "body_hash": symbol_state.body_hash,
                    "ast_hash": symbol_state.ast_hash,
                    "start_line": symbol_state.start_line,
                    "end_line": symbol_state.end_line,
                    "valid_from_commit": symbol_state.valid_from_commit,
                    "valid_to_commit": symbol_state.valid_to_commit,
                    "change_kind": symbol_state.change_kind,
                    "content": symbol_state.content,
                    "canonical_name": symbol_state.canonical_name,
                    "fq_name": symbol_state.fq_name,
                    "kind": symbol_state.kind,
                },
            )
            self.conn.execute(
                """
                MATCH (fs:FileState {file_state_id: $file_state_id}), (ss:SymbolState {symbol_state_id: $symbol_state_id})
                MERGE (fs)-[:DECLARES]->(ss)
                """,
                parameters={"file_state_id": symbol_state.file_state_id, "symbol_state_id": symbol_state.symbol_state_id},
            )
            self.conn.execute(
                """
                MATCH (ss:SymbolState {symbol_state_id: $symbol_state_id}), (s:Symbol {symbol_id: $symbol_id})
                MERGE (ss)-[:OF_SYMBOL]->(s)
                """,
                parameters={"symbol_state_id": symbol_state.symbol_state_id, "symbol_id": symbol_state.symbol_id},
            )

    def _upsert_calls(self, bundle: CommitBundle) -> None:
        for call in bundle.calls:
            self.conn.execute(
                """
                MATCH (a:SymbolState {symbol_state_id: $caller}), (b:SymbolState {symbol_state_id: $callee})
                MERGE (a)-[r:CALLS]->(b)
                SET r.valid_from_commit = $valid_from_commit,
                    r.valid_to_commit = $valid_to_commit
                """,
                parameters={
                    "caller": call.caller_symbol_state_id,
                    "callee": call.callee_symbol_state_id,
                    "valid_from_commit": call.valid_from_commit,
                    "valid_to_commit": call.valid_to_commit,
                },
            )

    def _close_states(self, bundle: CommitBundle) -> None:
        for file_state_id in bundle.closed_file_state_ids:
            self.conn.execute(
                """
                MATCH (fs:FileState {file_state_id: $file_state_id})
                SET fs.valid_to_commit = $commit_sha
                """,
                parameters={"file_state_id": file_state_id, "commit_sha": bundle.commit.commit_sha},
            )
        for symbol_state_id in bundle.closed_symbol_state_ids:
            self.conn.execute(
                """
                MATCH (ss:SymbolState {symbol_state_id: $symbol_state_id})
                SET ss.valid_to_commit = $commit_sha
                """,
                parameters={"symbol_state_id": symbol_state_id, "commit_sha": bundle.commit.commit_sha},
            )

    def _link_precedes(self, bundle: CommitBundle) -> None:
        for file_state in bundle.file_states:
            previous = self.conn.execute(
                """
                MATCH (prev:FileState {file_id: $file_id})
                WHERE prev.valid_to_commit = $valid_from_commit AND prev.file_state_id <> $file_state_id
                RETURN prev.file_state_id
                """,
                parameters={
                    "file_id": file_state.file_id,
                    "valid_from_commit": file_state.valid_from_commit,
                    "file_state_id": file_state.file_state_id,
                },
            )
            while previous.has_next():
                prev_state_id = previous.get_next()[0]
                self.conn.execute(
                    """
                    MATCH (prev:FileState {file_state_id: $prev_state_id}), (curr:FileState {file_state_id: $curr_state_id})
                    MERGE (prev)-[:PRECEDES_FILE]->(curr)
                    """,
                    parameters={"prev_state_id": prev_state_id, "curr_state_id": file_state.file_state_id},
                )

        for symbol_state in bundle.symbol_states:
            previous = self.conn.execute(
                """
                MATCH (prev:SymbolState {symbol_id: $symbol_id})
                WHERE prev.valid_to_commit = $valid_from_commit AND prev.symbol_state_id <> $symbol_state_id
                RETURN prev.symbol_state_id
                """,
                parameters={
                    "symbol_id": symbol_state.symbol_id,
                    "valid_from_commit": symbol_state.valid_from_commit,
                    "symbol_state_id": symbol_state.symbol_state_id,
                },
            )
            while previous.has_next():
                prev_state_id = previous.get_next()[0]
                self.conn.execute(
                    """
                    MATCH (prev:SymbolState {symbol_state_id: $prev_state_id}), (curr:SymbolState {symbol_state_id: $curr_state_id})
                    MERGE (prev)-[:PRECEDES_SYMBOL]->(curr)
                    """,
                    parameters={"prev_state_id": prev_state_id, "curr_state_id": symbol_state.symbol_state_id},
                )

    def _upsert_file_touches(self, bundle: CommitBundle) -> None:
        for file_touch in bundle.file_touches:
            self.conn.execute(
                """
                MATCH (c:Commit {commit_sha: $commit_sha}), (fs:FileState {file_state_id: $file_state_id})
                MERGE (c)-[r:TOUCHES]->(fs)
                SET r.change_type = $change_type,
                    r.additions = $additions,
                    r.deletions = $deletions,
                    r.similarity_score = $similarity_score
                """,
                parameters={
                    "commit_sha": file_touch.commit_sha,
                    "file_state_id": file_touch.file_state_id,
                    "change_type": file_touch.change_type,
                    "additions": file_touch.additions,
                    "deletions": file_touch.deletions,
                    "similarity_score": file_touch.similarity_score,
                },
            )

    def _upsert_symbol_touches(self, bundle: CommitBundle) -> None:
        for symbol_touch in bundle.symbol_touches:
            self.conn.execute(
                """
                MATCH (c:Commit {commit_sha: $commit_sha}), (ss:SymbolState {symbol_state_id: $symbol_state_id})
                MERGE (c)-[r:MODIFIES]->(ss)
                SET r.changed_lines = $changed_lines,
                    r.diff_hash = $diff_hash,
                    r.change_kind = $change_kind
                """,
                parameters={
                    "commit_sha": symbol_touch.commit_sha,
                    "symbol_state_id": symbol_touch.symbol_state_id,
                    "changed_lines": ",".join(str(line) for line in symbol_touch.changed_lines),
                    "diff_hash": symbol_touch.diff_hash,
                    "change_kind": symbol_touch.change_kind,
                },
            )

    def _write_csv(self, path: Path, rows: list, columns: list[str]) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            for row in rows:
                writer.writerow({column: getattr(row, column) for column in columns})

    def _write_rel_csv(self, path: Path, rows: list[dict]) -> None:
        columns = list(rows[0].keys()) if rows else ["from", "to"]
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)

    def _should_use_copy_from(self, bundle: CommitBundle, force_copy_from: bool | None = None) -> bool:
        if force_copy_from is not None:
            return force_copy_from
        if not self.bulk_ingest_policy.enable_copy_from:
            return False
        row_count = (
            len(bundle.file_facts)
            + len(bundle.file_states)
            + len(bundle.symbol_facts)
            + len(bundle.symbol_states)
            + len(bundle.calls)
            + len(bundle.file_touches)
            + len(bundle.symbol_touches)
        )
        return row_count >= self.bulk_ingest_policy.copy_from_row_threshold

    def _bulk_copy_bundle(self, bundle: CommitBundle, staging_dir: str | None = None) -> None:
        stage_root = Path(staging_dir) if staging_dir else Path(tempfile.mkdtemp(prefix="history_kuzu_"))
        csv_paths = self.export_snapshot_staging(bundle, str(stage_root))
        self._copy_csv_if_rows("File", csv_paths["files"], len(bundle.file_facts))
        self._copy_csv_if_rows("FileState", csv_paths["file_states"], len(bundle.file_states))
        self._copy_csv_if_rows("Symbol", csv_paths["symbols"], len(bundle.symbol_facts))
        self._copy_csv_if_rows("SymbolState", csv_paths["symbol_states"], len(bundle.symbol_states))
        self._copy_csv_if_rows("OF_FILE", csv_paths["of_file"], len(bundle.file_states))
        self._copy_csv_if_rows("DECLARES", csv_paths["declares"], len(bundle.symbol_states))
        self._copy_csv_if_rows("OF_SYMBOL", csv_paths["of_symbol"], len(bundle.symbol_states))
        self._copy_csv_if_rows("TOUCHES", csv_paths["file_touches"], len(bundle.file_touches))
        self._copy_csv_if_rows("MODIFIES", csv_paths["symbol_touches"], len(bundle.symbol_touches))
        self._copy_csv_if_rows("CALLS", csv_paths["calls"], len(bundle.calls))

    def _copy_csv_if_rows(self, table_name: str, csv_path: str, row_count: int) -> None:
        if row_count <= 0:
            return
        safe_path = Path(csv_path).as_posix().replace("'", "''")
        self.conn.execute(f"COPY {table_name} FROM '{safe_path}' (HEADER=true, PARALLEL=false)")

    def _load_file_facts(self, repo_id: str) -> dict[str, FileFact]:
        result = self.conn.execute(
            """
            MATCH (f:File {repo_id: $repo_id})
            RETURN f.file_id, f.repo_id, f.canonical_path, f.first_seen_commit, f.last_seen_commit, f.status
            """,
            parameters={"repo_id": repo_id},
        )
        rows: dict[str, FileFact] = {}
        while result.has_next():
            row = result.get_next()
            rows[row[0]] = FileFact(
                file_id=row[0],
                repo_id=row[1],
                canonical_path=row[2],
                first_seen_commit=row[3],
                last_seen_commit=row[4],
                status=row[5],
            )
        return rows

    def _load_symbol_facts(self, repo_id: str) -> dict[str, SymbolFact]:
        result = self.conn.execute(
            """
            MATCH (s:Symbol {repo_id: $repo_id})
            RETURN s.symbol_id, s.repo_id, s.file_id, s.kind, s.fq_name, s.canonical_name, s.first_seen_commit, s.last_seen_commit
            """,
            parameters={"repo_id": repo_id},
        )
        rows: dict[str, SymbolFact] = {}
        while result.has_next():
            row = result.get_next()
            rows[row[0]] = SymbolFact(
                symbol_id=row[0],
                repo_id=row[1],
                file_id=row[2],
                kind=row[3],
                fq_name=row[4],
                canonical_name=row[5],
                first_seen_commit=row[6],
                last_seen_commit=row[7],
            )
        return rows

    def _load_latest_file_states(self, repo_id: str) -> dict[str, FileStateFact]:
        result = self.conn.execute(
            """
            MATCH (fs:FileState)-[:OF_FILE]->(f:File {repo_id: $repo_id})
            WHERE fs.valid_to_commit = ''
            RETURN fs.file_state_id, fs.file_id, fs.path_at_commit, fs.blob_sha, fs.language,
                   fs.size_bytes, fs.parse_status, fs.valid_from_commit, fs.valid_to_commit, fs.change_kind
            """,
            parameters={"repo_id": repo_id},
        )
        rows: dict[str, FileStateFact] = {}
        while result.has_next():
            row = result.get_next()
            rows[row[1]] = FileStateFact(
                file_state_id=row[0],
                file_id=row[1],
                path_at_commit=row[2],
                blob_sha=row[3],
                language=row[4],
                size_bytes=row[5],
                parse_status=row[6],
                valid_from_commit=row[7],
                valid_to_commit=row[8],
                change_kind=row[9],
            )
        return rows

    def _load_latest_symbol_states(self, repo_id: str) -> dict[str, SymbolStateFact]:
        result = self.conn.execute(
            """
            MATCH (ss:SymbolState)-[:OF_SYMBOL]->(s:Symbol {repo_id: $repo_id})
            WHERE ss.valid_to_commit = ''
            RETURN ss.symbol_state_id, ss.symbol_id, ss.file_state_id, ss.signature_hash, ss.body_hash,
                   ss.ast_hash, ss.start_line, ss.end_line, ss.valid_from_commit, ss.valid_to_commit,
                   ss.change_kind, ss.content, s.kind, s.canonical_name, s.fq_name
            """,
            parameters={"repo_id": repo_id},
        )
        rows: dict[str, SymbolStateFact] = {}
        while result.has_next():
            row = result.get_next()
            rows[row[1]] = SymbolStateFact(
                symbol_state_id=row[0],
                symbol_id=row[1],
                file_state_id=row[2],
                signature_hash=row[3],
                body_hash=row[4],
                ast_hash=row[5],
                start_line=row[6],
                end_line=row[7],
                valid_from_commit=row[8],
                valid_to_commit=row[9],
                change_kind=row[10],
                content=row[11],
                kind=row[12],
                canonical_name=row[13],
                fq_name=row[14],
            )
        return rows
