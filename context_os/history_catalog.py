from __future__ import annotations

import json
import sqlite3
from pathlib import Path


class HistoryCatalog:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS repo_watermark (
                repo_id TEXT PRIMARY KEY,
                last_commit_sha TEXT NOT NULL,
                last_success_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS commit_manifest (
                repo_id TEXT NOT NULL,
                commit_sha TEXT NOT NULL,
                history_table TEXT NOT NULL,
                lance_version_at_ingest INTEGER DEFAULT -1,
                kuzu_batch_id TEXT NOT NULL,
                status TEXT NOT NULL,
                row_counts TEXT NOT NULL,
                checksum TEXT NOT NULL,
                error_message TEXT DEFAULT '',
                updated_at TEXT NOT NULL,
                PRIMARY KEY (repo_id, commit_sha)
            );

            CREATE TABLE IF NOT EXISTS batch_run (
                ingest_batch_id TEXT PRIMARY KEY,
                repo_id TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT DEFAULT '',
                commit_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS embedding_models (
                model_name TEXT PRIMARY KEY,
                dimension INTEGER NOT NULL,
                metadata_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS surrogate_generators (
                generator_name TEXT PRIMARY KEY,
                version TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS repo_state (
                repo_id TEXT PRIMARY KEY,
                head_sha TEXT NOT NULL,
                branch TEXT NOT NULL,
                source_fingerprint TEXT NOT NULL,
                is_clean INTEGER NOT NULL,
                surrogate_version TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ingest_errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repo_id TEXT NOT NULL,
                commit_sha TEXT NOT NULL,
                ingest_batch_id TEXT NOT NULL,
                stage TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
        self._ensure_column("commit_manifest", "retrieval_mode", "TEXT NOT NULL DEFAULT 'BM25_ONLY'")
        self._ensure_column("commit_manifest", "surrogate_version", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column("commit_manifest", "fts_index_version", "TEXT NOT NULL DEFAULT ''")
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def register_embedding_model(self, model_name: str, dimension: int, metadata: dict) -> None:
        self.conn.execute(
            """
            INSERT INTO embedding_models(model_name, dimension, metadata_json)
            VALUES(?, ?, ?)
            ON CONFLICT(model_name) DO UPDATE SET
                dimension=excluded.dimension,
                metadata_json=excluded.metadata_json
            """,
            (model_name, dimension, json.dumps(metadata, sort_keys=True)),
        )
        self.conn.commit()

    def register_surrogate_generator(self, generator_name: str, version: str, metadata: dict) -> None:
        self.conn.execute(
            """
            INSERT INTO surrogate_generators(generator_name, version, metadata_json)
            VALUES(?, ?, ?)
            ON CONFLICT(generator_name) DO UPDATE SET
                version=excluded.version,
                metadata_json=excluded.metadata_json
            """,
            (generator_name, version, json.dumps(metadata, sort_keys=True)),
        )
        self.conn.commit()

    def start_batch(self, ingest_batch_id: str, repo_id: str, started_at: str) -> None:
        self.conn.execute(
            """
            INSERT INTO batch_run(ingest_batch_id, repo_id, status, started_at, commit_count)
            VALUES(?, ?, 'running', ?, 0)
            ON CONFLICT(ingest_batch_id) DO UPDATE SET
                status='running',
                started_at=excluded.started_at
            """,
            (ingest_batch_id, repo_id, started_at),
        )
        self.conn.commit()

    def finish_batch(self, ingest_batch_id: str, finished_at: str, commit_count: int, status: str) -> None:
        self.conn.execute(
            """
            UPDATE batch_run
            SET status=?, finished_at=?, commit_count=?
            WHERE ingest_batch_id=?
            """,
            (status, finished_at, commit_count, ingest_batch_id),
        )
        self.conn.commit()

    def begin_commit(
        self,
        repo_id: str,
        commit_sha: str,
        history_table: str,
        kuzu_batch_id: str,
        checksum: str,
        row_counts: dict,
        updated_at: str,
        retrieval_mode: str = "BM25_ONLY",
        surrogate_version: str = "",
        fts_index_version: str = "",
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO commit_manifest(
                repo_id, commit_sha, history_table, lance_version_at_ingest,
                kuzu_batch_id, status, row_counts, checksum, error_message, updated_at,
                retrieval_mode, surrogate_version, fts_index_version
            )
            VALUES(?, ?, ?, -1, ?, 'staging', ?, ?, '', ?, ?, ?, ?)
            ON CONFLICT(repo_id, commit_sha) DO UPDATE SET
                history_table=excluded.history_table,
                kuzu_batch_id=excluded.kuzu_batch_id,
                status='staging',
                row_counts=excluded.row_counts,
                checksum=excluded.checksum,
                retrieval_mode=excluded.retrieval_mode,
                surrogate_version=excluded.surrogate_version,
                fts_index_version=excluded.fts_index_version,
                error_message='',
                updated_at=excluded.updated_at
            """,
            (
                repo_id,
                commit_sha,
                history_table,
                kuzu_batch_id,
                json.dumps(row_counts, sort_keys=True),
                checksum,
                updated_at,
                retrieval_mode,
                surrogate_version,
                fts_index_version,
            ),
        )
        self.conn.commit()

    def mark_commit_committed(
        self,
        repo_id: str,
        commit_sha: str,
        lance_version: int,
        updated_at: str,
    ) -> None:
        self.conn.execute(
            """
            UPDATE commit_manifest
            SET status='committed',
                lance_version_at_ingest=?,
                updated_at=?,
                error_message=''
            WHERE repo_id=? AND commit_sha=?
            """,
            (lance_version, updated_at, repo_id, commit_sha),
        )
        self.conn.commit()

    def mark_commit_failed(
        self,
        repo_id: str,
        commit_sha: str,
        stage: str,
        message: str,
        updated_at: str,
        ingest_batch_id: str,
    ) -> None:
        self.conn.execute(
            """
            UPDATE commit_manifest
            SET status='pending_repair', error_message=?, updated_at=?
            WHERE repo_id=? AND commit_sha=?
            """,
            (f"{stage}: {message}", updated_at, repo_id, commit_sha),
        )
        self.conn.execute(
            """
            INSERT INTO ingest_errors(repo_id, commit_sha, ingest_batch_id, stage, message, created_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (repo_id, commit_sha, ingest_batch_id, stage, message, updated_at),
        )
        self.conn.commit()

    def update_watermark(self, repo_id: str, commit_sha: str, updated_at: str) -> None:
        self.conn.execute(
            """
            INSERT INTO repo_watermark(repo_id, last_commit_sha, last_success_at)
            VALUES(?, ?, ?)
            ON CONFLICT(repo_id) DO UPDATE SET
                last_commit_sha=excluded.last_commit_sha,
                last_success_at=excluded.last_success_at
            """,
            (repo_id, commit_sha, updated_at),
        )
        self.conn.commit()

    def update_repo_state(
        self,
        repo_id: str,
        head_sha: str,
        branch: str,
        source_fingerprint: str,
        is_clean: bool,
        surrogate_version: str,
        updated_at: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO repo_state(repo_id, head_sha, branch, source_fingerprint, is_clean, surrogate_version, updated_at)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(repo_id) DO UPDATE SET
                head_sha=excluded.head_sha,
                branch=excluded.branch,
                source_fingerprint=excluded.source_fingerprint,
                is_clean=excluded.is_clean,
                surrogate_version=excluded.surrogate_version,
                updated_at=excluded.updated_at
            """,
            (repo_id, head_sha, branch, source_fingerprint, int(is_clean), surrogate_version, updated_at),
        )
        self.conn.commit()

    def get_repo_state(self, repo_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM repo_state WHERE repo_id=?",
            (repo_id,),
        ).fetchone()

    def get_watermark(self, repo_id: str) -> str:
        row = self.conn.execute(
            "SELECT last_commit_sha FROM repo_watermark WHERE repo_id=?",
            (repo_id,),
        ).fetchone()
        return row["last_commit_sha"] if row else ""

    def is_commit_committed(self, repo_id: str, commit_sha: str) -> bool:
        row = self.conn.execute(
            """
            SELECT status FROM commit_manifest
            WHERE repo_id=? AND commit_sha=?
            """,
            (repo_id, commit_sha),
        ).fetchone()
        return bool(row and row["status"] == "committed")

    def pending_repairs(self, repo_id: str) -> list[str]:
        rows = self.conn.execute(
            """
            SELECT commit_sha FROM commit_manifest
            WHERE repo_id=? AND status='pending_repair'
            ORDER BY updated_at ASC
            """,
            (repo_id,),
        ).fetchall()
        return [row["commit_sha"] for row in rows]

    def get_manifest(self, repo_id: str, commit_sha: str) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT * FROM commit_manifest
            WHERE repo_id=? AND commit_sha=?
            """,
            (repo_id, commit_sha),
        ).fetchone()

    def _ensure_column(self, table: str, column: str, ddl: str) -> None:
        rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        existing = {row[1] for row in rows}
        if column in existing:
            return
        self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
