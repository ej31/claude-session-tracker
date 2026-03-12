from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from history_config import HistoryPipelineConfig
from history_catalog import HistoryCatalog
from history_embedding import (
    DeterministicSurrogateGenerator,
    QueryRewrite,
    lexical_similarity,
)
from history_extract import GitHistoryExtractor, RepositoryState, initial_state
from history_ids import (
    compute_source_fingerprint,
    detect_branch,
    detect_head,
    detect_remote,
    hash_content,
    is_worktree_clean,
    make_ingest_batch_id,
    make_repo_id,
    normalize_remote,
)
from history_kuzu import TemporalGraphStore
from history_lancedb import LanceHistoryStore
from history_queries import HybridQueryOrchestrator


class RepositoryHistoryPipeline:
    def __init__(
        self,
        repo_dir: str,
        storage_root: str,
        embedding_model=None,
        surrogate_generator: DeterministicSurrogateGenerator | None = None,
        query_rewriter: QueryRewrite | None = None,
        kuzu_config: dict | None = None,
        config: HistoryPipelineConfig | None = None,
    ):
        self.repo_dir = str(Path(repo_dir).resolve())
        self.repo_id = make_repo_id(self.repo_dir)
        self.storage_root = Path(storage_root)
        self.storage_root.mkdir(parents=True, exist_ok=True)
        self.config = config or HistoryPipelineConfig()
        self.surrogate_generator = surrogate_generator or DeterministicSurrogateGenerator()
        self.query_rewriter = query_rewriter or QueryRewrite()
        self.catalog = HistoryCatalog(str(self.storage_root / "catalog.sqlite"))
        self.lance_store = LanceHistoryStore(
            str(self.storage_root / "lancedb"),
            optimize_policy=self.config.lance,
        )
        self.graph_store = TemporalGraphStore(
            str(self.storage_root / "kuzu"),
            bulk_ingest_policy=self.config.kuzu,
            **(kuzu_config or {}),
        )
        self.extractor = GitHistoryExtractor(self.repo_dir, self.surrogate_generator)
        self.query_orchestrator = HybridQueryOrchestrator(
            self.repo_id,
            self.lance_store,
            self.query_rewriter,
        )
        self.catalog.register_surrogate_generator(
            self.surrogate_generator.name,
            self.surrogate_generator.version,
            {"source_label": self.surrogate_generator.source_label},
        )
        self._ingest_repository_once()
        self._state = self._recover_persisted_state()

    def bootstrap(self) -> list[str]:
        self._assert_repo_ready_for_sync()
        return self._ingest_commits(self.extractor.list_commits())

    def sync_incremental(self) -> list[str]:
        self._assert_repo_ready_for_sync()
        watermark = self.catalog.get_watermark(self.repo_id)
        return self._ingest_commits(self.extractor.list_commits(start_after=watermark))

    def repair_pending(self) -> list[str]:
        pending = set(self.catalog.pending_repairs(self.repo_id))
        if not pending:
            return []
        return self._ingest_commits(
            self.extractor.list_commits(include_only=pending),
            force_copy_from=False,
        )

    def load_snapshot_rows(self, commit_sha: str) -> list[dict]:
        return self.lance_store.snapshot_rows(self.repo_id, commit_sha)

    def find_core_developers_for_query(self, query_text: str, since_days: int = 30, limit: int = 10) -> list[dict]:
        since_dt = datetime.now(timezone.utc) - timedelta(days=since_days)
        orchestrated = self.execute_hybrid_query(
            "semantic-first",
            semantic_query=query_text,
            limit=limit,
            where=f"repo_id = '{self.repo_id}' AND author_ts >= '{since_dt.isoformat()}'",
        )
        rows = orchestrated["semantic_rows"]
        summary: dict[str, dict] = {}
        for row in rows:
            developer = self.graph_store.developer_for_commit(row["snapshot_commit_sha"])
            if not developer:
                continue
            entry = summary.setdefault(
                developer["developer_id"],
                {
                    "developer_id": developer["developer_id"],
                    "name": developer["name"],
                    "email_norm": developer["email_norm"],
                    "score": 0.0,
                    "commits": set(),
                },
            )
            entry["score"] += self._result_score(row)
            entry["commits"].add(row["snapshot_commit_sha"])
        result = []
        for entry in summary.values():
            result.append({
                "developer_id": entry["developer_id"],
                "name": entry["name"],
                "email_norm": entry["email_norm"],
                "score": round(entry["score"], 4),
                "commit_count": len(entry["commits"]),
            })
        result.sort(key=lambda row: (-row["score"], -row["commit_count"], row["name"]))
        return result[:limit]

    def analyze_signature_change_impact(self, canonical_name: str) -> list[dict]:
        self._assert_current_state_fresh()
        symbol_states = self.graph_store.latest_symbol_by_name(canonical_name)
        if not symbol_states:
            return []
        impacts = []
        for symbol_state in symbol_states:
            history = self.graph_store.symbol_state_history(symbol_state["symbol_id"])
            previous = None
            current = None
            for row in history:
                if row["symbol_state_id"] == symbol_state["symbol_state_id"]:
                    current = row
                    break
                previous = row
            if not previous or not current:
                continue
            if previous["signature_hash"] == current["signature_hash"]:
                continue
            callers = self.graph_store.callers_of_symbol_state(symbol_state["symbol_state_id"])
            orchestrated = self.execute_hybrid_query(
                "graph-first",
                graph_rows=callers,
                semantic_query=previous["content"],
                limit=5,
                where=(
                    f"repo_id = '{self.repo_id}' AND symbol_state_id <> '{symbol_state['symbol_state_id']}' "
                    "AND symbol_state_id <> ''"
                ),
            )
            alternative_rows = orchestrated["semantic_rows"]
            alternatives = [
                {
                    "symbol_state_id": row["symbol_state_id"],
                    "symbol_id": row["symbol_id"],
                    "path_at_commit": row["path_at_commit"],
                    "score": round(self._result_score(row), 4),
                }
                for row in alternative_rows
            ]
            impacts.append({
                "symbol_id": symbol_state["symbol_id"],
                "symbol_state_id": symbol_state["symbol_state_id"],
                "caller_count": len(callers),
                "callers": callers,
                "alternatives": alternatives,
            })
        return impacts

    def execute_hybrid_query(
        self,
        mode: str,
        *,
        semantic_query: str = "",
        graph_rows: list[dict] | None = None,
        limit: int = 10,
        where: str = "",
    ) -> dict:
        if mode == "fts-first":
            return self.query_orchestrator.fts_first(
                semantic_query,
                where=where,
                limit=limit,
            )
        if mode in self.config.retrieval.compatibility_aliases:
            return self.query_orchestrator.semantic_first(
                semantic_query,
                where=where,
                limit=limit,
            )
        if mode == "graph-first":
            return self.query_orchestrator.graph_first(
                graph_rows or [],
                semantic_query=semantic_query,
                where=where,
                limit=limit,
            )
        raise ValueError(f"Unsupported hybrid query mode: {mode}")

    def profile_query_paths(self, *, canonical_name: str = "open_connection") -> dict:
        explain_rows = self.graph_store.explain(
            "MATCH (ss:SymbolState)-[:OF_SYMBOL]->(s:Symbol) "
            "WHERE s.canonical_name = $canonical_name AND ss.valid_to_commit = '' "
            "RETURN s.symbol_id, ss.symbol_state_id",
            parameters={"canonical_name": canonical_name},
        )
        profile_rows = self.graph_store.profile(
            "MATCH (ss:SymbolState)-[:OF_SYMBOL]->(s:Symbol) "
            "WHERE s.canonical_name = $canonical_name AND ss.valid_to_commit = '' "
            "RETURN s.symbol_id, ss.symbol_state_id",
            parameters={"canonical_name": canonical_name},
        )
        return {
            "graph_explain": explain_rows,
            "graph_profile": profile_rows,
            "lance_storage": self.lance_store.describe_storage(),
        }

    def reindex_fts(self) -> None:
        self.lance_store.rebuild_surrogates(self.repo_id)

    def rebuild_surrogates(self) -> None:
        self.reindex_fts()

    def classify_commit_change(self, commit_sha: str) -> dict:
        touched = self.graph_store.touched_symbol_states(commit_sha)
        if not touched:
            return {"commit_sha": commit_sha, "classification": "unknown", "reason": "no_symbol_touches"}
        similarity_scores = []
        rename_hint = True
        for row in touched:
            history = self.graph_store.symbol_state_history(row["symbol_id"])
            current = next((item for item in history if item["symbol_state_id"] == row["symbol_state_id"]), None)
            current_index = history.index(current) if current in history else -1
            previous = history[current_index - 1] if current_index > 0 else None
            if not previous or not current:
                rename_hint = False
                continue
            similarity_scores.append(lexical_similarity(previous["content"], current["content"]))
            if row["change_kind"] not in {"moved", "renamed", "signature_changed"}:
                rename_hint = False

        touch_stats = self.graph_store.touch_stats(commit_sha)
        average_similarity = sum(similarity_scores) / len(similarity_scores) if similarity_scores else 0.0
        classification = "semantic_change"
        if rename_hint and average_similarity >= 0.92 and touch_stats["symbols"] >= 1:
            classification = "refactor_or_rename"
        return {
            "commit_sha": commit_sha,
            "classification": classification,
            "avg_similarity": round(average_similarity, 4),
            "touch_stats": touch_stats,
        }

    def _ingest_repository_once(self) -> None:
        created_at = datetime.now(timezone.utc).isoformat()
        self.graph_store.ingest_repository(
            self.repo_id,
            normalize_remote(detect_remote(self.repo_dir)),
            self.repo_dir,
            detect_branch(self.repo_dir),
            created_at,
        )

    def _recover_persisted_state(self) -> RepositoryState:
        saved = self.catalog.get_repo_state(self.repo_id)
        graph_state = self.graph_store.load_repository_state(self.repo_id)
        if saved is None or not graph_state["file_facts"]:
            return initial_state()

        commit_files = self.extractor.get_commit_files(saved["head_sha"])
        latest_file_states = graph_state["latest_file_states"]
        for file_state in latest_file_states.values():
            commit_file = commit_files.get(file_state.path_at_commit)
            if not commit_file:
                continue
            file_state.content = commit_file["content"]
            file_state.content_hash = hash_content(commit_file["content"])
            file_state.size_bytes = commit_file["size_bytes"]
            file_state.language = commit_file["language"]

        latest_symbol_states = graph_state["latest_symbol_states"]
        for symbol_state in latest_symbol_states.values():
            if symbol_state.content and not symbol_state.content_hash:
                symbol_state.content_hash = hash_content(symbol_state.content)

        path_to_file_id = {
            file_state.path_at_commit: file_state.file_id
            for file_state in latest_file_states.values()
            if not file_state.valid_to_commit
        }
        return RepositoryState(
            path_to_file_id=path_to_file_id,
            latest_file_states=latest_file_states,
            latest_symbol_states=latest_symbol_states,
            file_facts=graph_state["file_facts"],
            symbol_facts=graph_state["symbol_facts"],
        )

    def _ingest_commits(self, commits: list, force_copy_from: bool | None = None) -> list[str]:
        if not commits:
            return []

        ingest_batch_id = make_ingest_batch_id(self.repo_id)
        started_at = datetime.now(timezone.utc).isoformat()
        self.catalog.start_batch(ingest_batch_id, self.repo_id, started_at)

        committed: list[str] = []
        total = len(commits)
        for index, commit in enumerate(commits, start=1):
            if self.catalog.is_commit_committed(self.repo_id, commit.hexsha):
                continue

            bundle = self.extractor.materialize_commit(
                commit=commit,
                snapshot_seq=index,
                ingest_batch_id=ingest_batch_id,
                previous_state=self._state,
                is_head=(index == total and commit.hexsha == self.extractor.repo.head.commit.hexsha),
            )
            checksum = hash_content(json.dumps({
                "files": [chunk.file_state_id for chunk in bundle.chunks],
                "symbols": [chunk.symbol_state_id for chunk in bundle.chunks],
            }, sort_keys=True))
            row_counts = {
                "chunks": len(bundle.chunks),
                "file_states": len(bundle.file_states),
                "symbol_states": len(bundle.symbol_states),
                "calls": len(bundle.calls),
            }
            history_table = "pending"
            updated_at = datetime.now(timezone.utc).isoformat()
            self.catalog.begin_commit(
                self.repo_id,
                commit.hexsha,
                history_table,
                ingest_batch_id,
                checksum,
                row_counts,
                updated_at,
                retrieval_mode=self.config.retrieval.mode,
                surrogate_version=self.surrogate_generator.version,
                fts_index_version=self.surrogate_generator.version,
            )
            try:
                history_table, lance_version = self.lance_store.upsert_snapshot(
                    self.repo_id,
                    bundle.commit.commit_sha,
                    bundle.commit.committed_at,
                    bundle.chunks,
                )
                self.graph_store.ingest_commit_bundle(
                    bundle,
                    staging_dir=str(self.storage_root / self.config.kuzu.staging_subdir / commit.hexsha[:12]),
                    force_copy_from=force_copy_from,
                )
                self.catalog.begin_commit(
                    self.repo_id,
                    commit.hexsha,
                    history_table,
                    ingest_batch_id,
                    checksum,
                    row_counts,
                    updated_at,
                    retrieval_mode=self.config.retrieval.mode,
                    surrogate_version=self.surrogate_generator.version,
                    fts_index_version=self.surrogate_generator.version,
                )
                self.catalog.mark_commit_committed(
                    self.repo_id,
                    commit.hexsha,
                    lance_version,
                    datetime.now(timezone.utc).isoformat(),
                )
                self.catalog.update_watermark(
                    self.repo_id,
                    commit.hexsha,
                    datetime.now(timezone.utc).isoformat(),
                )
                if (len(committed) + 1) % self.config.lance.optimize_every_n_commits == 0:
                    self.lance_store.optimize_tables(self.repo_id, bundle.commit.committed_at)
                self._state = RepositoryState(
                    path_to_file_id=bundle.current_path_to_file_id,
                    latest_file_states=bundle.latest_file_states,
                    latest_symbol_states=bundle.latest_symbol_states,
                    file_facts={**self._state.file_facts, **{fact.file_id: fact for fact in bundle.file_facts}},
                    symbol_facts={**self._state.symbol_facts, **{fact.symbol_id: fact for fact in bundle.symbol_facts}},
                )
                self._record_repo_state()
                committed.append(commit.hexsha)
            except Exception as exc:
                self.catalog.mark_commit_failed(
                    self.repo_id,
                    commit.hexsha,
                    "ingest",
                    str(exc),
                    datetime.now(timezone.utc).isoformat(),
                    ingest_batch_id,
                )
                self.catalog.finish_batch(
                    ingest_batch_id,
                    datetime.now(timezone.utc).isoformat(),
                    len(committed),
                    "pending_repair",
                )
                raise

        self.catalog.finish_batch(
            ingest_batch_id,
            datetime.now(timezone.utc).isoformat(),
            len(committed),
            "committed",
        )
        return committed

    def _current_repo_state(self) -> dict:
        return {
            "head_sha": detect_head(self.repo_dir),
            "branch": detect_branch(self.repo_dir),
            "source_fingerprint": compute_source_fingerprint(self.repo_dir),
            "is_clean": is_worktree_clean(self.repo_dir),
        }

    def _record_repo_state(self) -> None:
        current = self._current_repo_state()
        self.catalog.update_repo_state(
            self.repo_id,
            current["head_sha"],
            current["branch"],
            current["source_fingerprint"],
            current["is_clean"],
            self.surrogate_generator.version,
            datetime.now(timezone.utc).isoformat(),
        )

    def _assert_repo_ready_for_sync(self) -> None:
        if self.config.freshness.require_clean_worktree and not is_worktree_clean(self.repo_dir):
            raise RuntimeError("History pipeline fail-closed: worktree has uncommitted changes")

    def _assert_current_state_fresh(self) -> None:
        if not self.config.freshness.fail_closed:
            return
        saved = self.catalog.get_repo_state(self.repo_id)
        current = self._current_repo_state()
        if saved is None:
            raise RuntimeError("History pipeline fail-closed: no recorded repo state")
        if self.config.freshness.require_clean_worktree and not current["is_clean"]:
            raise RuntimeError("History pipeline fail-closed: worktree has uncommitted changes")
        if current["head_sha"] != saved["head_sha"] or current["branch"] != saved["branch"]:
            raise RuntimeError("History pipeline fail-closed: repo HEAD/branch changed since last successful sync")
        if saved["surrogate_version"] != self.surrogate_generator.version:
            raise RuntimeError("History pipeline fail-closed: surrogate version changed; rebuild_surrogates() required")
        if (
            self.config.freshness.check_source_fingerprint
            and current["source_fingerprint"] != saved["source_fingerprint"]
        ):
            raise RuntimeError("History pipeline fail-closed: source fingerprint changed since last successful sync")

    @staticmethod
    def _result_score(row: dict) -> float:
        if "_score" in row:
            return float(row["_score"])
        if "_distance" in row:
            return 1.0 - float(row["_distance"])
        return 0.0
