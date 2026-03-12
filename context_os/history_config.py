from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class RetrievalPolicy:
    mode: str = "BM25_ONLY"
    fts_field: str = "search_text"
    compatibility_aliases: tuple[str, ...] = ("semantic-first", "fts-first")


@dataclass
class LanceOptimizePolicy:
    cleanup_older_than_days: int = 30
    min_rows_for_fts_index: int = 8
    optimize_every_n_commits: int = 1
    recreate_fts_every_n_commits: int = 1


@dataclass
class KuzuBulkIngestPolicy:
    enable_copy_from: bool = True
    copy_from_row_threshold: int = 25
    staging_subdir: str = "kuzu_staging"


@dataclass
class FreshnessPolicy:
    fail_closed: bool = True
    require_clean_worktree: bool = True
    check_source_fingerprint: bool = True


@dataclass
class BenchmarkPolicy:
    hook_warm_p95_goal_seconds: float = 2.0
    hook_warm_p95_max_seconds: float = 5.0
    hook_peak_rss_goal_bytes: int = 1_500_000_000
    hook_peak_rss_max_bytes: int = 3_000_000_000
    memory_pressure_yellow_threshold: float = 15.0
    memory_pressure_red_threshold: float = 5.0


@dataclass
class HistoryPipelineConfig:
    retrieval: RetrievalPolicy = field(default_factory=RetrievalPolicy)
    lance: LanceOptimizePolicy = field(default_factory=LanceOptimizePolicy)
    kuzu: KuzuBulkIngestPolicy = field(default_factory=KuzuBulkIngestPolicy)
    freshness: FreshnessPolicy = field(default_factory=FreshnessPolicy)
    benchmark: BenchmarkPolicy = field(default_factory=BenchmarkPolicy)
