from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("lancedb")
pytest.importorskip("pyarrow")

sys.path.insert(0, str(Path(__file__).parent.parent))
from tests.test_history_pipeline import history_repo  # reuse fixture
from history_benchmark import (
    benchmark_head_full_snapshot,
    benchmark_hook_suitability,
    classify_memory_pressure,
    evaluate_hook_suitability,
    parse_memory_pressure_output,
    parse_swap_usage,
)
from history_config import BenchmarkPolicy, HistoryPipelineConfig


def test_parse_memory_pressure_output():
    parsed = parse_memory_pressure_output(
        "System-wide memory free percentage: 12%\n"
    )
    assert parsed["memory_free_percent"] == 12.0


def test_parse_swap_usage():
    parsed = parse_swap_usage("vm.swapusage: total = 4096.00M  used = 2551.00M  free = 1545.00M")
    assert parsed["swap_total_bytes"] == 4096 * 1024 * 1024
    assert parsed["swap_used_bytes"] == 2551 * 1024 * 1024


def test_classify_memory_pressure():
    policy = BenchmarkPolicy(memory_pressure_yellow_threshold=15.0, memory_pressure_red_threshold=5.0)
    assert classify_memory_pressure(20.0, policy) == "green"
    assert classify_memory_pressure(10.0, policy) == "yellow"
    assert classify_memory_pressure(4.0, policy) == "red"


def test_evaluate_hook_suitability():
    policy = BenchmarkPolicy(
        hook_warm_p95_goal_seconds=2.0,
        hook_warm_p95_max_seconds=5.0,
        hook_peak_rss_goal_bytes=150,
        hook_peak_rss_max_bytes=300,
    )
    verdict = evaluate_hook_suitability(
        [
            {
                "wall_seconds": 6.0,
                "peak_rss_bytes": 400,
                "memory_pressure_after": {"state": "red", "memory_free_percent": 4.0},
                "swap_used_delta_bytes": 1,
            }
        ],
        policy,
    )
    assert verdict.decision == "withdraw"
    assert verdict.reasons


def test_benchmark_head_full_snapshot_smoke(history_repo, tmp_path):
    repo_dir, commits = history_repo
    report = benchmark_head_full_snapshot(
        repo_dir,
        tmp_path / "bench_store",
        config=HistoryPipelineConfig(),
    )
    assert report["head_commit"] == commits["commit2"]
    assert report["chunk_count"] > 0
    assert report["pending_repair"] == 0


def test_benchmark_hook_suitability_smoke(history_repo, tmp_path):
    repo_dir, _ = history_repo
    report = benchmark_hook_suitability(
        repo_dir,
        tmp_path / "hook_store",
        query_text="database connection timeout",
        repeats=2,
        config=HistoryPipelineConfig(),
    )
    assert len(report["hook_runs"]) == 2
    assert report["verdict"]["decision"] in {"keep", "hook-safe", "withdraw"}
