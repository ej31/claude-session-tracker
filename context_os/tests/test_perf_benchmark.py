"""perf_benchmark.py helper tests."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from perf_benchmark import parse_time_output, project_hourly_cpu_seconds, summarize_findings


def test_parse_time_output_macos_lp():
    metrics = parse_time_output(
        "\n".join(
            [
                "real 0.31",
                "user 0.17",
                "sys 0.13",
                "           163414016  maximum resident set size",
                "           145983424  peak memory footprint",
            ]
        )
    )
    assert metrics["real_seconds"] == 0.31
    assert metrics["cpu_seconds"] == pytest.approx(0.30)
    assert metrics["max_rss_bytes"] == 163414016
    assert metrics["peak_memory_bytes"] == 145983424


def test_parse_time_output_gnu_time_v():
    metrics = parse_time_output(
        "\n".join(
            [
                "User time (seconds): 0.42",
                "System time (seconds): 0.18",
                "Maximum resident set size (kbytes): 2048",
            ]
        )
    )
    assert metrics["cpu_seconds"] == pytest.approx(0.60)
    assert metrics["max_rss_bytes"] == 2048 * 1024
    assert metrics["peak_memory_bytes"] == 2048 * 1024


def test_project_hourly_cpu_seconds():
    total = project_hourly_cpu_seconds(
        {"cpu_seconds": 0.37},
        {"cpu_seconds": 0.36},
        edits_per_hour=60,
        turns_per_hour=20,
    )
    assert total == 29.4


def test_summarize_findings_flags_sync_and_background_risks():
    findings = summarize_findings(
        {
            "embedding": {"avg_us_per_embed": 180},
            "scenarios": {
                "cold_build_full": {"metrics": {"wall_seconds": 40.5}},
                "cold_build_code_only": {"metrics": {"wall_seconds": 35.7}},
                "compact_briefing_fresh": {"metrics": {"wall_seconds": 0.31}},
                "async_edit_hook_fresh": {"metrics": {"peak_memory_bytes": 152_000_000}},
                "async_turn_hook_fresh": {"metrics": {"peak_memory_bytes": 150_000_000}},
            },
            "usage_profile": {
                "projected_hourly_cpu_seconds": 29.4,
                "background_cpu_budget_seconds": 20.0,
            },
        }
    )
    assert any("Embedding is not the primary bottleneck" in item for item in findings)
    assert any("Cold full rebuild is too slow" in item for item in findings)
    assert any("Cold code-only rebuild is heavy enough" in item for item in findings)
    assert any("Projected background CPU burn exceeds" in item for item in findings)
    assert any("memory footprints" in item for item in findings)
