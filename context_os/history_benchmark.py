from __future__ import annotations

import argparse
import json
import math
import re
import resource
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from history_config import BenchmarkPolicy, HistoryPipelineConfig
from history_pipeline import RepositoryHistoryPipeline


@dataclass
class HookSuitabilityVerdict:
    decision: str
    reasons: list[str]
    metrics: dict


def parse_memory_pressure_output(output: str) -> dict[str, float | str]:
    free_pct = 0.0
    for line in output.splitlines():
        if "System-wide memory free percentage" in line:
            free_pct = float(line.rsplit(":", 1)[1].strip().rstrip("%"))
            break
    return {"memory_free_percent": free_pct}


def parse_swap_usage(output: str) -> dict[str, int]:
    values = {"swap_total_bytes": 0, "swap_used_bytes": 0, "swap_free_bytes": 0}
    for key, raw in re.findall(r"(total|used|free)\s*=\s*([0-9.]+[MG])", output):
        multiplier = 1024 * 1024 if raw.endswith("M") else 1024 * 1024 * 1024
        values[f"swap_{key}_bytes"] = int(float(raw[:-1]) * multiplier)
    return values


def classify_memory_pressure(free_percent: float, policy: BenchmarkPolicy) -> str:
    if free_percent <= policy.memory_pressure_red_threshold:
        return "red"
    if free_percent <= policy.memory_pressure_yellow_threshold:
        return "yellow"
    return "green"


def read_memory_pressure(policy: BenchmarkPolicy) -> dict:
    try:
        result = subprocess.run(["memory_pressure", "-Q"], capture_output=True, text=True, timeout=5, check=True)
        parsed = parse_memory_pressure_output(result.stdout)
    except Exception:
        parsed = {"memory_free_percent": 0.0}
    parsed["state"] = classify_memory_pressure(float(parsed["memory_free_percent"]), policy)
    return parsed


def read_swap_usage() -> dict:
    try:
        result = subprocess.run(["sysctl", "vm.swapusage"], capture_output=True, text=True, timeout=5, check=True)
        return parse_swap_usage(result.stdout)
    except Exception:
        return {"swap_total_bytes": 0, "swap_used_bytes": 0, "swap_free_bytes": 0}


def repo_facts(repo: Path) -> dict[str, int | str]:
    tracked = subprocess.run(["git", "ls-files"], cwd=repo, capture_output=True, text=True, check=True).stdout.splitlines()
    commits = subprocess.run(["git", "rev-list", "--count", "HEAD"], cwd=repo, capture_output=True, text=True, check=True).stdout.strip()
    return {"repo": str(repo), "tracked_files": len(tracked), "commit_count": int(commits)}


def storage_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def recent_commit_shas(repo: Path, limit: int) -> list[str]:
    result = subprocess.run(
        ["git", "rev-list", "--max-count", str(limit), "HEAD"],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    )
    commits = result.stdout.splitlines()
    commits.reverse()
    return commits


def measure_operation(fn, *, policy: BenchmarkPolicy) -> dict:
    before_swap = read_swap_usage()
    before_pressure = read_memory_pressure(policy)
    cpu_before = resource.getrusage(resource.RUSAGE_SELF)
    started = time.perf_counter()
    value = fn()
    wall_seconds = time.perf_counter() - started
    cpu_after = resource.getrusage(resource.RUSAGE_SELF)
    after_swap = read_swap_usage()
    after_pressure = read_memory_pressure(policy)
    cpu_seconds = (cpu_after.ru_utime + cpu_after.ru_stime) - (cpu_before.ru_utime + cpu_before.ru_stime)
    max_rss = max(cpu_after.ru_maxrss, cpu_before.ru_maxrss)
    # macOS reports bytes, Linux reports KB. This workspace is macOS.
    peak_rss_bytes = int(max_rss)
    return {
        "value": value,
        "metrics": {
            "wall_seconds": round(wall_seconds, 4),
            "cpu_seconds": round(cpu_seconds, 4),
            "peak_rss_bytes": peak_rss_bytes,
            "memory_pressure_before": before_pressure,
            "memory_pressure_after": after_pressure,
            "swap_before": before_swap,
            "swap_after": after_swap,
            "swap_used_delta_bytes": after_swap.get("swap_used_bytes", 0) - before_swap.get("swap_used_bytes", 0),
        },
    }


def benchmark_head_full_snapshot(repo: Path, storage_root: Path, config: HistoryPipelineConfig | None = None) -> dict:
    if storage_root.exists():
        shutil.rmtree(storage_root)
    pipeline = RepositoryHistoryPipeline(str(repo), str(storage_root), config=config)
    head_commit = pipeline.extractor.repo.head.commit
    measured = measure_operation(
        lambda: pipeline._ingest_commits([head_commit], force_copy_from=False),
        policy=(config or HistoryPipelineConfig()).benchmark,
    )
    rows = pipeline.load_snapshot_rows(head_commit.hexsha)
    return {
        "lane": "head_full_snapshot",
        "head_commit": head_commit.hexsha,
        "ingested_commits": measured["value"],
        "chunk_count": len(rows),
        "storage_bytes": storage_size_bytes(storage_root),
        "metrics": measured["metrics"],
        "pending_repair": len(pipeline.catalog.pending_repairs(pipeline.repo_id)),
        "storage": pipeline.lance_store.describe_storage(),
    }


def benchmark_recent_replay(
    repo: Path,
    storage_root: Path,
    *,
    commit_limit: int,
    batch_size: int,
    config: HistoryPipelineConfig | None = None,
) -> dict:
    if storage_root.exists():
        shutil.rmtree(storage_root)
    config = config or HistoryPipelineConfig()
    pipeline = RepositoryHistoryPipeline(str(repo), str(storage_root), config=config)
    commit_shas = recent_commit_shas(repo, commit_limit)
    commit_map = {commit.hexsha: commit for commit in pipeline.extractor.list_commits(include_only=set(commit_shas))}
    ordered = [commit_map[sha] for sha in commit_shas if sha in commit_map]

    lane_metrics = []
    total_ingested = 0
    for start in range(0, len(ordered), batch_size):
        batch = ordered[start:start + batch_size]
        measured = measure_operation(
            lambda batch=batch: pipeline._ingest_commits(batch, force_copy_from=None),
            policy=config.benchmark,
        )
        total_ingested += len(measured["value"])
        lane_metrics.append(measured["metrics"])

    return {
        "lane": f"recent_replay_{commit_limit}",
        "requested_commits": commit_limit,
        "batch_size": batch_size,
        "ingested_commits": total_ingested,
        "batch_metrics": lane_metrics,
        "storage_bytes": storage_size_bytes(storage_root),
        "pending_repair": len(pipeline.catalog.pending_repairs(pipeline.repo_id)),
    }


def benchmark_metadata_only_scan(repo: Path, policy: BenchmarkPolicy) -> dict:
    def _scan():
        result = subprocess.run(
            ["git", "log", "--pretty=format:%H%x09%P%x09%an%x09%ae", "--name-only"],
            cwd=repo,
            capture_output=True,
            text=True,
            check=True,
        )
        commits = 0
        touched_files = 0
        authors = set()
        for line in result.stdout.splitlines():
            if "\t" in line:
                commits += 1
                parts = line.split("\t")
                if len(parts) >= 4:
                    authors.add(parts[3])
            elif line.strip():
                touched_files += 1
        return {"commits": commits, "touched_files": touched_files, "authors": len(authors)}

    measured = measure_operation(_scan, policy=policy)
    return {
        "lane": "metadata_only_scan",
        "summary": measured["value"],
        "metrics": measured["metrics"],
    }


def benchmark_hook_suitability(
    repo: Path,
    storage_root: Path,
    *,
    query_text: str,
    repeats: int,
    config: HistoryPipelineConfig | None = None,
) -> dict:
    config = config or HistoryPipelineConfig()
    if storage_root.exists():
        shutil.rmtree(storage_root)
    pipeline = RepositoryHistoryPipeline(str(repo), str(storage_root), config=config)
    pipeline.bootstrap()
    hook_runs = []
    for _ in range(repeats):
        measured = measure_operation(
            lambda: pipeline.find_core_developers_for_query(query_text, since_days=365, limit=5),
            policy=config.benchmark,
        )
        hook_runs.append(measured["metrics"])
    verdict = evaluate_hook_suitability(hook_runs, config.benchmark)
    return {
        "hook_runs": hook_runs,
        "verdict": asdict(verdict),
    }


def evaluate_hook_suitability(run_metrics: list[dict], policy: BenchmarkPolicy) -> HookSuitabilityVerdict:
    wall = sorted(metric["wall_seconds"] for metric in run_metrics)
    p95_index = max(0, min(len(wall) - 1, int(math.ceil(len(wall) * 0.95)) - 1))
    warm_p95 = wall[p95_index]
    peak_rss = max(metric["peak_rss_bytes"] for metric in run_metrics)
    min_free_percent = min(metric["memory_pressure_after"]["memory_free_percent"] for metric in run_metrics)
    max_swap_delta = max(metric["swap_used_delta_bytes"] for metric in run_metrics)
    reasons = []
    decision = "hook-safe"
    if warm_p95 > policy.hook_warm_p95_max_seconds:
        decision = "withdraw"
        reasons.append("warm p95 wall time exceeds hard limit")
    elif warm_p95 > policy.hook_warm_p95_goal_seconds:
        reasons.append("warm p95 wall time exceeds target")

    if peak_rss > policy.hook_peak_rss_max_bytes:
        decision = "withdraw"
        reasons.append("peak RSS exceeds hard limit")
    elif peak_rss > policy.hook_peak_rss_goal_bytes:
        reasons.append("peak RSS exceeds target")

    pressure_states = [metric["memory_pressure_after"]["state"] for metric in run_metrics]
    if "red" in pressure_states:
        decision = "withdraw"
        reasons.append("memory pressure reached red")
    elif "yellow" in pressure_states:
        reasons.append("memory pressure reached yellow")

    if max_swap_delta > 0:
        reasons.append("swap usage increased during hook run")
        if decision != "withdraw":
            decision = "hook-safe"

    if not reasons:
        decision = "keep"
        reasons.append("hook run stayed within wall time, RSS, pressure, and swap limits")

    return HookSuitabilityVerdict(
        decision=decision,
        reasons=reasons,
        metrics={
            "warm_p95_seconds": warm_p95,
            "peak_rss_bytes": peak_rss,
            "min_memory_free_percent": min_free_percent,
            "max_swap_delta_bytes": max_swap_delta,
        },
    )


def build_ffmpeg_report(repo: Path, storage_root: Path, config: HistoryPipelineConfig | None = None) -> dict:
    config = config or HistoryPipelineConfig()
    report = {
        "repo": repo_facts(repo),
        "lane_a": benchmark_head_full_snapshot(repo, storage_root / "lane_a", config=config),
        "lane_b": benchmark_recent_replay(repo, storage_root / "lane_b", commit_limit=1000, batch_size=100, config=config),
        "lane_c": benchmark_recent_replay(repo, storage_root / "lane_c", commit_limit=10000, batch_size=100, config=config),
        "lane_d": benchmark_metadata_only_scan(repo, config.benchmark),
        "hook": benchmark_hook_suitability(repo, storage_root / "hook", query_text="database connection", repeats=3, config=config),
    }
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="History pipeline ffmpeg benchmark and hook suitability runner")
    parser.add_argument("--repo", default="/Users/yimtaejong/IdeaProjects/ffmpeg")
    parser.add_argument("--storage-root", default="./.history-bench")
    parser.add_argument("--mode", choices=["ffmpeg-report", "head", "replay", "metadata", "hook"], default="ffmpeg-report")
    parser.add_argument("--commit-limit", type=int, default=1000)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--query", default="database connection")
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()

    repo = Path(args.repo).expanduser().resolve()
    storage_root = Path(args.storage_root).expanduser().resolve()
    config = HistoryPipelineConfig()

    if args.mode == "head":
        report = benchmark_head_full_snapshot(repo, storage_root, config=config)
    elif args.mode == "replay":
        report = benchmark_recent_replay(repo, storage_root, commit_limit=args.commit_limit, batch_size=args.batch_size, config=config)
    elif args.mode == "metadata":
        report = benchmark_metadata_only_scan(repo, config.benchmark)
    elif args.mode == "hook":
        report = benchmark_hook_suitability(repo, storage_root, query_text=args.query, repeats=args.repeats, config=config)
    else:
        report = build_ffmpeg_report(repo, storage_root, config=config)
    json.dump(report, fp=sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
