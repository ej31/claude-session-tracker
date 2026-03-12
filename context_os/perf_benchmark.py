#!/usr/bin/env python3
"""Benchmark Context OS user-visible and background overhead.

This script focuses on the costs that matter for a "silent background"
experience:
- cold graph builds
- fresh-scope compact briefing
- fresh-scope async hook ingestion
- local embedding overhead

Battery percentage is intentionally not estimated here because it depends on the
machine, thermals, battery health, and OS scheduling. Instead, the report emits
wall time, CPU time, and memory proxies that can be validated on-device.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from history_embedding import HashEmbeddingModel


MACOS_TIME_PREFIX = ["/usr/bin/time", "-lp"]
GNU_TIME_PREFIX = ["/usr/bin/time", "-v"]


def parse_time_output(stderr: str) -> dict[str, float | int]:
    metrics: dict[str, float | int] = {}
    patterns: list[tuple[re.Pattern[str], str, callable | None]] = [
        (re.compile(r"^real\s+([0-9.]+)$", re.MULTILINE), "real_seconds", float),
        (re.compile(r"^user\s+([0-9.]+)$", re.MULTILINE), "user_seconds", float),
        (re.compile(r"^sys\s+([0-9.]+)$", re.MULTILINE), "sys_seconds", float),
        (
            re.compile(r"^\s*(\d+)\s+maximum resident set size$", re.MULTILINE),
            "max_rss_bytes",
            int,
        ),
        (
            re.compile(r"^\s*(\d+)\s+peak memory footprint$", re.MULTILINE),
            "peak_memory_bytes",
            int,
        ),
        (
            re.compile(r"^User time \(seconds\):\s*([0-9.]+)$", re.MULTILINE),
            "user_seconds",
            float,
        ),
        (
            re.compile(r"^System time \(seconds\):\s*([0-9.]+)$", re.MULTILINE),
            "sys_seconds",
            float,
        ),
        (
            re.compile(r"^Maximum resident set size \(kbytes\):\s*(\d+)$", re.MULTILINE),
            "max_rss_bytes",
            lambda value: int(value) * 1024,
        ),
    ]
    for pattern, key, cast in patterns:
        match = pattern.search(stderr)
        if match:
            metrics[key] = cast(match.group(1)) if cast is not None else match.group(1)
    if "peak_memory_bytes" not in metrics and "max_rss_bytes" in metrics:
        metrics["peak_memory_bytes"] = metrics["max_rss_bytes"]
    if "user_seconds" in metrics or "sys_seconds" in metrics:
        metrics["cpu_seconds"] = float(metrics.get("user_seconds", 0.0)) + float(metrics.get("sys_seconds", 0.0))
    return metrics


def measure_command(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    stdin: str | None = None,
) -> dict:
    env_vars = {**os.environ, **(env or {})}
    wrapped = command
    if sys.platform == "darwin" and Path(MACOS_TIME_PREFIX[0]).exists():
        wrapped = MACOS_TIME_PREFIX + command
    elif Path(GNU_TIME_PREFIX[0]).exists():
        wrapped = GNU_TIME_PREFIX + command

    started = time.perf_counter()
    result = subprocess.run(
        wrapped,
        cwd=cwd,
        env=env_vars,
        input=stdin,
        capture_output=True,
        text=True,
    )
    wall_seconds = time.perf_counter() - started
    metrics = parse_time_output(result.stderr)
    metrics["wall_seconds"] = wall_seconds
    return {
        "command": command,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "metrics": metrics,
    }


def repo_facts(repo: Path) -> dict[str, int | str]:
    tracked = subprocess.run(
        ["git", "ls-files"],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    commits = subprocess.run(
        ["git", "rev-list", "--count", "HEAD"],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    return {
        "repo": str(repo),
        "tracked_files": len(tracked),
        "commit_count": int(commits),
    }


def benchmark_embedding(repo: Path, *, embeds_per_run: int, runs: int, sample_chars: int) -> dict:
    sample_path = repo / "README.md"
    sample_text = sample_path.read_text(encoding="utf-8")[:sample_chars]
    model = HashEmbeddingModel()
    timings = []
    for _ in range(runs):
        started = time.perf_counter()
        for _ in range(embeds_per_run):
            model.embed(sample_text)
        timings.append(time.perf_counter() - started)
    average = sum(timings) / len(timings)
    timings.sort()
    midpoint = len(timings) // 2
    p50 = timings[midpoint] if len(timings) % 2 == 1 else (timings[midpoint - 1] + timings[midpoint]) / 2
    return {
        "model": model.name,
        "dimension": model.dimension,
        "sample_chars": len(sample_text),
        "embeds_per_run": embeds_per_run,
        "runs": runs,
        "p50_ms_per_run": round(p50 * 1000, 3),
        "avg_us_per_embed": round((average / embeds_per_run) * 1_000_000, 3),
    }


def _python_c(code: str) -> list[str]:
    return ["python3", "-c", textwrap.dedent(code)]


def _build_command(repo: Path, *, code_only: bool) -> list[str]:
    command = ["python3", str(repo / "context_os" / "build_context_os.py"), "--repo", str(repo)]
    if code_only:
        command.append("--code-only")
    return command


def _seed_scope(repo: Path, *, home: str, code_only: bool) -> dict:
    return measure_command(_build_command(repo, code_only=code_only), cwd=repo, env={"HOME": home})


def _fresh_check_command(include_git_history: bool) -> list[str]:
    return _python_c(
        f"""
        import json
        import sys
        import time
        sys.path.insert(0, 'context_os')
        from build_context_os import ensure_scope_current

        started = time.perf_counter()
        ok = ensure_scope_current('.', include_git_history={include_git_history})
        print(json.dumps({{'ok': ok, 'wall_ms': round((time.perf_counter() - started) * 1000, 3)}}))
        """
    )


def _briefing_command(repo: Path) -> list[str]:
    return ["python3", str(repo / "context_os" / "context_briefing.py")]


def _edit_command(repo: Path) -> list[str]:
    return ["python3", str(repo / "context_os" / "kuzu_ingest_edit.py")]


def _turn_command(repo: Path) -> list[str]:
    return ["python3", str(repo / "context_os" / "kuzu_ingest_turn.py")]


def project_hourly_cpu_seconds(
    edit_metrics: dict[str, float | int],
    turn_metrics: dict[str, float | int],
    *,
    edits_per_hour: int,
    turns_per_hour: int,
) -> float:
    edit_cpu = float(edit_metrics.get("cpu_seconds", 0.0))
    turn_cpu = float(turn_metrics.get("cpu_seconds", 0.0))
    return round((edit_cpu * edits_per_hour) + (turn_cpu * turns_per_hour), 3)


def summarize_findings(report: dict) -> list[str]:
    findings = []
    embedding = report["embedding"]
    cold_full = report["scenarios"]["cold_build_full"]["metrics"]
    cold_code = report["scenarios"]["cold_build_code_only"]["metrics"]
    briefing = report["scenarios"]["compact_briefing_fresh"]["metrics"]
    edit = report["scenarios"]["async_edit_hook_fresh"]["metrics"]
    turn = report["scenarios"]["async_turn_hook_fresh"]["metrics"]
    usage = report["usage_profile"]

    if float(embedding["avg_us_per_embed"]) < 1000:
        findings.append("Embedding is not the primary bottleneck on this repo; graph sync and hook startup dominate.")
    if float(cold_full.get("wall_seconds", 0.0)) > 5:
        findings.append("Cold full rebuild is too slow for any synchronous compact path; keep it install/manual-only.")
    if float(cold_code.get("wall_seconds", 0.0)) > 5:
        findings.append("Cold code-only rebuild is heavy enough that async hooks need debounce or batching.")
    if float(briefing.get("wall_seconds", 0.0)) > 0.5:
        findings.append("Fresh compact briefing is above 500ms and may be noticeable to users.")
    if usage["projected_hourly_cpu_seconds"] > usage["background_cpu_budget_seconds"]:
        findings.append("Projected background CPU burn exceeds the configured hourly budget for silent mode.")
    if float(edit.get("peak_memory_bytes", 0)) > 120_000_000 or float(turn.get("peak_memory_bytes", 0)) > 120_000_000:
        findings.append("Async hook processes have six-figure MB memory footprints, so repeated process churn can affect battery.")
    return findings


def clean_result(result: dict) -> dict:
    return {
        "returncode": result["returncode"],
        "metrics": result["metrics"],
        "stdout_preview": result["stdout"][:400],
        "stderr_preview": result["stderr"][:400],
    }


def build_report(
    repo: Path,
    *,
    embeds_per_run: int,
    embedding_runs: int,
    sample_chars: int,
    edits_per_hour: int,
    turns_per_hour: int,
    background_cpu_budget_seconds: float,
) -> dict:
    report = {
        "repo": repo_facts(repo),
        "embedding": benchmark_embedding(
            repo,
            embeds_per_run=embeds_per_run,
            runs=embedding_runs,
            sample_chars=sample_chars,
        ),
        "scenarios": {},
    }

    with tempfile.TemporaryDirectory(prefix="contextos_perf_full_") as home:
        cold_full = _seed_scope(repo, home=home, code_only=False)
        if cold_full["returncode"] != 0:
            raise RuntimeError(f"cold full build failed: {cold_full['stderr'][-500:]}")
        report["scenarios"]["cold_build_full"] = clean_result(cold_full)
        report["scenarios"]["ensure_scope_current_full_fresh"] = clean_result(
            measure_command(
                _fresh_check_command(True),
                cwd=repo,
                env={"HOME": home},
            )
        )
        report["scenarios"]["ensure_scope_current_code_fresh"] = clean_result(
            measure_command(
                _fresh_check_command(False),
                cwd=repo,
                env={"HOME": home},
            )
        )
        report["scenarios"]["compact_briefing_fresh"] = clean_result(
            measure_command(
                _briefing_command(repo),
                cwd=repo,
                env={"HOME": home},
                stdin=json.dumps({"session_id": "perf-bench", "cwd": str(repo)}),
            )
        )

    with tempfile.TemporaryDirectory(prefix="contextos_perf_code_") as home:
        cold_code = _seed_scope(repo, home=home, code_only=True)
        if cold_code["returncode"] != 0:
            raise RuntimeError(f"cold code-only build failed: {cold_code['stderr'][-500:]}")
        report["scenarios"]["cold_build_code_only"] = clean_result(cold_code)
        report["scenarios"]["async_edit_hook_fresh"] = clean_result(
            measure_command(
                _edit_command(repo),
                cwd=repo,
                env={"HOME": home},
                stdin=json.dumps(
                    {
                        "tool_name": "Edit",
                        "tool_input": {"file_path": str(repo / "README.md")},
                        "session_id": "perf-bench",
                        "cwd": str(repo),
                    }
                ),
            )
        )
        report["scenarios"]["async_turn_hook_fresh"] = clean_result(
            measure_command(
                _turn_command(repo),
                cwd=repo,
                env={"HOME": home},
                stdin=json.dumps(
                    {
                        "session_id": "perf-bench",
                        "last_assistant_message": "Use open_connection() from `db.py`.",
                        "cwd": str(repo),
                    }
                ),
            )
        )

    edit_metrics = report["scenarios"]["async_edit_hook_fresh"]["metrics"]
    turn_metrics = report["scenarios"]["async_turn_hook_fresh"]["metrics"]
    report["usage_profile"] = {
        "edits_per_hour": edits_per_hour,
        "turns_per_hour": turns_per_hour,
        "background_cpu_budget_seconds": background_cpu_budget_seconds,
        "projected_hourly_cpu_seconds": project_hourly_cpu_seconds(
            edit_metrics,
            turn_metrics,
            edits_per_hour=edits_per_hour,
            turns_per_hour=turns_per_hour,
        ),
    }
    report["findings"] = summarize_findings(report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Context OS latency and background-cost benchmark")
    parser.add_argument("--repo", default=".", help="Target repository path")
    parser.add_argument("--embeds-per-run", type=int, default=1000, help="Embedding repetitions per run")
    parser.add_argument("--embedding-runs", type=int, default=20, help="Number of embedding benchmark runs")
    parser.add_argument("--sample-chars", type=int, default=2000, help="Embedding sample length")
    parser.add_argument("--edits-per-hour", type=int, default=60, help="Assumed edit hook frequency")
    parser.add_argument("--turns-per-hour", type=int, default=20, help="Assumed stop hook frequency")
    parser.add_argument(
        "--background-cpu-budget-seconds",
        type=float,
        default=20.0,
        help="Hourly CPU budget for silent-mode background activity",
    )
    args = parser.parse_args()

    repo = Path(args.repo).expanduser().resolve()
    report = build_report(
        repo,
        embeds_per_run=args.embeds_per_run,
        embedding_runs=args.embedding_runs,
        sample_chars=args.sample_chars,
        edits_per_hour=args.edits_per_hour,
        turns_per_hour=args.turns_per_hour,
        background_cpu_budget_seconds=args.background_cpu_budget_seconds,
    )
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
