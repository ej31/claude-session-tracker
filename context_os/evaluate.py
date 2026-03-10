#!/usr/bin/env python3
"""Context OS - 평가 프레임워크

compact 전후 맥락 복원 품질을 측정한다.

사용법
  python3 evaluate.py checkpoint                                        # ground truth 저장
  python3 evaluate.py score --recall 4 --accuracy 3 --continuation 5    # 점수 기록
  python3 evaluate.py compare                                           # ground truth vs briefing 비교
  python3 evaluate.py report                                            # 결과 CSV 출력
"""
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from build_context_os import get_db_connection, setup_logger

logger = setup_logger("context-os-eval")

EVAL_DIR = Path("~/.claude/context_os/eval").expanduser()


def _ensure_eval_dir() -> None:
    EVAL_DIR.mkdir(parents=True, exist_ok=True)


def _get_eval_path(name: str) -> Path:
    _ensure_eval_dir()
    return EVAL_DIR / name


def _get_git_diff(cwd: str = ".") -> str:
    """현재 git diff 통계 조회"""
    try:
        result = subprocess.run(
            ["git", "diff", "--stat"],
            capture_output=True,
            text=True,
            cwd=cwd,
            timeout=10,
        )
        return result.stdout.strip()
    except Exception:
        return ""


def _get_recent_symbols(conn) -> list:
    """DB에서 최근 수정된 심볼 조회"""
    result = conn.execute(
        "MATCH (t:Turn)-[:MODIFIED_BY]->(s:Symbol) "
        "WITH s, max(t.timestamp) AS latest "
        "RETURN s.name, s.file_path, s.type "
        "ORDER BY latest DESC LIMIT 20"
    )
    symbols = []
    while result.has_next():
        row = result.get_next()
        symbols.append({"name": row[0], "file_path": row[1], "type": row[2]})
    return symbols


def _get_briefing_symbols(conn) -> set:
    """DB에서 브리핑에 포함될 심볼 이름 조회"""
    result = conn.execute(
        "MATCH (t:Turn)-[:ABOUT|MODIFIED_BY]->(s:Symbol) "
        "RETURN DISTINCT s.name "
        "LIMIT 50"
    )
    names = set()
    while result.has_next():
        names.add(result.get_next()[0])
    return names


# ─── 서브커맨드 ───────────────────────────────────────────────────────────────

def cmd_checkpoint(args: argparse.Namespace) -> int:
    """Ground truth 저장 (현재 git diff + 최근 수정 심볼)"""
    try:
        db, conn = get_db_connection()
    except Exception as e:
        logger.error(f"DB 연결 실패: {e}")
        return 1

    git_diff = _get_git_diff()
    recent_symbols = _get_recent_symbols(conn)

    checkpoint = {
        "timestamp": datetime.now().isoformat(),
        "git_diff": git_diff,
        "symbols": recent_symbols,
    }

    filename = f"checkpoint_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    path = _get_eval_path(filename)
    with open(path, "w") as f:
        json.dump(checkpoint, f, indent=2, ensure_ascii=False)

    logger.info(f"Checkpoint 저장: {path} (심볼 {len(recent_symbols)}개)")
    print(f"Checkpoint saved: {path}")
    return 0


def cmd_score(args: argparse.Namespace) -> int:
    """사용자 평가 점수 기록"""
    score = {
        "timestamp": datetime.now().isoformat(),
        "symbol_recall": args.recall,
        "accuracy": args.accuracy,
        "continuation": args.continuation,
        "notes": args.notes or "",
    }

    path = _get_eval_path("scores.jsonl")
    with open(path, "a") as f:
        f.write(json.dumps(score, ensure_ascii=False) + "\n")

    logger.info(
        f"평가 기록: recall={args.recall}, "
        f"accuracy={args.accuracy}, "
        f"continuation={args.continuation}"
    )
    print(
        f"Score recorded: recall={args.recall}, "
        f"accuracy={args.accuracy}, "
        f"continuation={args.continuation}"
    )
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    """Ground truth vs briefing 비교 (Symbol Recall Rate 자동 계산)"""
    checkpoints = sorted(EVAL_DIR.glob("checkpoint_*.json"), reverse=True)
    if not checkpoints:
        print("No checkpoints found. Run 'evaluate.py checkpoint' first.")
        return 1

    with open(checkpoints[0]) as f:
        checkpoint = json.load(f)

    ground_truth_symbols = {s["name"] for s in checkpoint.get("symbols", [])}
    if not ground_truth_symbols:
        print("Checkpoint has no symbols.")
        return 1

    try:
        db, conn = get_db_connection()
        briefing_symbols = _get_briefing_symbols(conn)
    except Exception as e:
        logger.error(f"브리핑 심볼 조회 실패: {e}")
        return 1

    # Symbol Recall Rate
    recalled = ground_truth_symbols & briefing_symbols
    total_gt = len(ground_truth_symbols)
    recall_rate = len(recalled) / total_gt if total_gt > 0 else 0

    # Hallucination Rate
    hallucinated = briefing_symbols - ground_truth_symbols
    total_briefing = len(briefing_symbols)
    hallucination_rate = len(hallucinated) / total_briefing if total_briefing > 0 else 0

    print(f"Symbol Recall Rate: {recall_rate:.2%} ({len(recalled)}/{total_gt})")
    print(f"Hallucination Rate: {hallucination_rate:.2%} ({len(hallucinated)}/{total_briefing})")
    print(f"Recalled: {', '.join(sorted(recalled)) or '(none)'}")
    print(f"Missed: {', '.join(sorted(ground_truth_symbols - recalled)) or '(none)'}")
    print(f"Hallucinated: {', '.join(sorted(hallucinated)) or '(none)'}")

    # 비교 결과 저장
    comparison = {
        "timestamp": datetime.now().isoformat(),
        "checkpoint_file": str(checkpoints[0].name),
        "recall_rate": recall_rate,
        "hallucination_rate": hallucination_rate,
        "recalled_count": len(recalled),
        "total_ground_truth": total_gt,
        "hallucinated_count": len(hallucinated),
    }
    path = _get_eval_path("comparisons.jsonl")
    with open(path, "a") as f:
        f.write(json.dumps(comparison, ensure_ascii=False) + "\n")

    return 0


def cmd_report(args: argparse.Namespace) -> int:
    """평가 결과 CSV 리포트 출력"""
    fieldnames = [
        "timestamp", "type", "recall", "accuracy",
        "continuation", "auto_recall_rate", "hallucination_rate", "notes",
    ]
    rows = []

    scores_path = _get_eval_path("scores.jsonl")
    if scores_path.exists():
        with open(scores_path) as f:
            for line in f:
                if not line.strip():
                    continue
                data = json.loads(line)
                rows.append({
                    "timestamp": data["timestamp"],
                    "type": "manual_score",
                    "recall": data.get("symbol_recall", ""),
                    "accuracy": data.get("accuracy", ""),
                    "continuation": data.get("continuation", ""),
                    "auto_recall_rate": "",
                    "hallucination_rate": "",
                    "notes": data.get("notes", ""),
                })

    comparisons_path = _get_eval_path("comparisons.jsonl")
    if comparisons_path.exists():
        with open(comparisons_path) as f:
            for line in f:
                if not line.strip():
                    continue
                data = json.loads(line)
                rows.append({
                    "timestamp": data["timestamp"],
                    "type": "auto_compare",
                    "recall": "",
                    "accuracy": "",
                    "continuation": "",
                    "auto_recall_rate": f"{data.get('recall_rate', 0):.2%}",
                    "hallucination_rate": f"{data.get('hallucination_rate', 0):.2%}",
                    "notes": data.get("checkpoint_file", ""),
                })

    if not rows:
        print("No evaluation data found.")
        return 0

    rows.sort(key=lambda r: r["timestamp"])
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return 0


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="Context OS 평가 프레임워크")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("checkpoint", help="Ground truth 저장")

    score_parser = subparsers.add_parser("score", help="평가 점수 기록")
    score_parser.add_argument(
        "--recall", type=int, required=True, help="Symbol Recall (1-5)",
    )
    score_parser.add_argument(
        "--accuracy", type=int, required=True, help="Accuracy (1-5)",
    )
    score_parser.add_argument(
        "--continuation", type=int, required=True, help="Continuation (1-5)",
    )
    score_parser.add_argument("--notes", type=str, default="", help="비고")

    subparsers.add_parser("compare", help="Ground truth vs briefing 비교")
    subparsers.add_parser("report", help="평가 결과 CSV 출력")

    args = parser.parse_args()

    commands = {
        "checkpoint": cmd_checkpoint,
        "score": cmd_score,
        "compare": cmd_compare,
        "report": cmd_report,
    }
    return commands[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
