"""Temporal git-history pipeline integration tests."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("lancedb")
pytest.importorskip("pyarrow")

sys.path.insert(0, str(Path(__file__).parent.parent))
from history_config import HistoryPipelineConfig, KuzuBulkIngestPolicy
from history_embedding import DeterministicSurrogateGenerator
from history_pipeline import RepositoryHistoryPipeline


def _commit_all(repo_dir: Path, message: str, name: str, email: str) -> str:
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": name,
        "GIT_AUTHOR_EMAIL": email,
        "GIT_COMMITTER_NAME": name,
        "GIT_COMMITTER_EMAIL": email,
    }
    subprocess.run(["git", "add", "."], cwd=repo_dir, capture_output=True, check=True, env=env)
    subprocess.run(["git", "commit", "-m", message], cwd=repo_dir, capture_output=True, check=True, env=env)
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_dir,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


@pytest.fixture
def history_repo(tmp_path):
    repo_dir = tmp_path / "history_repo"
    repo_dir.mkdir()
    subprocess.run(["git", "init", "-b", "main"], cwd=repo_dir, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.email", "test@test.com"], cwd=repo_dir, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo_dir, capture_output=True, check=True)

    (repo_dir / "db.py").write_text(
        'def open_connection(dsn):\n'
        '    """open database connection"""\n'
        '    return {"dsn": dsn, "timeout": 5}\n'
        '\n'
        'def helper_ping(conn):\n'
        '    return conn["dsn"]\n',
        encoding="utf-8",
    )
    (repo_dir / "service.py").write_text(
        "from db import open_connection\n\n"
        "def fetch_users():\n"
        '    conn = open_connection("postgres://localhost")\n'
        '    return [conn["dsn"]]\n',
        encoding="utf-8",
    )
    commit1 = _commit_all(repo_dir, "feat: add database primitives", "Alice", "alice@example.com")

    (repo_dir / "db.py").write_text(
        'def open_connection(dsn, timeout=30):\n'
        '    """open database connection with timeout"""\n'
        '    return {"dsn": dsn, "timeout": timeout}\n'
        '\n'
        'def helper_ping(conn):\n'
        '    return conn["dsn"]\n',
        encoding="utf-8",
    )
    commit2 = _commit_all(repo_dir, "feat: add timeout to open_connection", "Bob", "bob@example.com")
    return repo_dir, {"commit1": commit1, "commit2": commit2}


def test_bootstrap_persists_snapshots_and_graph(history_repo, tmp_path):
    repo_dir, commits = history_repo
    pipeline = RepositoryHistoryPipeline(str(repo_dir), str(tmp_path / "store"))

    ingested = pipeline.bootstrap()

    assert ingested == [commits["commit1"], commits["commit2"]]
    commit1_rows = pipeline.load_snapshot_rows(commits["commit1"])
    commit2_rows = pipeline.load_snapshot_rows(commits["commit2"])
    assert any(row["path_at_commit"] == "db.py" for row in commit1_rows)
    assert any(row["path_at_commit"] == "service.py" for row in commit2_rows)

    latest_open = pipeline.graph_store.latest_symbol_by_name("open_connection")
    assert latest_open
    assert any(row["symbol_state_id"] == latest_open[0]["symbol_state_id"] for row in commit2_rows)

    developers = pipeline.find_core_developers_for_query("database connection timeout", since_days=365, limit=5)
    assert developers
    assert developers[0]["name"] == "Bob"

    impacts = pipeline.analyze_signature_change_impact("open_connection")
    assert impacts
    assert impacts[0]["caller_count"] >= 1
    caller_names = {caller["caller_name"] for caller in impacts[0]["callers"]}
    assert "fetch_users" in caller_names

    fts_rows = pipeline.execute_hybrid_query("fts-first", semantic_query="database connection timeout", limit=5)
    assert fts_rows["semantic_rows"]
    top = fts_rows["semantic_rows"][0]
    assert "search_text" in top
    assert "timeout" in top["search_text"]


def test_incremental_sync_reuses_unchanged_symbol_state(history_repo, tmp_path):
    repo_dir, commits = history_repo
    pipeline = RepositoryHistoryPipeline(str(repo_dir), str(tmp_path / "store"))
    pipeline.bootstrap()

    (repo_dir / "db.py").write_text(
        'def open_connection(dsn, timeout=45):\n'
        '    """open database connection with timeout"""\n'
        '    details = {"dsn": dsn, "timeout": timeout}\n'
        "    return details\n"
        '\n'
        'def helper_ping(conn):\n'
        '    return conn["dsn"]\n',
        encoding="utf-8",
    )
    commit3 = _commit_all(repo_dir, "fix: tune open_connection body", "Bob", "bob@example.com")

    ingested = pipeline.sync_incremental()

    assert ingested == [commit3]
    fetch_users = pipeline.graph_store.latest_symbol_by_name("fetch_users")[0]
    fetch_history = pipeline.graph_store.symbol_state_history(fetch_users["symbol_id"])
    assert len(fetch_history) == 1

    open_connection = pipeline.graph_store.latest_symbol_by_name("open_connection")[0]
    open_history = pipeline.graph_store.symbol_state_history(open_connection["symbol_id"])
    assert len(open_history) == 3

    previous_rows = pipeline.load_snapshot_rows(commits["commit2"])
    current_rows = pipeline.load_snapshot_rows(commit3)
    prev_fetch_state = next(row["symbol_state_id"] for row in previous_rows if "fetch_users" in row["content"])
    curr_fetch_state = next(row["symbol_state_id"] for row in current_rows if "fetch_users" in row["content"])
    assert prev_fetch_state == curr_fetch_state


def test_pending_repair_can_replay_without_duplicate_lance_rows(history_repo, tmp_path, monkeypatch):
    repo_dir, _ = history_repo
    pipeline = RepositoryHistoryPipeline(str(repo_dir), str(tmp_path / "store"))
    pipeline.bootstrap()

    (repo_dir / "service.py").write_text(
        "from db import open_connection\n\n"
        "def fetch_users():\n"
        '    conn = open_connection("postgres://localhost")\n'
        '    return [conn["dsn"], "users"]\n',
        encoding="utf-8",
    )
    commit3 = _commit_all(repo_dir, "feat: expand service response", "Bob", "bob@example.com")

    original = pipeline.graph_store.ingest_commit_bundle
    state = {"raised": False}

    def flaky(bundle, *args, **kwargs):
        if not state["raised"]:
            state["raised"] = True
            raise RuntimeError("graph write failed")
        return original(bundle, *args, **kwargs)

    monkeypatch.setattr(pipeline.graph_store, "ingest_commit_bundle", flaky)
    with pytest.raises(RuntimeError):
        pipeline.sync_incremental()

    manifest = pipeline.catalog.get_manifest(pipeline.repo_id, commit3)
    assert manifest is not None
    assert manifest["status"] == "pending_repair"

    monkeypatch.setattr(pipeline.graph_store, "ingest_commit_bundle", original)
    repaired = pipeline.repair_pending()
    assert repaired == [commit3]

    manifest = pipeline.catalog.get_manifest(pipeline.repo_id, commit3)
    assert manifest["status"] == "committed"
    rows = pipeline.load_snapshot_rows(commit3)
    assert rows
    assert len(rows) == len({row["chunk_id"] for row in rows})


def test_rename_preserves_file_lineage_and_classifies_refactor(history_repo, tmp_path):
    repo_dir, commits = history_repo
    pipeline = RepositoryHistoryPipeline(str(repo_dir), str(tmp_path / "store"))
    pipeline.bootstrap()

    subprocess.run(["git", "mv", "db.py", "database.py"], cwd=repo_dir, capture_output=True, check=True)
    rename_commit = _commit_all(repo_dir, "refactor: rename db module", "Bob", "bob@example.com")

    ingested = pipeline.sync_incremental()
    assert ingested == [rename_commit]

    previous_rows = pipeline.load_snapshot_rows(commits["commit2"])
    db_file_id = next(row["file_id"] for row in previous_rows if row["path_at_commit"] == "db.py")
    latest_file_state = pipeline.graph_store.get_latest_file_state(db_file_id)
    assert latest_file_state is not None
    assert latest_file_state["path_at_commit"] == "database.py"

    verdict = pipeline.classify_commit_change(rename_commit)
    assert verdict["classification"] == "refactor_or_rename"


def test_bulk_copy_from_path_and_profile_helpers(history_repo, tmp_path):
    repo_dir, _ = history_repo
    pipeline = RepositoryHistoryPipeline(
        str(repo_dir),
        str(tmp_path / "store"),
        config=HistoryPipelineConfig(
            kuzu=KuzuBulkIngestPolicy(enable_copy_from=True, copy_from_row_threshold=1),
        ),
    )

    pipeline.bootstrap()

    stage_root = tmp_path / "store" / pipeline.config.kuzu.staging_subdir
    assert stage_root.exists()
    assert any(path.name == "calls.csv" for path in stage_root.rglob("*.csv"))

    profile = pipeline.profile_query_paths(canonical_name="open_connection")
    assert profile["graph_explain"]
    assert "history_tables" in profile["lance_storage"]


def test_fail_closed_on_dirty_worktree_for_current_state_queries(history_repo, tmp_path):
    repo_dir, _ = history_repo
    pipeline = RepositoryHistoryPipeline(str(repo_dir), str(tmp_path / "store"))
    pipeline.bootstrap()

    (repo_dir / "db.py").write_text(
        'def open_connection(dsn, timeout=99):\n'
        '    """dirty change not committed"""\n'
        '    return {"dsn": dsn, "timeout": timeout}\n',
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="fail-closed"):
        pipeline.analyze_signature_change_impact("open_connection")


def test_fail_closed_on_surrogate_version_mismatch(history_repo, tmp_path):
    repo_dir, _ = history_repo
    pipeline = RepositoryHistoryPipeline(str(repo_dir), str(tmp_path / "store"))
    pipeline.bootstrap()

    newer_surrogate = DeterministicSurrogateGenerator(version="surrogate-v999")
    restarted = RepositoryHistoryPipeline(
        str(repo_dir),
        str(tmp_path / "store"),
        surrogate_generator=newer_surrogate,
    )

    with pytest.raises(RuntimeError, match="surrogate version changed"):
        restarted.analyze_signature_change_impact("open_connection")


def test_restart_recovers_state_for_multiple_unsynced_commits(history_repo, tmp_path):
    repo_dir, commits = history_repo
    store_root = tmp_path / "store"
    pipeline = RepositoryHistoryPipeline(str(repo_dir), str(store_root))
    pipeline.bootstrap()

    (repo_dir / "service.py").write_text(
        "from db import open_connection\n\n"
        "def fetch_users():\n"
        '    conn = open_connection("postgres://localhost")\n'
        '    return [conn["dsn"], "users"]\n',
        encoding="utf-8",
    )
    commit3 = _commit_all(repo_dir, "feat: expand service response", "Bob", "bob@example.com")

    (repo_dir / "db.py").write_text(
        'def open_connection(dsn, timeout=45):\n'
        '    """open database connection with timeout"""\n'
        '    return {"dsn": dsn, "timeout": timeout}\n'
        '\n'
        'def helper_ping(conn):\n'
        '    return conn["dsn"]\n',
        encoding="utf-8",
    )
    commit4 = _commit_all(repo_dir, "feat: tune timeout default", "Bob", "bob@example.com")

    restarted = RepositoryHistoryPipeline(str(repo_dir), str(store_root))
    ingested = restarted.sync_incremental()

    assert ingested == [commit3, commit4]
    commit3_rows = restarted.load_snapshot_rows(commit3)
    commit4_rows = restarted.load_snapshot_rows(commit4)

    commit3_open = next(
        row for row in commit3_rows
        if row["path_at_commit"] == "db.py" and row["content"].startswith("def open_connection")
    )
    commit4_open = next(
        row for row in commit4_rows
        if row["path_at_commit"] == "db.py" and row["content"].startswith("def open_connection")
    )

    assert "timeout=30" in commit3_open["content"]
    assert "timeout=45" in commit4_open["content"]
    assert commit3_open["symbol_state_id"] != commit4_open["symbol_state_id"]
    assert any('"users"' in row["content"] for row in commit3_rows)
    assert not any('"users"' in row["content"] for row in restarted.load_snapshot_rows(commits["commit2"]))


def test_restart_repair_pending_uses_recovered_state(history_repo, tmp_path, monkeypatch):
    repo_dir, _ = history_repo
    store_root = tmp_path / "store"
    pipeline = RepositoryHistoryPipeline(str(repo_dir), str(store_root))
    pipeline.bootstrap()

    (repo_dir / "service.py").write_text(
        "from db import open_connection\n\n"
        "def fetch_users():\n"
        '    conn = open_connection("postgres://localhost")\n'
        '    return [conn["dsn"], "users"]\n',
        encoding="utf-8",
    )
    commit3 = _commit_all(repo_dir, "feat: expand service response", "Bob", "bob@example.com")

    original = pipeline.graph_store.ingest_commit_bundle
    state = {"raised": False}

    def flaky(bundle, *args, **kwargs):
        if not state["raised"]:
            state["raised"] = True
            raise RuntimeError("graph write failed")
        return original(bundle, *args, **kwargs)

    monkeypatch.setattr(pipeline.graph_store, "ingest_commit_bundle", flaky)
    with pytest.raises(RuntimeError):
        pipeline.sync_incremental()

    restarted = RepositoryHistoryPipeline(str(repo_dir), str(store_root))
    repaired = restarted.repair_pending()

    assert repaired == [commit3]
    rows = restarted.load_snapshot_rows(commit3)
    assert any("users" in row["content"] for row in rows)
    fetch_users = restarted.graph_store.latest_symbol_by_name("fetch_users")[0]
    history = restarted.graph_store.symbol_state_history(fetch_users["symbol_id"])
    assert len(history) == 2
