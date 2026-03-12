from __future__ import annotations

import ast
import re
import subprocess
from dataclasses import dataclass
from datetime import timezone
from pathlib import Path
from typing import Iterable

from git import Repo

from history_embedding import DeterministicSurrogateGenerator
from history_ids import (
    SUPPORTED_SUFFIXES,
    detect_branch,
    hash_content,
    make_chunk_id,
    make_chunk_namespace,
    make_developer_id,
    make_file_id,
    make_file_state_id,
    make_repo_id,
    make_symbol_id,
    make_symbol_state_id,
)
from history_ir import (
    CallFact,
    ChunkFact,
    CommitBundle,
    CommitFact,
    DeveloperFact,
    FileFact,
    FileStateFact,
    FileTouchFact,
    ParsedFile,
    ParsedSymbol,
    SymbolFact,
    SymbolStateFact,
    SymbolTouchFact,
)


LANGUAGE_MAP = {
    ".py": "python",
    ".js": "javascript",
    ".jsx": "javascript",
    ".mjs": "javascript",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".c": "c",
    ".h": "c-header",
    ".S": "assembly",
    ".asm": "assembly",
    ".mak": "make",
    ".txt": "text",
    ".md": "markdown",
    ".sh": "shell",
    "": "text",
}

JS_DEF_RE = re.compile(
    r"^(?:export\s+)?(?:(function)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\((.*?)\)\s*\{|"
    r"(const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*\((.*?)\)\s*=>\s*(\{)?)"
)
JS_CLASS_RE = re.compile(r"^(?:export\s+)?class\s+([A-Za-z_][A-Za-z0-9_]*)")
CALL_NAME_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_\.]*)\s*\(")
SKIP_CALL_NAMES = {
    "if", "for", "while", "switch", "return", "typeof", "catch",
    "print", "len", "range", "console.log", "Promise", "Math",
}


@dataclass
class RepositoryState:
    path_to_file_id: dict[str, str]
    latest_file_states: dict[str, FileStateFact]
    latest_symbol_states: dict[str, SymbolStateFact]
    file_facts: dict[str, FileFact]
    symbol_facts: dict[str, SymbolFact]


@dataclass
class DeltaEntry:
    old_path: str
    new_path: str
    change_type: str
    additions: int
    deletions: int
    similarity_score: float
    changed_lines: list[int]
    diff_hash: str


def initial_state() -> RepositoryState:
    return RepositoryState({}, {}, {}, {}, {})


class PythonParser:
    def parse(self, file_path: str, source: str) -> ParsedFile:
        try:
            tree = ast.parse(source)
        except SyntaxError:
            return ParsedFile(parse_status="syntax_error", symbols=[])

        lines = source.splitlines()
        symbols: list[ParsedSymbol] = []

        class Visitor(ast.NodeVisitor):
            def __init__(self) -> None:
                self.stack: list[str] = []

            def _build_symbol(self, node: ast.AST, kind: str, name: str) -> None:
                start_line = getattr(node, "lineno", 1)
                end_line = getattr(node, "end_lineno", start_line)
                body = "\n".join(lines[start_line - 1:end_line])
                if kind in {"function", "method"}:
                    args = []
                    for arg in getattr(node, "args").args:
                        args.append(arg.arg)
                    signature = f"def {name}({', '.join(args)})"
                else:
                    signature = f"class {name}"
                fq_name = ".".join([*self.stack, name]) if self.stack else name
                doc = ast.get_docstring(node) or ""
                calls = self._extract_calls(node)
                symbols.append(
                    ParsedSymbol(
                        kind=kind,
                        canonical_name=name,
                        fq_name=fq_name,
                        start_line=start_line,
                        end_line=end_line,
                        signature=signature,
                        body=body,
                        doc_intent=doc.splitlines()[0] if doc else "",
                        calls=calls,
                    )
                )

            def _extract_calls(self, node: ast.AST) -> list[str]:
                names: list[str] = []
                for child in ast.walk(node):
                    if not isinstance(child, ast.Call):
                        continue
                    func = child.func
                    if isinstance(func, ast.Name):
                        names.append(func.id)
                    elif isinstance(func, ast.Attribute):
                        names.append(func.attr)
                return names

            def visit_ClassDef(self, node: ast.ClassDef) -> None:
                self._build_symbol(node, "class", node.name)
                self.stack.append(node.name)
                for child in node.body:
                    if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        self._build_symbol(child, "method", child.name)
                self.generic_visit(node)
                self.stack.pop()

            def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
                if self.stack:
                    return
                self._build_symbol(node, "function", node.name)

            def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
                if self.stack:
                    return
                self._build_symbol(node, "function", node.name)

        Visitor().visit(tree)
        return ParsedFile(parse_status="ok", symbols=symbols)


class JsTsParser:
    def parse(self, file_path: str, source: str) -> ParsedFile:
        lines = source.splitlines()
        symbols: list[ParsedSymbol] = []
        idx = 0
        while idx < len(lines):
            line = lines[idx].strip()
            class_match = JS_CLASS_RE.match(line)
            if class_match:
                name = class_match.group(1)
                end_idx = _find_block_end(lines, idx)
                body = "\n".join(lines[idx:end_idx + 1])
                symbols.append(
                    ParsedSymbol(
                        kind="class",
                        canonical_name=name,
                        fq_name=name,
                        start_line=idx + 1,
                        end_line=end_idx + 1,
                        signature=f"class {name}",
                        body=body,
                        doc_intent="",
                        calls=_extract_js_calls(body),
                    )
                )
                idx = end_idx + 1
                continue

            func_match = JS_DEF_RE.match(line)
            if not func_match:
                idx += 1
                continue

            name = func_match.group(2) or func_match.group(5)
            params = func_match.group(3) or func_match.group(6) or ""
            has_block = bool(func_match.group(1) or func_match.group(7))
            if has_block:
                end_idx = _find_block_end(lines, idx)
            else:
                end_idx = idx
            body = "\n".join(lines[idx:end_idx + 1])
            symbols.append(
                ParsedSymbol(
                    kind="function",
                    canonical_name=name,
                    fq_name=name,
                    start_line=idx + 1,
                    end_line=end_idx + 1,
                    signature=f"function {name}({params})",
                    body=body,
                    doc_intent=_extract_js_doc(lines, idx),
                    calls=_extract_js_calls(body),
                )
            )
            idx = end_idx + 1
        return ParsedFile(parse_status="ok", symbols=symbols)


def _extract_js_doc(lines: list[str], idx: int) -> str:
    if idx == 0:
        return ""
    prev = lines[idx - 1].strip()
    if not prev.startswith("/**"):
        return ""
    return prev.replace("/**", "").replace("*/", "").strip()


def _extract_js_calls(body: str) -> list[str]:
    result = []
    for match in CALL_NAME_RE.findall(body):
        if match in SKIP_CALL_NAMES:
            continue
        result.append(match.rsplit(".", 1)[-1])
    return result


def _find_block_end(lines: list[str], start_idx: int) -> int:
    balance = 0
    saw_open = False
    for idx in range(start_idx, len(lines)):
        line = lines[idx]
        balance += line.count("{")
        if line.count("{"):
            saw_open = True
        balance -= line.count("}")
        if saw_open and balance <= 0:
            return idx
    return start_idx


class GitHistoryExtractor:
    def __init__(
        self,
        repo_dir: str,
        surrogate_generator: DeterministicSurrogateGenerator | None = None,
    ):
        self.repo_dir = str(Path(repo_dir).resolve())
        self.repo = Repo(self.repo_dir)
        self.repo_id = make_repo_id(self.repo_dir)
        self.branch_hint = detect_branch(self.repo_dir)
        self.surrogate_generator = surrogate_generator or DeterministicSurrogateGenerator()
        self.python_parser = PythonParser()
        self.js_parser = JsTsParser()

    def list_commits(self, start_after: str = "", include_only: set[str] | None = None) -> list:
        commits = list(self.repo.iter_commits("HEAD"))
        commits.reverse()
        if include_only:
            return [commit for commit in commits if commit.hexsha in include_only]
        if not start_after:
            return commits

        filtered = []
        seen = False
        for commit in commits:
            if seen:
                filtered.append(commit)
            if commit.hexsha == start_after:
                seen = True
        return filtered

    def get_commit_files(self, commit_sha: str) -> dict[str, dict]:
        commit = self.repo.commit(commit_sha)
        return self._read_commit_files(commit)

    def materialize_commit(
        self,
        commit,
        snapshot_seq: int,
        ingest_batch_id: str,
        previous_state: RepositoryState,
        is_head: bool,
    ) -> CommitBundle:
        developer = self._build_developer(commit)
        commit_fact = self._build_commit(commit, ingest_batch_id, developer)
        deltas = self._load_deltas(commit)
        next_paths = dict(previous_state.path_to_file_id)
        for delta in deltas.values():
            if delta.change_type == "renamed":
                file_id = next_paths.pop(delta.old_path, "")
                if not file_id:
                    file_id = make_file_id(self.repo_id, delta.old_path or delta.new_path)
                next_paths[delta.new_path] = file_id
            elif delta.change_type == "deleted":
                next_paths.pop(delta.old_path, None)
            else:
                path = delta.new_path or delta.old_path
                if path not in next_paths:
                    next_paths[path] = make_file_id(self.repo_id, path)

        current_files = self._read_commit_files(commit)
        for path in current_files:
            next_paths.setdefault(path, make_file_id(self.repo_id, path))

        file_facts = dict(previous_state.file_facts)
        symbol_facts = dict(previous_state.symbol_facts)
        latest_file_states = dict(previous_state.latest_file_states)
        latest_symbol_states = dict(previous_state.latest_symbol_states)
        current_symbol_ids_by_name: dict[str, list[str]] = {}

        bundle_file_facts: list[FileFact] = []
        bundle_file_states: list[FileStateFact] = []
        bundle_symbol_facts: list[SymbolFact] = []
        bundle_symbol_states: list[SymbolStateFact] = []
        bundle_file_touches: list[FileTouchFact] = []
        bundle_symbol_touches: list[SymbolTouchFact] = []
        symbol_call_specs: list[tuple[str, list[str]]] = []
        closed_file_state_ids: list[str] = []
        closed_symbol_state_ids: list[str] = []

        open_file_ids = set()
        open_symbol_ids = set()

        for path, file_info in current_files.items():
            file_id = next_paths[path]
            open_file_ids.add(file_id)
            file_fact = file_facts.get(file_id)
            if not file_fact:
                file_fact = FileFact(
                    file_id=file_id,
                    repo_id=self.repo_id,
                    canonical_path=path,
                    first_seen_commit=commit.hexsha,
                    last_seen_commit=commit.hexsha,
                    status="active",
                )
                bundle_file_facts.append(file_fact)
            else:
                file_fact.last_seen_commit = commit.hexsha
                file_fact.status = "active"
            file_facts[file_id] = file_fact

            prev_file_state = latest_file_states.get(file_id)
            if (
                prev_file_state
                and prev_file_state.blob_sha == file_info["blob_sha"]
                and prev_file_state.path_at_commit == path
            ):
                current_file_state = prev_file_state
            else:
                change_kind = "added" if not prev_file_state else "modified"
                if path in deltas and deltas[path].change_type == "renamed":
                    change_kind = "renamed"
                current_file_state = FileStateFact(
                    file_state_id=make_file_state_id(file_id, file_info["blob_sha"], path),
                    file_id=file_id,
                    path_at_commit=path,
                    blob_sha=file_info["blob_sha"],
                    language=file_info["language"],
                    size_bytes=file_info["size_bytes"],
                    parse_status="ok",
                    valid_from_commit=commit.hexsha,
                    content=file_info["content"],
                    content_hash=hash_content(file_info["content"]),
                    change_kind=change_kind,
                )
                bundle_file_states.append(current_file_state)
                if prev_file_state:
                    prev_file_state.valid_to_commit = commit.hexsha
                    if prev_file_state.file_state_id not in closed_file_state_ids:
                        closed_file_state_ids.append(prev_file_state.file_state_id)
                latest_file_states[file_id] = current_file_state

            delta = deltas.get(path) or deltas.get(file_info["legacy_path"], DeltaEntry(
                old_path="",
                new_path=path,
                change_type="unchanged",
                additions=0,
                deletions=0,
                similarity_score=0.0,
                changed_lines=[],
                diff_hash="",
            ))
            bundle_file_touches.append(
                FileTouchFact(
                    commit_sha=commit.hexsha,
                    file_id=file_id,
                    file_state_id=current_file_state.file_state_id,
                    change_type=delta.change_type,
                    additions=delta.additions,
                    deletions=delta.deletions,
                    similarity_score=delta.similarity_score,
                    path_at_commit=path,
                )
            )

            parsed = self._parse_file(path, file_info["content"])
            current_file_state.parse_status = parsed.parse_status

            file_symbol_state_ids = set()
            for symbol in parsed.symbols:
                symbol_id = make_symbol_id(
                    self.repo_id,
                    file_id,
                    symbol.kind,
                    symbol.fq_name,
                )
                open_symbol_ids.add(symbol_id)
                symbol_fact = symbol_facts.get(symbol_id)
                if not symbol_fact:
                    symbol_fact = SymbolFact(
                        symbol_id=symbol_id,
                        repo_id=self.repo_id,
                        file_id=file_id,
                        kind=symbol.kind,
                        fq_name=symbol.fq_name,
                        canonical_name=symbol.canonical_name,
                        first_seen_commit=commit.hexsha,
                        last_seen_commit=commit.hexsha,
                    )
                    bundle_symbol_facts.append(symbol_fact)
                else:
                    symbol_fact.last_seen_commit = commit.hexsha
                symbol_facts[symbol_id] = symbol_fact

                signature_hash = hash_content(symbol.signature)
                body_hash = hash_content(symbol.body)
                ast_hash = hash_content(f"{symbol.kind}:{symbol.fq_name}:{symbol.start_line}:{symbol.end_line}")
                prev_symbol_state = latest_symbol_states.get(symbol_id)
                if (
                    prev_symbol_state
                    and prev_symbol_state.signature_hash == signature_hash
                    and prev_symbol_state.body_hash == body_hash
                    and prev_symbol_state.file_state_id == current_file_state.file_state_id
                ):
                    symbol_state = prev_symbol_state
                else:
                    if not prev_symbol_state:
                        change_kind = "added"
                    elif prev_symbol_state.signature_hash != signature_hash:
                        change_kind = "signature_changed"
                    elif prev_symbol_state.body_hash != body_hash:
                        change_kind = "modified"
                    else:
                        change_kind = "moved"
                    symbol_state = SymbolStateFact(
                        symbol_state_id=make_symbol_state_id(
                            symbol_id,
                            signature_hash,
                            body_hash,
                            current_file_state.file_state_id,
                        ),
                        symbol_id=symbol_id,
                        file_state_id=current_file_state.file_state_id,
                        signature_hash=signature_hash,
                        body_hash=body_hash,
                        ast_hash=ast_hash,
                        start_line=symbol.start_line,
                        end_line=symbol.end_line,
                        valid_from_commit=commit.hexsha,
                        change_kind=change_kind,
                        content=symbol.body,
                        content_hash=hash_content(symbol.body),
                        kind=symbol.kind,
                        canonical_name=symbol.canonical_name,
                        fq_name=symbol.fq_name,
                    )
                    bundle_symbol_states.append(symbol_state)
                    if prev_symbol_state:
                        prev_symbol_state.valid_to_commit = commit.hexsha
                        if prev_symbol_state.symbol_state_id not in closed_symbol_state_ids:
                            closed_symbol_state_ids.append(prev_symbol_state.symbol_state_id)
                    latest_symbol_states[symbol_id] = symbol_state

                file_symbol_state_ids.add(symbol_state.symbol_state_id)
                current_symbol_ids_by_name.setdefault(symbol.canonical_name, []).append(symbol_id)
                symbol_call_specs.append((symbol_state.symbol_state_id, symbol.calls))

                if delta.changed_lines and _overlaps(delta.changed_lines, symbol.start_line, symbol.end_line):
                    changed_lines = [
                        line for line in delta.changed_lines
                        if symbol.start_line <= line <= symbol.end_line
                    ]
                    bundle_symbol_touches.append(
                        SymbolTouchFact(
                            commit_sha=commit.hexsha,
                            symbol_state_id=symbol_state.symbol_state_id,
                            changed_lines=changed_lines,
                            diff_hash=delta.diff_hash,
                            change_kind=symbol_state.change_kind,
                        )
                    )

        for file_id, file_state in list(latest_file_states.items()):
            if file_id in open_file_ids:
                continue
            if not file_state.valid_to_commit:
                file_state.valid_to_commit = commit.hexsha
                closed_file_state_ids.append(file_state.file_state_id)
            if file_id in file_facts:
                file_facts[file_id].status = "deleted"
                file_facts[file_id].last_seen_commit = commit.hexsha
            delta = deltas.get(file_state.path_at_commit)
            bundle_file_touches.append(
                FileTouchFact(
                    commit_sha=commit.hexsha,
                    file_id=file_id,
                    file_state_id=file_state.file_state_id,
                    change_type="deleted",
                    additions=0,
                    deletions=(delta.deletions if delta else 0),
                    similarity_score=0.0,
                    path_at_commit=file_state.path_at_commit,
                )
            )

        for symbol_id, symbol_state in list(latest_symbol_states.items()):
            if symbol_id in open_symbol_ids:
                continue
            if not symbol_state.valid_to_commit:
                symbol_state.valid_to_commit = commit.hexsha
                closed_symbol_state_ids.append(symbol_state.symbol_state_id)
                bundle_symbol_touches.append(
                    SymbolTouchFact(
                        commit_sha=commit.hexsha,
                        symbol_state_id=symbol_state.symbol_state_id,
                        changed_lines=[],
                        diff_hash="",
                        change_kind="deleted",
                    )
                )

        calls = self._resolve_calls(
            symbol_call_specs,
            latest_symbol_states,
            current_symbol_ids_by_name,
            commit.hexsha,
        )
        chunks = self._make_chunks(
            snapshot_seq=snapshot_seq,
            commit_sha=commit.hexsha,
            authored_at=commit_fact.committed_at,
            ingest_batch_id=ingest_batch_id,
            is_head=is_head,
            commit_message=commit_fact.message,
            latest_file_states=latest_file_states,
            latest_symbol_states=latest_symbol_states,
        )

        return CommitBundle(
            repo_id=self.repo_id,
            developer=developer,
            commit=commit_fact,
            file_facts=bundle_file_facts,
            file_states=bundle_file_states,
            symbol_facts=bundle_symbol_facts,
            symbol_states=bundle_symbol_states,
            calls=calls,
            chunks=chunks,
            file_touches=bundle_file_touches,
            symbol_touches=bundle_symbol_touches,
            current_path_to_file_id=next_paths,
            latest_file_states=latest_file_states,
            latest_symbol_states=latest_symbol_states,
            current_symbol_ids_by_name=current_symbol_ids_by_name,
            closed_file_state_ids=closed_file_state_ids,
            closed_symbol_state_ids=closed_symbol_state_ids,
        )

    def _build_developer(self, commit) -> DeveloperFact:
        email_norm = (commit.author.email or "").strip().lower()
        return DeveloperFact(
            developer_id=make_developer_id(email_norm, str(commit.author)),
            name=str(commit.author),
            email_norm=email_norm,
        )

    def _build_commit(self, commit, ingest_batch_id: str, developer: DeveloperFact) -> CommitFact:
        return CommitFact(
            commit_sha=commit.hexsha,
            tree_sha=commit.tree.hexsha,
            authored_at=commit.authored_datetime.astimezone(timezone.utc).isoformat(),
            committed_at=commit.committed_datetime.astimezone(timezone.utc).isoformat(),
            message=commit.message.strip().splitlines()[0],
            parent_shas=[parent.hexsha for parent in commit.parents],
            parent_count=len(commit.parents),
            ingest_batch_id=ingest_batch_id,
            developer_id=developer.developer_id,
        )

    def _read_commit_files(self, commit) -> dict[str, dict]:
        files: dict[str, dict] = {}
        for blob in commit.tree.traverse():
            if blob.type != "blob":
                continue
            path = blob.path
            suffix = Path(path).suffix
            if suffix not in SUPPORTED_SUFFIXES:
                continue
            try:
                raw = blob.data_stream.read()
                if b"\x00" in raw:
                    continue
                content = raw.decode("utf-8")
            except UnicodeDecodeError:
                continue
            files[path] = {
                "path": path,
                "legacy_path": path,
                "blob_sha": blob.hexsha,
                "language": LANGUAGE_MAP.get(suffix, "text"),
                "size_bytes": blob.size,
                "content": content,
            }
        return files

    def _parse_file(self, path: str, content: str) -> ParsedFile:
        suffix = Path(path).suffix
        if suffix == ".py":
            return self.python_parser.parse(path, content)
        if suffix in {".js", ".jsx", ".mjs", ".ts", ".tsx"}:
            return self.js_parser.parse(path, content)
        return ParsedFile(parse_status="file_only", symbols=[])

    def _resolve_calls(
        self,
        symbol_call_specs: list[tuple[str, list[str]]],
        latest_symbol_states: dict[str, SymbolStateFact],
        current_symbol_ids_by_name: dict[str, list[str]],
        commit_sha: str,
    ) -> list[CallFact]:
        calls: list[CallFact] = []
        for caller_symbol_state_id, callees in symbol_call_specs:
            for callee_name in callees:
                for callee_symbol_id in current_symbol_ids_by_name.get(callee_name, []):
                    callee_state = latest_symbol_states.get(callee_symbol_id)
                    if not callee_state:
                        continue
                    calls.append(
                        CallFact(
                            caller_symbol_state_id=caller_symbol_state_id,
                            callee_symbol_id=callee_symbol_id,
                            callee_symbol_state_id=callee_state.symbol_state_id,
                            valid_from_commit=commit_sha,
                        )
                    )
        unique = {}
        for call in calls:
            unique[(call.caller_symbol_state_id, call.callee_symbol_state_id)] = call
        return list(unique.values())

    def _make_chunks(
        self,
        snapshot_seq: int,
        commit_sha: str,
        authored_at: str,
        ingest_batch_id: str,
        is_head: bool,
        commit_message: str,
        latest_file_states: dict[str, FileStateFact],
        latest_symbol_states: dict[str, SymbolStateFact],
    ) -> list[ChunkFact]:
        file_symbols: dict[str, list[SymbolStateFact]] = {}
        for symbol_state in latest_symbol_states.values():
            if symbol_state.valid_to_commit:
                continue
            file_symbols.setdefault(symbol_state.file_state_id, []).append(symbol_state)

        chunks: list[ChunkFact] = []
        ordinal = 0
        for file_state in latest_file_states.values():
            if file_state.valid_to_commit:
                continue
            symbols = sorted(
                file_symbols.get(file_state.file_state_id, []),
                key=lambda item: (item.start_line, item.end_line),
            )
            content_lines = file_state.content.splitlines()
            occupied_lines = set()
            for symbol_state in symbols:
                for line_no in range(symbol_state.start_line, symbol_state.end_line + 1):
                    occupied_lines.add(line_no)
                surrogate = self.surrogate_generator.generate(
                    path_at_commit=file_state.path_at_commit,
                    language=file_state.language,
                    chunk_scope=symbol_state.kind,
                    content=symbol_state.content,
                    symbol_name=symbol_state.canonical_name,
                    fq_name=symbol_state.fq_name,
                    commit_message=commit_message,
                )
                chunks.append(
                    ChunkFact(
                        chunk_id=make_chunk_id(
                            commit_sha,
                            make_chunk_namespace(symbol_state.symbol_state_id, file_state.file_state_id),
                            ordinal,
                        ),
                        snapshot_commit_sha=commit_sha,
                        snapshot_seq=snapshot_seq,
                        repo_id=self.repo_id,
                        file_id=file_state.file_id,
                        file_state_id=file_state.file_state_id,
                        path_at_commit=file_state.path_at_commit,
                        symbol_id=symbol_state.symbol_id,
                        symbol_state_id=symbol_state.symbol_state_id,
                        chunk_scope=symbol_state.kind,
                        ordinal=ordinal,
                        start_line=symbol_state.start_line,
                        end_line=symbol_state.end_line,
                        language=file_state.language,
                        content=symbol_state.content,
                        content_hash=symbol_state.content_hash,
                        search_text=surrogate.search_text,
                        intent_text=surrogate.intent_text,
                        alias_text=surrogate.alias_text,
                        symbol_text=surrogate.symbol_text,
                        doc_text=surrogate.doc_text,
                        surrogate_version=surrogate.surrogate_version,
                        surrogate_source=surrogate.surrogate_source,
                        fts_ready=surrogate.fts_ready,
                        author_ts=authored_at,
                        branch_hint=self.branch_hint,
                        is_head=is_head,
                        ingest_batch_id=ingest_batch_id,
                    )
                )
                ordinal += 1

            residual_segments = _residual_segments(content_lines, occupied_lines)
            for start_line, end_line, segment in residual_segments:
                if not segment.strip():
                    continue
                surrogate = self.surrogate_generator.generate(
                    path_at_commit=file_state.path_at_commit,
                    language=file_state.language,
                    chunk_scope="file_residual",
                    content=segment,
                    commit_message=commit_message,
                )
                chunks.append(
                    ChunkFact(
                        chunk_id=make_chunk_id(
                            commit_sha,
                            make_chunk_namespace("", file_state.file_state_id),
                            ordinal,
                        ),
                        snapshot_commit_sha=commit_sha,
                        snapshot_seq=snapshot_seq,
                        repo_id=self.repo_id,
                        file_id=file_state.file_id,
                        file_state_id=file_state.file_state_id,
                        path_at_commit=file_state.path_at_commit,
                        symbol_id="",
                        symbol_state_id="",
                        chunk_scope="file_residual",
                        ordinal=ordinal,
                        start_line=start_line,
                        end_line=end_line,
                        language=file_state.language,
                        content=segment,
                        content_hash=hash_content(segment),
                        search_text=surrogate.search_text,
                        intent_text=surrogate.intent_text,
                        alias_text=surrogate.alias_text,
                        symbol_text=surrogate.symbol_text,
                        doc_text=surrogate.doc_text,
                        surrogate_version=surrogate.surrogate_version,
                        surrogate_source=surrogate.surrogate_source,
                        fts_ready=surrogate.fts_ready,
                        author_ts=authored_at,
                        branch_hint=self.branch_hint,
                        is_head=is_head,
                        ingest_batch_id=ingest_batch_id,
                    )
                )
                ordinal += 1
        return chunks

    def _load_deltas(self, commit) -> dict[str, DeltaEntry]:
        parent_sha = commit.parents[0].hexsha if commit.parents else ""
        if parent_sha:
            name_status_cmd = [
                "git", "-C", self.repo_dir, "diff-tree",
                "--no-commit-id", "--find-renames", "--name-status", "-r", parent_sha, commit.hexsha,
            ]
        else:
            name_status_cmd = [
                "git", "-C", self.repo_dir, "diff-tree",
                "--no-commit-id", "--root", "--find-renames", "--name-status", "-r", commit.hexsha,
            ]
        result = subprocess.run(
            name_status_cmd,
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
        entries: dict[str, DeltaEntry] = {}
        for line in result.stdout.splitlines():
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            status = parts[0]
            if status.startswith("R"):
                if len(parts) < 3:
                    continue
                old_path = parts[1]
                new_path = parts[2]
                change_type = "renamed"
                similarity = float(status[1:] or 0) / 100.0
            else:
                old_path = parts[1] if len(parts) > 1 else ""
                new_path = parts[1] if len(parts) > 1 else ""
                change_type = {
                    "A": "added",
                    "M": "modified",
                    "D": "deleted",
                }.get(status[0], "modified")
                similarity = 0.0

            additions, deletions, changed_lines, diff_hash = self._load_patch_stats(
                parent_sha,
                commit.hexsha,
                old_path,
                new_path,
            )
            entry = DeltaEntry(
                old_path=old_path,
                new_path=new_path,
                change_type=change_type,
                additions=additions,
                deletions=deletions,
                similarity_score=similarity,
                changed_lines=changed_lines,
                diff_hash=diff_hash,
            )
            entries[new_path or old_path] = entry
            if old_path and old_path != new_path:
                entries[old_path] = entry
        return entries

    def _load_patch_stats(
        self,
        parent_sha: str,
        commit_sha: str,
        old_path: str,
        new_path: str,
    ) -> tuple[int, int, list[int], str]:
        path = new_path or old_path
        if not path:
            return 0, 0, [], ""
        if parent_sha:
            cmd = [
                "git", "-C", self.repo_dir, "diff", "--unified=0",
                parent_sha, commit_sha, "--", path,
            ]
        else:
            cmd = [
                "git", "-C", self.repo_dir, "show", "--format=", "--unified=0",
                commit_sha, "--", path,
            ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
        additions = 0
        deletions = 0
        changed_lines: list[int] = []
        current_line = 0
        for line in result.stdout.splitlines():
            if line.startswith("@@"):
                match = re.search(r"\+(\d+)(?:,(\d+))?", line)
                current_line = int(match.group(1)) if match else 0
            elif line.startswith("+") and not line.startswith("+++"):
                additions += 1
                changed_lines.append(current_line)
                current_line += 1
            elif line.startswith("-") and not line.startswith("---"):
                deletions += 1
            elif not line.startswith("\\"):
                current_line += 1
        return additions, deletions, changed_lines, hash_content(result.stdout)


def _overlaps(changed_lines: list[int], start_line: int, end_line: int) -> bool:
    return any(start_line <= line <= end_line for line in changed_lines)


def _residual_segments(content_lines: list[str], occupied_lines: set[int]) -> Iterable[tuple[int, int, str]]:
    start = None
    acc: list[str] = []
    for idx, line in enumerate(content_lines, start=1):
        if idx in occupied_lines:
            if acc:
                yield start or idx, idx - 1, "\n".join(acc)
                acc = []
                start = None
            continue
        if start is None:
            start = idx
        acc.append(line)
    if acc:
        yield start or 1, len(content_lines), "\n".join(acc)
