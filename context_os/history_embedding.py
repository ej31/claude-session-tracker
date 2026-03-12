from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass, field


TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
CAMEL_RE = re.compile(r"([a-z0-9])([A-Z])")
COMMENT_RE = re.compile(
    r"/\*+(.*?)\*/|//(.*)$|^\s*#(.*)$|^\s*\"\"\"(.*?)\"\"\"|^\s*'''(.*?)'''",
    re.MULTILINE | re.DOTALL,
)
INCLUDE_RE = re.compile(r'#include\s+[<"]([^>"]+)[>"]')
IMPORT_RE = re.compile(
    r"^\s*(?:from\s+([A-Za-z0-9_\.]+)\s+import|import\s+([A-Za-z0-9_\.]+)|require\(['\"]([^'\"]+)['\"]\))",
    re.MULTILINE,
)
CALL_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_\.]*)\s*\(")

SYNONYMS = {
    "db": ["database", "datasource", "storage"],
    "database": ["db", "datasource", "storage"],
    "conn": ["connection", "client", "socket"],
    "connection": ["connect", "client", "socket"],
    "pkt": ["packet", "frame"],
    "packet": ["pkt", "frame", "buffer"],
    "pts": ["timestamp", "presentation", "time"],
    "dts": ["timestamp", "decode", "time"],
    "init": ["initialize", "setup", "bootstrap"],
    "initialize": ["init", "setup"],
    "setup": ["initialize", "config", "prepare"],
    "hw": ["hardware", "accelerator", "gpu"],
    "accel": ["accelerator", "hardware", "gpu"],
    "resample": ["convert", "audio", "rate"],
    "codec": ["encoder", "decoder", "format"],
    "demux": ["demultiplex", "input", "stream"],
    "mux": ["multiplex", "output", "stream"],
}
STOPWORDS = {
    "the", "and", "for", "with", "from", "into", "that", "this", "return", "returns",
    "true", "false", "none", "null", "void", "const", "static", "struct", "class",
}


@dataclass(frozen=True)
class SurrogateRecord:
    intent_text: str
    alias_text: str
    symbol_text: str
    doc_text: str
    search_text: str
    surrogate_version: str
    surrogate_source: str
    fts_ready: bool = True


@dataclass(frozen=True)
class QueryRewriteResult:
    original_query: str
    normalized_query: str
    expanded_terms: list[str]
    rewritten_query: str


def split_identifier_terms(value: str) -> list[str]:
    normalized = CAMEL_RE.sub(r"\1 \2", value.replace("/", " ").replace(".", " ").replace("-", " ").replace("_", " "))
    terms = [token.lower() for token in TOKEN_RE.findall(normalized)]
    return [term for term in terms if term and term not in STOPWORDS]


def extract_comment_terms(text: str) -> list[str]:
    comments = []
    for match in COMMENT_RE.finditer(text):
        groups = [group for group in match.groups() if group]
        comments.extend(groups)
    return split_identifier_terms(" ".join(comments))


def extract_include_terms(text: str) -> list[str]:
    includes = INCLUDE_RE.findall(text)
    imports = []
    for match in IMPORT_RE.finditer(text):
        imports.extend([group for group in match.groups() if group])
    return split_identifier_terms(" ".join(includes + imports))


def extract_call_terms(text: str) -> list[str]:
    terms = []
    for match in CALL_RE.findall(text):
        terms.extend(split_identifier_terms(match.rsplit(".", 1)[-1]))
    return terms


def lexical_similarity(left: str, right: str) -> float:
    left_terms = set(split_identifier_terms(left))
    right_terms = set(split_identifier_terms(right))
    if not left_terms or not right_terms:
        return 0.0
    return len(left_terms & right_terms) / len(left_terms | right_terms)


@dataclass(frozen=True)
class DeterministicSurrogateGenerator:
    name: str = "deterministic-surrogate"
    version: str = "surrogate-v1"
    source_label: str = "deterministic-local"

    def generate(
        self,
        *,
        path_at_commit: str,
        language: str,
        chunk_scope: str,
        content: str,
        symbol_name: str = "",
        fq_name: str = "",
        commit_message: str = "",
    ) -> SurrogateRecord:
        path_terms = split_identifier_terms(path_at_commit)
        symbol_terms = split_identifier_terms(symbol_name or fq_name)
        comment_terms = extract_comment_terms(content)
        include_terms = extract_include_terms(content)
        call_terms = extract_call_terms(content)
        commit_terms = split_identifier_terms(commit_message)

        alias_terms = []
        for term in path_terms + symbol_terms + comment_terms + include_terms + call_terms + commit_terms:
            alias_terms.append(term)
            alias_terms.extend(SYNONYMS.get(term, []))

        doc_text = " ".join(_dedupe(comment_terms + include_terms)[:24]).strip()
        symbol_text = " ".join(_dedupe(symbol_terms + path_terms + split_identifier_terms(language) + split_identifier_terms(chunk_scope))[:24]).strip()

        if doc_text:
            intent_text = doc_text
        else:
            intent_terms = _dedupe(symbol_terms + include_terms + call_terms + commit_terms)[:18]
            intent_text = " ".join(intent_terms)

        alias_text = " ".join(_dedupe(alias_terms)[:48]).strip()
        search_text = " ".join(
            piece for piece in [
                intent_text,
                alias_text,
                symbol_text,
                doc_text,
                " ".join(_dedupe(split_identifier_terms(content))[:32]),
            ] if piece
        ).strip()

        return SurrogateRecord(
            intent_text=intent_text,
            alias_text=alias_text,
            symbol_text=symbol_text,
            doc_text=doc_text,
            search_text=search_text,
            surrogate_version=self.version,
            surrogate_source=self.source_label,
            fts_ready=bool(search_text),
        )


@dataclass(frozen=True)
class QueryRewrite:
    synonyms: dict[str, list[str]] = field(default_factory=lambda: SYNONYMS)

    def rewrite(self, query: str) -> QueryRewriteResult:
        normalized_terms = split_identifier_terms(query)
        expanded = []
        for term in normalized_terms:
            expanded.append(term)
            expanded.extend(self.synonyms.get(term, []))
        deduped = _dedupe(expanded)
        return QueryRewriteResult(
            original_query=query,
            normalized_query=" ".join(normalized_terms),
            expanded_terms=deduped,
            rewritten_query=" ".join(deduped),
        )


# Read-only compatibility for existing local utilities that still import this.
@dataclass(frozen=True)
class HashEmbeddingModel:
    name: str = "hash-embedding-v1"
    dimension: int = 32

    def embed(self, text: str) -> list[float]:
        tokens = TOKEN_RE.findall(text.lower())
        vector = [0.0] * self.dimension
        if not tokens:
            return vector
        for token in tokens:
            digest = hashlib.sha256(token.encode("utf-8")).digest()
            bucket = digest[0] % self.dimension
            sign = 1.0 if digest[1] % 2 == 0 else -1.0
            vector[bucket] += sign
        norm = math.sqrt(sum(value * value for value in vector)) or 1.0
        return [value / norm for value in vector]

    def embed_many(self, texts: list[str]) -> list[list[float]]:
        return [self.embed(text) for text in texts]


def cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right:
        return 0.0
    numerator = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(a * a for a in left)) or 1.0
    right_norm = math.sqrt(sum(b * b for b in right)) or 1.0
    return numerator / (left_norm * right_norm)


def _dedupe(values: list[str]) -> list[str]:
    seen = set()
    ordered = []
    for value in values:
        if not value or value in STOPWORDS or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered
