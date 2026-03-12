from __future__ import annotations

from history_embedding import QueryRewrite


class HybridQueryOrchestrator:
    def __init__(self, repo_id: str, lance_store, query_rewriter: QueryRewrite):
        self.repo_id = repo_id
        self.lance_store = lance_store
        self.query_rewriter = query_rewriter

    def fts_first(self, query_text: str, *, where: str = "", limit: int = 10) -> dict:
        rewritten = self.query_rewriter.rewrite(query_text)
        return {
            "mode": "fts-first",
            "query_text": query_text,
            "rewritten_query": rewritten.rewritten_query,
            "semantic_rows": self.lance_store.query_chunks(
                self.repo_id,
                rewritten.rewritten_query or rewritten.normalized_query or query_text,
                limit=limit,
                where=where,
            ),
        }

    # Compatibility alias for the previous dense-style naming.
    def semantic_first(self, semantic_query: str, *, where: str = "", limit: int = 10) -> dict:
        result = self.fts_first(semantic_query, where=where, limit=limit)
        result["mode"] = "semantic-first"
        return result

    def graph_first(
        self,
        graph_rows: list[dict],
        *,
        semantic_query: str = "",
        semantic_field: str = "content",
        where: str = "",
        limit: int = 10,
    ) -> dict:
        query_text = semantic_query or (str(graph_rows[0].get(semantic_field, "")) if graph_rows else "")
        rewritten = self.query_rewriter.rewrite(query_text)
        semantic_rows = []
        if query_text:
            semantic_rows = self.lance_store.query_chunks(
                self.repo_id,
                rewritten.rewritten_query or rewritten.normalized_query or query_text,
                limit=limit,
                where=where,
            )
        return {
            "mode": "graph-first",
            "graph_rows": graph_rows,
            "query_text": query_text,
            "rewritten_query": rewritten.rewritten_query,
            "semantic_rows": semantic_rows,
        }
