"""MCP server entry point using the official `mcp` SDK (FastMCP).

Tools exposed:
  - search_messages
  - get_message
  - get_thread
  - list_recent_threads
  - find_commitfest_entry
  - summarize_thread
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from mcp.server.fastmcp import FastMCP

from ..config import Config
from ..ingest.embedder import Embedder, make_embedder
from ..store.repo import Repo
from . import commitfest as cf
from . import tools as t

log = logging.getLogger(__name__)


def build_server(cfg: Optional[Config] = None) -> FastMCP:
    cfg = cfg or Config.from_env()
    repo = Repo(cfg)
    repo.init_schema()

    # Embedder is loaded lazily on first semantic query so the server starts
    # quickly even when sentence-transformers isn't installed.
    _embedder_cache: dict[str, Optional[Embedder]] = {}

    def get_embedder() -> Optional[Embedder]:
        if "e" in _embedder_cache:
            return _embedder_cache["e"]
        if cfg.embedder == "none":
            _embedder_cache["e"] = None
            return None
        try:
            emb = make_embedder(cfg.embedder, cfg.embedder_model, cfg.embedding_dim)
        except Exception as exc:  # noqa: BLE001
            log.warning("embedder unavailable: %s; falling back to FTS-only", exc)
            emb = None
        _embedder_cache["e"] = emb
        return emb

    mcp = FastMCP("pg-mail-mcp")

    @mcp.tool()
    def search_messages(
        query: str,
        list: str = "pgsql-hackers",
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        author: Optional[str] = None,
        limit: int = 20,
        mode: str = "hybrid",
    ) -> dict:
        """Search PostgreSQL mailing-list messages.

        Args:
            query: Natural-language or keyword query.
            list: Mailing list name (default pgsql-hackers).
            date_from: ISO date / datetime lower bound (inclusive).
            date_to: ISO date / datetime upper bound (inclusive).
            author: Substring matched against From name or email.
            limit: Max results.
            mode: 'hybrid' (default), 'fts', or 'semantic'.
        """
        return t.search_messages(
            cfg, repo, get_embedder(), query, list, date_from, date_to,
            author, limit, mode,
        )

    @mcp.tool()
    def get_message(message_id: str, include_body: bool = True) -> dict:
        """Fetch a single message by Message-ID."""
        return t.get_message(repo, message_id, include_body)

    @mcp.tool()
    def get_thread(id: str, max_messages: int = 200) -> dict:
        """Fetch a full thread tree. `id` may be a thread_id (sha1 hex) or a
        Message-ID belonging to the thread."""
        return t.get_thread(repo, id, max_messages)

    @mcp.tool()
    def list_recent_threads(
        list: str = "pgsql-hackers", since_days: int = 7, limit: int = 50
    ) -> dict:
        """List threads with activity in the recent window."""
        return t.list_recent_threads(repo, list, since_days, limit)

    @mcp.tool()
    def find_commitfest_entry(query: str, limit: int = 10) -> dict:
        """Search the PostgreSQL commitfest app for matching patch entries."""
        return cf.search_commitfest(cfg, repo, query, limit)

    @mcp.tool()
    def summarize_thread(id: str, max_messages: int = 200) -> dict:
        """Return a structured, quote-stripped digest of a thread for the
        calling LLM to summarize."""
        return t.summarize_thread(repo, id, max_messages)

    @mcp.tool()
    def archive_coverage() -> dict:
        """Report what data is actually present locally: date range, totals,
        per-list breakdown, last ingest timestamp, and detected month gaps.
        Call this FIRST when answering questions about 'latest', 'recent',
        or 'popular' so you know whether the local archive is fresh."""
        return t.archive_coverage(repo)

    @mcp.tool()
    def top_threads_by_activity(
        list: str = "pgsql-hackers",
        since_days: int = 14,
        order_by: str = "message_count",
        limit: int = 20,
    ) -> dict:
        """Most-active threads in the window. order_by is 'message_count'
        (default) or 'participants'."""
        return t.top_threads_by_activity(repo, list, since_days, order_by, limit)

    @mcp.tool()
    def top_authors(
        list: str = "pgsql-hackers",
        since_days: int = 30,
        limit: int = 20,
    ) -> dict:
        """Top posters in the window with message + distinct-thread counts."""
        return t.top_authors(repo, list, since_days, limit)

    @mcp.tool()
    def find_thread_by_url(url: str) -> dict:
        """Resolve a https://www.postgresql.org/message-id/... URL (or a raw
        Message-ID) to the local message and its thread."""
        return t.find_thread_by_url(repo, url)

    @mcp.tool()
    def get_commitfest_entry(entry_id: int) -> dict:
        """Fetch a commitfest entry by numeric id. Returns title, status,
        authors, reviewers, target version, and linked discussion URLs."""
        return cf.get_commitfest_entry(cfg, repo, entry_id)

    @mcp.tool()
    def commitfest_for_thread(thread_id: str, limit: int = 5) -> dict:
        """Best-effort lookup of commitfest entries that look related to the
        given thread (uses the thread subject as a search query)."""
        return cf.commitfest_for_thread(cfg, repo, thread_id, limit)

    @mcp.tool()
    def commits_for_thread(thread_id: str) -> dict:
        """Return commits in postgres/postgres whose `Discussion:` trailer
        points at any message in the given thread. Requires PGMAIL_PG_GIT_PATH
        to point at a local clone."""
        return t.commits_for_thread(cfg, repo, thread_id)

    @mcp.tool()
    def recent_commits(
        since_days: int = 7,
        path_glob: Optional[str] = None,
        limit: int = 50,
    ) -> dict:
        """List commits to postgres/postgres in the recent window. Optionally
        filter by a path glob (e.g. 'src/backend/storage/buffer/').
        Requires PGMAIL_PG_GIT_PATH."""
        return t.recent_commits(cfg, repo, since_days, path_glob, limit)

    @mcp.tool()
    def thread_timeline(id: str, max_messages: int = 200) -> dict:
        """Classified, chronological timeline of a thread. Each message gets
        a kind: patch_v / review / nack / consensus / discussion. Returns
        the latest non-discussion kind as the thread `status`."""
        return t.thread_timeline(repo, id, max_messages)

    @mcp.tool()
    def compare_threads(ids: list[str]) -> dict:
        """Side-by-side comparison of 2-6 threads with status, message count,
        participants, age in days, and the latest poster."""
        return t.compare_threads(repo, ids)

    @mcp.tool()
    def watchlist_add(
        label: str, kind: str, value: str,
        list: str = "pgsql-hackers",
    ) -> dict:
        """Add a watchlist entry. kind = 'thread' (value=thread_id),
        'query' (value=search query), or 'author' (value=email substring)."""
        return t.watchlist_add(repo, label, kind, value, list)

    @mcp.tool()
    def watchlist_list() -> dict:
        """List all watchlist entries."""
        return t.watchlist_list(repo)

    @mcp.tool()
    def watchlist_remove(id: int) -> dict:
        """Remove a watchlist entry by id."""
        return t.watchlist_remove(repo, id)

    @mcp.tool()
    def watchlist_check(touch: bool = True, per_entry_limit: int = 25) -> dict:
        """For each watchlist entry, return new messages since
        last_checked_epoch. If `touch` is true (default), advance
        last_checked_epoch to now after the check."""
        return t.watchlist_check(cfg, repo, get_embedder(), touch, per_entry_limit)

    @mcp.tool()
    def export_thread(id: str, format: str = "markdown",
                      max_messages: int = 500) -> dict:
        """Export a thread as 'markdown' (default) or 'json'. Markdown is
        ready to paste into a doc; JSON is the raw structured payload."""
        return t.export_thread(repo, id, format, max_messages)

    return mcp


def serve() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    server = build_server()
    server.run()  # stdio transport
