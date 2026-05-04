"""Runtime configuration loaded from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _default_db_path() -> Path:
    base = os.environ.get("PGMAIL_DB_PATH")
    if base:
        return Path(base).expanduser()
    # Default: <package-root>/data/pgmail.sqlite
    return Path.home() / ".pg-mail-mcp" / "pgmail.sqlite"


@dataclass(frozen=True)
class Config:
    db_path: Path = field(default_factory=_default_db_path)
    lists: tuple[str, ...] = ("pgsql-hackers",)
    embedder: str = "none"  # "local" | "openai" | "none"
    embedder_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    embedding_dim: int = 384
    user_agent: str = "pg-mail-mcp/0.1 (+https://github.com/local)"
    archive_base: str = "https://www.postgresql.org"
    commitfest_base: str = "https://commitfest.postgresql.org"
    http_timeout: float = 30.0
    # Raw Cookie header value copied from a logged-in browser session against
    # postgresql.org (the mbox download endpoint requires authentication).
    # Example: "pgweb_session=...; csrftoken=..."
    cookie: str = ""
    # Optional path to a local clone of postgres/postgres for git correlation
    # tools (commits_for_thread, recent_commits). If unset, those tools return
    # a graceful 'git_not_configured' error.
    pg_git_path: str = ""

    @classmethod
    def from_env(cls) -> "Config":
        lists_env = os.environ.get("PGMAIL_LISTS")
        lists = tuple(s.strip() for s in lists_env.split(",")) if lists_env else ("pgsql-hackers",)
        embedder = os.environ.get("PGMAIL_EMBEDDER", "none").lower()
        model = os.environ.get(
            "PGMAIL_EMBEDDER_MODEL", "sentence-transformers/all-MiniLM-L6-v2"
        )
        dim = int(os.environ.get("PGMAIL_EMBEDDING_DIM", "384"))
        return cls(
            db_path=_default_db_path(),
            lists=lists,
            embedder=embedder,
            embedder_model=model,
            embedding_dim=dim,
            cookie=os.environ.get("PGMAIL_COOKIE", ""),
            pg_git_path=os.environ.get("PGMAIL_PG_GIT_PATH", ""),
        )

    def ensure_db_dir(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
