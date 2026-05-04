"""Git correlation: read commits from a local clone of postgres/postgres.

Triggered by the `commits_for_thread` and `recent_commits` MCP tools.
Cache is populated lazily into the `git_commits` SQLite table.

The commit subjects in the PostgreSQL repo conventionally include a
``Discussion: https://www.postgresql.org/message-id/...`` trailer that links
to the mailing-list thread that birthed the commit. We extract the first
such URL per commit so we can join commits back to local messages.
"""
from __future__ import annotations

import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from .repo import Repo


_DISCUSSION_RE = re.compile(
    r"Discussion:\s*(https?://www\.postgresql\.org/message-id/\S+)",
    re.IGNORECASE,
)
# Each record separator is a NUL byte; field separator is the literal "\x1f".
_GIT_FORMAT = "%H%x1f%an%x1f%ae%x1f%aI%x1f%at%x1f%s%x1f%b"


class GitNotConfigured(RuntimeError):
    pass


def _ensure_path(pg_git_path: str) -> Path:
    if not pg_git_path:
        raise GitNotConfigured(
            "PGMAIL_PG_GIT_PATH is not set. Point it at a local clone of "
            "postgres/postgres to enable git correlation tools."
        )
    p = Path(pg_git_path).expanduser()
    if not (p / ".git").exists():
        raise GitNotConfigured(
            f"{p} does not look like a git clone (no .git directory). "
            f"Run: git clone https://git.postgresql.org/git/postgresql.git {p}"
        )
    return p


def _run_git_log(repo_path: Path, since: Optional[str], until: Optional[str],
                 path_glob: Optional[str], limit: int) -> str:
    cmd = ["git", "-C", str(repo_path), "log",
           f"--pretty=format:{_GIT_FORMAT}", "-z"]
    if since:
        cmd += ["--since", since]
    if until:
        cmd += ["--until", until]
    if limit:
        cmd += [f"-n", str(limit)]
    if path_glob:
        cmd += ["--", path_glob]
    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                         errors="replace", check=False)
    if res.returncode != 0:
        raise RuntimeError(f"git log failed: {res.stderr.strip()}")
    return res.stdout


def _parse(stream: str) -> list[dict]:
    """Parse the NUL-separated git log output into commit dicts."""
    commits: list[dict] = []
    if not stream:
        return commits
    for record in stream.split("\x00"):
        record = record.strip("\n")
        if not record:
            continue
        parts = record.split("\x1f")
        if len(parts) < 7:
            continue
        sha, an, ae, aiso, aepoch, subj, body = parts[:7]
        m = _DISCUSSION_RE.search(body)
        commits.append({
            "sha": sha,
            "author_name": an,
            "author_email": ae,
            "committed_at": aiso,
            "committed_epoch": int(aepoch) if aepoch.isdigit() else None,
            "subject": subj,
            "discussion_url": m.group(1).rstrip(".,);") if m else None,
            "body": body,
        })
    return commits


def index_recent_commits(repo: Repo, pg_git_path: str, since_days: int = 30) -> int:
    """Refresh the local git_commits cache for the given window. Returns the
    number of commits upserted."""
    p = _ensure_path(pg_git_path)
    since = f"{since_days}.days.ago"
    raw = _run_git_log(p, since=since, until=None, path_glob=None, limit=10000)
    commits = _parse(raw)
    if not commits:
        return 0
    conn = repo.connect()
    conn.executemany(
        """INSERT INTO git_commits(sha, author_name, author_email, committed_at,
                                   committed_epoch, subject, discussion_url, body)
           VALUES(:sha,:author_name,:author_email,:committed_at,:committed_epoch,
                  :subject,:discussion_url,:body)
           ON CONFLICT(sha) DO UPDATE SET
               subject=excluded.subject,
               discussion_url=excluded.discussion_url,
               body=excluded.body""",
        commits,
    )
    return len(commits)


def commits_for_thread(repo: Repo, pg_git_path: str, thread_id: str) -> list[dict]:
    """Return commits whose Discussion: trailer points at any message URL in
    the given thread. Refreshes the index lazily for the last 365 days.
    """
    _ensure_path(pg_git_path)
    # Ensure the cache is reasonably fresh.
    index_recent_commits(repo, pg_git_path, since_days=365)
    conn = repo.connect()
    rows = conn.execute(
        """SELECT g.* FROM git_commits g
           JOIN messages m ON m.web_url = g.discussion_url
           WHERE m.thread_id = ?
           ORDER BY g.committed_epoch DESC""",
        (thread_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def recent_commits(repo: Repo, pg_git_path: str, since_days: int = 7,
                   path_glob: Optional[str] = None, limit: int = 50) -> list[dict]:
    """Return commits from the last `since_days`, optionally filtered by path.
    For path-filtered queries we always re-run git log (no cache benefit);
    for unfiltered queries we use the cache after refreshing it.
    """
    p = _ensure_path(pg_git_path)
    if path_glob:
        raw = _run_git_log(p, since=f"{since_days}.days.ago", until=None,
                           path_glob=path_glob, limit=limit)
        return _parse(raw)[:limit]
    index_recent_commits(repo, pg_git_path, since_days=since_days)
    cutoff = int(datetime.now(timezone.utc).timestamp()) - since_days * 86400
    conn = repo.connect()
    rows = conn.execute(
        """SELECT * FROM git_commits
           WHERE committed_epoch >= ?
           ORDER BY committed_epoch DESC LIMIT ?""",
        (cutoff, limit),
    ).fetchall()
    return [dict(r) for r in rows]
