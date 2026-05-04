"""Tool implementations. Pure functions over Repo + optional embedder."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from dateutil import parser as dateparse

from ..config import Config
from ..ingest.embedder import Embedder
from ..store import vec as vec_mod
from ..store.repo import Repo


# ---------------------------------------------------------------- helpers

def _parse_date(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    dt = dateparse.parse(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.astimezone(timezone.utc).timestamp())


def _msg_summary(row: dict) -> dict:
    return {
        "message_id": row["message_id"],
        "list": row["list"],
        "subject": row.get("subject"),
        "from": {"name": row.get("from_name"), "email": row.get("from_email")},
        "date": row.get("date_utc"),
        "thread_id": row.get("thread_id"),
        "url": row.get("web_url"),
    }


def _snippet(text: Optional[str], max_chars: int = 240) -> str:
    if not text:
        return ""
    t = " ".join(text.split())
    return t[:max_chars] + ("…" if len(t) > max_chars else "")


# ---------------------------------------------------------------- tools

def search_messages(
    cfg: Config,
    repo: Repo,
    embedder: Optional[Embedder],
    query: str,
    list_name: str = "pgsql-hackers",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    author: Optional[str] = None,
    limit: int = 20,
    mode: str = "hybrid",
) -> dict:
    df = _parse_date(date_from)
    dt = _parse_date(date_to)
    fts_hits: list[tuple[int, float, dict]] = []
    if mode in ("hybrid", "fts"):
        fts_hits = repo.search_fts(query, list_name, df, dt, author, limit * 3)

    sem_hits: list[tuple[int, float]] = []
    if mode in ("hybrid", "semantic") and embedder is not None and embedder.dim > 0:
        conn = repo.connect()
        if vec_mod.ensure_vec_table(conn, embedder.dim):
            qvec = embedder.embed([query])
            if qvec and qvec[0]:
                sem_hits = vec_mod.search_vec(conn, qvec[0], limit * 3)

    # Reciprocal-rank fusion
    K = 60
    scores: dict[int, float] = {}
    for rank, (rid, _bm25, _row) in enumerate(fts_hits):
        scores[rid] = scores.get(rid, 0.0) + 1.0 / (K + rank)
    for rank, (rid, _dist) in enumerate(sem_hits):
        scores[rid] = scores.get(rid, 0.0) + 1.0 / (K + rank)

    if not scores:
        return {"query": query, "count": 0, "results": []}

    ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[:limit]
    rows = repo.messages_by_rowids([rid for rid, _ in ranked])
    results = []
    for rid, score in ranked:
        row = rows.get(rid)
        if not row:
            continue
        out = _msg_summary(row)
        out["score"] = round(score, 6)
        out["snippet"] = _snippet(row.get("body_text"))
        results.append(out)
    return {"query": query, "mode": mode, "count": len(results), "results": results}


def get_message(repo: Repo, message_id: str, include_body: bool = True) -> dict:
    row = repo.get_message(message_id)
    if not row:
        return {"error": "not_found", "message_id": message_id}
    out = _msg_summary(row)
    out["in_reply_to"] = row.get("in_reply_to")
    out["raw_url"] = row.get("raw_url")
    if include_body:
        out["body"] = row.get("body_text") or ""
    return out


def get_thread(repo: Repo, id_: str, max_messages: int = 200) -> dict:
    # Accept either a thread_id (sha1 hex) or a message_id.
    thread_id = id_
    if not _looks_like_thread_id(id_):
        tid = repo.thread_id_for_message(id_)
        if not tid:
            return {"error": "not_found", "id": id_}
        thread_id = tid
    msgs = repo.get_thread_messages(thread_id, limit=max_messages)
    if not msgs:
        return {"error": "not_found", "thread_id": thread_id}
    # Build parent->children map for a flattened tree with depth
    by_id = {m["message_id"]: m for m in msgs}
    children: dict[Optional[str], list[dict]] = {}
    for m in msgs:
        parent = m.get("in_reply_to") if m.get("in_reply_to") in by_id else None
        children.setdefault(parent, []).append(m)
    for sibs in children.values():
        sibs.sort(key=lambda x: (x.get("date_epoch") or 0, x["message_id"]))

    flat: list[dict] = []
    def walk(parent: Optional[str], depth: int) -> None:
        for m in children.get(parent, []):
            entry = _msg_summary(m)
            entry["depth"] = depth
            entry["body"] = m.get("body_text") or ""
            flat.append(entry)
            walk(m["message_id"], depth + 1)
    walk(None, 0)

    participants = sorted({
        (m.get("from_name") or "", m.get("from_email") or "")
        for m in msgs
    })
    return {
        "thread_id": thread_id,
        "subject": msgs[0].get("subject"),
        "message_count": len(msgs),
        "participants": [{"name": n, "email": e} for n, e in participants],
        "messages": flat,
    }


def list_recent_threads(
    repo: Repo, list_name: str = "pgsql-hackers", since_days: int = 7, limit: int = 50
) -> dict:
    now = int(datetime.now(timezone.utc).timestamp())
    since = now - since_days * 86400
    threads = repo.recent_threads(list_name, since, limit)
    return {
        "list": list_name,
        "since_days": since_days,
        "count": len(threads),
        "threads": [
            {
                "thread_id": t["thread_id"],
                "root_message_id": t["root_message_id"],
                "subject": t["subject_norm"],
                "first_date_epoch": t["first_date_epoch"],
                "last_date_epoch": t["last_date_epoch"],
                "message_count": t["message_count"],
            }
            for t in threads
        ],
    }


def summarize_thread(repo: Repo, id_: str, max_messages: int = 200) -> dict:
    """Returns the structured thread payload (chronological) so the calling LLM
    can summarize. We strip quoted reply lines from each body to make the
    summarizer's job easier."""
    payload = get_thread(repo, id_, max_messages=max_messages)
    if "error" in payload:
        return payload
    digest = []
    for m in payload["messages"]:
        digest.append({
            "message_id": m["message_id"],
            "from": m["from"],
            "date": m["date"],
            "depth": m["depth"],
            "body": _strip_quotes(m.get("body", "")),
            "url": m["url"],
        })
    payload["digest"] = digest
    return payload


def _strip_quotes(body: str) -> str:
    out_lines = []
    for line in body.splitlines():
        ls = line.lstrip()
        if ls.startswith(">"):
            continue
        if ls.startswith("On ") and ls.rstrip().endswith("wrote:"):
            continue
        out_lines.append(line)
    return "\n".join(out_lines).strip()


def _looks_like_thread_id(s: str) -> bool:
    if len(s) != 40:
        return False
    try:
        int(s, 16)
        return True
    except ValueError:
        return False


# ---------------------------------------------------------------- discovery / stats tools

def archive_coverage(repo: Repo) -> dict:
    """Report what data is actually present in the local archive: date range,
    total messages/threads, per-list breakdown, last ingest timestamp, and
    detected month gaps. Call this first when answering 'latest', 'recent',
    or 'popular' questions so you know whether the data is fresh."""
    cov = repo.archive_coverage()
    out: dict[str, Any] = {
        "messages_total": cov["messages_total"],
        "threads_total": cov["threads_total"],
        "last_ingest_at": cov["last_ingest_at"],
        "lists": [],
        "gaps_by_list": cov["gaps_by_list"],
    }
    if cov["min_date_epoch"]:
        out["min_date"] = datetime.fromtimestamp(
            cov["min_date_epoch"], tz=timezone.utc
        ).isoformat()
    if cov["max_date_epoch"]:
        out["max_date"] = datetime.fromtimestamp(
            cov["max_date_epoch"], tz=timezone.utc
        ).isoformat()
    for r in cov["lists"]:
        entry = {
            "list": r["list"],
            "messages": r["messages"],
        }
        if r.get("first_epoch"):
            entry["first_date"] = datetime.fromtimestamp(
                r["first_epoch"], tz=timezone.utc
            ).isoformat()
        if r.get("last_epoch"):
            entry["last_date"] = datetime.fromtimestamp(
                r["last_epoch"], tz=timezone.utc
            ).isoformat()
        out["lists"].append(entry)
    return out


def top_threads_by_activity(
    repo: Repo,
    list_name: str = "pgsql-hackers",
    since_days: int = 14,
    order_by: str = "message_count",
    limit: int = 20,
) -> dict:
    """Most-active threads in the window, ordered by total message count or
    unique participant count."""
    now = int(datetime.now(timezone.utc).timestamp())
    since = now - since_days * 86400 if since_days > 0 else None
    rows = repo.top_threads_by_activity(
        list_name=list_name or None,
        since_epoch=since,
        until_epoch=None,
        order_by=order_by,
        limit=limit,
    )
    return {
        "list": list_name,
        "since_days": since_days,
        "order_by": order_by,
        "count": len(rows),
        "threads": [
            {
                "thread_id": r["thread_id"],
                "list": r["list"],
                "subject": r["subject_norm"],
                "root_message_id": r["root_message_id"],
                "message_count": r["message_count"],
                "participants": r["participants"],
                "first_date_epoch": r["first_date_epoch"],
                "last_date_epoch": r["last_date_epoch"],
            }
            for r in rows
        ],
    }


def top_authors(
    repo: Repo,
    list_name: str = "pgsql-hackers",
    since_days: int = 30,
    limit: int = 20,
) -> dict:
    """Top posters in the window with message + thread counts."""
    now = int(datetime.now(timezone.utc).timestamp())
    since = now - since_days * 86400 if since_days > 0 else None
    rows = repo.top_authors(
        list_name=list_name or None,
        since_epoch=since,
        until_epoch=None,
        limit=limit,
    )
    return {
        "list": list_name,
        "since_days": since_days,
        "count": len(rows),
        "authors": [
            {
                "author": r["author"],
                "email": r["from_email"],
                "messages": r["messages"],
                "threads": r["threads"],
                "first_date_epoch": r["first_epoch"],
                "last_date_epoch": r["last_epoch"],
            }
            for r in rows
        ],
    }


def find_thread_by_url(repo: Repo, url: str) -> dict:
    """Resolve a postgresql.org message URL (or a raw Message-ID) to the local
    message and its thread."""
    row = repo.find_by_url(url)
    if not row:
        return {"error": "not_found", "url": url}
    summary = _msg_summary(row)
    summary["thread_id"] = row.get("thread_id")
    return {"match": summary}


# ---------------------------------------------------------------- git correlation tools

def commits_for_thread(cfg: Config, repo: Repo, thread_id: str) -> dict:
    """Return commits whose `Discussion:` trailer points at any message in
    the given thread. Requires PGMAIL_PG_GIT_PATH to be set."""
    from ..store import git_log
    try:
        rows = git_log.commits_for_thread(repo, cfg.pg_git_path, thread_id)
    except git_log.GitNotConfigured as e:
        return {"error": "git_not_configured", "hint": str(e)}
    except Exception as e:  # noqa: BLE001
        return {"error": "git_failed", "detail": str(e)}
    return {
        "thread_id": thread_id,
        "count": len(rows),
        "commits": [
            {
                "sha": r["sha"],
                "author": r["author_name"],
                "email": r["author_email"],
                "committed_at": r["committed_at"],
                "subject": r["subject"],
                "discussion_url": r["discussion_url"],
            }
            for r in rows
        ],
    }


def recent_commits(
    cfg: Config,
    repo: Repo,
    since_days: int = 7,
    path_glob: Optional[str] = None,
    limit: int = 50,
) -> dict:
    """List commits to postgres/postgres in the recent window. Optionally
    filter by a path glob (e.g. 'src/backend/storage/buffer/')."""
    from ..store import git_log
    try:
        rows = git_log.recent_commits(repo, cfg.pg_git_path,
                                      since_days=since_days,
                                      path_glob=path_glob, limit=limit)
    except git_log.GitNotConfigured as e:
        return {"error": "git_not_configured", "hint": str(e)}
    except Exception as e:  # noqa: BLE001
        return {"error": "git_failed", "detail": str(e)}
    return {
        "since_days": since_days,
        "path_glob": path_glob,
        "count": len(rows),
        "commits": [
            {
                "sha": r["sha"],
                "author": r["author_name"],
                "email": r["author_email"],
                "committed_at": r["committed_at"],
                "subject": r["subject"],
                "discussion_url": r["discussion_url"],
            }
            for r in rows
        ],
    }


# ---------------------------------------------------------------- synthesis tools

# Rule-based message classifier used by thread_timeline. Order matters: the
# first matching kind wins (consensus > nack > patch_v > review > discussion).
import re as _re

_PATCH_HEADER_RE = _re.compile(r"(?m)^From [0-9a-f]{40} ")
_PATCH_DIFF_RE = _re.compile(r"(?m)^diff --git ")
_PATCH_VERSION_RE = _re.compile(r"\bv\d{1,3}(?:[-.]\d+)?\b")
_NACK_PATTERNS = (
    "i don't think we should",
    "i don't think this is",
    "strongly object",
    "cannot accept",
    "-1 from me",
    "i'm against",
)
_REVIEW_PATTERNS = (
    "reviewed-by:",
    "looks good to me",
    "lgtm",
    "+1 from me",
    "i agree",
    "minor nit",
    "review comments",
)
_CONSENSUS_PATTERNS = (
    "committed.",
    "pushed.",
    "applied as",
    "i've pushed",
    "i have pushed",
    "i've committed",
    "i have committed",
    "marking as committed",
)


def _classify_message(subject: str, body: str) -> str:
    s = (subject or "").lower()
    b = (body or "").lower()
    # Consensus / committed wins.
    for p in _CONSENSUS_PATTERNS:
        if p in b:
            return "consensus"
    for p in _NACK_PATTERNS:
        if p in b:
            return "nack"
    # Patch detection: explicit git patch header, or vNN in subject.
    if _PATCH_HEADER_RE.search(body or "") or _PATCH_DIFF_RE.search(body or ""):
        return "patch_v"
    if _PATCH_VERSION_RE.search(s):
        return "patch_v"
    for p in _REVIEW_PATTERNS:
        if p in b:
            return "review"
    return "discussion"


def thread_timeline(repo: Repo, id_: str, max_messages: int = 200) -> dict:
    """Classified, chronological timeline of a thread.

    Each message gets a `kind` of patch_v / review / nack / consensus /
    discussion based on simple body+subject heuristics.
    """
    payload = get_thread(repo, id_, max_messages=max_messages)
    if "error" in payload:
        return payload
    timeline = []
    counts: dict[str, int] = {
        "patch_v": 0, "review": 0, "nack": 0, "consensus": 0, "discussion": 0,
    }
    # get_thread returns messages depth-first; re-sort by date for the timeline.
    msgs = sorted(
        payload["messages"],
        key=lambda m: (m.get("date") or "", m["message_id"]),
    )
    for m in msgs:
        kind = _classify_message(m.get("subject", ""), m.get("body", ""))
        counts[kind] = counts.get(kind, 0) + 1
        timeline.append({
            "message_id": m["message_id"],
            "date": m["date"],
            "from": m["from"],
            "kind": kind,
            "subject": m.get("subject"),
            "url": m.get("url"),
            "snippet": _snippet(m.get("body", "")),
        })
    # Status = the latest non-discussion kind, else discussion.
    status = "discussion"
    for entry in reversed(timeline):
        if entry["kind"] != "discussion":
            status = entry["kind"]
            break
    return {
        "thread_id": payload["thread_id"],
        "subject": payload["subject"],
        "message_count": payload["message_count"],
        "status": status,
        "kind_counts": counts,
        "timeline": timeline,
    }


def compare_threads(repo: Repo, ids: list[str]) -> dict:
    """Side-by-side comparison of 2-6 threads. Each entry includes status
    derived from thread_timeline so the comparison is meaningful."""
    if not ids:
        return {"error": "no_ids", "hint": "pass 2-6 thread_ids or message_ids"}
    if len(ids) > 6:
        return {"error": "too_many", "hint": "compare at most 6 threads"}
    results = []
    for raw in ids:
        thread_id = raw
        if not _looks_like_thread_id(raw):
            tid = repo.thread_id_for_message(raw)
            if not tid:
                results.append({"input_id": raw, "error": "not_found"})
                continue
            thread_id = tid
        summary = repo.thread_summary(thread_id)
        if not summary:
            results.append({"input_id": raw, "thread_id": thread_id, "error": "not_found"})
            continue
        timeline = thread_timeline(repo, thread_id, max_messages=500)
        # open_for_days from epochs
        open_days = None
        if summary.get("first_date_epoch") and summary.get("last_date_epoch"):
            open_days = round(
                (summary["last_date_epoch"] - summary["first_date_epoch"]) / 86400, 1
            )
        results.append({
            "thread_id": thread_id,
            "subject": summary.get("subject_norm"),
            "list": summary.get("list"),
            "message_count": summary.get("message_count"),
            "participants": summary.get("participants"),
            "first_date_epoch": summary.get("first_date_epoch"),
            "last_date_epoch": summary.get("last_date_epoch"),
            "open_for_days": open_days,
            "status": timeline.get("status"),
            "kind_counts": timeline.get("kind_counts"),
            "latest_from": {
                "name": summary.get("latest_from_name"),
                "email": summary.get("latest_from_email"),
            },
            "latest_date": summary.get("latest_date"),
        })
    return {"count": len(results), "threads": results}


# ---------------------------------------------------------------- watchlist tools

def watchlist_add(
    repo: Repo, label: str, kind: str, value: str,
    list_name: str = "pgsql-hackers",
) -> dict:
    """Add a watchlist entry. kind = 'thread' (value=thread_id),
    'query' (value=search query), or 'author' (value=email substring)."""
    try:
        wid = repo.watchlist_add(label, kind, value, list_name)
    except ValueError as e:
        return {"error": "invalid_kind", "detail": str(e)}
    return {"id": wid, "label": label, "kind": kind, "value": value, "list": list_name}


def watchlist_list(repo: Repo) -> dict:
    rows = repo.watchlist_list()
    return {
        "count": len(rows),
        "entries": [
            {
                "id": r["id"], "label": r["label"], "kind": r["kind"],
                "value": r["value"], "list": r["list_name"],
                "last_checked_epoch": r["last_checked_epoch"],
                "created_at": r["created_at"],
            }
            for r in rows
        ],
    }


def watchlist_remove(repo: Repo, watch_id: int) -> dict:
    ok = repo.watchlist_remove(int(watch_id))
    return {"removed": ok, "id": int(watch_id)}


def watchlist_check(
    cfg: Config, repo: Repo, embedder: Optional[Embedder],
    touch: bool = True, per_entry_limit: int = 25,
) -> dict:
    """For each watchlist entry, return new messages since last_checked_epoch.
    If `touch` is true, advance last_checked_epoch to now after the check."""
    now = int(datetime.now(timezone.utc).timestamp())
    entries = repo.watchlist_list()
    out = []
    for r in entries:
        since = r["last_checked_epoch"] or (now - 30 * 86400)  # default 30d
        kind = r["kind"]
        new_msgs: list[dict] = []
        if kind == "thread":
            new_msgs = repo.messages_in_thread_since(r["value"], since)
        elif kind == "author":
            new_msgs = repo.messages_by_author_since(
                r["value"], r["list_name"] or "pgsql-hackers", since, per_entry_limit
            )
        elif kind == "query":
            # Reuse search_messages with date_from=since.
            df_iso = datetime.fromtimestamp(since, tz=timezone.utc).isoformat()
            res = search_messages(
                cfg, repo, embedder, r["value"],
                list_name=r["list_name"] or "pgsql-hackers",
                date_from=df_iso, date_to=None, author=None,
                limit=per_entry_limit, mode="hybrid",
            )
            # search_messages returns its own shape; flatten to message list
            new_msgs = res.get("results", [])
        out.append({
            "id": r["id"],
            "label": r["label"],
            "kind": kind,
            "value": r["value"],
            "since_epoch": since,
            "new_count": len(new_msgs),
            "new_messages": [
                _msg_summary(m) if "message_id" in m and "thread_id" in m and "from_name" in m
                else m
                for m in new_msgs[:per_entry_limit]
            ],
        })
        if touch:
            repo.watchlist_touch(r["id"], now)
    return {"checked_at_epoch": now, "count": len(out), "entries": out}


# ---------------------------------------------------------------- export

def export_thread(repo: Repo, id_: str, format: str = "markdown",
                  max_messages: int = 500) -> dict:
    """Export a thread as Markdown or JSON. Markdown format is suitable for
    pasting into a doc; JSON is the raw structured payload."""
    if format not in ("markdown", "json"):
        return {"error": "invalid_format", "hint": "format must be 'markdown' or 'json'"}
    payload = get_thread(repo, id_, max_messages=max_messages)
    if "error" in payload:
        return payload
    if format == "json":
        return {"format": "json", "thread": payload}
    # Markdown rendering
    lines: list[str] = []
    lines.append(f"# {payload.get('subject') or '(no subject)'}")
    lines.append("")
    lines.append(f"- **Thread ID:** `{payload['thread_id']}`")
    lines.append(f"- **Messages:** {payload['message_count']}")
    parts = payload.get("participants") or []
    if parts:
        names = ", ".join(
            f"{p.get('name') or p.get('email')}" for p in parts if p.get('name') or p.get('email')
        )
        lines.append(f"- **Participants:** {names}")
    lines.append("")
    lines.append("---")
    lines.append("")
    for m in payload["messages"]:
        depth = m.get("depth", 0)
        prefix = ">" * depth + (" " if depth else "")
        author = (m.get("from") or {}).get("name") or (m.get("from") or {}).get("email") or "(unknown)"
        date = m.get("date") or ""
        url = m.get("url") or ""
        lines.append(f"{prefix}### {author} \u2014 {date}")
        if url:
            lines.append(f"{prefix}<{url}>")
        lines.append("")
        body = (m.get("body") or "").rstrip()
        if body:
            for bl in body.splitlines():
                lines.append(f"{prefix}{bl}" if depth else bl)
        lines.append("")
        lines.append(f"{prefix}---")
        lines.append("")
    md = "\n".join(lines)
    return {
        "format": "markdown",
        "thread_id": payload["thread_id"],
        "subject": payload.get("subject"),
        "message_count": payload["message_count"],
        "markdown": md,
    }
