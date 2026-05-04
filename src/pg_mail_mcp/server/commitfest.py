"""Thin client over the postgresql.org commitfest API.

Caching: payloads are cached in the SQLite `commitfest_cache` table for 24h
keyed by URL.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx

from ..config import Config
from ..store.repo import Repo


CACHE_TTL = timedelta(hours=24)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _cached(repo: Repo, key: str) -> Optional[Any]:
    conn = repo.connect()
    row = conn.execute(
        "SELECT payload, fetched_at FROM commitfest_cache WHERE cache_key = ?", (key,)
    ).fetchone()
    if not row:
        return None
    fetched = datetime.fromisoformat(row["fetched_at"])
    if _now_utc() - fetched > CACHE_TTL:
        return None
    return json.loads(row["payload"])


def _cache(repo: Repo, key: str, payload: Any) -> None:
    conn = repo.connect()
    conn.execute(
        """INSERT INTO commitfest_cache(cache_key, payload, fetched_at)
           VALUES (?, ?, ?)
           ON CONFLICT(cache_key) DO UPDATE SET
               payload=excluded.payload, fetched_at=excluded.fetched_at""",
        (key, json.dumps(payload), _now_utc().isoformat()),
    )


def search_commitfest(cfg: Config, repo: Repo, query: str, limit: int = 10) -> list[dict]:
    """Search commitfest entries. The CF app does not expose a documented JSON
    search endpoint at a stable URL, so we use the public HTML search and
    return links the agent can follow. We cache by query string.
    """
    key = f"cf:search:{query.lower()}:{limit}"
    cached = _cached(repo, key)
    if cached is not None:
        return cached
    url = f"{cfg.commitfest_base}/search/"
    params = {"searchterm": query}
    headers = {"User-Agent": cfg.user_agent}
    # The CF app may require auth via the same postgresql.org SSO. Reuse the
    # PGMAIL_COOKIE if set so the search isn't bounced to a login redirect.
    cookies: dict[str, str] = {}
    if cfg.cookie:
        for part in cfg.cookie.split(";"):
            if "=" in part:
                k, v = part.split("=", 1)
                cookies[k.strip()] = v.strip()
    try:
        with httpx.Client(timeout=cfg.http_timeout, follow_redirects=True,
                          headers=headers, cookies=cookies) as c:
            resp = c.get(url, params=params)
            resp.raise_for_status()
            html = resp.text
            final_url = str(resp.url)
    except httpx.HTTPError as e:
        return {"error": f"commitfest fetch failed: {e}", "query": query}

    if "_auth/" in final_url or "/login" in final_url or "/account/auth/" in final_url:
        return {
            "error": "commitfest search requires a logged-in session. "
                     "Open the search_url in a browser to view results.",
            "query": query,
            "search_url": f"{cfg.commitfest_base}/search/?searchterm={query}",
        }

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    results: list[dict] = []
    seen: set[str] = set()
    for a in soup.select("a[href*='/patch/'], a[href*='/cf/']"):
        href = a.get("href", "")
        if not href.startswith("http"):
            href = cfg.commitfest_base.rstrip("/") + href
        if href in seen:
            continue
        seen.add(href)
        title = a.get_text(strip=True)
        if not title:
            continue
        results.append({"title": title, "url": href})
        if len(results) >= limit:
            break
    payload = {"query": query, "search_url": final_url, "results": results}
    _cache(repo, key, payload)
    return payload


def get_commitfest_entry(cfg: Config, repo: Repo, entry_id: int) -> dict:
    """Fetch and parse a single commitfest entry page.

    The CF app exposes patch detail at /patch/<id>/ as public HTML (no SSO
    needed for read). We extract title, status, authors, reviewers, target
    version, and the linked Discussion mailing-list threads.
    """
    key = f"cf:entry:{int(entry_id)}"
    cached = _cached(repo, key)
    if cached is not None:
        return cached
    url = f"{cfg.commitfest_base.rstrip('/')}/patch/{int(entry_id)}/"
    headers = {"User-Agent": cfg.user_agent}
    cookies: dict[str, str] = {}
    if cfg.cookie:
        for part in cfg.cookie.split(";"):
            if "=" in part:
                k, v = part.split("=", 1)
                cookies[k.strip()] = v.strip()
    try:
        with httpx.Client(timeout=cfg.http_timeout, follow_redirects=True,
                          headers=headers, cookies=cookies) as c:
            resp = c.get(url)
            resp.raise_for_status()
            html = resp.text
            final_url = str(resp.url)
    except httpx.HTTPError as e:
        return {"error": f"commitfest entry fetch failed: {e}", "entry_id": entry_id}

    if "_auth/" in final_url or "/login" in final_url:
        return {
            "error": "commitfest entry requires authentication",
            "entry_id": entry_id,
            "url": url,
        }

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")

    def _label_value(label: str) -> Optional[str]:
        for dt in soup.find_all(["dt", "th"]):
            if dt.get_text(strip=True).lower().rstrip(":") == label.lower():
                sib = dt.find_next_sibling(["dd", "td"])
                if sib:
                    return sib.get_text(" ", strip=True)
        return None

    # Title is usually in the page <h1>.
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else None

    # Discussion thread links — anchors that point at the message-id archive.
    discussion: list[str] = []
    seen: set[str] = set()
    for a in soup.select("a[href*='/message-id/']"):
        href = a.get("href", "")
        if href.startswith("/"):
            href = cfg.archive_base.rstrip("/") + href
        if href in seen:
            continue
        seen.add(href)
        discussion.append(href)

    payload = {
        "entry_id": int(entry_id),
        "url": final_url,
        "title": title,
        "status": _label_value("Status"),
        "target_version": _label_value("Target version") or _label_value("Target"),
        "authors": _label_value("Authors") or _label_value("Author"),
        "reviewers": _label_value("Reviewers") or _label_value("Reviewer"),
        "committer": _label_value("Committer"),
        "discussion_urls": discussion,
    }
    _cache(repo, key, payload)
    return payload


def commitfest_for_thread(
    cfg: Config, repo: Repo, thread_id: str, limit: int = 5
) -> dict:
    """Best-effort: find commitfest entries that look related to a thread.

    Strategy: take the thread's normalized subject and run the commitfest
    search. The CF search requires SSO; if that fails we surface the search
    URL so the user can open it manually.
    """
    conn = repo.connect()
    row = conn.execute(
        "SELECT subject_norm, root_message_id FROM threads WHERE thread_id = ?",
        (thread_id,),
    ).fetchone()
    if not row:
        return {"error": "thread_not_found", "thread_id": thread_id}
    subject = (row["subject_norm"] or "").strip()
    if not subject:
        return {"error": "thread_has_no_subject", "thread_id": thread_id}
    # Truncate to keep the query short and selective.
    short = subject[:80]
    return search_commitfest(cfg, repo, short, limit)

