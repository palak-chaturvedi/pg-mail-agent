"""Repository: typed SQLite query helpers."""
from __future__ import annotations

import json
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from importlib import resources
from pathlib import Path
from typing import Iterable, Iterator, Optional

from ..config import Config
from . import vec as vec_mod


@dataclass
class MessageRow:
    message_id: str
    list: str
    subject: Optional[str]
    from_name: Optional[str]
    from_email: Optional[str]
    date_utc: Optional[str]
    date_epoch: Optional[int]
    in_reply_to: Optional[str]
    thread_id: Optional[str]
    body_text: Optional[str]
    web_url: Optional[str]
    raw_url: Optional[str]


_FTS_SPECIAL = re.compile(r'["\(\)\*:^]')


def sanitize_fts_query(q: str) -> str:
    """Make a user-provided string safe to embed in an FTS5 MATCH expression
    by quoting each whitespace-separated term."""
    terms = [t for t in _FTS_SPECIAL.sub(" ", q).split() if t]
    return " ".join(f'"{t}"' for t in terms) if terms else ""


class Repo:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        cfg.ensure_db_dir()
        self._conn: Optional[sqlite3.Connection] = None

    # ------------------------------------------------------------------ conn
    def connect(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        conn = sqlite3.connect(self.cfg.db_path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        self._conn = conn
        return conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        conn.execute("BEGIN")
        try:
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------------ init
    def init_schema(self) -> None:
        conn = self.connect()
        sql = resources.files("pg_mail_mcp.store").joinpath("schema.sql").read_text(
            encoding="utf-8"
        )
        conn.executescript(sql)
        if self.cfg.embedder != "none":
            vec_mod.ensure_vec_table(conn, self.cfg.embedding_dim)

    # ------------------------------------------------------------------ writes
    def upsert_message(self, m: MessageRow, refs: list[str]) -> int:
        conn = self.connect()
        conn.execute(
            """
            INSERT INTO messages(
                message_id, list, subject, subject_norm, from_name, from_email,
                date_utc, date_epoch, in_reply_to, thread_id, body_text,
                raw_url, web_url
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(message_id) DO UPDATE SET
                list=excluded.list,
                subject=excluded.subject,
                subject_norm=excluded.subject_norm,
                from_name=excluded.from_name,
                from_email=excluded.from_email,
                date_utc=excluded.date_utc,
                date_epoch=excluded.date_epoch,
                in_reply_to=excluded.in_reply_to,
                thread_id=excluded.thread_id,
                body_text=excluded.body_text,
                raw_url=excluded.raw_url,
                web_url=excluded.web_url
            """,
            (
                m.message_id, m.list, m.subject, _normalize_subject(m.subject or ""),
                m.from_name, m.from_email, m.date_utc, m.date_epoch,
                m.in_reply_to, m.thread_id, m.body_text, m.raw_url, m.web_url,
            ),
        )
        conn.execute("DELETE FROM message_refs WHERE message_id = ?", (m.message_id,))
        if refs:
            conn.executemany(
                "INSERT OR IGNORE INTO message_refs(message_id, ref_message_id, position) VALUES (?,?,?)",
                [(m.message_id, r, i) for i, r in enumerate(refs)],
            )
        row = conn.execute(
            "SELECT rowid FROM messages WHERE message_id = ?", (m.message_id,)
        ).fetchone()
        return int(row[0])

    def upsert_thread(
        self,
        thread_id: str,
        list_name: str,
        root_message_id: Optional[str],
        subject_norm: str,
        first_epoch: Optional[int],
        last_epoch: Optional[int],
        message_count: int,
    ) -> None:
        conn = self.connect()
        conn.execute(
            """
            INSERT INTO threads(thread_id, list, root_message_id, subject_norm,
                                first_date_epoch, last_date_epoch, message_count)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(thread_id) DO UPDATE SET
                root_message_id=excluded.root_message_id,
                subject_norm=excluded.subject_norm,
                first_date_epoch=MIN(threads.first_date_epoch, excluded.first_date_epoch),
                last_date_epoch=MAX(threads.last_date_epoch, excluded.last_date_epoch),
                message_count=excluded.message_count
            """,
            (
                thread_id, list_name, root_message_id, subject_norm,
                first_epoch, last_epoch, message_count,
            ),
        )

    def record_ingest(
        self,
        list_name: str,
        yyyymm: str,
        etag: Optional[str],
        last_modified: Optional[str],
        message_count: int,
    ) -> None:
        conn = self.connect()
        conn.execute(
            """
            INSERT INTO ingest_state(list, yyyymm, etag, last_modified, fetched_at, message_count)
            VALUES (?,?,?,?, datetime('now'), ?)
            ON CONFLICT(list, yyyymm) DO UPDATE SET
                etag=excluded.etag,
                last_modified=excluded.last_modified,
                fetched_at=excluded.fetched_at,
                message_count=excluded.message_count
            """,
            (list_name, yyyymm, etag, last_modified, message_count),
        )

    def get_ingest_state(self, list_name: str, yyyymm: str) -> Optional[sqlite3.Row]:
        conn = self.connect()
        return conn.execute(
            "SELECT * FROM ingest_state WHERE list=? AND yyyymm=?",
            (list_name, yyyymm),
        ).fetchone()

    # ------------------------------------------------------------------ reads
    def get_message(self, message_id: str) -> Optional[dict]:
        conn = self.connect()
        row = conn.execute(
            "SELECT * FROM messages WHERE message_id = ?", (message_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_thread_messages(self, thread_id: str, limit: int = 500) -> list[dict]:
        conn = self.connect()
        rows = conn.execute(
            """SELECT * FROM messages WHERE thread_id = ?
               ORDER BY date_epoch ASC, message_id ASC LIMIT ?""",
            (thread_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def thread_id_for_message(self, message_id: str) -> Optional[str]:
        conn = self.connect()
        row = conn.execute(
            "SELECT thread_id FROM messages WHERE message_id = ?", (message_id,)
        ).fetchone()
        return row[0] if row else None

    def recent_threads(self, list_name: str, since_epoch: int, limit: int) -> list[dict]:
        conn = self.connect()
        rows = conn.execute(
            """SELECT * FROM threads
               WHERE list = ? AND last_date_epoch >= ?
               ORDER BY last_date_epoch DESC LIMIT ?""",
            (list_name, since_epoch, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def search_fts(
        self,
        query: str,
        list_name: Optional[str],
        date_from_epoch: Optional[int],
        date_to_epoch: Optional[int],
        author: Optional[str],
        limit: int,
    ) -> list[tuple[int, float, dict]]:
        conn = self.connect()
        match = sanitize_fts_query(query)
        if not match:
            return []
        sql = [
            "SELECT m.rowid AS rid, bm25(messages_fts) AS score, m.* ",
            "FROM messages_fts JOIN messages m ON m.rowid = messages_fts.rowid ",
            "WHERE messages_fts MATCH ? ",
        ]
        params: list = [match]
        if list_name:
            sql.append("AND m.list = ? "); params.append(list_name)
        if date_from_epoch is not None:
            sql.append("AND m.date_epoch >= ? "); params.append(date_from_epoch)
        if date_to_epoch is not None:
            sql.append("AND m.date_epoch <= ? "); params.append(date_to_epoch)
        if author:
            sql.append("AND (m.from_name LIKE ? OR m.from_email LIKE ?) ")
            params.extend([f"%{author}%", f"%{author}%"])
        sql.append("ORDER BY score LIMIT ?"); params.append(limit)
        rows = conn.execute("".join(sql), params).fetchall()
        return [(int(r["rid"]), float(r["score"]), dict(r)) for r in rows]

    def messages_by_rowids(self, rowids: Iterable[int]) -> dict[int, dict]:
        ids = list(rowids)
        if not ids:
            return {}
        conn = self.connect()
        qmarks = ",".join("?" * len(ids))
        rows = conn.execute(
            f"SELECT rowid AS rid, * FROM messages WHERE rowid IN ({qmarks})", ids
        ).fetchall()
        return {int(r["rid"]): dict(r) for r in rows}

    # ------------------------------------------------------------------ stats / discovery

    def archive_coverage(self) -> dict:
        """Summarize what data is actually present in the local store."""
        conn = self.connect()
        totals = conn.execute(
            "SELECT COUNT(*) AS n, MIN(date_epoch) AS mn, MAX(date_epoch) AS mx FROM messages"
        ).fetchone()
        thread_total = conn.execute("SELECT COUNT(*) AS n FROM threads").fetchone()["n"]
        per_list = conn.execute(
            """SELECT list, COUNT(*) AS messages,
                      MIN(date_epoch) AS first_epoch,
                      MAX(date_epoch) AS last_epoch
               FROM messages GROUP BY list ORDER BY messages DESC"""
        ).fetchall()
        last_ingest = conn.execute(
            "SELECT MAX(fetched_at) AS ts FROM ingest_state"
        ).fetchone()["ts"]
        # Months covered per list, plus gap detection
        months_rows = conn.execute(
            "SELECT list, yyyymm, message_count FROM ingest_state ORDER BY list, yyyymm"
        ).fetchall()
        months_by_list: dict[str, list[dict]] = {}
        for r in months_rows:
            months_by_list.setdefault(r["list"], []).append(
                {"yyyymm": r["yyyymm"], "message_count": r["message_count"]}
            )
        gaps: dict[str, list[str]] = {}
        for lst, months in months_by_list.items():
            ms = [m["yyyymm"] for m in months]
            gaps[lst] = _detect_month_gaps(ms)
        return {
            "messages_total": int(totals["n"] or 0),
            "threads_total": int(thread_total or 0),
            "min_date_epoch": totals["mn"],
            "max_date_epoch": totals["mx"],
            "last_ingest_at": last_ingest,
            "lists": [dict(r) for r in per_list],
            "months_by_list": months_by_list,
            "gaps_by_list": gaps,
        }

    def top_threads_by_activity(
        self,
        list_name: Optional[str],
        since_epoch: Optional[int],
        until_epoch: Optional[int],
        order_by: str = "message_count",
        limit: int = 20,
    ) -> list[dict]:
        """Top threads ordered by message count or unique participant count
        within the given time window (based on threads.last_date_epoch)."""
        if order_by not in ("message_count", "participants"):
            order_by = "message_count"
        conn = self.connect()
        sql = [
            "SELECT t.thread_id, t.list, t.subject_norm, t.root_message_id, ",
            "       t.first_date_epoch, t.last_date_epoch, t.message_count, ",
            "       (SELECT COUNT(DISTINCT COALESCE(m.from_email, m.from_name)) ",
            "          FROM messages m WHERE m.thread_id = t.thread_id) AS participants ",
            "FROM threads t WHERE 1=1 ",
        ]
        params: list = []
        if list_name:
            sql.append("AND t.list = ? "); params.append(list_name)
        if since_epoch is not None:
            sql.append("AND t.last_date_epoch >= ? "); params.append(since_epoch)
        if until_epoch is not None:
            sql.append("AND t.last_date_epoch <= ? "); params.append(until_epoch)
        if order_by == "participants":
            sql.append("ORDER BY participants DESC, t.message_count DESC ")
        else:
            sql.append("ORDER BY t.message_count DESC, t.last_date_epoch DESC ")
        sql.append("LIMIT ?"); params.append(limit)
        rows = conn.execute("".join(sql), params).fetchall()
        return [dict(r) for r in rows]

    def top_authors(
        self,
        list_name: Optional[str],
        since_epoch: Optional[int],
        until_epoch: Optional[int],
        limit: int = 20,
    ) -> list[dict]:
        """Top posters by message count in the window."""
        conn = self.connect()
        sql = [
            "SELECT COALESCE(NULLIF(from_name, ''), from_email) AS author, ",
            "       from_email, ",
            "       COUNT(*) AS messages, ",
            "       COUNT(DISTINCT thread_id) AS threads, ",
            "       MIN(date_epoch) AS first_epoch, ",
            "       MAX(date_epoch) AS last_epoch ",
            "FROM messages WHERE from_email IS NOT NULL ",
        ]
        params: list = []
        if list_name:
            sql.append("AND list = ? "); params.append(list_name)
        if since_epoch is not None:
            sql.append("AND date_epoch >= ? "); params.append(since_epoch)
        if until_epoch is not None:
            sql.append("AND date_epoch <= ? "); params.append(until_epoch)
        sql.append("GROUP BY from_email ORDER BY messages DESC LIMIT ?")
        params.append(limit)
        rows = conn.execute("".join(sql), params).fetchall()
        return [dict(r) for r in rows]

    def find_by_url(self, url: str) -> Optional[dict]:
        """Resolve a postgresql.org message URL (or raw Message-ID) to the
        local message + thread."""
        if not url:
            return None
        # Extract the message-id segment from the URL if it looks like one.
        candidate = url.strip()
        marker = "/message-id/"
        if marker in candidate:
            candidate = candidate.split(marker, 1)[1]
            # strip trailing slash, anchors, query
            for sep in ("#", "?"):
                if sep in candidate:
                    candidate = candidate.split(sep, 1)[0]
            candidate = candidate.rstrip("/")
        # candidate is now expected to be a Message-ID (URL-encoded sometimes)
        from urllib.parse import unquote
        candidate = unquote(candidate)
        conn = self.connect()
        row = conn.execute(
            "SELECT * FROM messages WHERE message_id = ? OR web_url = ?",
            (candidate, url.strip()),
        ).fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------ watchlist

    def watchlist_add(self, label: str, kind: str, value: str,
                      list_name: str = "pgsql-hackers") -> int:
        if kind not in ("thread", "query", "author"):
            raise ValueError(f"invalid watchlist kind: {kind}")
        conn = self.connect()
        cur = conn.execute(
            """INSERT INTO watchlist(label, kind, value, list_name, last_checked_epoch)
               VALUES (?,?,?,?,?)
               ON CONFLICT(kind, value, list_name) DO UPDATE SET label=excluded.label
               RETURNING id""",
            (label, kind, value, list_name, None),
        )
        row = cur.fetchone()
        return int(row[0])

    def watchlist_list(self) -> list[dict]:
        conn = self.connect()
        rows = conn.execute(
            "SELECT * FROM watchlist ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rows]

    def watchlist_remove(self, watch_id: int) -> bool:
        conn = self.connect()
        cur = conn.execute("DELETE FROM watchlist WHERE id = ?", (watch_id,))
        return cur.rowcount > 0

    def watchlist_touch(self, watch_id: int, epoch: int) -> None:
        conn = self.connect()
        conn.execute(
            "UPDATE watchlist SET last_checked_epoch = ? WHERE id = ?",
            (epoch, watch_id),
        )

    def messages_in_thread_since(
        self, thread_id: str, since_epoch: int
    ) -> list[dict]:
        conn = self.connect()
        rows = conn.execute(
            """SELECT * FROM messages
               WHERE thread_id = ? AND date_epoch > ?
               ORDER BY date_epoch ASC""",
            (thread_id, since_epoch),
        ).fetchall()
        return [dict(r) for r in rows]

    def messages_by_author_since(
        self, author_value: str, list_name: str, since_epoch: int, limit: int
    ) -> list[dict]:
        conn = self.connect()
        like = f"%{author_value}%"
        rows = conn.execute(
            """SELECT * FROM messages
               WHERE list = ?
                 AND (from_email LIKE ? OR from_name LIKE ?)
                 AND date_epoch > ?
               ORDER BY date_epoch DESC LIMIT ?""",
            (list_name, like, like, since_epoch, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def thread_summary(self, thread_id: str) -> Optional[dict]:
        """Lightweight thread metadata for compare/timeline tools."""
        conn = self.connect()
        t = conn.execute(
            "SELECT * FROM threads WHERE thread_id = ?", (thread_id,)
        ).fetchone()
        if not t:
            return None
        participants = conn.execute(
            """SELECT COUNT(DISTINCT COALESCE(from_email, from_name)) AS n
               FROM messages WHERE thread_id = ?""",
            (thread_id,),
        ).fetchone()["n"]
        latest = conn.execute(
            """SELECT from_name, from_email, date_utc, body_text, subject
               FROM messages WHERE thread_id = ?
               ORDER BY date_epoch DESC LIMIT 1""",
            (thread_id,),
        ).fetchone()
        out = dict(t)
        out["participants"] = int(participants or 0)
        if latest:
            out["latest_from_name"] = latest["from_name"]
            out["latest_from_email"] = latest["from_email"]
            out["latest_date"] = latest["date_utc"]
            out["latest_body"] = latest["body_text"] or ""
            out["latest_subject"] = latest["subject"]
        return out


def _detect_month_gaps(months: list[str]) -> list[str]:
    """Given a sorted list of YYYYMM strings, return missing months between
    the first and last entry."""
    if len(months) < 2:
        return []
    def to_int(s: str) -> int:
        y, m = int(s[:4]), int(s[4:6])
        return y * 12 + (m - 1)
    def from_int(x: int) -> str:
        y, m = divmod(x, 12)
        return f"{y:04d}{m + 1:02d}"
    present = {to_int(s) for s in months}
    lo, hi = min(present), max(present)
    return [from_int(i) for i in range(lo, hi + 1) if i not in present]


def _normalize_subject(s: str) -> str:
    s = re.sub(r"^(?:\s*(?:re|fwd?|aw)\s*:\s*)+", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s
