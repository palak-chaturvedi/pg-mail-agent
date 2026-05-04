"""sqlite-vec helpers. Optional: if the extension is unavailable, semantic
search becomes a no-op and the server falls back to FTS-only ranking."""
from __future__ import annotations

import sqlite3
from typing import Optional


def load_vec(conn: sqlite3.Connection) -> bool:
    """Try to load the sqlite-vec extension. Returns True on success."""
    try:
        import sqlite_vec  # type: ignore
    except ImportError:
        return False
    try:
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        conn.enable_load_extension(False)
        return True
    except (sqlite3.OperationalError, AttributeError):
        return False


def ensure_vec_table(conn: sqlite3.Connection, dim: int) -> bool:
    """Create the vec0 virtual table for message embeddings if missing.
    Returns True if the table is available after this call."""
    if not load_vec(conn):
        return False
    # vec0 exposes an implicit INTEGER rowid; we map it to messages.rowid.
    conn.execute(
        f"CREATE VIRTUAL TABLE IF NOT EXISTS messages_vec USING vec0(embedding FLOAT[{dim}])"
    )
    return True


def upsert_vector(conn: sqlite3.Connection, rowid: int, embedding: bytes) -> None:
    conn.execute("DELETE FROM messages_vec WHERE rowid = ?", (rowid,))
    conn.execute(
        "INSERT INTO messages_vec(rowid, embedding) VALUES (?, ?)",
        (rowid, embedding),
    )


def search_vec(
    conn: sqlite3.Connection, query_embedding: bytes, limit: int
) -> list[tuple[int, float]]:
    cur = conn.execute(
        """
        SELECT rowid, distance
        FROM messages_vec
        WHERE embedding MATCH ?
        ORDER BY distance
        LIMIT ?
        """,
        (query_embedding, limit),
    )
    return [(int(r[0]), float(r[1])) for r in cur.fetchall()]
