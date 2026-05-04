-- pg-mail-mcp SQLite schema. Safe to run repeatedly (IF NOT EXISTS).

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS messages (
    message_id     TEXT PRIMARY KEY,
    list           TEXT NOT NULL,
    subject        TEXT,
    subject_norm   TEXT,
    from_name      TEXT,
    from_email     TEXT,
    date_utc       TEXT,                 -- ISO-8601 UTC
    date_epoch     INTEGER,
    in_reply_to    TEXT,
    thread_id      TEXT,
    body_text      TEXT,
    raw_url        TEXT,
    web_url        TEXT,
    inserted_at    TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_messages_list_date ON messages(list, date_epoch DESC);
CREATE INDEX IF NOT EXISTS idx_messages_thread    ON messages(thread_id);
CREATE INDEX IF NOT EXISTS idx_messages_in_reply  ON messages(in_reply_to);

CREATE TABLE IF NOT EXISTS message_refs (
    message_id     TEXT NOT NULL,
    ref_message_id TEXT NOT NULL,
    position       INTEGER NOT NULL,
    PRIMARY KEY (message_id, ref_message_id)
);

CREATE TABLE IF NOT EXISTS threads (
    thread_id        TEXT PRIMARY KEY,
    list             TEXT NOT NULL,
    root_message_id  TEXT,
    subject_norm     TEXT,
    first_date_epoch INTEGER,
    last_date_epoch  INTEGER,
    message_count    INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_threads_last ON threads(list, last_date_epoch DESC);

-- FTS5 over searchable fields. Content table = messages, so we sync via triggers.
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
    subject, body_text, from_name,
    content='messages', content_rowid='rowid',
    tokenize='porter unicode61'
);

CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
    INSERT INTO messages_fts(rowid, subject, body_text, from_name)
    VALUES (new.rowid, new.subject, new.body_text, new.from_name);
END;
CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
    INSERT INTO messages_fts(messages_fts, rowid, subject, body_text, from_name)
    VALUES('delete', old.rowid, old.subject, old.body_text, old.from_name);
END;
CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
    INSERT INTO messages_fts(messages_fts, rowid, subject, body_text, from_name)
    VALUES('delete', old.rowid, old.subject, old.body_text, old.from_name);
    INSERT INTO messages_fts(rowid, subject, body_text, from_name)
    VALUES (new.rowid, new.subject, new.body_text, new.from_name);
END;

-- Vector index (sqlite-vec). Created lazily by code that knows the dim.
-- See store/vec.py: ensure_vec_table(dim).

CREATE TABLE IF NOT EXISTS ingest_state (
    list         TEXT NOT NULL,
    yyyymm       TEXT NOT NULL,
    etag         TEXT,
    last_modified TEXT,
    fetched_at   TEXT,
    message_count INTEGER,
    PRIMARY KEY (list, yyyymm)
);

CREATE TABLE IF NOT EXISTS commitfest_cache (
    cache_key   TEXT PRIMARY KEY,
    payload     TEXT NOT NULL,
    fetched_at  TEXT NOT NULL
);

-- Cache of git commits scraped from a local postgres/postgres clone. Used
-- by the `commits_for_thread` and `recent_commits` tools. Populated lazily
-- by store/git_log.py:index_recent_commits().
CREATE TABLE IF NOT EXISTS git_commits (
    sha            TEXT PRIMARY KEY,
    author_name    TEXT,
    author_email   TEXT,
    committed_at   TEXT,
    committed_epoch INTEGER,
    subject        TEXT,
    discussion_url TEXT,    -- first Discussion: trailer URL, or NULL
    body           TEXT
);

CREATE INDEX IF NOT EXISTS idx_git_commits_date ON git_commits(committed_epoch DESC);
CREATE INDEX IF NOT EXISTS idx_git_commits_url  ON git_commits(discussion_url);

-- Watchlist: persistent saved searches / followed threads / tracked authors.
-- `kind` = 'thread' (value=thread_id) | 'query' (value=search query)
--        | 'author' (value=email substring).
-- `last_checked_epoch` is updated by watchlist_check() and used to compute
-- "what's new since you last asked".
CREATE TABLE IF NOT EXISTS watchlist (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    label              TEXT NOT NULL,
    kind               TEXT NOT NULL CHECK(kind IN ('thread','query','author')),
    value              TEXT NOT NULL,
    list_name          TEXT DEFAULT 'pgsql-hackers',
    last_checked_epoch INTEGER,
    created_at         TEXT DEFAULT (datetime('now')),
    UNIQUE(kind, value, list_name)
);
