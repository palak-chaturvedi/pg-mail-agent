# pg-mail-mcp

Local MCP server that exposes the **PostgreSQL mailing list archives** (default: `pgsql-hackers`) as structured tools for AI assistants — so VS Code Copilot Chat (or any MCP client) can search, walk threads, and summarize discussions instead of guessing.

## What it does

1. Downloads monthly mbox archives from `https://www.postgresql.org/list-archive-mbox/<list>/<YYYYMM>/` (incremental, ETag-aware).
2. Parses messages, reconstructs threads (JWZ-style: `In-Reply-To` + `References` + subject fallback).
3. Indexes them into a single SQLite database with **FTS5** (keyword) and **sqlite-vec** (semantic embeddings).
4. Exposes 6 MCP tools over stdio.

## MCP tools

| Tool | Purpose |
|---|---|
| `search_messages` | Hybrid (FTS + semantic) search with date / author / list filters. |
| `get_message` | Fetch one message by `Message-ID`. |
| `get_thread` | Fetch a full thread tree (accepts thread id or any `Message-ID` in it). |
| `list_recent_threads` | Threads with activity in the last N days. |
| `find_commitfest_entry` | Search the Commitfest app for matching patches. |
| `summarize_thread` | Returns a quote-stripped, chronological digest for the LLM to summarize. |

## Install

Requires Python 3.11+.

```powershell
cd C:\Users\chaturvedipa\projects\pg-mail-mcp
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .
```

The first run of the local embedder will download the `all-MiniLM-L6-v2` model (~90 MB).

## Initial ingest

The monthly mbox download endpoint at `postgresql.org` requires a logged-in
community account. To ingest, paste a session cookie from your browser:

1. Sign in at <https://www.postgresql.org/account/>.
2. Open DevTools → Application/Storage → Cookies → `https://www.postgresql.org`.
3. Copy the `pgweb_session` (and ideally `csrftoken`) cookie values into a single header string and export it:

```powershell
$env:PGMAIL_COOKIE = "pgweb_session=...; csrftoken=..."
pg-mail-mcp ingest --from 2025-01 --to latest --no-embeddings
```

Without `PGMAIL_COOKIE`, the fetcher errors with `mbox endpoint redirected to login`.

Re-running is incremental thanks to ETag/Last-Modified caching.

## Register in VS Code

The repo includes a working [.vscode/mcp.json](.vscode/mcp.json). Open this folder in VS Code; Copilot Chat will discover the `pg-mail` server. (For a global registration, copy the `servers.pg-mail` block into your user `mcp.json`.)

## Configuration (env vars)

| Variable | Default | Notes |
|---|---|---|
| `PGMAIL_DB_PATH` | `~/.pg-mail-mcp/pgmail.sqlite` | SQLite database location. |
| `PGMAIL_LISTS` | `pgsql-hackers` | Comma-separated list names. |
| `PGMAIL_COOKIE` | _(empty)_ | Cookie header value from a logged-in postgresql.org session. Required for ingest. |
| `PGMAIL_EMBEDDER` | `none` | `none` \| `local` \| `openai`. |
| `PGMAIL_EMBEDDER_MODEL` | `sentence-transformers/all-MiniLM-L6-v2` | Model id (when `local`). |
| `PGMAIL_EMBEDDING_DIM` | `384` | Must match the model. |

For local semantic search: `pip install -e .[local-embed]`, then set `PGMAIL_EMBEDDER=local`.

For OpenAI: `pip install -e .[openai]`, set `OPENAI_API_KEY`, set `PGMAIL_EMBEDDER=openai`, `PGMAIL_EMBEDDER_MODEL=text-embedding-3-small`, `PGMAIL_EMBEDDING_DIM=1536`.

## Diagnose

```powershell
pg-mail-mcp doctor
```

## Sample prompts to try in Copilot Chat

- "Find pgsql-hackers threads from the last 6 months about MERGE RETURNING."
- "Summarize the libpq sslnegotiation thread."
- "What did Tom Lane say in <message-id>?"
- "List recent commitfest entries about logical replication and link the threads."

## Out of scope (today)

- Posting to lists / SMTP.
- Real-time IMAP push (ingest is pull-based).
- Attachment / patch file extraction.
