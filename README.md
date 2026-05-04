# pg-mail-mcp

Local **MCP server** that exposes the **PostgreSQL mailing list archives** (default: `pgsql-hackers`) as structured tools for AI assistants — so VS Code Copilot Chat (or any MCP client) can search threads, follow patch reviews, correlate Commitfest entries with discussions, and produce briefings instead of guessing.

Backs two custom agents shipped in `.github/agents/`:

- **PG Hackers Researcher** — Q&A and patch-review research.
- **PG Hackers Briefing** — fixed-format weekly roundup of list activity.

## What it does

1. Downloads monthly mbox archives from `https://www.postgresql.org/list-archive-mbox/<list>/<YYYYMM>/` (incremental, ETag-aware).
2. Parses messages and reconstructs threads (JWZ-style: `In-Reply-To` + `References` + subject fallback).
3. Indexes them into a single SQLite database with **FTS5** (keyword) and **sqlite-vec** (semantic embeddings, optional).
4. Optionally correlates with the local PostgreSQL git mirror (commit ↔ thread).
5. Exposes **20 MCP tools** over stdio.

## MCP tools

### Search & retrieval
| Tool | Purpose |
|---|---|
| `search_messages` | Hybrid (FTS + semantic) search with date / author / list filters. |
| `get_message` | Fetch one message by `Message-ID`. |
| `get_thread` | Fetch a full thread tree (accepts thread id or any `Message-ID` in it). |
| `list_recent_threads` | Threads with activity in the last N days. |
| `summarize_thread` | Quote-stripped chronological digest for the LLM. |
| `find_thread_by_url` | Resolve a `postgresql.org/message-id/...` URL to its thread. |
| `export_thread` | Dump a thread as Markdown or JSON for offline use. |

### Activity & ranking
| Tool | Purpose |
|---|---|
| `archive_coverage` | Earliest / latest message dates + last ingest timestamp (freshness banner). |
| `top_threads_by_activity` | Hottest threads in a window, ranked by message count or participants. |
| `top_authors` | Most active contributors in a window. |
| `thread_timeline` | Per-message classification (patch-vN / review / nack / consensus / discussion). |
| `compare_threads` | Side-by-side stats for multiple threads. |

### Commitfest
| Tool | Purpose |
|---|---|
| `find_commitfest_entry` | Search the Commitfest app for matching patches. |
| `get_commitfest_entry` | Fetch a single Commitfest entry (status, authors, reviewers, attachments). |
| `commitfest_for_thread` | Reverse-lookup: what CF entry references this thread? |

### Git correlation _(optional, requires local pg git mirror)_
| Tool | Purpose |
|---|---|
| `commits_for_thread` | Commits whose log message references this thread's URL or message-id. |
| `recent_commits` | Recent commits to master. |

### Watchlist
| Tool | Purpose |
|---|---|
| `watchlist_add` / `watchlist_list` / `watchlist_remove` | Persist threads or authors of interest. |
| `watchlist_check` | Count new messages since you last checked each watch entry. |

## Install

Requires Python 3.11+.

```powershell
git clone git@github.com:palak-chaturvedi/pg-mail-agent.git pg-mail-mcp
cd pg-mail-mcp
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .
```

Optional extras:

- `pip install -e .[local-embed]` → local sentence-transformers embedder (~90 MB model download on first use).
- `pip install -e .[openai]` → OpenAI embeddings.

## Configure VS Code

Copy the template and fill in your cookie:

```powershell
Copy-Item .vscode\mcp.example.json .vscode\mcp.json
```

`.vscode/mcp.json` is **gitignored** because it holds your postgresql.org session cookie. Edit it and set `PGMAIL_COOKIE`.

To get the cookie:
1. Sign in at <https://www.postgresql.org/account/>.
2. DevTools → Application → Cookies → `https://www.postgresql.org`.
3. Copy `sessionid=...; csrftoken=...` (other cookies optional) into `PGMAIL_COOKIE`.

Reopen VS Code; Copilot Chat will discover the `pg-mail` server automatically.

## Initial ingest

```powershell
$env:PGMAIL_COOKIE = "sessionid=...; csrftoken=..."
pg-mail-mcp ingest --from 2025-01 --to latest --no-embeddings
```

The fetcher detects login redirects and raises `IngestAuthRequired` if the cookie is missing or expired.

Backfill semantic embeddings later:

```powershell
$env:PGMAIL_EMBEDDER = "local"     # or "openai"
pg-mail-mcp embed-pending
```

Re-running `ingest` is incremental (ETag/Last-Modified caching).

## CLI

```powershell
pg-mail-mcp serve            # MCP stdio server (used by Copilot Chat)
pg-mail-mcp ingest --from 2025-01 --to latest
pg-mail-mcp embed-pending    # backfill embeddings
pg-mail-mcp doctor           # health check (db, schema, fts, vec, coverage)
```

## Configuration (env vars)

| Variable | Default | Notes |
|---|---|---|
| `PGMAIL_DB_PATH` | `~/.pg-mail-mcp/pgmail.sqlite` | SQLite database location. |
| `PGMAIL_LISTS` | `pgsql-hackers` | Comma-separated list names. |
| `PGMAIL_COOKIE` | _(empty)_ | Cookie header from a logged-in postgresql.org session. Required for ingest. |
| `PGMAIL_EMBEDDER` | `none` | `none` \| `local` \| `openai`. |
| `PGMAIL_EMBEDDER_MODEL` | `sentence-transformers/all-MiniLM-L6-v2` | Model id (when `local`). |
| `PGMAIL_EMBEDDING_DIM` | `384` | Must match the model (e.g. 1536 for OpenAI `text-embedding-3-small`). |
| `PGMAIL_PG_GIT_PATH` | _(empty)_ | Path to a local clone of `git.postgresql.org/postgresql.git` for commit↔thread correlation. |

## Sample prompts

- "What happened on pgsql-hackers this week?"
- "Summarize the *Changing shared_buffers without restart* discussion and its Commitfest status."
- "Find pgsql-hackers threads from the last 6 months about MERGE RETURNING."
- "Top 10 hottest threads in the last 30 days, by participant count."
- "Add the *PG19 release notes* thread to my watchlist and tell me what's new since yesterday."
- "Export the libpq sslnegotiation thread as Markdown."

## Repo layout

```
src/pg_mail_mcp/
  cli.py                 # serve / ingest / embed-pending / doctor
  config.py
  ingest/                # fetcher (auth-aware), parser, threader, embedder, run
  server/                # FastMCP app + tool wrappers + commitfest scraper
  store/                 # SQLite repo, schema, sqlite-vec adapter, git_log scraper
.github/agents/          # researcher + briefing agent prompts
.vscode/mcp.example.json # template MCP config (copy to mcp.json, add cookie)
```

## Security

`.vscode/mcp.json`, `*.sqlite`, `*.log`, `.env`, and `data/` are gitignored. **Never commit your `PGMAIL_COOKIE`** — rotate it (sign out + sign in) if you suspect a leak.

## Out of scope

- Posting to lists / SMTP.
- Real-time IMAP push (ingest is pull-based).
- Attachment / patch file extraction (URLs are surfaced via `get_commitfest_entry`).
