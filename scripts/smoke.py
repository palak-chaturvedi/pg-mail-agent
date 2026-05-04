"""Smoke-test the in-process tools against the local DB."""
import json
from pg_mail_mcp.config import Config
from pg_mail_mcp.store.repo import Repo
from pg_mail_mcp.server import tools

cfg = Config.from_env()
repo = Repo(cfg)
repo.init_schema()
conn = repo.connect()

print("=== row counts ===")
for table in ("messages", "threads", "message_refs", "ingest_state"):
    n = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    print(f"  {table:15s} {n}")

print("\n=== recent threads (last 30 days) ===")
out = tools.list_recent_threads(repo, "pgsql-hackers", since_days=30, limit=5)
for t in out["threads"]:
    print(f"  [{t['message_count']:3d}] {t['subject'][:80]}")

print("\n=== search: 'merge returning' ===")
out = tools.search_messages(cfg, repo, None, "merge returning", limit=5, mode="fts")
for r in out["results"]:
    print(f"  {r['date']}  {r['from']['name'][:25]:25s} | {(r['subject'] or '')[:70]}")

# Pick first hit and walk the thread
if out["results"]:
    mid = out["results"][0]["message_id"]
    print(f"\n=== get_thread for first hit ({mid[:50]}...) ===")
    th = tools.get_thread(repo, mid, max_messages=5)
    print(f"  thread_id: {th['thread_id']}")
    print(f"  subject:   {th['subject']}")
    print(f"  messages:  {th['message_count']}")
    for m in th["messages"][:3]:
        print(f"    [{m['depth']}] {m['from']['name'][:25]:25s}  {m['date']}")
