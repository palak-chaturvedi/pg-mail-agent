"""Exercise every MCP tool with realistic prompts and show the output."""
import json
import textwrap

from pg_mail_mcp.config import Config
from pg_mail_mcp.store.repo import Repo
from pg_mail_mcp.server import tools, commitfest

cfg = Config.from_env()
repo = Repo(cfg)
repo.init_schema()


def banner(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def show(payload, max_chars=2000):
    s = json.dumps(payload, indent=2, default=str)
    if len(s) > max_chars:
        s = s[:max_chars] + f"\n... [{len(s) - max_chars} more chars truncated]"
    print(s)


# ---------------------------------------------------------------- 1
banner("PROMPT 1: 'Find pgsql-hackers threads about MERGE RETURNING in April 2026'")
res = tools.search_messages(
    cfg, repo, embedder=None,
    query="MERGE RETURNING",
    list_name="pgsql-hackers",
    date_from="2026-04-01",
    date_to="2026-04-30",
    limit=5,
    mode="fts",
)
print(f"hits: {res['count']}")
for r in res["results"]:
    print(f"  - {r['date']}  {r['from']['name'][:22]:22s}  {(r['subject'] or '')[:75]}")
    print(f"      {r['url']}")

# ---------------------------------------------------------------- 2
banner("PROMPT 2: 'Show the most active recent thread (last 30 days)'")
recent = tools.list_recent_threads(repo, "pgsql-hackers", since_days=30, limit=3)
top = max(recent["threads"], key=lambda t: t["message_count"])
print(f"top thread: {top['subject']!r} ({top['message_count']} messages)")
print(f"  thread_id: {top['thread_id']}")

th = tools.get_thread(repo, top["thread_id"], max_messages=8)
print(f"\nfirst {min(8, th['message_count'])} messages of {th['message_count']}:")
for m in th["messages"][:8]:
    indent = "  " * (m["depth"] + 1)
    print(f"{indent}[{m['depth']}] {m['from']['name'][:25]:25s}  {m['date']}")
print(f"\nparticipants ({len(th['participants'])} unique):")
for p in th["participants"][:6]:
    print(f"  - {p['name']} <{p['email']}>")

# ---------------------------------------------------------------- 3
banner("PROMPT 3: 'Get one specific message in full'")
mid = top["root_message_id"]
msg = tools.get_message(repo, mid, include_body=True)
print(f"subject: {msg['subject']}")
print(f"from:    {msg['from']['name']} <{msg['from']['email']}>")
print(f"date:    {msg['date']}")
print(f"url:     {msg['url']}")
print("body (first 600 chars):")
print(textwrap.indent((msg["body"] or "")[:600], "  | "))

# ---------------------------------------------------------------- 4
banner("PROMPT 4: 'Summarize a thread' (returns quote-stripped digest for an LLM)")
dig = tools.summarize_thread(repo, top["thread_id"], max_messages=5)
print(f"thread: {dig['subject']!r}  ({dig['message_count']} msgs total)")
print("digest entries (quote-stripped):")
for d in dig["digest"][:3]:
    body = (d["body"] or "").strip()
    snippet = body[:300].replace("\n", " ")
    print(f"\n  [depth {d['depth']}] {d['from']['name']}  ({d['date']})")
    print(f"  {snippet}{'...' if len(body) > 300 else ''}")

# ---------------------------------------------------------------- 5
banner("PROMPT 5: 'Find commitfest entries for logical replication'")
cf = commitfest.search_commitfest(cfg, repo, "logical replication", limit=5)
print(f"search_url: {cf.get('search_url')}")
print(f"results: {len(cf.get('results', []))}")
for r in (cf.get("results") or [])[:5]:
    print(f"  - {r['title'][:70]}")
    print(f"      {r['url']}")

# ---------------------------------------------------------------- 6
banner("PROMPT 6: 'Search by author'")
res = tools.search_messages(
    cfg, repo, embedder=None,
    query="patch",
    author="Tom Lane",
    limit=5,
    mode="fts",
)
print(f"hits by Tom Lane mentioning 'patch': {res['count']}")
for r in res["results"]:
    print(f"  - {r['date']}  {(r['subject'] or '')[:78]}")
