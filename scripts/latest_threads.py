"""Print the most recent threads from the local pg-mail-mcp SQLite store."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pg_mail_mcp.config import Config
from pg_mail_mcp.server.tools import list_recent_threads
from pg_mail_mcp.store.repo import Repo


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--list", dest="list_name", default="pgsql-hackers")
    p.add_argument("--days", type=int, default=14)
    p.add_argument("--limit", type=int, default=20)
    args = p.parse_args()

    cfg = Config.from_env()
    repo = Repo(cfg)
    r = list_recent_threads(repo, args.list_name, since_days=args.days, limit=args.limit)
    print(f"list={r['list']}  since_days={r['since_days']}  count={r['count']}\n")
    for t in r["threads"]:
        last = datetime.fromtimestamp(t["last_date_epoch"], timezone.utc)
        print(f"{last:%Y-%m-%d %H:%M}Z  msgs={t['message_count']:>3}  {t['subject']}")
        print(f"    thread_id={t['thread_id']}  root={t['root_message_id']}")


if __name__ == "__main__":
    main()
