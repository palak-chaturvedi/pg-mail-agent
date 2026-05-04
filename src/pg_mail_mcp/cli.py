"""Command-line entry point: `pg-mail-mcp`."""
from __future__ import annotations

import logging
import sys
from datetime import date, datetime
from typing import Optional

import click

from .config import Config
from .ingest.run import ingest_range
from .server.app import serve as serve_mcp


def _parse_month(s: str) -> date:
    if s.lower() == "latest":
        today = date.today()
        return date(today.year, today.month, 1)
    # Accept YYYY-MM or YYYYMM
    s = s.replace("-", "")
    if len(s) != 6:
        raise click.BadParameter("expected YYYY-MM or 'latest'")
    return date(int(s[:4]), int(s[4:6]), 1)


@click.group()
@click.option("--verbose", "-v", is_flag=True)
def main(verbose: bool) -> None:
    """pg-mail-mcp: PostgreSQL mailing-list MCP server."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )


@main.command()
@click.option("--list", "list_name", default="pgsql-hackers", show_default=True)
@click.option("--from", "from_", default=None, help="Start month YYYY-MM (default: 36 months ago)")
@click.option("--to", "to_", default="latest", show_default=True, help="End month YYYY-MM or 'latest'")
@click.option("--no-embeddings", is_flag=True, help="Skip semantic embeddings (FTS only).")
@click.option("--force", is_flag=True, help="Ignore ETag/Last-Modified cache.")
def ingest(list_name: str, from_: Optional[str], to_: str, no_embeddings: bool, force: bool) -> None:
    """Download + index mbox months for a list."""
    cfg = Config.from_env()
    if from_ is None:
        today = date.today()
        # 36 months back
        y = today.year - 3
        start = date(y, today.month, 1)
    else:
        start = _parse_month(from_)
    end = _parse_month(to_)
    total = ingest_range(cfg, list_name, start, end, skip_embeddings=no_embeddings, force=force)
    click.echo(f"ingested {total} messages from {list_name} ({start:%Y-%m}..{end:%Y-%m})")


@main.command()
def serve() -> None:
    """Start the MCP server on stdio."""
    serve_mcp()


@main.command("embed-pending")
@click.option("--batch-size", default=64, show_default=True, type=int)
@click.option("--limit", default=0, show_default=True, type=int,
              help="Max messages to embed in this run (0 = no cap).")
def embed_pending(batch_size: int, limit: int) -> None:
    """Backfill semantic embeddings for messages missing from messages_vec.

    Useful after re-ingesting with PGMAIL_EMBEDDER=none, or after switching
    embedder/model. Requires PGMAIL_EMBEDDER to be set to 'local' or 'openai'.
    """
    from .ingest.embedder import chunk_text, make_embedder
    from .store import vec as vec_mod
    from .store.repo import Repo

    cfg = Config.from_env()
    if cfg.embedder == "none":
        click.echo("PGMAIL_EMBEDDER=none; nothing to do. "
                   "Set PGMAIL_EMBEDDER to 'local' or 'openai'.")
        sys.exit(1)
    repo = Repo(cfg)
    repo.init_schema()
    conn = repo.connect()
    if not vec_mod.ensure_vec_table(conn, cfg.embedding_dim):
        click.echo("sqlite-vec extension is not loadable. Install sqlite-vec "
                   "and ensure your sqlite3 supports load_extension.")
        sys.exit(1)
    # Find rowids missing from messages_vec.
    sql = (
        "SELECT m.rowid AS rid, m.message_id, m.subject, m.body_text "
        "FROM messages m "
        "LEFT JOIN messages_vec v ON v.rowid = m.rowid "
        "WHERE v.rowid IS NULL "
        "ORDER BY m.rowid DESC"
    )
    if limit > 0:
        sql += f" LIMIT {int(limit)}"
    rows = conn.execute(sql).fetchall()
    if not rows:
        click.echo("Nothing to embed; all messages already have vectors.")
        return
    click.echo(f"Embedding {len(rows)} message(s) in batches of {batch_size}...")
    embedder = make_embedder(cfg.embedder, cfg.embedder_model, cfg.embedding_dim)
    done = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        texts: list[str] = []
        rids: list[int] = []
        for r in chunk:
            heads = chunk_text(r["body_text"] or "")
            head = heads[0] if heads else ""
            texts.append((r["subject"] or "").strip() + "\n\n" + head)
            rids.append(int(r["rid"]))
        blobs = embedder.embed(texts)
        with conn:
            for rid, blob in zip(rids, blobs):
                if not blob:
                    continue
                vec_mod.upsert_vector(conn, rid, blob)
        done += len(chunk)
        click.echo(f"  {done}/{len(rows)}")
    click.echo(f"done; embedded {done} message(s)")


@main.command()
def doctor() -> None:
    """Diagnose configuration and dependencies."""
    cfg = Config.from_env()
    click.echo(f"db_path: {cfg.db_path}")
    click.echo(f"lists:   {', '.join(cfg.lists)}")
    click.echo(f"embedder: {cfg.embedder} ({cfg.embedder_model}, dim={cfg.embedding_dim})")
    # sqlite-vec
    try:
        import sqlite3
        from .store import vec as vec_mod
        cfg.ensure_db_dir()
        conn = sqlite3.connect(cfg.db_path)
        ok = vec_mod.load_vec(conn)
        click.echo(f"sqlite-vec loadable: {ok}")
        conn.close()
    except Exception as e:  # noqa: BLE001
        click.echo(f"sqlite-vec check failed: {e}")
    # embedder
    if cfg.embedder == "local":
        try:
            import sentence_transformers  # noqa: F401
            click.echo("sentence-transformers: ok")
        except Exception as e:  # noqa: BLE001
            click.echo(f"sentence-transformers: missing ({e})")


if __name__ == "__main__":
    main()
