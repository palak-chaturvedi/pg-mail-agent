"""High-level ingestion driver: month-by-month fetch -> parse -> thread -> store."""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Iterable, Optional

import httpx

from ..config import Config
from ..store.repo import MessageRow, Repo
from ..store import vec as vec_mod
from .embedder import Embedder, chunk_text, make_embedder
from .fetcher import fetch_month
from .parser import ParsedMessage, parse_mbox_bytes
from .threader import assign_threads, normalize_subject

log = logging.getLogger(__name__)


def iter_months(start: date, end: date) -> Iterable[str]:
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield f"{y:04d}{m:02d}"
        m += 1
        if m > 12:
            y += 1
            m = 1


def _web_url(cfg: Config, message_id: str) -> str:
    return f"{cfg.archive_base}/message-id/{message_id}"


def _raw_url(cfg: Config, message_id: str) -> str:
    return f"{cfg.archive_base}/message-id/raw/{message_id}"


def ingest_month(
    cfg: Config,
    repo: Repo,
    embedder: Optional[Embedder],
    list_name: str,
    yyyymm: str,
    client: Optional[httpx.Client] = None,
    force: bool = False,
) -> int:
    """Ingest one month of one list. Returns number of messages written."""
    state = repo.get_ingest_state(list_name, yyyymm)
    prev_etag = None if force else (state["etag"] if state else None)
    prev_lm = None if force else (state["last_modified"] if state else None)

    result = fetch_month(cfg, list_name, yyyymm, prev_etag, prev_lm, client=client)
    if result.not_modified:
        log.info("%s/%s: not modified", list_name, yyyymm)
        return 0
    if not result.body:
        log.warning("%s/%s: empty body", list_name, yyyymm)
        return 0

    messages = list(parse_mbox_bytes(result.body))
    if not messages:
        repo.record_ingest(list_name, yyyymm, result.etag, result.last_modified, 0)
        return 0

    threads = assign_threads(messages)

    # Persist messages + thread rollups in one transaction.
    rowids: dict[str, int] = {}
    with repo.transaction():
        for pm in messages:
            ta = threads[pm.message_id]
            row = MessageRow(
                message_id=pm.message_id,
                list=list_name,
                subject=pm.subject,
                from_name=pm.from_name,
                from_email=pm.from_email,
                date_utc=pm.date_utc,
                date_epoch=pm.date_epoch,
                in_reply_to=pm.in_reply_to,
                thread_id=ta.thread_id,
                body_text=pm.body_text,
                web_url=_web_url(cfg, pm.message_id),
                raw_url=_raw_url(cfg, pm.message_id),
            )
            rid = repo.upsert_message(row, pm.references)
            rowids[pm.message_id] = rid

        # Thread rollups (one row per unique thread)
        seen_threads: set[str] = set()
        for ta in threads.values():
            if ta.thread_id in seen_threads:
                continue
            seen_threads.add(ta.thread_id)
            repo.upsert_thread(
                thread_id=ta.thread_id,
                list_name=list_name,
                root_message_id=ta.root_message_id,
                subject_norm=ta.subject_norm,
                first_epoch=ta.first_epoch,
                last_epoch=ta.last_epoch,
                message_count=len(ta.member_ids),
            )
        repo.record_ingest(list_name, yyyymm, result.etag, result.last_modified, len(messages))

    # Embeddings (outside the main txn; large model calls)
    if embedder is not None and embedder.dim > 0:
        conn = repo.connect()
        if vec_mod.ensure_vec_table(conn, embedder.dim):
            _embed_and_store(conn, embedder, messages, rowids)

    log.info("%s/%s: ingested %d messages", list_name, yyyymm, len(messages))
    return len(messages)


def _embed_and_store(
    conn,
    embedder: Embedder,
    messages: list[ParsedMessage],
    rowids: dict[str, int],
) -> None:
    # One embedding per message: subject + first chunk of body. Cheap, good
    # enough for thread-level retrieval. Per-chunk embeddings can be added
    # later by extending messages_vec to a (message_id, chunk_idx) table.
    texts: list[str] = []
    rids: list[int] = []
    for pm in messages:
        chunks = chunk_text(pm.body_text or "")
        head = chunks[0] if chunks else ""
        text = (pm.subject or "").strip() + "\n\n" + head
        texts.append(text)
        rids.append(rowids[pm.message_id])

    BATCH = 32
    for i in range(0, len(texts), BATCH):
        batch_texts = texts[i : i + BATCH]
        batch_rids = rids[i : i + BATCH]
        blobs = embedder.embed(batch_texts)
        with conn:
            for rid, blob in zip(batch_rids, blobs):
                if not blob:
                    continue
                vec_mod.upsert_vector(conn, rid, blob)


def ingest_range(
    cfg: Config,
    list_name: str,
    start: date,
    end: date,
    skip_embeddings: bool = False,
    force: bool = False,
) -> int:
    repo = Repo(cfg)
    repo.init_schema()
    embedder: Optional[Embedder] = None
    if not skip_embeddings and cfg.embedder != "none":
        embedder = make_embedder(cfg.embedder, cfg.embedder_model, cfg.embedding_dim)
    total = 0
    with httpx.Client(timeout=cfg.http_timeout, follow_redirects=True) as client:
        for yyyymm in iter_months(start, end):
            try:
                total += ingest_month(cfg, repo, embedder, list_name, yyyymm, client=client, force=force)
            except httpx.HTTPStatusError as e:
                log.warning("%s/%s: HTTP %s", list_name, yyyymm, e.response.status_code)
            except Exception:
                log.exception("%s/%s: failed", list_name, yyyymm)
    return total
