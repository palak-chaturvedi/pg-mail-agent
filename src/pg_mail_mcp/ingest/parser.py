"""Parse mbox bytes into normalized message records."""
from __future__ import annotations

import email
import email.policy
import mailbox
import re
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.header import decode_header, make_header
from email.utils import getaddresses, parsedate_to_datetime
from io import BytesIO
from pathlib import Path
from typing import Iterator, Optional

from bs4 import BeautifulSoup


@dataclass
class ParsedMessage:
    message_id: str
    subject: str
    from_name: str
    from_email: str
    date_utc: Optional[str]
    date_epoch: Optional[int]
    in_reply_to: Optional[str]
    references: list[str] = field(default_factory=list)
    body_text: str = ""


_MSGID_RE = re.compile(r"<([^<>\s]+)>")


def _decode(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return value


def _extract_msgids(value: Optional[str]) -> list[str]:
    if not value:
        return []
    return _MSGID_RE.findall(value)


def _first_msgid(value: Optional[str]) -> Optional[str]:
    ids = _extract_msgids(value)
    return ids[0] if ids else None


def _from_parts(value: Optional[str]) -> tuple[str, str]:
    if not value:
        return "", ""
    addrs = getaddresses([value])
    if not addrs:
        return "", ""
    name, addr = addrs[0]
    return _decode(name), addr or ""


def _parse_date(value: Optional[str]) -> tuple[Optional[str], Optional[int]]:
    if not value:
        return None, None
    try:
        dt = parsedate_to_datetime(value)
    except Exception:
        return None, None
    if dt is None:
        return None, None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.isoformat(), int(dt_utc.timestamp())


def _extract_text(msg: email.message.Message) -> str:
    """Best-effort plain-text body extraction. Strips HTML when needed."""
    text_parts: list[str] = []
    html_parts: list[str] = []
    for part in msg.walk():
        if part.is_multipart():
            continue
        ctype = part.get_content_type()
        if ctype == "text/plain":
            try:
                payload = part.get_content()
            except (LookupError, KeyError, UnicodeDecodeError):
                payload = (part.get_payload(decode=True) or b"").decode(
                    part.get_content_charset() or "utf-8", errors="replace"
                )
            text_parts.append(payload)
        elif ctype == "text/html":
            try:
                payload = part.get_content()
            except (LookupError, KeyError, UnicodeDecodeError):
                payload = (part.get_payload(decode=True) or b"").decode(
                    part.get_content_charset() or "utf-8", errors="replace"
                )
            html_parts.append(payload)
    if text_parts:
        return "\n\n".join(p.strip() for p in text_parts if p)
    if html_parts:
        soup = BeautifulSoup("\n".join(html_parts), "lxml")
        return soup.get_text("\n").strip()
    return ""


def parse_mbox_bytes(data: bytes) -> Iterator[ParsedMessage]:
    """Parse an mbox blob into ParsedMessage records.

    `mailbox.mbox` requires a file path, so we use a temp file. Memory-mapping
    avoids loading bodies twice.
    """
    with tempfile.NamedTemporaryFile(delete=False, suffix=".mbox") as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    try:
        box = mailbox.mbox(str(tmp_path), factory=lambda f: email.message_from_binary_file(f, policy=email.policy.default))
        try:
            for msg in box:
                pm = _to_parsed(msg)
                if pm is not None:
                    yield pm
        finally:
            box.close()
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass


def _to_parsed(msg: email.message.Message) -> Optional[ParsedMessage]:
    msgid = _first_msgid(msg.get("Message-ID") or msg.get("Message-Id"))
    if not msgid:
        return None
    subject = _decode(msg.get("Subject"))
    from_name, from_email = _from_parts(msg.get("From"))
    date_iso, date_epoch = _parse_date(msg.get("Date"))
    in_reply = _first_msgid(msg.get("In-Reply-To"))
    refs = _extract_msgids(msg.get("References"))
    body = _extract_text(msg)
    return ParsedMessage(
        message_id=msgid,
        subject=subject,
        from_name=from_name,
        from_email=from_email,
        date_utc=date_iso,
        date_epoch=date_epoch,
        in_reply_to=in_reply,
        references=refs,
        body_text=body,
    )
