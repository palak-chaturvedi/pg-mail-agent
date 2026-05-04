"""Download monthly mbox files from postgresql.org with ETag/Last-Modified caching."""
from __future__ import annotations

import gzip
from dataclasses import dataclass
from typing import Optional

import httpx

from ..config import Config


@dataclass
class FetchResult:
    yyyymm: str
    body: Optional[bytes]   # raw mbox bytes; None when not modified
    etag: Optional[str]
    last_modified: Optional[str]
    not_modified: bool


class IngestAuthRequired(RuntimeError):
    """Raised when the archive endpoint redirects to or serves a login page.
    Caller should catch this once per month and surface a clear error rather
    than silently recording 0 messages.
    """


def _mbox_url(cfg: Config, list_name: str, yyyymm: str) -> str:
    # The archive UI exposes monthly mboxes via POST to this path:
    #   /list/<list>/mbox/<list>.<YYYYMM>
    # The form is CSRF-protected by Django's csrf middleware; fetching the
    # parent listing page first to obtain a session + csrftoken cookie.
    return f"{cfg.archive_base}/list/{list_name}/mbox/{list_name}.{yyyymm}"


def _list_page_url(cfg: Config, list_name: str) -> str:
    return f"{cfg.archive_base}/list/{list_name}/"


def fetch_month(
    cfg: Config,
    list_name: str,
    yyyymm: str,
    prev_etag: Optional[str] = None,
    prev_last_modified: Optional[str] = None,
    client: Optional[httpx.Client] = None,
) -> FetchResult:
    headers = {"User-Agent": cfg.user_agent, "Accept-Encoding": "gzip"}
    if prev_etag:
        headers["If-None-Match"] = prev_etag
    if prev_last_modified:
        headers["If-Modified-Since"] = prev_last_modified

    own_client = client is None
    c = client or httpx.Client(timeout=cfg.http_timeout, follow_redirects=True)
    try:
        # Inject cookies from PGMAIL_COOKIE (raw Cookie header) so that the
        # mbox download endpoint accepts the request.
        cookie_header = cfg.cookie.strip() if cfg.cookie else ""
        if cookie_header:
            for part in cookie_header.split(";"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    c.cookies.set(k.strip(), v.strip(), domain=".postgresql.org")
        # 1. GET the list page to refresh CSRF cookie if not already set.
        list_page = _list_page_url(cfg, list_name)
        c.get(list_page, headers={"User-Agent": cfg.user_agent})
        csrf = c.cookies.get("csrftoken") or ""
        post_headers = dict(headers)
        post_headers["Referer"] = list_page
        if csrf:
            post_headers["X-CSRFToken"] = csrf
        # 2. POST to the mbox endpoint.
        url = _mbox_url(cfg, list_name, yyyymm)
        resp = c.post(url, headers=post_headers, data={"csrfmiddlewaretoken": csrf})
        if resp.status_code == 304:
            return FetchResult(yyyymm, None, prev_etag, prev_last_modified, True)
        # Detect that we got bounced to the login page — either via redirect
        # URL or by inspecting the response payload (some flows return 200
        # with the login form HTML rather than a 302).
        final_url = str(resp.url)
        ct = (resp.headers.get("content-type") or "").lower()
        login_markers = ("_auth/accounts/login", "/account/login", "/auth/login")
        if any(m in final_url for m in login_markers):
            raise IngestAuthRequired(
                f"{list_name} {yyyymm}: archive redirected to login "
                f"({final_url}). Refresh PGMAIL_COOKIE from a logged-in browser."
            )
        if resp.status_code == 200 and "text/html" in ct:
            head = resp.content[:4096].lower()
            if (b"<html" in head
                    and (b"login" in head or b"sign in" in head
                         or b"csrfmiddlewaretoken" in head)):
                raise IngestAuthRequired(
                    f"{list_name} {yyyymm}: mbox endpoint returned an HTML "
                    f"login page. Refresh PGMAIL_COOKIE."
                )
        resp.raise_for_status()
        body = resp.content
        if body[:2] == b"\x1f\x8b":
            body = gzip.decompress(body)
        return FetchResult(
            yyyymm,
            body,
            resp.headers.get("ETag"),
            resp.headers.get("Last-Modified"),
            False,
        )
    finally:
        if own_client:
            c.close()
