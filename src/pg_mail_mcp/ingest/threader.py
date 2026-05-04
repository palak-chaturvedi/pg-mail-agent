"""Threading: assign a stable thread_id to each message using In-Reply-To /
References, with a normalized-subject fallback.

Strategy (simplified JWZ):
- Build a union-find over Message-IDs that appear together via In-Reply-To /
  References (each message links to its first reference and its in-reply-to).
- Within each connected component, choose the message with the earliest date
  (or, if absent, the lowest message-id) as the root.
- Thread id = sha1 of root message-id (stable across re-ingests).
- Messages whose subject normalizes identically and arrive within 30 days of
  an existing thread root may also be merged in (subject-fallback).
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Iterable

from .parser import ParsedMessage


_RE_SUBJECT_PREFIX = re.compile(r"^(?:\s*(?:re|fwd?|aw)\s*:\s*)+", re.IGNORECASE)


def normalize_subject(s: str) -> str:
    s = _RE_SUBJECT_PREFIX.sub("", s or "")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


class _UF:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


@dataclass
class ThreadAssignment:
    thread_id: str
    root_message_id: str
    subject_norm: str
    first_epoch: int | None
    last_epoch: int | None
    member_ids: list[str]


def assign_threads(messages: Iterable[ParsedMessage]) -> dict[str, ThreadAssignment]:
    """Return mapping from message_id -> ThreadAssignment."""
    msgs = list(messages)
    by_id = {m.message_id: m for m in msgs}
    uf = _UF()

    # Header-based unions
    for m in msgs:
        uf.find(m.message_id)
        if m.in_reply_to:
            uf.union(m.in_reply_to, m.message_id)
        for ref in m.references:
            uf.union(ref, m.message_id)

    # Group present messages by component root
    components: dict[str, list[ParsedMessage]] = {}
    for m in msgs:
        components.setdefault(uf.find(m.message_id), []).append(m)

    # Subject-fallback: merge components that share a normalized non-empty
    # subject and whose date ranges overlap within 30 days.
    by_subject: dict[str, list[str]] = {}
    comp_meta: dict[str, tuple[int | None, int | None, str]] = {}
    for root, members in components.items():
        members_sorted = sorted(members, key=lambda x: (x.date_epoch or 0, x.message_id))
        first = next((x.date_epoch for x in members_sorted if x.date_epoch), None)
        last = next((x.date_epoch for x in reversed(members_sorted) if x.date_epoch), None)
        # Subject of earliest known message in the component
        subj_norm = normalize_subject(members_sorted[0].subject)
        comp_meta[root] = (first, last, subj_norm)
        if subj_norm:
            by_subject.setdefault(subj_norm, []).append(root)

    THIRTY_DAYS = 30 * 86400
    for subj_norm, roots in by_subject.items():
        if len(roots) < 2:
            continue
        roots_sorted = sorted(roots, key=lambda r: (comp_meta[r][0] or 0))
        for i in range(len(roots_sorted) - 1):
            a, b = roots_sorted[i], roots_sorted[i + 1]
            af, al, _ = comp_meta[a]
            bf, _, _ = comp_meta[b]
            if af is None or bf is None:
                continue
            if bf - (al or af) <= THIRTY_DAYS:
                uf.union(a, b)

    # Re-group after subject merges
    final_components: dict[str, list[ParsedMessage]] = {}
    for m in msgs:
        final_components.setdefault(uf.find(m.message_id), []).append(m)

    assignments: dict[str, ThreadAssignment] = {}
    for _root, members in final_components.items():
        members_sorted = sorted(members, key=lambda x: (x.date_epoch or 0, x.message_id))
        root_msg = members_sorted[0]
        # Prefer a true root: a member whose in_reply_to is empty or points
        # outside the set; otherwise the earliest-dated one.
        member_ids = {m.message_id for m in members}
        candidates = [m for m in members_sorted if not m.in_reply_to or m.in_reply_to not in member_ids]
        if candidates:
            root_msg = min(candidates, key=lambda x: (x.date_epoch or 0, x.message_id))
        thread_id = hashlib.sha1(root_msg.message_id.encode("utf-8")).hexdigest()
        first = next((x.date_epoch for x in members_sorted if x.date_epoch), None)
        last = next((x.date_epoch for x in reversed(members_sorted) if x.date_epoch), None)
        ta = ThreadAssignment(
            thread_id=thread_id,
            root_message_id=root_msg.message_id,
            subject_norm=normalize_subject(root_msg.subject),
            first_epoch=first,
            last_epoch=last,
            member_ids=sorted(member_ids),
        )
        for m in members:
            assignments[m.message_id] = ta
    return assignments
