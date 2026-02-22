from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
from typing import Literal


ACTION_TOKEN_PATTERN = re.compile(r"<!--\s*mergexo-action:([0-9a-f]{64})\s*-->")
FeedbackKind = Literal["review", "issue"]


@dataclass(frozen=True)
class FeedbackEventRecord:
    event_key: str
    pr_number: int
    issue_number: int
    kind: FeedbackKind
    comment_id: int
    updated_at: str


def event_key(*, pr_number: int, kind: FeedbackKind, comment_id: int, updated_at: str) -> str:
    return f"{pr_number}:{kind}:{comment_id}:{updated_at}"


def is_bot_login(login: str) -> bool:
    normalized = login.strip().lower()
    return normalized.endswith("[bot]")


def has_action_token(text: str) -> bool:
    return ACTION_TOKEN_PATTERN.search(text) is not None


def extract_action_tokens(text: str) -> tuple[str, ...]:
    return tuple(match.group(1) for match in ACTION_TOKEN_PATTERN.finditer(text))


def compute_turn_key(*, pr_number: int, head_sha: str, pending_event_keys: tuple[str, ...]) -> str:
    stable_event_keys = sorted(pending_event_keys)
    payload = f"{pr_number}:{head_sha}:{'|'.join(stable_event_keys)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_review_reply_token(*, turn_key: str, review_comment_id: int, body: str) -> str:
    payload = f"{turn_key}:review_reply:{review_comment_id}:{body}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_general_comment_token(*, turn_key: str, body: str) -> str:
    payload = f"{turn_key}:general_comment:{body}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_history_rewrite_token(
    *, pr_number: int, expected_head_sha: str, observed_head_sha: str, reason: str
) -> str:
    payload = f"{pr_number}:history_rewrite:{reason}:{expected_head_sha}:{observed_head_sha}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def append_action_token(*, body: str, token: str) -> str:
    marker = f"<!-- mergexo-action:{token} -->"
    stripped = body.strip()
    if marker in stripped:
        return stripped
    if not stripped:
        return marker
    return f"{stripped}\n\n{marker}"
