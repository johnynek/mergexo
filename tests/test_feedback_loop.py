from __future__ import annotations

from mergexo.feedback_loop import (
    append_action_token,
    compute_general_comment_token,
    compute_review_reply_token,
    compute_turn_key,
    event_key,
    extract_action_tokens,
    has_action_token,
    is_bot_login,
)


def test_event_key_and_turn_key_are_stable() -> None:
    key = event_key(pr_number=12, kind="review", comment_id=33, updated_at="2026-02-21T01:02:03Z")
    assert key == "12:review:33:2026-02-21T01:02:03Z"

    turn_a = compute_turn_key(
        pr_number=12,
        head_sha="abc",
        pending_event_keys=("b", "a"),
    )
    turn_b = compute_turn_key(
        pr_number=12,
        head_sha="abc",
        pending_event_keys=("a", "b"),
    )
    assert turn_a == turn_b


def test_action_token_helpers() -> None:
    reply_token = compute_review_reply_token(turn_key="turn", review_comment_id=7, body="Thanks")
    general_token = compute_general_comment_token(turn_key="turn", body="Updated")
    assert reply_token != general_token

    body = append_action_token(body="Done", token=reply_token)
    assert has_action_token(body)
    assert extract_action_tokens(body) == (reply_token,)

    # Appending same token twice should be idempotent.
    body2 = append_action_token(body=body, token=reply_token)
    assert body2 == body

    # Empty body should still emit a valid marker.
    marker_only = append_action_token(body="   ", token=general_token)
    assert marker_only == f"<!-- mergexo-action:{general_token} -->"


def test_bot_detection() -> None:
    assert is_bot_login("github-actions[bot]") is True
    assert is_bot_login("reviewer") is False
