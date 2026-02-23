from __future__ import annotations

from mergexo.feedback_loop import (
    ParsedOperatorCommand,
    append_action_token,
    compute_general_comment_token,
    compute_history_rewrite_token,
    compute_operator_command_token,
    compute_review_reply_token,
    compute_source_issue_redirect_token,
    compute_turn_key,
    event_key,
    extract_action_tokens,
    has_action_token,
    is_bot_login,
    operator_command_key,
    operator_commands_help,
    parse_operator_command,
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


def test_history_rewrite_token_is_stable_and_specific() -> None:
    token_a = compute_history_rewrite_token(
        pr_number=12,
        expected_head_sha="abc",
        observed_head_sha="def",
        reason="cross_cycle:ahead",
    )
    token_b = compute_history_rewrite_token(
        pr_number=12,
        expected_head_sha="abc",
        observed_head_sha="def",
        reason="cross_cycle:ahead",
    )
    token_c = compute_history_rewrite_token(
        pr_number=12,
        expected_head_sha="abc",
        observed_head_sha="xyz",
        reason="cross_cycle:ahead",
    )
    assert token_a == token_b
    assert token_a != token_c


def test_bot_detection() -> None:
    assert is_bot_login("github-actions[bot]") is True
    assert is_bot_login("reviewer") is False


def test_operator_command_key_and_token_are_stable() -> None:
    key = operator_command_key(issue_number=44, comment_id=101, updated_at="2026-02-22T08:30:00Z")
    assert key == "44:101:2026-02-22T08:30:00Z"

    token_a = compute_operator_command_token(command_key=key)
    token_b = compute_operator_command_token(command_key=key)
    assert token_a == token_b


def test_source_issue_redirect_token_is_stable() -> None:
    token_a = compute_source_issue_redirect_token(issue_number=44, pr_number=101, comment_id=77)
    token_b = compute_source_issue_redirect_token(issue_number=44, pr_number=101, comment_id=77)
    token_c = compute_source_issue_redirect_token(issue_number=44, pr_number=101, comment_id=78)
    assert token_a == token_b
    assert token_a != token_c


def test_parse_operator_command_valid_variants() -> None:
    parsed_help = parse_operator_command("/mergexo help")
    assert isinstance(parsed_help, ParsedOperatorCommand)
    assert parsed_help.command == "help"
    assert parsed_help.parse_error is None
    assert parsed_help.normalized_command == "/mergexo help"

    parsed_unblock = parse_operator_command("please run:\n/mergexo unblock head_sha=ABC1234")
    assert isinstance(parsed_unblock, ParsedOperatorCommand)
    assert parsed_unblock.command == "unblock"
    assert parsed_unblock.get_arg("head_sha") == "abc1234"
    assert parsed_unblock.normalized_command == "/mergexo unblock head_sha=abc1234"

    parsed_cross_pr = parse_operator_command("/mergexo unblock pr=7 head_sha=abc1234")
    assert isinstance(parsed_cross_pr, ParsedOperatorCommand)
    assert parsed_cross_pr.command == "unblock"
    assert parsed_cross_pr.get_arg("pr") == "7"
    assert parsed_cross_pr.normalized_command == "/mergexo unblock pr=7 head_sha=abc1234"

    parsed_restart = parse_operator_command("/mergexo restart mode=PYPI")
    assert isinstance(parsed_restart, ParsedOperatorCommand)
    assert parsed_restart.command == "restart"
    assert parsed_restart.get_arg("mode") == "pypi"
    assert parsed_restart.normalized_command == "/mergexo restart mode=pypi"


def test_parse_operator_command_errors_and_non_commands() -> None:
    assert parse_operator_command("regular comment") is None

    missing_subcommand = parse_operator_command("/mergexo")
    assert isinstance(missing_subcommand, ParsedOperatorCommand)
    assert missing_subcommand.command == "invalid"
    assert missing_subcommand.parse_error == "Missing subcommand."

    unknown = parse_operator_command("/mergexo unknown")
    assert isinstance(unknown, ParsedOperatorCommand)
    assert unknown.command == "invalid"
    assert "Unknown subcommand" in (unknown.parse_error or "")

    bad_pr = parse_operator_command("/mergexo unblock pr=-1")
    assert isinstance(bad_pr, ParsedOperatorCommand)
    assert bad_pr.command == "invalid"
    assert "positive integer" in (bad_pr.parse_error or "")

    bad_head_sha = parse_operator_command("/mergexo unblock head_sha=bad-sha")
    assert isinstance(bad_head_sha, ParsedOperatorCommand)
    assert bad_head_sha.command == "invalid"
    assert "7-64 character hex" in (bad_head_sha.parse_error or "")

    bad_restart_mode = parse_operator_command("/mergexo restart mode=rolling")
    assert isinstance(bad_restart_mode, ParsedOperatorCommand)
    assert bad_restart_mode.command == "invalid"
    assert "mode must be one of" in (bad_restart_mode.parse_error or "")

    missing_equals = parse_operator_command("/mergexo restart mode")
    assert isinstance(missing_equals, ParsedOperatorCommand)
    assert "Expected key=value" in (missing_equals.parse_error or "")

    empty_value = parse_operator_command("/mergexo restart mode=")
    assert isinstance(empty_value, ParsedOperatorCommand)
    assert "Expected key=value" in (empty_value.parse_error or "")

    duplicate_arg = parse_operator_command("/mergexo restart mode=pypi mode=git_checkout")
    assert isinstance(duplicate_arg, ParsedOperatorCommand)
    assert "Duplicate argument" in (duplicate_arg.parse_error or "")

    help_with_arg = parse_operator_command("/mergexo help mode=pypi")
    assert isinstance(help_with_arg, ParsedOperatorCommand)
    assert "does not accept arguments" in (help_with_arg.parse_error or "")

    unknown_restart_arg = parse_operator_command("/mergexo restart foo=bar")
    assert isinstance(unknown_restart_arg, ParsedOperatorCommand)
    assert "Unknown restart arguments" in (unknown_restart_arg.parse_error or "")

    unknown_unblock_arg = parse_operator_command("/mergexo unblock foo=bar")
    assert isinstance(unknown_unblock_arg, ParsedOperatorCommand)
    assert "Unknown unblock arguments" in (unknown_unblock_arg.parse_error or "")


def test_operator_command_help_text_contains_anchor() -> None:
    text = operator_commands_help()
    assert "/mergexo unblock" in text
    assert "README.md#github-operator-commands" in text
