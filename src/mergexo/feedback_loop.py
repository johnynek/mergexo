from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
from typing import Literal


ACTION_TOKEN_PATTERN = re.compile(r"<!--\s*mergexo-action:([0-9a-f]{64})\s*-->")
_OPERATOR_COMMAND_PATTERN = re.compile(r"(?mi)^\s*/mergexo(?:\s+(.+))?\s*$")
_HEAD_SHA_PATTERN = re.compile(r"[0-9a-fA-F]{7,64}")
FeedbackKind = Literal["review", "issue", "actions"]
OperatorCommandName = Literal["unblock", "restart", "help", "invalid"]


@dataclass(frozen=True)
class FeedbackEventRecord:
    event_key: str
    pr_number: int
    issue_number: int
    kind: FeedbackKind
    comment_id: int
    updated_at: str


@dataclass(frozen=True)
class ParsedOperatorCommand:
    command: OperatorCommandName
    normalized_command: str
    args: tuple[tuple[str, str], ...]
    parse_error: str | None

    def get_arg(self, key: str) -> str | None:
        for arg_key, arg_value in self.args:
            if arg_key == key:
                return arg_value
        return None


def event_key(*, pr_number: int, kind: FeedbackKind, comment_id: int, updated_at: str) -> str:
    return f"{pr_number}:{kind}:{comment_id}:{updated_at}"


def operator_command_key(*, issue_number: int, comment_id: int, updated_at: str) -> str:
    return f"{issue_number}:{comment_id}:{updated_at}"


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


def compute_operator_command_token(*, command_key: str) -> str:
    payload = f"operator_command:{command_key}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_source_issue_redirect_token(
    *, issue_number: int, pr_number: int, comment_id: int
) -> str:
    payload = f"source_issue_redirect:{issue_number}:{pr_number}:{comment_id}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_pre_pr_checkpoint_token(*, issue_number: int, checkpoint_sha: str) -> str:
    payload = f"pre_pr_checkpoint:{issue_number}:{checkpoint_sha}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def append_action_token(*, body: str, token: str) -> str:
    marker = f"<!-- mergexo-action:{token} -->"
    stripped = body.strip()
    if marker in stripped:
        return stripped
    if not stripped:
        return marker
    return f"{stripped}\n\n{marker}"


def parse_operator_command(text: str) -> ParsedOperatorCommand | None:
    match = _OPERATOR_COMMAND_PATTERN.search(text)
    if match is None:
        return None

    remainder = (match.group(1) or "").strip()
    if not remainder:
        return ParsedOperatorCommand(
            command="invalid",
            normalized_command="/mergexo",
            args=(),
            parse_error="Missing subcommand.",
        )

    tokens = remainder.split()
    command_token = tokens[0].strip().lower()
    arg_tokens = tokens[1:]

    parsed_args: dict[str, str] = {}
    for raw_arg in arg_tokens:
        if "=" not in raw_arg:
            return ParsedOperatorCommand(
                command="invalid",
                normalized_command=f"/mergexo {remainder}",
                args=tuple(sorted(parsed_args.items())),
                parse_error=f"Invalid argument format: {raw_arg!r}. Expected key=value.",
            )
        key_raw, value_raw = raw_arg.split("=", 1)
        key = key_raw.strip().lower()
        value = value_raw.strip()
        if not key or not value:
            return ParsedOperatorCommand(
                command="invalid",
                normalized_command=f"/mergexo {remainder}",
                args=tuple(sorted(parsed_args.items())),
                parse_error=f"Invalid argument format: {raw_arg!r}. Expected key=value.",
            )
        if key in parsed_args:
            return ParsedOperatorCommand(
                command="invalid",
                normalized_command=f"/mergexo {remainder}",
                args=tuple(sorted(parsed_args.items())),
                parse_error=f"Duplicate argument: {key}.",
            )
        parsed_args[key] = value

    if command_token == "help":
        if parsed_args:
            return ParsedOperatorCommand(
                command="invalid",
                normalized_command="/mergexo help",
                args=tuple(sorted(parsed_args.items())),
                parse_error="/mergexo help does not accept arguments.",
            )
        return ParsedOperatorCommand(
            command="help",
            normalized_command="/mergexo help",
            args=(),
            parse_error=None,
        )

    if command_token == "restart":
        allowed_keys = {"mode"}
        unknown_keys = sorted(key for key in parsed_args if key not in allowed_keys)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            return ParsedOperatorCommand(
                command="invalid",
                normalized_command=f"/mergexo restart {' '.join(arg_tokens)}".strip(),
                args=tuple(sorted(parsed_args.items())),
                parse_error=f"Unknown restart arguments: {unknown}.",
            )
        if "mode" in parsed_args:
            mode = parsed_args["mode"].strip().lower()
            if mode not in {"git_checkout", "pypi"}:
                return ParsedOperatorCommand(
                    command="invalid",
                    normalized_command=f"/mergexo restart mode={parsed_args['mode']}",
                    args=tuple(sorted(parsed_args.items())),
                    parse_error="mode must be one of: git_checkout, pypi.",
                )
            parsed_args["mode"] = mode
        normalized_args: list[str] = []
        if "mode" in parsed_args:
            normalized_args.append(f"mode={parsed_args['mode']}")
        normalized = "/mergexo restart"
        if normalized_args:
            normalized += " " + " ".join(normalized_args)
        return ParsedOperatorCommand(
            command="restart",
            normalized_command=normalized,
            args=tuple((key, parsed_args[key]) for key in ("mode",) if key in parsed_args),
            parse_error=None,
        )

    if command_token == "unblock":
        allowed_keys = {"pr", "head_sha"}
        unknown_keys = sorted(key for key in parsed_args if key not in allowed_keys)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            return ParsedOperatorCommand(
                command="invalid",
                normalized_command=f"/mergexo unblock {' '.join(arg_tokens)}".strip(),
                args=tuple(sorted(parsed_args.items())),
                parse_error=f"Unknown unblock arguments: {unknown}.",
            )
        if "pr" in parsed_args:
            pr_raw = parsed_args["pr"]
            if not pr_raw.isdigit() or int(pr_raw) < 1:
                return ParsedOperatorCommand(
                    command="invalid",
                    normalized_command=f"/mergexo unblock pr={pr_raw}",
                    args=tuple(sorted(parsed_args.items())),
                    parse_error="pr must be a positive integer.",
                )
            parsed_args["pr"] = str(int(pr_raw))
        if "head_sha" in parsed_args:
            head_sha = parsed_args["head_sha"].strip()
            if _HEAD_SHA_PATTERN.fullmatch(head_sha) is None:
                return ParsedOperatorCommand(
                    command="invalid",
                    normalized_command=f"/mergexo unblock head_sha={head_sha}",
                    args=tuple(sorted(parsed_args.items())),
                    parse_error="head_sha must be a 7-64 character hex git SHA.",
                )
            parsed_args["head_sha"] = head_sha.lower()
        normalized_parts: list[str] = []
        if "pr" in parsed_args:
            normalized_parts.append(f"pr={parsed_args['pr']}")
        if "head_sha" in parsed_args:
            normalized_parts.append(f"head_sha={parsed_args['head_sha']}")
        normalized = "/mergexo unblock"
        if normalized_parts:
            normalized += " " + " ".join(normalized_parts)
        return ParsedOperatorCommand(
            command="unblock",
            normalized_command=normalized,
            args=tuple((key, parsed_args[key]) for key in ("pr", "head_sha") if key in parsed_args),
            parse_error=None,
        )

    return ParsedOperatorCommand(
        command="invalid",
        normalized_command=f"/mergexo {remainder}",
        args=tuple(sorted(parsed_args.items())),
        parse_error=f"Unknown subcommand: {command_token}.",
    )


def operator_commands_help(readme_anchor: str = "README.md#github-operator-commands") -> str:
    return (
        "Supported commands:\n"
        "- `/mergexo unblock`\n"
        "- `/mergexo unblock head_sha=<sha>`\n"
        "- `/mergexo unblock pr=<number> [head_sha=<sha>]`\n"
        "- `/mergexo restart`\n"
        "- `/mergexo restart mode=git_checkout|pypi`\n"
        "- `/mergexo help`\n\n"
        f"See {readme_anchor} for details."
    )
