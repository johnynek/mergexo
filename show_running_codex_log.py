#!/usr/bin/env python3
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re
import sys


KEY_VALUE_RE = re.compile(r"(\w+)=([^\s]+)")
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S,%f"
DEFAULT_LOG_DIR = Path.home() / ".local" / "share" / "mergexo" / "logs"
INVOCATION_KEY_FIELDS = (
    "repo_full_name",
    "issue_number",
    "mode",
    "pr_number",
)


@dataclass(frozen=True)
class InvocationLine:
    timestamp: datetime
    line: str
    fields: dict[str, str]


def _most_recent_log_file(log_dir: Path) -> Path:
    candidates = [path for path in log_dir.iterdir() if path.is_file()]
    if not candidates:
        raise FileNotFoundError(f"No log files found in {log_dir}")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _parse_invocation_line(line: str) -> InvocationLine | None:
    if "event=codex_invocation_" not in line:
        return None
    timestamp_str = line[:23]
    try:
        timestamp = datetime.strptime(timestamp_str, TIMESTAMP_FORMAT)
    except ValueError:
        return None
    fields = dict(KEY_VALUE_RE.findall(line))
    event = fields.get("event")
    if event not in {"codex_invocation_started", "codex_invocation_finished"}:
        return None
    return InvocationLine(timestamp=timestamp, line=line.rstrip("\n"), fields=fields)


def _invocation_key(fields: dict[str, str]) -> tuple[str, ...]:
    return tuple(fields.get(name, "") for name in INVOCATION_KEY_FIELDS)


def _running_starts(lines: list[InvocationLine]) -> list[InvocationLine]:
    starts_by_key: dict[tuple[str, ...], deque[InvocationLine]] = defaultdict(deque)
    for entry in lines:
        event = entry.fields.get("event")
        key = _invocation_key(entry.fields)
        if event == "codex_invocation_started":
            starts_by_key[key].append(entry)
            continue
        queue = starts_by_key.get(key)
        if queue:
            queue.popleft()
    running = [entry for queue in starts_by_key.values() for entry in queue]
    return sorted(running, key=lambda entry: entry.timestamp)


def main() -> int:
    log_dir = DEFAULT_LOG_DIR
    if not log_dir.exists():
        print(f"Log directory does not exist: {log_dir}", file=sys.stderr)
        return 1

    try:
        log_file = _most_recent_log_file(log_dir)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    parsed_lines: list[InvocationLine] = []
    with log_file.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            parsed = _parse_invocation_line(line)
            if parsed is not None:
                parsed_lines.append(parsed)

    running = _running_starts(parsed_lines)
    if not running:
        print(f"No running codex invocations in {log_file}")
        return 0

    for entry in running:
        print(entry.line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
