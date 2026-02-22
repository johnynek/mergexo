---
issue: 24
priority: 3
touch_paths:
  - src/mergexo/config.py
  - src/mergexo/models.py
  - src/mergexo/github_gateway.py
  - src/mergexo/orchestrator.py
  - mergexo.toml.example
  - README.md
  - tests/test_config.py
  - tests/test_github_gateway.py
  - tests/test_orchestrator.py
  - tests/test_cli.py
depends_on: []
estimated_size: M
generated_at: 2026-02-22T17:52:14Z
---

# Support Authentication Configuration

_Issue: #24 (https://github.com/johnynek/mergexo/issues/24)_

## Summary

Add explicit allowlist-based actor filtering so MergeXO only processes issues and feedback from configured GitHub users, and fully ignores all other users.

## Context
MergeXO currently accepts any labeled issue and any non-bot PR feedback comment. That is acceptable for private repos, but unsafe for public repos where any GitHub account can create labeled issues or leave comments that trigger agent work.

Issue #24 requires an authentication configuration that guarantees we only process events from approved users and 100% ignore everyone else.

## Goals
1. Add a config-driven allowlist of GitHub usernames.
2. Enforce allowlist checks for issue intake (`issues filed`) and feedback intake (`PR review comments` and `PR issue comments`).
3. Ensure unauthorized users cause zero agent, git, or GitHub side effects.
4. Keep behavior deterministic, observable, and easy to test.

## Non-goals
1. Team/org-based authorization via GitHub API.
2. Role-based permissions per flow (`design_doc`, `bugfix`, `small_job`).
3. Supporting wildcard patterns or regex user matching.
4. Adding new webhook ingestion in this issue.

## Proposed Architecture

### 1. Configuration Contract
Add a new required `[auth]` table in `mergexo.toml`:

- `allowed_users = ["alice", "bob"]`

Contract details:
1. `allowed_users` must be a non-empty list of non-empty strings.
2. Usernames are normalized to lowercase and trimmed at load time.
3. Matching is case-insensitive at runtime.
4. Missing `[auth]` table or empty list is a startup `ConfigError` (fail closed).

Data model updates:
1. Add `AuthConfig` to `src/mergexo/config.py`.
2. Extend `AppConfig` with an `auth` field.
3. Add helper API like `AuthConfig.allows(login: str) -> bool`.

### 2. Actor Identity Propagation
To enforce issue-author checks, we need author login in issue models.

Changes:
1. Extend `Issue` in `src/mergexo/models.py` with `author_login: str` (or equivalent field).
2. Populate `author_login` from `user.login` in `GitHubGateway.list_open_issues_with_label`.
3. Populate `author_login` from `user.login` in `GitHubGateway.get_issue`.
4. In dedupe logic (`list_open_issues_with_any_labels`), preserve author login consistently.

If `user.login` is missing or malformed, treat as unauthorized by default.

### 3. Enforcement Points

#### 3.1 Issue intake (new work scheduling)
In `Phase1Orchestrator._enqueue_new_work`:
1. Check `issue.author_login` against `config.auth`.
2. If unauthorized, skip before flow resolution and before `state.mark_running`.
3. Emit a structured skip log reason (for example `reason=unauthorized_issue_author`).

Effect: unauthorized issue authors never reach agent execution, git branch creation, PR creation, or bot comments.

#### 3.2 Direct/implementation safety re-check
Add defensive checks in `_process_issue` and `_process_implementation_candidate` before side effects.

Rationale:
1. Prevent accidental processing if unauthorized data enters from stale state or future code paths.
2. Preserve the "100% ignore" invariant even if scheduling code changes later.

#### 3.3 Feedback intake (comments on PRs/issues)
In `_normalize_review_events` and `_normalize_issue_events`:
1. Reject comments where `comment.user_login` is not allowlisted.
2. Keep existing bot/token filters.
3. Do not ingest unauthorized comments into `feedback_events`.

Effect: unauthorized comments are invisible to feedback-turn construction and cannot trigger agent replies or commits.

#### 3.4 Legacy tracked PR guard
At start of `_process_feedback_turn`, after loading the linked issue:
1. If issue author is no longer authorized, stop feedback processing for that PR.
2. Mark PR state internally as blocked/ignored with explicit reason.
3. Do not post any GitHub comment for unauthorized actors (remain silent/ignore).

This prevents endless rescans of pre-existing tracked PRs when auth policy changes.

### 4. Ignore Semantics (Hard Requirement)
For unauthorized users, MergeXO must perform none of the following:
1. Start agent turns.
2. Create/reset branches.
3. Commit/push code.
4. Open PRs.
5. Post issue comments or review replies.
6. Mark unauthorized feedback events as pending work.

Allowed behavior for unauthorized events:
1. Emit structured logs/metrics.
2. Apply minimal internal state transitions required to stop retry loops for legacy tracked PRs.

### 5. Observability
Add explicit auth-related logs so operators can audit behavior without engaging unauthorized users.

Suggested events:
1. `auth_issue_ignored` with `issue_number`, `author_login`.
2. `auth_feedback_ignored` with `pr_number`, `comment_id`, `kind`, `user_login`.
3. `auth_pr_blocked` with `pr_number`, `issue_number`, `author_login`, `reason`.

## Implementation Plan
1. Add `[auth]` parsing and validation in `src/mergexo/config.py`, plus test coverage in `tests/test_config.py`.
2. Update sample and operator docs in `mergexo.toml.example` and `README.md`.
3. Extend `Issue` model and GitHub parsing in `src/mergexo/models.py` and `src/mergexo/github_gateway.py`, with parser tests in `tests/test_github_gateway.py`.
4. Add orchestrator allowlist enforcement at issue intake and feedback normalization in `src/mergexo/orchestrator.py`.
5. Add orchestrator tests for authorized/unauthorized issue authors and comment authors in `tests/test_orchestrator.py`.
6. Update config construction helpers in tests (notably `tests/test_cli.py`) for the new `AppConfig.auth` field.

## Testing Plan
1. Config parsing accepts valid `[auth].allowed_users` and rejects missing/empty/invalid values.
2. `list_open_issues_with_label` and `get_issue` parse `user.login` into issue author data.
3. Unauthorized labeled issues are skipped during enqueue and produce no PR/comment side effects.
4. Authorized labeled issues still follow existing flow behavior.
5. Unauthorized PR review comments are filtered out and never produce feedback turns.
6. Unauthorized PR issue comments are filtered out and never produce feedback turns.
7. Mixed feedback (authorized + unauthorized) processes only authorized comments.
8. Legacy tracked PR with unauthorized issue author transitions to blocked/ignored state without GitHub writes.

## Acceptance Criteria
1. MergeXO fails to start when `[auth]` is missing or `allowed_users` is empty.
2. A labeled issue opened by a non-allowlisted user is ignored and results in no branch, commit, PR, or bot comment.
3. A labeled issue opened by an allowlisted user is processed normally.
4. PR review comments from non-allowlisted users are ignored and do not trigger agent responses.
5. PR issue comments from non-allowlisted users are ignored and do not trigger agent responses.
6. Unauthorized events are excluded from pending feedback state and cannot be replayed into agent turns.
7. Existing bot/token filters remain intact and compatible with allowlist checks.
8. Logs clearly indicate when and why an event was ignored due to auth policy.

## Risks and Mitigations
1. Risk: misconfigured allowlist blocks all automation.
Mitigation: fail-fast config validation, clear error messages, and updated example config/docs.

2. Risk: username casing differences cause false denies.
Mitigation: normalize both config entries and runtime logins to lowercase.

3. Risk: missing `user.login` fields from API payloads create ambiguous behavior.
Mitigation: treat missing login as unauthorized (fail closed) and log it.

4. Risk: behavior shift for existing installs that had no auth section.
Mitigation: document migration in README and release notes; require explicit allowlist before upgrade rollout.

## Rollout Notes
1. Add `[auth].allowed_users` to `mergexo.toml` in staging first.
2. Run `uv run mergexo run --config mergexo.toml --once --verbose` and verify auth-ignore logs appear as expected.
3. Validate with one allowlisted account and one non-allowlisted account on a test repo.
4. After verification, deploy to production runners.
5. Any allowlist change requires process restart to reload config.
6. Monitor skipped-event logs for one week to catch missing legitimate users, then tighten operations runbook with a standard allowlist update process.
