---
issue: 72
priority: 3
touch_paths:
  - docs/design/72-design-bound-polling-fetch-cost-over-time-incremental-github-fetching.md
  - src/mergexo/github_gateway.py
  - src/mergexo/state.py
  - src/mergexo/orchestrator.py
  - src/mergexo/feedback_loop.py
  - src/mergexo/config.py
  - src/mergexo/cli.py
  - mergexo.toml.example
  - README.md
  - tests/test_github_gateway.py
  - tests/test_state.py
  - tests/test_orchestrator.py
  - tests/test_config.py
depends_on: []
estimated_size: M
generated_at: 2026-02-24T19:39:09Z
---

# Design: bound polling fetch cost over time (incremental GitHub fetching)

_Issue: #72 (https://github.com/johnynek/mergexo/issues/72)_

## Summary

Make all GitHub comment polling incremental and cursor-driven so steady-state work scales with active monitored objects plus new or edited comments, not with lifetime comment history. The design covers feedback polling, source-issue routing, operator-command scans, and action-token reconciliation.

## Goals

1. Guarantee bounded polling growth over time.
2. Preserve feedback correctness: no missed actionable comments, no duplicate processing.
3. Preserve idempotent posting behavior for tokenized MergeXO comments.
4. Keep current product semantics: closing an active PR is still the stop signal.
5. Provide migration, rollout, observability, and tests sufficient for safe implementation.

## Non-goals

1. Webhook migration in this issue.
2. Replacing deterministic event-key dedupe with a new feedback identity model.
3. Redesigning Git history rewrite safeguards.
4. Reprocessing old historical comments after a PR has been explicitly stopped.

## Runtime mode compatibility (console + service + top)

Main now supports `mergexo console`, which runs service polling and the observability TUI together in one process (`run_service` on a background thread plus TUI in the foreground), while `mergexo service` remains the supervisor-oriented non-interactive mode.

Design implications for issue #72:

1. Incremental GitHub polling remains owned by orchestrator/service paths only.
2. TUI (`top` or console-integrated) remains sqlite read-only and must not trigger GitHub fetch paths.
3. `console` mode should produce the same bounded fetch profile as `service` mode for the same tracked workload.
4. No algorithm change is needed for the incremental cursor model; rollout and tests must validate both runtime modes.

## Target invariants

1. Polling complexity is bounded: for each poll cycle, fetch work is O(active_surfaces + new_or_updated_comments_since_last_poll).
2. For monitored PRs in `awaiting_feedback`, every qualifying review/issue comment create or edit is ingested at least once into `feedback_events`.
3. Event processing remains exactly-once at effect level via existing `event_key` dedupe and `processed_at` marking.
4. Same-second updates are not lost at cursor boundaries.
5. Cursor advancement and event ingestion are atomic within a single sqlite transaction.
6. Action-token checks do not require repeated full-thread scans.
7. Merged and closed PRs are terminal by default and do not continue polling.

## Proposed architecture

### 1. Cursor model

Add a new state table `github_comment_poll_cursors`:

1. `repo_full_name TEXT NOT NULL`
2. `surface TEXT NOT NULL`
3. `scope_number INTEGER NOT NULL`
4. `last_updated_at TEXT NOT NULL`
5. `last_comment_id INTEGER NOT NULL`
6. `bootstrap_complete INTEGER NOT NULL DEFAULT 0`
7. `updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))`
8. Primary key `(repo_full_name, surface, scope_number)`

Surface values:

1. `pr_review_comments`
2. `pr_issue_comments`
3. `issue_pre_pr_followups`
4. `issue_post_pr_redirects`
5. `issue_operator_commands`

Cursor semantics:

1. Watermark is tuple `(last_updated_at, last_comment_id)`.
2. Tuple comparison is lexicographic on normalized UTC timestamps plus numeric `comment_id` tie-break.
3. `issue_comment_cursors` stays in place for semantic pre/post id routing; new table is for incremental fetch bounds.

### 2. Action-token index

Add a new table `action_tokens`:

1. `repo_full_name TEXT NOT NULL`
2. `token TEXT NOT NULL`
3. `scope_kind TEXT NOT NULL` with values `pr` or `issue`
4. `scope_number INTEGER NOT NULL`
5. `source TEXT NOT NULL`
6. `status TEXT NOT NULL` with values `planned`, `posted`, `observed`, `failed`
7. `attempt_count INTEGER NOT NULL DEFAULT 0`
8. `observed_comment_id INTEGER NULL`
9. `observed_updated_at TEXT NULL`
10. `created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))`
11. `updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))`
12. Primary key `(repo_full_name, token)`
13. Index `(repo_full_name, scope_kind, scope_number, status)`

Purpose:

1. Idempotent local source of truth for outbound tokenized comments.
2. Replaces repeated full-thread token-existence scans with indexed lookup.
3. Marks tokens as `observed` when incremental polling sees them in remote comments.

### 3. StateStore API additions

Add explicit cursor and batch APIs:

1. `get_poll_cursor(surface, scope_number, repo_full_name)`
2. `upsert_poll_cursor(surface, scope_number, last_updated_at, last_comment_id, bootstrap_complete, repo_full_name)`
3. `ingest_feedback_scan_batch(...)` that inserts feedback events, observes tokens, and advances both PR cursors in one transaction.
4. `record_action_token_planned(...)`, `record_action_token_posted(...)`, `record_action_token_observed(...)`, `get_action_token(...)`.
5. `seed_feedback_cursors_from_full_scan(...)` for migration/bootstrap.

Atomicity rule:

1. Cursor advance for feedback surfaces happens only in the same transaction that ingests corresponding events and token observations.

## Fetch algorithm

### 1. Common incremental query strategy

For each surface/scope:

1. Load cursor `(u, id)`.
2. Compute `since = u - overlap_seconds` where overlap default is 5 seconds and floor is epoch.
3. Call GitHub endpoint with `per_page=100`, `since`, pagination loop.
4. Continue pages until page size < 100.
5. Parse comments and sort by `(updated_at, comment_id)` before local filtering.
6. Keep items where `(updated_at, comment_id) > (u, id)`.
7. Process qualifying items per surface rules.
8. Advance cursor to max tuple seen in fetched page set within the same transaction as persistence.

Why overlap is required:

1. GitHub `since` filtering is timestamp-based; same-second writes at boundaries can be missed without replay.
2. Overlap plus tuple filtering prevents skips while keeping replay bounded.

### 2. Endpoint-specific details

Feedback review comments:

1. Endpoint `/repos/{owner}/{repo}/pulls/{pr}/comments`.
2. Query params: `per_page=100`, `since`, `page`.
3. Output feeds `_normalize_review_events`.

Feedback issue comments:

1. Endpoint `/repos/{owner}/{repo}/issues/{pr}/comments`.
2. Query params: `per_page=100`, `since`, `page`.
3. Output feeds `_normalize_issue_events`.

Pre-PR followup source-issue comments:

1. Endpoint `/repos/{owner}/{repo}/issues/{issue}/comments` with incremental cursor on `issue_pre_pr_followups`.
2. Business filtering still gates by `comment_id > pre_pr_last_consumed_comment_id`.

Post-PR redirect source-issue comments:

1. Same issue-comment endpoint with `issue_post_pr_redirects` cursor.
2. Business filtering still gates by `comment_id > post_pr_last_redirected_comment_id`.

Operator command scans:

1. Same issue-comment endpoint with `issue_operator_commands` cursor.
2. Parse only newly fetched comments for `/mergexo ...` commands.
3. Keep one-reply-per-command by `command_key` plus tokenized reply dedupe from `action_tokens`.

### 3. Boundary conditions

Same-second updates:

1. Tuple comparator uses `comment_id` tie-break.
2. Overlap replay window guarantees re-read around boundary.

Clock skew:

1. Cursor progression uses GitHub-provided `updated_at` only.
2. Local wall clock is used only to compute overlap floor and metrics.

Out-of-order payload arrival:

1. Always sort fetched items by tuple before filtering and persistence.

Comment edits:

1. Existing event key already includes `updated_at`, so edits emit new event keys.
2. Incremental polling catches edits via `since` and tuple comparison.

Deleted comments:

1. Deletions are not directly listed by these endpoints.
2. Behavior remains: deleted comments are treated as non-actionable once absent from fetched payload.

Force-push/rebase:

1. No change to existing head transition checks and history rewrite blocking.
2. Incremental comment fetch is orthogonal to commit graph validation.

### 4. Restart, outage, and corruption behavior

Outage recovery:

1. After downtime, first poll fetches changes since persisted cursor.
2. Cost scales with downtime changes, not total history.

Bootstrap for existing tracked PRs during migration:

1. Run one-time paginated full scan for each currently tracked `awaiting_feedback` PR.
2. Ingest events and token observations.
3. Seed `pr_review_comments` and `pr_issue_comments` cursors to max tuple from bootstrap scan.
4. Set `bootstrap_complete=1`.

Bootstrap for operator and source-issue surfaces:

1. Seed issue cursors to latest visible tuple so old history is not replayed.
2. Keep existing id cursors for pre/post issue-routing behavior.

Cursor corruption handling:

1. If cursor contains invalid type or future timestamp, log `cursor_invalid` and reset to `now - safe_backfill_window`.
2. Safe backfill window default is 24 hours.
3. Dedupe prevents duplicate side effects during replay.

## Status lifecycle semantics

1. `awaiting_feedback` is actively monitored with incremental cursors.
2. `blocked` is monitored for operator commands only; feedback event ingestion is paused.
3. `closed` and `merged` remain terminal and are not auto-resumed.
4. Re-opened PRs do not auto-reenter monitoring to preserve documented stop-signal semantics.
5. `unblock` workflow remains for `blocked` only and does not alter `closed` or `merged` rows.

Optional explicit resume path (recommended for usability):

1. Add `mergexo feedback resume --pr <n> [--repo <owner/name>] [--head-sha <sha>]`.
2. Resume is allowed only if remote PR is open and not merged.
3. On resume, state is set back to `awaiting_feedback`, `last_seen_head_sha` is reseeded from remote PR head, and feedback cursors are bootstrapped with a bounded backfill scan.
4. `merged` PR resume attempts are rejected.

## Token reconciliation redesign

Current problem:

1. `_token_exists_remotely` and `_issue_has_action_token` trigger full comment scans.

New design:

1. Before posting a tokenized comment, record token as `planned` in `action_tokens`.
2. After successful GitHub write, mark token `posted` and increment `attempt_count`.
3. Every incremental comment fetch parses action markers and marks matching token rows `observed`.
4. Token existence checks for dedupe use `action_tokens` index first.
5. If token is `planned` or `posted` but unobserved beyond timeout, run bounded targeted refetch for that scope using `since = created_at - overlap` and then re-check.
6. Remove full-thread token reconciliation scans from feedback and operator paths.

Result:

1. Idempotency remains deterministic.
2. Token reconciliation cost is bounded by incremental surfaces and occasional bounded targeted refetches.

## Operator-command incremental redesign

1. Track per-issue operator cursor in `github_comment_poll_cursors` with surface `issue_operator_commands`.
2. For each poll, fetch only new/updated comments since cursor.
3. Parse and process commands from incremental set only.
4. Keep `operator_commands` primary key dedupe on `command_key` unchanged.
5. Keep one reply per command by tokenized replies tracked in `action_tokens`.
6. Reconciliation (`_reconcile_operator_command`) posts only when command result exists and token is not already `observed` or `posted`.

## Migration plan

### 1. Schema migration

1. Add `github_comment_poll_cursors` and `action_tokens` as additive tables in `_init_schema`.
2. No destructive table rewrite.
3. Existing `issue_comment_cursors`, `feedback_events`, and `operator_commands` remain valid.

### 2. Initialization policy

1. For each tracked `awaiting_feedback` PR: one-time full bootstrap scan and cursor seed.
2. For blocked PR issue threads and optional operations issue: seed operator cursor to latest tuple.
3. For source issue pre/post routing: seed incremental issue cursors to latest tuple and continue using existing id cursors for semantic gating.

### 3. Rollout controls

1. Add config flag `runtime.enable_incremental_comment_fetch` default `false`.
2. Add config knobs:
   1. `runtime.comment_fetch_overlap_seconds` default `5`.
   2. `runtime.comment_fetch_safe_backfill_seconds` default `86400`.
3. When flag is `false`, retain current full-scan behavior.
4. Rollback is immediate by disabling the flag.

### 4. Roll-forward plan

1. Phase 1: ship schema and metrics with feature off.
2. Phase 2: enable on one canary repo.
3. Phase 3: enable for feedback surfaces first.
4. Phase 4: enable operator/source-issue incremental scans and token-index checks.
5. Phase 5: remove legacy full-scan code paths after stable canary window.

## Observability and SLOs

Metrics to add:

1. `github_comment_fetch_requests_total{surface}`
2. `github_comment_fetch_pages_total{surface}`
3. `github_comment_fetched_total{surface}`
4. `github_comment_new_total{surface}`
5. `github_comment_replay_total{surface}` for overlap duplicates
6. `github_comment_cursor_lag_seconds{surface}`
7. `feedback_events_ingested_total`
8. `feedback_events_deduped_total`
9. `action_tokens_total{status,source}`
10. `action_token_unobserved_age_seconds`
11. `operator_commands_seen_total` and `operator_commands_applied_total`

Structured log events to add:

1. `incremental_scan_started`
2. `incremental_scan_completed`
3. `incremental_scan_backfill`
4. `cursor_invalid`
5. `token_observed`
6. `token_reconcile_timeout`

SLO targets:

1. For quiescent monitored PRs, p95 `github_comment_new_total` is 0 and does not drift upward with thread age.
2. For quiescent monitored PRs, p95 `github_comment_fetched_total` remains bounded by overlap replay constants.
3. For active PRs, end-to-end comment-to-ingest lag p95 <= `2 * poll_interval_seconds + 30s`.

## Test plan

### 1. Unit tests

1. Cursor tuple comparison and overlap-window filtering.
2. `since` computation and floor behavior.
3. Atomic transaction tests for `ingest_feedback_scan_batch`.
4. Token state transitions: `planned -> posted -> observed`.

### 2. GitHub gateway tests

1. Query composition includes `since`, `per_page`, and page loop.
2. Pagination across >100 and >1000 comments.
3. Retry/failure behavior still raises `GitHubPollingError` for polling reads.

### 3. Orchestrator tests

1. Feedback polling ingests only incremental deltas across multiple polls.
2. Edited comment (same id, new `updated_at`) creates a new feedback event.
3. Same-second boundary test with overlap and tie-break id.
4. Token checks no longer trigger full-thread scans.
5. Operator command scanning processes only incremental comments and preserves one reply per command.
6. Pre-PR and post-PR source issue flows use incremental fetch while preserving id cursor semantics.
7. Closed/merged terminal behavior unchanged.
8. Reopened PR behavior matches explicit policy.

### 4. Runtime-mode integration tests

1. In `service` mode, verify bounded polling behavior and cursor advancement under steady-state load.
2. In `console` mode (service + TUI together), verify bounded polling behavior is identical to `service` mode for the same tracked workload.
3. Verify TUI refresh activity does not increase GitHub API call volume.
4. Verify graceful console shutdown does not regress cursor durability or event-ingestion correctness.

### 5. Property or invariant tests

1. Generate random streams of comment creates/edits with random page boundaries.
2. Assert that final ingested event-key set equals reference full-scan event-key set.
3. Assert no duplicate processing side effects for identical input streams.

### 6. Long-thread scenarios

1. PR with 100 comments, 1000 comments, and 10000 comments.
2. Verify steady-state polling cost remains flat after bootstrap.
3. Verify one new comment produces bounded additional fetch.

## Risks and mitigations

1. Risk: cursor boundary bugs may skip comments.
   Mitigation: overlap window, tuple tie-break, dual-read canary comparison, property tests.
2. Risk: token state divergence on crashes.
   Mitigation: explicit token lifecycle table, bounded targeted refetch before retry, observability alarms.
3. Risk: migration bootstrap spikes API usage.
   Mitigation: bootstrap only active tracked PRs, spread bootstrap across polls, canary rollout.
4. Risk: endpoint behavior differences across comment surfaces.
   Mitigation: gateway-level contract tests per endpoint and fallback feature flag.
5. Risk: reopened PR expectations differ across teams.
   Mitigation: explicit README policy and optional explicit resume command.
6. Risk: concurrent TUI reads in `console` mode hide or distort polling regressions.
   Mitigation: include per-surface API-call metrics and canary assertions in both `service` and `console` modes.

## Rollout notes

1. Start with feature flag off in production.
2. Enable on one repository and run for at least one week.
3. During canary, sample-compare incremental event ingestion against legacy full-scan in shadow mode for tracked PRs.
4. Run canary validation in both runtime entrypoints: `mergexo service` and `mergexo console`.
5. Promote to all repos only after bounded-growth metrics and correctness checks pass in both modes.
6. Keep fallback to legacy mode for one release after global enable.

## Acceptance criteria

1. Design is detailed enough that implementation does not require unresolved algorithm decisions.
2. New cursor schema and token schema are fully specified, including keys and transactional behavior.
3. Polling algorithm specifies exact use of `since`, pagination, overlap, and tuple boundary handling.
4. Feedback, source-issue routing, operator-command scanning, and token reconciliation all have bounded incremental fetch paths.
5. Close, merge, blocked, and reopened semantics are explicitly documented, including unblock interactions.
6. Migration plan includes additive schema, initialization for existing tracked PRs/issues, and rollback strategy.
7. Observability section defines concrete metrics and SLO targets for bounded growth.
8. Test plan covers boundary cases, edits, long threads, outage recovery, and reopened PR semantics.
9. Empirical success metric: with a PR containing at least 1000 historical comments and no new activity, steady-state fetched comment count per poll remains constant over time and independent of total history size.
10. Empirical success metric: for N active tracked PRs and M blocked/operator issue threads, API calls per poll remain linear in N+M plus pages for newly changed comments only.
11. The above growth metrics hold in both `mergexo service` and `mergexo console` modes.
12. No regression to feedback correctness is introduced in canary validation.
13. README/operator docs are updated for any behavior changes introduced by this design.
