---
issue: 131
priority: 3
touch_paths:
  - docs/design/131-mergexo-triggered-releases.md
  - src/mergexo/config.py
  - src/mergexo/models.py
  - src/mergexo/prompts.py
  - src/mergexo/agent_adapter.py
  - src/mergexo/codex_adapter.py
  - src/mergexo/task_handlers.py
  - src/mergexo/github_gateway.py
  - src/mergexo/git_ops.py
  - src/mergexo/feedback_loop.py
  - src/mergexo/state.py
  - src/mergexo/orchestrator.py
  - src/mergexo/observability_queries.py
  - README.md
  - mergexo.toml.example
  - tests/test_config.py
  - tests/test_models_and_prompts.py
  - tests/test_agent_adapter.py
  - tests/test_codex_adapter.py
  - tests/test_github_gateway.py
  - tests/test_git_ops.py
  - tests/test_feedback_loop.py
  - tests/test_state.py
  - tests/test_orchestrator.py
  - tests/test_observability_queries.py
  - tests/test_task_handlers.py
depends_on: []
estimated_size: M
generated_at: 2026-03-03T07:09:02Z
---

# Issue-driven triggered releases and reusable task handlers

_Issue: #131 (https://github.com/johnynek/mergexo/issues/131)_

## Summary

Add a generic issue-triggered task framework with a first `release` task that gathers repository release state, asks the agent to approve/reject, and runs a deterministic configured release script only after approval.

## Context

MergeXO currently routes labeled issues into PR-producing flows (`design_doc`, `bugfix`, `small_job`) and has a separate PR feedback loop. Issue #131 asks for a new issue-driven automation path for releases where maintainers can trigger release operations from GitHub web or mobile by opening an issue such as `release v0.3.3`.

The requested behavior is:
1. Detect a release request issue with a dedicated label.
2. Collect current repository release state (existing tags, current default-branch SHA, CI status for that SHA, latest release metadata).
3. Ask the agent to review that state and either approve or reject the release request with reasons.
4. If approved, run a deterministic local release script with the requested tag as input.

This is also a foundation for other issue-triggered operational tasks, not only releases.

## Goals

1. Add an issue-triggered task channel using a dedicated label (`agent:task` by default).
2. Implement a first task kind: `release` with request syntax like `release <tag>`.
3. Gate script execution on explicit agent approval based on collected repo state.
4. Keep execution deterministic, restart-safe, and idempotent.
5. Post clear status comments on the source issue for reject, approve, success, and failure states.
6. Define an extension shape so future task kinds can reuse the same orchestration and state machine.

## Non-goals

1. Replacing existing `design_doc`, `bugfix`, `small_job`, or PR feedback flows.
2. Letting the agent execute arbitrary shell commands for release operations.
3. Supporting arbitrary free-form task grammars in this issue.
4. Auto-closing issues or auto-publishing GitHub Releases beyond what the configured script performs.
5. Building a general webhook-based event model in this issue.

## Proposed architecture

### 1. Add a triggered-task channel in config and flow selection

Add new config keys:
1. `runtime.enable_triggered_tasks` (default `false`).
2. `repo.task_label` (default `agent:task`).
3. `repo.release_task_script` (repo-relative or absolute executable path; required when release tasking is enabled).
4. `repo.release_request_regex` (optional override; default supports `release <tag>` and `release: <tag>`).

Flow precedence update:
1. `ignore_label` still preempts all automation.
2. `task_label` preempts `bugfix`, `small_job`, and `trigger_label` when both are present.

Rationale:
1. Release/task issues should never accidentally open implementation PRs.
2. Feature-flagged rollout keeps existing repos unaffected until explicitly enabled.

### 2. Introduce a task-handler abstraction with a first `release` handler

Add a small internal handler interface in `src/mergexo/task_handlers.py`:
1. `parse_request(issue) -> TriggeredTaskRequest | None`.
2. `collect_snapshot(...) -> TriggeredTaskSnapshot`.
3. `build_review_prompt(...) -> str`.
4. `execute(...) -> TriggeredTaskExecutionResult`.

Implement `ReleaseTaskHandler` first:
1. Parses requested tag from issue title first, then first non-empty body line.
2. Normalizes and validates tag syntax with a strict safe pattern.
3. Produces `task_kind = release` and `resource_key = <normalized_tag>` for idempotency/locking.

Future tasks can add handlers without rewriting orchestrator poll logic.

### 3. Persist task lifecycle state separately from PR feedback state

Add new SQLite table `issue_task_state` in `StateStore`:
1. `repo_full_name`, `issue_number` primary key.
2. `task_kind`.
3. `resource_key` (for release: normalized tag).
4. `status` with values: `pending_review`, `rejected`, `approved`, `executing`, `completed`, `failed`.
5. `request_json`.
6. `snapshot_json`.
7. `review_json`.
8. `execution_json`.
9. `last_error`.
10. `active_run_id`.
11. `created_at`, `updated_at`.

Add partial uniqueness for active resources:
1. Unique index on `(repo_full_name, task_kind, resource_key)` when status is one of `pending_review`, `approved`, `executing`.

State APIs:
1. Claim a task run start for issue.
2. Upsert snapshot/review payloads.
3. Transition terminal states.
4. Reconcile stale `executing` rows after restart.

This table is additive and does not alter existing issue/PR state machines.

### 4. Collect deterministic release snapshot before review

Add read APIs in `GitHubGateway` for release context:
1. `get_default_branch_head_sha(default_branch)`.
2. `list_repository_tags(limit)`.
3. `get_latest_release()` (or latest by published timestamp).
4. `list_workflow_runs_for_branch_head(default_branch, head_sha)` for push/main CI evidence.

Snapshot payload for agent review includes:
1. Requested tag.
2. Current default branch and head SHA.
3. Whether tag already exists remotely and what SHA it points to.
4. Most recent N tags.
5. Latest release tag/name/url/published_at when present.
6. Most recent workflow runs for that head SHA including status/conclusion/url.

Snapshot collection is deterministic and versioned in JSON so later task kinds can add fields without schema churn.

### 5. Add an explicit agent review turn for triggered tasks

Extend adapter contracts:
1. Add `TriggeredTaskReviewResult` with `decision` (`approve` or `reject`) and `reason`.
2. Add `AgentAdapter.review_triggered_task(...)` and `CodexAdapter` implementation with a strict output schema.
3. Add prompt builder `build_triggered_task_review_prompt(...)` in `prompts.py`.

Prompt rules for release review:
1. Reject if tag already exists.
2. Reject if default-branch CI for target SHA is missing or non-green.
3. Reject if requested version appears older/equal than latest release (when comparable).
4. Approve only when evidence supports release readiness.
5. Return a concrete reason tied to snapshot fields.

Agent output is advisory only; orchestration policy and execution are deterministic.

### 6. Deterministic release execution contract

Execution path after agent approval:
1. Re-fetch and revalidate critical invariants immediately before execution.
2. Verify default-branch head SHA has not drifted from reviewed snapshot.
3. Verify requested tag still does not exist remotely.
4. Resolve and validate `repo.release_task_script` path.
5. Run script with argv `[script_path, <normalized_tag>]` from a clean worker checkout at `origin/<default_branch>`.
6. Capture stdout/stderr tails for state + issue reporting.
7. Post-success verify that the tag now exists remotely.

No shell interpolation is used; tag is passed as a single argv element.

### 7. Orchestrator integration and scheduling

Add a new poll step `enqueue_triggered_tasks`:
1. Only runs when `runtime.enable_triggered_tasks` is true.
2. Scans open issues with `repo.task_label`.
3. Applies author allowlist and takeover checks.
4. Claims task run state and submits worker futures.

Add a dedicated running-futures map for tasks so `_reap_finished` can finalize task rows independently from PR work.

Worker flow:
1. Parse and validate request.
2. Collect snapshot.
3. Run agent review.
4. On reject: post reason comment, mark task `rejected`.
5. On approve: run deterministic execution path, mark `completed` or `failed`.

### 8. Idempotent comments and restart safety

Reuse action-token patterns for issue comments in task flow:
1. `task_snapshot_recorded`.
2. `task_rejected`.
3. `task_execution_started`.
4. `task_completed`.
5. `task_failed`.

Crash handling:
1. On startup reconciliation, `executing` tasks are checked for post-condition.
2. If release tag exists and matches expectation, mark `completed`.
3. If tag is absent, mark `failed` with `interrupted_during_execution` and require a fresh task issue or explicit retry command in future work.

This prevents accidental double-release when process restarts mid-run.

### 9. Extensibility for additional triggered tasks

The handler abstraction plus JSON snapshot/review payloads allows future tasks (for example docs publish, branch cut, artifact promotion) by adding a handler and config keys, while reusing:
1. Polling and claim logic.
2. State transitions.
3. Agent review gate.
4. Deterministic script execution wrapper.
5. Tokenized issue comments.

## Implementation plan

1. Extend config models/parsing/defaults for `enable_triggered_tasks`, `task_label`, `release_task_script`, and request-regex override.
2. Add triggered-task models and review result dataclasses in `models.py` and `agent_adapter.py`.
3. Add new task review prompt builder and tests in `prompts.py` and `tests/test_models_and_prompts.py`.
4. Implement `CodexAdapter.review_triggered_task` with output schema validation and tests.
5. Add `task_handlers.py` with `ReleaseTaskHandler` parse/snapshot/execute scaffolding.
6. Add GitHub gateway methods for tags/releases/default-branch head/branch-head workflow runs.
7. Add `issue_task_state` schema + APIs + migration-safe initialization in `StateStore`.
8. Integrate new poll step + worker execution + task future reaping in `orchestrator.py`.
9. Add token helpers for task issue comments in `feedback_loop.py`.
10. Extend observability query filters to include `task_flow` run kind where appropriate.
11. Update README and `mergexo.toml.example` with configuration, issue syntax, and operational behavior.
12. Add end-to-end tests for approve, reject, invariant drift, script failure, and restart reconciliation.

## Acceptance criteria

1. With `runtime.enable_triggered_tasks = true`, an open issue labeled `repo.task_label` and titled `release <tag>` is discovered and processed exactly once per claim.
2. If `repo.ignore_label` is present on the issue, triggered task automation is skipped.
3. If issue author is outside `repo.allowed_users`, task execution is rejected with a deterministic issue comment.
4. MergeXO captures snapshot fields: requested tag, default-branch head SHA, tag existence, latest release metadata, and recent CI runs for head SHA.
5. Agent review receives that snapshot and returns a structured approve/reject decision.
6. On reject, MergeXO posts the agent reason on the issue and does not run the release script.
7. On approve, MergeXO runs only the configured release script with the normalized tag argument and no shell interpolation.
8. MergeXO revalidates head SHA and tag non-existence immediately before script execution; drift causes failure with explanatory comment.
9. On successful execution, MergeXO verifies the tag exists remotely and posts a success comment with key evidence.
10. On script failure, MergeXO marks task failed and posts a deterministic failure comment with summarized stderr/stdout tails.
11. Restart during `executing` state does not cause duplicate script runs; reconciliation marks completed when post-condition is already satisfied.
12. Existing design/bugfix/small-job/feedback behavior is unchanged when triggered-task feature is disabled.
13. Task flow is implemented through a handler interface so additional task kinds can be added without rewriting orchestrator scheduling/state plumbing.

## Risks and mitigations

1. Risk: accidental or unsafe release from malformed issue text.
Mitigation: strict request regex, tag normalization, and explicit rejection for ambiguous requests.

2. Risk: stale readiness snapshot (main SHA or CI changes between review and execution).
Mitigation: mandatory invariant revalidation immediately before script invocation.

3. Risk: duplicate release attempts across retries/crashes.
Mitigation: active resource uniqueness (`task_kind + resource_key`), tokenized comments, and restart reconciliation based on remote tag existence.

4. Risk: script path misuse or command injection.
Mitigation: validated executable path, argv-only invocation, and no shell command concatenation.

5. Risk: GitHub API cost increase from tags/releases/workflow polling.
Mitigation: only poll task-labeled issues, bounded list calls, and reuse existing polling cadence.

6. Risk: confusing overlap with existing flow labels.
Mitigation: explicit precedence documentation and deterministic skip logs when task label is present.

## Rollout notes

1. Ship behind `runtime.enable_triggered_tasks = false` by default.
2. Land schema/config/handler scaffolding first without enabling in production repos.
3. Canary on one repo with a noop release script that only logs the provided tag.
4. Validate reject path by opening a request when main CI is failing.
5. Validate approve path and confirm single script invocation plus idempotent comments under forced process restarts.
6. Enable broadly after canary stability and update README examples for operator workflow.
