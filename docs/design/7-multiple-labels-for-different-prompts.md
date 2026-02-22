---
issue: 7
priority: 3
touch_paths:
  - src/mergexo/config.py
  - src/mergexo/models.py
  - src/mergexo/github_gateway.py
  - src/mergexo/prompts.py
  - src/mergexo/agent_adapter.py
  - src/mergexo/codex_adapter.py
  - src/mergexo/git_ops.py
  - src/mergexo/orchestrator.py
  - README.md
  - mergexo.toml.example
  - tests/test_config.py
  - tests/test_github_gateway.py
  - tests/test_models_and_prompts.py
  - tests/test_agent_adapter.py
  - tests/test_codex_adapter.py
  - tests/test_git_ops.py
  - tests/test_orchestrator.py
depends_on: []
estimated_size: M
generated_at: 2026-02-22T01:27:47Z
---

# Multiple labels for different prompts

_Issue: #7 (https://github.com/johnynek/mergexo/issues/7)_

## Summary

Introduce label-based routing for three issue intake flows (design doc, bugfix, and small job), with direct PR generation for bugfix/small-job issues, regression-test enforcement for bugfixes, and backward-compatible rollout.

---
issue: 7
status: proposed
priority: 2
touch_paths:
  - src/mergexo/config.py
  - src/mergexo/models.py
  - src/mergexo/github_gateway.py
  - src/mergexo/prompts.py
  - src/mergexo/agent_adapter.py
  - src/mergexo/codex_adapter.py
  - src/mergexo/git_ops.py
  - src/mergexo/orchestrator.py
  - README.md
  - mergexo.toml.example
  - tests/test_config.py
  - tests/test_github_gateway.py
  - tests/test_models_and_prompts.py
  - tests/test_agent_adapter.py
  - tests/test_codex_adapter.py
  - tests/test_git_ops.py
  - tests/test_orchestrator.py
depends_on: []
estimated_size: M
generated_at: 2026-02-22T00:00:00Z
---

# Multiple labels for different prompts

_Issue: #7 (https://github.com/johnynek/mergexo/issues/7)_

## Summary

Add multi-label issue routing so MergeXO can start one of three flows from issue intake:
1. design doc flow (current behavior),
2. bugfix flow (direct code PR with regression test requirement),
3. small job flow (direct code PR without design-doc stage).

The implementation keeps design flow backward compatible while adding deterministic flow selection, new start prompts, and direct-PR orchestration for bugfix and small-job labels.

## Context

Current intake behavior is single-path:
1. Poll issues using only `repo.trigger_label`.
2. Route every matched issue to the design-doc prompt.
3. Create only a design-doc PR.

This is insufficient for issues that are already actionable as code changes. The issue request calls out two common alternatives:
1. bug reports that should produce a direct bugfix PR with regression coverage,
2. small scoped jobs that should skip a design-doc review stage.

## Goals

1. Support three label-driven issue flows: `design_doc`, `bugfix`, and `small_job`.
2. Keep existing design-doc behavior intact for current users.
3. Add dedicated bugfix and small-job prompts instead of reusing the design-doc prompt.
4. Ensure bugfix PRs include a `Fixes #<issue>` link and regression-test changes.
5. Reuse the existing PR feedback loop after the initial PR is opened, regardless of flow.
6. Keep routing deterministic when an issue carries multiple trigger labels.

## Non goals

1. A fully dynamic workflow registry or plugin system.
2. Automatic issue classification from issue text without labels.
3. Redesigning feedback-loop token/idempotency logic.
4. Implementing design-doc-to-implementation promotion changes in this issue.

## Flow classes and labels

1. `design_doc` flow:
- Trigger label: `repo.trigger_label` (existing config key, typically `agent:design`).
- Behavior: create a design doc at `repo.design_docs_dir/<issue>-<slug>.md` and open a design PR.

2. `bugfix` flow:
- Trigger label: new config key `repo.bugfix_label`, default `agent:bugfix`.
- Behavior: apply code changes directly, require regression-test changes, open bugfix PR.

3. `small_job` flow:
- Trigger label: new config key `repo.small_job_label`, default `agent:small-job`.
- Behavior: apply code changes directly and open PR without design-doc stage.

Flow precedence for multi-labeled issues is:
1. `bugfix`
2. `small_job`
3. `design_doc`

This keeps behavior deterministic and biases toward the most specific direct-action path.

## Architecture

### 1. Intake routing and dedupe

`GitHubGateway` adds a helper that fetches open issues for a set of labels and deduplicates by issue number. Orchestrator then resolves each issue to one flow using the precedence rules above.

Routing contract:
1. Build label set from config: design + bugfix + small job.
2. Fetch open issues matching any of those labels.
3. Resolve a single flow per issue.
4. Enqueue only one run per issue number (existing state behavior).

### 2. Start-flow execution model

`Phase1Orchestrator._process_issue` becomes flow-aware and dispatches to one of two execution shapes.

Design-doc shape:
1. Create `agent/design/<issue>-<slug>` branch.
2. Call `start_design_from_issue`.
3. Render and write design doc.
4. Commit, push, create PR, and comment on source issue.

Direct-PR shape (bugfix and small_job):
1. Create branch by flow:
- bugfix: `agent/bugfix/<issue>-<slug>`
- small job: `agent/small/<issue>-<slug>`
2. Call flow-specific agent start method.
3. If agent returns a blocked reason, post that reason on the issue and mark run failed.
4. Commit staged changes, push, create PR, and comment on source issue.

### 3. Prompt and adapter contracts

`prompts.py` adds:
1. `build_bugfix_prompt`
2. `build_small_job_prompt`

Bugfix prompt requirements:
1. Reproduce the issue from provided report details.
2. Add regression tests in the relevant suite.
3. Implement the fix.
4. Return structured JSON for PR metadata and fallback blocked reasoning.

Small-job prompt requirements:
1. Implement the requested scoped change directly.
2. Return structured JSON for PR metadata and fallback blocked reasoning.

`agent_adapter.py` adds typed results for direct flows, for example a `DirectStartResult` that includes:
1. `pr_title`
2. `pr_summary`
3. `commit_message`
4. `blocked_reason`
5. `session`

`codex_adapter.py` adds parsing/validation for the new direct-flow schemas and routes to the correct prompt builder.

### 4. Bugfix regression-test guard

To enforce the bugfix requirement, orchestrator validates staged file paths before commit:
1. Add a git helper to list staged files.
2. For `bugfix`, require at least one staged file under `tests/` (or configured test root in future).
3. If missing, fail the run with an explanatory issue comment instead of opening a PR.

This gives a concrete enforcement layer beyond prompt instructions.

### 5. PR body invariants

PR body templates are orchestrator-owned to enforce repository policy:
1. bugfix PRs include `Fixes #<issue_number>`.
2. small-job PRs include `Fixes #<issue_number>`.
3. design-doc PRs keep current `Refs #<issue_number>` behavior.

All flows include source issue URL for traceability.

### 6. Feedback loop compatibility

No feedback-loop redesign is needed. Once any flow opens a PR and state is marked completed, existing feedback polling and tokenized idempotent replies continue to work unchanged.

### 7. Backward compatibility

1. Existing configs with only `repo.trigger_label` continue to work.
2. New labels default to `agent:bugfix` and `agent:small-job` when omitted.
3. Existing design-doc behavior and tests remain valid.

## Implementation plan

1. Extend config and models for flow labels and flow kind typing.
- `src/mergexo/config.py`
- `src/mergexo/models.py`
- `tests/test_config.py`

2. Add multi-label issue fetch helper and routing tests.
- `src/mergexo/github_gateway.py`
- `tests/test_github_gateway.py`

3. Add bugfix and small-job prompt builders plus tests.
- `src/mergexo/prompts.py`
- `tests/test_models_and_prompts.py`

4. Extend adapter interfaces and Codex parsing for direct flows.
- `src/mergexo/agent_adapter.py`
- `src/mergexo/codex_adapter.py`
- `tests/test_agent_adapter.py`
- `tests/test_codex_adapter.py`

5. Add staged-file introspection helper for bugfix validation.
- `src/mergexo/git_ops.py`
- `tests/test_git_ops.py`

6. Implement flow-aware orchestrator dispatch and PR creation logic.
- `src/mergexo/orchestrator.py`
- `tests/test_orchestrator.py`

7. Update operator-facing documentation and sample config.
- `README.md`
- `mergexo.toml.example`

## Acceptance criteria

1. Issues with only the design label continue to create design-doc PRs exactly as before.
2. Issues with the bugfix label use the bugfix prompt and do not create a design doc file.
3. Issues with the small-job label use the small-job prompt and do not create a design doc file.
4. Bugfix PR bodies include `Fixes #<issue_number>`.
5. Small-job PR bodies include `Fixes #<issue_number>`.
6. Bugfix flow refuses to open a PR when no regression-test file is staged, and posts a clear issue comment.
7. If agent returns a blocked reason for bugfix or small-job, orchestrator posts that reason and opens no PR.
8. Multi-labeled issues are routed deterministically with precedence `bugfix > small_job > design_doc`.
9. Existing config files that only define `repo.trigger_label` still load successfully.
10. Unit tests cover routing, prompt selection, direct-flow schema parsing, regression-test guard, and PR body invariants.

## Risks

1. Risk: mislabeling can send an issue down the wrong flow.
Mitigation: deterministic precedence, explicit docs for labels, and issue comment text that states chosen flow.

2. Risk: regression-test guard based on path prefix may reject valid nonstandard test layouts.
Mitigation: keep guard simple for now and follow with configurable test roots if needed.

3. Risk: direct flows bypass design-doc review and may increase code-change risk.
Mitigation: preserve human PR review and existing feedback loop before merge.

4. Risk: intake now performs multiple label queries and may increase API usage.
Mitigation: dedupe queries, reuse poll interval, and keep per-cycle worker concurrency caps.

5. Risk: failed direct runs remain non-retryable due current `can_enqueue` semantics.
Mitigation: document operator recovery for now and track automatic retry semantics as follow-up work.

## Rollout notes

1. Land config/parser updates first with backward compatibility to avoid breaking existing deployments.
2. Deploy with labels documented but only apply `agent:bugfix` and `agent:small-job` to a small canary set of issues initially.
3. Verify canary outcomes:
- bugfix PRs include regression tests and `Fixes #`.
- small-job issues skip design-doc generation.
4. Monitor run failures and issue comments for blocked reasons during first week.
5. Expand usage gradually to all repos after stable canary behavior.
