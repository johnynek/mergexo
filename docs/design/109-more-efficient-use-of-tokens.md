---
issue: 109
priority: 3
touch_paths:
  - docs/design/109-more-efficient-use-of-tokens.md
  - src/mergexo/config.py
  - src/mergexo/codex_adapter.py
  - README.md
  - mergexo.toml.example
  - tests/test_config.py
  - tests/test_codex_adapter.py
depends_on: []
estimated_size: M
generated_at: 2026-02-26T05:24:45Z
---

# Design doc for issue #109: more efficient use of tokens

_Issue: #109 (https://github.com/johnynek/mergexo/issues/109)_

## Summary

Adds a concrete architecture for token-efficient Codex usage in MergeXO via first-class reasoning effort support and per-task model/effort overrides, with backward compatibility, acceptance criteria, risks, and rollout guidance.

---
issue: 109
priority: 2
touch_paths:
  - docs/design/109-more-efficient-use-of-tokens.md
  - src/mergexo/config.py
  - src/mergexo/codex_adapter.py
  - README.md
  - mergexo.toml.example
  - tests/test_config.py
  - tests/test_codex_adapter.py
depends_on:
  - 7
estimated_size: M
generated_at: 2026-02-26T00:00:00Z
---

# More efficient use of tokens

_Issue: #109 (https://github.com/johnynek/mergexo/issues/109)_

## Summary

Add first-class token-efficiency controls to MergeXO by supporting Codex reasoning effort and per-task model and effort overrides. Keep behavior backward compatible and deterministic, and use existing flow labels, especially `small_job_label`, as the simple-task routing lane in v1.

## Problem statement

Today MergeXO applies one Codex settings bundle per repo for all work types. That means:
1. small and routine tasks can consume unnecessarily expensive reasoning settings,
2. feedback turns can run with the same cost profile as initial implementation turns,
3. operators cannot tune reasoning effort directly without fragile `extra_args` hacks.

Issue #109 asks for a practical way to spend fewer tokens on simpler tasks and reserve expensive settings for harder work.

## Goals

1. Add explicit Codex reasoning effort support in config.
2. Allow per-task model and reasoning overrides for `design_doc`, `bugfix`, `small_job`, `implementation`, and `feedback`.
3. Keep existing config files working unchanged.
4. Keep routing deterministic and easy to reason about.
5. Document a low-friction way to treat simple jobs as cheaper work.

## Non-goals

1. Running an automatic difficulty-classification Codex turn before every issue in this issue.
2. Redesigning issue-flow selection logic.
3. Adding a brand-new `simple-job` flow label in v1.
4. Changing PR feedback-loop semantics.

## Codex CLI options examined

From local Codex CLI behavior (`codex-cli 0.105.0`):
1. `--model` is supported on both `codex exec` and `codex exec resume`.
2. There is no dedicated `--reasoning-effort` flag.
3. Reasoning effort is set via config override argument: `-c model_reasoning_effort='<effort>'`.
4. `-c` overrides the same key loaded from config/profile for that invocation only (it does not persist edits to config files).
5. Valid efforts are `none`, `minimal`, `low`, `medium`, `high`, `xhigh`.
6. `codex exec resume` accepts `-c` overrides, so reasoning effort can be applied to feedback turns too.

## Proposed architecture

### 1. Extend Codex config model

Add optional `reasoning_effort` to `[codex]` and repo overrides.

Add optional task-scoped overrides:
- `[codex.task.design_doc]`
- `[codex.task.bugfix]`
- `[codex.task.small_job]`
- `[codex.task.implementation]`
- `[codex.task.feedback]`

And repo-scoped task overrides:
- `[codex.repo.<repo_id>.task.<task>]`

Each task table supports the same invocation fields used for command building:
- `model`
- `reasoning_effort`
- `sandbox`
- `profile`
- `extra_args`

### 2. Deterministic settings resolution

For each invocation, resolve effective settings in this order:
1. global base `[codex]`
2. global task override `[codex.task.<task>]`
3. repo base `[codex.repo.<repo_id>]`
4. repo task override `[codex.repo.<repo_id>.task.<task>]`

Later layers override earlier layers field-by-field.

### 3. Codex adapter command generation

In `CodexAdapter`, map each method to one task key:
1. `start_design_from_issue` -> `design_doc`
2. `start_bugfix_from_issue` -> `bugfix`
3. `start_small_job_from_issue` -> `small_job`
4. `start_implementation_from_design` -> `implementation`
5. `respond_to_feedback` -> `feedback`

Command build rules:
1. Keep existing `--model`, `--sandbox`, `--profile`, `extra_args` behavior.
2. When `reasoning_effort` is set, append `-c model_reasoning_effort='<value>'`.
3. Keep resume filtering for unsupported options (`--sandbox`, `--profile`) while preserving model, reasoning effort, and supported extra args.
4. Include selected `model` and `reasoning_effort` in structured adapter logs for observability.

### 4. Simpler-task routing strategy

Decision for v1:
1. Do not add a new `simple-job` label.
2. Use existing flow labels from issue #7 (`bugfix_label`, `small_job_label`, `trigger_label`) and tune cost through per-task overrides.
3. Treat `small_job_label` as the default simple-task lane for cheaper settings.

This avoids new orchestration complexity while still giving immediate token savings.

### 5. Why auto difficulty ranking is deferred

A classifier turn adds extra latency and token spend per issue. Savings are uncertain until deterministic controls are tuned first. The design keeps the resolver extensible so a future optional classifier can be added behind a flag after baseline tuning data is collected.

## Example configuration

```toml
[codex]
enabled = true
model = 'gpt-5.3-codex'
reasoning_effort = 'medium'
extra_args = []

[codex.task.small_job]
model = 'gpt-5.3-codex'
reasoning_effort = 'low'

[codex.task.feedback]
reasoning_effort = 'low'

[codex.repo.mergexo]
model = 'gpt-5.3-codex'

[codex.repo.mergexo.task.design_doc]
reasoning_effort = 'high'
```

## Implementation plan

1. Update config schema and parser in `src/mergexo/config.py`:
- add reasoning effort type validation,
- add task override parsing for global and repo scopes,
- preserve backward compatibility defaults.
2. Update command-resolution logic in `src/mergexo/codex_adapter.py`:
- resolve per-task effective settings,
- append reasoning-effort config override argument,
- keep resume option filtering behavior correct.
3. Update docs:
- `README.md` codex options and precedence,
- `mergexo.toml.example` with practical low-cost `small_job` and `feedback` examples.
4. Add tests:
- config parsing and precedence in `tests/test_config.py`,
- command composition and resume handling in `tests/test_codex_adapter.py`.

## Testing plan

1. `tests/test_config.py`
- accepts all allowed reasoning effort values,
- rejects invalid values,
- verifies task override parsing and precedence.
2. `tests/test_codex_adapter.py`
- verifies `-c model_reasoning_effort='<value>'` is passed for start turns,
- verifies same override is passed for resume turns,
- verifies task-specific overrides choose expected model and effort,
- verifies unsupported resume args remain filtered.

## Acceptance criteria

1. Existing configs without `reasoning_effort` or `task` tables continue to load and run unchanged.
2. `[codex].reasoning_effort` is supported and validated against allowed values.
3. Invalid reasoning effort values fail config load with a clear error.
4. Start turns include resolved reasoning effort via `-c model_reasoning_effort='<value>'` when configured.
5. Feedback resume turns include resolved reasoning effort via the same mechanism.
6. Task-level overrides apply to the correct invocation types.
7. Repo task overrides take precedence over global task overrides.
8. Operators can configure `small_job` to use a cheaper model or effort than design and implementation.
9. README and sample config document new keys, precedence, and recommended low-cost settings.
10. Unit tests cover parsing, precedence, and command construction behavior.

## Risks and mitigations

1. Risk: low reasoning settings increase retries and negate token savings.
Mitigation: start with `small_job` and `feedback` only; tune based on observed outcomes.

2. Risk: unsupported effort and tool combinations cause runtime failures.
Mitigation: strict config validation plus explicit docs; recommend avoiding `minimal` unless validated in the deployment environment.

3. Risk: precedence rules are misconfigured or misunderstood.
Mitigation: document precedence table and add targeted precedence tests.

4. Risk: resume path diverges from start path and uses different settings.
Mitigation: explicit adapter tests for both `exec` and `exec resume` command builders.

## Rollout notes

1. Ship with backward-compatible defaults so no repo changes are required.
2. Canary in one repo with:
- `small_job` set to lower effort,
- `feedback` set to lower effort.
3. Monitor canary for:
- blocked rate,
- retry count,
- issue-to-PR latency,
- token usage from Codex JSON turn usage events.
4. If stable, expand per-task tuning to other repos.
5. Re-evaluate after canary whether an optional auto difficulty classifier or cross-flow simple modifier label is still needed.
