---
issue: 110
priority: 3
touch_paths:
  - src/mergexo/codehost.py
  - src/mergexo/codehost_factory.py
  - src/mergexo/github_gateway.py
  - src/mergexo/gitlab_gateway.py
  - src/mergexo/bitbucket_gateway.py
  - src/mergexo/config.py
  - src/mergexo/models.py
  - src/mergexo/cli.py
  - src/mergexo/service_runner.py
  - src/mergexo/orchestrator.py
  - src/mergexo/git_ops.py
  - src/mergexo/observability_tui.py
  - src/mergexo/observability.py
  - README.md
  - mergexo.toml.example
  - tests/test_github_gateway.py
  - tests/test_gitlab_gateway.py
  - tests/test_bitbucket_gateway.py
  - tests/test_codehost_factory.py
  - tests/test_config.py
  - tests/test_cli.py
  - tests/test_service_runner.py
  - tests/test_orchestrator.py
  - tests/test_observability_tui.py
depends_on: []
estimated_size: M
generated_at: 2026-02-26T05:34:03Z
---

# Design doc for issue #110: abstract the codehost provider

_Issue: #110 (https://github.com/johnynek/mergexo/issues/110)_

## Summary

Introduce a `CodehostGateway` abstraction with GitHub, GitLab, and Bitbucket adapters so MergeXO can support multiple code hosts without rewriting orchestrator/state management.

---
issue: 110
priority: 2
touch_paths:
  - docs/design/110-abstract-the-codehost-provider.md
  - src/mergexo/codehost.py
  - src/mergexo/codehost_factory.py
  - src/mergexo/github_gateway.py
  - src/mergexo/gitlab_gateway.py
  - src/mergexo/bitbucket_gateway.py
  - src/mergexo/config.py
  - src/mergexo/models.py
  - src/mergexo/cli.py
  - src/mergexo/service_runner.py
  - src/mergexo/orchestrator.py
  - src/mergexo/git_ops.py
  - src/mergexo/observability_tui.py
  - src/mergexo/observability.py
  - README.md
  - mergexo.toml.example
  - tests/test_github_gateway.py
  - tests/test_gitlab_gateway.py
  - tests/test_bitbucket_gateway.py
  - tests/test_codehost_factory.py
  - tests/test_config.py
  - tests/test_cli.py
  - tests/test_service_runner.py
  - tests/test_orchestrator.py
  - tests/test_observability_tui.py
depends_on:
  - 33
estimated_size: L
generated_at: 2026-02-26T00:00:00Z
---

# Abstract the codehost provider

_Issue: #110 (https://github.com/johnynek/mergexo/issues/110)_

## Summary

Introduce a provider abstraction layer so MergeXO can run the same orchestrator and state machine against GitHub, GitLab, and Bitbucket. The design keeps state semantics and lifecycle transitions intact, while moving host-specific API details behind a new `CodehostGateway` contract.

## Context

Current code hardcodes GitHub assumptions in:

1. Gateway construction and typing (`GitHubGateway`) in CLI/service/orchestrator.
2. API behavior (`gh api`, endpoint shapes, error classes) in `src/mergexo/github_gateway.py`.
3. URL generation in observability (`https://github.com/...`).
4. Default remote URL generation (`git@github.com:<owner>/<name>.git`).
5. Naming (`github_*`) in state helper APIs and events.

At the same time, existing runtime state is already mostly host-neutral:

1. Keys are `repo_full_name + issue_number/pr_number/comment_id`.
2. Event dedupe is based on stable tuples and timestamps, not GitHub-specific IDs.
3. Agent prompts and worker lifecycle are not tied to one host.

That means we can avoid a state-model rewrite if we abstract I/O boundaries and keep the same normalized models.

## Goals

1. Support GitHub, GitLab, and Bitbucket from one orchestrator/state code path.
2. Preserve current state-machine behavior (`pending`, `running`, `awaiting_feedback`, `blocked`, `merged`, etc.).
3. Avoid destructive state schema migration for existing GitHub installs.
4. Keep GitHub behavior unchanged by default.
5. Make non-GitHub support explicit and configurable per repo.

## Non-goals

1. Full CI-provider parity in this issue (GitHub Actions remains first-class in v1).
2. Webhook migration.
3. Renaming every legacy `github_*` symbol/table immediately.
4. Replacing existing agent prompt contracts.

## Provider API findings

The required primitives exist for GitLab and Bitbucket, but with shape differences:

1. GitLab:
- Issues with labels: `GET /projects/:id/issues` with `state` and `labels` filters.
- Merge requests: `GET /projects/:id/merge_requests` supports branch filters (`source_branch`, `target_branch`) and `state`.
- Review-thread data: merge request discussions API.
- Commit comparison: repository compare API.

2. Bitbucket Cloud:
- Pull request listing supports query expressions (`q`) on source/destination branch and state.
- PR comments and diffstat endpoints exist.
- Issue tracker supports query expressions (`q`) and includes fields such as `kind` and `state`; label semantics are not equivalent to GitHub/GitLab labels.

Reference docs:

1. GitLab Issues API: https://docs.gitlab.com/api/issues/
2. GitLab Merge Requests API: https://docs.gitlab.com/api/merge_requests/
3. GitLab Discussions API: https://docs.gitlab.com/api/discussions/
4. GitLab compare API: https://docs.gitlab.com/api/repositories/#compare-branches-tags-or-commits
5. Bitbucket issue tracker API: https://developer.atlassian.com/cloud/bitbucket/rest/api-group-issue-tracker/
6. Bitbucket pull requests API: https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/

## Proposed architecture

### 1. Add a host-agnostic gateway contract

Create `src/mergexo/codehost.py` with:

1. `CodehostGateway` protocol containing the methods currently consumed by orchestrator/service.
2. Provider-neutral errors:
- `CodehostPollingError`
- `CodehostAuthenticationError`
3. `CodehostCapabilities` to declare host feature support (labels, CI feedback, compare status, review-reply support).

Goal: orchestrator depends on one interface, not concrete GitHub types.

### 2. Keep normalized domain models unchanged

Keep `Issue`, `PullRequest`, `PullRequestSnapshot`, `PullRequestReviewComment`, and `PullRequestIssueComment` as canonical types returned from all providers.

Provider adapters handle mapping differences internally so state and prompts do not fork.

### 3. Provider adapters

1. `GitHubGateway` remains and is adapted to implement `CodehostGateway` directly.
2. New `GitLabGateway` maps Merge Request discussions/notes to normalized review and issue comments.
3. New `BitbucketGateway` maps PR comment payloads into:
- review comments when inline metadata exists
- issue comments when comment is top-level

### 4. API endpoint mapping by operation

| Operation | GitHub | GitLab | Bitbucket |
| --- | --- | --- | --- |
| List open issues for intake | issues + labels query | project issues + labels query | issues endpoint + `q` filters |
| Create PR/MR | pulls create | merge_requests create | pullrequests create |
| Find PR/MR by branch | pulls with `head`/`base` | merge_requests with source/target filters | pullrequests with `q` branch filters |
| Read PR snapshot | pull request endpoint | merge request endpoint | pull request endpoint |
| List changed files | pull request files endpoint | merge request changes endpoint | pull request diffstat endpoint |
| List review comments | pull request review comments | merge request discussions | pull request comments (inline only) |
| List general PR comments | issue comments on PR | merge request notes | pull request comments (non-inline) |
| Post general comment | issue comments create | issue/MR notes create | issue/PR comments create |
| Post review reply | review reply endpoint | discussion reply endpoint | comment reply endpoint |

### 5. Repo config and identity changes

Extend `RepoConfig` in `src/mergexo/config.py` and `src/mergexo/models.py`:

1. `codehost`: `github | gitlab | bitbucket` (default `github`).
2. `host`: optional hostname (defaults by provider: `github.com`, `gitlab.com`, `bitbucket.org`).
3. `api_base_url` and `web_base_url` optional overrides (self-managed GitLab support).
4. Auth env keys for non-GitHub:
- GitLab: token env var (default `GITLAB_TOKEN`)
- Bitbucket: username + app-password env vars (defaults `BITBUCKET_USERNAME`, `BITBUCKET_APP_PASSWORD`)

Identity compatibility rule:

1. Existing GitHub repos on `github.com` keep current `repo_full_name` (`owner/name`) to avoid state reset.
2. Non-GitHub (or non-default host) uses a qualified key: `<codehost>:<host>/<owner>/<name>`.

This keeps state schemas intact while preventing cross-host key collisions.

### 6. State strategy (no rewrite)

State storage remains structurally unchanged in this issue:

1. Keep current tables and primary keys.
2. Continue storing normalized numeric issue/PR/comment IDs.
3. Continue using the same feedback/event keys.
4. Treat current `github_*` table names as legacy names with provider-neutral semantics.

Optional cleanup follow-up: rename table/API symbols to `codehost_*` after provider support is stable.

### 7. Orchestrator and runtime wiring changes

1. `cli.py` and `service_runner.py` construct gateways via `codehost_factory.build_gateway(repo)`.
2. `orchestrator.py` changes type annotations and exception handling from GitHub-specific to codehost-generic.
3. Existing business logic (enqueue, feedback loop, restart/operator commands) stays unchanged.
4. `git_ops.py` default remote URL generation becomes provider-aware.

### 8. Intake trigger strategy across hosts

Label parity differs by host, so intake is resolved in order:

1. Primary: existing labels (`trigger_label`, `bugfix_label`, `small_job_label`) when provider supports labels.
2. Fallback: issue body/title markers (`/mergexo flow design_doc|bugfix|small_job`) for hosts where labels are not equivalent.

This preserves current GitHub/GitLab behavior while giving Bitbucket a deterministic intake path without changing state semantics.

### 9. CI monitoring behavior by capability

1. Keep current GitHub Actions monitor as-is for GitHub repos.
2. For providers without configured CI support in v1, enforce a no-op policy (`never`) and emit a structured warning event.
3. Follow-up issues can add GitLab Pipelines and Bitbucket Pipelines adapters behind the same capability flag.

### 10. Observability URL generation

Replace hardcoded `github.com` URL builders in `src/mergexo/observability_tui.py` with provider-aware URL formatting:

1. GitHub: `/issues/<n>`, `/pull/<n>`, `/tree/<branch>`, `/commit/<sha>`
2. GitLab: `/-/issues/<n>`, `/-/merge_requests/<n>`, `/-/tree/<branch>`, `/-/commit/<sha>`
3. Bitbucket: `/issues/<n>`, `/pull-requests/<n>`, `/src/<branch>`, `/commits/<sha>`

Legacy GitHub repo keys continue to render exactly as today.

## Implementation plan

1. Add `codehost.py` contract + errors + capability model.
2. Add `codehost_factory.py` and route CLI/service runtime construction through it.
3. Adapt `GitHubGateway` to implement contract with no behavior changes.
4. Add `GitLabGateway` implementation for issue intake, MR lifecycle, comments, and review replies.
5. Add `BitbucketGateway` implementation for issue intake, PR lifecycle, comments, and replies.
6. Update config parser and sample config for provider and auth fields.
7. Update orchestrator/service runner typing and exception handling to generic codehost types.
8. Update git remote defaults/path layout logic for provider hostnames.
9. Implement provider-aware URL builders in observability TUI.
10. Update README with per-provider setup and known capability differences.
11. Add provider contract tests and per-provider adapter tests.
12. Add integration tests for mixed-provider polling in one process.

## Testing plan

1. Contract tests:
- one shared suite that runs against fake GitHub/GitLab/Bitbucket adapters and asserts normalized outputs.

2. Provider adapter unit tests:
- `tests/test_github_gateway.py` regression parity.
- `tests/test_gitlab_gateway.py` endpoint mapping and payload parsing.
- `tests/test_bitbucket_gateway.py` query filters and comment mapping.

3. Runtime wiring tests:
- `tests/test_codehost_factory.py`
- `tests/test_cli.py`
- `tests/test_service_runner.py`

4. Orchestrator behavior tests:
- `tests/test_orchestrator.py` unchanged state transitions across providers.
- auth/polling error handling through `CodehostAuthenticationError` and `CodehostPollingError`.

5. Config and observability tests:
- `tests/test_config.py` provider config defaults and validation.
- `tests/test_observability_tui.py` host-specific URLs.

## Acceptance criteria

1. Existing GitHub configuration (without new fields) continues to work unchanged.
2. A repo can be configured with `codehost = "gitlab"` and complete the issue -> design PR/MR -> feedback loop using existing state transitions.
3. A repo can be configured with `codehost = "bitbucket"` and complete the issue -> PR -> feedback loop using existing state transitions.
4. Mixed-host operation (GitHub + GitLab + Bitbucket) works in one process using current multi-repo scheduler.
5. State tables and status enums are reused; no destructive DB reinit is required for GitHub-only upgrades.
6. Repo-key collisions are prevented across hosts.
7. Review replies are posted correctly for all supported providers where review comments are present.
8. Intake flow selection remains label-based on GitHub/GitLab and supports marker fallback where labels are not equivalent.
9. `mergexo init` computes correct provider-specific default remotes when `remote_url` is omitted.
10. Observability links open correct host/provider URLs for issue/PR/branch/commit.
11. Polling/auth failures are surfaced via provider-neutral exceptions and existing retry behavior.
12. CI monitoring remains functional on GitHub and degrades safely on unsupported providers.

## Risks and mitigations

1. Risk: comment threading semantics differ (notably GitLab discussions).
Mitigation: resolve reply targets inside provider adapter; keep normalized comment model stable.

2. Risk: Bitbucket issue metadata does not mirror label behavior.
Mitigation: add deterministic flow-marker fallback and document it.

3. Risk: auth setup complexity increases for non-GitHub hosts.
Mitigation: explicit env-var contract per provider, startup validation, and clear README examples.

4. Risk: cross-host identity collisions in state and checkout paths.
Mitigation: qualified repo keys for non-default hosts and provider-aware checkout layout.

5. Risk: broad refactor accidentally regresses GitHub behavior.
Mitigation: phase rollout with GitHub adapter parity first and contract/regression tests before enabling new providers.

6. Risk: naming debt (`github_*`) causes confusion.
Mitigation: keep names as compatibility layer now; schedule rename once providers are stable.

## Rollout notes

1. Phase 1: land abstraction + factory + GitHub parity only.
2. Phase 2: enable GitLab behind explicit `codehost = "gitlab"` config and run one-repo canary.
3. Phase 3: enable Bitbucket with marker-based intake canary.
4. Phase 4: run mixed-host canary and verify scheduler fairness, auth failures, and observability links.
5. Keep non-GitHub provider support opt-in until adapter test coverage and canary metrics are stable.

## Open questions

1. Should we add GitLab Pipelines/Bitbucket Pipelines in this issue or a follow-up?
2. Do we want to rename `github_*` state APIs/tables immediately after migration or defer until v2 cleanup?
3. Should Bitbucket intake support additional mapping from `kind` values to flow labels by default?
