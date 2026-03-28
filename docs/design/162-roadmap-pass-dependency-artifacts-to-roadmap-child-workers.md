---
issue: 162
priority: 3
touch_paths:
  - src/mergexo/orchestrator.py
  - src/mergexo/prompts.py
  - src/mergexo/agent_adapter.py
  - src/mergexo/models.py
  - src/mergexo/github_gateway.py
  - tests/test_orchestrator.py
  - tests/test_models_and_prompts.py
  - tests/test_agent_adapter.py
  - tests/test_github_gateway.py
  - README.md
depends_on: []
estimated_size: M
generated_at: 2026-03-28T23:10:32Z
---

# Design roadmap dependency artifact handoff for child workers

_Issue: #162 (https://github.com/johnynek/mergexo/issues/162)_

## Summary

Define a versioned dependency handoff contract for roadmap-created child issues. Store a durable snapshot in the child issue body, project it explicitly into worker prompts, limit it to direct dependencies, and include exact artifact paths plus GitHub/git provenance so downstream workers can consume `reference_doc`, `design_doc`, `small_job`, and `roadmap` outputs without reconstructing history.

## Context

Issue #159 / PR #161 fixed roadmap graph semantics by adding `reference_doc`, but execution still only enforces dependency ordering. The roadmap adjustment gate already receives rich `dependency_artifacts`; roadmap child workers do not.

Today a roadmap-created child issue carries only parent/node markers, raw dependency ids, and the node body. Direct worker prompts for `reference_doc`, `design_doc`, `small_job`, and `roadmap` still mostly hand the agent `issue.title`, `issue.url`, and `issue.body`. That leaves downstream workers reconstructing what upstream work actually produced from repo state or GitHub history.

## Problem

The current handoff is insufficient whenever a dependency is meant to provide a concrete artifact rather than just sequencing:
- `reference_doc.planned` does not tell the worker which merged doc path to open.
- `design_doc.planned` does not surface the accepted design artifact or the review constraints that shaped it.
- `small_job.implemented` does not identify which merged PR and files constitute the upstream output.
- `roadmap.planned` does not point the worker at the child roadmap markdown and graph it should treat as input.

Workers should not need MergeXO-specific discovery logic, graph walking, or git-history reconstruction to recover those inputs.

## Goals

1. Define a first-class worker-facing dependency handoff contract for roadmap child execution.
2. Give each child worker deterministic references to the outputs of its satisfied direct dependencies.
3. Surface exact document and artifact paths for `reference_doc` and `design_doc` dependencies.
4. Keep the handoff bounded and stable enough to embed in issue bodies and prompts.
5. Make prompt wording truthful for agents in non-MergeXO repositories.

## Non-goals

1. Reworking roadmap node semantics again after issue #159.
2. Replacing the roadmap adjustment artifact contract.
3. Making transitive graph walking or git-history discovery the primary way workers find inputs.
4. Adding a separate artifact store outside GitHub metadata, repo contents, and existing sqlite state.

## Decision

- MergeXO should hand off only direct dependency artifacts to a roadmap child worker. If transitive input matters, it must either be represented by an explicit edge in the roadmap graph or summarized into a direct dependency artifact by the upstream node itself.
- The handoff should live in both the child issue body and the worker prompt. The child issue body is the canonical durable snapshot. Worker prompts should parse that snapshot and render an explicit `Dependency Handoff` section so agents do not need to understand MergeXO internals.
- Workers should never be expected to walk the roadmap graph or git history to discover their required inputs. MergeXO computes the dependency view ahead of time.

Direct-only scope is the right default because it keeps the contract bounded, makes the graph remain the source of truth for dependency intent, and avoids prompt bloat from repeated transitive context. If a downstream node truly needs `A` as well as `B`, the roadmap should say so with two edges.

## Worker-Facing Contract

### Canonical payload

Add a versioned dependency handoff marker to roadmap-created child issue bodies, using the same hidden-marker pattern already used for roadmap node kind metadata. A representative shape is:
- hidden marker: `<!-- mergexo-roadmap-dependency-handoff:v1:<base64url-json> -->`
- visible summary: a short human-readable `Dependency handoff` section under the existing parent roadmap and node metadata

The hidden payload is canonical. The visible summary is a derived projection for humans.

Top-level payload fields should be:
- `schema_version`: currently `1`
- `roadmap_issue_number`
- `node_id`
- `node_kind`
- `dependencies`: ordered array of direct dependency artifacts

Each dependency artifact should include nulls or empty arrays instead of omitted keys when data is unavailable.

### Per-dependency fields

| Field | Meaning |
| --- | --- |
| `node_id`, `kind`, `title`, `requires` | Identity of the direct dependency edge. |
| `satisfied_by` | The milestone that unblocked the current node, `planned` or `implemented`. |
| `child_issue_number`, `child_issue_url`, `child_issue_title` | Durable GitHub reference to the upstream child issue. |
| `pr_number`, `pr_url`, `pr_title`, `pr_merged` | PR that satisfied the dependency when a PR exists. These stay null for cases like `small_job.planned`. |
| `branch`, `head_sha`, `merge_commit_sha` | Git provenance. Prefer `merge_commit_sha` when GitHub provides it; otherwise fall back to `head_sha`. |
| `artifact_paths` | Repo-relative paths on the default branch, with one `primary` entry when the worker should open a specific artifact first. |
| `changed_files` | Bounded changed-file list for the satisfying PR or activation change. |
| `review_notes`, `issue_notes` | Deterministically truncated human constraints from review summaries and issue comments. |
| `child_issue_body_excerpt`, `pr_body_excerpt` | Optional bounded text when bodies still contain relevant guidance. |

Each `artifact_paths` entry should include:
- `path`
- `role`
- `default_branch_url`
- `blob_url`

`blob_url` should use `merge_commit_sha` when available, else `head_sha`.

## Kind-Specific Outputs

- `reference_doc`
  - Always surface one `primary` doc path under `repo.design_docs_dir`.
  - Include the merged reference-doc PR, branch, head SHA, merge commit SHA when available, and salient review notes.
  - `planned` and `implemented` are the same event, so both provenance and artifact fields refer to the merged doc artifact.
- `design_doc`
  - Always surface the design doc path as `primary`.
  - For `requires = planned`, PR provenance points at the merged design PR.
  - For `requires = implemented`, PR provenance points at the implementation PR that satisfied the node, while `artifact_paths` still includes the design doc path so downstream workers can read the accepted design artifact directly.
- `small_job`
  - For `requires = implemented`, include merged PR provenance, changed files, and review or issue notes.
  - For `requires = planned`, PR and git fields may legitimately be null because only the child issue exists. In that case the worker should treat the upstream child issue text as the only available artifact. If downstream work needs merged code, the roadmap must use `requires = implemented`.
- `roadmap`
  - For `requires = planned`, surface the child roadmap markdown path and `.graph.json` path on the default branch, plus roadmap PR provenance.
  - For `requires = implemented`, keep those paths and add the fact that the child roadmap reached completed status.
  - Downstream workers should read the child roadmap artifacts directly rather than reconstructing them from issue comments.

## Path And Provenance Rules

MergeXO should hand off both of these references for artifact-like dependencies:
- the current default-branch path and URL, so the worker can open the artifact in the local checkout immediately
- an exact merged blob reference, so the worker can reason about provenance without guessing which commit introduced it

For `reference_doc` and `design_doc`, exact document-location rules should be deterministic:
1. Prefer markdown files under `repo.design_docs_dir` from the satisfying PR.
2. If exactly one such file exists, mark it `primary`.
3. If several exist, prefer the file matching the child issue number and slug convention; mark the rest as supporting paths.
4. If no primary doc path can be resolved for a doc dependency, do not issue the downstream child. Leave the node blocked and surface a roadmap error, because the handoff contract would otherwise be untruthful.

This same rule answers the issue-body question for issue #151: downstream nodes should receive the exact doc path, not just the fact that `review_design` exists.

## Size And Truncation Rules

The contract should be deterministic and bounded by serializer rules, not by ad hoc prompt trimming. The raw canonical JSON payload must stay under `16000` characters before base64 encoding.

Serialization should apply these rules in order:

1. Include only direct dependencies.
2. Sort dependencies by `node_id`.
3. Sort `artifact_paths` and `changed_files` lexically after selecting the primary path.
4. Apply initial collection limits:
   - keep at most `10` `artifact_paths`
   - keep at most `25` `changed_files`
   - keep at most `3` `review_notes`
   - keep at most `3` `issue_notes`
   - truncate each review or issue note to `400` characters
   - truncate each body excerpt to `800` characters
5. If the payload still exceeds `16000` characters after the initial limits, reduce data in this order until it fits:
   - drop `child_issue_body_excerpt` and `pr_body_excerpt`
   - reduce `changed_files` to at most `10`
   - reduce `review_notes` and `issue_notes` to at most `1` each
   - reduce supporting `artifact_paths` to at most `3`
6. Never drop identity fields, milestone fields, issue or PR links, git refs, or the `primary` artifact path.

The visible issue-body summary should be smaller than the hidden payload, for example no more than `5` files and `2` notes per dependency. Prompts should render from the parsed canonical payload so retries remain stable.

## Architecture

### Shared collection and serialization

Factor the current orchestrator dependency-artifact gathering into a reusable per-node collector that produces a provenance snapshot. Keep the adjustment gate projection and the worker handoff projection separate, but make them share the same underlying collection logic.

That collector should add:
- exact git provenance fields needed by workers
- kind-specific `artifact_paths`
- deterministic truncation before serialization

`PullRequestSnapshot` and the GitHub gateway should also expose `merge_commit_sha` so the worker contract can point at a pinned merge result when GitHub provides one.

No sqlite migration should be required. The handoff is derived from existing roadmap state plus GitHub metadata, then persisted durably in the child issue body.

### Child issue materialization

`_render_roadmap_child_issue_body(...)` should accept a rendered worker handoff, not just raw dependency ids. The issue body should contain:
- the existing roadmap node kind marker
- the new hidden dependency handoff marker
- a short visible dependency summary
- the original `body_markdown`

This keeps the durable handoff attached to the child issue for retries, manual inspection, and later follow-on flows.

### Prompt consumption

`build_design_prompt(...)`, `build_small_job_prompt(...)`, `build_roadmap_prompt(...)`, and `build_implementation_prompt(...)` should detect the handoff marker and render a dedicated `Dependency Handoff` section before the raw issue body.

`build_implementation_prompt(...)` matters because a roadmap-created `design_doc` issue can later continue into implementation from the same child issue. That same issue should carry the same upstream dependency snapshot throughout its lifecycle.

When the marker is absent, prompts should fall back to current behavior so already-issued legacy child issues keep working.

## Issue #151 / PR #154 Example

Once `review_design` in issue #151 / PR #154 is modeled as `reference_doc`, both `review_config` and `review_contract` should receive one direct dependency entry for `review_design` with:
- `requires = planned`
- `kind = reference_doc`
- the `review_design` child issue number, URL, and title
- the merged reference-doc PR number and URL
- `artifact_paths.primary` pointing at the merged design or reference doc under `docs/design/`
- `branch = agent/reference/...`, `head_sha`, and `merge_commit_sha` when GitHub provides it
- truncated review and issue notes carrying any accepted constraints from the design review

The worker's first step should be to open `artifact_paths.primary`, not infer the document location from node ids or search git history.

## Implementation Plan

1. Add worker-facing handoff dataclasses and serialization helpers, and extend PR snapshots with optional `merge_commit_sha`.
2. Factor orchestrator dependency collection into a shared provenance collector that can project both adjustment artifacts and child-worker handoffs.
3. Extend `_render_roadmap_child_issue_body(...)` to embed the canonical handoff marker plus a short human-readable summary.
4. Update worker prompt builders to parse the marker and render an explicit `Dependency Handoff` section for roadmap-created `reference_doc`, `design_doc`, `small_job`, `roadmap`, and implementation-from-design turns.
5. Add tests for direct-only scope, kind-specific artifact projection, primary doc-path resolution, prompt rendering, and deterministic truncation.
6. Update README guidance so roadmap authors know that downstream workers receive direct dependency artifacts and should model additional required inputs as explicit edges.

## Risks

- If the issue-body renderer and prompt parser drift, workers could read a different handoff from what humans see. The same serializer and parser should back both surfaces.
- If human edits remove or corrupt the hidden marker, prompt rendering could silently degrade. Marker parsing should fail closed to legacy behavior and log a deterministic warning.
- `design_doc` dependencies that reach `implemented` need both implementation provenance and design-doc location. Tests should cover that mixed case explicitly.
- Ambiguous doc-path resolution for doc dependencies would undermine the main goal of the issue. Child issuance should block rather than guess.
- Large dependency fan-in or noisy PR comment threads can still bloat prompts if truncation is not enforced before serialization.

## Rollout Notes

- Ship child-issue rendering, prompt parsing, provenance collection, and tests in the same change.
- Do not backfill closed child issues. For already-issued legacy child issues, keep prompt behavior backward compatible by treating the handoff marker as optional.
- New roadmap child issues should immediately carry the v1 handoff marker and visible summary.
- Update roadmap docs and examples after rollout so operators understand that direct dependencies, not transitive closure, define what downstream workers receive.

## Acceptance Criteria

1. A design doc defines the worker-facing dependency artifact handoff contract for roadmap child execution.
2. The design doc specifies that the contract covers direct dependencies only, and explains why transitive closure is excluded.
3. The design doc specifies that the handoff lives in both the child issue body and worker prompt, with the child issue body carrying the canonical serialized snapshot.
4. The design doc defines minimum provenance fields per dependency artifact, including stable GitHub links, branch or head SHA, merge commit SHA when available, and exact artifact paths.
5. The design doc explains how `reference_doc`, `design_doc`, `small_job`, and `roadmap` dependencies surface their relevant outputs.
6. The design doc defines deterministic size limits and truncation rules so retries and prompt generation stay stable.
7. The design doc uses issue #151 / PR #154 as a motivating example and states what downstream workers should concretely receive from `review_design`.
8. The design leaves workers with no requirement to walk the roadmap graph or git history just to discover dependency outputs.
