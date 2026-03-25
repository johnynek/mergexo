---
issue: 155
priority: 3
touch_paths:
  - roadmap_json_to_md.py
  - src/mergexo/roadmap_markdown.py
  - src/mergexo/prompts.py
  - src/mergexo/codex_adapter.py
  - src/mergexo/orchestrator.py
  - README.md
  - tests/test_models_and_prompts.py
  - tests/test_codex_adapter.py
  - tests/test_orchestrator.py
  - tests/test_roadmap_markdown.py
depends_on: []
estimated_size: M
generated_at: 2026-03-25T04:29:29Z
---

# Design graph-first roadmap support

_Issue: #155 (https://github.com/johnynek/mergexo/issues/155)_

## Summary

Make roadmap graph JSON the only agent-authored artifact, generate reviewable markdown deterministically from that graph, and regenerate the markdown automatically during roadmap authoring, revision, and feedback flows.

## Context

Today the roadmap flow asks agents to author two artifacts for the same plan: a human-facing markdown document and a machine-facing `graph_json`. The current roadmap prompts require both payloads, the Codex adapter validates only the graph structure, and the orchestrator writes both files into the PR branch. The roadmap feedback flow repeats the same pattern by telling the agent to keep the markdown and graph in sync manually during review.

That duplication is the core source of friction in issue #155. We already have a canonical graph schema, parser, and versioning rules. The markdown file is valuable for review, but it should be a deterministic view of the graph instead of a second authored source of truth.

## Problem

The current dual-authoring model has four concrete problems:

1. It creates avoidable drift. The graph is the thing MergeXO actually validates and executes, but the markdown can say something slightly different.
2. It wastes agent effort. Initial roadmap authoring, same-roadmap revisions, and roadmap feedback all spend tokens and edits maintaining two representations.
3. It weakens review guarantees. The current roadmap feedback validation checks that markdown exists and the graph parses, but it does not prove the markdown faithfully represents the graph.
4. It complicates repair loops. When roadmap feedback fails validation, the system tells the agent to fix a pair of files instead of making the graph valid and regenerating the derived view.

## Goals

1. Make roadmap graph JSON the only roadmap artifact authored by agents.
2. Generate human-readable roadmap markdown deterministically from the validated graph.
3. Reuse one validation and rendering path across initial roadmap authoring, same-roadmap revision authoring, and roadmap feedback.
4. Keep the existing roadmap graph schema and runtime state model working without a sqlite migration.
5. Make roadmap review workflow explicit: edit the graph, regenerate markdown, commit both.

## Non-goals

1. Redesigning the roadmap graph schema.
2. Retroactively backfilling all historical roadmap markdown files on `main`.
3. Adding external dependencies, templating engines, or a second rendering stack.
4. Solving richer free-form roadmap narrative beyond what can be represented by the existing graph fields.

## Proposed Design

### 1. Make graph JSON the authored source of truth

The agent-facing contracts should become graph-first:

- Initial roadmap authoring returns `title`, `summary`, and `graph_json`.
- Roadmap adjustment and requested roadmap revision return `action`, `summary`, `details`, and `updated_graph_json` when `action = revise`.
- Roadmap feedback remains a file-editing turn, but the prompt should explicitly say that the agent edits only the roadmap `.graph.json` file and does not hand-edit the generated `.md` file.

The graph schema itself stays unchanged: `roadmap_issue_number`, `version`, `nodes`, and per-node `depends_on` entries with explicit `requires`.

To minimize downstream churn, internal dataclasses may still carry markdown strings where the orchestrator already expects them, but those strings become derived data produced inside MergeXO after graph validation. They are no longer agent-authored inputs.

### 2. Add a shared renderer and zero-dependency CLI

Add two new pieces:

- `src/mergexo/roadmap_markdown.py`: a pure helper that takes a validated roadmap graph and returns deterministic markdown.
- `roadmap_json_to_md.py`: a repo-root zero-dependency script that validates roadmap JSON and emits markdown using the shared helper.

Implementation shape:

1. Parse input JSON with the existing roadmap parser.
2. Reuse the parser's canonicalization and validation rules.
3. Render a stable markdown view.
4. Exit non-zero with a clear stderr message on invalid JSON, schema mismatch, missing dependency references, or cycles.

The renderer should optimize for reviewability, not prose flourish. For a valid graph, it should emit:

- a short generated note telling humans to edit the `.graph.json` file,
- roadmap metadata such as issue number, version, and node count,
- a dependency overview in stable topological order with `node_id` tie-breaking,
- one section per node showing `node_id`, `kind`, dependencies, `title`, and `body_markdown`.

This keeps the markdown deterministic for a given canonical graph JSON. If the graph does not change, the markdown does not change.

The CLI should support file-in to stdout and optional file-out so both agents and humans can use it directly during iteration.

### 3. Use one materialization path in orchestration

Add a small orchestrator helper that validates graph JSON, renders markdown, and writes the target roadmap markdown file only when the content differs.

Use that helper at every point where MergeXO materializes roadmap artifacts:

1. initial roadmap PR creation,
2. same-roadmap revision draft materialization and revision PR updates,
3. roadmap feedback before validation, required tests, commit, and push.

The important behavior change is in roadmap feedback. Today `_commit_push_feedback_with_required_tests(...)` commits before roadmap artifact validation. For issue #155, roadmap markdown generation must happen before `commit_all(...)` so the commit always contains the canonical graph plus the freshly generated markdown.

This design keeps the current state layer intact. No sqlite migration is required because the stored graph checksum, graph version, and graph schema do not change.

### 4. Change prompt contracts to teach the tool

`src/mergexo/prompts.py` should change roadmap prompts in three ways:

1. The initial roadmap prompt should stop asking for `roadmap_markdown` and instead tell the agent that markdown is generated from `graph_json`.
2. The roadmap adjustment and requested revision prompts should stop asking for `updated_roadmap_markdown` and instead require only `updated_graph_json` for a revision.
3. The roadmap feedback prompt should explicitly say:
   - edit the roadmap `.graph.json` file,
   - do not hand-edit the roadmap `.md` file,
   - use `python roadmap_json_to_md.py ...` locally if the agent wants to preview the generated markdown,
   - MergeXO will regenerate the markdown before validation and push.

Validation repair comments in roadmap feedback should also change. The actionable repair is now “make the graph valid so markdown can be regenerated,” not “keep both files in sync manually.”

### 5. Keep Codex adapter changes narrow

The Codex adapter should absorb most of the contract change so the orchestrator does not need a broad refactor.

Specifically:

- Update the roadmap output schemas to drop authored markdown fields.
- After parsing `graph_json`, call the shared roadmap markdown renderer.
- Continue returning internal objects that include derived markdown where existing orchestrator code still expects it.
- For `revise`, preserve the current validation behavior around issue number and exact version bump, then derive markdown from the accepted graph JSON before returning the result.

This keeps the agent boundary graph-only while preserving most of the current internal call structure.

### 6. Tighten roadmap feedback validation around graph generation

`_validate_roadmap_feedback_artifacts(...)` should become graph-first:

1. validate the roadmap graph file,
2. regenerate markdown from that graph,
3. verify the generated markdown file now exists and was written successfully.

That turns roadmap feedback validation from “did the agent manually update both files?” into “is the graph valid, and can MergeXO generate the review view from it?”

If graph validation fails, the repair loop should feed the parse/render error back into the agent turn. If markdown generation fails unexpectedly, block the push and report that renderer failure explicitly.

## Implementation Plan

1. Add `src/mergexo/roadmap_markdown.py` and `roadmap_json_to_md.py`, plus focused tests for valid rendering, invalid shape, cycle rejection, and deterministic ordering.
2. Update roadmap prompt text and Codex output schemas so initial roadmap and revision turns return only graph JSON; derive markdown inside the adapter.
3. Add orchestrator helper logic to regenerate roadmap markdown during initial roadmap PR creation, same-roadmap revision materialization, and roadmap feedback before commit and push.
4. Update roadmap feedback validation messages and README docs to reflect the graph-first workflow.
5. Extend orchestrator tests so roadmap feedback succeeds when only the graph file is edited and the markdown file is regenerated automatically.

## Acceptance Criteria

1. Initial roadmap authoring accepts `title`, `summary`, and `graph_json` only; `roadmap_markdown` is no longer required from the agent.
2. Roadmap adjustment and requested revision accept `updated_graph_json` only for `revise`; markdown is derived inside MergeXO after validation.
3. `roadmap_json_to_md.py` validates the existing roadmap graph schema, rejects malformed or cyclic graphs with a non-zero exit code, and emits deterministic markdown for valid input.
4. Orchestrator writes both `.graph.json` and generated `.md` files before opening an initial roadmap PR or same-roadmap revision PR.
5. Roadmap feedback can complete successfully when the agent edits only the `.graph.json` file; MergeXO regenerates the `.md` file before validation, required tests, commit, and push.
6. Generated roadmap markdown clearly indicates that it is derived from graph JSON and consistently reflects the graph contents.
7. Existing merged roadmaps continue to work without a state migration or backfill job because the graph schema and stored checksums stay unchanged.

## Risks

- Generated markdown will be less free-form than today's hand-authored roadmap narrative. If that proves insufficient, the right follow-up is to add explicit graph-level narrative fields, not to reintroduce dual authoring.
- Existing open roadmap PRs and the first post-rollout roadmap revisions may show large markdown diffs because the renderer will normalize prior hand-written markdown into the generated format.
- If render logic is duplicated between the CLI and orchestrator, drift will reappear. The implementation should keep one shared render function and make the script a thin wrapper.

## Rollout Notes

- This can ship without a new feature flag because the graph schema and state model remain compatible; the change is internal to roadmap authoring and materialization.
- Do not backfill historical roadmap markdown on `main`. Normalize markdown only when a roadmap is newly authored, revised, or updated through roadmap feedback.
- After rollout, roadmap prompts and generated markdown should make it obvious that `.md` is derived from `.graph.json`, so humans and agents know where edits belong.
