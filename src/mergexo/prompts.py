from __future__ import annotations

import json

from mergexo.agent_adapter import FeedbackTurn, RoadmapDependencyArtifact, RoadmapFeedbackTurn
from mergexo.models import Issue


def _coding_guidelines_task_lines(*, coding_guidelines_path: str | None) -> str:
    if coding_guidelines_path:
        return f"- Review and follow the coding/testing guidelines in: {coding_guidelines_path}"
    return (
        "- Coding/testing guidelines file is not present in this repo version.\n"
        "- Follow a style consistent with the existing codebase and target 100% test coverage."
    )


def _implementation_checks_line(*, coding_guidelines_path: str | None) -> str:
    if coding_guidelines_path:
        return f"  - Re-run formatting and CI-required checks from {coding_guidelines_path}."
    return "  - Re-run formatting and tests expected by this repo and target 100% test coverage."


def _truncate_prompt_text(value: str, *, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    return value[: max_chars - 15] + "... [truncated]"


def _roadmap_dependency_artifacts_json(
    artifacts: tuple[RoadmapDependencyArtifact, ...],
) -> str:
    payload = [
        {
            "dependency_node_id": artifact.dependency_node_id,
            "dependency_kind": artifact.dependency_kind,
            "dependency_title": artifact.dependency_title,
            "frontier_references": [
                {
                    "ready_node_id": reference.ready_node_id,
                    "requires": reference.requires,
                }
                for reference in artifact.frontier_references
            ],
            "child_issue_number": artifact.child_issue_number,
            "child_issue_url": artifact.child_issue_url,
            "child_issue_title": artifact.child_issue_title,
            "child_issue_body": (
                _truncate_prompt_text(artifact.child_issue_body, max_chars=1200)
                if artifact.child_issue_body is not None
                else None
            ),
            "issue_run_status": artifact.issue_run_status,
            "issue_run_branch": artifact.issue_run_branch,
            "issue_run_error": (
                _truncate_prompt_text(artifact.issue_run_error, max_chars=600)
                if artifact.issue_run_error is not None
                else None
            ),
            "resolution_markers": list(artifact.resolution_markers),
            "pr_number": artifact.pr_number,
            "pr_url": artifact.pr_url,
            "pr_title": artifact.pr_title,
            "pr_body": (
                _truncate_prompt_text(artifact.pr_body, max_chars=1600)
                if artifact.pr_body is not None
                else None
            ),
            "pr_state": artifact.pr_state,
            "pr_merged": artifact.pr_merged,
            "changed_files": list(artifact.changed_files),
            "review_summaries": [
                {
                    "user_login": comment.user_login,
                    "body": _truncate_prompt_text(comment.body, max_chars=600),
                    "html_url": comment.html_url,
                }
                for comment in artifact.review_summaries
            ],
            "issue_comments": [
                {
                    "user_login": comment.user_login,
                    "body": _truncate_prompt_text(comment.body, max_chars=600),
                    "html_url": comment.html_url,
                }
                for comment in artifact.issue_comments
            ],
        }
        for artifact in artifacts
    ]
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _direct_output_contract_lines() -> str:
    return """
Required output fields:
- Always include `pr_title`, `pr_summary`, `commit_message`, `blocked_reason`, and `escalation`.
- `pr_title`: non-empty string.
- `pr_summary`: non-empty string.
- `commit_message`: non-empty string or null. Use a string only when the repository changes are ready for MergeXO to commit and push.
- `blocked_reason`: non-empty string or null. Use a string only when you cannot proceed safely.
- `escalation`: null or an object with exactly `kind`, `summary`, and `details`.
- Use `escalation.kind = "roadmap_revision"` only when the current roadmap assumptions are fundamentally wrong; otherwise set `escalation` to null.
- Do not omit nullable keys. Use null when a field does not apply.
""".strip()


def _feedback_output_contract_lines(*, allow_escalation: bool) -> str:
    top_level_keys = (
        "`git_ops`, `review_replies`, `general_comment`, `commit_message`, "
        "`flaky_test_report`, and `escalation`"
        if allow_escalation
        else "`git_ops`, `review_replies`, `general_comment`, `commit_message`, and `flaky_test_report`"
    )
    escalation_lines = (
        "- `escalation`: null or an object with exactly `kind`, `summary`, and `details`.\n"
        '- Use `escalation.kind = "roadmap_revision"` only for foundational roadmap flaws; otherwise set `escalation` to null.\n'
        if allow_escalation
        else "- Do not include an `escalation` key for this task.\n"
    )
    return (
        "Required output fields:\n"
        f"- Always include {top_level_keys}.\n"
        "- `git_ops`: array. Use `[]` when no Git operation request is needed.\n"
        "- `review_replies`: array. Use `[]` when no line-level reply is needed.\n"
        "- `general_comment`: non-empty string or null.\n"
        "- `commit_message`: non-empty string or null.\n"
        "- `flaky_test_report`: null or an object with exactly `run_id`, `title`, `summary`, and `relevant_log_excerpt`.\n"
        f"{escalation_lines}"
        "- Do not omit nullable fields. Use null when a field does not apply.\n"
    ).strip()


def build_design_prompt(
    *,
    issue: Issue,
    repo_full_name: str,
    design_doc_path: str,
    default_branch: str,
) -> str:
    return f"""
You are the design-doc agent for repository {repo_full_name}.

Task:
- Create structured content for a design doc that addresses issue #{issue.number}.
- The resulting PR will add the file at: {design_doc_path}
- Base branch is: {default_branch}

Output requirements:
- Focus on architecture and implementation plan for the issue.
- Include clear acceptance criteria.
- Include risks and rollout notes.
- You MUST report likely implementation file paths in touch_paths.
- Keep touch_paths concrete and repository-relative.
- `title` and `summary` are rendered separately by the orchestrator.
- `design_doc_markdown` must contain only the body that comes after the auto-generated header.
- Do NOT include YAML frontmatter, the H1 title, the issue line, or a `## Summary` section in `design_doc_markdown`.
- Start `design_doc_markdown` at the first substantive section, for example `## Context`, `## Problem`, `## Goals`, or equivalent.

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

Required output fields:
- Always include `title`, `summary`, `touch_paths`, and `design_doc_markdown`.
- `title`: non-empty string for the design PR title.
- `summary`: non-empty string for the design PR summary.
- `touch_paths`: non-empty array of likely implementation file paths.
- `design_doc_markdown`: non-empty string containing only the markdown body described above.
- Do not omit keys.

Issue title:
{issue.title}

Issue URL:
{issue.html_url}

Issue body:
{issue.body}
""".strip()


def build_bugfix_prompt(
    *,
    issue: Issue,
    repo_full_name: str,
    default_branch: str,
    coding_guidelines_path: str | None,
) -> str:
    coding_guidelines_lines = _coding_guidelines_task_lines(
        coding_guidelines_path=coding_guidelines_path
    )
    return f"""
You are the bugfix agent for repository {repo_full_name}.

Task:
- Resolve issue #{issue.number} directly with code changes.
- Base branch is: {default_branch}
{coding_guidelines_lines}
- Reproduce the issue behavior from the report details.
- Add or update regression tests that fail before the fix and pass after the fix.

Output requirements:
- Implement the fix and any required supporting updates.
- Return PR metadata with a clear summary of what changed.
- If you cannot proceed safely, return a blocked_reason.
- If roadmap assumptions are fundamentally invalid, set escalation with:
  - kind = "roadmap_revision"
  - summary
  - details

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

{_direct_output_contract_lines()}

Issue title:
{issue.title}

Issue URL:
{issue.html_url}

Issue body:
{issue.body}
""".strip()


def build_small_job_prompt(
    *,
    issue: Issue,
    repo_full_name: str,
    default_branch: str,
    coding_guidelines_path: str | None,
) -> str:
    coding_guidelines_lines = _coding_guidelines_task_lines(
        coding_guidelines_path=coding_guidelines_path
    )
    return f"""
You are the small-job agent for repository {repo_full_name}.

Task:
- Implement issue #{issue.number} directly with focused code changes.
- Base branch is: {default_branch}
{coding_guidelines_lines}
- Keep scope tight to the requested job.

Output requirements:
- Implement only what is needed for the requested change.
- Return PR metadata with a concise summary of what changed.
- If you cannot proceed safely, return a blocked_reason.
- If roadmap assumptions are fundamentally invalid, set escalation with:
  - kind = "roadmap_revision"
  - summary
  - details

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

{_direct_output_contract_lines()}

Issue title:
{issue.title}

Issue URL:
{issue.html_url}

Issue body:
{issue.body}
""".strip()


def build_implementation_prompt(
    *,
    issue: Issue,
    repo_full_name: str,
    default_branch: str,
    coding_guidelines_path: str | None,
    design_doc_path: str,
    design_doc_markdown: str,
    design_pr_number: int | None,
    design_pr_url: str | None,
) -> str:
    coding_guidelines_lines = _coding_guidelines_task_lines(
        coding_guidelines_path=coding_guidelines_path
    )
    implementation_checks_line = _implementation_checks_line(
        coding_guidelines_path=coding_guidelines_path
    )
    design_pr_line = (
        f"- Source design PR: #{design_pr_number} ({design_pr_url})"
        if design_pr_number is not None and design_pr_url
        else "- Source design PR: unavailable in local state"
    )
    return f"""
You are the implementation agent for repository {repo_full_name}.

Task:
- Implement issue #{issue.number} by following the merged design doc.
- Base branch is: {default_branch}
{coding_guidelines_lines}
- Design doc path on base branch: {design_doc_path}
{design_pr_line}

Output requirements:
- Implement the accepted design in repository code and tests.
- Keep changes aligned to the design doc scope unless explicitly blocked by missing requirements.
- Return PR metadata with a clear summary of what changed.
- If you cannot proceed safely, return a blocked_reason.
- If roadmap assumptions are fundamentally invalid, set escalation with:
  - kind = "roadmap_revision"
  - summary
  - details
- Before finalizing your output:
  - Re-read the full diff against {default_branch}.
  - Re-read the design doc and any PR comments provided in context.
  - Confirm all requested work is addressed.
  - Minimize duplication and remove confusing or unnecessary code.
  - Add concise comments around subtle logic to help future readers.
{implementation_checks_line}
  - Only finalize when those checks pass so MergeXO can commit and push confidently.

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

{_direct_output_contract_lines()}

Issue title:
{issue.title}

Issue URL:
{issue.html_url}

Issue body:
{issue.body}

Merged design doc markdown ({design_doc_path}):
{design_doc_markdown}
""".strip()


def build_roadmap_prompt(
    *,
    issue: Issue,
    repo_full_name: str,
    default_branch: str,
    roadmap_docs_dir: str,
    recommended_node_count: int,
    coding_guidelines_path: str | None,
) -> str:
    coding_guidelines_lines = _coding_guidelines_task_lines(
        coding_guidelines_path=coding_guidelines_path
    )
    markdown_path = f"{roadmap_docs_dir}/{issue.number}-<slug>.md"
    graph_path = f"{roadmap_docs_dir}/{issue.number}-<slug>.graph.json"
    return f"""
You are the roadmap agent for repository {repo_full_name}.

Task:
- Author the initial roadmap PR for issue #{issue.number}.
- Base branch is: {default_branch}
{coding_guidelines_lines}
- Create both roadmap artifacts:
  - narrative markdown
  - canonical machine-readable graph JSON

Output requirements:
- This turn is for roadmap authoring, not repository code implementation.
- Produce a practical execution plan that decomposes the issue into child work MergeXO can issue incrementally.
- `roadmap_markdown` and `graph_json` must describe the same plan.
- Each roadmap node should represent one concrete child issue with reviewable scope, a clear completion outcome, and text that can plausibly become that child issue's title/body.
- Keep the graph acyclic with internal `node_id` references only.
- Allowed node kinds: `design_doc`, `small_job`, `roadmap`.
- Prefer:
  - `design_doc` when downstream work should wait for a reviewed design artifact.
  - `small_job` for bounded implementation work that should directly produce a PR.
  - `roadmap` only when a subtree is large enough to deserve its own nested roadmap.
- Milestone semantics for dependency planning:
  - `design_doc.planned`: the design PR has merged.
  - `design_doc.implemented`: implementation from that design has merged.
  - `small_job.planned`: the child issue has been created and labeled.
  - `small_job.implemented`: the child PR has merged.
  - `roadmap.planned`: the child roadmap PR has merged and its graph is activated.
  - `roadmap.implemented`: the child roadmap has completed.
- Dependency `requires` must be `planned` or `implemented`.
- Include an explicit `requires` on every dependency object.
- Include an explicit `depends_on` array on every node, using `[]` when there are no dependencies.
- Use `implemented` when downstream work truly depends on the completed outcome.
- Use `planned` only when downstream work can safely begin once the upstream plan/design/activation milestone exists.
- Choose short, unique, stable `node_id` values that will still make sense if the roadmap is revised later.
- Recommended node count is around {recommended_node_count}; larger DAGs are allowed but should include decomposition notes.
- A useful `roadmap_markdown` usually includes:
  - a short overview of the overall strategy,
  - dependency or sequencing notes for the DAG,
  - a node-by-node breakdown keyed by `node_id` so reviewers can compare the markdown against the graph,
  - decomposition notes when the roadmap is intentionally larger than the recommended size.

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

Required output fields:
- Always include `title`, `summary`, `roadmap_markdown`, and `graph_json`.
- `title`: non-empty string for the roadmap PR title.
- `summary`: non-empty string for the roadmap PR summary.
- `roadmap_markdown`: non-empty string containing the full roadmap markdown body for the target file.
- `graph_json` as a JSON object, not a string, with exactly:
  - `roadmap_issue_number`: integer. Set this to {issue.number}.
  - `version`: integer. For the initial roadmap, set this to `1`.
  - `nodes`: non-empty array of node objects.
- Each item in `graph_json.nodes` must be an object with exactly:
  - `node_id`: non-empty string. Use a short stable internal identifier.
  - `kind`: one of `design_doc`, `small_job`, `roadmap`.
  - `title`: non-empty string for the child issue title.
  - `body_markdown`: non-empty string for the child issue body.
  - `depends_on`: array of dependency objects, using `[]` when there are no dependencies.
- Each item in `depends_on` must be an object with exactly:
  - `node_id`: string referencing another node in this same graph.
  - `requires`: one of `planned`, `implemented`.
- Do not add extra keys outside this schema.

Target file paths:
- roadmap markdown: {markdown_path}
- roadmap graph: {graph_path}

Issue title:
{issue.title}

Issue URL:
{issue.html_url}

Issue body:
{issue.body}
""".strip()


def build_roadmap_adjustment_prompt(
    *,
    issue: Issue,
    repo_full_name: str,
    default_branch: str,
    coding_guidelines_path: str | None,
    roadmap_doc_path: str,
    graph_path: str,
    graph_version: int,
    ready_node_ids: tuple[str, ...],
    dependency_artifacts: tuple[RoadmapDependencyArtifact, ...],
    roadmap_status_report: str,
    roadmap_markdown: str,
    canonical_graph_json: str,
) -> str:
    coding_guidelines_lines = _coding_guidelines_task_lines(
        coding_guidelines_path=coding_guidelines_path
    )
    ready_frontier_json = json.dumps(list(ready_node_ids), separators=(",", ":"))
    dependency_artifacts_json = _roadmap_dependency_artifacts_json(dependency_artifacts)
    return f"""
You are the roadmap-adjustment agent for repository {repo_full_name}.

Task:
- Evaluate whether roadmap issue #{issue.number} should proceed with its ready frontier or pause for a same-roadmap revision.
- Base branch is: {default_branch}
{coding_guidelines_lines}

Decision rules:
- Return `action = "proceed"` when the current roadmap still looks sound for the ready frontier.
- Return `action = "revise"` when the roadmap should change before issuing more child work.
- Return `action = "abandon"` only when continuing the roadmap is no longer viable.
- Prefer `proceed` unless the current roadmap is materially wrong.

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

Required output fields:
- `action`: one of `proceed`, `revise`, `abandon`
- `summary`: short rationale
- `details`: full rationale, referencing the ready frontier and the current roadmap state
- `updated_roadmap_markdown`: string or null
- `updated_graph_json`: object or null
- Always include all five top-level keys. Do not omit nullable keys; use null when a field does not apply.

Payload rules:
- When `action = "revise"`, set both `updated_roadmap_markdown` and `updated_graph_json`.
- When `action` is `proceed` or `abandon`, set both `updated_roadmap_markdown` and `updated_graph_json` to null.
- `updated_roadmap_markdown` must be the full revised markdown body for `{roadmap_doc_path}`.
- `updated_graph_json` must be a JSON object, not a string, with exactly:
  - `roadmap_issue_number`: integer. Keep this set to {issue.number}.
  - `version`: integer. Bump the graph version from {graph_version} to {graph_version + 1}.
  - `nodes`: non-empty array of node objects.
- Each item in `updated_graph_json.nodes` must be an object with exactly:
  - `node_id`: non-empty string.
  - `kind`: one of `design_doc`, `small_job`, `roadmap`.
  - `title`: non-empty string.
  - `body_markdown`: non-empty string.
  - `depends_on`: array of dependency objects, using `[]` when there are no dependencies.
- Each item in `depends_on` must be an object with exactly:
  - `node_id`: string referencing another node in this same graph.
  - `requires`: one of `planned`, `implemented`.
- The revised graph must stay acyclic and use only internal `node_id` references.
- Allowed node kinds remain `design_doc`, `small_job`, and `roadmap`.
- Include an explicit `depends_on` array on every revised node, using `[]` when there are no dependencies.
- Include an explicit `requires` on every revised dependency object.
- The revised graph must keep `roadmap_issue_number = {issue.number}` and bump the graph `version` from {graph_version} to {graph_version + 1}.
- Treat retained `node_id`s as stable work identities.
- If you keep an existing `node_id`, preserve its `kind`, `title`, `body_markdown`, and `depends_on` exactly.
- If pending work needs to change materially, retire the old pending node and add replacement work under a new `node_id`.
- Do not remove any node whose work has already started. Treat nodes with child issues, completed work, blocked work, abandoned work, or other in-flight progress as started.
- Do not churn `node_id`s for unchanged work. If the work is unchanged, keep the same `node_id`.
- Keep the revised graph internally consistent with the revised markdown narrative.

Current roadmap metadata:
- roadmap markdown path: {roadmap_doc_path}
- roadmap graph path: {graph_path}
- roadmap graph version: {graph_version}
- ready frontier node_ids: {ready_frontier_json}
- dependency artifacts JSON: {dependency_artifacts_json}

Roadmap issue title:
{issue.title}

Roadmap issue URL:
{issue.html_url}

Roadmap issue body:
{issue.body}

Current roadmap markdown ({roadmap_doc_path}):
{roadmap_markdown}

Current canonical roadmap graph JSON ({graph_path}):
{canonical_graph_json}

Current roadmap status report:
{roadmap_status_report}
""".strip()


def build_requested_roadmap_revision_prompt(
    *,
    issue: Issue,
    repo_full_name: str,
    default_branch: str,
    coding_guidelines_path: str | None,
    roadmap_doc_path: str,
    graph_path: str,
    graph_version: int,
    request_reason: str,
    roadmap_status_report: str,
    roadmap_markdown: str,
    canonical_graph_json: str,
) -> str:
    coding_guidelines_lines = _coding_guidelines_task_lines(
        coding_guidelines_path=coding_guidelines_path
    )
    return f"""
You are the roadmap-revision agent for repository {repo_full_name}.

Task:
- A same-roadmap revision has been explicitly requested for roadmap issue #{issue.number}.
- Base branch is: {default_branch}
{coding_guidelines_lines}

Decision rules:
- Return `action = "revise"` when you can author a revised roadmap safely.
- Return `action = "abandon"` only when the roadmap should be abandoned instead of revised.
- Do not return `action = "proceed"` for this task.

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

Required output fields:
- `action`: one of `revise`, `abandon`
- `summary`: short rationale
- `details`: full rationale for the requested revision
- `updated_roadmap_markdown`: string or null
- `updated_graph_json`: object or null
- Always include all five top-level keys. Do not omit nullable keys; use null when a field does not apply.

Payload rules:
- When `action = "revise"`, set both `updated_roadmap_markdown` and `updated_graph_json`.
- When `action = "abandon"`, set both `updated_roadmap_markdown` and `updated_graph_json` to null.
- `updated_roadmap_markdown` must be the full revised markdown body for `{roadmap_doc_path}`.
- `updated_graph_json` must be a JSON object, not a string, with exactly:
  - `roadmap_issue_number`: integer. Keep this set to {issue.number}.
  - `version`: integer. Bump the graph version from {graph_version} to {graph_version + 1}.
  - `nodes`: non-empty array of node objects.
- Each item in `updated_graph_json.nodes` must be an object with exactly:
  - `node_id`: non-empty string.
  - `kind`: one of `design_doc`, `small_job`, `roadmap`.
  - `title`: non-empty string.
  - `body_markdown`: non-empty string.
  - `depends_on`: array of dependency objects, using `[]` when there are no dependencies.
- Each item in `depends_on` must be an object with exactly:
  - `node_id`: string referencing another node in this same graph.
  - `requires`: one of `planned`, `implemented`.
- The revised graph must stay acyclic and use only internal `node_id` references.
- Allowed node kinds remain `design_doc`, `small_job`, and `roadmap`.
- Include an explicit `depends_on` array on every revised node, using `[]` when there are no dependencies.
- Include an explicit `requires` on every revised dependency object.
- The revised graph must keep `roadmap_issue_number = {issue.number}` and bump the graph `version` from {graph_version} to {graph_version + 1}.
- Treat retained `node_id`s as stable work identities.
- If you keep an existing `node_id`, preserve its `kind`, `title`, `body_markdown`, and `depends_on` exactly.
- If pending work needs to change materially, retire the old pending node and add replacement work under a new `node_id`.
- Do not remove any node whose work has already started. Treat nodes with child issues, completed work, blocked work, abandoned work, or other in-flight progress as started.
- Do not churn `node_id`s for unchanged work. If the work is unchanged, keep the same `node_id`.
- Keep the revised graph internally consistent with the revised markdown narrative.

Revision request reason:
{request_reason}

Current roadmap metadata:
- roadmap markdown path: {roadmap_doc_path}
- roadmap graph path: {graph_path}
- roadmap graph version: {graph_version}

Roadmap issue title:
{issue.title}

Roadmap issue URL:
{issue.html_url}

Roadmap issue body:
{issue.body}

Current roadmap markdown ({roadmap_doc_path}):
{roadmap_markdown}

Current canonical roadmap graph JSON ({graph_path}):
{canonical_graph_json}

Current roadmap status report:
{roadmap_status_report}
""".strip()


def build_feedback_prompt(*, turn: FeedbackTurn) -> str:
    review_comments = [
        {
            "review_comment_id": comment.comment_id,
            "path": comment.path,
            "line": comment.line,
            "side": comment.side,
            "in_reply_to_id": comment.in_reply_to_id,
            "body": comment.body,
            "user_login": comment.user_login,
            "html_url": comment.html_url,
        }
        for comment in turn.review_comments
    ]
    issue_comments = [
        {
            "comment_id": comment.comment_id,
            "body": comment.body,
            "user_login": comment.user_login,
            "html_url": comment.html_url,
        }
        for comment in turn.issue_comments
    ]
    changed_files = (
        "\n".join(f"- {path}" for path in turn.changed_files) or "- (no changed files reported)"
    )

    return f"""
You are the PR-feedback agent for repository issue #{turn.issue.number}.

Pull request:
- number: {turn.pull_request.number}
- title: {turn.pull_request.title}
- head_sha: {turn.pull_request.head_sha}
- base_sha: {turn.pull_request.base_sha}
- turn_key: {turn.turn_key}

Changed files:
{changed_files}

Review comments (line-level):
{json.dumps(review_comments, indent=2)}

Issue comments on the PR:
{json.dumps(issue_comments, indent=2)}

Return JSON only with this object shape:
{{
  "git_ops": [
    {{"op": "fetch_origin"}},
    {{"op": "merge_origin_default_branch"}}
  ],
  "review_replies": [
    {{"review_comment_id": 123, "body": "reply body"}}
  ],
  "general_comment": "optional summary comment for the PR",
  "commit_message": "optional commit message if code changes are needed",
  "escalation": {{
    "kind": "roadmap_revision",
    "summary": "short escalation summary",
    "details": "full escalation details with impacted assumptions"
  }},
  "flaky_test_report": {{
    "run_id": 123456789,
    "title": "meaningful flaky test issue title",
    "summary": "why this failure appears unrelated to current PR and how to reproduce",
    "relevant_log_excerpt": "direct CI log excerpt supporting flaky classification"
  }}
}}

{_feedback_output_contract_lines(allow_escalation=True)}

Rules:
- Primary objective: resolve review feedback by editing repository files, then provide commit_message.
- If CI failure context from GitHub Actions is present, reproduce those failures locally before finalizing.
- Repair code/tests until local required pre-push checks pass before returning commit_message.
- A non-null commit_message is the ready-to-push signal.
- If blocked, set commit_message to null and explain the blocker concretely in general_comment.
- For comments on design docs (for example `docs/design/*.md`), prefer updating the doc directly over explanatory discussion-only replies.
- If a comment can be addressed by a concrete file change, make that change in this turn.
- Reply to specific review comments using their exact review_comment_id.
- Do not invent IDs.
- Use review_replies to summarize what changed and where.
- review_replies may only target IDs listed under "Review comments (line-level)".
- Never put PR issue-thread comment IDs into review_replies.
- Synthetic MergeXO reminders may appear in issue_comments with negative IDs; never use those in review_replies.
- Use general_comment for PR issue-thread responses when a thread-level reply is needed.
- Set flaky_test_report only when the failure is an unrelated flaky CI test and you are confident.
- flaky_test_report must include run_id/title/summary/relevant_log_excerpt with concrete reproduction details.
- Keep the total JSON response compact and under 32000 characters.
- If you have more detail than that, prioritize actionable facts and summarize the rest.
- If flaky_test_report is non-null, commit_message MUST be null.
- If uncertain whether it is flaky, leave flaky_test_report as null and continue normal remediation.
- If you discover a foundational roadmap flaw, set escalation with kind=roadmap_revision.
- Allowed git_ops are exactly: `fetch_origin`, `merge_origin_default_branch`.
- If you need MergeXO to run one of those git operations (for example because of sandbox git metadata limits), request it via git_ops and set commit_message to null for that response.
- When git_ops are requested, do not post proposal-only review replies yet; wait for the follow-up turn with operation results and then implement/finalize.
- Never rewrite git history. Do not run: `git rebase`, `git commit --amend`, `git reset`, `git push --force`, `git push --force-with-lease`.
- Keep history append-only: only add new commits on top of the current PR head.
- If you think rewrite is necessary, do not request commit_message; explain the constraint in general_comment so a human can decide.
- If you provide commit_message, you MUST have edited repository files for this turn.
- Only skip file edits when blocked by genuine ambiguity or missing requirements; in that case, set commit_message to null and ask a precise clarifying question in the review reply.
- Do not claim you pushed or updated files unless you actually edited them in this turn.
- Do not post "proposed fix" text when you can implement the change directly.
- Use null or empty values only when no action is needed or genuinely blocked.
""".strip()


def build_roadmap_feedback_prompt(*, turn: RoadmapFeedbackTurn) -> str:
    review_comments = [
        {
            "review_comment_id": comment.comment_id,
            "path": comment.path,
            "line": comment.line,
            "side": comment.side,
            "in_reply_to_id": comment.in_reply_to_id,
            "body": comment.body,
            "user_login": comment.user_login,
            "html_url": comment.html_url,
        }
        for comment in turn.review_comments
    ]
    issue_comments = [
        {
            "comment_id": comment.comment_id,
            "body": comment.body,
            "user_login": comment.user_login,
            "html_url": comment.html_url,
        }
        for comment in turn.issue_comments
    ]
    changed_files = (
        "\n".join(f"- {path}" for path in turn.changed_files) or "- (no changed files reported)"
    )
    markdown_status = (
        "missing in the current checkout" if turn.roadmap_markdown_missing else "present"
    )
    if turn.graph_missing:
        graph_status = "missing in the current checkout"
    elif turn.graph_validation_error is None:
        graph_status = f"valid JSON graph for issue #{turn.issue.number}" + (
            f" at version {turn.graph_version}" if turn.graph_version is not None else ""
        )
    else:
        graph_status = f"invalid: {turn.graph_validation_error}"
    graph_version_line = (
        f"- Current graph version in this PR: `{turn.graph_version}`. Preserve that version unless a review comment explicitly requires correcting it."
        if turn.graph_version is not None
        else "- Current graph version in this PR could not be validated. Fix the graph and preserve the intended PR version if it is evident from review context."
    )

    return f"""
You are the roadmap-feedback agent for roadmap issue #{turn.issue.number}.

Pull request:
- number: {turn.pull_request.number}
- title: {turn.pull_request.title}
- head_sha: {turn.pull_request.head_sha}
- base_sha: {turn.pull_request.base_sha}
- turn_key: {turn.turn_key}

Roadmap artifacts under review:
- roadmap markdown path: {turn.roadmap_doc_path}
- roadmap graph path: {turn.graph_path}
- roadmap markdown status: {markdown_status}
- roadmap graph status: {graph_status}

Changed files:
{changed_files}

Review comments (line-level):
{json.dumps(review_comments, indent=2)}

Issue comments on the PR:
{json.dumps(issue_comments, indent=2)}

Current roadmap markdown ({turn.roadmap_doc_path}):
{turn.roadmap_markdown}

Current roadmap graph text ({turn.graph_path}):
{turn.graph_json_text}

Return JSON only with this object shape:
{{
  "git_ops": [
    {{"op": "fetch_origin"}},
    {{"op": "merge_origin_default_branch"}}
  ],
  "review_replies": [
    {{"review_comment_id": 123, "body": "reply body"}}
  ],
  "general_comment": "optional summary comment for the PR",
  "commit_message": "optional commit message if file changes are ready",
  "flaky_test_report": {{
    "run_id": 123456789,
    "title": "meaningful flaky test issue title",
    "summary": "why this failure appears unrelated to current PR and how to reproduce",
    "relevant_log_excerpt": "direct CI log excerpt supporting flaky classification"
  }}
}}

{_feedback_output_contract_lines(allow_escalation=False)}

Roadmap review rules:
- Primary objective: resolve roadmap review feedback by editing the roadmap markdown and roadmap graph in this PR, then provide commit_message.
- Treat `{turn.roadmap_doc_path}` and `{turn.graph_path}` as a paired artifact set. Keep them in sync.
- If you change roadmap strategy, sequencing, node scope, node titles, node bodies, or dependencies in one artifact, update the other artifact in the same turn.
- Do not return `escalation` for this task. This PR itself is the place to fix roadmap review feedback.
- Prefer concrete file edits over explanatory-only replies when a roadmap comment can be addressed directly.
- Before finalizing, re-read both roadmap artifacts even if the comment mentions only one of them.

Roadmap markdown expectations:
- The markdown should remain the full roadmap narrative body for `{turn.roadmap_doc_path}`.
- Keep it aligned with the machine graph.
- A useful roadmap markdown includes a short overview, sequencing notes, and a node-by-node breakdown keyed by `node_id`.

Roadmap graph contract:
- The graph file must be valid JSON and must describe the same plan as the markdown.
- `roadmap_issue_number` must be the integer issue number `{turn.issue.number}`.
{graph_version_line}
- `nodes` must be a non-empty array of node objects.
- Each node object must contain exactly:
  - `node_id`: non-empty string.
  - `kind`: one of `design_doc`, `small_job`, `roadmap`.
  - `title`: non-empty string.
  - `body_markdown`: non-empty string.
  - `depends_on`: array, using `[]` when there are no dependencies.
- Each dependency object in `depends_on` must contain exactly:
  - `node_id`: string referencing another node in the same graph.
  - `requires`: one of `planned`, `implemented`.
- Keep the graph acyclic and use only internal `node_id` references.
- Do not add extra keys outside this schema.

Review workflow rules:
- Reply to specific review comments using their exact review_comment_id.
- Do not invent IDs.
- Use review_replies to summarize what changed and where.
- review_replies may only target IDs listed under "Review comments (line-level)".
- Never put PR issue-thread comment IDs into review_replies.
- Synthetic MergeXO reminders may appear in issue_comments with negative IDs; never use those in review_replies.
- Use general_comment for PR issue-thread responses when a thread-level reply is needed.
- If CI failure context from GitHub Actions is present, reproduce those failures locally before finalizing.
- Repair the roadmap artifacts and any required tests until local required pre-push checks pass before returning commit_message.
- A non-null commit_message is the ready-to-push signal.
- If blocked, set commit_message to null and explain the blocker concretely in general_comment.
- Set flaky_test_report only when the failure is an unrelated flaky CI test and you are confident.
- flaky_test_report must include run_id/title/summary/relevant_log_excerpt with concrete reproduction details.
- If flaky_test_report is non-null, commit_message MUST be null.
- Allowed git_ops are exactly: `fetch_origin`, `merge_origin_default_branch`.
- If you need MergeXO to run one of those git operations, request it via git_ops and set commit_message to null for that response.
- When git_ops are requested, do not post proposal-only review replies yet; wait for the follow-up turn with operation results and then implement/finalize.
- Never rewrite git history. Do not run: `git rebase`, `git commit --amend`, `git reset`, `git push --force`, `git push --force-with-lease`.
- Keep history append-only: only add new commits on top of the current PR head.
- If you provide commit_message, you MUST have edited repository files for this turn.
- Do not claim you updated roadmap artifacts unless you actually edited them in this turn.
- Use null or empty values only when no action is needed or genuinely blocked.
""".strip()
