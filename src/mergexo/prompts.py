from __future__ import annotations

import json

from mergexo.agent_adapter import FeedbackTurn
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
- Create the full content for a design doc that addresses issue #{issue.number}.
- The resulting PR will add the file at: {design_doc_path}
- Base branch is: {default_branch}

Output requirements:
- Focus on architecture and implementation plan for the issue.
- Include clear acceptance criteria.
- Include risks and rollout notes.
- You MUST report likely implementation file paths in touch_paths.
- Keep touch_paths concrete and repository-relative.

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

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

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

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

Response format:
- Return JSON only.
- The response must satisfy the provided schema.
- Do not include markdown code fences in the JSON fields.

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

Issue title:
{issue.title}

Issue URL:
{issue.html_url}

Issue body:
{issue.body}

Merged design doc markdown ({design_doc_path}):
{design_doc_markdown}
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
  "commit_message": "optional commit message if code changes are needed"
}}

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
