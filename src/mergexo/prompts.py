from __future__ import annotations

import json

from mergexo.agent_adapter import FeedbackTurn
from mergexo.models import Issue


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
) -> str:
    return f"""
You are the bugfix agent for repository {repo_full_name}.

Task:
- Resolve issue #{issue.number} directly with code changes.
- Base branch is: {default_branch}
- Reproduce the issue behavior from the report details.
- Add or update regression tests in tests/ that fail before the fix and pass after the fix.

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
) -> str:
    return f"""
You are the small-job agent for repository {repo_full_name}.

Task:
- Implement issue #{issue.number} directly with focused code changes.
- Base branch is: {default_branch}
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
  "review_replies": [
    {{"review_comment_id": 123, "body": "reply body"}}
  ],
  "general_comment": "optional summary comment for the PR",
  "commit_message": "optional commit message if code changes are needed"
}}

Rules:
- Reply to specific review comments using their exact review_comment_id.
- Do not invent IDs.
- If you provide commit_message, you MUST have edited repository files for this turn.
- Do not claim you pushed or updated files unless you actually edited them in this turn.
- Use null or empty values only when no action is needed.
""".strip()
