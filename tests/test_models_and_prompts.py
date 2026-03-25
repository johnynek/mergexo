from __future__ import annotations

from mergexo.agent_adapter import (
    FeedbackTurn,
    RoadmapDependencyArtifact,
    RoadmapDependencyReference,
    RoadmapFeedbackTurn,
)
from mergexo import __version__
from mergexo.models import (
    FlakyTestReport,
    GeneratedDesign,
    GeneratedRoadmap,
    Issue,
    OperatorCommandRecord,
    PullRequest,
    PullRequestIssueComment,
    PullRequestReviewComment,
    PullRequestSnapshot,
    RoadmapDependency,
    RoadmapNode,
    RoadmapRevisionEscalation,
    RuntimeOperationRecord,
    WorkflowJobSnapshot,
    WorkflowRunSnapshot,
    WorkResult,
)
from mergexo.prompts import (
    build_bugfix_prompt,
    build_design_prompt,
    build_feedback_prompt,
    build_implementation_prompt,
    build_requested_roadmap_revision_prompt,
    build_roadmap_adjustment_prompt,
    build_roadmap_feedback_prompt,
    build_roadmap_prompt,
    build_small_job_prompt,
)


def test_model_dataclasses_and_version() -> None:
    issue = Issue(number=1, title="t", body="b", html_url="u", labels=("x",))
    pr = PullRequest(number=2, html_url="pr")
    snapshot = PullRequestSnapshot(
        number=2,
        title="pr",
        body="desc",
        head_sha="h",
        base_sha="b",
        draft=False,
        state="open",
        merged=False,
    )
    review_comment = PullRequestReviewComment(
        comment_id=1,
        body="comment",
        path="src/a.py",
        line=7,
        side="RIGHT",
        in_reply_to_id=None,
        user_login="dev",
        html_url="u",
        created_at="t1",
        updated_at="t2",
    )
    issue_comment = PullRequestIssueComment(
        comment_id=2,
        body="note",
        user_login="dev",
        html_url="u",
        created_at="t1",
        updated_at="t2",
    )
    gen = GeneratedDesign(
        title="Title", design_doc_markdown="# Doc", touch_paths=("a.py",), summary="sum"
    )
    roadmap = GeneratedRoadmap(
        title="Roadmap",
        summary="Summary",
        roadmap_markdown="# Roadmap",
        roadmap_issue_number=1,
        version=1,
        graph_nodes=(
            RoadmapNode(
                node_id="n1",
                kind="small_job",
                title="Ship",
                body_markdown="Implement",
                depends_on=(RoadmapDependency(node_id="n0", requires="planned"),),
            ),
        ),
        canonical_graph_json="{}",
    )
    escalation = RoadmapRevisionEscalation(
        kind="roadmap_revision",
        summary="Assumption failed",
        details="Need to revise dependency ordering.",
    )
    result = WorkResult(issue_number=1, branch="b", pr_number=2, pr_url="u")
    operator_command = OperatorCommandRecord(
        command_key="77:1:2026-02-22T00:00:00Z",
        issue_number=77,
        pr_number=None,
        comment_id=1,
        author_login="alice",
        command="restart",
        args_json="{}",
        status="applied",
        result="ok",
        created_at="t1",
        updated_at="t2",
    )
    runtime_op = RuntimeOperationRecord(
        op_name="restart",
        status="pending",
        requested_by="alice",
        request_command_key="77:1:2026-02-22T00:00:00Z",
        mode="git_checkout",
        detail=None,
        created_at="t1",
        updated_at="t2",
    )
    workflow_run = WorkflowRunSnapshot(
        run_id=11,
        name="ci",
        status="completed",
        conclusion="failure",
        html_url="https://example/runs/11",
        head_sha="abc123",
        created_at="t1",
        updated_at="t2",
    )
    workflow_job = WorkflowJobSnapshot(
        job_id=21,
        name="tests",
        status="completed",
        conclusion="failure",
        html_url="https://example/jobs/21",
    )
    flaky_report = FlakyTestReport(
        run_id=777,
        title="Flaky scheduler shard",
        summary="Intermittent timeout in shard 2.",
        relevant_log_excerpt="TimeoutError: queue did not drain",
    )

    assert __version__ == "0.1.0"
    assert issue.labels == ("x",)
    assert pr.number == 2
    assert snapshot.head_sha == "h"
    assert review_comment.path == "src/a.py"
    assert issue_comment.body == "note"
    assert gen.touch_paths == ("a.py",)
    assert roadmap.graph_nodes[0].depends_on[0].requires == "planned"
    assert escalation.kind == "roadmap_revision"
    assert result.branch == "b"
    assert operator_command.command == "restart"
    assert runtime_op.mode == "git_checkout"
    assert workflow_run.run_id == 11
    assert workflow_job.job_id == 21
    assert flaky_report.run_id == 777


def test_build_design_prompt_contains_required_contract() -> None:
    issue = Issue(
        number=12,
        title="Improve scheduler",
        body="Need to optimize queueing.",
        html_url="https://example/issue/12",
        labels=("agent:design",),
    )

    prompt = build_design_prompt(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        design_doc_path="docs/design/12-improve-scheduler.md",
        default_branch="main",
    )

    assert "You MUST report likely implementation file paths in touch_paths." in prompt
    assert "`design_doc_markdown` must contain only the body" in prompt
    assert "Do NOT include YAML frontmatter" in prompt
    assert "Return JSON only." in prompt
    assert "Always include `title`, `summary`, `touch_paths`, and `design_doc_markdown`." in prompt
    assert "Do not omit keys." in prompt
    assert "issue #12" in prompt.lower()
    assert "docs/design/12-improve-scheduler.md" in prompt


def test_build_feedback_prompt_contains_structured_sections() -> None:
    turn = FeedbackTurn(
        turn_key="turn-key-1",
        issue=Issue(number=9, title="t", body="b", html_url="u", labels=("x",)),
        pull_request=PullRequestSnapshot(
            number=5,
            title="PR",
            body="desc",
            head_sha="head",
            base_sha="base",
            draft=False,
            state="open",
            merged=False,
        ),
        review_comments=(
            PullRequestReviewComment(
                comment_id=11,
                body="Please rename",
                path="src/a.py",
                line=3,
                side="RIGHT",
                in_reply_to_id=None,
                user_login="reviewer",
                html_url="u",
                created_at="t1",
                updated_at="t2",
            ),
        ),
        issue_comments=(
            PullRequestIssueComment(
                comment_id=21,
                body="General note",
                user_login="reviewer",
                html_url="u2",
                created_at="t3",
                updated_at="t4",
            ),
        ),
        changed_files=("src/a.py",),
    )

    prompt = build_feedback_prompt(turn=turn)

    assert "review_comment_id" in prompt
    assert "issue comments on the pr" in prompt.lower()
    assert "src/a.py" in prompt
    assert "turn_key" in prompt
    assert "If you provide commit_message" in prompt
    assert "Primary objective: resolve review feedback by editing repository files" in prompt
    assert "For comments on design docs" in prompt
    assert "Only skip file edits when blocked by genuine ambiguity" in prompt
    assert "Never rewrite git history." in prompt
    assert "`git rebase`" in prompt
    assert "`git commit --amend`" in prompt
    assert "Keep history append-only" in prompt
    assert "If CI failure context from GitHub Actions is present" in prompt
    assert "A non-null commit_message is the ready-to-push signal." in prompt
    assert '"flaky_test_report"' in prompt
    assert '"relevant_log_excerpt"' in prompt
    assert "under 32000 characters" in prompt
    assert "If flaky_test_report is non-null, commit_message MUST be null." in prompt
    assert '"escalation"' in prompt
    assert (
        "Always include `git_ops`, `review_replies`, `general_comment`, `commit_message`, `flaky_test_report`, and `escalation`."
        in prompt
    )
    assert "Do not omit nullable fields. Use null when a field does not apply." in prompt


def test_build_roadmap_feedback_prompt_contains_graph_contract() -> None:
    turn = RoadmapFeedbackTurn(
        turn_key="turn-key-2",
        issue=Issue(
            number=22, title="Roadmap issue", body="Body", html_url="u", labels=("agent:roadmap",)
        ),
        pull_request=PullRequestSnapshot(
            number=12,
            title="Roadmap PR",
            body="desc",
            head_sha="head",
            base_sha="base",
            draft=False,
            state="open",
            merged=False,
        ),
        review_comments=(),
        issue_comments=(),
        changed_files=("docs/roadmap/22-roadmap.md", "docs/roadmap/22-roadmap.graph.json"),
        roadmap_doc_path="docs/roadmap/22-roadmap.md",
        graph_path="docs/roadmap/22-roadmap.graph.json",
        roadmap_markdown="# Overview\n\n- n1",
        graph_json_text='{"roadmap_issue_number":22,"version":1,"nodes":[]}',
        graph_version=1,
        roadmap_markdown_missing=False,
        graph_missing=False,
        graph_validation_error=None,
    )

    prompt = build_roadmap_feedback_prompt(turn=turn)

    assert "roadmap-feedback agent" in prompt
    assert "docs/roadmap/22-roadmap.md" in prompt
    assert "docs/roadmap/22-roadmap.graph.json" in prompt
    assert "Keep them in sync" in prompt
    assert "Do not return `escalation` for this task" in prompt
    assert (
        "Always include `git_ops`, `review_replies`, `general_comment`, `commit_message`, and `flaky_test_report`."
        in prompt
    )
    assert "`roadmap_issue_number` must be the integer issue number `22`" in prompt
    assert (
        "Preserve that version unless a review comment explicitly requires correcting it" in prompt
    )
    assert "`nodes` must be a non-empty array of node objects" in prompt
    assert "Each node object must contain exactly" in prompt
    assert "`kind`: one of `design_doc`, `small_job`, `roadmap`." in prompt
    assert "Each dependency object in `depends_on` must contain exactly" in prompt
    assert "`requires`: one of `planned`, `implemented`." in prompt
    assert "Do not add extra keys outside this schema." in prompt
    assert '"flaky_test_report"' in prompt


def test_build_roadmap_feedback_prompt_reports_invalid_graph_status() -> None:
    turn = RoadmapFeedbackTurn(
        turn_key="turn-key-3",
        issue=Issue(
            number=22, title="Roadmap issue", body="Body", html_url="u", labels=("agent:roadmap",)
        ),
        pull_request=PullRequestSnapshot(
            number=12,
            title="Roadmap PR",
            body="desc",
            head_sha="head",
            base_sha="base",
            draft=False,
            state="open",
            merged=False,
        ),
        review_comments=(),
        issue_comments=(),
        changed_files=("docs/roadmap/22-roadmap.md", "docs/roadmap/22-roadmap.graph.json"),
        roadmap_doc_path="docs/roadmap/22-roadmap.md",
        graph_path="docs/roadmap/22-roadmap.graph.json",
        roadmap_markdown="# Overview\n\n- n1",
        graph_json_text="{not valid json",
        graph_version=None,
        roadmap_markdown_missing=False,
        graph_missing=False,
        graph_validation_error="unexpected token at line 1",
    )

    prompt = build_roadmap_feedback_prompt(turn=turn)

    assert "- roadmap graph status: invalid: unexpected token at line 1" in prompt
    assert "current graph version in this pr could not be validated" in prompt.lower()


def test_build_roadmap_adjustment_prompt_contains_decision_contract() -> None:
    issue = Issue(
        number=22,
        title="Roadmap",
        body="Continue implementing the roadmap.",
        html_url="https://example/issue/22",
        labels=("agent:roadmap",),
    )

    prompt = build_roadmap_adjustment_prompt(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
        roadmap_doc_path="docs/roadmap/22-roadmap.md",
        graph_path="docs/roadmap/22-roadmap.graph.json",
        graph_version=3,
        ready_node_ids=("n2", "n3"),
        dependency_artifacts=(
            RoadmapDependencyArtifact(
                dependency_node_id="n1",
                dependency_kind="small_job",
                dependency_title="Dependency",
                frontier_references=(
                    RoadmapDependencyReference(ready_node_id="n2", requires="implemented"),
                    RoadmapDependencyReference(ready_node_id="n3", requires="planned"),
                ),
                child_issue_number=201,
                child_issue_url="https://example/issues/201",
                child_issue_title="Dependency issue",
                child_issue_body="Dependency issue body " + ("x" * 1300),
                issue_run_status="merged",
                issue_run_branch="agent/impl/201-dependency",
                issue_run_error="Needs follow-up " + ("e" * 700),
                resolution_markers=("issue_run_status=merged",),
                pr_number=301,
                pr_url="https://example/pr/301",
                pr_title="Dependency PR",
                pr_body="PR body " + ("p" * 1700),
                pr_state="closed",
                pr_merged=True,
                changed_files=("src/dep.py",),
                review_summaries=(),
                issue_comments=(
                    PullRequestIssueComment(
                        comment_id=1,
                        body="This changed the interface. " + ("c" * 700),
                        user_login="reviewer",
                        html_url="https://example/comment/1",
                        created_at="t1",
                        updated_at="t2",
                    ),
                ),
            ),
        ),
        roadmap_status_report="status report",
        roadmap_markdown="# Roadmap",
        canonical_graph_json='{"roadmap_issue_number":22}',
    )

    assert "roadmap-adjustment agent" in prompt
    assert '`action = "proceed"`' in prompt
    assert '`action = "revise"`' in prompt
    assert '`action = "abandon"`' in prompt
    assert "`updated_roadmap_markdown`" in prompt
    assert "`updated_graph_json`" in prompt
    assert "Always include all five top-level keys." in prompt
    assert "`updated_graph_json` must be a JSON object, not a string, with exactly" in prompt
    assert "`roadmap_issue_number`: integer. Keep this set to 22." in prompt
    assert "`version`: integer. Bump the graph version from 3 to 4." in prompt
    assert "Each item in `updated_graph_json.nodes` must be an object with exactly" in prompt
    assert "bump the graph `version` from 3 to 4" in prompt
    assert "Treat retained `node_id`s as stable work identities" in prompt
    assert "preserve its `kind`, `title`, `body_markdown`, and `depends_on` exactly" in prompt
    assert "Do not remove any node whose work has already started" in prompt
    assert "Do not churn `node_id`s for unchanged work" in prompt
    assert "ready frontier node_ids" in prompt
    assert '["n2","n3"]' in prompt
    assert '"dependency_node_id":"n1"' in prompt
    assert prompt.count('"dependency_node_id":"n1"') == 1
    assert '"pr_title":"Dependency PR"' in prompt
    assert "... [truncated]" in prompt
    assert "docs/roadmap/22-roadmap.graph.json" in prompt
    assert "status report" in prompt


def test_build_requested_roadmap_revision_prompt_contains_request_contract() -> None:
    issue = Issue(
        number=23,
        title="Revise roadmap",
        body="Please revise the roadmap.",
        html_url="https://example/issue/23",
        labels=("agent:roadmap", "agent:roadmap-revise"),
    )

    prompt = build_requested_roadmap_revision_prompt(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
        roadmap_doc_path="docs/roadmap/23-roadmap.md",
        graph_path="docs/roadmap/23-roadmap.graph.json",
        graph_version=5,
        request_reason="operator requested roadmap revision",
        roadmap_status_report="status report",
        roadmap_markdown="# Roadmap",
        canonical_graph_json='{"roadmap_issue_number":23}',
    )

    assert "roadmap-revision agent" in prompt
    assert 'Do not return `action = "proceed"`' in prompt
    assert "`action`: one of `revise`, `abandon`" in prompt
    assert "`action`: one of `proceed`, `revise`, `abandon`" not in prompt
    assert "Always include all five top-level keys." in prompt
    assert "`updated_graph_json` must be a JSON object, not a string, with exactly" in prompt
    assert "`roadmap_issue_number`: integer. Keep this set to 23." in prompt
    assert "`version`: integer. Bump the graph version from 5 to 6." in prompt
    assert "Treat retained `node_id`s as stable work identities" in prompt
    assert "Do not remove any node whose work has already started" in prompt
    assert "Do not churn `node_id`s for unchanged work" in prompt
    assert "operator requested roadmap revision" in prompt
    assert "docs/roadmap/23-roadmap.md" in prompt
    assert "bump the graph `version` from 5 to 6" in prompt


def test_build_bugfix_prompt_requires_regression_tests() -> None:
    issue = Issue(
        number=21,
        title="Fix flaky scheduler",
        body="Queue occasionally stalls after retries.",
        html_url="https://example/issue/21",
        labels=("agent:bugfix",),
    )

    prompt = build_bugfix_prompt(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
    )

    assert "regression tests that fail before the fix and pass after the fix" in prompt
    assert "docs/python_style.md" in prompt
    assert "blocked_reason" in prompt
    assert (
        "Always include `pr_title`, `pr_summary`, `commit_message`, `blocked_reason`, and `escalation`."
        in prompt
    )
    assert "Do not omit nullable keys. Use null when a field does not apply." in prompt
    assert "issue #21" in prompt.lower()


def test_build_bugfix_prompt_without_guidelines_path_uses_fallback() -> None:
    issue = Issue(
        number=210,
        title="Fix edge case",
        body="Handle sparse payloads.",
        html_url="https://example/issue/210",
        labels=("agent:bugfix",),
    )

    prompt = build_bugfix_prompt(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path=None,
    )

    assert "Coding/testing guidelines file is not present" in prompt
    assert "target 100% test coverage" in prompt
    assert "docs/python_style.md" not in prompt


def test_build_small_job_prompt_is_scoped() -> None:
    issue = Issue(
        number=22,
        title="Add docs index",
        body="Create docs/index.md and link from README.",
        html_url="https://example/issue/22",
        labels=("agent:small-job",),
    )

    prompt = build_small_job_prompt(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
    )

    assert "small-job agent" in prompt
    assert "docs/python_style.md" in prompt
    assert "blocked_reason" in prompt
    assert "Keep scope tight" in prompt
    assert (
        "Always include `pr_title`, `pr_summary`, `commit_message`, `blocked_reason`, and `escalation`."
        in prompt
    )


def test_build_roadmap_prompt_requires_graph_contract() -> None:
    issue = Issue(
        number=42,
        title="Multi-step platform migration",
        body="Need staged rollout across subsystems.",
        html_url="https://example/issue/42",
        labels=("agent:roadmap",),
    )

    prompt = build_roadmap_prompt(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        roadmap_docs_dir="docs/roadmap",
        recommended_node_count=7,
        coding_guidelines_path="docs/python_style.md",
    )

    assert "roadmap agent" in prompt
    assert "graph_json" in prompt
    assert "docs/roadmap/42-<slug>.graph.json" in prompt
    assert "design_doc" in prompt
    assert "small_job" in prompt
    assert "roadmap" in prompt
    assert "This turn is for roadmap authoring, not repository code implementation" in prompt
    assert "Produce a practical execution plan" in prompt
    assert "`roadmap_markdown` and `graph_json` must describe the same plan" in prompt
    assert "Always include `title`, `summary`, `roadmap_markdown`, and `graph_json`." in prompt
    assert (
        "`roadmap_markdown`: non-empty string containing the full roadmap markdown body" in prompt
    )
    assert "design_doc.planned" in prompt
    assert "small_job.implemented" in prompt
    assert "roadmap.planned" in prompt
    assert "Use `planned` only when downstream work can safely begin" in prompt
    assert "Choose short, unique, stable `node_id` values" in prompt
    assert "node-by-node breakdown keyed by `node_id`" in prompt
    assert "Recommended node count is around 7" in prompt
    assert "`graph_json` as a JSON object, not a string, with exactly" in prompt
    assert "`roadmap_issue_number`: integer. Set this to 42." in prompt
    assert "`version`: integer. For the initial roadmap, set this to `1`." in prompt
    assert "`nodes`: non-empty array of node objects." in prompt
    assert "Each item in `graph_json.nodes` must be an object with exactly" in prompt
    assert "`title`: non-empty string for the child issue title." in prompt
    assert "`body_markdown`: non-empty string for the child issue body." in prompt
    assert "Each item in `depends_on` must be an object with exactly" in prompt
    assert "`node_id`: string referencing another node in this same graph." in prompt
    assert "`requires`: one of `planned`, `implemented`." in prompt
    assert "Do not add extra keys outside this schema." in prompt


def test_build_implementation_prompt_links_design_context() -> None:
    issue = Issue(
        number=23,
        title="Ship worker pool scheduling",
        body="Implement the accepted design and rollout checks.",
        html_url="https://example/issue/23",
        labels=("agent:design",),
    )

    prompt = build_implementation_prompt(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
        design_doc_path="docs/design/23-ship-worker-pool.md",
        design_doc_markdown="# Design\n\nDo the thing.",
        design_pr_number=144,
        design_pr_url="https://example/pr/144",
    )

    assert "implementation agent" in prompt
    assert "docs/python_style.md" in prompt
    assert "docs/design/23-ship-worker-pool.md" in prompt
    assert "Source design PR: #144 (https://example/pr/144)" in prompt
    assert "blocked_reason" in prompt
    assert (
        "Always include `pr_title`, `pr_summary`, `commit_message`, `blocked_reason`, and `escalation`."
        in prompt
    )
    assert "Merged design doc markdown" in prompt
    assert "Re-read the full diff against main." in prompt
    assert "Add concise comments around subtle logic" in prompt
    assert "Re-run formatting and CI-required checks from docs/python_style.md." in prompt


def test_build_implementation_prompt_without_guidelines_path_uses_fallback() -> None:
    issue = Issue(
        number=231,
        title="Ship scheduler",
        body="Implement with tests.",
        html_url="https://example/issue/231",
        labels=("agent:design",),
    )

    prompt = build_implementation_prompt(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path=None,
        design_doc_path="docs/design/231-ship.md",
        design_doc_markdown="# Design",
        design_pr_number=200,
        design_pr_url="https://example/pr/200",
    )

    assert "Coding/testing guidelines file is not present" in prompt
    assert "target 100% test coverage" in prompt
    assert "Re-run formatting and tests expected by this repo" in prompt
    assert "docs/python_style.md" not in prompt
