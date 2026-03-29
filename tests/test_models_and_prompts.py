from __future__ import annotations

import pytest

from mergexo.agent_adapter import (
    FeedbackTurn,
    RoadmapChildDependencyArtifact,
    RoadmapChildDependencyArtifactPath,
    RoadmapChildDependencyHandoff,
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
    _render_roadmap_child_dependency_handoff_prompt_section,
    _roadmap_child_dependency_handoff_from_payload,
    _serialize_roadmap_child_dependency_handoff,
    build_bugfix_prompt,
    build_design_prompt,
    build_feedback_prompt,
    build_implementation_prompt,
    build_requested_roadmap_revision_prompt,
    build_roadmap_adjustment_prompt,
    build_roadmap_feedback_prompt,
    build_roadmap_prompt,
    build_small_job_prompt,
    parse_roadmap_child_dependency_handoff_marker,
    render_roadmap_child_dependency_handoff_summary,
    render_roadmap_child_dependency_handoff_marker,
)


def _roadmap_child_handoff(
    *,
    review_notes: tuple[str, ...] = ("Accepted the interface constraint.",),
    issue_notes: tuple[str, ...] = ("Downstream worker must read the primary doc first.",),
    changed_files: tuple[str, ...] = ("docs/design/151-review-design.md",),
    supporting_artifact_paths: tuple[RoadmapChildDependencyArtifactPath, ...] = (),
    child_issue_body_excerpt: str | None = "Use the merged design as the primary input.",
    pr_body_excerpt: str | None = "Reference doc with accepted review constraints.",
) -> RoadmapChildDependencyHandoff:
    return RoadmapChildDependencyHandoff(
        schema_version=1,
        roadmap_issue_number=151,
        node_id="review_contract",
        node_kind="small_job",
        dependencies=(
            RoadmapChildDependencyArtifact(
                node_id="review_design",
                kind="reference_doc",
                title="Review design",
                requires="planned",
                satisfied_by="planned",
                child_issue_number=152,
                child_issue_url="https://example/issues/152",
                child_issue_title="Review design",
                pr_number=154,
                pr_url="https://example/pr/154",
                pr_title="Reference doc for review design",
                pr_merged=True,
                branch="agent/reference/152-review-design",
                head_sha="head-154",
                merge_commit_sha="merge-154",
                artifact_paths=(
                    RoadmapChildDependencyArtifactPath(
                        path="docs/design/151-review-design.md",
                        role="primary",
                        default_branch_url=(
                            "https://github.com/johnynek/mergexo/blob/main/"
                            "docs/design/151-review-design.md"
                        ),
                        blob_url=(
                            "https://github.com/johnynek/mergexo/blob/merge-154/"
                            "docs/design/151-review-design.md"
                        ),
                    ),
                    *supporting_artifact_paths,
                ),
                changed_files=changed_files,
                review_notes=review_notes,
                issue_notes=issue_notes,
                child_issue_body_excerpt=child_issue_body_excerpt,
                pr_body_excerpt=pr_body_excerpt,
            ),
        ),
    )


def _assert_roadmap_authoring_contract(prompt: str, *, include_per_kind_guidance: bool) -> None:
    assert "Roadmap dependency handoff contract:" in prompt
    assert (
        "Downstream workers receive direct dependency artifacts and satisfied dependency "
        "states only, not the transitive closure of the roadmap graph." in prompt
    )
    assert (
        "The same direct dependency handoff appears in both the child issue body and the "
        "worker prompt." in prompt
    )
    assert (
        "If a downstream worker needs another upstream artifact or satisfied upstream state, "
        "add an explicit direct dependency edge to that node." in prompt
    )
    assert (
        "Choose `planned` or `implemented` based on the earliest milestone at which MergeXO "
        "can truthfully hand the downstream worker the concrete direct artifact or "
        "satisfied state it needs." in prompt
    )
    assert (
        "`review_config` and `review_contract` should each depend directly on "
        "`review_design` with `requires = planned`" in prompt
    )
    if include_per_kind_guidance:
        assert (
            "`reference_doc`: downstream workers receive the merged doc artifact with its "
            "exact path and provenance." in prompt
        )
        assert (
            "`design_doc`: `planned` hands off the merged design artifact path and design PR "
            "provenance; `implemented` waits for the shipped implementation outcome from "
            "that same child issue." in prompt
        )
        assert (
            "`small_job`: `planned` means the child issue and its issue text exist; merged "
            "code, changed files, and implementation behavior are not available until "
            "`implemented`." in prompt
        )
        assert (
            "`roadmap`: `planned` hands off the child roadmap markdown and graph artifact "
            "paths plus activation provenance; `implemented` means the child roadmap has "
            "completed." in prompt
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
    assert snapshot.merge_commit_sha is None
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

    _assert_roadmap_authoring_contract(prompt, include_per_kind_guidance=False)
    assert "roadmap-feedback agent" in prompt
    assert "docs/roadmap/22-roadmap.md" in prompt
    assert "docs/roadmap/22-roadmap.graph.json" in prompt
    assert "Do not hand-edit `docs/roadmap/22-roadmap.md`" in prompt
    assert (
        "MergeXO will regenerate `docs/roadmap/22-roadmap.md` before validation and push" in prompt
    )
    assert "python roadmap_json_to_md.py docs/roadmap/22-roadmap.graph.json" in prompt
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
    assert "`kind`: one of `reference_doc`, `design_doc`, `small_job`, `roadmap`." in prompt
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

    _assert_roadmap_authoring_contract(prompt, include_per_kind_guidance=True)
    assert "roadmap-adjustment agent" in prompt
    assert '`action = "proceed"`' in prompt
    assert '`action = "revise"`' in prompt
    assert '`action = "abandon"`' in prompt
    assert "`updated_graph_json`" in prompt
    assert "Always include all four top-level keys." in prompt
    assert "`updated_graph_json` must be a JSON object, not a string, with exactly" in prompt
    assert "`roadmap_issue_number`: integer. Keep this set to 22." in prompt
    assert "`version`: integer. Bump the graph version from 3 to 4." in prompt
    assert "Each item in `updated_graph_json.nodes` must be an object with exactly" in prompt
    assert "bump the graph `version` from 3 to 4" in prompt
    assert "Treat retained `node_id`s as stable work identities" in prompt
    assert (
        "Preserve `kind` too, except you may change an unstarted node between `design_doc` and `reference_doc`"
        in prompt
    )
    assert "Do not remove any node whose work has already started" in prompt
    assert "Do not churn `node_id`s for unchanged work" in prompt
    assert "MergeXO will derive `docs/roadmap/22-roadmap.md` from the revised graph" in prompt
    assert "python roadmap_json_to_md.py docs/roadmap/22-roadmap.graph.json" in prompt
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

    _assert_roadmap_authoring_contract(prompt, include_per_kind_guidance=True)
    assert "roadmap-revision agent" in prompt
    assert 'Do not return `action = "proceed"`' in prompt
    assert "`action`: one of `revise`, `abandon`" in prompt
    assert "`action`: one of `proceed`, `revise`, `abandon`" not in prompt
    assert "Always include all four top-level keys." in prompt
    assert "`updated_graph_json` must be a JSON object, not a string, with exactly" in prompt
    assert "`roadmap_issue_number`: integer. Keep this set to 23." in prompt
    assert "`version`: integer. Bump the graph version from 5 to 6." in prompt
    assert "Treat retained `node_id`s as stable work identities" in prompt
    assert (
        "Preserve `kind` too, except you may change an unstarted node between `design_doc` and `reference_doc`"
        in prompt
    )
    assert "Do not remove any node whose work has already started" in prompt
    assert "Do not churn `node_id`s for unchanged work" in prompt
    assert "MergeXO will derive `docs/roadmap/23-roadmap.md` from the revised graph" in prompt
    assert "python roadmap_json_to_md.py docs/roadmap/23-roadmap.graph.json" in prompt
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

    _assert_roadmap_authoring_contract(prompt, include_per_kind_guidance=True)
    assert "roadmap agent" in prompt
    assert "graph_json" in prompt
    assert "docs/roadmap/42-<slug>.graph.json" in prompt
    assert "reference_doc" in prompt
    assert "design_doc" in prompt
    assert "small_job" in prompt
    assert "roadmap" in prompt
    assert "This turn is for roadmap authoring, not repository code implementation" in prompt
    assert "Produce a practical execution plan" in prompt
    assert (
        "MergeXO will generate the review markdown from that graph before opening the PR" in prompt
    )
    assert (
        "The generated markdown is a deterministic review view derived from `graph_json`" in prompt
    )
    assert "python roadmap_json_to_md.py docs/roadmap/42-<slug>.graph.json" in prompt
    assert "Always include `title`, `summary`, and `graph_json`." in prompt
    assert "Per-kind handoff guidance:" in prompt
    assert "Choose short, unique, stable `node_id` values" in prompt
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


def test_roadmap_child_dependency_handoff_marker_round_trips_and_truncates() -> None:
    handoff = _roadmap_child_handoff(
        review_notes=("r" * 450, "keep this note", "drop this note", "drop this too"),
        issue_notes=("i" * 450, "keep issue note", "drop issue note"),
        changed_files=tuple(f"src/file_{idx:02d}.py" for idx in range(30)),
        supporting_artifact_paths=tuple(
            RoadmapChildDependencyArtifactPath(
                path=f"docs/design/support-{idx:02d}.md",
                role="supporting",
                default_branch_url=(
                    "https://github.com/johnynek/mergexo/blob/main/"
                    f"docs/design/support-{idx:02d}.md"
                ),
                blob_url=(
                    "https://github.com/johnynek/mergexo/blob/merge-154/"
                    f"docs/design/support-{idx:02d}.md"
                ),
            )
            for idx in range(12)
        ),
        child_issue_body_excerpt="body " + ("x" * 2000),
        pr_body_excerpt="pr " + ("y" * 2000),
    )

    marker = render_roadmap_child_dependency_handoff_marker(handoff)
    parsed = parse_roadmap_child_dependency_handoff_marker(marker)

    assert parsed is not None
    assert parsed.schema_version == 1
    assert parsed.roadmap_issue_number == 151
    dependency = parsed.dependencies[0]
    assert dependency.node_id == "review_design"
    assert len(dependency.changed_files) == 25
    assert len(dependency.review_notes) == 3
    assert len(dependency.issue_notes) == 3
    assert dependency.review_notes[0].endswith("... [truncated]")
    assert dependency.issue_notes[0].endswith("... [truncated]")
    assert dependency.child_issue_body_excerpt is not None
    assert dependency.child_issue_body_excerpt.endswith("... [truncated]")
    assert dependency.pr_body_excerpt is not None
    assert dependency.pr_body_excerpt.endswith("... [truncated]")
    assert len(dependency.artifact_paths) == 10
    assert dependency.artifact_paths[0].role == "primary"
    assert all(path.role == "supporting" for path in dependency.artifact_paths[1:])


def test_prompt_builders_render_dependency_handoff_from_issue_body() -> None:
    marker = render_roadmap_child_dependency_handoff_marker(_roadmap_child_handoff())
    issue_body = (
        "<!-- mergexo-roadmap-node-kind:small_job -->\n\n"
        f"{marker}\n\n"
        "Dependency handoff:\n"
        "- visible summary\n\n"
        "Implement the next child node."
    )

    design_prompt = build_design_prompt(
        issue=Issue(
            number=152,
            title="Review config",
            body=issue_body,
            html_url="https://example/issues/152",
            labels=("agent:design",),
        ),
        repo_full_name="johnynek/mergexo",
        design_doc_path="docs/design/152-review-config.md",
        default_branch="main",
    )
    small_job_prompt = build_small_job_prompt(
        issue=Issue(
            number=152,
            title="Review config",
            body=issue_body,
            html_url="https://example/issues/152",
            labels=("agent:small-job",),
        ),
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
    )
    roadmap_prompt = build_roadmap_prompt(
        issue=Issue(
            number=152,
            title="Review config",
            body=issue_body,
            html_url="https://example/issues/152",
            labels=("agent:roadmap",),
        ),
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        roadmap_docs_dir="docs/roadmap",
        recommended_node_count=7,
        coding_guidelines_path="docs/python_style.md",
    )
    implementation_prompt = build_implementation_prompt(
        issue=Issue(
            number=152,
            title="Review config",
            body=issue_body,
            html_url="https://example/issues/152",
            labels=("agent:design",),
        ),
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
        design_doc_path="docs/design/152-review-config.md",
        design_doc_markdown="# Design",
        design_pr_number=154,
        design_pr_url="https://example/pr/154",
    )

    for prompt in (design_prompt, small_job_prompt, roadmap_prompt, implementation_prompt):
        assert "Dependency Handoff:" in prompt
        assert "Direct dependencies only." in prompt
        assert "review_design [reference_doc] requires=planned satisfied_by=planned" in prompt
        assert "docs/design/151-review-design.md" in prompt
        assert "visible summary" in prompt
        assert "Implement the next child node." in prompt
        assert "mergexo-roadmap-dependency-handoff" not in prompt


def test_serialize_roadmap_child_dependency_handoff_rejects_bad_schema_version() -> None:
    with pytest.raises(RuntimeError, match="Unsupported roadmap dependency handoff schema version"):
        _serialize_roadmap_child_dependency_handoff(
            RoadmapChildDependencyHandoff(
                schema_version=2,
                roadmap_issue_number=151,
                node_id="review_contract",
                node_kind="small_job",
                dependencies=(),
            )
        )


def test_serialize_roadmap_child_dependency_handoff_rejects_oversized_payload() -> None:
    oversized_handoff = RoadmapChildDependencyHandoff(
        schema_version=1,
        roadmap_issue_number=151,
        node_id="review_contract",
        node_kind="small_job",
        dependencies=tuple(
            RoadmapChildDependencyArtifact(
                node_id=f"review_design_{idx:02d}_{'x' * 150}",
                kind="reference_doc",
                title="Dependency " + ("y" * 200),
                requires="planned",
                satisfied_by="planned",
                child_issue_number=152 + idx,
                child_issue_url=f"https://example/issues/{152 + idx}",
                child_issue_title="Review design " + ("z" * 120),
                pr_number=300 + idx,
                pr_url=f"https://example/pr/{300 + idx}",
                pr_title="Reference doc " + ("w" * 120),
                pr_merged=True,
                branch="agent/reference/" + ("b" * 120),
                head_sha="head-" + ("h" * 120),
                merge_commit_sha="merge-" + ("m" * 120),
                artifact_paths=(
                    RoadmapChildDependencyArtifactPath(
                        path=f"docs/design/{idx:02d}-review.md",
                        role="primary",
                        default_branch_url=(
                            "https://github.com/johnynek/mergexo/blob/main/"
                            f"docs/design/{idx:02d}-review.md"
                        ),
                        blob_url=(
                            "https://github.com/johnynek/mergexo/blob/merge/"
                            f"docs/design/{idx:02d}-review.md"
                        ),
                    ),
                ),
                changed_files=(),
                review_notes=(),
                issue_notes=(),
                child_issue_body_excerpt=None,
                pr_body_excerpt=None,
            )
            for idx in range(20)
        ),
    )

    with pytest.raises(RuntimeError, match="payload exceeds size limit"):
        _serialize_roadmap_child_dependency_handoff(oversized_handoff)


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ("not-an-object", "payload must be an object"),
        (
            {
                "schema_version": 2,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [],
            },
            "unsupported roadmap dependency handoff schema version",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": "151",
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [],
            },
            "roadmap_issue_number must be an integer",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": 1,
                "node_kind": "small_job",
                "dependencies": [],
            },
            "node_id must be a string",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": 9,
                "dependencies": [],
            },
            "node_kind must be a string",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": "bad",
            },
            "dependencies must be a list",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": ["bad"],
            },
            "dependency entry must be an object",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [{"artifact_paths": "bad"}],
            },
            "artifact_paths must be a list",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [{"artifact_paths": ["bad"]}],
            },
            "artifact path entry must be an object",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [
                            {
                                "path": 1,
                                "role": "primary",
                                "default_branch_url": "u",
                                "blob_url": None,
                            }
                        ]
                    }
                ],
            },
            "artifact path must be a string",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [
                            {"path": "p", "role": 1, "default_branch_url": "u", "blob_url": None}
                        ]
                    }
                ],
            },
            "artifact role must be a string",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [
                            {
                                "path": "p",
                                "role": "primary",
                                "default_branch_url": 1,
                                "blob_url": None,
                            }
                        ]
                    }
                ],
            },
            "default_branch_url must be a string",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [
                            {
                                "path": "p",
                                "role": "primary",
                                "default_branch_url": "u",
                                "blob_url": 9,
                            }
                        ]
                    }
                ],
            },
            "blob_url must be a string or null",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [
                            {
                                "path": "p",
                                "role": "tertiary",
                                "default_branch_url": "u",
                                "blob_url": None,
                            }
                        ]
                    }
                ],
            },
            "artifact role must be primary or supporting",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [],
                        "changed_files": [1],
                        "review_notes": [],
                        "issue_notes": [],
                        "node_id": "d1",
                        "kind": "small_job",
                        "title": "t",
                        "requires": "planned",
                        "satisfied_by": "planned",
                    }
                ],
            },
            "changed_files must be a string list",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [],
                        "changed_files": [],
                        "review_notes": [1],
                        "issue_notes": [],
                        "node_id": "d1",
                        "kind": "small_job",
                        "title": "t",
                        "requires": "planned",
                        "satisfied_by": "planned",
                    }
                ],
            },
            "review_notes must be a string list",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [],
                        "changed_files": [],
                        "review_notes": [],
                        "issue_notes": [1],
                        "node_id": "d1",
                        "kind": "small_job",
                        "title": "t",
                        "requires": "planned",
                        "satisfied_by": "planned",
                    }
                ],
            },
            "issue_notes must be a string list",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [],
                        "changed_files": [],
                        "review_notes": [],
                        "issue_notes": [],
                        "node_id": "d1",
                        "kind": "small_job",
                        "title": "t",
                        "requires": "planned",
                        "satisfied_by": "planned",
                        "child_issue_url": 1,
                    }
                ],
            },
            "child_issue_url must be a string or null",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [],
                        "changed_files": [],
                        "review_notes": [],
                        "issue_notes": [],
                        "node_id": "d1",
                        "kind": "small_job",
                        "title": "t",
                        "requires": "planned",
                        "satisfied_by": "planned",
                        "child_issue_number": "1",
                    }
                ],
            },
            "child_issue_number must be an integer or null",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [],
                        "changed_files": [],
                        "review_notes": [],
                        "issue_notes": [],
                        "node_id": "d1",
                        "kind": "small_job",
                        "title": "t",
                        "requires": "planned",
                        "satisfied_by": "planned",
                        "pr_merged": "yes",
                    }
                ],
            },
            "pr_merged must be a boolean or null",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [],
                        "changed_files": [],
                        "review_notes": [],
                        "issue_notes": [],
                        "node_id": 1,
                        "kind": "small_job",
                        "title": "t",
                        "requires": "planned",
                        "satisfied_by": "planned",
                    }
                ],
            },
            "dependency node_id must be a string",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [],
                        "changed_files": [],
                        "review_notes": [],
                        "issue_notes": [],
                        "node_id": "d1",
                        "kind": 9,
                        "title": "t",
                        "requires": "planned",
                        "satisfied_by": "planned",
                    }
                ],
            },
            "dependency kind must be a string",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [],
                        "changed_files": [],
                        "review_notes": [],
                        "issue_notes": [],
                        "node_id": "d1",
                        "kind": "small_job",
                        "title": 9,
                        "requires": "planned",
                        "satisfied_by": "planned",
                    }
                ],
            },
            "dependency title must be a string",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [],
                        "changed_files": [],
                        "review_notes": [],
                        "issue_notes": [],
                        "node_id": "d1",
                        "kind": "small_job",
                        "title": "t",
                        "requires": 9,
                        "satisfied_by": "planned",
                    }
                ],
            },
            "dependency requires must be a string",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [],
                        "changed_files": [],
                        "review_notes": [],
                        "issue_notes": [],
                        "node_id": "d1",
                        "kind": "small_job",
                        "title": "t",
                        "requires": "planned",
                        "satisfied_by": 9,
                    }
                ],
            },
            "dependency satisfied_by must be a string",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [],
                        "changed_files": [],
                        "review_notes": [],
                        "issue_notes": [],
                        "node_id": "d1",
                        "kind": "unknown",
                        "title": "t",
                        "requires": "planned",
                        "satisfied_by": "planned",
                    }
                ],
            },
            "dependency kind is unsupported",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [],
                        "changed_files": [],
                        "review_notes": [],
                        "issue_notes": [],
                        "node_id": "d1",
                        "kind": "small_job",
                        "title": "t",
                        "requires": "reviewed",
                        "satisfied_by": "planned",
                    }
                ],
            },
            "dependency requires is unsupported",
        ),
        (
            {
                "schema_version": 1,
                "roadmap_issue_number": 151,
                "node_id": "n1",
                "node_kind": "small_job",
                "dependencies": [
                    {
                        "artifact_paths": [],
                        "changed_files": [],
                        "review_notes": [],
                        "issue_notes": [],
                        "node_id": "d1",
                        "kind": "small_job",
                        "title": "t",
                        "requires": "planned",
                        "satisfied_by": "reviewed",
                    }
                ],
            },
            "dependency satisfied_by is unsupported",
        ),
    ],
)
def test_roadmap_child_dependency_handoff_from_payload_validation_errors(
    payload: object, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _roadmap_child_dependency_handoff_from_payload(payload)


def test_parse_roadmap_child_dependency_handoff_marker_handles_invalid_markers() -> None:
    valid_payload = _serialize_roadmap_child_dependency_handoff(_roadmap_child_handoff())
    unsupported_marker = (
        "<!-- mergexo-roadmap-dependency-handoff:v2:"
        f"{render_roadmap_child_dependency_handoff_marker(_roadmap_child_handoff()).split(':', 2)[2]}"
    )
    invalid_payload_marker = "<!-- mergexo-roadmap-dependency-handoff:v1:not-base64 -->"
    invalid_json_marker = (
        "<!-- mergexo-roadmap-dependency-handoff:v1:" + "bm90LWpzb24".rstrip("=") + " -->"
    )

    assert parse_roadmap_child_dependency_handoff_marker(unsupported_marker) is None
    assert parse_roadmap_child_dependency_handoff_marker(invalid_payload_marker) is None
    assert (
        parse_roadmap_child_dependency_handoff_marker(
            "<!-- mergexo-roadmap-dependency-handoff:v1:" + "bm90IGpzb24=".rstrip("=") + " -->"
        )
        is None
    )
    assert valid_payload
    assert (
        parse_roadmap_child_dependency_handoff_marker(
            "<!-- mergexo-roadmap-dependency-handoff:v1:"
            + "eyJub3QiOiAianNvbiJ9".rstrip("=")
            + " -->"
        )
        is None
    )


def test_roadmap_child_dependency_handoff_summary_and_prompt_cover_optional_sections() -> None:
    handoff = _roadmap_child_handoff(
        supporting_artifact_paths=(
            RoadmapChildDependencyArtifactPath(
                path="docs/design/supporting.md",
                role="supporting",
                default_branch_url="https://example/supporting",
                blob_url="https://example/blob/supporting",
            ),
        ),
        changed_files=("src/review_contract.py",),
        issue_notes=("Track rollout ordering.",),
    )
    summary = render_roadmap_child_dependency_handoff_summary(handoff)
    assert "supporting artifacts: docs/design/supporting.md" in summary
    assert "changed files: src/review_contract.py" in summary
    assert "issue notes: Track rollout ordering." in summary

    empty_handoff = RoadmapChildDependencyHandoff(
        schema_version=1,
        roadmap_issue_number=151,
        node_id="review_contract",
        node_kind="small_job",
        dependencies=(),
    )
    assert render_roadmap_child_dependency_handoff_summary(empty_handoff) == (
        "Dependency handoff:\n- none"
    )
    assert "Dependency artifacts: none" in _render_roadmap_child_dependency_handoff_prompt_section(
        empty_handoff
    )


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
