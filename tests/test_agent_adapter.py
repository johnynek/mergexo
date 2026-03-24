from __future__ import annotations

from pathlib import Path

from mergexo.agent_adapter import (
    AgentAdapter,
    DirectStartResult,
    AgentSession,
    DesignStartResult,
    FeedbackResult,
    FeedbackTurn,
    GitOpRequest,
    RoadmapDependencyArtifact,
    RoadmapDependencyReference,
    RoadmapAdjustmentResult,
    RoadmapStartResult,
    ReviewReply,
)
from mergexo.models import (
    GeneratedDesign,
    GeneratedRoadmap,
    Issue,
    PullRequestIssueComment,
    PullRequestReviewComment,
    PullRequestSnapshot,
    RoadmapNode,
)


class DummyAdapter(AgentAdapter):
    def start_design_from_issue(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        design_doc_path: str,
        default_branch: str,
        cwd: Path,
    ) -> DesignStartResult:
        _ = issue, repo_full_name, design_doc_path, default_branch, cwd
        return DesignStartResult(
            design=GeneratedDesign(
                title="t",
                design_doc_markdown="doc",
                touch_paths=("x",),
                summary="s",
            ),
            session=AgentSession(adapter="dummy", thread_id="th"),
        )

    def respond_to_feedback(
        self,
        *,
        session: AgentSession,
        turn: FeedbackTurn,
        cwd: Path,
    ) -> FeedbackResult:
        _ = session, turn, cwd
        return FeedbackResult(
            session=AgentSession(adapter="dummy", thread_id="th"),
            review_replies=(ReviewReply(review_comment_id=1, body="ok"),),
            general_comment="done",
            commit_message="commit",
            git_ops=(GitOpRequest(op="fetch_origin"),),
        )

    def start_roadmap_from_issue(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        roadmap_docs_dir: str,
        recommended_node_count: int,
        cwd: Path,
    ) -> RoadmapStartResult:
        _ = (
            issue,
            repo_full_name,
            default_branch,
            roadmap_docs_dir,
            recommended_node_count,
            cwd,
        )
        return RoadmapStartResult(
            roadmap=GeneratedRoadmap(
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
                        body_markdown="Do it",
                    ),
                ),
                canonical_graph_json=(
                    '{"nodes":[{"body_markdown":"Do it","depends_on":[],"kind":"small_job",'
                    '"node_id":"n1","title":"Ship"}],"roadmap_issue_number":1,"version":1}'
                ),
            ),
            session=AgentSession(adapter="dummy", thread_id="th"),
        )

    def evaluate_roadmap_adjustment(
        self,
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
        cwd: Path,
    ) -> RoadmapAdjustmentResult:
        _ = (
            issue,
            repo_full_name,
            default_branch,
            coding_guidelines_path,
            roadmap_doc_path,
            graph_path,
            graph_version,
            ready_node_ids,
            dependency_artifacts,
            roadmap_status_report,
            roadmap_markdown,
            canonical_graph_json,
            cwd,
        )
        return RoadmapAdjustmentResult(
            action="proceed",
            summary="Proceed",
            details="The roadmap frontier can proceed.",
        )

    def start_bugfix_from_issue(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        coding_guidelines_path: str | None,
        cwd: Path,
    ) -> DirectStartResult:
        _ = issue, repo_full_name, default_branch, coding_guidelines_path, cwd
        return DirectStartResult(
            pr_title="Fix bug",
            pr_summary="Summary",
            commit_message="fix: bug",
            blocked_reason=None,
            session=AgentSession(adapter="dummy", thread_id="th"),
        )

    def start_small_job_from_issue(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        coding_guidelines_path: str | None,
        cwd: Path,
    ) -> DirectStartResult:
        _ = issue, repo_full_name, default_branch, coding_guidelines_path, cwd
        return DirectStartResult(
            pr_title="Small job",
            pr_summary="Summary",
            commit_message="feat: small",
            blocked_reason=None,
            session=AgentSession(adapter="dummy", thread_id="th"),
        )

    def start_implementation_from_design(
        self,
        *,
        issue: Issue,
        repo_full_name: str,
        default_branch: str,
        coding_guidelines_path: str | None,
        design_doc_path: str,
        design_doc_markdown: str,
        design_pr_number: int | None,
        design_pr_url: str | None,
        cwd: Path,
    ) -> DirectStartResult:
        _ = (
            issue,
            repo_full_name,
            default_branch,
            coding_guidelines_path,
            design_doc_path,
            design_doc_markdown,
            design_pr_number,
            design_pr_url,
            cwd,
        )
        return DirectStartResult(
            pr_title="Implement design",
            pr_summary="Summary",
            commit_message="feat: implementation",
            blocked_reason=None,
            session=AgentSession(adapter="dummy", thread_id="th"),
        )


def test_agent_adapter_data_model() -> None:
    issue = Issue(number=1, title="t", body="b", html_url="u", labels=("x",))
    turn = FeedbackTurn(
        turn_key="turn-1",
        issue=issue,
        pull_request=PullRequestSnapshot(
            number=2,
            title="pr",
            body="desc",
            head_sha="h",
            base_sha="b",
            draft=False,
            state="open",
            merged=False,
        ),
        review_comments=(
            PullRequestReviewComment(
                comment_id=1,
                body="line",
                path="src/a.py",
                line=1,
                side="RIGHT",
                in_reply_to_id=None,
                user_login="r",
                html_url="u",
                created_at="t1",
                updated_at="t2",
            ),
        ),
        issue_comments=(
            PullRequestIssueComment(
                comment_id=2,
                body="general",
                user_login="r",
                html_url="u2",
                created_at="t1",
                updated_at="t2",
            ),
        ),
        changed_files=("src/a.py",),
    )

    adapter = DummyAdapter()
    start = adapter.start_design_from_issue(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        design_doc_path="docs/design/1-t.md",
        default_branch="main",
        cwd=Path("."),
    )
    roadmap = adapter.start_roadmap_from_issue(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        roadmap_docs_dir="docs/roadmap",
        recommended_node_count=7,
        cwd=Path("."),
    )
    adjustment = adapter.evaluate_roadmap_adjustment(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
        roadmap_doc_path="docs/roadmap/1-t.md",
        graph_path="docs/roadmap/1-t.graph.json",
        graph_version=1,
        ready_node_ids=("n1",),
        dependency_artifacts=(
            RoadmapDependencyArtifact(
                dependency_node_id="d1",
                dependency_kind="small_job",
                dependency_title="Dependency",
                frontier_references=(
                    RoadmapDependencyReference(ready_node_id="n1", requires="implemented"),
                ),
                child_issue_number=10,
                child_issue_url="https://example/issues/10",
                child_issue_title="Dependency issue",
                child_issue_body="Dependency issue body",
                issue_run_status="merged",
                issue_run_branch="agent/impl/10-dependency",
                issue_run_error=None,
                resolution_markers=("issue_run_status=merged",),
                pr_number=11,
                pr_url="https://example/pr/11",
                pr_title="Dependency PR",
                pr_body="PR body",
                pr_state="closed",
                pr_merged=True,
                changed_files=("src/a.py",),
                review_summaries=(),
                issue_comments=(),
            ),
        ),
        roadmap_status_report="status",
        roadmap_markdown="# Roadmap",
        canonical_graph_json="{}",
        cwd=Path("."),
    )
    bugfix = adapter.start_bugfix_from_issue(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
        cwd=Path("."),
    )
    small_job = adapter.start_small_job_from_issue(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
        cwd=Path("."),
    )
    implementation = adapter.start_implementation_from_design(
        issue=issue,
        repo_full_name="johnynek/mergexo",
        default_branch="main",
        coding_guidelines_path="docs/python_style.md",
        design_doc_path="docs/design/1-t.md",
        design_doc_markdown="# Design",
        design_pr_number=44,
        design_pr_url="https://example/pr/44",
        cwd=Path("."),
    )
    feedback = adapter.respond_to_feedback(
        session=AgentSession(adapter="dummy", thread_id="th"),
        turn=turn,
        cwd=Path("."),
    )

    assert start.session is not None
    assert start.session.thread_id == "th"
    assert roadmap.roadmap.title == "Roadmap"
    assert adjustment.action == "proceed"
    assert bugfix.pr_title == "Fix bug"
    assert small_job.pr_title == "Small job"
    assert implementation.pr_title == "Implement design"
    assert feedback.review_replies[0].review_comment_id == 1
    assert feedback.git_ops[0].op == "fetch_origin"
