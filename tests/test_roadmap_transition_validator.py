from __future__ import annotations

import pytest

from mergexo.models import RoadmapDependency, RoadmapNode
from mergexo.roadmap_parser import RoadmapGraph
from mergexo.roadmap_transition_validator import (
    ExistingRoadmapNodeState,
    RoadmapGraphTransitionError,
    validate_roadmap_graph_transition,
)


def test_validate_roadmap_graph_transition_allows_pending_node_retirement() -> None:
    transition = validate_roadmap_graph_transition(
        current_graph_version=1,
        current_nodes=(
            ExistingRoadmapNodeState(
                node_id="design",
                kind="design_doc",
                title="Design",
                body_markdown="Doc",
                depends_on=(),
                status="completed",
                child_issue_number=100,
                implemented_at="2026-03-01T00:00:00.000Z",
            ),
            ExistingRoadmapNodeState(
                node_id="impl",
                kind="small_job",
                title="Implement",
                body_markdown="Ship it",
                depends_on=(RoadmapDependency(node_id="design", requires="implemented"),),
                status="pending",
                child_issue_number=None,
                implemented_at=None,
            ),
        ),
        proposed_graph=RoadmapGraph(
            roadmap_issue_number=55,
            version=2,
            nodes=(
                RoadmapNode(
                    node_id="design",
                    kind="design_doc",
                    title="Design",
                    body_markdown="Doc",
                ),
                RoadmapNode(
                    node_id="verify",
                    kind="small_job",
                    title="Verify",
                    body_markdown="Check it",
                    depends_on=(RoadmapDependency(node_id="design", requires="implemented"),),
                ),
            ),
        ),
    )

    assert transition.retained_node_ids == ("design",)
    assert transition.added_node_ids == ("verify",)
    assert transition.retired_node_ids == ("impl",)


def test_validate_roadmap_graph_transition_rejects_removing_issued_node() -> None:
    with pytest.raises(RoadmapGraphTransitionError, match="cannot remove roadmap node"):
        validate_roadmap_graph_transition(
            current_graph_version=2,
            current_nodes=(
                ExistingRoadmapNodeState(
                    node_id="impl",
                    kind="small_job",
                    title="Implement",
                    body_markdown="Ship it",
                    depends_on=(),
                    status="issued",
                    child_issue_number=101,
                    implemented_at=None,
                ),
            ),
            proposed_graph=RoadmapGraph(
                roadmap_issue_number=55,
                version=3,
                nodes=(),
            ),
        )


def test_validate_roadmap_graph_transition_rejects_mutating_protected_dependencies() -> None:
    with pytest.raises(
        RoadmapGraphTransitionError, match="cannot change roadmap node dependencies"
    ):
        validate_roadmap_graph_transition(
            current_graph_version=3,
            current_nodes=(
                ExistingRoadmapNodeState(
                    node_id="impl",
                    kind="small_job",
                    title="Implement",
                    body_markdown="Ship it",
                    depends_on=(RoadmapDependency(node_id="design", requires="planned"),),
                    status="issued",
                    child_issue_number=101,
                    implemented_at=None,
                ),
            ),
            proposed_graph=RoadmapGraph(
                roadmap_issue_number=55,
                version=4,
                nodes=(
                    RoadmapNode(
                        node_id="impl",
                        kind="small_job",
                        title="Implement",
                        body_markdown="Ship it",
                        depends_on=(RoadmapDependency(node_id="design", requires="implemented"),),
                    ),
                ),
            ),
        )


@pytest.mark.parametrize(
    ("version", "match"),
    (
        (1, "version must increase by exactly 1"),
        (3, "version must increase by exactly 1"),
    ),
)
def test_validate_roadmap_graph_transition_rejects_invalid_version(
    version: int, match: str
) -> None:
    with pytest.raises(RoadmapGraphTransitionError, match=match):
        validate_roadmap_graph_transition(
            current_graph_version=1,
            current_nodes=(),
            proposed_graph=RoadmapGraph(
                roadmap_issue_number=55,
                version=version,
                nodes=(
                    RoadmapNode(
                        node_id="n1",
                        kind="small_job",
                        title="One",
                        body_markdown="Body",
                    ),
                ),
            ),
        )


@pytest.mark.parametrize(
    ("field", "proposed_node", "match"),
    (
        (
            "kind",
            RoadmapNode(
                node_id="impl",
                kind="design_doc",
                title="Implement",
                body_markdown="Ship it",
            ),
            "cannot change roadmap node kind",
        ),
        (
            "title",
            RoadmapNode(
                node_id="impl",
                kind="small_job",
                title="Implement v2",
                body_markdown="Ship it",
            ),
            "cannot change roadmap node title",
        ),
        (
            "body",
            RoadmapNode(
                node_id="impl",
                kind="small_job",
                title="Implement",
                body_markdown="Ship it differently",
            ),
            "cannot change roadmap node body",
        ),
    ),
)
def test_validate_roadmap_graph_transition_rejects_mutating_protected_node_fields(
    field: str, proposed_node: RoadmapNode, match: str
) -> None:
    _ = field
    with pytest.raises(RoadmapGraphTransitionError, match=match):
        validate_roadmap_graph_transition(
            current_graph_version=4,
            current_nodes=(
                ExistingRoadmapNodeState(
                    node_id="impl",
                    kind="small_job",
                    title="Implement",
                    body_markdown="Ship it",
                    depends_on=(),
                    status="issued",
                    child_issue_number=101,
                    implemented_at=None,
                ),
            ),
            proposed_graph=RoadmapGraph(
                roadmap_issue_number=55,
                version=5,
                nodes=(proposed_node,),
            ),
        )
