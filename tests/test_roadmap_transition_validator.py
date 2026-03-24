from __future__ import annotations

import pytest

from mergexo.models import RoadmapDependency, RoadmapNode
from mergexo.roadmap_parser import RoadmapGraph
from mergexo.roadmap_transition_validator import (
    ExistingRoadmapNodeState,
    RoadmapGraphTransitionError,
    validate_roadmap_graph_transition,
)


def _existing_node(
    *,
    node_id: str,
    kind: str = "small_job",
    title: str = "Implement",
    body_markdown: str = "Ship it",
    depends_on: tuple[RoadmapDependency, ...] = (),
    status: str = "pending",
    child_issue_number: int | None = None,
    implemented_at: str | None = None,
) -> ExistingRoadmapNodeState:
    return ExistingRoadmapNodeState(
        node_id=node_id,
        kind=kind,
        title=title,
        body_markdown=body_markdown,
        depends_on=depends_on,
        status=status,
        child_issue_number=child_issue_number,
        implemented_at=implemented_at,
    )


def _node(
    *,
    node_id: str,
    kind: str = "small_job",
    title: str = "Implement",
    body_markdown: str = "Ship it",
    depends_on: tuple[RoadmapDependency, ...] = (),
) -> RoadmapNode:
    return RoadmapNode(
        node_id=node_id,
        kind=kind,
        title=title,
        body_markdown=body_markdown,
        depends_on=depends_on,
    )


def test_validate_roadmap_graph_transition_allows_pending_node_retirement() -> None:
    transition = validate_roadmap_graph_transition(
        current_graph_version=1,
        current_nodes=(
            _existing_node(
                node_id="design",
                kind="design_doc",
                title="Design",
                body_markdown="Doc",
                status="completed",
                child_issue_number=100,
                implemented_at="2026-03-01T00:00:00.000Z",
            ),
            _existing_node(
                node_id="impl",
                title="Implement",
                depends_on=(RoadmapDependency(node_id="design", requires="implemented"),),
            ),
        ),
        proposed_graph=RoadmapGraph(
            roadmap_issue_number=55,
            version=2,
            nodes=(
                _node(
                    node_id="design",
                    kind="design_doc",
                    title="Design",
                    body_markdown="Doc",
                ),
                _node(
                    node_id="verify",
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


def test_validate_roadmap_graph_transition_allows_replacing_pending_work_with_new_node_id() -> None:
    transition = validate_roadmap_graph_transition(
        current_graph_version=3,
        current_nodes=(_existing_node(node_id="impl", title="Implement", body_markdown="Ship it"),),
        proposed_graph=RoadmapGraph(
            roadmap_issue_number=55,
            version=4,
            nodes=(_node(node_id="verify", title="Verify", body_markdown="Check it"),),
        ),
    )

    assert transition.retained_node_ids == ()
    assert transition.added_node_ids == ("verify",)
    assert transition.retired_node_ids == ("impl",)


def test_validate_roadmap_graph_transition_rejects_reintroducing_same_pending_work_with_new_node_id() -> (
    None
):
    with pytest.raises(
        RoadmapGraphTransitionError,
        match="cannot reintroduce unchanged roadmap work under a new node_id",
    ):
        validate_roadmap_graph_transition(
            current_graph_version=2,
            current_nodes=(
                _existing_node(node_id="impl", title="Implement", body_markdown="Ship it"),
            ),
            proposed_graph=RoadmapGraph(
                roadmap_issue_number=55,
                version=3,
                nodes=(_node(node_id="impl_v2", title="Implement", body_markdown="Ship it"),),
            ),
        )


def test_validate_roadmap_graph_transition_rejects_removing_issued_node() -> None:
    with pytest.raises(RoadmapGraphTransitionError, match="cannot remove roadmap node"):
        validate_roadmap_graph_transition(
            current_graph_version=2,
            current_nodes=(
                _existing_node(
                    node_id="impl",
                    status="issued",
                    child_issue_number=101,
                ),
            ),
            proposed_graph=RoadmapGraph(
                roadmap_issue_number=55,
                version=3,
                nodes=(),
            ),
        )


@pytest.mark.parametrize(
    ("proposed_node", "match"),
    (
        (
            _node(node_id="impl", title="Implement v2"),
            "retained roadmap node must keep the same title",
        ),
        (
            _node(node_id="impl", body_markdown="Ship it differently"),
            "retained roadmap node must keep the same body",
        ),
        (
            _node(
                node_id="impl",
                depends_on=(RoadmapDependency(node_id="design", requires="planned"),),
            ),
            "retained roadmap node must keep the same dependencies",
        ),
    ),
)
def test_validate_roadmap_graph_transition_rejects_mutating_pending_retained_node_identity(
    proposed_node: RoadmapNode, match: str
) -> None:
    with pytest.raises(RoadmapGraphTransitionError, match=match):
        validate_roadmap_graph_transition(
            current_graph_version=4,
            current_nodes=(
                _existing_node(
                    node_id="impl",
                    depends_on=(RoadmapDependency(node_id="design", requires="implemented"),),
                ),
            ),
            proposed_graph=RoadmapGraph(
                roadmap_issue_number=55,
                version=5,
                nodes=(proposed_node,),
            ),
        )


def test_validate_roadmap_graph_transition_rejects_mutating_protected_dependencies() -> None:
    with pytest.raises(
        RoadmapGraphTransitionError,
        match="cannot change roadmap node dependencies after work has started",
    ):
        validate_roadmap_graph_transition(
            current_graph_version=3,
            current_nodes=(
                _existing_node(
                    node_id="impl",
                    depends_on=(RoadmapDependency(node_id="design", requires="planned"),),
                    status="issued",
                    child_issue_number=101,
                ),
            ),
            proposed_graph=RoadmapGraph(
                roadmap_issue_number=55,
                version=4,
                nodes=(
                    _node(
                        node_id="impl",
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
                nodes=(_node(node_id="n1", title="One", body_markdown="Body"),),
            ),
        )


@pytest.mark.parametrize(
    ("proposed_node", "match"),
    (
        (
            _node(node_id="impl", kind="design_doc"),
            "cannot change roadmap node kind",
        ),
        (
            _node(node_id="impl", title="Implement v2"),
            "cannot change roadmap node title",
        ),
        (
            _node(node_id="impl", body_markdown="Ship it differently"),
            "cannot change roadmap node body",
        ),
    ),
)
def test_validate_roadmap_graph_transition_rejects_mutating_protected_node_fields(
    proposed_node: RoadmapNode, match: str
) -> None:
    with pytest.raises(RoadmapGraphTransitionError, match=match):
        validate_roadmap_graph_transition(
            current_graph_version=4,
            current_nodes=(
                _existing_node(
                    node_id="impl",
                    status="issued",
                    child_issue_number=101,
                ),
            ),
            proposed_graph=RoadmapGraph(
                roadmap_issue_number=55,
                version=5,
                nodes=(proposed_node,),
            ),
        )
