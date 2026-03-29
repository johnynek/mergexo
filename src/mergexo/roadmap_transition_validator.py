from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypeAlias

from mergexo.models import RoadmapDependency, RoadmapNode, RoadmapNodeKind
from mergexo.roadmap_parser import RoadmapGraph


ExistingRoadmapNodeStatus = Literal["pending", "issued", "completed", "blocked", "abandoned"]


@dataclass(frozen=True)
class ExistingRoadmapNodeState:
    node_id: str
    kind: RoadmapNodeKind
    title: str
    body_markdown: str
    depends_on: tuple[RoadmapDependency, ...]
    status: ExistingRoadmapNodeStatus
    child_issue_number: int | None
    implemented_at: str | None


@dataclass(frozen=True)
class RoadmapGraphTransition:
    retained_node_ids: tuple[str, ...]
    added_node_ids: tuple[str, ...]
    retired_node_ids: tuple[str, ...]


class RoadmapGraphTransitionError(ValueError):
    pass


def validate_roadmap_graph_transition(
    *,
    current_graph_version: int,
    current_nodes: tuple[ExistingRoadmapNodeState, ...],
    proposed_graph: RoadmapGraph,
) -> RoadmapGraphTransition:
    if proposed_graph.version != current_graph_version + 1:
        raise RoadmapGraphTransitionError(
            "roadmap graph version must increase by exactly 1: "
            f"expected {current_graph_version + 1}, got {proposed_graph.version}"
        )

    proposed_nodes = {node.node_id: node for node in proposed_graph.nodes}
    proposed_signatures: dict[str, _RoadmapNodeSignature] = {
        node.node_id: _proposed_node_signature(node) for node in proposed_graph.nodes
    }
    retained_node_ids: list[str] = []
    retired_node_ids: list[str] = []
    retired_pending_signatures: dict[_RoadmapNodeSignature, str] = {}

    for current_node in current_nodes:
        proposed_node = proposed_nodes.get(current_node.node_id)
        protected = _node_work_started(current_node)
        if proposed_node is None:
            if protected:
                raise RoadmapGraphTransitionError(
                    f"cannot remove roadmap node after work has started: {current_node.node_id}"
                )
            retired_node_ids.append(current_node.node_id)
            retired_pending_signatures[_existing_node_signature(current_node)] = (
                current_node.node_id
            )
            continue

        retained_node_ids.append(current_node.node_id)
        if proposed_node.kind != current_node.kind:
            if protected or not _is_pending_doc_kind_migration(
                current_kind=current_node.kind, proposed_kind=proposed_node.kind
            ):
                raise RoadmapGraphTransitionError(
                    f"cannot change roadmap node kind for existing node_id: {current_node.node_id}"
                )
        _validate_retained_node_identity(
            current_node=current_node,
            proposed_node=proposed_node,
            protected=protected,
        )

    added_node_ids = [
        node.node_id for node in proposed_graph.nodes if node.node_id not in retained_node_ids
    ]
    for added_node_id in added_node_ids:
        matching_retired_node_id = retired_pending_signatures.get(
            proposed_signatures[added_node_id]
        )
        if matching_retired_node_id is not None:
            raise RoadmapGraphTransitionError(
                "cannot reintroduce unchanged roadmap work under a new node_id: "
                f"old={matching_retired_node_id}, new={added_node_id}"
            )
    return RoadmapGraphTransition(
        retained_node_ids=tuple(sorted(retained_node_ids)),
        added_node_ids=tuple(sorted(added_node_ids)),
        retired_node_ids=tuple(sorted(retired_node_ids)),
    )


def _dependency_pairs(
    dependencies: tuple[RoadmapDependency, ...],
) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((dependency.node_id, dependency.requires) for dependency in dependencies))


_RoadmapNodeSignature: TypeAlias = tuple[RoadmapNodeKind, str, str, tuple[tuple[str, str], ...]]


def _is_pending_doc_kind_migration(
    *, current_kind: RoadmapNodeKind, proposed_kind: RoadmapNodeKind
) -> bool:
    return {current_kind, proposed_kind} == {"design_doc", "reference_doc"}


def _node_work_started(node: ExistingRoadmapNodeState) -> bool:
    return (
        node.child_issue_number is not None
        or node.implemented_at is not None
        or node.status in {"issued", "completed", "blocked", "abandoned"}
    )


def _existing_node_signature(node: ExistingRoadmapNodeState) -> _RoadmapNodeSignature:
    return (node.kind, node.title, node.body_markdown, _dependency_pairs(node.depends_on))


def _proposed_node_signature(node: RoadmapNode) -> _RoadmapNodeSignature:
    return (
        node.kind,
        node.title,
        node.body_markdown,
        _dependency_pairs(node.depends_on),
    )


def _validate_retained_node_identity(
    *,
    current_node: ExistingRoadmapNodeState,
    proposed_node: RoadmapNode,
    protected: bool,
) -> None:
    if proposed_node.title != current_node.title:
        if protected:
            raise RoadmapGraphTransitionError(
                f"cannot change roadmap node title after work has started: {current_node.node_id}"
            )
        raise RoadmapGraphTransitionError(
            "retained roadmap node must keep the same title; use a new node_id for replaced work: "
            f"{current_node.node_id}"
        )
    if proposed_node.body_markdown != current_node.body_markdown:
        if protected:
            raise RoadmapGraphTransitionError(
                f"cannot change roadmap node body after work has started: {current_node.node_id}"
            )
        raise RoadmapGraphTransitionError(
            "retained roadmap node must keep the same body; use a new node_id for replaced work: "
            f"{current_node.node_id}"
        )
    if _dependency_pairs(proposed_node.depends_on) != _dependency_pairs(current_node.depends_on):
        if protected:
            raise RoadmapGraphTransitionError(
                "cannot change roadmap node dependencies after work has started: "
                f"{current_node.node_id}"
            )
        raise RoadmapGraphTransitionError(
            "retained roadmap node must keep the same dependencies; use a new node_id for "
            f"replaced work: {current_node.node_id}"
        )
