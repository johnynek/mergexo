from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from mergexo.models import RoadmapDependency, RoadmapNodeKind
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
    retained_node_ids: list[str] = []
    retired_node_ids: list[str] = []

    for current_node in current_nodes:
        proposed_node = proposed_nodes.get(current_node.node_id)
        protected = (
            current_node.child_issue_number is not None
            or current_node.implemented_at is not None
            or current_node.status in {"issued", "completed", "blocked", "abandoned"}
        )
        if proposed_node is None:
            if protected:
                raise RoadmapGraphTransitionError(
                    f"cannot remove roadmap node after work has started: {current_node.node_id}"
                )
            retired_node_ids.append(current_node.node_id)
            continue

        retained_node_ids.append(current_node.node_id)
        if proposed_node.kind != current_node.kind:
            raise RoadmapGraphTransitionError(
                f"cannot change roadmap node kind for existing node_id: {current_node.node_id}"
            )
        if not protected:
            continue
        if proposed_node.title != current_node.title:
            raise RoadmapGraphTransitionError(
                f"cannot change roadmap node title after work has started: {current_node.node_id}"
            )
        if proposed_node.body_markdown != current_node.body_markdown:
            raise RoadmapGraphTransitionError(
                f"cannot change roadmap node body after work has started: {current_node.node_id}"
            )
        if _dependency_pairs(proposed_node.depends_on) != _dependency_pairs(
            current_node.depends_on
        ):
            raise RoadmapGraphTransitionError(
                "cannot change roadmap node dependencies after work has started: "
                f"{current_node.node_id}"
            )

    added_node_ids = [
        node.node_id for node in proposed_graph.nodes if node.node_id not in retained_node_ids
    ]
    return RoadmapGraphTransition(
        retained_node_ids=tuple(sorted(retained_node_ids)),
        added_node_ids=tuple(sorted(added_node_ids)),
        retired_node_ids=tuple(sorted(retired_node_ids)),
    )


def _dependency_pairs(
    dependencies: tuple[RoadmapDependency, ...],
) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((dependency.node_id, dependency.requires) for dependency in dependencies))
