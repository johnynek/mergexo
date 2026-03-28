from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import cast

from mergexo.models import (
    ROADMAP_NODE_KINDS,
    RoadmapDependency,
    RoadmapDependencyRequirement,
    RoadmapNode,
    RoadmapNodeKind,
)


@dataclass(frozen=True)
class RoadmapGraph:
    roadmap_issue_number: int
    version: int
    nodes: tuple[RoadmapNode, ...]


@dataclass(frozen=True)
class ParsedRoadmapGraph:
    graph: RoadmapGraph
    canonical_json: str
    checksum: str


class RoadmapGraphValidationError(ValueError):
    pass


def parse_roadmap_graph_json(
    raw_json: str, *, expected_issue_number: int | None = None
) -> ParsedRoadmapGraph:
    try:
        payload = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise RoadmapGraphValidationError(f"Invalid roadmap graph JSON: {exc}") from exc
    return parse_roadmap_graph_object(payload, expected_issue_number=expected_issue_number)


def parse_roadmap_graph_object(
    payload: object, *, expected_issue_number: int | None = None
) -> ParsedRoadmapGraph:
    obj = _require_object(payload, field="graph")

    issue_number = _require_positive_int(
        obj.get("roadmap_issue_number"), field="roadmap_issue_number"
    )
    if expected_issue_number is not None and issue_number != expected_issue_number:
        raise RoadmapGraphValidationError(
            "roadmap_issue_number does not match the source issue: "
            f"expected {expected_issue_number}, got {issue_number}"
        )

    version = _require_positive_int(obj.get("version"), field="version")
    nodes_payload = obj.get("nodes")
    if not isinstance(nodes_payload, list) or not nodes_payload:
        raise RoadmapGraphValidationError("nodes must be a non-empty array")

    parsed_nodes: list[RoadmapNode] = []
    node_ids: set[str] = set()
    for raw_node in nodes_payload:
        node_obj = _require_object(raw_node, field="nodes[]")
        node_id = _require_non_empty_str(node_obj.get("node_id"), field="nodes[].node_id")
        if node_id in node_ids:
            raise RoadmapGraphValidationError(f"Duplicate node_id: {node_id}")
        node_ids.add(node_id)

        kind_value = _require_non_empty_str(node_obj.get("kind"), field=f"{node_id}.kind")
        if kind_value not in ROADMAP_NODE_KINDS:
            raise RoadmapGraphValidationError(
                f"Invalid node kind for {node_id}: {kind_value!r}. "
                "Expected one of: " + ", ".join(ROADMAP_NODE_KINDS)
            )
        kind = cast(RoadmapNodeKind, kind_value)

        title = _require_non_empty_str(node_obj.get("title"), field=f"{node_id}.title")
        body_markdown = _require_non_empty_str(
            node_obj.get("body_markdown"), field=f"{node_id}.body_markdown"
        )
        depends_payload = node_obj.get("depends_on", [])
        if not isinstance(depends_payload, list):
            raise RoadmapGraphValidationError(
                f"{node_id}.depends_on must be an array when provided"
            )

        dependencies: list[RoadmapDependency] = []
        dependency_keys: set[tuple[str, str]] = set()
        for raw_dep in depends_payload:
            dep_obj = _require_object(raw_dep, field=f"{node_id}.depends_on[]")
            dep_node_id = _require_non_empty_str(
                dep_obj.get("node_id"), field=f"{node_id}.depends_on[].node_id"
            )
            requires_raw = dep_obj.get("requires")
            requires_value = (
                "implemented"
                if requires_raw is None
                else _require_non_empty_str(requires_raw, field=f"{node_id}.depends_on[].requires")
            )
            if requires_value not in {"planned", "implemented"}:
                raise RoadmapGraphValidationError(
                    f"Invalid dependency requirement in {node_id}: {requires_value!r}. "
                    "Expected planned or implemented"
                )
            dep_key = (dep_node_id, requires_value)
            if dep_key in dependency_keys:
                raise RoadmapGraphValidationError(
                    f"Duplicate dependency entry in {node_id}: "
                    f"node_id={dep_node_id}, requires={requires_value}"
                )
            dependency_keys.add(dep_key)
            dependencies.append(
                RoadmapDependency(
                    node_id=dep_node_id,
                    requires=cast(RoadmapDependencyRequirement, requires_value),
                )
            )

        parsed_nodes.append(
            RoadmapNode(
                node_id=node_id,
                kind=kind,
                title=title,
                body_markdown=body_markdown,
                depends_on=tuple(dependencies),
            )
        )

    _validate_references_and_dag(nodes=tuple(parsed_nodes))

    graph = RoadmapGraph(
        roadmap_issue_number=issue_number,
        version=version,
        nodes=_normalize_nodes(tuple(parsed_nodes)),
    )
    canonical_json = _canonical_graph_json(graph)
    return ParsedRoadmapGraph(
        graph=graph,
        canonical_json=canonical_json,
        checksum=hashlib.sha256(canonical_json.encode("utf-8")).hexdigest(),
    )


def _canonical_graph_json(graph: RoadmapGraph) -> str:
    payload = {
        "roadmap_issue_number": graph.roadmap_issue_number,
        "version": graph.version,
        "nodes": [
            {
                "node_id": node.node_id,
                "kind": node.kind,
                "title": node.title,
                "body_markdown": node.body_markdown,
                "depends_on": [
                    {"node_id": dep.node_id, "requires": dep.requires}
                    for dep in sorted(
                        node.depends_on, key=lambda item: (item.node_id, item.requires)
                    )
                ],
            }
            for node in sorted(graph.nodes, key=lambda item: item.node_id)
        ],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _normalize_nodes(nodes: tuple[RoadmapNode, ...]) -> tuple[RoadmapNode, ...]:
    return tuple(
        RoadmapNode(
            node_id=node.node_id,
            kind=node.kind,
            title=node.title,
            body_markdown=node.body_markdown,
            depends_on=tuple(
                sorted(node.depends_on, key=lambda item: (item.node_id, item.requires))
            ),
        )
        for node in sorted(nodes, key=lambda item: item.node_id)
    )


def _validate_references_and_dag(*, nodes: tuple[RoadmapNode, ...]) -> None:
    node_by_id = {node.node_id: node for node in nodes}
    for node in nodes:
        for dependency in node.depends_on:
            if dependency.node_id == node.node_id:
                raise RoadmapGraphValidationError(f"Node {node.node_id} cannot depend on itself")
            if dependency.node_id not in node_by_id:
                raise RoadmapGraphValidationError(
                    f"Node {node.node_id} depends on missing node_id: {dependency.node_id}"
                )

    visited: set[str] = set()
    visiting: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in visited:
            return
        if node_id in visiting:
            raise RoadmapGraphValidationError("Roadmap graph must be acyclic")
        visiting.add(node_id)
        node = node_by_id[node_id]
        for dependency in node.depends_on:
            visit(dependency.node_id)
        visiting.remove(node_id)
        visited.add(node_id)

    for node in nodes:
        visit(node.node_id)


def _require_object(payload: object, *, field: str) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise RoadmapGraphValidationError(f"{field} must be an object")
    if not all(isinstance(key, str) for key in payload.keys()):
        raise RoadmapGraphValidationError(f"{field} must have string keys")
    return cast(dict[str, object], payload)


def _require_positive_int(value: object, *, field: str) -> int:
    if not isinstance(value, int) or value < 1:
        raise RoadmapGraphValidationError(f"{field} must be an integer >= 1")
    return value


def _require_non_empty_str(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RoadmapGraphValidationError(f"{field} must be a non-empty string")
    return value.strip()
