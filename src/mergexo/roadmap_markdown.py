from __future__ import annotations

import heapq

from mergexo.models import RoadmapNode
from mergexo.roadmap_parser import RoadmapGraph


_GENERATED_NOTE_LINES = (
    "> Generated from roadmap graph JSON.",
    "> Edit the `.graph.json` file, not this `.md` file.",
    "> Regenerate with `python roadmap_json_to_md.py <path/to.graph.json> [--output <path/to.md>]`.",
)


def render_roadmap_markdown(graph: RoadmapGraph) -> str:
    ordered_nodes = _topologically_sorted_nodes(graph.nodes)
    lines = [
        f"# Roadmap #{graph.roadmap_issue_number}",
        "",
        *_GENERATED_NOTE_LINES,
        "",
        "## Metadata",
        "",
        f"- Roadmap issue: `#{graph.roadmap_issue_number}`",
        f"- Graph version: `{graph.version}`",
        f"- Node count: `{len(ordered_nodes)}`",
        "",
        "## Dependency Overview",
        "",
    ]
    for idx, node in enumerate(ordered_nodes, start=1):
        lines.append(f"{idx}. `{node.node_id}` (`{node.kind}`): {_inline_dependencies(node)}")

    lines.extend(("", "## Nodes", ""))
    for node in ordered_nodes:
        lines.extend(
            (
                f"### `{node.node_id}`",
                "",
                f"- Kind: `{node.kind}`",
                f"- Title: {node.title}",
                f"- Depends on: {_inline_dependencies(node)}",
                "",
                "#### Body",
                "",
                node.body_markdown,
                "",
            )
        )
    return "\n".join(lines).rstrip() + "\n"


def _topologically_sorted_nodes(nodes: tuple[RoadmapNode, ...]) -> tuple[RoadmapNode, ...]:
    node_by_id = {node.node_id: node for node in nodes}
    indegree = {node.node_id: len(node.depends_on) for node in nodes}
    dependents: dict[str, list[str]] = {node.node_id: [] for node in nodes}
    for node in nodes:
        for dependency in node.depends_on:
            dependents[dependency.node_id].append(node.node_id)

    ready = list(node_id for node_id, degree in indegree.items() if degree == 0)
    heapq.heapify(ready)
    ordered: list[RoadmapNode] = []
    while ready:
        node_id = heapq.heappop(ready)
        ordered.append(node_by_id[node_id])
        for dependent_id in sorted(dependents[node_id]):
            indegree[dependent_id] -= 1
            if indegree[dependent_id] == 0:
                heapq.heappush(ready, dependent_id)

    if len(ordered) != len(nodes):
        raise ValueError("Roadmap graph must be acyclic")
    return tuple(ordered)


def _inline_dependencies(node: RoadmapNode) -> str:
    if not node.depends_on:
        return "none"
    return ", ".join(
        f"`{dependency.node_id}` (`{dependency.requires}`)" for dependency in node.depends_on
    )
