from __future__ import annotations

import pytest

from mergexo.roadmap_parser import (
    RoadmapGraphValidationError,
    _canonical_graph_json,
    _require_non_empty_str,
    _require_object,
    _require_positive_int,
    parse_roadmap_graph_json,
    parse_roadmap_graph_object,
)


def _valid_graph_payload() -> dict[str, object]:
    return {
        "roadmap_issue_number": 141,
        "version": 1,
        "nodes": [
            {
                "node_id": "design",
                "kind": "design_doc",
                "title": "Design",
                "body_markdown": "Write design",
                "depends_on": [],
            },
            {
                "node_id": "impl",
                "kind": "small_job",
                "title": "Implement",
                "body_markdown": "Ship code",
                "depends_on": [{"node_id": "design", "requires": "planned"}],
            },
        ],
    }


def test_parse_roadmap_graph_json_happy_path_and_canonical_checksum() -> None:
    parsed = parse_roadmap_graph_json(
        """
{
  "version": 1,
  "roadmap_issue_number": 141,
  "nodes": [
    {
      "node_id": "impl",
      "kind": "small_job",
      "title": "Implement",
      "body_markdown": "Ship code",
      "depends_on": [{"node_id": "design", "requires": "planned"}]
    },
    {
      "node_id": "design",
      "kind": "design_doc",
      "title": "Design",
      "body_markdown": "Write design",
      "depends_on": []
    }
  ]
}
""".strip(),
        expected_issue_number=141,
    )

    assert parsed.graph.roadmap_issue_number == 141
    assert parsed.graph.version == 1
    assert {node.node_id for node in parsed.graph.nodes} == {"design", "impl"}
    assert parsed.graph.nodes[1].depends_on[0].requires == "planned"
    assert parsed.checksum
    # Canonical rendering sorts nodes by node_id and normalizes JSON formatting.
    assert parsed.canonical_json == _canonical_graph_json(parsed.graph)


def test_parse_roadmap_graph_defaults_dependency_requires_to_implemented() -> None:
    payload = _valid_graph_payload()
    nodes = payload["nodes"]
    assert isinstance(nodes, list)
    impl_node = nodes[1]
    assert isinstance(impl_node, dict)
    depends_on = impl_node["depends_on"]
    assert isinstance(depends_on, list)
    depends_on[0].pop("requires")  # type: ignore[index]

    parsed = parse_roadmap_graph_object(payload)
    impl = next(node for node in parsed.graph.nodes if node.node_id == "impl")
    assert impl.depends_on[0].requires == "implemented"


def test_parse_roadmap_graph_rejects_invalid_top_level_shapes() -> None:
    with pytest.raises(RoadmapGraphValidationError, match="Invalid roadmap graph JSON"):
        parse_roadmap_graph_json("{")
    with pytest.raises(RoadmapGraphValidationError, match="must be an object"):
        parse_roadmap_graph_object([])
    with pytest.raises(RoadmapGraphValidationError, match="roadmap_issue_number"):
        parse_roadmap_graph_object({"roadmap_issue_number": 0, "version": 1, "nodes": []})
    with pytest.raises(RoadmapGraphValidationError, match="does not match"):
        parse_roadmap_graph_object(_valid_graph_payload(), expected_issue_number=1)
    with pytest.raises(RoadmapGraphValidationError, match="version"):
        parse_roadmap_graph_object(
            {"roadmap_issue_number": 1, "version": 0, "nodes": [{"node_id": "n"}]}
        )
    with pytest.raises(RoadmapGraphValidationError, match="nodes must be a non-empty array"):
        parse_roadmap_graph_object({"roadmap_issue_number": 1, "version": 1, "nodes": []})


@pytest.mark.parametrize(
    "payload,error",
    [
        (
            {
                "roadmap_issue_number": 1,
                "version": 1,
                "nodes": [
                    {
                        "node_id": "n1",
                        "kind": "small_job",
                        "title": "T",
                        "body_markdown": "B",
                    },
                    {
                        "node_id": "n1",
                        "kind": "small_job",
                        "title": "T2",
                        "body_markdown": "B2",
                    },
                ],
            },
            "Duplicate node_id",
        ),
        (
            {
                "roadmap_issue_number": 1,
                "version": 1,
                "nodes": [
                    {
                        "node_id": "n1",
                        "kind": "unknown",
                        "title": "T",
                        "body_markdown": "B",
                    }
                ],
            },
            "Invalid node kind",
        ),
        (
            {
                "roadmap_issue_number": 1,
                "version": 1,
                "nodes": [
                    {
                        "node_id": "n1",
                        "kind": "small_job",
                        "title": "T",
                        "body_markdown": "B",
                        "depends_on": "bad",
                    }
                ],
            },
            "depends_on must be an array",
        ),
        (
            {
                "roadmap_issue_number": 1,
                "version": 1,
                "nodes": [
                    {
                        "node_id": "n1",
                        "kind": "small_job",
                        "title": "T",
                        "body_markdown": "B",
                        "depends_on": [{"node_id": "n2", "requires": "soon"}],
                    }
                ],
            },
            "Invalid dependency requirement",
        ),
        (
            {
                "roadmap_issue_number": 1,
                "version": 1,
                "nodes": [
                    {
                        "node_id": "n1",
                        "kind": "small_job",
                        "title": "T",
                        "body_markdown": "B",
                        "depends_on": [
                            {"node_id": "n2", "requires": "planned"},
                            {"node_id": "n2", "requires": "planned"},
                        ],
                    },
                    {
                        "node_id": "n2",
                        "kind": "small_job",
                        "title": "T2",
                        "body_markdown": "B2",
                    },
                ],
            },
            "Duplicate dependency entry",
        ),
        (
            {
                "roadmap_issue_number": 1,
                "version": 1,
                "nodes": [
                    {
                        "node_id": "n1",
                        "kind": "small_job",
                        "title": "T",
                        "body_markdown": "B",
                        "depends_on": [{"node_id": "n1", "requires": "implemented"}],
                    }
                ],
            },
            "cannot depend on itself",
        ),
        (
            {
                "roadmap_issue_number": 1,
                "version": 1,
                "nodes": [
                    {
                        "node_id": "n1",
                        "kind": "small_job",
                        "title": "T",
                        "body_markdown": "B",
                        "depends_on": [{"node_id": "missing", "requires": "implemented"}],
                    }
                ],
            },
            "depends on missing node_id",
        ),
        (
            {
                "roadmap_issue_number": 1,
                "version": 1,
                "nodes": [
                    {
                        "node_id": "a",
                        "kind": "small_job",
                        "title": "A",
                        "body_markdown": "A",
                        "depends_on": [{"node_id": "b", "requires": "implemented"}],
                    },
                    {
                        "node_id": "b",
                        "kind": "small_job",
                        "title": "B",
                        "body_markdown": "B",
                        "depends_on": [{"node_id": "a", "requires": "implemented"}],
                    },
                ],
            },
            "acyclic",
        ),
    ],
)
def test_parse_roadmap_graph_rejects_invalid_nodes(payload: dict[str, object], error: str) -> None:
    with pytest.raises(RoadmapGraphValidationError, match=error):
        parse_roadmap_graph_object(payload)


def test_roadmap_parser_basic_validators() -> None:
    assert _require_positive_int(1, field="x") == 1
    assert _require_non_empty_str("x", field="x") == "x"
    assert _require_object({"x": 1}, field="x") == {"x": 1}

    with pytest.raises(RoadmapGraphValidationError, match="must be an integer"):
        _require_positive_int(0, field="x")
    with pytest.raises(RoadmapGraphValidationError, match="must be a non-empty string"):
        _require_non_empty_str("", field="x")
    with pytest.raises(RoadmapGraphValidationError, match="must be an object"):
        _require_object([], field="x")
    with pytest.raises(RoadmapGraphValidationError, match="must have string keys"):
        _require_object({1: "x"}, field="x")  # type: ignore[dict-item]
