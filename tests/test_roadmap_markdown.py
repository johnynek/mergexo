from __future__ import annotations

from pathlib import Path
import json
import subprocess
import sys

import pytest

from mergexo.models import RoadmapDependency, RoadmapNode
from mergexo.roadmap_markdown import render_roadmap_markdown
from mergexo.roadmap_parser import RoadmapGraph, parse_roadmap_graph_json


def test_render_roadmap_markdown_uses_stable_topological_order() -> None:
    parsed = parse_roadmap_graph_json(
        json.dumps(
            {
                "roadmap_issue_number": 155,
                "version": 2,
                "nodes": [
                    {
                        "node_id": "ship",
                        "kind": "small_job",
                        "title": "Ship it",
                        "body_markdown": "Land the rollout.",
                        "depends_on": [{"node_id": "design", "requires": "implemented"}],
                    },
                    {
                        "node_id": "docs",
                        "kind": "small_job",
                        "title": "Document it",
                        "body_markdown": "Explain the workflow.",
                        "depends_on": [{"node_id": "design", "requires": "planned"}],
                    },
                    {
                        "node_id": "design",
                        "kind": "design_doc",
                        "title": "Design it",
                        "body_markdown": "Write the design.",
                        "depends_on": [],
                    },
                ],
            }
        ),
        expected_issue_number=155,
    )

    markdown = render_roadmap_markdown(parsed.graph)

    assert markdown.startswith("# Roadmap #155\n")
    assert "Generated from roadmap graph JSON." in markdown
    assert "Graph version: `2`" in markdown
    assert "Node count: `3`" in markdown
    assert markdown.index("1. `design` (`design_doc`): none") < markdown.index(
        "2. `docs` (`small_job`): `design` (`planned`)"
    )
    assert markdown.index("2. `docs` (`small_job`): `design` (`planned`)") < markdown.index(
        "3. `ship` (`small_job`): `design` (`implemented`)"
    )
    assert (
        markdown.index("### `design`") < markdown.index("### `docs`") < markdown.index("### `ship`")
    )
    assert "#### Body\n\nExplain the workflow." in markdown


def test_render_roadmap_markdown_tie_breaks_independent_nodes_by_node_id() -> None:
    graph = RoadmapGraph(
        roadmap_issue_number=12,
        version=1,
        nodes=(
            RoadmapNode(
                node_id="zeta",
                kind="small_job",
                title="Last alphabetically",
                body_markdown="Z body",
            ),
            RoadmapNode(
                node_id="alpha",
                kind="small_job",
                title="First alphabetically",
                body_markdown="A body",
            ),
        ),
    )

    markdown = render_roadmap_markdown(graph)

    assert markdown.index("1. `alpha` (`small_job`): none") < markdown.index(
        "2. `zeta` (`small_job`): none"
    )


def test_render_roadmap_markdown_rejects_cycles() -> None:
    graph = RoadmapGraph(
        roadmap_issue_number=99,
        version=1,
        nodes=(
            RoadmapNode(
                node_id="a",
                kind="small_job",
                title="A",
                body_markdown="A body",
                depends_on=(RoadmapDependency(node_id="b"),),
            ),
            RoadmapNode(
                node_id="b",
                kind="small_job",
                title="B",
                body_markdown="B body",
                depends_on=(RoadmapDependency(node_id="a"),),
            ),
        ),
    )

    with pytest.raises(ValueError, match="acyclic"):
        render_roadmap_markdown(graph)


def test_roadmap_json_to_md_cli_writes_stdout_and_output_file(tmp_path: Path) -> None:
    input_path = tmp_path / "roadmap.graph.json"
    output_path = tmp_path / "roadmap.md"
    input_path.write_text(
        json.dumps(
            {
                "roadmap_issue_number": 5,
                "version": 1,
                "nodes": [
                    {
                        "node_id": "n1",
                        "kind": "small_job",
                        "title": "Ship",
                        "body_markdown": "Do it",
                        "depends_on": [],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "roadmap_json_to_md.py",
            str(input_path),
            "--output",
            str(output_path),
        ],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "Generated from roadmap graph JSON." in result.stdout
    assert output_path.read_text(encoding="utf-8") == result.stdout


def test_roadmap_json_to_md_cli_reports_invalid_graph(tmp_path: Path) -> None:
    input_path = tmp_path / "roadmap.graph.json"
    input_path.write_text('{"roadmap_issue_number":1,"version":1,"nodes":[]}', encoding="utf-8")

    result = subprocess.run(
        [sys.executable, "roadmap_json_to_md.py", str(input_path)],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "roadmap_json_to_md.py:" in result.stderr
    assert "nodes must be a non-empty array" in result.stderr
