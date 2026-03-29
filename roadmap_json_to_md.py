#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parent
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from mergexo.roadmap_markdown import render_roadmap_markdown
from mergexo.roadmap_parser import RoadmapGraphValidationError, parse_roadmap_graph_json


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=("Validate a roadmap graph JSON file and emit the deterministic markdown view.")
    )
    parser.add_argument("input_path", help="Path to the roadmap .graph.json file")
    parser.add_argument(
        "--output",
        "-o",
        dest="output_path",
        help="Optional path to also write the generated markdown file",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    input_path = Path(args.input_path)
    output_path = Path(args.output_path) if args.output_path else None

    try:
        raw_json = input_path.read_text(encoding="utf-8")
        parsed = parse_roadmap_graph_json(raw_json)
        markdown = render_roadmap_markdown(parsed.graph)
        if output_path is not None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(markdown, encoding="utf-8")
    except (OSError, RoadmapGraphValidationError, ValueError) as exc:
        print(f"roadmap_json_to_md.py: {exc}", file=sys.stderr)
        return 1

    sys.stdout.write(markdown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
