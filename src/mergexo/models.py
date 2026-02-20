from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Issue:
    number: int
    title: str
    body: str
    html_url: str
    labels: tuple[str, ...]


@dataclass(frozen=True)
class PullRequest:
    number: int
    html_url: str


@dataclass(frozen=True)
class GeneratedDesign:
    title: str
    design_doc_markdown: str
    touch_paths: tuple[str, ...]
    summary: str


@dataclass(frozen=True)
class WorkResult:
    issue_number: int
    branch: str
    pr_number: int
    pr_url: str
