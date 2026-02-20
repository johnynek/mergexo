from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib
from typing import cast


@dataclass(frozen=True)
class RuntimeConfig:
    base_dir: Path
    worker_count: int
    poll_interval_seconds: int


@dataclass(frozen=True)
class RepoConfig:
    owner: str
    name: str
    default_branch: str
    trigger_label: str
    design_docs_dir: str
    local_clone_source: str | None
    remote_url: str | None

    @property
    def full_name(self) -> str:
        return f"{self.owner}/{self.name}"

    @property
    def effective_remote_url(self) -> str:
        if self.remote_url:
            return self.remote_url
        return f"git@github.com:{self.owner}/{self.name}.git"


@dataclass(frozen=True)
class CodexConfig:
    enabled: bool
    model: str | None
    sandbox: str | None
    profile: str | None
    extra_args: tuple[str, ...]


@dataclass(frozen=True)
class AppConfig:
    runtime: RuntimeConfig
    repo: RepoConfig
    codex: CodexConfig


class ConfigError(ValueError):
    pass


def load_config(path: Path) -> AppConfig:
    with path.open("rb") as fh:
        data = tomllib.load(fh)

    runtime_data = _require_table(data, "runtime")
    repo_data = _require_table(data, "repo")
    codex_data = data.get("codex", {})
    if not isinstance(codex_data, dict):
        raise ConfigError("[codex] must be a TOML table")

    runtime = RuntimeConfig(
        base_dir=Path(_require_str(runtime_data, "base_dir")).expanduser(),
        worker_count=_require_int(runtime_data, "worker_count"),
        poll_interval_seconds=_require_int(runtime_data, "poll_interval_seconds"),
    )

    if runtime.worker_count < 1:
        raise ConfigError("runtime.worker_count must be >= 1")
    if runtime.poll_interval_seconds < 5:
        raise ConfigError("runtime.poll_interval_seconds must be >= 5")

    repo = RepoConfig(
        owner=_require_str(repo_data, "owner"),
        name=_require_str(repo_data, "name"),
        default_branch=_require_str(repo_data, "default_branch"),
        trigger_label=_require_str(repo_data, "trigger_label"),
        design_docs_dir=_require_str(repo_data, "design_docs_dir"),
        local_clone_source=_optional_str(repo_data, "local_clone_source"),
        remote_url=_optional_str(repo_data, "remote_url"),
    )

    codex = CodexConfig(
        enabled=_bool_with_default(codex_data, "enabled", True),
        model=_optional_str(codex_data, "model"),
        sandbox=_optional_str(codex_data, "sandbox"),
        profile=_optional_str(codex_data, "profile"),
        extra_args=_tuple_of_str(codex_data, "extra_args"),
    )

    return AppConfig(runtime=runtime, repo=repo, codex=codex)


def _require_table(data: dict[str, object], key: str) -> dict[str, object]:
    value = data.get(key)
    if not isinstance(value, dict):
        raise ConfigError(f"[{key}] is required and must be a TOML table")
    if not all(isinstance(item, str) for item in value.keys()):
        raise ConfigError(f"[{key}] must have string keys")
    return cast(dict[str, object], value)


def _require_str(data: dict[str, object], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{key} is required and must be a non-empty string")
    return value


def _optional_str(data: dict[str, object], key: str) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{key} must be a non-empty string if provided")
    return value


def _require_int(data: dict[str, object], key: str) -> int:
    value = data.get(key)
    if not isinstance(value, int):
        raise ConfigError(f"{key} is required and must be an integer")
    return value


def _bool_with_default(data: dict[str, object], key: str, default: bool) -> bool:
    value = data.get(key, default)
    if not isinstance(value, bool):
        raise ConfigError(f"{key} must be a boolean")
    return value


def _tuple_of_str(data: dict[str, object], key: str) -> tuple[str, ...]:
    value = data.get(key, [])
    if not isinstance(value, list):
        raise ConfigError(f"{key} must be a list of strings")
    out: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ConfigError(f"{key} must be a list of strings")
        out.append(item)
    return tuple(out)
