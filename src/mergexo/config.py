from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib
from typing import cast

from mergexo.models import RestartMode


@dataclass(frozen=True)
class RuntimeConfig:
    base_dir: Path
    worker_count: int
    poll_interval_seconds: int
    enable_feedback_loop: bool
    enable_github_operations: bool = False
    restart_drain_timeout_seconds: int = 900
    restart_default_mode: RestartMode = "git_checkout"
    restart_supported_modes: tuple[RestartMode, ...] = ("git_checkout",)
    git_checkout_root: Path | None = None
    service_python: str | None = None


@dataclass(frozen=True)
class RepoConfig:
    owner: str
    name: str
    default_branch: str
    trigger_label: str
    bugfix_label: str
    small_job_label: str
    coding_guidelines_path: str
    design_docs_dir: str
    local_clone_source: str | None
    remote_url: str | None
    operations_issue_number: int | None = None
    operator_logins: tuple[str, ...] = ()

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
class AuthConfig:
    allowed_users: frozenset[str]

    def allows(self, login: str) -> bool:
        normalized = login.strip().lower()
        if not normalized:
            return False
        return normalized in self.allowed_users


@dataclass(frozen=True)
class AppConfig:
    runtime: RuntimeConfig
    repo: RepoConfig
    codex: CodexConfig
    auth: AuthConfig


class ConfigError(ValueError):
    pass


def load_config(path: Path) -> AppConfig:
    with path.open("rb") as fh:
        data = tomllib.load(fh)

    runtime_data = _require_table(data, "runtime")
    repo_data = _require_table(data, "repo")
    auth_data = _require_table(data, "auth")
    codex_data = data.get("codex", {})
    if not isinstance(codex_data, dict):
        raise ConfigError("[codex] must be a TOML table")

    runtime = RuntimeConfig(
        base_dir=Path(_require_str(runtime_data, "base_dir")).expanduser(),
        worker_count=_require_int(runtime_data, "worker_count"),
        poll_interval_seconds=_require_int(runtime_data, "poll_interval_seconds"),
        enable_feedback_loop=_bool_with_default(runtime_data, "enable_feedback_loop", False),
        enable_github_operations=_bool_with_default(
            runtime_data, "enable_github_operations", False
        ),
        restart_drain_timeout_seconds=_int_with_default(
            runtime_data, "restart_drain_timeout_seconds", 900
        ),
        restart_default_mode=_restart_mode_with_default(
            runtime_data, "restart_default_mode", "git_checkout"
        ),
        restart_supported_modes=_restart_modes_with_default(
            runtime_data, "restart_supported_modes", ("git_checkout",)
        ),
        git_checkout_root=_optional_path(runtime_data, "git_checkout_root"),
        service_python=_optional_str(runtime_data, "service_python"),
    )

    if runtime.worker_count < 1:
        raise ConfigError("runtime.worker_count must be >= 1")
    if runtime.poll_interval_seconds < 5:
        raise ConfigError("runtime.poll_interval_seconds must be >= 5")
    if runtime.restart_drain_timeout_seconds < 1:
        raise ConfigError("runtime.restart_drain_timeout_seconds must be >= 1")
    if runtime.restart_default_mode not in runtime.restart_supported_modes:
        raise ConfigError(
            "runtime.restart_default_mode must be included in runtime.restart_supported_modes"
        )

    repo = RepoConfig(
        owner=_require_str(repo_data, "owner"),
        name=_require_str(repo_data, "name"),
        default_branch=_require_str(repo_data, "default_branch"),
        trigger_label=_require_str(repo_data, "trigger_label"),
        bugfix_label=_str_with_default(repo_data, "bugfix_label", "agent:bugfix"),
        small_job_label=_str_with_default(repo_data, "small_job_label", "agent:small-job"),
        coding_guidelines_path=_str_with_default(
            repo_data, "coding_guidelines_path", "docs/python_style.md"
        ),
        design_docs_dir=_require_str(repo_data, "design_docs_dir"),
        local_clone_source=_optional_str(repo_data, "local_clone_source"),
        remote_url=_optional_str(repo_data, "remote_url"),
        operations_issue_number=_optional_positive_int(repo_data, "operations_issue_number"),
        operator_logins=_operator_logins_with_default(repo_data, "operator_logins", ()),
    )

    codex = CodexConfig(
        enabled=_bool_with_default(codex_data, "enabled", True),
        model=_optional_str(codex_data, "model"),
        sandbox=_optional_str(codex_data, "sandbox"),
        profile=_optional_str(codex_data, "profile"),
        extra_args=_tuple_of_str(codex_data, "extra_args"),
    )

    auth = AuthConfig(allowed_users=_require_allowed_users(auth_data, "allowed_users"))

    return AppConfig(runtime=runtime, repo=repo, codex=codex, auth=auth)


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


def _int_with_default(data: dict[str, object], key: str, default: int) -> int:
    value = data.get(key, default)
    if not isinstance(value, int):
        raise ConfigError(f"{key} must be an integer")
    return value


def _bool_with_default(data: dict[str, object], key: str, default: bool) -> bool:
    value = data.get(key, default)
    if not isinstance(value, bool):
        raise ConfigError(f"{key} must be a boolean")
    return value


def _str_with_default(data: dict[str, object], key: str, default: str) -> str:
    value = data.get(key, default)
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{key} must be a non-empty string")
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


def _optional_positive_int(data: dict[str, object], key: str) -> int | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, int) or value < 1:
        raise ConfigError(f"{key} must be an integer >= 1 if provided")
    return value


def _optional_path(data: dict[str, object], key: str) -> Path | None:
    value = _optional_str(data, key)
    if value is None:
        return None
    return Path(value).expanduser()


def _restart_modes_with_default(
    data: dict[str, object], key: str, default: tuple[RestartMode, ...]
) -> tuple[RestartMode, ...]:
    value = data.get(key, list(default))
    if not isinstance(value, list):
        raise ConfigError(f"{key} must be a list of restart modes")
    if not value:
        raise ConfigError(f"{key} must contain at least one restart mode")
    parsed: list[RestartMode] = []
    for item in value:
        parsed_mode = _parse_restart_mode(item, key=key)
        if parsed_mode not in parsed:
            parsed.append(parsed_mode)
    return tuple(parsed)


def _restart_mode_with_default(
    data: dict[str, object], key: str, default: RestartMode
) -> RestartMode:
    value = data.get(key, default)
    return _parse_restart_mode(value, key=key)


def _parse_restart_mode(value: object, *, key: str) -> RestartMode:
    if not isinstance(value, str):
        raise ConfigError(f"{key} must be one of: git_checkout, pypi")
    normalized = value.strip().lower()
    if normalized not in {"git_checkout", "pypi"}:
        raise ConfigError(f"{key} must be one of: git_checkout, pypi")
    return cast(RestartMode, normalized)


def _operator_logins_with_default(
    data: dict[str, object], key: str, default: tuple[str, ...]
) -> tuple[str, ...]:
    value = data.get(key, list(default))
    if not isinstance(value, list):
        raise ConfigError(f"{key} must be a list of strings")
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ConfigError(f"{key} must be a list of strings")
        login = item.strip().lower()
        if not login:
            raise ConfigError(f"{key} entries must be non-empty strings")
        if login not in normalized:
            normalized.append(login)
    return tuple(normalized)


def _require_allowed_users(data: dict[str, object], key: str) -> frozenset[str]:
    value = data.get(key)
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{key} is required and must be a non-empty list of strings")

    out: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            raise ConfigError(f"{key} is required and must be a non-empty list of strings")
        normalized = item.strip().lower()
        if not normalized:
            raise ConfigError(f"{key} is required and must be a non-empty list of strings")
        out.add(normalized)
    return frozenset(out)
