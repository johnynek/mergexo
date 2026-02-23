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
    enable_github_operations: bool = False
    enable_issue_comment_routing: bool = False
    restart_drain_timeout_seconds: int = 900
    restart_default_mode: RestartMode = "git_checkout"
    restart_supported_modes: tuple[RestartMode, ...] = ("git_checkout",)
    git_checkout_root: Path | None = None
    service_python: str | None = None


@dataclass(frozen=True)
class RepoConfig:
    repo_id: str
    owner: str
    name: str
    default_branch: str
    trigger_label: str
    bugfix_label: str
    small_job_label: str
    coding_guidelines_path: str
    design_docs_dir: str
    allowed_users: frozenset[str]
    local_clone_source: str | None
    remote_url: str | None
    required_tests: str | None = None
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

    def allows(self, login: str) -> bool:
        normalized = login.strip().lower()
        if not normalized:
            return False
        return normalized in self.allowed_users


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
    repos: tuple[RepoConfig, ...]
    codex: CodexConfig
    codex_overrides: tuple[tuple[str, CodexConfig], ...] = ()

    @property
    def repo(self) -> RepoConfig:
        if not self.repos:
            raise RuntimeError("AppConfig.repos is empty")
        return self.repos[0]

    @property
    def auth(self) -> AuthConfig:
        # Single-repo compatibility shim for existing code/tests.
        return AuthConfig(allowed_users=self.repo.allowed_users)

    def codex_for_repo(self, repo: RepoConfig | str) -> CodexConfig:
        repo_id = repo.repo_id if isinstance(repo, RepoConfig) else repo
        for configured_repo_id, codex in self.codex_overrides:
            if configured_repo_id == repo_id:
                return codex
        return self.codex


class ConfigError(ValueError):
    pass


def load_config(path: Path) -> AppConfig:
    with path.open("rb") as fh:
        data = tomllib.load(fh)

    runtime_data = _require_table(data, "runtime")
    repo_data = _require_table(data, "repo")
    raw_codex_data = data.get("codex", {})
    if not isinstance(raw_codex_data, dict):
        raise ConfigError("[codex] must be a TOML table")
    codex_data = cast(dict[str, object], raw_codex_data)
    auth_data = _optional_table(data, "auth")

    runtime = RuntimeConfig(
        base_dir=Path(_require_str(runtime_data, "base_dir")).expanduser(),
        worker_count=_require_int(runtime_data, "worker_count"),
        poll_interval_seconds=_require_int(runtime_data, "poll_interval_seconds"),
        enable_github_operations=_bool_with_default(
            runtime_data, "enable_github_operations", False
        ),
        enable_issue_comment_routing=_bool_with_default(
            runtime_data, "enable_issue_comment_routing", False
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

    repos = _load_repo_configs(repo_data=repo_data, auth_data=auth_data)

    codex = _parse_codex_config(codex_data=codex_data)
    codex_overrides = _load_codex_overrides(codex_data=codex_data, repos=repos, defaults=codex)

    return AppConfig(
        runtime=runtime,
        repos=repos,
        codex=codex,
        codex_overrides=codex_overrides,
    )


def _load_repo_configs(
    *, repo_data: dict[str, object], auth_data: dict[str, object] | None
) -> tuple[RepoConfig, ...]:
    if not repo_data:
        raise ConfigError("[repo] must define one repo configuration")

    keyed_items = [(key, value) for key, value in repo_data.items() if isinstance(value, dict)]
    scalar_items = [(key, value) for key, value in repo_data.items() if not isinstance(value, dict)]

    if keyed_items and scalar_items:
        raise ConfigError("Cannot mix legacy [repo] fields with [repo.<id>] tables")

    if scalar_items:
        return (_parse_legacy_repo_config(repo_data=repo_data, auth_data=auth_data),)

    repos: list[RepoConfig] = []
    for repo_id, raw_value in sorted(keyed_items):
        if not isinstance(repo_id, str):
            raise ConfigError("[repo] must have string keys")
        repo_table = _require_repo_table(raw_value, table_name=f"[repo.{repo_id}]")
        repos.append(_parse_keyed_repo_config(repo_id=repo_id, repo_data=repo_table))
    _ensure_unique_full_names(repos)
    return tuple(repos)


def _parse_legacy_repo_config(
    *, repo_data: dict[str, object], auth_data: dict[str, object] | None
) -> RepoConfig:
    owner = _require_str(repo_data, "owner")
    name = _require_str(repo_data, "name")
    repo_allowed_users = _optional_allowed_users(repo_data, "allowed_users")
    if repo_allowed_users is not None:
        allowed_users = repo_allowed_users
    elif auth_data is not None and "allowed_users" in auth_data:
        allowed_users = _require_allowed_users(auth_data, "allowed_users")
    else:
        allowed_users = _default_allowed_users(owner=owner)
    return _parse_repo_config(
        repo_id=name,
        repo_data=repo_data,
        owner=owner,
        name=name,
        allowed_users=allowed_users,
    )


def _parse_keyed_repo_config(*, repo_id: str, repo_data: dict[str, object]) -> RepoConfig:
    owner = _require_str(repo_data, "owner")
    name = _str_with_default(repo_data, "name", repo_id)
    allowed_users = _optional_allowed_users(repo_data, "allowed_users")
    if allowed_users is None:
        allowed_users = _default_allowed_users(owner=owner)
    return _parse_repo_config(
        repo_id=repo_id,
        repo_data=repo_data,
        owner=owner,
        name=name,
        allowed_users=allowed_users,
    )


def _parse_repo_config(
    *,
    repo_id: str,
    repo_data: dict[str, object],
    owner: str,
    name: str,
    allowed_users: frozenset[str],
) -> RepoConfig:
    return RepoConfig(
        repo_id=repo_id,
        owner=owner,
        name=name,
        default_branch=_str_with_default(repo_data, "default_branch", "main"),
        trigger_label=_str_with_default(repo_data, "trigger_label", "agent:design"),
        bugfix_label=_str_with_default(repo_data, "bugfix_label", "agent:bugfix"),
        small_job_label=_str_with_default(repo_data, "small_job_label", "agent:small-job"),
        coding_guidelines_path=_require_str(repo_data, "coding_guidelines_path"),
        design_docs_dir=_str_with_default(repo_data, "design_docs_dir", "docs/design"),
        allowed_users=allowed_users,
        local_clone_source=_optional_str(repo_data, "local_clone_source"),
        remote_url=_optional_str(repo_data, "remote_url"),
        required_tests=_optional_str(repo_data, "required_tests"),
        operations_issue_number=_optional_positive_int(repo_data, "operations_issue_number"),
        operator_logins=_operator_logins_with_default(repo_data, "operator_logins", ()),
    )


def _parse_codex_config(
    *, codex_data: dict[str, object], defaults: CodexConfig | None = None
) -> CodexConfig:
    enabled_default = defaults.enabled if defaults is not None else True
    model_default = defaults.model if defaults is not None else None
    sandbox_default = defaults.sandbox if defaults is not None else None
    profile_default = defaults.profile if defaults is not None else None
    extra_args_default = defaults.extra_args if defaults is not None else ()
    return CodexConfig(
        enabled=_bool_with_default(codex_data, "enabled", enabled_default),
        model=_optional_str_with_default(codex_data, "model", model_default),
        sandbox=_optional_str_with_default(codex_data, "sandbox", sandbox_default),
        profile=_optional_str_with_default(codex_data, "profile", profile_default),
        extra_args=_tuple_of_str_with_default(codex_data, "extra_args", extra_args_default),
    )


def _load_codex_overrides(
    *,
    codex_data: dict[str, object],
    repos: tuple[RepoConfig, ...],
    defaults: CodexConfig,
) -> tuple[tuple[str, CodexConfig], ...]:
    repo_data = codex_data.get("repo")
    if repo_data is None:
        return ()
    if not isinstance(repo_data, dict):
        raise ConfigError("[codex.repo] must be a TOML table")
    if not all(isinstance(item, str) for item in repo_data.keys()):
        raise ConfigError("[codex.repo] must have string keys")
    normalized_repo_data = cast(dict[str, object], repo_data)

    configured_repo_ids = {repo.repo_id for repo in repos}
    overrides: list[tuple[str, CodexConfig]] = []
    for repo_id, raw_value in sorted(normalized_repo_data.items()):
        if repo_id not in configured_repo_ids:
            available = ", ".join(sorted(configured_repo_ids))
            raise ConfigError(
                f"Unknown repo id {repo_id!r} in [codex.repo.{repo_id}]; expected one of: "
                f"{available}"
            )
        override_table = _require_repo_table(raw_value, table_name=f"[codex.repo.{repo_id}]")
        overrides.append(
            (
                repo_id,
                _parse_codex_config(codex_data=override_table, defaults=defaults),
            )
        )
    return tuple(overrides)


def _require_table(data: dict[str, object], key: str) -> dict[str, object]:
    value = data.get(key)
    if not isinstance(value, dict):
        raise ConfigError(f"[{key}] is required and must be a TOML table")
    if not all(isinstance(item, str) for item in value.keys()):
        raise ConfigError(f"[{key}] must have string keys")
    return cast(dict[str, object], value)


def _optional_table(data: dict[str, object], key: str) -> dict[str, object] | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ConfigError(f"[{key}] must be a TOML table when provided")
    if not all(isinstance(item, str) for item in value.keys()):
        raise ConfigError(f"[{key}] must have string keys")
    return cast(dict[str, object], value)


def _require_repo_table(value: object, *, table_name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ConfigError(f"{table_name} must be a TOML table")
    if not all(isinstance(item, str) for item in value.keys()):
        raise ConfigError(f"{table_name} must have string keys")
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


def _optional_str_with_default(
    data: dict[str, object], key: str, default: str | None
) -> str | None:
    if key not in data:
        return default
    return _optional_str(data, key)


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


def _tuple_of_str_with_default(
    data: dict[str, object], key: str, default: tuple[str, ...]
) -> tuple[str, ...]:
    if key not in data:
        return default
    return _tuple_of_str(data, key)


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


def _optional_allowed_users(data: dict[str, object], key: str) -> frozenset[str] | None:
    if key not in data:
        return None
    return _require_allowed_users(data, key)


def _default_allowed_users(*, owner: str) -> frozenset[str]:
    return frozenset({owner.strip().lower()})


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


def _ensure_unique_full_names(repos: list[RepoConfig]) -> None:
    seen: dict[str, str] = {}
    for repo in repos:
        existing_id = seen.get(repo.full_name)
        if existing_id is not None:
            raise ConfigError(
                f"Duplicate repo full_name {repo.full_name!r} across repo ids "
                f"{existing_id!r} and {repo.repo_id!r}"
            )
        seen[repo.full_name] = repo.repo_id
