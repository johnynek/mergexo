from __future__ import annotations

from pathlib import Path
import re
from typing import cast

import pytest

from mergexo import config
from mergexo.config import AppConfig, ConfigError


def _write(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


def test_load_config_legacy_single_repo_normalizes_and_applies_defaults(tmp_path: Path) -> None:
    cfg_path = _write(
        tmp_path / "mergexo.toml",
        """
[runtime]
base_dir = "~/tmp/mergexo"
worker_count = 2
poll_interval_seconds = 60
enable_github_operations = true
restart_drain_timeout_seconds = 120
restart_default_mode = "git_checkout"
restart_supported_modes = ["git_checkout", "pypi"]
git_checkout_root = "~/code/mergexo"
service_python = "/usr/bin/python3"

[repo]
owner = "johnynek"
name = "repo"
coding_guidelines_path = "docs/guidelines.md"
local_clone_source = "/tmp/local.git"
operations_issue_number = 99
operator_logins = ["Alice", "bob"]

[auth]
allowed_users = [" Alice ", "BOB", "alice"]

[codex]
enabled = true
model = "gpt"
sandbox = "workspace-write"
profile = "default"
extra_args = ["--full-auto"]
""".strip(),
    )

    loaded = config.load_config(cfg_path)

    assert isinstance(loaded, AppConfig)
    assert len(loaded.repos) == 1
    assert loaded.runtime.worker_count == 2
    assert loaded.runtime.base_dir.as_posix().endswith("/tmp/mergexo")
    assert loaded.runtime.enable_github_operations is True
    assert loaded.runtime.restart_drain_timeout_seconds == 120
    assert loaded.runtime.restart_default_mode == "git_checkout"
    assert loaded.runtime.restart_supported_modes == ("git_checkout", "pypi")
    assert loaded.runtime.git_checkout_root is not None
    assert loaded.runtime.git_checkout_root.as_posix().endswith("/code/mergexo")
    assert loaded.runtime.service_python == "/usr/bin/python3"
    assert loaded.repo.repo_id == "repo"
    assert loaded.repo.full_name == "johnynek/repo"
    assert loaded.repo.default_branch == "main"
    assert loaded.repo.trigger_label == "agent:design"
    assert loaded.repo.bugfix_label == "agent:bugfix"
    assert loaded.repo.small_job_label == "agent:small-job"
    assert loaded.repo.design_docs_dir == "docs/design"
    assert loaded.repo.effective_remote_url == "git@github.com:johnynek/repo.git"
    assert loaded.repo.coding_guidelines_path == "docs/guidelines.md"
    assert loaded.repo.operations_issue_number == 99
    assert loaded.repo.operator_logins == ("alice", "bob")
    assert loaded.repo.allowed_users == frozenset({"alice", "bob"})
    assert loaded.repo.allows(" ALICE ")
    assert loaded.codex.extra_args == ("--full-auto",)
    # Single-repo compatibility shim:
    assert loaded.auth.allowed_users == frozenset({"alice", "bob"})


def test_load_config_multi_repo_keyed_tables_and_name_inference(tmp_path: Path) -> None:
    cfg_path = _write(
        tmp_path / "mergexo.toml",
        """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5

[repo.mergexo]
owner = "johnynek"
coding_guidelines_path = "docs/python_style.md"
allowed_users = ["Alice", "bob", "alice"]

[repo.bosatsu]
owner = "johnynek"
name = "bosatsu"
coding_guidelines_path = "docs/python_style.md"
""".strip(),
    )

    loaded = config.load_config(cfg_path)
    assert len(loaded.repos) == 2

    by_id = {repo.repo_id: repo for repo in loaded.repos}
    assert by_id["mergexo"].name == "mergexo"
    assert by_id["mergexo"].full_name == "johnynek/mergexo"
    assert by_id["mergexo"].allowed_users == frozenset({"alice", "bob"})

    assert by_id["bosatsu"].name == "bosatsu"
    assert by_id["bosatsu"].full_name == "johnynek/bosatsu"
    # owner default when allowed_users omitted in multi-repo mode
    assert by_id["bosatsu"].allowed_users == frozenset({"johnynek"})


def test_load_config_legacy_defaults_owner_when_no_allowlists(tmp_path: Path) -> None:
    cfg_path = _write(
        tmp_path / "mergexo.toml",
        """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5

[repo]
owner = "owner"
name = "repo"
coding_guidelines_path = "docs/python_style.md"
""".strip(),
    )
    loaded = config.load_config(cfg_path)
    assert loaded.repo.allowed_users == frozenset({"owner"})


def test_load_config_legacy_repo_allowed_users_overrides_auth(tmp_path: Path) -> None:
    cfg_path = _write(
        tmp_path / "mergexo.toml",
        """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5

[repo]
owner = "owner"
name = "repo"
coding_guidelines_path = "docs/python_style.md"
allowed_users = ["repo-user"]

[auth]
allowed_users = ["auth-user"]
""".strip(),
    )
    loaded = config.load_config(cfg_path)
    assert loaded.repo.allowed_users == frozenset({"repo-user"})


def test_load_config_rejects_missing_coding_guidelines(tmp_path: Path) -> None:
    cfg_path = _write(
        tmp_path / "bad.toml",
        """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5

[repo]
owner = "o"
name = "n"
""".strip(),
    )
    with pytest.raises(ConfigError, match=re.escape("coding_guidelines_path is required")):
        config.load_config(cfg_path)


def test_load_config_rejects_mixed_repo_shapes(tmp_path: Path) -> None:
    cfg_path = _write(
        tmp_path / "bad.toml",
        """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5

[repo]
owner = "o"
name = "n"
coding_guidelines_path = "docs/python_style.md"

[repo.other]
owner = "o"
coding_guidelines_path = "docs/python_style.md"
""".strip(),
    )
    with pytest.raises(ConfigError, match="Cannot mix legacy"):
        config.load_config(cfg_path)


def test_load_config_rejects_duplicate_repo_full_names(tmp_path: Path) -> None:
    cfg_path = _write(
        tmp_path / "bad.toml",
        """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5

[repo.a]
owner = "o"
name = "same"
coding_guidelines_path = "docs/python_style.md"

[repo.b]
owner = "o"
name = "same"
coding_guidelines_path = "docs/python_style.md"
""".strip(),
    )
    with pytest.raises(ConfigError, match="Duplicate repo full_name"):
        config.load_config(cfg_path)


@pytest.mark.parametrize(
    "content, expected",
    [
        ("[repo]\nname='x'", "[runtime] is required"),
        (
            """
[runtime]
base_dir = "/tmp/x"
worker_count = 0
poll_interval_seconds = 60

[repo]
owner = "o"
name = "n"
coding_guidelines_path = "docs/python_style.md"
""".strip(),
            "worker_count",
        ),
        (
            """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 1

[repo]
owner = "o"
name = "n"
coding_guidelines_path = "docs/python_style.md"
""".strip(),
            "poll_interval_seconds",
        ),
        (
            """
codex = []

[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5

[repo]
owner = "o"
name = "n"
coding_guidelines_path = "docs/python_style.md"
""".strip(),
            "[codex] must be a TOML table",
        ),
        (
            """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5
restart_drain_timeout_seconds = 0

[repo]
owner = "o"
name = "n"
coding_guidelines_path = "docs/python_style.md"
""".strip(),
            "restart_drain_timeout_seconds",
        ),
        (
            """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5
restart_default_mode = "pypi"
restart_supported_modes = ["git_checkout"]

[repo]
owner = "o"
name = "n"
coding_guidelines_path = "docs/python_style.md"
""".strip(),
            "restart_default_mode",
        ),
    ],
)
def test_load_config_runtime_and_shape_errors(tmp_path: Path, content: str, expected: str) -> None:
    cfg_path = _write(tmp_path / "bad.toml", content)
    with pytest.raises(ConfigError, match=re.escape(expected)):
        config.load_config(cfg_path)


def test_helper_require_table_and_strings() -> None:
    assert config._require_table({"x": {}}, "x") == {}

    with pytest.raises(ConfigError, match="required and must be a TOML table"):
        config._require_table({"x": 3}, "x")

    with pytest.raises(ConfigError, match="must have string keys"):
        config._require_table({"x": {1: "v"}}, "x")

    assert config._require_str({"k": "v"}, "k") == "v"
    with pytest.raises(ConfigError, match="required and must be a non-empty string"):
        config._require_str({"k": ""}, "k")

    assert config._optional_str({}, "k") is None
    assert config._optional_str({"k": "v"}, "k") == "v"
    with pytest.raises(ConfigError, match="non-empty string"):
        config._optional_str({"k": ""}, "k")

    assert config._optional_table({}, "x") is None
    assert config._optional_table({"x": {}}, "x") == {}
    with pytest.raises(ConfigError, match="must be a TOML table"):
        config._optional_table({"x": 1}, "x")
    with pytest.raises(ConfigError, match="must have string keys"):
        config._optional_table({"x": cast(dict[str, object], {1: "v"})}, "x")

    with pytest.raises(ConfigError, match="must be a TOML table"):
        config._require_repo_table(1, table_name="[repo.bad]")
    with pytest.raises(ConfigError, match="must have string keys"):
        config._require_repo_table(cast(object, {1: "v"}), table_name="[repo.bad]")


def test_helper_numeric_bool_and_tuple() -> None:
    assert config._require_int({"k": 3}, "k") == 3
    with pytest.raises(ConfigError, match="must be an integer"):
        config._require_int({"k": "3"}, "k")
    assert config._int_with_default({}, "k", 7) == 7
    with pytest.raises(ConfigError, match="must be an integer"):
        config._int_with_default({"k": "x"}, "k", 7)

    assert config._bool_with_default({}, "k", True) is True
    assert config._bool_with_default({"k": False}, "k", True) is False
    with pytest.raises(ConfigError, match="must be a boolean"):
        config._bool_with_default({"k": "yes"}, "k", True)

    assert config._str_with_default({}, "k", "default") == "default"
    with pytest.raises(ConfigError, match="non-empty string"):
        config._str_with_default({"k": ""}, "k", "default")

    assert config._tuple_of_str({}, "k") == ()
    assert config._tuple_of_str({"k": ["a", "b"]}, "k") == ("a", "b")
    with pytest.raises(ConfigError, match="list of strings"):
        config._tuple_of_str({"k": "oops"}, "k")
    with pytest.raises(ConfigError, match="list of strings"):
        config._tuple_of_str({"k": ["ok", 3]}, "k")

    assert config._optional_positive_int({}, "k") is None
    assert config._optional_positive_int({"k": 2}, "k") == 2
    with pytest.raises(ConfigError, match="integer >= 1"):
        config._optional_positive_int({"k": 0}, "k")

    assert config._optional_path({}, "k") is None
    assert config._optional_path({"k": "~/tmp"}, "k") is not None

    assert config._restart_mode_with_default({}, "k", "git_checkout") == "git_checkout"
    assert config._restart_modes_with_default({}, "k", ("git_checkout",)) == ("git_checkout",)
    assert config._restart_modes_with_default(
        {"k": ["git_checkout", "pypi"]}, "k", ("git_checkout",)
    ) == ("git_checkout", "pypi")
    with pytest.raises(ConfigError, match="at least one"):
        config._restart_modes_with_default({"k": []}, "k", ("git_checkout",))
    with pytest.raises(ConfigError, match="list of restart modes"):
        config._restart_modes_with_default({"k": "git_checkout"}, "k", ("git_checkout",))
    with pytest.raises(ConfigError, match="one of"):
        config._parse_restart_mode("bad", key="k")
    with pytest.raises(ConfigError, match="one of"):
        config._parse_restart_mode(1, key="k")


def test_repo_auth_and_app_config_helpers() -> None:
    repo = config.RepoConfig(
        repo_id="id",
        owner="owner",
        name="repo",
        default_branch="main",
        trigger_label="agent:design",
        bugfix_label="agent:bugfix",
        small_job_label="agent:small-job",
        coding_guidelines_path="docs/python_style.md",
        design_docs_dir="docs/design",
        allowed_users=frozenset({"alice"}),
        local_clone_source=None,
        remote_url="https://example.invalid/repo.git",
    )
    assert repo.effective_remote_url == "https://example.invalid/repo.git"
    assert repo.allows("") is False
    assert repo.allows(" alice ")

    auth = config.AuthConfig(allowed_users=frozenset({"bob"}))
    assert auth.allows("") is False
    assert auth.allows(" BOB ")

    app = config.AppConfig(
        runtime=config.RuntimeConfig(
            base_dir=Path("/tmp"),
            worker_count=1,
            poll_interval_seconds=60,
        ),
        repos=(),
        codex=config.CodexConfig(
            enabled=True,
            model=None,
            sandbox=None,
            profile=None,
            extra_args=(),
        ),
    )
    with pytest.raises(RuntimeError, match="AppConfig.repos is empty"):
        _ = app.repo


def test_load_repo_configs_rejects_empty_and_non_string_keys() -> None:
    with pytest.raises(ConfigError, match=r"\[repo\] must define one repo configuration"):
        config._load_repo_configs(repo_data={}, auth_data=None)

    bad_repo_data = cast(dict[str, object], {1: {"owner": "o", "coding_guidelines_path": "p"}})
    with pytest.raises(ConfigError, match=r"\[repo\] must have string keys"):
        config._load_repo_configs(repo_data=bad_repo_data, auth_data=None)

    assert config._operator_logins_with_default({}, "k", ()) == ()
    assert config._operator_logins_with_default({"k": ["A", "a", "b"]}, "k", ()) == ("a", "b")
    with pytest.raises(ConfigError, match="list of strings"):
        config._operator_logins_with_default({"k": "alice"}, "k", ())
    with pytest.raises(ConfigError, match="list of strings"):
        config._operator_logins_with_default({"k": ["alice", 7]}, "k", ())
    with pytest.raises(ConfigError, match="non-empty"):
        config._operator_logins_with_default({"k": [""]}, "k", ())


def test_helper_allowed_user_parsing() -> None:
    assert config._require_allowed_users({"users": [" Alice ", "BOB"]}, "users") == frozenset(
        {"alice", "bob"}
    )
    assert config._optional_allowed_users({}, "users") is None
    assert config._default_allowed_users(owner="Owner") == frozenset({"owner"})
    with pytest.raises(ConfigError, match="non-empty list of strings"):
        config._require_allowed_users({"users": []}, "users")
    with pytest.raises(ConfigError, match="non-empty list of strings"):
        config._require_allowed_users({"users": ["ok", "   "]}, "users")
    with pytest.raises(ConfigError, match="non-empty list of strings"):
        config._require_allowed_users({"users": ["ok", 1]}, "users")
