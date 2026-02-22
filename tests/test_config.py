from __future__ import annotations

from pathlib import Path
import re

import pytest

from mergexo import config
from mergexo.config import AppConfig, ConfigError


def _write(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


def test_load_config_happy_path(tmp_path: Path) -> None:
    cfg_path = _write(
        tmp_path / "mergexo.toml",
        """
[runtime]
base_dir = "~/tmp/mergexo"
worker_count = 2
poll_interval_seconds = 60
enable_feedback_loop = true
enable_github_operations = true
restart_drain_timeout_seconds = 120
restart_default_mode = "git_checkout"
restart_supported_modes = ["git_checkout", "pypi"]
git_checkout_root = "~/code/mergexo"
service_python = "/usr/bin/python3"

[repo]
owner = "johnynek"
name = "repo"
default_branch = "main"
trigger_label = "agent:design"
bugfix_label = "agent:bugfix-custom"
small_job_label = "agent:small-custom"
coding_guidelines_path = "docs/guidelines.md"
design_docs_dir = "docs/design"
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
    assert loaded.runtime.worker_count == 2
    assert loaded.runtime.base_dir.as_posix().endswith("/tmp/mergexo")
    assert loaded.runtime.enable_feedback_loop is True
    assert loaded.runtime.enable_github_operations is True
    assert loaded.runtime.restart_drain_timeout_seconds == 120
    assert loaded.runtime.restart_default_mode == "git_checkout"
    assert loaded.runtime.restart_supported_modes == ("git_checkout", "pypi")
    assert loaded.runtime.git_checkout_root is not None
    assert loaded.runtime.git_checkout_root.as_posix().endswith("/code/mergexo")
    assert loaded.runtime.service_python == "/usr/bin/python3"
    assert loaded.repo.full_name == "johnynek/repo"
    assert loaded.repo.effective_remote_url == "git@github.com:johnynek/repo.git"
    assert loaded.repo.bugfix_label == "agent:bugfix-custom"
    assert loaded.repo.small_job_label == "agent:small-custom"
    assert loaded.repo.coding_guidelines_path == "docs/guidelines.md"
    assert loaded.repo.operations_issue_number == 99
    assert loaded.repo.operator_logins == ("alice", "bob")
    assert loaded.codex.extra_args == ("--full-auto",)
    assert loaded.auth.allowed_users == frozenset({"alice", "bob"})
    assert loaded.auth.allows("ALICE")
    assert loaded.auth.allows(" bob ")
    assert loaded.auth.allows("carol") is False
    assert loaded.auth.allows("   ") is False


def test_load_config_uses_explicit_remote(tmp_path: Path) -> None:
    cfg_path = _write(
        tmp_path / "mergexo.toml",
        """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5

[repo]
owner = "o"
name = "n"
default_branch = "main"
trigger_label = "l"
design_docs_dir = "docs/design"
remote_url = "git@github.com:example/custom.git"

[auth]
allowed_users = ["o"]
""".strip(),
    )

    loaded = config.load_config(cfg_path)
    assert loaded.repo.effective_remote_url == "git@github.com:example/custom.git"
    assert loaded.repo.bugfix_label == "agent:bugfix"
    assert loaded.repo.small_job_label == "agent:small-job"
    assert loaded.repo.coding_guidelines_path == "docs/python_style.md"
    assert loaded.runtime.enable_feedback_loop is False
    assert loaded.runtime.enable_github_operations is False
    assert loaded.runtime.restart_default_mode == "git_checkout"
    assert loaded.runtime.restart_supported_modes == ("git_checkout",)
    assert loaded.runtime.git_checkout_root is None
    assert loaded.repo.operations_issue_number is None
    assert loaded.repo.operator_logins == ()
    assert loaded.auth.allowed_users == frozenset({"o"})


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
default_branch = "main"
trigger_label = "l"
design_docs_dir = "docs/design"

[auth]
allowed_users = ["o"]
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
default_branch = "main"
trigger_label = "l"
design_docs_dir = "docs/design"

[auth]
allowed_users = ["o"]
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
default_branch = "main"
trigger_label = "l"
design_docs_dir = "docs/design"

[auth]
allowed_users = ["o"]
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
default_branch = "main"
trigger_label = "l"
design_docs_dir = "docs/design"

[auth]
allowed_users = ["o"]
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
default_branch = "main"
trigger_label = "l"
design_docs_dir = "docs/design"

[auth]
allowed_users = ["o"]
""".strip(),
            "restart_default_mode",
        ),
    ],
)
def test_load_config_errors(tmp_path: Path, content: str, expected: str) -> None:
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

    assert config._operator_logins_with_default({}, "k", ()) == ()
    assert config._operator_logins_with_default({"k": ["A", "a", "b"]}, "k", ()) == ("a", "b")
    with pytest.raises(ConfigError, match="list of strings"):
        config._operator_logins_with_default({"k": "alice"}, "k", ())
    with pytest.raises(ConfigError, match="list of strings"):
        config._operator_logins_with_default({"k": ["alice", 7]}, "k", ())
    with pytest.raises(ConfigError, match="non-empty"):
        config._operator_logins_with_default({"k": [""]}, "k", ())


@pytest.mark.parametrize(
    "content, expected",
    [
        (
            """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5

[repo]
owner = "o"
name = "n"
default_branch = "main"
trigger_label = "l"
design_docs_dir = "docs/design"
""".strip(),
            "[auth] is required and must be a TOML table",
        ),
        (
            """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5

[repo]
owner = "o"
name = "n"
default_branch = "main"
trigger_label = "l"
design_docs_dir = "docs/design"

[auth]
allowed_users = []
""".strip(),
            "allowed_users is required and must be a non-empty list of strings",
        ),
        (
            """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5

[repo]
owner = "o"
name = "n"
default_branch = "main"
trigger_label = "l"
design_docs_dir = "docs/design"

[auth]
allowed_users = ["ok", "   "]
""".strip(),
            "allowed_users is required and must be a non-empty list of strings",
        ),
    ],
)
def test_load_config_auth_errors(tmp_path: Path, content: str, expected: str) -> None:
    cfg_path = _write(tmp_path / "bad-auth.toml", content)
    with pytest.raises(ConfigError, match=re.escape(expected)):
        config.load_config(cfg_path)


def test_helper_require_allowed_users() -> None:
    assert config._require_allowed_users({"users": [" Alice ", "BOB"]}, "users") == frozenset(
        {"alice", "bob"}
    )
    with pytest.raises(ConfigError, match="non-empty list of strings"):
        config._require_allowed_users({"users": []}, "users")
    with pytest.raises(ConfigError, match="non-empty list of strings"):
        config._require_allowed_users({"users": ["ok", "   "]}, "users")
    with pytest.raises(ConfigError, match="non-empty list of strings"):
        config._require_allowed_users({"users": ["ok", 1]}, "users")
