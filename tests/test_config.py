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
enable_issue_comment_routing = true
enable_pr_actions_monitoring = true
enable_incremental_comment_fetch = true
comment_fetch_overlap_seconds = 7
comment_fetch_safe_backfill_seconds = 1000
pr_actions_log_tail_lines = 250
restart_drain_timeout_seconds = 120
restart_default_mode = "git_checkout"
restart_supported_modes = ["git_checkout", "pypi"]
git_checkout_root = "~/code/mergexo"
service_python = "/usr/bin/python3"
observability_refresh_seconds = 3
observability_default_window = "7d"
observability_row_limit = 150
observability_history_retention_days = 120

[repo]
owner = "johnynek"
name = "repo"
coding_guidelines_path = "docs/guidelines.md"
required_tests = "scripts/required-tests.sh"
pr_actions_feedback_policy = "first_fail"
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

[codex.repo.repo]
sandbox = "danger-full-access"
extra_args = ["--repo-mode"]
""".strip(),
    )

    loaded = config.load_config(cfg_path)

    assert isinstance(loaded, AppConfig)
    assert len(loaded.repos) == 1
    assert loaded.runtime.worker_count == 2
    assert loaded.runtime.base_dir.as_posix().endswith("/tmp/mergexo")
    assert loaded.runtime.enable_github_operations is True
    assert loaded.runtime.enable_issue_comment_routing is True
    assert loaded.runtime.enable_pr_actions_monitoring is True
    assert loaded.runtime.enable_incremental_comment_fetch is True
    assert loaded.runtime.comment_fetch_overlap_seconds == 7
    assert loaded.runtime.comment_fetch_safe_backfill_seconds == 1000
    assert loaded.runtime.pr_actions_log_tail_lines == 250
    assert loaded.runtime.restart_drain_timeout_seconds == 120
    assert loaded.runtime.restart_default_mode == "git_checkout"
    assert loaded.runtime.restart_supported_modes == ("git_checkout", "pypi")
    assert loaded.runtime.git_checkout_root is not None
    assert loaded.runtime.git_checkout_root.as_posix().endswith("/code/mergexo")
    assert loaded.runtime.service_python == "/usr/bin/python3"
    assert loaded.runtime.observability_refresh_seconds == 3
    assert loaded.runtime.observability_default_window == "7d"
    assert loaded.runtime.observability_row_limit == 150
    assert loaded.runtime.observability_history_retention_days == 120
    assert loaded.repo.repo_id == "repo"
    assert loaded.repo.full_name == "johnynek/repo"
    assert loaded.repo.default_branch == "main"
    assert loaded.repo.trigger_label == "agent:design"
    assert loaded.repo.bugfix_label == "agent:bugfix"
    assert loaded.repo.small_job_label == "agent:small-job"
    assert loaded.repo.design_docs_dir == "docs/design"
    assert loaded.repo.effective_remote_url == "git@github.com:johnynek/repo.git"
    assert loaded.repo.coding_guidelines_path == "docs/guidelines.md"
    assert loaded.repo.required_tests == "scripts/required-tests.sh"
    assert loaded.repo.pr_actions_feedback_policy == "first_fail"
    assert loaded.repo.test_file_regex is None
    assert loaded.repo.operations_issue_number == 99
    assert loaded.repo.operator_logins == ("alice", "bob")
    assert loaded.repo.allowed_users == frozenset({"alice", "bob"})
    assert loaded.repo.allows(" ALICE ")
    assert loaded.codex.sandbox == "workspace-write"
    assert loaded.codex.extra_args == ("--full-auto",)
    repo_codex = loaded.codex_for_repo(loaded.repo)
    assert repo_codex.model == "gpt"
    assert repo_codex.sandbox == "danger-full-access"
    assert repo_codex.profile == "default"
    assert repo_codex.extra_args == ("--repo-mode",)
    assert loaded.codex_for_repo("unknown") == loaded.codex
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
pr_actions_feedback_policy = "never"

[repo.bosatsu]
owner = "johnynek"
name = "bosatsu"
coding_guidelines_path = "docs/python_style.md"
""".strip(),
    )

    loaded = config.load_config(cfg_path)
    assert len(loaded.repos) == 2
    assert loaded.runtime.enable_issue_comment_routing is False
    assert loaded.runtime.enable_pr_actions_monitoring is False
    assert loaded.runtime.enable_incremental_comment_fetch is False
    assert loaded.runtime.comment_fetch_overlap_seconds == 5
    assert loaded.runtime.comment_fetch_safe_backfill_seconds == 86400
    assert loaded.runtime.pr_actions_log_tail_lines == 500
    assert loaded.runtime.observability_refresh_seconds == 2
    assert loaded.runtime.observability_default_window == "24h"
    assert loaded.runtime.observability_row_limit == 200
    assert loaded.runtime.observability_history_retention_days == 90

    by_id = {repo.repo_id: repo for repo in loaded.repos}
    assert by_id["mergexo"].name == "mergexo"
    assert by_id["mergexo"].full_name == "johnynek/mergexo"
    assert by_id["mergexo"].allowed_users == frozenset({"alice", "bob"})
    assert by_id["mergexo"].pr_actions_feedback_policy == "never"

    assert by_id["bosatsu"].name == "bosatsu"
    assert by_id["bosatsu"].full_name == "johnynek/bosatsu"
    # owner default when allowed_users omitted in multi-repo mode
    assert by_id["bosatsu"].allowed_users == frozenset({"johnynek"})
    assert by_id["bosatsu"].pr_actions_feedback_policy is None


@pytest.mark.parametrize("policy", ["never", "first_fail", "all_complete"])
def test_load_config_accepts_repo_actions_feedback_policy(tmp_path: Path, policy: str) -> None:
    cfg_path = _write(
        tmp_path / "mergexo.toml",
        f"""
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5

[repo]
owner = "owner"
name = "repo"
coding_guidelines_path = "docs/python_style.md"
pr_actions_feedback_policy = "{policy}"
""".strip(),
    )

    loaded = config.load_config(cfg_path)
    assert loaded.repo.pr_actions_feedback_policy == policy


def test_load_config_rejects_invalid_repo_actions_feedback_policy(tmp_path: Path) -> None:
    cfg_path = _write(
        tmp_path / "bad.toml",
        """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5

[repo]
owner = "owner"
name = "repo"
coding_guidelines_path = "docs/python_style.md"
pr_actions_feedback_policy = "eventually"
""".strip(),
    )

    with pytest.raises(ConfigError, match="pr_actions_feedback_policy must be one of"):
        config.load_config(cfg_path)


def test_load_config_keeps_runtime_monitoring_flag_for_fallback_when_repo_policy_missing(
    tmp_path: Path,
) -> None:
    cfg_path = _write(
        tmp_path / "mergexo.toml",
        """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5
enable_pr_actions_monitoring = true

[repo]
owner = "owner"
name = "repo"
coding_guidelines_path = "docs/python_style.md"
""".strip(),
    )

    loaded = config.load_config(cfg_path)
    assert loaded.runtime.enable_pr_actions_monitoring is True
    assert loaded.repo.pr_actions_feedback_policy is None


def test_load_config_codex_repo_overrides_inherit_global_defaults(tmp_path: Path) -> None:
    cfg_path = _write(
        tmp_path / "mergexo.toml",
        """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5

[repo.alpha]
owner = "o"
coding_guidelines_path = "docs/python_style.md"

[repo.beta]
owner = "o"
coding_guidelines_path = "docs/python_style.md"

[codex]
enabled = true
model = "gpt-global"
sandbox = "workspace-write"
extra_args = ["--full-auto"]

[codex.repo.alpha]
enabled = false
sandbox = "danger-full-access"

[codex.repo.beta]
profile = "strict"
extra_args = ["--beta-only"]
""".strip(),
    )

    loaded = config.load_config(cfg_path)
    by_id = {repo.repo_id: repo for repo in loaded.repos}
    alpha_codex = loaded.codex_for_repo(by_id["alpha"])
    beta_codex = loaded.codex_for_repo(by_id["beta"])

    assert alpha_codex.enabled is False
    assert alpha_codex.model == "gpt-global"
    assert alpha_codex.sandbox == "danger-full-access"
    assert alpha_codex.profile is None
    assert alpha_codex.extra_args == ("--full-auto",)

    assert beta_codex.enabled is True
    assert beta_codex.model == "gpt-global"
    assert beta_codex.sandbox == "workspace-write"
    assert beta_codex.profile == "strict"
    assert beta_codex.extra_args == ("--beta-only",)


@pytest.mark.parametrize(
    "runtime_lines,error",
    [
        ("comment_fetch_overlap_seconds = -1", "runtime.comment_fetch_overlap_seconds"),
        ("comment_fetch_safe_backfill_seconds = 0", "runtime.comment_fetch_safe_backfill_seconds"),
        (
            "comment_fetch_overlap_seconds = 10\ncomment_fetch_safe_backfill_seconds = 9",
            "runtime.comment_fetch_safe_backfill_seconds",
        ),
    ],
)
def test_load_config_rejects_invalid_incremental_fetch_bounds(
    tmp_path: Path,
    runtime_lines: str,
    error: str,
) -> None:
    cfg_path = _write(
        tmp_path / "bad.toml",
        (
            """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5
"""
            + runtime_lines
            + """

[repo]
owner = "o"
name = "n"
coding_guidelines_path = "docs/python_style.md"
""".rstrip()
        ),
    )
    with pytest.raises(ConfigError, match=error):
        config.load_config(cfg_path)


def test_load_config_rejects_codex_override_for_unknown_repo(tmp_path: Path) -> None:
    cfg_path = _write(
        tmp_path / "bad.toml",
        """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5

[repo]
owner = "o"
name = "known"
coding_guidelines_path = "docs/python_style.md"

[codex.repo.unknown]
enabled = false
""".strip(),
    )
    with pytest.raises(ConfigError, match="Unknown repo id"):
        config.load_config(cfg_path)


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


def test_load_config_rejects_invalid_required_tests_type(tmp_path: Path) -> None:
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
required_tests = 7
""".strip(),
    )
    with pytest.raises(ConfigError, match="required_tests must be a non-empty string if provided"):
        config.load_config(cfg_path)


def test_load_config_accepts_test_file_regex_as_string(tmp_path: Path) -> None:
    cfg_path = _write(
        tmp_path / "ok.toml",
        """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5

[repo]
owner = "o"
name = "n"
coding_guidelines_path = "docs/python_style.md"
test_file_regex = '^tests/.*\\.py$'
""".strip(),
    )

    loaded = config.load_config(cfg_path)
    assert loaded.repo.test_file_regex is not None
    assert tuple(pattern.pattern for pattern in loaded.repo.test_file_regex) == ("^tests/.*\\.py$",)


def test_load_config_accepts_test_file_regex_as_list(tmp_path: Path) -> None:
    cfg_path = _write(
        tmp_path / "ok.toml",
        """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5

[repo]
owner = "o"
name = "n"
coding_guidelines_path = "docs/python_style.md"
test_file_regex = ['^tests/.*\\.py$', '^integration/.*\\.scala$']
""".strip(),
    )

    loaded = config.load_config(cfg_path)
    assert loaded.repo.test_file_regex is not None
    assert tuple(pattern.pattern for pattern in loaded.repo.test_file_regex) == (
        "^tests/.*\\.py$",
        "^integration/.*\\.scala$",
    )


def test_load_config_rejects_invalid_test_file_regex_type(tmp_path: Path) -> None:
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
test_file_regex = 7
""".strip(),
    )

    with pytest.raises(
        ConfigError, match="test_file_regex must be a non-empty string or list of non-empty strings"
    ):
        config.load_config(cfg_path)


def test_load_config_rejects_invalid_test_file_regex_pattern(tmp_path: Path) -> None:
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
test_file_regex = "["
""".strip(),
    )

    with pytest.raises(ConfigError, match="test_file_regex contains invalid regex"):
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
pr_actions_log_tail_lines = 0

[repo]
owner = "o"
name = "n"
coding_guidelines_path = "docs/python_style.md"
""".strip(),
            "pr_actions_log_tail_lines",
        ),
        (
            """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5
pr_actions_log_tail_lines = 6001

[repo]
owner = "o"
name = "n"
coding_guidelines_path = "docs/python_style.md"
""".strip(),
            "pr_actions_log_tail_lines",
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
observability_refresh_seconds = 0

[repo]
owner = "o"
name = "n"
coding_guidelines_path = "docs/python_style.md"
""".strip(),
            "observability_refresh_seconds",
        ),
        (
            """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5
observability_history_retention_days = 0

[repo]
owner = "o"
name = "n"
coding_guidelines_path = "docs/python_style.md"
""".strip(),
            "observability_history_retention_days",
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
        (
            """
[runtime]
base_dir = "/tmp/x"
worker_count = 1
poll_interval_seconds = 5

[repo]
owner = "o"
name = "n"
coding_guidelines_path = "docs/python_style.md"

[codex]
repo = ["oops"]
""".strip(),
            "[codex.repo] must be a TOML table",
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
coding_guidelines_path = "docs/python_style.md"

[codex]
repo = { n = 1 }
""".strip(),
            "[codex.repo.n] must be a TOML table",
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

    assert config._optional_str_with_default({}, "k", "d") == "d"
    assert config._optional_str_with_default({"k": "v"}, "k", "d") == "v"
    with pytest.raises(ConfigError, match="non-empty string"):
        config._optional_str_with_default({"k": ""}, "k", "d")

    assert config._optional_regex_list({}, "k") is None
    compiled = config._optional_regex_list({"k": "^tests/"}, "k")
    assert compiled is not None
    assert tuple(pattern.pattern for pattern in compiled) == ("^tests/",)
    compiled_list = config._optional_regex_list({"k": ["^tests/", "^integration/"]}, "k")
    assert compiled_list is not None
    assert tuple(pattern.pattern for pattern in compiled_list) == ("^tests/", "^integration/")
    with pytest.raises(ConfigError, match="non-empty string or list of non-empty strings"):
        config._optional_regex_list({"k": []}, "k")
    with pytest.raises(ConfigError, match="non-empty string or list of non-empty strings"):
        config._optional_regex_list({"k": ["^tests/", 3]}, "k")
    with pytest.raises(ConfigError, match="contains invalid regex"):
        config._optional_regex_list({"k": "["}, "k")

    assert config._tuple_of_str({}, "k") == ()
    assert config._tuple_of_str({"k": ["a", "b"]}, "k") == ("a", "b")
    with pytest.raises(ConfigError, match="list of strings"):
        config._tuple_of_str({"k": "oops"}, "k")
    with pytest.raises(ConfigError, match="list of strings"):
        config._tuple_of_str({"k": ["ok", 3]}, "k")
    assert config._tuple_of_str_with_default({}, "k", ("base",)) == ("base",)
    assert config._tuple_of_str_with_default({"k": ["x"]}, "k", ("base",)) == ("x",)

    assert config._optional_positive_int({}, "k") is None
    assert config._optional_positive_int({"k": 2}, "k") == 2
    with pytest.raises(ConfigError, match="integer >= 1"):
        config._optional_positive_int({"k": 0}, "k")
    assert config._optional_positive_int_with_default({}, "k", 9) == 9
    assert config._optional_positive_int_with_default({"k": 3}, "k", 9) == 3
    with pytest.raises(ConfigError, match="integer >= 1"):
        config._optional_positive_int_with_default({"k": 0}, "k", 9)
    assert config._window_with_default({}, "w", "24h") == "24h"
    assert config._window_with_default({"w": "7d"}, "w", "24h") == "7d"
    with pytest.raises(ConfigError, match="must be one of"):
        config._window_with_default({"w": "2h"}, "w", "24h")

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

    assert config._optional_pr_actions_feedback_policy({}, "k") is None
    assert config._optional_pr_actions_feedback_policy({"k": " FIRST_FAIL "}, "k") == "first_fail"
    with pytest.raises(ConfigError, match="must be one of"):
        config._optional_pr_actions_feedback_policy({"k": "later"}, "k")
    with pytest.raises(ConfigError, match="must be one of"):
        config._optional_pr_actions_feedback_policy({"k": 1}, "k")


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

    override = config.CodexConfig(
        enabled=False,
        model="gpt",
        sandbox="danger-full-access",
        profile=None,
        extra_args=("--x",),
    )
    app_with_repo = config.AppConfig(
        runtime=app.runtime,
        repos=(repo,),
        codex=config.CodexConfig(
            enabled=True,
            model="base",
            sandbox="workspace-write",
            profile="default",
            extra_args=("--full-auto",),
        ),
        codex_overrides=((repo.repo_id, override),),
    )
    assert app_with_repo.codex_for_repo(repo) == override
    assert app_with_repo.codex_for_repo("missing") == app_with_repo.codex


def test_load_repo_configs_rejects_empty_and_non_string_keys() -> None:
    with pytest.raises(ConfigError, match=r"\[repo\] must define one repo configuration"):
        config._load_repo_configs(repo_data={}, auth_data=None)

    bad_repo_data = cast(dict[str, object], {1: {"owner": "o", "coding_guidelines_path": "p"}})
    with pytest.raises(ConfigError, match=r"\[repo\] must have string keys"):
        config._load_repo_configs(repo_data=bad_repo_data, auth_data=None)

    with pytest.raises(ConfigError, match=r"\[codex.repo\] must have string keys"):
        config._load_codex_overrides(
            codex_data=cast(dict[str, object], {"repo": cast(dict[str, object], {1: {}})}),
            repos=(),
            defaults=config.CodexConfig(
                enabled=True,
                model=None,
                sandbox=None,
                profile=None,
                extra_args=(),
            ),
        )

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
