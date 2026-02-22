from __future__ import annotations

from pathlib import Path
import logging
import subprocess


class CommandError(RuntimeError):
    pass


LOGGER = logging.getLogger("mergexo.shell")


def _preview(text: str, *, limit: int = 200) -> str:
    compact = text.replace("\n", "\\n").strip()
    if not compact:
        return "<empty>"
    if len(compact) <= limit:
        return compact
    return f"{compact[:limit]}..."


def run(
    argv: list[str],
    *,
    cwd: Path | None = None,
    input_text: str | None = None,
    check: bool = True,
) -> str:
    proc = subprocess.run(
        argv,
        cwd=str(cwd) if cwd else None,
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
    )
    if check and proc.returncode != 0:
        LOGGER.error(
            "event=command_failed command=%s exit_code=%s stderr=%s stdout=%s",
            " ".join(argv),
            proc.returncode,
            _preview(proc.stderr),
            _preview(proc.stdout),
        )
        raise CommandError(
            "Command failed\n"
            f"cmd: {' '.join(argv)}\n"
            f"exit: {proc.returncode}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    return proc.stdout
