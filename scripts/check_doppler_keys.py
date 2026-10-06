"""Check that Doppler's dev config holds every name this repo requires.

The required names are the uncommented ``NAME=`` lines in ``.env.example``;
commented ones are optional and only listed. The project comes from
``doppler.yaml``, and the config is always ``dev``: local runs never use
``prd``.

Prints names only, never values. Exits 1 when a required name is missing,
so it can gate a make target or a pre-commit hook.

Usage:
    uv run python scripts/check_doppler_keys.py
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONFIG = "dev"

_REQUIRED = re.compile(r"^([A-Z][A-Z0-9_]*)=")
_OPTIONAL = re.compile(r"^#\s*([A-Z][A-Z0-9_]*)=")


def declared_names(env_example: str) -> tuple[list[str], list[str]]:
    """``(required, optional)`` names from the text of ``.env.example``."""
    required: list[str] = []
    optional: list[str] = []
    for line in env_example.splitlines():
        line = line.strip()
        if m := _REQUIRED.match(line):
            required.append(m.group(1))
        elif m := _OPTIONAL.match(line):
            optional.append(m.group(1))
    return required, optional


def doppler_project(doppler_yaml: str) -> str:
    """The project ``doppler.yaml`` pins this repo to."""
    m = re.search(r"^\s*-?\s*project:\s*(\S+)\s*$", doppler_yaml, re.MULTILINE)
    if not m:
        raise SystemExit("doppler.yaml names no project")
    return m.group(1)


def doppler_names(project: str) -> set[str]:
    """The secret names in ``project``/dev. Values are read and discarded."""
    if shutil.which("doppler") is None:
        raise SystemExit(
            "The Doppler CLI is not installed: brew install dopplerhq/cli/doppler"
        )
    result = subprocess.run(
        [
            "doppler",
            "secrets",
            "download",
            "--no-file",
            "--format",
            "json",
            "--project",
            project,
            "--config",
            CONFIG,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(f"doppler failed: {result.stderr.strip()}")
    return set(json.loads(result.stdout))


def main() -> int:
    required, optional = declared_names((ROOT / ".env.example").read_text())
    project = doppler_project((ROOT / "doppler.yaml").read_text())
    have = doppler_names(project)

    missing = [n for n in required if n not in have]
    absent_optional = [n for n in optional if n not in have]

    print(f"Doppler {project}/{CONFIG}: {len(required)} required names checked.")
    if absent_optional:
        print("Optional, not set (defaults apply):", ", ".join(absent_optional))
    if missing:
        print("MISSING (required):", ", ".join(missing))
        return 1
    print("All required names are present.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
