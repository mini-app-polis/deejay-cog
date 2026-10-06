"""scripts/check_doppler_keys.py: which names count, and what it reports."""

from __future__ import annotations

import importlib.util
import json
import subprocess
from pathlib import Path

import pytest

_PATH = Path(__file__).resolve().parents[2] / "scripts" / "check_doppler_keys.py"
_spec = importlib.util.spec_from_file_location("check_doppler_keys", _PATH)
assert _spec and _spec.loader
check = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check)


def test_uncommented_names_are_required_and_commented_ones_optional() -> None:
    text = "# header\nA_KEY=\nB_KEY=value\n# C_KEY=1\n#   D_KEY=\n# prose, not a name\n"

    assert check.declared_names(text) == (["A_KEY", "B_KEY"], ["C_KEY", "D_KEY"])


def test_the_repo_env_example_requires_the_secrets() -> None:
    required, _ = check.declared_names((check.ROOT / ".env.example").read_text())

    assert "DEEJAY_COG_API_KEY" in required
    assert "GOOGLE_CREDENTIALS_JSON" in required
    assert "LOGGING_LEVEL" not in required  # has a default


def test_the_project_comes_from_doppler_yaml() -> None:
    assert check.doppler_project((check.ROOT / "doppler.yaml").read_text()) == (
        "mini-app-polis-ecosystem"
    )


def test_it_asks_for_dev_and_reports_missing_names_without_values(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[list[str]] = []

    def run(args, **_kwargs):
        calls.append(args)
        out = json.dumps({"GOOGLE_CREDENTIALS_JSON": "secret-value"})
        return subprocess.CompletedProcess(args, 0, stdout=out, stderr="")

    monkeypatch.setattr(check.shutil, "which", lambda _name: "/usr/bin/doppler")
    monkeypatch.setattr(check.subprocess, "run", run)

    assert check.main() == 1

    args = calls[0]
    assert args[args.index("--config") + 1] == "dev"
    out = capsys.readouterr().out
    assert "MISSING (required): KAIANO_API_BASE_URL_DEV, DEEJAY_COG_API_KEY" in out
    assert "secret-value" not in out
