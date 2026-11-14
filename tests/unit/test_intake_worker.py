"""The intake worker: new-music only, on its own queue."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from deejay_cog import intake_worker, worker


def _event(mode: str) -> dict:
    body = json.dumps({"type": "deejay.run", "version": 1, "payload": {"mode": mode}})
    return {
        "Records": [
            {
                "messageId": "m-0",
                "body": body,
                "attributes": {"ApproximateReceiveCount": "1"},
            }
        ]
    }


@pytest.fixture
def flows(monkeypatch: pytest.MonkeyPatch) -> dict[str, MagicMock]:
    fakes = {mode: MagicMock(name=mode) for mode in worker.MODES}
    monkeypatch.setattr(worker, "MODES", fakes)
    monkeypatch.setattr(worker, "post_run_finding", MagicMock())
    return fakes


OTHER_MODES = [m for m in worker.MODES if m != intake_worker.INTAKE_MODE]


def test_the_router_runs_new_music() -> None:
    assert intake_worker.INTAKE_MODE in worker.MODES


def test_new_music_runs(flows: dict[str, MagicMock]) -> None:
    result = intake_worker.lambda_handler(_event("new-music"), None)

    assert result == {"batchItemFailures": []}
    flows["new-music"].assert_called_once_with(run_id="m-0")


@pytest.mark.parametrize("mode", OTHER_MODES)
def test_any_other_mode_fails_and_runs_nothing(
    flows: dict[str, MagicMock], mode: str
) -> None:
    """A misrouted message comes back, and in time dead-letters."""
    result = intake_worker.lambda_handler(_event(mode), None)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-0"}]}
    for fake in flows.values():
        fake.assert_not_called()
