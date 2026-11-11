"""The library worker: library-* modes only, on their own queue."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from deejay_cog import library_worker, worker


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


LIBRARY_MODES = [m for m in worker.MODES if m.startswith("library-")]
OTHER_MODES = [m for m in worker.MODES if not m.startswith("library-")]


def test_the_router_has_library_modes_and_others() -> None:
    assert LIBRARY_MODES
    assert OTHER_MODES


@pytest.mark.parametrize("mode", LIBRARY_MODES)
def test_a_library_mode_runs(flows: dict[str, MagicMock], mode: str) -> None:
    result = library_worker.lambda_handler(_event(mode), None)

    assert result == {"batchItemFailures": []}
    flows[mode].assert_called_once_with(run_id="m-0")


@pytest.mark.parametrize("mode", OTHER_MODES)
def test_any_other_mode_fails_and_runs_nothing(
    flows: dict[str, MagicMock], mode: str
) -> None:
    """A misrouted message comes back, and in time dead-letters."""
    result = library_worker.lambda_handler(_event(mode), None)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-0"}]}
    for fake in flows.values():
        fake.assert_not_called()


@pytest.mark.parametrize("mode", LIBRARY_MODES + OTHER_MODES)
def test_the_shared_worker_still_runs_every_mode(
    flows: dict[str, MagicMock], mode: str
) -> None:
    result = worker.lambda_handler(_event(mode), None)

    assert result == {"batchItemFailures": []}
    flows[mode].assert_called_once_with(run_id="m-0")
