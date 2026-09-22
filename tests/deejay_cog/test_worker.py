"""The Lambda entrypoint: message shape, dispatch, and the delete rule."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from deejay_cog import worker


def _body(mode: object = "process-new-files", **overrides: object) -> str:
    message: dict[str, object] = {
        "type": "deejay.run",
        "version": 1,
        "payload": {"mode": mode},
    }
    message.update(overrides)
    return json.dumps(message)


def _event(*bodies: str) -> dict:
    return {
        "Records": [
            {
                "messageId": f"m-{i}",
                "body": body,
                "attributes": {"ApproximateReceiveCount": "1"},
            }
            for i, body in enumerate(bodies)
        ]
    }


@pytest.fixture
def flows(monkeypatch: pytest.MonkeyPatch) -> dict[str, MagicMock]:
    """Replace both flows; the tests are about routing, not the flows."""
    fakes = {mode: MagicMock(name=mode) for mode in worker.MODES}
    monkeypatch.setattr(worker, "MODES", fakes)
    return fakes


@pytest.fixture
def reported(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    fake = MagicMock()
    monkeypatch.setattr(worker, "post_run_finding", fake)
    return fake


def test_the_router_covers_exactly_the_modes_the_api_accepts() -> None:
    """The API's DeejayRunRequest accepts these two and no others."""
    assert set(worker.MODES) == {"process-new-files", "ingest-live-history"}
    assert set(worker.FLOW_NAMES) == set(worker.MODES)


@pytest.mark.parametrize("mode", ["process-new-files", "ingest-live-history"])
def test_a_message_runs_its_mode_under_the_message_id(
    flows: dict[str, MagicMock], reported: MagicMock, mode: str
) -> None:
    """The run id is the message id — the one the API answered the trigger with."""
    result = worker.lambda_handler(_event(_body(mode)), None)

    assert result == {"batchItemFailures": []}
    flows[mode].assert_called_once_with(run_id="m-0")
    for other, fake in flows.items():
        if other != mode:
            fake.assert_not_called()
    reported.assert_not_called()


@pytest.mark.parametrize(
    "body",
    [
        "not json",
        json.dumps(["a", "list"]),
        _body(version=2),
        _body(type="evaluation.repository"),
        _body(payload="not an object"),
        _body(mode="guess"),
        _body(mode=None),
    ],
)
def test_an_unprocessable_message_is_returned_not_dropped(
    flows: dict[str, MagicMock], reported: MagicMock, body: str
) -> None:
    """A producer bug goes to the DLQ where someone can see it."""
    result = worker.lambda_handler(_event(body), None)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-0"}]}
    for fake in flows.values():
        fake.assert_not_called()
    reported.assert_called_once()
    assert reported.call_args.args[1] == "ERROR"
    assert reported.call_args.kwargs["run_id"] == "m-0"


def test_a_failed_run_is_returned_and_reported_under_its_flow(
    flows: dict[str, MagicMock], reported: MagicMock
) -> None:
    """Replaces the Prefect failure hook: the flow never reached its own report."""
    flows["ingest-live-history"].side_effect = RuntimeError("drive down")

    result = worker.lambda_handler(_event(_body("ingest-live-history")), None)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-0"}]}
    flow_name, severity, text = reported.call_args.args
    assert flow_name == "ingest-live-history"
    assert severity == "ERROR"
    assert "RuntimeError: drive down" in text
    assert reported.call_args.kwargs == {"source": "queue_consumer", "run_id": "m-0"}


def test_only_the_failed_record_comes_back(
    flows: dict[str, MagicMock], reported: MagicMock
) -> None:
    """Per record, so a batch size above one never redelivers a good neighbour."""
    flows["process-new-files"].side_effect = RuntimeError("boom")

    result = worker.lambda_handler(
        _event(_body("ingest-live-history"), _body("process-new-files")), None
    )

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    flows["ingest-live-history"].assert_called_once_with(run_id="m-0")


def test_a_failing_report_does_not_turn_a_retry_into_a_delete(
    flows: dict[str, MagicMock], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The notification is not the job. The record must still come back."""
    flows["process-new-files"].side_effect = RuntimeError("boom")
    monkeypatch.setattr(
        worker, "post_run_finding", MagicMock(side_effect=OSError("api down"))
    )

    result = worker.lambda_handler(_event(_body()), None)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-0"}]}


@pytest.mark.parametrize("event", [{}, {"Records": []}, None, "junk"])
def test_an_empty_or_malformed_event_is_not_an_error(event: object) -> None:
    assert worker.lambda_handler(event, None) == {"batchItemFailures": []}  # type: ignore[arg-type]


def test_an_unprocessable_message_is_reported_once_not_per_receive(
    flows: dict[str, MagicMock], reported: MagicMock
) -> None:
    """A redelivery fails the same way; the DLQ alarm covers where it ends."""
    event = _event("not json")
    event["Records"][0]["attributes"]["ApproximateReceiveCount"] = "2"

    result = worker.lambda_handler(event, None)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-0"}]}
    reported.assert_not_called()
    for flow in flows.values():
        flow.assert_not_called()
