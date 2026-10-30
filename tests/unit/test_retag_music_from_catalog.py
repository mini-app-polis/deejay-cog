"""The retag-music worker mode: the cap, the deadline stop and the report."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from mini_app_polis import pipeline_status
from mini_app_polis.lambda_worker import RunOutOfTime

from deejay_cog import retag_music_from_catalog as flow
from deejay_cog.music_tags import FileResult, Outcome


def _files(*names: str) -> list[SimpleNamespace]:
    return [SimpleNamespace(id=f"id-{n}", name=n) for n in names]


@pytest.fixture
def outcomes(monkeypatch: pytest.MonkeyPatch) -> dict[str, Outcome | Exception]:
    """Per file name, what ``tag_file`` does; the lookup itself is tested in
    test_music_tags."""
    table: dict[str, Outcome | Exception] = {}

    def fake_tag_file(drive, api, file_id):
        name = file_id.removeprefix("id-")
        outcome = table.get(name, Outcome.TAGGED)
        if isinstance(outcome, Exception):
            raise outcome
        return FileResult(file_id, name, outcome)

    monkeypatch.setattr(flow, "tag_file", fake_tag_file)
    return table


@pytest.fixture
def deadline(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    fake = MagicMock(return_value=False)
    monkeypatch.setattr(flow, "shared_deadline_passed", fake)
    return fake


@pytest.fixture
def posted(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    fake = MagicMock()
    monkeypatch.setattr(pipeline_status, "post_run_finding", fake)
    return fake


@pytest.fixture
def folder(monkeypatch: pytest.MonkeyPatch) -> list[SimpleNamespace]:
    listed: list[SimpleNamespace] = []
    monkeypatch.setattr(flow, "GoogleAPI", MagicMock())
    monkeypatch.setattr(flow, "api_client", MagicMock())
    monkeypatch.setattr(flow, "_list_audio_files", lambda *_: listed)
    return listed


def test_the_worker_runs_this_mode() -> None:
    from deejay_cog import worker

    assert worker.MODES["retag-music"] is flow.retag_music


def test_every_file_is_scanned_without_a_cap(outcomes, deadline) -> None:
    summary = flow.RetagSummary()

    flow.tag_files(
        MagicMock(), MagicMock(), _files("a", "b", "c"), summary, max_files=0
    )

    assert summary.scanned == 3
    assert summary.tagged == 3


def test_the_cap_stops_the_sweep(outcomes, deadline) -> None:
    summary = flow.RetagSummary()

    flow.tag_files(
        MagicMock(), MagicMock(), _files("a", "b", "c"), summary, max_files=2
    )

    assert summary.scanned == 2
    assert summary.tagged == 2


def test_the_cap_comes_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MAX_UPLOADS_PER_RUN", raising=False)
    assert flow._max_uploads_per_run() == flow.DEFAULT_MAX_UPLOADS_PER_RUN
    monkeypatch.setenv("MAX_UPLOADS_PER_RUN", "7")
    assert flow._max_uploads_per_run() == 7


def test_the_deadline_stops_the_sweep_between_files(outcomes, deadline) -> None:
    deadline.side_effect = [False, True]
    summary = flow.RetagSummary()

    with pytest.raises(RunOutOfTime):
        flow.tag_files(
            MagicMock(), MagicMock(), _files("a", "b", "c"), summary, max_files=0
        )

    assert summary.scanned == 1
    assert summary.tagged == 1


def test_a_refusal_past_the_deadline_is_not_a_failed_file(outcomes, deadline) -> None:
    outcomes["a"] = TimeoutError("refused")
    deadline.side_effect = [False, True]

    with pytest.raises(RunOutOfTime):
        flow.tag_files(
            MagicMock(), MagicMock(), _files("a"), flow.RetagSummary(), max_files=0
        )


def test_one_failing_file_does_not_end_the_sweep(outcomes, deadline) -> None:
    outcomes["a"] = RuntimeError("boom")
    summary = flow.RetagSummary()

    flow.tag_files(MagicMock(), MagicMock(), _files("a", "b"), summary, max_files=0)

    assert summary.failed == ["a"]
    assert summary.tagged == 1


def test_the_report_counts_and_names_the_untouched_files(
    outcomes, deadline, posted, folder
) -> None:
    folder.extend(_files("new", "done", "unknown", "untitled", "broken"))
    outcomes["done"] = Outcome.ALREADY_TAGGED
    outcomes["unknown"] = Outcome.NOT_IN_CATALOG
    outcomes["untitled"] = Outcome.NEEDS_FINGERPRINT
    outcomes["broken"] = RuntimeError("boom")

    summary = flow.retag_music(run_id="m-1")

    assert summary.scanned == 5
    posted.assert_called_once()
    args, kwargs = posted.call_args
    assert args[0] == "retag-music"
    assert args[1] == "WARN"
    assert kwargs["run_id"] == "m-1"
    assert kwargs["scanned"] == 5
    assert kwargs["matched_in_catalog"] == 2
    assert kwargs["not_in_catalog"] == 1
    assert kwargs["tagged"] == 1
    assert kwargs["needing_fingerprinting"] == 1
    assert kwargs["failed"] == 1
    text = kwargs["text"]
    for name in ("unknown", "untitled", "broken"):
        assert name in text


def test_a_clean_run_reports_success(outcomes, deadline, posted, folder) -> None:
    folder.extend(_files("a"))

    flow.retag_music(run_id="m-1")

    assert posted.call_args.args[1] == "SUCCESS"
    assert posted.call_args.kwargs["notable"] is True


def test_a_run_stopped_at_the_deadline_still_reports(
    outcomes, deadline, posted, folder
) -> None:
    folder.extend(_files("a", "b"))
    deadline.side_effect = [False, True]

    with pytest.raises(RunOutOfTime):
        flow.retag_music(run_id="m-1")

    posted.assert_called_once()
    assert posted.call_args.kwargs["scanned"] == 1
    assert posted.call_args.kwargs["tagged"] == 1
    assert "Stopped early" in posted.call_args.kwargs["text"]
