"""The nightly library job runs its steps in order, and a failed step other
than the backfill does not stop the rest."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from mini_app_polis.lambda_worker import RunOutOfTime

from deejay_cog import library_nightly as nightly


@pytest.fixture
def order(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    seen: list[str] = []
    for name in (
        "library_scan",
        "library_trash",
        "library_identify",
        "library_backfill",
        "library_sync",
        "library_duplicates",
    ):
        monkeypatch.setattr(nightly, name, lambda _n=name, **_: seen.append(_n))
    return seen


@pytest.fixture
def finding(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    fake = MagicMock()
    monkeypatch.setattr(nightly, "post_run_finding", fake)
    return fake


def test_the_job_runs_its_steps_in_order(order, finding) -> None:
    nightly.nightly_library_run(run_id="m-1")

    assert order == [
        "library_scan",
        "library_trash",
        "library_identify",
        "library_backfill",
        "library_sync",
        "library_duplicates",
    ]
    finding.assert_not_called()


@pytest.mark.parametrize(
    "broken",
    [
        "library_scan",
        "library_trash",
        "library_identify",
        "library_sync",
        "library_duplicates",
    ],
)
def test_a_failed_step_is_reported_and_the_rest_runs(
    order, finding, monkeypatch: pytest.MonkeyPatch, broken: str
) -> None:
    def fail(*, run_id):
        raise RuntimeError("registry down")

    monkeypatch.setattr(nightly, broken, fail)

    nightly.nightly_library_run(run_id="m-1")

    assert "library_backfill" in order
    assert finding.call_args.args[1] == "ERROR"
    assert "registry down" in finding.call_args.args[2]


def test_the_deadline_stops_the_job(order, monkeypatch: pytest.MonkeyPatch) -> None:
    def late(*, run_id):
        raise RunOutOfTime("deadline")

    monkeypatch.setattr(nightly, "library_scan", late)

    with pytest.raises(RunOutOfTime):
        nightly.nightly_library_run(run_id="m-1")
    assert order == []


def test_steps_due_past_the_deadline_are_skipped_not_failed(
    order, finding, monkeypatch: pytest.MonkeyPatch
) -> None:
    passed = {"now": False}
    monkeypatch.setattr(nightly, "shared_deadline_passed", lambda: passed["now"])

    def backfill_then_late(**_):
        order.append("library_backfill")
        passed["now"] = True

    monkeypatch.setattr(nightly, "library_backfill", backfill_then_late)

    nightly.nightly_library_run(run_id="m-1")

    assert order == [
        "library_scan",
        "library_trash",
        "library_identify",
        "library_backfill",
    ]
    finding.assert_not_called()
