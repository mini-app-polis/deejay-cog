"""``ingest-live-history`` through the Lambda handler, end to end."""

from __future__ import annotations

import pytest
from harness import (
    API_BASE,
    VDJ_HISTORY_FOLDER,
    FakeApi,
    FakeGoogle,
    install_api,
    run_body,
    sqs_event,
    sqs_record,
)

MODE = "ingest-live-history"


def _line(time: str, artist: str, title: str) -> str:
    return (
        f"#EXTVDJ:<time>{time}</time><artist>{artist}</artist>"
        f"<title>{title}</title><songlength>240</songlength>"
    )


# A night that runs past midnight: the last play belongs to the next day.
TONIGHT = "\n".join(
    [
        "#EXTM3U",
        _line("21:04", "Bruno Mars", "Leave The Door Open"),
        "C:\\Music\\leave.mp3",
        _line("23:50", "Miguel", "Adorn"),
        "C:\\Music\\adorn.mp3",
        _line("00:15", "H.E.R.", "Best Part"),
        "C:\\Music\\best.mp3",
    ]
)
LAST_WEEK = "\n".join(["#EXTM3U", _line("20:00", "Old", "Song")])


def _event(message_id: str = "msg-1", *, receive_count: int = 1) -> dict:
    return sqs_event(
        sqs_record(run_body(MODE), message_id, receive_count=receive_count)
    )


def test_the_newest_history_file_is_sent_as_live_plays(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    google.drive.add_file(VDJ_HISTORY_FOLDER, "2025-09-27.m3u", LAST_WEEK)
    google.drive.add_file(VDJ_HISTORY_FOLDER, "2025-10-04.m3u", TONIGHT)

    result = handler(_event())

    assert result == {"batchItemFailures": []}
    [body] = api.bodies("/v1/live-plays")
    assert [(p["artist"], p["title"]) for p in body["plays"]] == [
        ("Bruno Mars", "Leave The Door Open"),
        ("Miguel", "Adorn"),
        ("H.E.R.", "Best Part"),
    ]
    # Local Chicago time (CDT in October), and the after-midnight play
    # lands on the 5th.
    assert [p["played_at"] for p in body["plays"]] == [
        "2025-10-04T21:04:00-05:00",
        "2025-10-04T23:50:00-05:00",
        "2025-10-05T00:15:00-05:00",
    ]
    [report] = api.reports()
    assert api.severities() == ["SUCCESS"]
    assert "ingest-live-history" in report["title"]
    assert "2025-10-04.m3u" in report["description"]


def test_no_history_files_is_an_idle_tick(
    google: FakeGoogle,  # noqa: ARG001
    api: FakeApi,
    handler,
) -> None:
    assert handler(_event()) == {"batchItemFailures": []}
    assert api.calls == {}


def test_the_same_message_twice_sends_identical_plays(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    """TEST-015 redelivery: one effect, by way of the API's deduplication.

    This flow re-sends the newest file on every run, so the cog's half of
    "one effect" is that a redelivery sends exactly the same plays —
    same timestamps, same order — for the API's upsert to collapse.
    """
    google.drive.add_file(VDJ_HISTORY_FOLDER, "2025-10-04.m3u", TONIGHT)

    handler(_event("msg-1"))
    handler(_event("msg-1", receive_count=2))

    first, second = api.bodies("/v1/live-plays")
    assert first == second


def test_an_api_rejection_warns_and_is_not_redelivered(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    google.drive.add_file(VDJ_HISTORY_FOLDER, "2025-10-04.m3u", TONIGHT)
    api.fail("/v1/live-plays", 422)

    result = handler(_event())

    assert result == {"batchItemFailures": []}
    assert api.severities() == ["WARN"]
    description = api.reports()[0]["description"]
    assert "plays_failed" in description
    assert "files_failed" in description


def test_a_history_file_with_no_plays_is_reported(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    """A file was there and nothing was sent: the run worth a message."""
    google.drive.add_file(VDJ_HISTORY_FOLDER, "2025-10-04.m3u", "#EXTM3U\n")

    handler(_event())

    assert api.bodies("/v1/live-plays") == []
    assert api.severities() == ["SUCCESS"]


def test_an_unreadable_history_file_warns(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    google.drive.add_file(VDJ_HISTORY_FOLDER, "2025-10-04.m3u", TONIGHT)
    google.drive.fail("download_m3u_file_data", RuntimeError("500 backendError"))

    assert handler(_event()) == {"batchItemFailures": []}
    assert api.bodies("/v1/live-plays") == []
    assert api.severities() == ["WARN"]


# ── known defects (strict: these turn red when fixed — then drop the mark) ─

DEV_BASE = "https://dev.api.deejay.test"


@pytest.mark.xfail(
    strict=True,
    reason=(
        "ingest_live_history gates on, and passes, the unsuffixed "
        "KAIANO_API_BASE_URL, so a non-production run posts live plays to "
        "the production API while its run report goes to dev."
    ),
)
def test_a_development_run_sends_plays_to_the_development_api(
    google: FakeGoogle, api: FakeApi, handler, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv("KAIANO_API_BASE_URL_DEV", DEV_BASE)
    dev = FakeApi(router=api.router)
    install_api(api.router, dev, DEV_BASE)
    google.drive.add_file(VDJ_HISTORY_FOLDER, "2025-10-04.m3u", TONIGHT)

    handler(_event())

    assert api.bodies("/v1/live-plays") == [], f"posted to {API_BASE}"
    assert len(dev.bodies("/v1/live-plays")) == 1
