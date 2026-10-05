"""``process-new-files`` through the Lambda handler, end to end.

Each test drops files into a fake Drive, sends the SQS event the API would
enqueue, and asserts on what a person would check afterwards: where the
files are, what the API received, what the run report said, and whether
the message was handed back to the queue.
"""

from __future__ import annotations

import pytest
from harness import (
    API_KEY,
    SOURCE_FOLDER,
    FakeApi,
    FakeGoogle,
    FakeSpotify,
    LambdaContext,
    install_api,
    run_body,
    sqs_event,
    sqs_record,
)

MODE = "process-new-files"

# What VirtualDJ's history export looks like when Excel has touched it: a
# BOM, a separator hint, a blank line, and a run of spaces in a cell.
SET_CSV = (
    "﻿sep=,\n"
    "Label,Title,Remix,Artist,Comment,Genre,Length,BPM,Year\n"
    "\n"
    "Atlantic,Leave The Door Open,,Bruno Mars,opener,R&B,4:02,74,2021\n"
    "Columbia,Adorn,Radio  Edit,Miguel,,R&B,3:13,88,2012\n"
    ",No Artist Row,,,,,,,\n"
)

SET_NAME = "2025-10-04 MADjam"


def _event(message_id: str = "msg-1", *, receive_count: int = 1) -> dict:
    return sqs_event(
        sqs_record(run_body(MODE), message_id, receive_count=receive_count)
    )


# ── the path that matters ────────────────────────────────────────────────


def test_a_new_set_is_uploaded_archived_ingested_and_reported(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    drive = google.drive
    drive.add_file(SOURCE_FOLDER, f"{SET_NAME}.csv", SET_CSV)

    result = handler(_event())

    assert result == {"batchItemFailures": []}

    # Drive: one sheet in the year folder, the CSV in its Archive, the
    # drop zone empty.
    year = drive.path("2025")
    assert year is not None
    sheets = drive.sheets_in(year)
    assert [s.name for s in sheets] == [f"{SET_NAME}.csv"]
    assert drive.names_in(drive.path("2025", "Archive")) == [f"{SET_NAME}.csv"]
    assert drive.names_in(SOURCE_FOLDER) == []
    assert google.sheets.formatted == [sheets[0].id]

    # The CSV was normalised before upload: no BOM/sep line, no blank row.
    header, *rows = google.sheets.values[sheets[0].id]
    assert header[:2] == ["Label", "Title"]
    assert len(rows) == 3

    # The API received the set, built from the sheet, with this cog's key.
    [ingest] = api.bodies("/v1/ingest")
    assert ingest["set_date"] == "2025-10-04"
    assert ingest["venue"] == "MADjam"
    assert ingest["source_file"] == SET_NAME
    assert [t["title"] for t in ingest["tracks"]] == ["Leave The Door Open", "Adorn"]
    first = ingest["tracks"][0]
    assert first["length_secs"] == 242
    assert first["bpm"] == 74.0
    assert first["release_year"] == 2021
    assert first["play_order"] == 1
    # Whitespace runs inside a cell are collapsed by the normaliser.
    assert ingest["tracks"][1]["remix"] == "Radio Edit"
    assert set(api.auth) == {f"Bearer {API_KEY}"}

    # One report, SUCCESS, naming the set and carrying the message id.
    [report] = api.reports()
    assert api.severities() == ["SUCCESS"]
    assert "process-new-csv-files" in report["title"]
    assert f"{SET_NAME}.csv" in report["description"]
    assert "run msg-1" in report["footer"]["text"]


def test_an_idle_sweep_sends_nothing(google: FakeGoogle, api: FakeApi, handler) -> None:
    """A trigger that finds an empty drop zone is an idle tick: no POSTs at all."""
    result = handler(_event())

    assert result == {"batchItemFailures": []}
    assert api.calls == {}
    assert google.drive.calls.count("upload_csv_as_google_sheet") == 0


# ── TEST-015: redelivery ─────────────────────────────────────────────────


def test_the_same_message_delivered_twice_imports_the_set_once(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    """SQS is at-least-once. The second delivery finds the CSV archived."""
    google.drive.add_file(SOURCE_FOLDER, f"{SET_NAME}.csv", SET_CSV)

    first = handler(_event("msg-1"))
    second = handler(_event("msg-1", receive_count=2))

    assert first == second == {"batchItemFailures": []}
    assert len(google.drive.sheets_in(google.drive.path("2025"))) == 1
    assert len(api.bodies("/v1/ingest")) == 1
    # The redelivery is an idle sweep, so it reports nothing.
    assert api.severities() == ["SUCCESS"]


# ── TEST-015: a bad record in a batch ────────────────────────────────────


def test_a_bad_record_is_returned_and_the_rest_of_the_batch_completes(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    google.drive.add_file(SOURCE_FOLDER, f"{SET_NAME}.csv", SET_CSV)
    event = sqs_event(
        sqs_record(run_body(MODE), "good-1"),
        sqs_record(
            '{"type": "deejay.run", "version": 1, "payload": {"mode": "guess"}}',
            "bad-1",
        ),
        sqs_record("not json at all", "bad-2"),
        sqs_record(run_body("ingest-live-history"), "good-2"),
    )

    result = handler(event)

    assert result == {
        "batchItemFailures": [{"itemIdentifier": "bad-1"}, {"itemIdentifier": "bad-2"}]
    }
    # The good process-new-files record did its work regardless.
    assert len(api.bodies("/v1/ingest")) == 1
    # One ERROR per unprocessable message (first receive), plus the
    # good run's SUCCESS. The idle live-history sweep reports nothing.
    assert sorted(api.severities()) == ["ERROR", "ERROR", "SUCCESS"]


def test_an_unprocessable_message_is_reported_only_on_its_first_receive(
    google: FakeGoogle,  # noqa: ARG001
    api: FakeApi,
    handler,
) -> None:
    bad = (
        '{"type": "deejay.run", "version": 2, "payload": {"mode": "process-new-files"}}'
    )

    for attempt in (1, 2, 3):
        result = handler(sqs_event(sqs_record(bad, "bad-1", receive_count=attempt)))
        assert result == {"batchItemFailures": [{"itemIdentifier": "bad-1"}]}

    assert api.severities() == ["ERROR"]


def test_a_run_that_cannot_list_the_drop_zone_is_redelivered_and_reported(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    google.drive.fail("list_files", RuntimeError("503 backendError"), times=None)

    result = handler(_event("msg-9"))

    assert result == {"batchItemFailures": [{"itemIdentifier": "msg-9"}]}
    [report] = api.reports()
    assert api.severities() == ["ERROR"]
    assert "process-new-csv-files" in report["title"]
    assert "503 backendError" in report["description"]


# ── what each file outcome leaves behind ─────────────────────────────────


def test_a_set_already_in_its_year_folder_is_flagged_not_reimported(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    drive = google.drive
    year = drive.ensure_folder(drive.path() or "", "2025")
    drive.add_file(
        year, SET_NAME, "", mime_type="application/vnd.google-apps.spreadsheet"
    )
    drive.add_file(SOURCE_FOLDER, f"{SET_NAME}.csv", SET_CSV)

    result = handler(_event())

    assert result == {"batchItemFailures": []}
    assert drive.names_in(SOURCE_FOLDER) == [f"possible_duplicate_{SET_NAME}.csv"]
    assert api.bodies("/v1/ingest") == []
    # Saw input, imported nothing: reported, so it is not silent.
    assert api.severities() == ["SUCCESS"]
    assert "duplicate_csv" in api.reports()[0]["description"]


def test_a_failed_upload_renames_the_csv_and_warns(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    drive = google.drive
    drive.add_file(SOURCE_FOLDER, f"{SET_NAME}.csv", SET_CSV)
    drive.fail("upload_csv_as_google_sheet", RuntimeError("403 storageQuotaExceeded"))

    result = handler(_event())

    # A per-file failure is contained: the run completes and is not retried.
    assert result == {"batchItemFailures": []}
    assert drive.names_in(SOURCE_FOLDER) == [f"FAILED_{SET_NAME}.csv"]
    assert api.bodies("/v1/ingest") == []
    assert api.severities() == ["WARN"]
    assert "sets_failed" in api.reports()[0]["description"]


def test_a_failed_csv_is_retried_by_the_next_run(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    """FAILED_ is not a parking state: the next run strips it and tries again.

    Documents current behaviour. A CSV that fails for a reason that will
    not go away (a malformed file) fails, and warns, on every trigger.
    """
    drive = google.drive
    drive.add_file(SOURCE_FOLDER, f"{SET_NAME}.csv", SET_CSV)
    drive.fail("upload_csv_as_google_sheet", RuntimeError("500"), times=1)

    handler(_event("msg-1"))
    handler(_event("msg-2"))

    assert drive.names_in(SOURCE_FOLDER) == []
    assert len(api.bodies("/v1/ingest")) == 1
    assert api.severities() == ["WARN", "SUCCESS"]


def test_an_api_rejection_leaves_the_set_imported_but_never_ingested(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    """The set is archived before the POST, so nothing retries it.

    Documents current behaviour, and the gap in it: the WARN is the only
    trace. The message is not redelivered (the flow returned), and a
    redelivery would not help — the CSV is no longer in the drop zone.
    """
    drive = google.drive
    drive.add_file(SOURCE_FOLDER, f"{SET_NAME}.csv", SET_CSV)
    api.fail("/v1/ingest", 503)

    first = handler(_event("msg-1"))
    second = handler(_event("msg-1", receive_count=2))

    assert first == second == {"batchItemFailures": []}
    assert drive.names_in(drive.path("2025", "Archive")) == [f"{SET_NAME}.csv"]
    assert len(api.bodies("/v1/ingest")) == 1  # the rejected attempt, only
    assert api.severities() == ["WARN"]
    assert "ingest_failed=1" in api.reports()[0]["description"]


def test_an_archive_failure_still_ingests_and_the_next_run_does_not_double_ingest(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    drive = google.drive
    drive.add_file(SOURCE_FOLDER, f"{SET_NAME}.csv", SET_CSV)
    drive.fail("move_file", RuntimeError("500 backendError"))

    handler(_event("msg-1"))
    handler(_event("msg-2"))

    assert len(api.bodies("/v1/ingest")) == 1
    assert len(drive.sheets_in(drive.path("2025"))) == 1
    # Left in the drop zone, then flagged by the second run.
    assert drive.names_in(SOURCE_FOLDER) == [f"possible_duplicate_{SET_NAME}.csv"]
    assert api.severities()[0] == "WARN"
    assert "archive_move_failed" in api.reports()[0]["description"]


def test_a_filename_without_a_venue_imports_but_skips_ingest(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    google.drive.add_file(SOURCE_FOLDER, "2025-10-04_untitled.csv", SET_CSV)

    handler(_event())

    assert len(google.drive.sheets_in(google.drive.path("2025"))) == 1
    assert api.bodies("/v1/ingest") == []
    assert api.severities() == ["WARN"]
    assert "bad_filename_in_file" in api.reports()[0]["description"]


def test_non_csv_and_unrecognised_files(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    drive = google.drive
    drive.add_file(
        SOURCE_FOLDER, "2025-10-04 MADjam.m4a", "audio", mime_type="audio/mp4"
    )
    drive.add_file(SOURCE_FOLDER, "notes.txt", "hello", mime_type="text/plain")

    result = handler(_event())

    assert result == {"batchItemFailures": []}
    assert drive.names_in(drive.path("2025")) == ["2025-10-04 MADjam.m4a"]
    assert drive.names_in(SOURCE_FOLDER) == ["notes.txt"]
    assert api.bodies("/v1/ingest") == []
    assert api.severities() == ["SUCCESS"]


def test_several_sets_in_one_sweep_are_each_imported(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    drive = google.drive
    drive.add_file(SOURCE_FOLDER, "2024-12-31 NYE Swing.csv", SET_CSV)
    drive.add_file(SOURCE_FOLDER, f"{SET_NAME}.csv", SET_CSV)

    handler(_event())

    assert {b["venue"] for b in api.bodies("/v1/ingest")} == {"NYE Swing", "MADjam"}
    assert len(drive.sheets_in(drive.path("2024"))) == 1
    assert len(drive.sheets_in(drive.path("2025"))) == 1
    assert api.severities() == ["SUCCESS"]


def test_an_import_invalidates_that_years_summary_sheet(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    drive = google.drive
    summary = drive.ensure_folder(drive.path() or "", "Summary")
    drive.add_file(
        summary, "2025 Summary", mime_type="application/vnd.google-apps.spreadsheet"
    )
    drive.add_file(
        summary, "2024 Summary", mime_type="application/vnd.google-apps.spreadsheet"
    )
    drive.add_file(SOURCE_FOLDER, f"{SET_NAME}.csv", SET_CSV)

    handler(_event())

    assert drive.names_in(summary) == ["2024 Summary"]
    assert api.severities() == ["SUCCESS"]


def test_status_prefixes_are_stripped_unless_the_name_is_taken(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    """Copy of / possible_duplicate_ / FAILED_ are cleared before the sweep."""
    drive = google.drive
    drive.add_file(SOURCE_FOLDER, f"Copy of {SET_NAME}.csv", SET_CSV)
    drive.add_file(
        SOURCE_FOLDER, "possible_duplicate_2024-12-31 NYE Swing.csv", SET_CSV
    )
    # Both a prefixed and an unprefixed copy: the prefixed one is left alone.
    drive.add_file(SOURCE_FOLDER, "FAILED_notes.txt", "x", mime_type="text/plain")
    drive.add_file(SOURCE_FOLDER, "notes.txt", "x", mime_type="text/plain")

    handler(_event())

    assert {b["venue"] for b in api.bodies("/v1/ingest")} == {"MADjam", "NYE Swing"}
    assert drive.names_in(SOURCE_FOLDER) == ["FAILED_notes.txt", "notes.txt"]


# ── Spotify ──────────────────────────────────────────────────────────────


def test_spotify_gets_a_set_playlist_the_radio_tracks_and_a_snapshot(
    google: FakeGoogle, api: FakeApi, spotify: FakeSpotify, handler
) -> None:
    spotify.catalog[("Bruno Mars", "Leave The Door Open")] = "spotify:track:1"
    google.drive.add_file(SOURCE_FOLDER, f"{SET_NAME}.csv", SET_CSV)

    result = handler(_event())

    assert result == {"batchItemFailures": []}
    set_playlist = spotify.find_playlist_by_name(SET_NAME)
    assert set_playlist is not None and set_playlist["uris"] == ["spotify:track:1"]
    assert spotify.playlists["pl-radio"]["uris"] == ["spotify:track:1"]
    assert spotify.trimmed == ["pl-radio"]
    [snapshot] = api.bodies("/v1/spotify/playlists")
    assert {p["name"] for p in snapshot["playlists"]} == {"WCS Radio", SET_NAME}
    assert api.severities() == ["SUCCESS"]


def test_a_rejected_playlist_snapshot_warns_without_failing_the_run(
    google: FakeGoogle,
    api: FakeApi,
    spotify: FakeSpotify,
    handler,  # noqa: ARG001
) -> None:
    google.drive.add_file(SOURCE_FOLDER, f"{SET_NAME}.csv", SET_CSV)
    api.fail("/v1/spotify/playlists", 500)

    result = handler(_event())

    assert result == {"batchItemFailures": []}
    assert len(api.bodies("/v1/ingest")) == 1
    assert api.severities() == ["WARN"]
    assert "spotify_failed" in api.reports()[0]["description"]


# ── known defects (strict: these turn red when fixed — then drop the mark) ─


@pytest.mark.xfail(
    strict=True,
    reason=(
        "RunOutOfTime subclasses Exception, so process_csv_file's handler "
        "catches it: the file is renamed FAILED_, the sweep carries on past "
        "the deadline, and the handler reports success."
    ),
)
def test_a_run_that_reaches_the_deadline_is_returned_to_the_queue(
    google: FakeGoogle, api: FakeApi, handler, monkeypatch: pytest.MonkeyPatch
) -> None:
    from deejay_cog import _deadline

    monkeypatch.setattr(_deadline, "DEADLINE_MARGIN_SECONDS", 0)
    drive = google.drive
    drive.add_file(SOURCE_FOLDER, f"{SET_NAME}.csv", SET_CSV)
    drive.slow("download_file", 1.0)

    result = handler(_event("msg-late"), LambdaContext(remaining_ms=300))

    assert result == {"batchItemFailures": [{"itemIdentifier": "msg-late"}]}
    assert drive.names_in(SOURCE_FOLDER) == [f"{SET_NAME}.csv"]
    assert api.severities() == ["ERROR"]


@pytest.mark.xfail(
    strict=True,
    reason=(
        "_ingest_set_to_api gates on the unsuffixed KAIANO_API_BASE_URL while "
        "the client it builds reads KAIANO_API_BASE_URL_DEV outside "
        "production, so a correctly configured dev run never ingests."
    ),
)
def test_a_development_run_ingests_to_the_development_api(
    google: FakeGoogle, api: FakeApi, handler, monkeypatch: pytest.MonkeyPatch
) -> None:
    dev_base = "https://dev.api.deejay.test"
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.delenv("KAIANO_API_BASE_URL")
    monkeypatch.setenv("KAIANO_API_BASE_URL_DEV", dev_base)
    dev = FakeApi(router=api.router)
    install_api(api.router, dev, dev_base)
    google.drive.add_file(SOURCE_FOLDER, f"{SET_NAME}.csv", SET_CSV)

    handler(_event())

    assert len(dev.bodies("/v1/ingest")) == 1
    assert dev.severities() == ["SUCCESS"]
