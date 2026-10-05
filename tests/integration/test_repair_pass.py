"""The repair pass, end to end: recent sets missing from the API or Spotify.

Dates are relative to today in the cog's own timezone (TEST-020), so a set
"three days old" is three days old whenever the suite runs.
"""

from __future__ import annotations

import datetime as dt

import pytest
from harness import (
    DJ_SETS_FOLDER,
    SOURCE_FOLDER,
    FakeApi,
    FakeGoogle,
    FakeSpotify,
    run_body,
    sqs_event,
    sqs_record,
)

from deejay_cog import repair

SET_CSV = (
    "Label,Title,Remix,Artist,Comment,Genre,Length,BPM,Year\n"
    "Atlantic,Leave The Door Open,,Bruno Mars,,R&B,4:02,74,2021\n"
    "Columbia,Adorn,,Miguel,,R&B,3:13,88,2012\n"
)
LEAVE = "spotify:track:leave"
ADORN = "spotify:track:adorn"


def _days_ago(n: int) -> dt.date:
    return repair.today() - dt.timedelta(days=n)


def _name(days_ago: int, venue: str) -> str:
    return f"{_days_ago(days_ago).isoformat()} {venue}"


def _event(message_id: str = "msg-1") -> dict:
    return sqs_event(sqs_record(run_body("process-new-files"), message_id))


def _imported_earlier(
    google: FakeGoogle, name: str, *, in_api: FakeApi | None = None
) -> str:
    """A set an earlier run uploaded and archived; optionally ingested too."""
    drive = google.drive
    year = drive.ensure_folder(DJ_SETS_FOLDER, name[:4])
    sheet_id = drive.add_sheet(year, f"{name}.csv", SET_CSV)
    if in_api is not None:
        in_api.has_set(name, name[:10], name[11:])
    return sheet_id


@pytest.fixture
def catalog(spotify: FakeSpotify) -> FakeSpotify:
    spotify.catalog[("Bruno Mars", "Leave The Door Open")] = LEAVE
    spotify.catalog[("Miguel", "Adorn")] = ADORN
    return spotify


# ── the case that started this ───────────────────────────────────────────


def test_a_set_whose_spotify_sync_failed_gets_its_playlist_next_run(
    google: FakeGoogle, api: FakeApi, catalog: FakeSpotify, handler
) -> None:
    """2026-10-02 TC Rebels: imported and ingested, Spotify failed, archived.

    The next sweep — even one with nothing new in the drop zone — builds the
    missing playlist, and because it is a new playlist its tracks go on the
    radio playlist too.
    """
    name = _name(3, "TC Rebels")
    _imported_earlier(google, name, in_api=api)

    result = handler(_event())

    assert result == {"batchItemFailures": []}
    playlist = catalog.find_playlist_by_name(name)
    assert playlist is not None and playlist["uris"] == [LEAVE, ADORN]
    assert catalog.playlists["pl-radio"]["uris"] == [LEAVE, ADORN]
    assert api.bodies("/v1/ingest") == []  # the API already had it
    [snapshot] = api.bodies("/v1/spotify/playlists")
    assert name in {p["name"] for p in snapshot["playlists"]}
    [report] = api.reports()
    assert api.severities() == ["SUCCESS"]
    assert "spotify playlist (repaired)" in report["description"]
    assert name in report["description"]


def test_the_full_story_fail_then_heal(
    google: FakeGoogle, api: FakeApi, catalog: FakeSpotify, handler
) -> None:
    name = _name(1, "TC Rebels")
    google.drive.add_file(SOURCE_FOLDER, f"{name}.csv", SET_CSV)
    catalog.fail("create_playlist", RuntimeError("503 from Spotify"))

    handler(_event("msg-1"))

    assert catalog.find_playlist_by_name(name) is None
    assert api.severities() == ["WARN"]
    assert google.drive.names_in(SOURCE_FOLDER) == []  # imported and archived

    handler(_event("msg-2"))

    assert catalog.find_playlist_by_name(name)["uris"] == [LEAVE, ADORN]
    assert catalog.playlists["pl-radio"]["uris"] == [LEAVE, ADORN]
    assert len(api.bodies("/v1/ingest")) == 1
    assert api.severities() == ["WARN", "SUCCESS"]


# ── what counts as missing ───────────────────────────────────────────────


def test_a_set_missing_from_the_api_is_ingested_from_its_sheet(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    name = _name(10, "MADjam")
    _imported_earlier(google, name)

    handler(_event())

    [body] = api.bodies("/v1/ingest")
    assert body["source_file"] == name
    assert body["set_date"] == name[:10]
    assert body["venue"] == "MADjam"
    assert [t["title"] for t in body["tracks"]] == ["Leave The Door Open", "Adorn"]
    assert "dj set in the API (repaired)" in api.reports()[0]["description"]


def test_a_complete_set_is_left_alone(
    google: FakeGoogle, api: FakeApi, catalog: FakeSpotify, handler
) -> None:
    name = _name(10, "MADjam")
    _imported_earlier(google, name, in_api=api)
    catalog.create_playlist(name, "")

    handler(_event())

    assert api.bodies("/v1/ingest") == []
    assert [a for a in catalog.added if a[0] == "pl-radio"] == []
    assert api.severities() == []  # nothing new, nothing repaired: an idle tick


def test_sets_older_than_the_lookback_are_not_repaired(
    google: FakeGoogle, api: FakeApi, catalog: FakeSpotify, handler
) -> None:
    _imported_earlier(google, _name(200, "Old Event"))

    handler(_event())

    assert api.bodies("/v1/ingest") == []
    assert len(catalog.playlists) == 1  # just the radio playlist


def test_a_venue_with_a_dot_keeps_its_whole_name(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    name = _name(5, "St. Paul Swing")
    _imported_earlier(google, name)

    handler(_event())

    [body] = api.bodies("/v1/ingest")
    assert body["source_file"] == name
    assert body["venue"] == "St. Paul Swing"


# ── bounds and failures ──────────────────────────────────────────────────


def test_repairs_are_capped_per_run_newest_first(
    google: FakeGoogle,
    api: FakeApi,
    handler,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from deejay_cog import config

    monkeypatch.setattr(config, "REPAIR_MAX_PER_RUN", 2)
    names = [_name(n, f"Event {n}") for n in (5, 10, 20)]
    for name in names:
        _imported_earlier(google, name)

    handler(_event("msg-1"))

    assert [b["source_file"] for b in api.bodies("/v1/ingest")] == names[:2]
    assert "repairs_pending=1" in api.reports()[0]["description"]

    handler(_event("msg-2"))

    assert [b["source_file"] for b in api.bodies("/v1/ingest")] == names
    assert "repairs_pending" not in api.reports()[1]["description"]


def test_a_failed_repair_warns_by_name_and_is_tried_again(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    name = _name(4, "Swingtime")
    _imported_earlier(google, name)
    api.fail("/v1/ingest", 503)

    result = handler(_event("msg-1"))

    assert result == {"batchItemFailures": []}  # the run's own work succeeded
    assert api.severities() == ["WARN"]
    description = api.reports()[0]["description"]
    assert "repair_failed" in description
    assert name in description

    handler(_event("msg-2"))

    assert len(api.bodies("/v1/ingest")) == 2
    assert api.severities() == ["WARN", "SUCCESS"]


def test_an_api_outage_while_checking_does_not_fail_the_sweep(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    _imported_earlier(google, _name(4, "Swingtime"))
    api.fail("GET /v1/sets", 503)

    result = handler(_event())

    assert result == {"batchItemFailures": []}
    assert api.bodies("/v1/ingest") == []
    assert api.severities() == ["WARN"]
    assert "api sets" in api.reports()[0]["description"]


def test_without_spotify_only_the_api_is_repaired(
    google: FakeGoogle, api: FakeApi, handler
) -> None:
    """No credentials: playlists are left alone rather than counted missing."""
    _imported_earlier(google, _name(4, "Swingtime"))

    handler(_event())

    assert len(api.bodies("/v1/ingest")) == 1
    assert api.severities() == ["SUCCESS"]


# ── the radio follows playlist creation ──────────────────────────────────


def test_a_radio_failure_after_creation_is_reported_and_not_retried(
    google: FakeGoogle, api: FakeApi, catalog: FakeSpotify, handler
) -> None:
    name = _name(2, "TC Rebels")
    _imported_earlier(google, name, in_api=api)
    catalog.fail("add_tracks:pl-radio", RuntimeError("radio is read-only"))

    handler(_event("msg-1"))

    assert catalog.find_playlist_by_name(name)["uris"] == [LEAVE, ADORN]
    assert catalog.playlists["pl-radio"]["uris"] == []
    assert api.severities() == ["WARN"]
    assert "radio: " in api.reports()[0]["description"]

    handler(_event("msg-2"))

    # The playlist exists, so nothing is owed: the radio is not revisited.
    assert catalog.playlists["pl-radio"]["uris"] == []
    assert len(api.reports()) == 1


def test_a_set_imported_this_run_is_not_repaired_in_the_same_run(
    google: FakeGoogle, api: FakeApi, catalog: FakeSpotify, handler
) -> None:
    name = _name(1, "TC Rebels")
    google.drive.add_file(SOURCE_FOLDER, f"{name}.csv", SET_CSV)

    handler(_event())

    assert len(api.bodies("/v1/ingest")) == 1
    assert catalog.playlists["pl-radio"]["uris"] == [LEAVE, ADORN]  # once
    assert "(repaired)" not in api.reports()[0]["description"]
