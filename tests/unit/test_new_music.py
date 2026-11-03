"""The new-music worker mode: each path a file can take, the modes, the
folder settings, the audio check and the run record."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from mini_app_polis import pipeline_status
from mini_app_polis.api.contract import CatalogLookupRequest, CatalogMatch
from mini_app_polis.lambda_worker import RunOutOfTime, parse_message
from music_fakes import AUDIO, FakeDrive, mp3
from mutagen.id3 import TALB, TCON, TIT2, TPE1

from deejay_cog import new_music as flow
from deejay_cog.fingerprint_jobs import FingerprintJobMessage, FingerprintJobs
from deejay_cog.music_safety import (
    SOURCE_PROPERTY,
    audio_payload,
    tag_snapshot,
)

IN, DEST, REVIEW, ORIG, STATE = "in", "dest", "review", "orig", "state"
_CATALOG_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")


def _match(**overrides) -> CatalogMatch:
    fields = {
        "id": _CATALOG_ID,
        "title": "Temple",
        "remix": "Niko The Kid Remix",
        "artist": "Thomas Gold",
        "genre": "WCS",
        "release_year": 2019,
    }
    fields.update(overrides)
    return CatalogMatch(**fields)


def _tagged(title: str = "temple", artist: str = "thomas gold") -> bytes:
    return mp3(
        TIT2(encoding=3, text=[title]),
        TPE1(encoding=3, text=[artist]),
        TALB(encoding=3, text=["Some Album"]),
    )


@dataclass
class FakeApi:
    answers: dict[str, CatalogMatch | None] = field(default_factory=dict)
    requests: list[CatalogLookupRequest] = field(default_factory=list)

    def lookup_catalog(self, payload: CatalogLookupRequest):
        self.requests.append(payload)
        return [self.answers.get(t.title.lower()) for t in payload.tracks]


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch) -> None:
    for role, value in {
        "input": IN,
        "destination": DEST,
        "review": REVIEW,
        "originals": ORIG,
        "state": STATE,
    }.items():
        monkeypatch.setenv(flow.FOLDER_VARS[role], value)
    monkeypatch.setenv(flow.MODE_VAR, "write")
    monkeypatch.delenv(flow.MAX_FILES_VAR, raising=False)


@pytest.fixture
def posted(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    fake = MagicMock()
    monkeypatch.setattr(pipeline_status, "post_run_finding", fake)
    return fake


@pytest.fixture
def errors(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    fake = MagicMock()
    monkeypatch.setattr(flow, "post_run_finding", fake)
    return fake


@pytest.fixture
def api() -> FakeApi:
    return FakeApi({"temple": _match()})


@pytest.fixture
def sqs() -> MagicMock:
    return MagicMock()


@pytest.fixture
def wired(
    monkeypatch: pytest.MonkeyPatch, drive: FakeDrive, api: FakeApi, sqs: MagicMock
) -> MagicMock:
    """The real flow on the fake Drive, API and queue."""
    google = MagicMock()
    google.from_env.return_value = SimpleNamespace(drive=drive)
    monkeypatch.setattr(flow, "GoogleAPI", google)
    monkeypatch.setattr(flow, "api_client", lambda **_: api)
    monkeypatch.setattr(
        flow,
        "fingerprint_jobs_from_env",
        lambda: FingerprintJobs("https://sqs.test/q.fifo", sqs),
    )
    monkeypatch.setattr(flow, "shared_deadline_passed", lambda: False)
    return google


def _record(drive: FakeDrive) -> dict:
    (item,) = drive.in_folder(STATE)
    return json.loads(item.data)


# ---------------------------------------------------------------------------
# Modes and settings
# ---------------------------------------------------------------------------


def test_the_worker_runs_this_mode() -> None:
    from deejay_cog import worker

    assert worker.MODES["new-music"] is flow.new_music
    assert worker.FLOW_NAMES["new-music"] == "new-music"


@pytest.mark.parametrize("value", [None, "", "on", "WRITE!", "yes"])
def test_unset_or_unknown_mode_is_off_and_nothing_is_touched(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch, value
) -> None:
    drive.add("a.mp3", IN, _tagged())
    if value is None:
        monkeypatch.delenv(flow.MODE_VAR)
    else:
        monkeypatch.setenv(flow.MODE_VAR, value)

    summary = flow.new_music(run_id="m-1")

    assert summary.scanned == 0
    wired.from_env.assert_not_called()
    assert drive.calls == []
    assert "is off" in posted.call_args.kwargs["text"]


@pytest.mark.parametrize(
    "role", ["input", "destination", "review", "originals", "state"]
)
def test_a_missing_folder_setting_does_nothing(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch, role
) -> None:
    drive.add("a.mp3", IN, _tagged())
    monkeypatch.delenv(flow.FOLDER_VARS[role])

    summary = flow.new_music(run_id="m-1")

    assert summary.scanned == 0
    assert drive.calls == []
    assert posted.call_args.args[1] == "WARN"
    assert flow.FOLDER_VARS[role] in posted.call_args.kwargs["text"]


def test_two_roles_on_one_folder_does_nothing(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    drive.add("a.mp3", IN, _tagged())
    monkeypatch.setenv(flow.FOLDER_VARS["destination"], IN)

    flow.new_music(run_id="m-1")

    assert drive.calls == []
    assert "must all be different" in posted.call_args.kwargs["text"]


# ---------------------------------------------------------------------------
# Identified in the catalog
# ---------------------------------------------------------------------------


def test_a_catalog_match_is_copied_tagged_and_renamed_and_the_original_kept(
    settings, wired, posted, drive
) -> None:
    original = _tagged()
    file_id = drive.add("track01.mp3", IN, original)

    summary = flow.new_music(run_id="m-1")

    assert summary.identified == [
        "track01.mp3 -> Temple (Niko The Kid Remix)_Thomas Gold.mp3"
    ]
    (copy,) = drive.in_folder(DEST)
    assert copy.name == "Temple (Niko The Kid Remix)_Thomas Gold.mp3"
    assert copy.app_properties == {SOURCE_PROPERTY: file_id}
    assert tag_snapshot(copy.data)["title"] == "Temple (Niko The Kid Remix)"
    assert tag_snapshot(copy.data)["catalog_id"] == str(_CATALOG_ID)
    assert audio_payload(copy.data) == AUDIO
    # The original: moved, never changed, never renamed.
    item = drive.items[file_id]
    assert item.parents == [ORIG]
    assert item.data == original
    assert item.name == "track01.mp3"
    assert not any(m == "update_file" and a[0] == file_id for m, a in drive.calls)

    record = _record(drive)
    assert record["flow"] == "new-music"
    assert record["run_id"] == "m-1"
    assert record["dry_run"] is False
    assert record["status"] == "complete"
    (entry,) = record["entries"]
    assert entry["file_id"] == file_id
    assert entry["original_name"] == "track01.mp3"
    assert entry["state"] == "done"
    assert entry["old_tags"]["title"] == "temple"
    assert entry["new_tags"]["artist"] == "Thomas Gold"
    assert entry["copy"] == {"id": copy.id, "folder_id": DEST, "name": copy.name}
    assert entry["move"] == {"from": IN, "to": ORIG}
    assert posted.call_args.args[1] == "SUCCESS"
    assert posted.call_args.kwargs["identified"] == 1


def test_dry_run_writes_nothing_but_its_record(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(flow.MODE_VAR, "dry-run")
    drive.add("track01.mp3", IN, _tagged())
    drive.add("clip.m4a", IN, b"m4a", mime_type="audio/mp4")
    drive.add("Temple (Niko The Kid Remix)_Thomas Gold.mp3", DEST, b"x")

    summary = flow.new_music(run_id="m-1")

    assert drive.writes(except_folder=STATE) == []
    # Only one write at all: the would-be record, created once.
    assert [m for m, _ in drive.writes()] == ["upload_bytes"]
    (item,) = drive.in_folder(STATE)
    assert item.name == "new-music-m-1.dry-run.json"
    record = json.loads(item.data)
    assert record["dry_run"] is True
    assert {e["state"] for e in record["entries"]} == {"planned"}
    assert [e["action"] for e in record["entries"]] == ["review", "review"]
    assert summary.collisions and summary.needs_review
    text = posted.call_args.kwargs["text"]
    assert "dry-run" in text


def test_dry_run_reports_what_it_would_write(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(flow.MODE_VAR, "dry-run")
    file_id = drive.add("track01.mp3", IN, _tagged())

    summary = flow.new_music(run_id="m-1")

    assert drive.writes(except_folder=STATE) == []
    assert drive.items[file_id].parents == [IN]
    (entry,) = _record(drive)["entries"]
    assert entry["action"] == "deliver"
    assert entry["new_name"] == "Temple (Niko The Kid Remix)_Thomas Gold.mp3"
    assert entry["old_tags"]["title"] == "temple"
    assert entry["new_tags"]["title"] == "Temple (Niko The Kid Remix)"
    assert entry["copy"] is None and entry["move"] is None
    assert summary.identified
    assert posted.call_args.kwargs["not_written_dry_run"] == 1


def test_a_catalog_value_never_empties_a_tag(
    settings, wired, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = mp3(
        TIT2(encoding=3, text=["temple"]),
        TPE1(encoding=3, text=["thomas gold"]),
        TCON(encoding=3, text=["Pop"]),
    )
    file_id = drive.add("track01.mp3", IN, original)
    emptied = mp3(
        TIT2(encoding=3, text=["Temple"]), TPE1(encoding=3, text=["Thomas Gold"])
    )
    monkeypatch.setattr(flow, "write_catalog_tags", lambda *_: emptied)

    summary = flow.new_music(run_id="m-1")

    assert summary.failed and "genre" in summary.failed[0]
    assert drive.writes(except_folder=STATE) == []
    assert drive.items[file_id].parents == [IN]


# ---------------------------------------------------------------------------
# Handed to the fingerprint function
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("data", "reason"),
    [
        (_tagged("brand new song", "new artist"), "not in the catalog"),
        (mp3(TALB(encoding=3, text=["Album only"])), "no usable title or artist"),
    ],
)
def test_a_file_the_catalog_does_not_know_goes_to_fingerprinting_untouched(
    settings, wired, drive, sqs, data, reason
) -> None:
    file_id = drive.add("new.mp3", IN, data)

    summary = flow.new_music(run_id="m-1")

    assert summary.sent_to_fingerprinting == [f"new.mp3 ({reason})"]
    assert drive.writes(except_folder=STATE) == []
    assert drive.items[file_id].parents == [IN]
    sent = sqs.send_message.call_args.kwargs
    assert sent["MessageDeduplicationId"] == f"new-music:{file_id}"
    payload = parse_message(sent["MessageBody"], FingerprintJobMessage).payload
    assert payload.flow == "new-music"
    assert payload.mode == "write"
    assert payload.drive_file_id == file_id


def test_with_no_queue_a_file_stays_in_the_input_folder(
    settings, wired, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(flow, "fingerprint_jobs_from_env", lambda: None)
    file_id = drive.add("new.mp3", IN, _tagged("brand new", "someone"))

    summary = flow.new_music(run_id="m-1")

    assert summary.needs_fingerprinting == ["new.mp3 (not in the catalog)"]
    assert drive.writes() == []
    assert drive.items[file_id].parents == [IN]


# ---------------------------------------------------------------------------
# Needs review and collisions
# ---------------------------------------------------------------------------


def test_a_file_that_is_not_an_mp3_goes_to_review_unchanged(
    settings, wired, drive
) -> None:
    file_id = drive.add("clip.m4a", IN, b"m4a bytes", mime_type="audio/mp4")

    summary = flow.new_music(run_id="m-1")

    assert summary.needs_review
    assert drive.items[file_id].parents == [REVIEW]
    assert drive.items[file_id].data == b"m4a bytes"
    assert drive.in_folder(DEST) == []


def test_a_name_already_in_the_destination_is_not_overwritten(
    settings, wired, posted, drive
) -> None:
    existing = drive.add("Temple (Niko The Kid Remix)_Thomas Gold.mp3", DEST, b"mine")
    original = _tagged()
    file_id = drive.add("track01.mp3", IN, original)

    summary = flow.new_music(run_id="m-1")

    assert summary.collisions == [
        "track01.mp3 -> Temple (Niko The Kid Remix)_Thomas Gold.mp3"
    ]
    assert [i.id for i in drive.in_folder(DEST)] == [existing]
    assert drive.items[existing].data == b"mine"
    assert drive.items[file_id].parents == [REVIEW]
    assert drive.items[file_id].data == original
    assert not any(m == "upload_bytes" and a[0] == DEST for m, a in drive.calls)
    assert posted.call_args.args[1] == "WARN"
    assert "track01.mp3" in posted.call_args.kwargs["text"]


def test_a_copy_an_earlier_run_made_is_used_not_made_twice(
    settings, wired, drive
) -> None:
    file_id = drive.add("track01.mp3", IN, _tagged())
    # As an earlier run stopped after its upload, before the move.
    earlier = drive.add(
        "Temple (Niko The Kid Remix)_Thomas Gold.mp3",
        DEST,
        mp3(TIT2(encoding=3, text=["Temple"]), TPE1(encoding=3, text=["T"])),
        app_properties={SOURCE_PROPERTY: file_id},
    )

    flow.new_music(run_id="m-2")

    assert [i.id for i in drive.in_folder(DEST)] == [earlier]
    assert drive.items[file_id].parents == [ORIG]
    (entry,) = _record(drive)["entries"]
    assert entry["copy"]["made_by"] == "an earlier run"


@pytest.mark.parametrize(
    ("tags", "name"),
    [
        ({"title": "A/B: C?", "artist": "D\\E"}, "A B C_D E.mp3"),
        ({"title": "  Hi  there ", "artist": "x\x00y"}, "Hi there_x y.mp3"),
        ({"title": "", "artist": "x"}, None),
        ({"title": "...", "artist": "x"}, None),
    ],
)
def test_names_are_sanitized(tags, name) -> None:
    assert flow.delivery_name(tags, "orig.MP3") == name


# ---------------------------------------------------------------------------
# The audio check stops the run
# ---------------------------------------------------------------------------


def test_audio_that_would_change_stops_the_run_before_any_write(
    settings, wired, posted, errors, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = drive.add("a.mp3", IN, _tagged())
    second = drive.add("b.mp3", IN, _tagged())
    bad = mp3(
        TIT2(encoding=3, text=["Temple"]),
        TPE1(encoding=3, text=["Thomas Gold"]),
        TALB(encoding=3, text=["Some Album"]),
        audio=AUDIO[:-1] + b"\x01",
    )
    monkeypatch.setattr(flow, "write_catalog_tags", lambda *_: bad)

    summary = flow.new_music(run_id="m-1")

    assert summary.aborted
    assert summary.scanned == 1
    assert drive.writes() == []
    assert drive.items[first].parents == [IN]
    assert drive.items[second].parents == [IN]
    errors.assert_called_once()
    assert errors.call_args.args[:2] == ("new-music", "ERROR")
    assert posted.call_args.args[1] == "WARN"


def test_audio_changed_by_the_upload_trashes_the_copy_and_stops(
    settings, wired, errors, drive
) -> None:
    drive.corrupt_uploads = True
    first = drive.add("a.mp3", IN, _tagged())
    second = drive.add("b.mp3", IN, _tagged())

    summary = flow.new_music(run_id="m-1")

    assert summary.aborted
    assert drive.in_folder(DEST) == []  # the bad copy is in the trash
    trashed = [i for i in drive.items.values() if i.trashed]
    assert len(trashed) == 1 and DEST in trashed[0].parents
    assert drive.items[first].parents == [IN]
    assert drive.items[second].parents == [IN]
    record = _record(drive)
    assert record["status"] == "aborted"
    assert record["entries"][0]["state"] == "failed"
    assert record["entries"][0]["copy"]["trashed"] == "true"
    errors.assert_called_once()
    assert not any(m == "delete_file" for m, _ in drive.calls)


# ---------------------------------------------------------------------------
# Cap, deadline, report
# ---------------------------------------------------------------------------


def test_the_cap_limits_a_run(
    settings, wired, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(flow.MAX_FILES_VAR, "1")
    drive.add("a.mp3", IN, _tagged())
    drive.add("b.mp3", IN, _tagged("brand new", "x"))

    summary = flow.new_music(run_id="m-1")

    assert summary.scanned == 1


def test_the_deadline_stops_between_files_and_the_record_survives(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    drive.add("a.mp3", IN, _tagged())
    drive.add("b.mp3", IN, _tagged())
    ticks = iter([False, True])
    monkeypatch.setattr(flow, "shared_deadline_passed", lambda: next(ticks))

    with pytest.raises(RunOutOfTime):
        flow.new_music(run_id="m-1")

    record = _record(drive)
    assert record["status"] == "stopped"
    assert len(record["entries"]) == 1
    assert record["entries"][0]["state"] == "done"
    assert "Stopped early" in posted.call_args.kwargs["text"]


def test_a_failing_file_does_not_end_the_sweep(
    settings, wired, drive, api, monkeypatch: pytest.MonkeyPatch
) -> None:
    drive.add("a.mp3", IN, _tagged())
    drive.add("b.mp3", IN, _tagged())
    calls = iter([RuntimeError("api down")])

    real = api.lookup_catalog

    def flaky(payload):
        exc = next(calls, None)
        if exc:
            raise exc
        return real(payload)

    monkeypatch.setattr(api, "lookup_catalog", flaky)

    summary = flow.new_music(run_id="m-1")

    assert len(summary.failed) == 1
    assert len(summary.identified) == 1


def test_a_record_that_cannot_be_saved_stops_before_any_write(
    settings, wired, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    file_id = drive.add("track01.mp3", IN, _tagged())

    def refuse(**_):
        raise RuntimeError("state folder not writable")

    monkeypatch.setattr(drive, "upload_bytes", refuse)

    summary = flow.new_music(run_id="m-1")

    assert summary.failed
    assert drive.items[file_id].parents == [IN]
    assert drive.in_folder(DEST) == []


def test_a_file_that_left_the_input_folder_is_never_moved(
    settings, wired, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    file_id = drive.add("track01.mp3", IN, _tagged())
    real_upload = drive.upload_bytes

    def upload_then_owner_files_it(**kw):
        new_id = real_upload(**kw)
        if kw["parent_id"] == DEST:
            drive.items[file_id].parents = [IN, "library"]
        return new_id

    monkeypatch.setattr(drive, "upload_bytes", upload_then_owner_files_it)

    summary = flow.new_music(run_id="m-1")

    assert summary.failed and "no longer only in the input folder" in summary.failed[0]
    assert drive.items[file_id].parents == [IN, "library"]
    assert not any(m == "move_file" for m, _ in drive.calls)
