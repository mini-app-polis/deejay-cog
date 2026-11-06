"""The library-backfill worker mode: the manifest and its resumable listing,
the cursor and chunks, each path a file can take, the dry run, the audio
check, and rolling a run back."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from mini_app_polis import pipeline_status
from mini_app_polis.api.contract import CatalogLookupRequest, CatalogMatch
from mini_app_polis.lambda_worker import RunOutOfTime
from music_fakes import AUDIO, DriveItem, FakeDrive, mp3
from mutagen.id3 import TALB, TIT2, TPE1

from deejay_cog import library_backfill as flow
from deejay_cog import music_rollback as rb
from deejay_cog.music_safety import tag_snapshot

ROOT, STATE = "root", "state"
FOLDER = flow.FOLDER_MIME_TYPE
_CATALOG_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")


def _match(title: str = "Temple", artist: str = "Thomas Gold") -> CatalogMatch:
    return CatalogMatch(
        id=_CATALOG_ID,
        title=title,
        remix=None,
        artist=artist,
        genre="WCS",
        release_year=2019,
    )


def _song(title: str = "temple", artist: str = "thomas gold", **kw) -> bytes:
    return mp3(
        TIT2(encoding=3, text=[title]),
        TPE1(encoding=3, text=[artist]),
        TALB(encoding=3, text=["Some Album"]),
        **kw,
    )


@dataclass
class FakeApi:
    answers: dict[str, CatalogMatch | None] = field(default_factory=dict)
    requests: list[CatalogLookupRequest] = field(default_factory=list)

    def lookup_catalog(self, payload: CatalogLookupRequest):
        self.requests.append(payload)
        return [self.answers.get(t.title.lower()) for t in payload.tracks]


@pytest.fixture
def api() -> FakeApi:
    return FakeApi({f"song {n}": _match(f"Song {n}", "Artist") for n in range(30)})


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(flow.ROOT_FOLDER_VAR, ROOT)
    monkeypatch.setenv(flow.STATE_FOLDER_VAR, STATE)
    monkeypatch.setenv(flow.MODE_VAR, "write")
    monkeypatch.delenv(flow.CHUNK_SIZE_VAR, raising=False)
    monkeypatch.delenv(flow.SKIP_IDS_VAR, raising=False)


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
def downloads(drive: FakeDrive, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The ids of every library file downloaded (not the state files)."""
    seen: list[str] = []
    real = drive.download_file_bytes

    def download(file_id: str):
        if STATE not in drive.items[file_id].parents:
            seen.append(file_id)
        return real(file_id)

    monkeypatch.setattr(drive, "download_file_bytes", download)
    return seen


@pytest.fixture
def wired(monkeypatch: pytest.MonkeyPatch, drive: FakeDrive, api: FakeApi) -> MagicMock:
    """The real flow on the fake Drive and API."""
    google = MagicMock()
    google.from_env.return_value = SimpleNamespace(drive=drive)
    monkeypatch.setattr(flow, "GoogleAPI", google)
    monkeypatch.setattr(flow, "api_client", lambda **_: api)
    monkeypatch.setattr(flow, "shared_deadline_passed", lambda: False)
    return google


def _library(drive: FakeDrive, count: int, folder: str = ROOT) -> list[str]:
    """``count`` songs the catalog knows, untagged from it, named in order."""
    return [
        drive.add(f"{n:02d} song.mp3", folder, _song(f"song {n}", "artist"))
        for n in range(count)
    ]


def _state(drive: FakeDrive, name: str) -> dict | None:
    found = [i for i in drive.in_folder(STATE) if i.name == name]
    assert len(found) <= 1
    return json.loads(found[0].data) if found else None


def _manifest(drive: FakeDrive) -> dict:
    data = _state(drive, flow.MANIFEST_NAME)
    assert data is not None
    return data


def _cursor(drive: FakeDrive) -> dict | None:
    return _state(drive, flow.CURSOR_NAME)


def _record(drive: FakeDrive, run_id: str, *, dry: bool = False) -> dict:
    suffix = ".dry-run.json" if dry else ".json"
    data = _state(drive, f"{flow.FLOW_NAME}-{run_id}{suffix}")
    assert data is not None
    return data


def _music_writes(drive: FakeDrive) -> list:
    return drive.writes(except_folder=STATE)


def _text(posted: MagicMock) -> str:
    return posted.call_args.kwargs["text"]


# ---------------------------------------------------------------------------
# Modes and settings
# ---------------------------------------------------------------------------


def test_the_worker_runs_this_mode() -> None:
    from deejay_cog import worker

    assert worker.MODES["library-backfill"] is flow.library_backfill
    assert worker.FLOW_NAMES["library-backfill"] == "library-backfill"


@pytest.mark.parametrize("value", [None, "", "on", "Write please"])
def test_unset_or_unknown_mode_is_off_and_nothing_is_touched(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch, value
) -> None:
    _library(drive, 2)
    if value is None:
        monkeypatch.delenv(flow.MODE_VAR)
    else:
        monkeypatch.setenv(flow.MODE_VAR, value)

    summary = flow.library_backfill(run_id="m-1")

    assert summary.scanned == 0
    wired.from_env.assert_not_called()
    assert drive.calls == []
    assert "is off" in _text(posted)


@pytest.mark.parametrize("var", [flow.ROOT_FOLDER_VAR, flow.STATE_FOLDER_VAR])
def test_a_missing_folder_setting_does_nothing(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch, var
) -> None:
    _library(drive, 2)
    monkeypatch.delenv(var)

    summary = flow.library_backfill(run_id="m-1")

    assert summary.scanned == 0
    assert drive.calls == []
    assert posted.call_args.args[1] == "WARN"
    assert var in _text(posted)


def test_the_same_folder_for_root_and_state_does_nothing(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    _library(drive, 2)
    monkeypatch.setenv(flow.STATE_FOLDER_VAR, ROOT)

    flow.library_backfill(run_id="m-1")

    assert drive.calls == []
    assert "must be different" in _text(posted)


@pytest.mark.parametrize("value", ["0", "-3", "ten"])
def test_a_bad_chunk_size_does_nothing(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch, value
) -> None:
    _library(drive, 2)
    monkeypatch.setenv(flow.CHUNK_SIZE_VAR, value)

    flow.library_backfill(run_id="m-1")

    assert drive.calls == []
    assert flow.CHUNK_SIZE_VAR in _text(posted)


# ---------------------------------------------------------------------------
# The manifest
# ---------------------------------------------------------------------------


def _tree(drive: FakeDrive) -> dict[str, str]:
    """root: b.mp3, a.mp3, Shortcut, Sub/ (c.mp3, Deeper/ (d.mp3)), A Dir/ (e.mp3)
    and the state folder nested in the library."""
    ids = {}
    ids["b"] = drive.add("b.mp3", ROOT, _song("song 2", "artist"))
    ids["a"] = drive.add("a.mp3", ROOT, _song("song 1", "artist"))
    drive.add("Shortcut", ROOT, mime_type=flow.SHORTCUT_MIME_TYPE)
    sub = drive.add("Sub", ROOT, mime_type=FOLDER)
    deeper = drive.add("Deeper", sub, mime_type=FOLDER)
    a_dir = drive.add("A Dir", ROOT, mime_type=FOLDER)
    ids["c"] = drive.add("c.mp3", sub, _song("song 3", "artist"))
    ids["d"] = drive.add("d.mp3", deeper, _song("song 4", "artist"))
    ids["e"] = drive.add("e.mp3", a_dir, _song("song 5", "artist"))
    # The state folder inside the library is never listed into it.
    drive.items[STATE] = DriveItem(STATE, "state", [ROOT], mime_type=FOLDER)
    drive.add("other-run.json", STATE, b"{}", mime_type="application/json")
    return ids


def test_the_first_run_lists_the_library_without_downloading_it(
    settings, wired, drive, downloads, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(flow.MODE_VAR, "dry-run")
    monkeypatch.setenv(flow.CHUNK_SIZE_VAR, "1")
    ids = _tree(drive)

    summary = flow.library_backfill(run_id="m-1")

    manifest = _manifest(drive)
    assert manifest["complete"] is True
    assert manifest["root_folder_id"] == ROOT
    assert manifest["pending_folders"] == []
    assert manifest["shortcuts_skipped"] == 1
    # Breadth first; by name within a folder.
    assert [f["id"] for f in manifest["files"]] == [
        ids["a"],
        ids["b"],
        ids["e"],
        ids["c"],
        ids["d"],
    ]
    assert manifest["folders"][manifest["files"][4]["folder_id"]] == "Sub/Deeper"
    assert summary.total == 5
    # The one chunk file is the only download.
    assert downloads == [ids["a"]]


def test_a_listing_stopped_by_the_deadline_resumes_where_it_stopped(
    settings, wired, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    ids = _tree(drive)
    ticks = iter([False, False, True])  # two folders, then the deadline
    monkeypatch.setattr(flow, "shared_deadline_passed", lambda: next(ticks, False))

    with pytest.raises(RunOutOfTime):
        flow.library_backfill(run_id="m-1")

    partial = _manifest(drive)
    assert partial["complete"] is False
    assert len(partial["visited_folders"]) == 2
    assert partial["pending_folders"]
    assert _cursor(drive) is None
    assert _music_writes(drive) == []

    calls_before = len(drive.calls)
    flow.library_backfill(run_id="m-2")

    manifest = _manifest(drive)
    assert manifest["complete"] is True
    assert manifest["generation"] == partial["generation"]
    # Not listed again from the root: the visited folders are not re-read.
    assert manifest["visited_folders"][:2] == partial["visited_folders"]
    assert [f["id"] for f in manifest["files"]] == [
        ids["a"],
        ids["b"],
        ids["e"],
        ids["c"],
        ids["d"],
    ]
    assert len(drive.calls) > calls_before


def test_a_listing_in_progress_is_saved_every_few_folders(
    settings, wired, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(flow, "LISTING_SAVE_EVERY", 2)
    _tree(drive)
    saves: list[int] = []
    real = flow.StateFile.save

    def save(self, data):
        if self.name == flow.MANIFEST_NAME:
            saves.append(len(data["pending_folders"]))
        return real(self, data)

    monkeypatch.setattr(flow.StateFile, "save", save)

    flow.library_backfill(run_id="m-1")

    assert len(saves) >= 2
    assert saves[-1] == 0


def test_two_manifests_in_the_state_folder_stop_the_run(
    settings, wired, posted, drive
) -> None:
    _library(drive, 1)
    drive.add(flow.MANIFEST_NAME, STATE, b"{}", mime_type="application/json")
    drive.add(flow.MANIFEST_NAME, STATE, b"{}", mime_type="application/json")

    flow.library_backfill(run_id="m-1")

    assert _music_writes(drive) == []
    assert "trash all but one" in _text(posted)


def test_a_manifest_of_another_root_stops_the_run(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    _library(drive, 1)
    flow.library_backfill(run_id="m-1")
    monkeypatch.setenv(flow.ROOT_FOLDER_VAR, "another-root")
    calls_before = len(drive.calls)

    flow.library_backfill(run_id="m-2")

    assert drive.calls[calls_before:] == []
    assert "trash it to list the new root" in _text(posted)


# ---------------------------------------------------------------------------
# The cursor and chunks
# ---------------------------------------------------------------------------


def test_each_run_takes_the_next_chunk_and_the_cursor_resumes(
    settings, wired, drive, downloads, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(flow.CHUNK_SIZE_VAR, "2")
    ids = _library(drive, 5)

    first = flow.library_backfill(run_id="m-1")
    assert first.scanned == 2
    assert _cursor(drive)["position"] == 2

    second = flow.library_backfill(run_id="m-2")
    assert second.scanned == 2
    assert _cursor(drive)["position"] == 4

    third = flow.library_backfill(run_id="m-3")
    assert third.scanned == 1
    assert third.done is True
    assert _cursor(drive)["position"] == 5
    assert _cursor(drive)["done_at"]

    # Each file read once (and once more to check the write), in manifest
    # order: never a rescan.
    assert list(dict.fromkeys(downloads)) == ids
    assert len(downloads) == 2 * len(ids)
    assert _cursor(drive)["counts"]["written"] == 5


def test_the_chunk_is_ten_files_by_default(settings, wired, drive) -> None:
    _library(drive, 12)

    summary = flow.library_backfill(run_id="m-1")

    assert summary.scanned == 10
    assert summary.remaining == 2


def test_at_the_end_of_the_library_a_run_does_nothing(
    settings, wired, posted, drive, downloads
) -> None:
    _library(drive, 2)
    flow.library_backfill(run_id="m-1")
    downloads.clear()
    writes_before = len(_music_writes(drive))

    summary = flow.library_backfill(run_id="m-2")

    assert summary.done is True
    assert summary.scanned == 0
    assert downloads == []
    assert len(_music_writes(drive)) == writes_before
    assert "trash library-backfill-manifest.json" in _text(posted)


def test_trashing_the_manifest_rebuilds_it_and_starts_from_the_first_file(
    settings, wired, posted, drive
) -> None:
    _library(drive, 2)
    flow.library_backfill(run_id="m-1")
    old = _manifest(drive)
    (item,) = [i for i in drive.in_folder(STATE) if i.name == flow.MANIFEST_NAME]
    item.trashed = True
    _library(drive, 1, folder=ROOT)  # a file added since

    summary = flow.library_backfill(run_id="m-2")

    new = _manifest(drive)
    assert new["generation"] != old["generation"]
    assert len(new["files"]) == 3
    assert summary.restarted is True
    assert summary.start == 0
    assert summary.scanned == 3
    assert _cursor(drive)["generation"] == new["generation"]
    assert "starting from its first file" in _text(posted)


def test_a_redelivered_run_finishes_its_own_chunk_not_a_new_one(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(flow.CHUNK_SIZE_VAR, "3")
    _library(drive, 8)
    flow.library_backfill(run_id="m-0")  # lists, and does the first 3
    ticks = iter([False, True])
    monkeypatch.setattr(flow, "shared_deadline_passed", lambda: next(ticks, False))

    with pytest.raises(RunOutOfTime):
        flow.library_backfill(run_id="m-1")

    record = _record(drive, "m-1")
    assert record["status"] == "stopped"
    assert [e["state"] for e in record["entries"]] == ["done"]
    assert _cursor(drive)["position"] == 4
    assert "Stopped early" in _text(posted)

    # The same message comes back: it finishes files 5 and 6, its chunk,
    # rather than take three more, and carries on the same record.
    again = flow.library_backfill(run_id="m-1")
    assert again.scanned == 2
    assert _cursor(drive)["position"] == 6
    record = _record(drive, "m-1")
    assert record["status"] == "complete"
    assert len(record["entries"]) == 3


# ---------------------------------------------------------------------------
# The dry run
# ---------------------------------------------------------------------------


def test_a_dry_run_writes_no_music_and_does_not_move_the_cursor(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(flow.MODE_VAR, "dry-run")
    monkeypatch.setenv(flow.CHUNK_SIZE_VAR, "2")
    ids = _library(drive, 3)
    before = {i: drive.items[i].data for i in ids}

    first = flow.library_backfill(run_id="d-1")
    second = flow.library_backfill(run_id="d-2")

    assert _music_writes(drive) == []
    assert not any(m == "keep_revision" for m, _ in drive.calls)
    assert {i: drive.items[i].data for i in ids} == before
    assert _cursor(drive) is None
    assert first.start == second.start == 0
    assert len(first.outcomes["would_write"]) == 2
    record = _record(drive, "d-1", dry=True)
    assert record["dry_run"] is True
    entry = record["entries"][0]
    assert entry["state"] == "planned"
    # Only the link changes: the empty genre stays empty.
    changed = {
        k for k, v in entry["new_tags"].items() if entry["old_tags"].get(k, "") != v
    }
    assert changed == {"catalog_id"}
    assert entry["folder"] == ROOT
    assert "catalog_id: '' -> " in first.outcomes["would_write"][0]
    assert "genre:" not in first.outcomes["would_write"][0]
    assert "title:" not in first.outcomes["would_write"][0]
    assert "dry-run" in _text(posted)

    # Then written: the same chunk the dry run showed.
    monkeypatch.setenv(flow.MODE_VAR, "write")
    written = flow.library_backfill(run_id="w-1")
    assert written.start == 0
    assert [e["file_id"] for e in _record(drive, "w-1")["entries"]] == ids[:2]


# ---------------------------------------------------------------------------
# Each path a file can take
# ---------------------------------------------------------------------------


def test_a_catalog_match_is_written_in_place_and_recorded(
    settings, wired, drive
) -> None:
    original = _song("song 0", "artist")
    file_id = drive.add("Song_Artist.mp3", ROOT, original)

    summary = flow.library_backfill(run_id="m-1")

    item = drive.items[file_id]
    assert item.name == "Song_Artist.mp3"
    assert item.parents == [ROOT]
    assert not item.trashed
    tags = tag_snapshot(item.data)
    assert tags["title"] == "song 0"
    assert tags["genre"] == ""  # empty stays empty: only the id is written
    assert tags["catalog_id"] == str(_CATALOG_ID)
    assert "Some Album" in str(item.data)  # other frames kept
    assert [m for m, _ in _music_writes(drive)] == ["keep_revision", "update_file"]
    assert item.kept == {"r1"}
    assert summary.outcomes["written"] == ["Song_Artist.mp3"]
    (entry,) = _record(drive, "m-1")["entries"]
    assert entry["action"] == "in_place"
    assert entry["state"] == "done"
    assert entry["folder"] == ROOT
    assert entry["revision_kept"] == "r1"
    assert entry["original_md5"] and entry["written_md5"]
    assert entry["old_tags"]["genre"] == entry["new_tags"]["genre"] == ""
    assert entry["new_tags"]["catalog_id"] == str(_CATALOG_ID)


def test_a_file_already_tagged_from_the_catalog_is_not_written(
    settings, wired, drive
) -> None:
    file_id = drive.add("a.mp3", ROOT, _song("song 0", "artist"))
    flow.library_backfill(run_id="m-1")
    tagged = drive.items[file_id].data
    (item,) = [i for i in drive.in_folder(STATE) if i.name == flow.MANIFEST_NAME]
    item.trashed = True  # start over over the same library
    writes_before = len(_music_writes(drive))

    summary = flow.library_backfill(run_id="m-2")

    assert summary.outcomes["already_tagged"] == ["a.mp3"]
    assert len(_music_writes(drive)) == writes_before
    assert drive.items[file_id].data == tagged


def test_files_left_as_they_are_are_named(
    settings, wired, posted, drive, downloads, monkeypatch: pytest.MonkeyPatch
) -> None:
    sub = drive.add("Crates", ROOT, mime_type=FOLDER)
    unknown = drive.add("1 unknown.mp3", sub, _song("never played", "nobody"))
    untagged = drive.add("2 untagged.mp3", sub, mp3())
    flac = drive.add("3 song.flac", sub, _song(), mime_type="audio/flac")
    moved = drive.add("4 moved.mp3", sub, _song("song 1", "artist"))
    skipped = drive.add("5 skip.mp3", sub, _song("song 2", "artist"))
    monkeypatch.setenv(flow.SKIP_IDS_VAR, f" {skipped} ,other")
    real_list = flow.list_one_folder

    def list_then_owner_moves_one(drive_, manifest, **kw):
        real_list(drive_, manifest, **kw)
        if any(f["id"] == moved for f in manifest["files"]):
            drive.items[moved].parents = ["elsewhere"]

    monkeypatch.setattr(flow, "list_one_folder", list_then_owner_moves_one)
    sent = MagicMock()
    monkeypatch.setattr("deejay_cog.fingerprint_jobs.FingerprintJobs.send", sent)

    summary = flow.library_backfill(run_id="m-1")

    assert _music_writes(drive) == []
    assert summary.outcomes["not_in_catalog"] == [
        "Crates/1 unknown.mp3 (looked up 'never played' by 'nobody')"
    ]
    assert summary.outcomes["needs_fingerprinting"] == ["Crates/2 untagged.mp3"]
    assert summary.outcomes["not_mp3"] == ["Crates/3 song.flac"]
    assert summary.outcomes["moved_since"] == ["Crates/4 moved.mp3"]
    assert summary.outcomes["skipped_by_setting"] == ["Crates/5 skip.mp3"]
    sent.assert_not_called()  # library files never go to fingerprinting
    assert flac not in downloads and moved not in downloads
    assert skipped not in downloads
    assert unknown in downloads and untagged in downloads
    record = _record(drive, "m-1")
    assert {e["action"] for e in record["entries"]} == {"skip"}
    by_name = {e["original_name"]: e for e in record["entries"]}
    # A miss keeps the tags it was looked up by, so the record shows why.
    assert by_name["1 unknown.mp3"]["old_tags"]["title"] == "never played"
    assert by_name["1 unknown.mp3"]["old_tags"]["artist"] == "nobody"
    assert by_name["2 untagged.mp3"]["old_tags"]["title"] == ""
    assert _cursor(drive)["position"] == 5
    text = _text(posted)
    for name in ("1 unknown.mp3", "2 untagged.mp3", "3 song.flac", "4 moved.mp3"):
        assert name in text


def test_every_file_in_the_chunk_is_named_in_the_report(
    settings, wired, posted, drive
) -> None:
    names = [
        drive.add(f"{n} unknown.mp3", ROOT, _song(f"never played {n}", "nobody"))
        for n in range(6)
    ]

    flow.library_backfill(run_id="m-1")

    text = _text(posted)
    assert "more" not in text
    for n in range(len(names)):
        assert f"{n} unknown.mp3" in text


def test_a_failing_file_is_recorded_and_passed(
    settings, wired, posted, drive, api, monkeypatch: pytest.MonkeyPatch
) -> None:
    _library(drive, 2)
    calls = iter([RuntimeError("api down")])
    real = api.lookup_catalog

    def flaky(payload):
        exc = next(calls, None)
        if exc:
            raise exc
        return real(payload)

    monkeypatch.setattr(api, "lookup_catalog", flaky)

    summary = flow.library_backfill(run_id="m-1")

    assert len(summary.outcomes["failed"]) == 1
    assert len(summary.outcomes["written"]) == 1
    cursor = _cursor(drive)
    assert cursor["position"] == 2
    assert cursor["failed"][0]["name"] == "00 song.mp3"
    assert posted.call_args.args[1] == "WARN"


def test_a_record_that_cannot_be_saved_stops_before_any_music_write(
    settings, wired, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    ids = _library(drive, 2)
    real = drive.upload_bytes

    def refuse_records(**kw):
        if kw["filename"].startswith(f"{flow.FLOW_NAME}-m-"):
            raise RuntimeError("state folder not writable")
        return real(**kw)

    monkeypatch.setattr(drive, "upload_bytes", refuse_records)

    with pytest.raises(RuntimeError, match="not writable"):
        flow.library_backfill(run_id="m-1")

    assert _music_writes(drive) == []
    assert [drive.items[i].revisions[-1] for i in ids] == [
        drive.items[i].revisions[0] for i in ids
    ]
    assert _cursor(drive)["position"] == 0


def test_no_tag_is_emptied(
    settings, wired, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    file_id = drive.add("a.mp3", ROOT, _song("song 0", "artist"))
    monkeypatch.setattr(
        flow, "write_catalog_tags", lambda *_, **__: mp3(TALB(text=["x"]))
    )

    summary = flow.library_backfill(run_id="m-1")

    assert "would empty" in summary.outcomes["failed"][0]
    assert _music_writes(drive) == []
    assert drive.items[file_id].data == _song("song 0", "artist")


# ---------------------------------------------------------------------------
# The audio check
# ---------------------------------------------------------------------------


def test_audio_that_would_change_stops_the_run_and_keeps_the_cursor_on_it(
    settings, wired, posted, errors, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    ids = _library(drive, 3)
    bad = _song("Song 0", "Artist", audio=AUDIO[:-1] + b"\x01")
    monkeypatch.setattr(flow, "write_catalog_tags", lambda *_, **__: bad)

    summary = flow.library_backfill(run_id="m-1")

    assert summary.aborted
    assert summary.scanned == 1
    assert _music_writes(drive) == []
    assert _cursor(drive)["position"] == 0
    record = _record(drive, "m-1")
    assert record["status"] == "aborted"
    assert record["entries"][0]["file_id"] == ids[0]
    errors.assert_called_once()
    assert errors.call_args.args[:2] == ("library-backfill", "ERROR")
    assert posted.call_args.args[1] == "WARN"

    # The next run tries that file first, and stops again without writing.
    again = flow.library_backfill(run_id="m-2")
    assert again.aborted and again.start == 0
    assert _music_writes(drive) == []

    # Listed in the skip setting, it is passed and the rest are written.
    monkeypatch.setenv(flow.SKIP_IDS_VAR, ids[0])
    monkeypatch.setattr(flow, "write_catalog_tags", _real_write_catalog_tags)
    passed = flow.library_backfill(run_id="m-3")
    assert passed.outcomes["skipped_by_setting"] == ["00 song.mp3"]
    assert len(passed.outcomes["written"]) == 2


def test_audio_changed_by_the_write_is_restored_and_stops_the_run(
    settings, wired, errors, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    ids = _library(drive, 2)
    originals = {i: drive.items[i].data for i in ids}
    real_update = drive.update_file
    corrupted: list[str] = []

    def corrupting_update(file_id, filepath, *, mime_type=None):
        real_update(file_id, filepath, mime_type=mime_type)
        if file_id in originals and not corrupted:
            corrupted.append(file_id)
            data = drive.items[file_id].data
            drive.items[file_id].data = data[:-1] + bytes([data[-1] ^ 0xFF])

    monkeypatch.setattr(drive, "update_file", corrupting_update)

    summary = flow.library_backfill(run_id="m-1")

    assert summary.aborted
    assert drive.items[ids[0]].data == originals[ids[0]]  # restored
    assert drive.items[ids[1]].data == originals[ids[1]]  # never reached
    entry = _record(drive, "m-1")["entries"][0]
    assert entry["state"] == "restored"
    assert _cursor(drive)["position"] == 0
    errors.assert_called_once()


def test_a_write_cut_off_before_its_check_is_checked_anyway(
    settings, wired, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    (file_id,) = _library(drive, 1)
    original = drive.items[file_id].data
    real_update = drive.update_file
    real_download = drive.download_file_bytes
    state = {"written": False, "refused": False}

    def update(file_id_, filepath, *, mime_type=None):
        real_update(file_id_, filepath, mime_type=mime_type)
        if file_id_ == file_id and not state["written"]:
            state["written"] = True
            data = drive.items[file_id].data
            drive.items[file_id].data = data[:-1] + bytes([data[-1] ^ 0xFF])

    def download(file_id_):
        if file_id_ == file_id and state["written"] and not state["refused"]:
            state["refused"] = True
            raise TimeoutError("refused past the run deadline")
        return real_download(file_id_)

    monkeypatch.setattr(drive, "update_file", update)
    monkeypatch.setattr(drive, "download_file_bytes", download)

    summary = flow.library_backfill(run_id="m-1")

    assert summary.aborted
    assert drive.items[file_id].data == original
    assert _record(drive, "m-1")["entries"][0]["state"] == "restored"


# ---------------------------------------------------------------------------
# Rollback
# ---------------------------------------------------------------------------


def test_a_backfill_run_is_rolled_back_from_its_kept_revisions(
    settings, wired, drive
) -> None:
    ids = _library(drive, 2)
    drive.add("z.flac", ROOT, b"flac", mime_type="audio/flac")
    originals = {i: drive.items[i].data for i in ids}
    flow.library_backfill(run_id="m-1")
    assert all(drive.items[i].data != originals[i] for i in ids)
    drive.items[ids[1]].data += b"edited since"  # changed by the owner since
    before = len(drive.calls)

    _, dry_steps = rb.rollback(drive, STATE, "m-1", apply=False)
    assert drive.calls[before:] == []
    assert len(dry_steps) == 2

    _, steps = rb.rollback(drive, STATE, "m-1", apply=True)

    assert drive.items[ids[0]].data == originals[ids[0]]
    assert drive.items[ids[1]].data.endswith(b"edited since")  # left alone
    assert sorted(bool(s.skipped) for s in steps) == [False, True]
    for file_id in ids:
        assert drive.items[file_id].parents == [ROOT]
    assert not any(m in ("move_file", "trash_file") for m, _ in drive.calls)


def test_a_dry_backfill_run_cannot_be_rolled_back(
    settings, wired, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(flow.MODE_VAR, "dry-run")
    _library(drive, 1)
    flow.library_backfill(run_id="d-1")

    with pytest.raises(rb.RollbackError, match="dry run"):
        rb.rollback(drive, STATE, "d-1", apply=True)


_real_write_catalog_tags = flow.write_catalog_tags
