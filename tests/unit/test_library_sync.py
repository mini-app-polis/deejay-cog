"""The library-sync step: catalog edits and display-rule changes reach the
files they affect, in place and safely; the catalog wins over a tag another
tool wrote; a dry run changes nothing."""

from __future__ import annotations

import datetime as dt
import io
import json
import uuid
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from mini_app_polis import pipeline_status
from mini_app_polis.api.contract import (
    CatalogDisplay,
    CatalogDisplayRequest,
    LibraryFile,
    LibraryFilesSynced,
    LibraryFilesSyncedResult,
)
from music_fakes import DriveItem, FakeDrive, mp3
from mutagen.id3 import COMM, ID3, TCON, TIT2, TPE1, TXXX

from deejay_cog import library_backfill as backfill
from deejay_cog import library_sync as sync
from deejay_cog.music_safety import FlowMode

ROOT, STATE = "root", "state"
_NOW = dt.datetime(2026, 11, 7, tzinfo=dt.UTC)


def _display(entry: uuid.UUID, **kw: Any) -> CatalogDisplay:
    values = {
        "id": entry,
        "title": "Honesty",
        "artist": "Pink Sweat$ feat. Kehlani",
        "release_year": 2019,
        "genre": "R&B/Soul",
        "bpm": 95.5,
        "label": None,
        "changed_at": _NOW,
        **kw,
    }
    return CatalogDisplay(**values)


def _song(entry: uuid.UUID, title: str, artist: str, genre: str = "R&B/Soul") -> bytes:
    return mp3(
        TIT2(encoding=3, text=[title]),
        TPE1(encoding=3, text=[artist]),
        TCON(encoding=3, text=[genre]),
        COMM(encoding=3, lang="eng", desc="", text=["cue: drop at 1:02"]),
        TXXX(encoding=3, desc="CATALOG_ID", text=[str(entry)]),
    )


def _frames(data: bytes) -> dict[str, str]:
    return {k: str(f.text[0]) for k, f in ID3(io.BytesIO(data)).items()}


@dataclass
class FakeCatalog:
    rows: list[LibraryFile] = field(default_factory=list)
    displays: dict[uuid.UUID, CatalogDisplay] = field(default_factory=dict)
    marked: list[LibraryFilesSynced] = field(default_factory=list)

    def list_library_files_needing_sync(self, **kwargs: Any) -> list[LibraryFile]:
        assert kwargs["library_root_id"] == ROOT
        done = {i for m in self.marked for i in m.drive_file_ids}
        waiting = [r for r in self.rows if r.drive_file_id not in done]
        return waiting[: kwargs["limit"]]

    def read_catalog_display(self, payload: CatalogDisplayRequest):
        return [self.displays.get(i) for i in payload.ids]

    def mark_library_files_synced(self, payload: LibraryFilesSynced):
        self.marked.append(payload)
        return LibraryFilesSyncedResult(marked=len(payload.drive_file_ids))


@pytest.fixture
def drive() -> FakeDrive:
    drive = FakeDrive()
    drive.items[STATE] = DriveItem(STATE, "state", ["elsewhere"], mime_type="folder")
    return drive


@pytest.fixture
def catalog() -> FakeCatalog:
    return FakeCatalog()


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(backfill.ROOT_FOLDER_VAR, ROOT)
    monkeypatch.setenv(backfill.STATE_FOLDER_VAR, STATE)
    monkeypatch.setenv(sync.MODE_VAR, "write")
    monkeypatch.delenv(sync.CHUNK_SIZE_VAR, raising=False)
    monkeypatch.delenv(backfill.CHUNK_SIZE_VAR, raising=False)


@pytest.fixture
def posted(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    fake = MagicMock()
    monkeypatch.setattr(pipeline_status, "post_run_finding", fake)
    return fake


@pytest.fixture
def wired(
    monkeypatch: pytest.MonkeyPatch, drive: FakeDrive, catalog: FakeCatalog
) -> None:
    google = MagicMock()
    google.from_env.return_value = SimpleNamespace(drive=drive)
    monkeypatch.setattr(sync, "GoogleAPI", google)
    monkeypatch.setattr(sync, "api_client", lambda **_: catalog)
    monkeypatch.setattr(sync, "shared_deadline_passed", lambda: False)


def _linked(
    drive: FakeDrive,
    catalog: FakeCatalog,
    name: str,
    data: bytes,
    entry: uuid.UUID,
    label: str | None = None,
) -> str:
    drive_id = drive.add(name, ROOT, data)
    catalog.rows.append(
        LibraryFile(
            id=uuid.uuid4(),
            drive_file_id=drive_id,
            library_root_id=ROOT,
            path=f"RnB/{name}",
            identification_status="identified",
            catalog_id=entry,
            edit_label=label,
            present=True,
            trash_requested=False,
            first_seen=_NOW,
            last_seen=_NOW,
        )
    )
    return drive_id


def _state(drive: FakeDrive, name: str) -> dict | None:
    found = [i for i in drive.in_folder(STATE) if i.name == name]
    return json.loads(found[0].data) if found else None


def _text(posted: MagicMock) -> str:
    return posted.call_args.kwargs["text"]


def test_off_reads_nothing(
    settings, wired, drive, catalog, posted, monkeypatch
) -> None:
    monkeypatch.delenv(sync.MODE_VAR)

    summary = sync.library_sync(run_id="y-1")

    assert summary.mode is FlowMode.OFF
    assert drive.calls == [] and catalog.marked == []
    posted.assert_not_called()


def test_an_entry_edit_reaches_every_file_linked_to_it(
    settings, wired, drive, catalog, posted
) -> None:
    entry = uuid.uuid4()
    # The entry was edited: its parts now write out a featured artist.
    catalog.displays[entry] = _display(entry)
    clean = _linked(
        drive,
        catalog,
        "honesty.mp3",
        _song(entry, "Honesty (feat. Kehlani)", "Pink Sweat$"),
        entry,
    )
    radio = _linked(
        drive,
        catalog,
        "honesty-radio.mp3",
        _song(entry, "Honesty (Radio Edit)", "Pink Sweat$"),
        entry,
        label="Radio Edit",
    )

    summary = sync.library_sync(run_id="y-1")

    assert _frames(drive.items[clean].data)["TIT2"] == "Honesty"
    assert _frames(drive.items[radio].data)["TIT2"] == "Honesty (Radio Edit)"
    for drive_id in (clean, radio):
        frames = _frames(drive.items[drive_id].data)
        assert frames["TPE1"] == "Pink Sweat$ feat. Kehlani"
        assert frames["TBPM"] == "95.5"
        assert frames["COMM::eng"] == "cue: drop at 1:02"  # never written
        assert drive.items[drive_id].kept  # the revision before is kept
        assert drive.items[drive_id].parents == [ROOT]  # never moved
    assert sorted(summary.written) == ["RnB/honesty-radio.mp3", "RnB/honesty.mp3"]
    [marked] = catalog.marked
    assert set(marked.drive_file_ids) == {clean, radio}
    assert summary.reached == {"Pink Sweat$ feat. Kehlani - Honesty": 2}
    assert "Pink Sweat$ feat. Kehlani - Honesty: 2 files" in _text(posted)
    assert _state(drive, "library-sync-y-1.json")["entries"][0]["action"] == "in_place"


def test_a_rule_change_rewrites_files_whose_entry_did_not_change(
    settings, wired, drive, catalog
) -> None:
    entry = uuid.uuid4()
    # The rules moved featured artists into the title.
    catalog.displays[entry] = _display(
        entry, title="Honesty (feat. Kehlani)", artist="Pink Sweat$"
    )
    drive_id = _linked(
        drive,
        catalog,
        "honesty.mp3",
        _song(entry, "Honesty", "Pink Sweat$ feat. Kehlani"),
        entry,
    )

    sync.library_sync(run_id="y-1")

    frames = _frames(drive.items[drive_id].data)
    assert (frames["TIT2"], frames["TPE1"]) == (
        "Honesty (feat. Kehlani)",
        "Pink Sweat$",
    )


def test_a_tag_another_tool_changed_gets_the_catalogs_value(
    settings, wired, drive, catalog
) -> None:
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    # VirtualDJ wrote its own genre back.
    drive_id = _linked(
        drive,
        catalog,
        "honesty.mp3",
        _song(entry, "Honesty", "Pink Sweat$ feat. Kehlani", genre="Neo Soul"),
        entry,
    )

    sync.library_sync(run_id="y-1")

    assert _frames(drive.items[drive_id].data)["TCON"] == "R&B/Soul"


def test_a_file_already_in_step_is_marked_synced_without_a_write(
    settings, wired, drive, catalog
) -> None:
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry, bpm=None, release_year=None)
    drive_id = _linked(
        drive,
        catalog,
        "honesty.mp3",
        _song(entry, "Honesty", "Pink Sweat$ feat. Kehlani"),
        entry,
    )

    summary = sync.library_sync(run_id="y-1")

    assert summary.already == ["RnB/honesty.mp3"] and summary.written == []
    assert drive.writes(except_folder=STATE) == []
    assert catalog.marked[0].drive_file_ids == [drive_id]


def test_a_dry_run_writes_nothing_and_marks_nothing(
    settings, wired, drive, catalog, posted, monkeypatch
) -> None:
    monkeypatch.setenv(sync.MODE_VAR, "dry-run")
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    drive_id = _linked(
        drive,
        catalog,
        "honesty.mp3",
        _song(entry, "Honesty (feat. Kehlani)", "Pink Sweat$"),
        entry,
    )
    before = drive.items[drive_id].data

    summary = sync.library_sync(run_id="y-1")

    assert summary.would_write == ["RnB/honesty.mp3"]
    assert drive.items[drive_id].data == before
    assert drive.writes(except_folder=STATE) == []
    assert catalog.marked == []
    assert "would_reach" in _text(posted)


def test_each_run_takes_a_chunk(settings, wired, drive, catalog, monkeypatch) -> None:
    monkeypatch.setenv(sync.CHUNK_SIZE_VAR, "2")
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    for n in range(3):
        _linked(
            drive, catalog, f"{n}.mp3", _song(entry, "Honesty", "Pink Sweat$"), entry
        )

    first = sync.library_sync(run_id="y-1")
    second = sync.library_sync(run_id="y-2")

    assert (len(first.written), len(second.written)) == (2, 1)


def test_an_entry_the_catalog_no_longer_has_is_named(
    settings, wired, drive, catalog, posted
) -> None:
    gone = uuid.uuid4()
    _linked(drive, catalog, "honesty.mp3", _song(gone, "Honesty", "Pink Sweat$"), gone)

    summary = sync.library_sync(run_id="y-1")

    assert summary.failed and catalog.marked == []
    assert "is gone" in _text(posted)


def test_audio_that_would_change_stops_the_run_and_marks_nothing(
    settings, wired, drive, catalog, monkeypatch
) -> None:
    from deejay_cog import music_safety

    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    _linked(
        drive,
        catalog,
        "a.mp3",
        _song(entry, "Honesty (feat. Kehlani)", "Pink Sweat$"),
        entry,
    )
    _linked(
        drive,
        catalog,
        "b.mp3",
        _song(entry, "Honesty (feat. Kehlani)", "Pink Sweat$"),
        entry,
    )
    finding = MagicMock()
    monkeypatch.setattr(sync, "post_run_finding", finding)

    def refuse(original, candidate):
        raise music_safety.AudioChanged("payload differs")

    monkeypatch.setattr(music_safety, "check_audio_unchanged", refuse)

    summary = sync.library_sync(run_id="y-1")

    assert summary.aborted and catalog.marked == []
    assert drive.writes(except_folder=STATE) == []
    assert finding.call_args.args[1] == "ERROR"


def test_a_bad_chunk_size_does_nothing(
    settings, wired, catalog, posted, monkeypatch
) -> None:
    monkeypatch.setenv(sync.CHUNK_SIZE_VAR, "none")

    summary = sync.library_sync(run_id="y-1")

    assert summary.problems and catalog.marked == []
    assert sync.CHUNK_SIZE_VAR in _text(posted)
