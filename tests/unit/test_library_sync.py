"""The library-sync step: catalog edits and display-rule changes reach the
files they affect, in place and safely; only the fields whose catalog value
moved since the last sync are written, so a value the file keeps on purpose
survives an edit to another field; a dry run changes nothing."""

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
    LibraryFieldEdit,
    LibraryFile,
    LibraryFileEdited,
    LibraryFileEditResult,
    LibraryFilesSynced,
    LibraryFilesSyncedResult,
    LibraryFileTags,
)
from music_fakes import DriveItem, FakeDrive, mp3
from mutagen.id3 import COMM, ID3, TBPM, TCON, TIT2, TPE1, TXXX

from deejay_cog import library_backfill as backfill
from deejay_cog import library_scan as scan
from deejay_cog import library_sync as sync
from deejay_cog.music_safety import FlowMode
from flag_fakes import FLAGS

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


#: The values the last sync recorded for a file linked to ``_display()``.
SYNCED = LibraryFileTags(
    title="Honesty",
    artist="Pink Sweat$ feat. Kehlani",
    year="2019",
    genre="R&B/Soul",
    bpm="95.5",
)


def _synced(**kw: str | None) -> LibraryFileTags:
    return SYNCED.model_copy(update=kw)


def _song(
    entry: uuid.UUID,
    title: str,
    artist: str,
    genre: str = "R&B/Soul",
    bpm: str | None = None,
) -> bytes:
    frames: list[Any] = [
        TIT2(encoding=3, text=[title]),
        TPE1(encoding=3, text=[artist]),
        TCON(encoding=3, text=[genre]),
        COMM(encoding=3, lang="eng", desc="", text=["cue: drop at 1:02"]),
        TXXX(encoding=3, desc="CATALOG_ID", text=[str(entry)]),
    ]
    if bpm is not None:
        frames.append(TBPM(encoding=3, text=[bpm]))
    return mp3(*frames)


def _frames(data: bytes) -> dict[str, str]:
    return {k: str(f.text[0]) for k, f in ID3(io.BytesIO(data)).items()}


@dataclass
class FakeCatalog:
    rows: list[LibraryFile] = field(default_factory=list)
    displays: dict[uuid.UUID, CatalogDisplay] = field(default_factory=dict)
    marked: list[LibraryFilesSynced] = field(default_factory=list)
    edited: list[LibraryFileEdited] = field(default_factory=list)
    #: What the API answers for a file's edits, by Drive id.
    edit_answers: dict[str, list[LibraryFieldEdit]] = field(default_factory=dict)

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

    def list_library_files(self, **kwargs: Any) -> list[LibraryFile]:
        assert kwargs["present"] is True
        start = kwargs["offset"]
        return self.rows[start : start + kwargs["limit"]]

    def report_library_file_edited(self, payload: LibraryFileEdited):
        self.edited.append(payload)
        row = next(r for r in self.rows if r.drive_file_id == payload.drive_file_id)
        return LibraryFileEditResult(
            drive_file_id=row.drive_file_id,
            catalog_id=row.catalog_id,
            fields=self.edit_answers.get(row.drive_file_id, []),
        )


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
    monkeypatch.setitem(FLAGS, sync.MODE_FLAG, "write")
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
    synced: LibraryFileTags | None = SYNCED,
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
            last_synced=_NOW if synced is not None else None,
            synced=synced,
        )
    )
    return drive_id


def _listed(drive: FakeDrive, changed: dict[str, str]) -> None:
    """The scan's listing, with when Drive last saw each file change."""
    files = [
        {
            "id": drive_id,
            "name": drive.items[drive_id].name,
            "folder_id": ROOT,
            "mime_type": "audio/mpeg",
            "modified": when,
        }
        for drive_id, when in changed.items()
    ]
    drive.add(
        scan.LISTING_NAME,
        STATE,
        json.dumps({"files": files}).encode(),
        mime_type="application/json",
    )


def _state(drive: FakeDrive, name: str) -> dict | None:
    found = [i for i in drive.in_folder(STATE) if i.name == name]
    return json.loads(found[0].data) if found else None


def _text(posted: MagicMock) -> str:
    return posted.call_args.kwargs["text"]


def test_off_reads_nothing(
    settings, wired, drive, catalog, posted, monkeypatch
) -> None:
    monkeypatch.delitem(FLAGS, sync.MODE_FLAG, raising=False)

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
    before = {"title": "Honesty (feat. Kehlani)", "artist": "Pink Sweat$"}
    clean = _linked(
        drive,
        catalog,
        "honesty.mp3",
        _song(entry, "Honesty (feat. Kehlani)", "Pink Sweat$"),
        entry,
        synced=_synced(**before),
    )
    radio = _linked(
        drive,
        catalog,
        "honesty-radio.mp3",
        _song(entry, "Honesty (Radio Edit)", "Pink Sweat$"),
        entry,
        label="Radio Edit",
        synced=_synced(
            title="Honesty (feat. Kehlani) (Radio Edit)", artist="Pink Sweat$"
        ),
    )

    summary = sync.library_sync(run_id="y-1")

    assert _frames(drive.items[clean].data)["TIT2"] == "Honesty"
    assert _frames(drive.items[radio].data)["TIT2"] == "Honesty (Radio Edit)"
    for drive_id in (clean, radio):
        frames = _frames(drive.items[drive_id].data)
        assert frames["TPE1"] == "Pink Sweat$ feat. Kehlani"
        assert "TBPM" not in frames  # its value did not move: not written
        assert frames["COMM::eng"] == "cue: drop at 1:02"  # never written
        assert drive.items[drive_id].kept  # the revision before is kept
        assert drive.items[drive_id].parents == [ROOT]  # never moved
    assert sorted(summary.written) == [
        "RnB/honesty-radio.mp3 (artist, title)",
        "RnB/honesty.mp3 (artist, title)",
    ]
    [marked] = catalog.marked
    assert set(marked.drive_file_ids) == {clean, radio}
    assert marked.values[clean] == SYNCED
    assert marked.values[radio].title == "Honesty (Radio Edit)"
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


def test_a_held_bpm_survives_a_genre_change(settings, wired, drive, catalog) -> None:
    entry = uuid.uuid4()
    # The catalog says 95.5; the owner held the file's own 96 in the
    # reconcile. Then the entry's genre was edited.
    catalog.displays[entry] = _display(entry, genre="Neo Soul")
    drive_id = _linked(
        drive,
        catalog,
        "honesty.mp3",
        _song(entry, "Honesty", "Pink Sweat$ feat. Kehlani", bpm="96"),
        entry,
    )

    summary = sync.library_sync(run_id="y-1")

    frames = _frames(drive.items[drive_id].data)
    assert frames["TCON"] == "Neo Soul"
    assert frames["TBPM"] == "96"  # held: its catalog value did not move
    assert summary.written == ["RnB/honesty.mp3 (genre)"]
    assert catalog.marked[0].values[drive_id] == _synced(genre="Neo Soul")


def test_a_field_whose_catalog_value_moved_gets_it_over_the_files_own(
    settings, wired, drive, catalog
) -> None:
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry, bpm=96.0)
    # VirtualDJ wrote its own genre back; the entry's BPM was then edited.
    drive_id = _linked(
        drive,
        catalog,
        "honesty.mp3",
        _song(entry, "Honesty", "Pink Sweat$ feat. Kehlani", "House", bpm="95.5"),
        entry,
    )

    sync.library_sync(run_id="y-1")

    frames = _frames(drive.items[drive_id].data)
    assert frames["TBPM"] == "96"  # the catalog wins on the field that moved
    assert frames["TCON"] == "House"  # left for the reconcile to find


def test_a_file_with_no_recorded_values_gets_them_recorded_and_nothing_written(
    settings, wired, drive, catalog
) -> None:
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry, genre="Neo Soul")
    drive_id = _linked(
        drive,
        catalog,
        "honesty.mp3",
        _song(entry, "Honesty", "Pink Sweat$", bpm="96"),
        entry,
        synced=None,
    )

    summary = sync.library_sync(run_id="y-1")

    assert summary.recorded == ["RnB/honesty.mp3"] and summary.written == []
    assert drive.calls == []  # not even read
    [marked] = catalog.marked
    assert marked.values[drive_id] == _synced(genre="Neo Soul")


def test_an_entry_change_that_moves_no_field_writes_nothing(
    settings, wired, drive, catalog
) -> None:
    entry = uuid.uuid4()
    # Its subgenre changed, which a file does not carry; and the catalog no
    # longer has a BPM, so the file keeps its own.
    catalog.displays[entry] = _display(entry, bpm=None)
    drive_id = _linked(
        drive,
        catalog,
        "honesty.mp3",
        _song(entry, "Honesty", "Pink Sweat$ feat. Kehlani", bpm="96"),
        entry,
    )

    summary = sync.library_sync(run_id="y-1")

    assert summary.unmoved == ["RnB/honesty.mp3"] and summary.written == []
    assert drive.calls == []
    assert catalog.marked[0].values[drive_id] == _synced(bpm=None)


def test_a_file_already_in_step_is_marked_synced_without_a_write(
    settings, wired, drive, catalog
) -> None:
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry, genre="Neo Soul")
    # Its genre moved, and the file already says so.
    drive_id = _linked(
        drive,
        catalog,
        "honesty.mp3",
        _song(entry, "Honesty", "Pink Sweat$ feat. Kehlani", "Neo Soul"),
        entry,
    )

    summary = sync.library_sync(run_id="y-1")

    assert summary.already == ["RnB/honesty.mp3"] and summary.written == []
    assert drive.writes(except_folder=STATE) == []
    assert catalog.marked[0].drive_file_ids == [drive_id]


def test_a_dry_run_writes_nothing_and_marks_nothing(
    settings, wired, drive, catalog, posted, monkeypatch
) -> None:
    monkeypatch.setitem(FLAGS, sync.MODE_FLAG, "dry-run")
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    drive_id = _linked(
        drive,
        catalog,
        "honesty.mp3",
        _song(entry, "Honesty (feat. Kehlani)", "Pink Sweat$"),
        entry,
        synced=_synced(title="Honesty (feat. Kehlani)", artist="Pink Sweat$"),
    )
    before = drive.items[drive_id].data

    summary = sync.library_sync(run_id="y-1")

    assert summary.would_write == ["RnB/honesty.mp3 (artist, title)"]
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
            drive,
            catalog,
            f"{n}.mp3",
            _song(entry, "Honesty", "Pink Sweat$"),
            entry,
            synced=_synced(artist="Pink Sweat$"),
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
        synced=_synced(artist="Pink Sweat$"),
    )
    _linked(
        drive,
        catalog,
        "b.mp3",
        _song(entry, "Honesty (feat. Kehlani)", "Pink Sweat$"),
        entry,
        synced=_synced(artist="Pink Sweat$"),
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


def test_a_file_changed_in_drive_after_its_last_sync_has_its_edits_sent_up(
    settings, wired, drive, catalog, posted
) -> None:
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    # Edited in VirtualDJ the day after its last sync.
    drive_id = _linked(
        drive,
        catalog,
        "honesty.mp3",
        _song(entry, "Honesty", "Pink Sweat$ feat. Kehlani", "Neo Soul", bpm="95.5"),
        entry,
    )
    _listed(drive, {drive_id: "2026-11-08T09:30:00.000Z"})
    catalog.edit_answers[drive_id] = [
        LibraryFieldEdit(
            field="genre",
            synced="R&B/Soul",
            file="Neo Soul",
            catalog="R&B/Soul",
            outcome="sent_up",
        )
    ]

    summary = sync.library_sync(run_id="y-1")

    [edited] = catalog.edited
    assert edited.drive_file_id == drive_id and edited.preview is False
    assert edited.tags.genre == "Neo Soul" and edited.tags.bpm == "95.5"
    assert edited.tags.label is None  # a frame the file lacks is not sent
    assert summary.edits == {
        "sent_up": ["RnB/honesty.mp3: genre 'R&B/Soul' -> 'Neo Soul'"]
    }
    assert "sent up to the catalog" in _text(posted)


def test_a_file_not_changed_since_its_last_sync_is_not_read_for_edits(
    settings, wired, drive, catalog
) -> None:
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    # Last changed by the sync that wrote it, before it was marked synced.
    drive_id = _linked(
        drive, catalog, "honesty.mp3", _song(entry, "Honesty", "Pink Sweat$"), entry
    )
    _listed(drive, {drive_id: "2026-11-06T23:59:00.000Z"})

    summary = sync.library_sync(run_id="y-1")

    assert catalog.edited == [] and summary.edits_read == 0


def test_a_file_with_no_recorded_values_is_not_read_for_edits(
    settings, wired, drive, catalog
) -> None:
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    drive_id = _linked(
        drive,
        catalog,
        "honesty.mp3",
        _song(entry, "Honesty", "Pink Sweat$"),
        entry,
        synced=None,
    )
    _listed(drive, {drive_id: "2026-11-08T09:30:00.000Z"})

    sync.library_sync(run_id="y-1")

    assert catalog.edited == []


def test_a_dry_run_only_asks_what_would_become_of_the_edits(
    settings, wired, drive, catalog, posted, monkeypatch
) -> None:
    monkeypatch.setitem(FLAGS, sync.MODE_FLAG, "dry-run")
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    drive_id = _linked(
        drive,
        catalog,
        "honesty.mp3",
        _song(entry, "Honesty", "Pink Sweat$ feat. Kehlani", "Neo Soul"),
        entry,
    )
    _listed(drive, {drive_id: "2026-11-08T09:30:00.000Z"})
    catalog.edit_answers[drive_id] = [
        LibraryFieldEdit(field="genre", file="Neo Soul", outcome="sent_up"),
        LibraryFieldEdit(
            field="artist",
            file="Pink Sweat$ & Kehlani",
            outcome="held",
            why="not split into parts cleanly",
        ),
    ]

    sync.library_sync(run_id="y-1")

    assert catalog.edited[0].preview is True
    text = _text(posted)
    assert "would_send_up" in text and "not split into parts cleanly" in text


#: Before the last sync (``_NOW``), and after it.
_BEFORE_SYNC = "2026-11-06T00:00:00.000Z"
_AFTER_SYNC = "2026-11-08T09:30:00.000Z"


def _flooded(
    drive: FakeDrive,
    catalog: FakeCatalog,
    count: int,
    *,
    held: bytes | None = None,
    now: bytes | None = None,
) -> list[str]:
    """``count`` files a tagger rewrote at once after their last sync: each
    held ``held`` then (Drive's revision from before the sync) and holds
    ``now`` (its stale tags) since."""
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    held = held or _song(entry, "Honesty", "Pink Sweat$ feat. Kehlani", bpm="95.5")
    now = now or _song(entry, "honesty", "Pink Sweat$", "Contemporary R&b", bpm="95")
    ids = []
    for n in range(count):
        drive_id = _linked(drive, catalog, f"honesty{n}.mp3", held, entry)
        drive.items[drive_id].revision_times[0] = _BEFORE_SYNC
        drive.rewrite(drive_id, now, _AFTER_SYNC)
        ids.append(drive_id)
    _listed(drive, dict.fromkeys(ids, _AFTER_SYNC))
    return ids


def test_a_bulk_change_is_held_nothing_goes_up_and_nothing_is_written(
    settings, wired, drive, catalog, posted, monkeypatch
) -> None:
    """No automatic answer tells a stale write-back from a repair, so the
    files keep what they hold — not their last-sync revision, which may
    already be the stale one — and the run says so."""
    monkeypatch.setenv(sync.EDIT_FLOOD_VAR, "2")
    ids = _flooded(drive, catalog, 3)

    summary = sync.library_sync(run_id="y-1")

    assert summary.flood == 3 and catalog.edited == []
    assert drive.writes(except_folder=STATE) == []
    for drive_id in ids:
        assert _frames(drive.items[drive_id].data)["TIT2"] == "honesty"
    marked = {i: v for m in catalog.marked for i, v in m.values.items()}
    assert set(marked) >= set(ids)
    assert all(marked[i] == SYNCED for i in ids)
    assert "bulk_change_held" in str(posted.call_args_list)


def test_edits_under_the_bulk_limit_still_go_up(
    settings, wired, drive, catalog, monkeypatch
) -> None:
    monkeypatch.setenv(sync.EDIT_FLOOD_VAR, "3")
    _flooded(drive, catalog, 3)

    summary = sync.library_sync(run_id="y-1")

    assert summary.flood == 0 and len(catalog.edited) == 3


def test_a_bulk_change_in_a_dry_run_writes_nothing(
    settings, wired, drive, catalog, monkeypatch
) -> None:
    monkeypatch.setitem(FLAGS, sync.MODE_FLAG, "dry-run")
    monkeypatch.setenv(sync.EDIT_FLOOD_VAR, "2")
    ids = _flooded(drive, catalog, 3)

    summary = sync.library_sync(run_id="y-1")

    assert summary.flood == 3 and catalog.edited == [] and catalog.marked == []
    for drive_id in ids:
        assert _frames(drive.items[drive_id].data)["TIT2"] == "honesty"


def test_a_bad_bulk_limit_does_nothing(
    settings, wired, catalog, posted, monkeypatch
) -> None:
    monkeypatch.setenv(sync.EDIT_FLOOD_VAR, "lots")

    summary = sync.library_sync(run_id="y-1")

    assert summary.problems and catalog.marked == []
    assert sync.EDIT_FLOOD_VAR in _text(posted)
