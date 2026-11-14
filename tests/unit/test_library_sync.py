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
from audio_fixtures import NAMES, OTHER_FORMATS, tagged
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

ROOT, STATE, FX = "root", "state", "fx"
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
        done = {i for m in self.marked for i in m.drive_file_ids}
        waiting = [
            r
            for r in self.rows
            if r.drive_file_id not in done
            and r.library_root_id == kwargs["library_root_id"]
        ]
        return waiting[: kwargs["limit"]]

    def read_catalog_display(self, payload: CatalogDisplayRequest):
        return [self.displays.get(i) for i in payload.ids]

    def mark_library_files_synced(self, payload: LibraryFilesSynced):
        self.marked.append(payload)
        return LibraryFilesSyncedResult(marked=len(payload.drive_file_ids))

    def list_library_files(self, **kwargs: Any) -> list[LibraryFile]:
        assert kwargs["present"] is True
        rows = [r for r in self.rows if r.library_root_id == kwargs["library_root_id"]]
        start = kwargs["offset"]
        return rows[start : start + kwargs["limit"]]

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
    root: str = ROOT,
) -> str:
    drive_id = drive.add(name, root, data)
    catalog.rows.append(
        LibraryFile(
            id=uuid.uuid4(),
            drive_file_id=drive_id,
            library_root_id=root,
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


def _listed(
    drive: FakeDrive, changed: dict[str, str], name: str = scan.LISTING_NAME
) -> None:
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
        name,
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


def test_the_edits_step_stops_at_its_share_and_the_sync_step_still_runs(
    settings, wired, drive, catalog, posted, monkeypatch
) -> None:
    ids = _flooded(drive, catalog, 3)
    # Seconds left at each check: the cutoff is half of the first.
    left = iter([100.0, 90.0, 80.0, 40.0])
    monkeypatch.setattr(sync, "remaining_seconds", lambda: next(left, 40.0))

    summary = sync.library_sync(run_id="y-1")

    assert not summary.stopped
    assert summary.edits_read == 1 and len(catalog.edited) == 1
    assert summary.edits_left == ids[1:]
    # The sync step ran, on the file whose edits were read only: one not
    # read must not be marked synced over its unread edit.
    assert [i for m in catalog.marked for i in m.drive_file_ids] == ids[:1]
    assert "changed_in_drive_left" in _text(posted)


def test_with_no_deadline_the_edits_step_reads_every_changed_file(
    settings, wired, drive, catalog, monkeypatch
) -> None:
    _flooded(drive, catalog, 3)
    monkeypatch.setattr(sync, "remaining_seconds", lambda: None)

    summary = sync.library_sync(run_id="y-1")

    assert summary.edits_read == 3 and summary.edits_left == []


def test_the_report_counts_the_run_records_read_for_replaced_values(
    settings, wired, drive, catalog
) -> None:
    [drive_id] = _flooded(drive, catalog, 1)
    _replaced_by_pipeline(drive, drive_id, {"genre": "Pop"}, {"genre": "R&B/Soul"})

    summary = sync.library_sync(run_id="y-1")

    assert summary.run_records_read == 1


def test_a_run_record_already_read_is_not_downloaded_again(
    settings, wired, drive, catalog
) -> None:
    [drive_id] = _flooded(drive, catalog, 1)
    _replaced_by_pipeline(drive, drive_id, {"genre": "Pop"}, {"genre": "R&B/Soul"})
    sync.library_sync(run_id="y-1")
    catalog.marked.clear()

    summary = sync.library_sync(run_id="y-2")

    assert summary.run_records_read == 0 and summary.run_records_left == 0
    index = _state(drive, sync.REPLACED_INDEX_NAME)
    assert index is not None and index["replaced"][drive_id] == {"genre": ["Pop"]}


def test_run_records_left_unread_hold_the_edits_and_keep_the_progress(
    settings, wired, drive, catalog, posted, monkeypatch
) -> None:
    ids = _flooded(drive, catalog, 2)
    for n, drive_id in enumerate(ids):
        record = {
            "flow": "library-reconcile",
            "run_id": f"r-{n}",
            "entries": [
                {
                    "file_id": drive_id,
                    "original_name": "x.mp3",
                    "action": "in_place",
                    "state": "done",
                    "old_tags": {"genre": "Pop"},
                    "new_tags": {"genre": "R&B/Soul"},
                }
            ],
        }
        drive.add(
            f"library-reconcile-r-{n}.json",
            STATE,
            json.dumps(record).encode(),
            mime_type="application/json",
        )
    # Seconds left at each check: the cutoff is half of the first; the
    # second record comes up after the share is spent.
    left = iter([100.0, 80.0, 40.0])
    monkeypatch.setattr(sync, "remaining_seconds", lambda: next(left, 40.0))

    summary = sync.library_sync(run_id="y-1")

    assert summary.run_records_read == 1 and summary.run_records_left == 1
    assert catalog.edited == [] and summary.edits_left == ids
    assert catalog.marked == []  # files with unread edits are not synced
    assert "run_records_left" in _text(posted)

    monkeypatch.setattr(sync, "remaining_seconds", lambda: None)
    again = sync.library_sync(run_id="y-2")

    assert again.run_records_read == 1 and again.run_records_left == 0
    assert again.edits_read == 2


def test_a_bad_bulk_limit_does_nothing(
    settings, wired, catalog, posted, monkeypatch
) -> None:
    monkeypatch.setenv(sync.EDIT_FLOOD_VAR, "lots")

    summary = sync.library_sync(run_id="y-1")

    assert summary.problems and catalog.marked == []
    assert sync.EDIT_FLOOD_VAR in _text(posted)


# -- stale write-backs, told by value ---------------------------------------


def _replaced_by_pipeline(
    drive: FakeDrive, drive_id: str, old: dict[str, str], new: dict[str, str]
) -> None:
    """A run record showing the pipeline replaced ``old`` with ``new`` in
    the file, in place."""
    record = {
        "flow": "library-reconcile",
        "run_id": "r-1",
        "entries": [
            {
                "file_id": drive_id,
                "original_name": drive.items[drive_id].name,
                "action": "in_place",
                "state": "done",
                "old_tags": old,
                "new_tags": new,
            }
        ],
    }
    drive.add(
        "library-reconcile-r-1.json",
        STATE,
        json.dumps(record).encode(),
        mime_type="application/json",
    )


def _changed(
    drive: FakeDrive, catalog: FakeCatalog, entry: uuid.UUID, now: bytes, name: str
) -> str:
    """A linked file that held the catalog's values at its last sync and has
    held ``now`` since a tool wrote it after."""
    held = _song(entry, "Honesty", "Pink Sweat$ feat. Kehlani", bpm="95.5")
    drive_id = _linked(drive, catalog, name, held, entry)
    drive.items[drive_id].revision_times[0] = _BEFORE_SYNC
    drive.rewrite(drive_id, now, _AFTER_SYNC)
    return drive_id


def test_the_backstop_is_high_by_default() -> None:
    assert sync.DEFAULT_EDIT_FLOOD == 1000


def test_more_files_than_the_old_limit_with_real_edits_all_go_up(
    settings, wired, drive, catalog
) -> None:
    """A big editing session is not a bulk change: none of its values is one
    the pipeline replaced, so every change goes up and nothing is put back."""
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    now = _song(entry, "Honesty", "Pink Sweat$ feat. Kehlani", "Neo Soul", bpm="95.5")
    ids = [_changed(drive, catalog, entry, now, f"honesty{n}.mp3") for n in range(30)]
    _listed(drive, dict.fromkeys(ids, _AFTER_SYNC))

    summary = sync.library_sync(run_id="y-1")

    assert summary.flood == 0 and summary.put_back == []
    assert {e.drive_file_id for e in catalog.edited} == set(ids)
    assert all(e.tags.genre == "Neo Soul" for e in catalog.edited)
    assert drive.writes(except_folder=STATE) == []


def test_a_value_the_pipeline_replaced_is_put_back_and_not_sent_up(
    settings, wired, drive, catalog, posted
) -> None:
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    stale = _song(entry, "honesty", "Pink Sweat$ feat. Kehlani", bpm="95.5")
    drive_id = _changed(drive, catalog, entry, stale, "honesty.mp3")
    _replaced_by_pipeline(drive, drive_id, {"title": "honesty"}, {"title": "Honesty"})
    _listed(drive, {drive_id: _AFTER_SYNC})

    summary = sync.library_sync(run_id="y-1")

    assert _frames(drive.items[drive_id].data)["TIT2"] == "Honesty"
    assert _frames(drive.items[drive_id].data)["COMM::eng"] == "cue: drop at 1:02"
    [edited] = catalog.edited
    assert edited.tags.title == "Honesty"  # what the file holds again
    assert summary.put_back == ["RnB/honesty.mp3: title 'honesty' -> 'Honesty'"]
    record = _state(drive, "library-sync-y-1.json")
    assert record is not None
    [put] = [e for e in record["entries"] if e["file_id"] == drive_id]
    assert put["identified_by"] == sync.STALE_PUT_BACK and put["state"] == "done"
    assert put["revision_kept"]  # music_rollback can undo it
    assert "stale write-back put back" in str(posted.call_args_list)


def _head(monkeypatch: pytest.MonkeyPatch, data: bytes, by: int) -> None:
    """Set the start a tag read takes to ``by`` bytes past ``data``'s tag
    (before it, when negative)."""
    from deejay_cog import music_safety

    length = music_safety.id3v2_length(data)
    assert length is not None
    monkeypatch.setattr(music_safety, "TAG_HEAD_BYTES", length + by)


@pytest.mark.parametrize("by", [16, -1], ids=["inside", "past"])
def test_edits_are_read_the_same_from_a_ranged_read(
    settings, wired, drive, catalog, monkeypatch, by
) -> None:
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    now = _song(entry, "Honesty", "Pink Sweat$ feat. Kehlani", "Neo Soul", bpm="95.5")
    drive_id = _changed(drive, catalog, entry, now, "honesty.mp3")
    _listed(drive, {drive_id: _AFTER_SYNC})
    _head(monkeypatch, now, by)

    summary = sync.library_sync(run_id="y-1")

    from deejay_cog.music_tags import read_tag_row

    [edited] = catalog.edited
    assert edited.tags.model_dump() == {
        k: v or None for k, v in read_tag_row(now).items()
    }
    assert summary.put_back == []
    # The whole file only when its tag runs past the start read.
    assert (drive_id in drive.downloads) is (by < 0)


def test_a_stale_put_back_is_written_from_the_whole_file(
    settings, wired, drive, catalog, monkeypatch
) -> None:
    from deejay_cog import music_safety

    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    stale = _song(entry, "honesty", "Pink Sweat$ feat. Kehlani", bpm="95.5")
    drive_id = _changed(drive, catalog, entry, stale, "honesty.mp3")
    _replaced_by_pipeline(drive, drive_id, {"title": "honesty"}, {"title": "Honesty"})
    _listed(drive, {drive_id: _AFTER_SYNC})
    _head(monkeypatch, stale, 16)
    seen: dict[str, list[bytes]] = {"tags": [], "in_place": [], "audio": []}
    real_tags, real_place = sync.write_owned_fields, sync.write_in_place
    real_audio = music_safety.check_audio_unchanged

    def tags(data, fields):
        seen["tags"].append(data)
        return real_tags(data, fields)

    def in_place(drive_, record, **kw):
        seen["in_place"].append(kw["original"])
        return real_place(drive_, record, **kw)

    def audio(original, candidate):
        seen["audio"].append(original)
        return real_audio(original, candidate)

    monkeypatch.setattr(sync, "write_owned_fields", tags)
    monkeypatch.setattr(sync, "write_in_place", in_place)
    monkeypatch.setattr(music_safety, "check_audio_unchanged", audio)

    summary = sync.library_sync(run_id="y-1")

    assert summary.put_back == ["RnB/honesty.mp3: title 'honesty' -> 'Honesty'"]
    assert seen["tags"] == seen["in_place"] == [stale]
    assert seen["audio"] and all(o == stale for o in seen["audio"])
    assert _frames(drive.items[drive_id].data)["TIT2"] == "Honesty"
    [edited] = catalog.edited
    assert edited.tags.title == "Honesty"


def test_a_file_with_both_has_the_stale_field_put_back_and_the_edit_sent_up(
    settings, wired, drive, catalog
) -> None:
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    both = _song(entry, "honesty", "Pink Sweat$ feat. Kehlani", "Neo Soul", "95.5")
    drive_id = _changed(drive, catalog, entry, both, "honesty.mp3")
    _replaced_by_pipeline(drive, drive_id, {"title": "honesty"}, {"title": "Honesty"})
    _listed(drive, {drive_id: _AFTER_SYNC})

    summary = sync.library_sync(run_id="y-1")

    frames = _frames(drive.items[drive_id].data)
    assert frames["TIT2"] == "Honesty" and frames["TCON"] == "Neo Soul"
    [edited] = catalog.edited
    assert edited.tags.title == "Honesty" and edited.tags.genre == "Neo Soul"
    assert summary.put_back == ["RnB/honesty.mp3: title 'honesty' -> 'Honesty'"]


def test_a_stale_field_with_no_revision_from_the_last_sync_is_left_and_named(
    settings, wired, drive, catalog, posted
) -> None:
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    stale = _song(entry, "honesty", "Pink Sweat$ feat. Kehlani", bpm="95.5")
    drive_id = _changed(drive, catalog, entry, stale, "honesty.mp3")
    drive.items[drive_id].revision_times[0] = None  # Drive kept none that old
    _replaced_by_pipeline(drive, drive_id, {"title": "honesty"}, {"title": "Honesty"})
    _listed(drive, {drive_id: _AFTER_SYNC})

    summary = sync.library_sync(run_id="y-1")

    assert _frames(drive.items[drive_id].data)["TIT2"] == "honesty"
    assert catalog.edited == [] and summary.put_back == []
    assert drive.writes(except_folder=STATE) == []
    assert "stale_write_back_left" in str(posted.call_args_list)


def test_a_last_sync_value_the_pipeline_also_replaced_is_not_put_back(
    settings, wired, drive, catalog
) -> None:
    """The revision from the last sync may itself hold a stale value; then
    no value the file held is safe, and the field is left."""
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    stale = _song(entry, "honesty", "Pink Sweat$ feat. Kehlani", bpm="95.5")
    drive_id = _changed(drive, catalog, entry, stale, "honesty.mp3")
    _replaced_by_pipeline(
        drive, drive_id, {"title": "honesty"}, {"title": "Honesty (Live)"}
    )
    drive.add(
        "library-sync-r-0.json",
        STATE,
        json.dumps(
            {
                "flow": "library-sync",
                "run_id": "r-0",
                "entries": [
                    {
                        "file_id": drive_id,
                        "original_name": "honesty.mp3",
                        "action": "in_place",
                        "state": "done",
                        "old_tags": {"title": "Honesty"},
                        "new_tags": {"title": "HONESTY"},
                    }
                ],
            }
        ).encode(),
        mime_type="application/json",
    )
    _listed(drive, {drive_id: _AFTER_SYNC})

    summary = sync.library_sync(run_id="y-1")

    assert _frames(drive.items[drive_id].data)["TIT2"] == "honesty"
    assert summary.put_back == [] and catalog.edited == []
    assert summary.stale_left


def test_a_dry_run_names_the_put_back_and_writes_nothing(
    settings, wired, drive, catalog, posted, monkeypatch
) -> None:
    monkeypatch.setitem(FLAGS, sync.MODE_FLAG, "dry-run")
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    stale = _song(entry, "honesty", "Pink Sweat$ feat. Kehlani", bpm="95.5")
    drive_id = _changed(drive, catalog, entry, stale, "honesty.mp3")
    _replaced_by_pipeline(drive, drive_id, {"title": "honesty"}, {"title": "Honesty"})
    _listed(drive, {drive_id: _AFTER_SYNC})

    summary = sync.library_sync(run_id="y-1")

    assert _frames(drive.items[drive_id].data)["TIT2"] == "honesty"
    assert drive.writes(except_folder=STATE) == []
    [edited] = catalog.edited
    assert edited.preview is True and edited.tags.title == "Honesty"
    assert summary.put_back and "would_put_back" in str(posted.call_args_list)


def test_a_dry_run_record_and_files_left_in_the_state_folder_are_not_records(
    settings, wired, drive, catalog
) -> None:
    """Only done, in-place writes in real run records count as replaced."""
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    stale = _song(entry, "honesty", "Pink Sweat$ feat. Kehlani", bpm="95.5")
    drive_id = _changed(drive, catalog, entry, stale, "honesty.mp3")
    planned = {
        "flow": "library-reconcile",
        "run_id": "r-2",
        "entries": [
            {
                "file_id": drive_id,
                "action": "in_place",
                "state": "planned",
                "old_tags": {"title": "honesty"},
                "new_tags": {"title": "Honesty"},
            }
        ],
    }
    drive.add(
        "library-reconcile-r-2.dry-run.json",
        STATE,
        json.dumps(planned).encode(),
        mime_type="application/json",
    )
    _listed(drive, {drive_id: _AFTER_SYNC})

    summary = sync.library_sync(run_id="y-1")

    assert summary.put_back == []
    [edited] = catalog.edited
    # An edit as far as the run can tell, so a title left for the review.
    assert edited.tags.title == "Honesty"
    assert summary.held_for_review == ["RnB/honesty.mp3: title 'Honesty' -> 'honesty'"]


# ---------------------------------------------------------------------------
# Owner edits
# ---------------------------------------------------------------------------

_BABY = "Baby What You Want Me To Do"


def test_an_owner_edit_entry_is_written_with_its_version_in_the_owner_edit_pair(
    settings, wired, drive, catalog
) -> None:
    entry = uuid.uuid4()
    # The entry's version was edited; it is an owner edit, shown in < >.
    catalog.displays[entry] = _display(
        entry, title=f"{_BABY} <Intro Cut>", artist="Etta James", owner_edit=True
    )
    drive_id = _linked(
        drive,
        catalog,
        "baby.mp3",
        _song(entry, f"{_BABY} <Intro Edit> (Clean)", "Etta James"),
        entry,
        label="Clean",
        synced=_synced(title=f"{_BABY} <Intro Edit> (Clean)", artist="Etta James"),
    )

    summary = sync.library_sync(run_id="y-1")

    frames = _frames(drive.items[drive_id].data)
    assert frames["TIT2"] == f"{_BABY} <Intro Cut> (Clean)"
    assert frames["COMM::eng"] == "cue: drop at 1:02"
    assert summary.written == ["RnB/baby.mp3 (title)"]


def test_a_file_retitled_as_an_owner_edit_is_left_for_the_reconcile(
    settings, wired, drive, catalog, posted
) -> None:
    entry = uuid.uuid4()
    # The published song's entry; its title changed in the catalog too.
    catalog.displays[entry] = _display(entry, title=f"{_BABY}!", artist="Etta James")
    retitled = f"{_BABY} <Intro Cut>"
    data = _song(entry, retitled, "Etta James")
    drive_id = _linked(
        drive,
        catalog,
        "baby.mp3",
        data,
        entry,
        synced=_synced(title=_BABY, artist="Etta James"),
    )
    _listed(drive, {drive_id: "2026-11-08T09:30:00.000Z"})

    summary = sync.library_sync(run_id="y-1")

    # Not sent up as an edit to the published entry, not written over, and
    # not marked synced: the reconcile links it to its owner-edit entry.
    assert catalog.edited == []
    assert drive.items[drive_id].data == data
    assert catalog.marked == []
    assert summary.owner_edits == [
        f"RnB/baby.mp3: title {retitled!r} (an owner edit: the reconcile links "
        "it to its own entry)"
    ]
    assert "owner_edit_left_for_reconcile" in _text(posted)


def test_an_owner_edit_on_its_own_entry_has_its_edits_sent_up_as_before(
    settings, wired, drive, catalog
) -> None:
    entry = uuid.uuid4()
    title = f"{_BABY} <Intro Cut>"
    catalog.displays[entry] = _display(
        entry, title=title, artist="Etta James", owner_edit=True
    )
    drive_id = _linked(
        drive,
        catalog,
        "baby.mp3",
        _song(entry, title, "Etta James", "Blues"),
        entry,
        synced=_synced(title=title, artist="Etta James"),
    )
    _listed(drive, {drive_id: "2026-11-08T09:30:00.000Z"})

    sync.library_sync(run_id="y-1")

    [edited] = catalog.edited
    assert edited.tags.title == title and edited.tags.genre == "Blues"


# ---------------------------------------------------------------------------
# More than one library root (ADR-0009, ADR-0010)
# ---------------------------------------------------------------------------


@pytest.fixture
def two_roots(settings, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(backfill.FX_ROOT_FOLDER_VAR, FX)


def test_the_catalog_is_never_written_over_an_fx_file(
    two_roots, wired, drive, catalog, posted
) -> None:
    entry, fx_entry = uuid.uuid4(), uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    catalog.displays[fx_entry] = _display(fx_entry)
    before = {"title": "Honesty (feat. Kehlani)", "artist": "Pink Sweat$"}
    main = _linked(
        drive,
        catalog,
        "honesty.mp3",
        _song(entry, "Honesty (feat. Kehlani)", "Pink Sweat$"),
        entry,
        synced=_synced(**before),
    )
    fx_data = _song(fx_entry, "Honesty (feat. Kehlani)", "Pink Sweat$")
    fx = _linked(
        drive,
        catalog,
        "bow.mp3",
        fx_data,
        fx_entry,
        synced=_synced(**before),
        root=FX,
    )

    summary = sync.library_sync(run_id="y-1")

    (_, main_part), (_, fx_part) = summary.roots
    assert main_part.written == ["RnB/honesty.mp3 (artist, title)"]
    assert _frames(drive.items[main].data)["TPE1"] == "Pink Sweat$ feat. Kehlani"
    # The FX file wins: not written, and recorded with what it already held.
    assert drive.items[fx].data == fx_data
    assert fx_part.written == [] and fx_part.fx_kept == ["RnB/bow.mp3 (artist, title)"]
    marked = {i: m.values[i] for m in catalog.marked for i in m.drive_file_ids}
    assert marked[fx] == _synced(**before)
    text = _text(posted)
    assert "FX Music: RnB/bow.mp3 (artist, title)" in text
    assert "Main library: RnB/honesty.mp3" in text


def test_an_edit_made_in_an_fx_file_goes_up_from_its_own_listing(
    two_roots, wired, drive, catalog
) -> None:
    fx_entry = uuid.uuid4()
    catalog.displays[fx_entry] = _display(fx_entry, title=_BABY, artist="Etta James")
    # A cut the owner retitled: an FX file's title is its own, so it goes up.
    retitled = f"{_BABY} <Intro Cut>"
    fx = _linked(
        drive,
        catalog,
        "baby.mp3",
        _song(fx_entry, retitled, "Etta James"),
        fx_entry,
        synced=_synced(title=_BABY, artist="Etta James"),
        root=FX,
    )
    _listed(drive, {fx: "2026-11-08T09:30:00.000Z"}, scan.listing_name(_fx_root()))

    summary = sync.library_sync(run_id="y-1")

    [edited] = catalog.edited
    assert edited.drive_file_id == fx and edited.tags.title == retitled
    assert summary.roots[1][1].owner_edits == []


@pytest.mark.parametrize("fmt", OTHER_FORMATS, ids=lambda f: f.value)
def test_an_fx_file_in_any_supported_format_is_never_written_and_its_edits_go_up(
    two_roots, wired, drive, catalog, fmt
) -> None:
    fx_entry = uuid.uuid4()
    catalog.displays[fx_entry] = _display(fx_entry)
    # Edited in the file: a genre of its own, and a title the catalog lacks.
    data = tagged(
        fmt,
        catalog_id=str(fx_entry),
        title="Honesty (Bow Cut)",
        artist="Pink Sweat$",
        genre="Neo Soul",
    )
    # Its entry moved since its last sync: the catalog would write, but the
    # FX file wins.
    fx = _linked(
        drive,
        catalog,
        NAMES[fmt],
        data,
        fx_entry,
        synced=_synced(artist="Pink Sweat$"),
        root=FX,
    )
    _listed(drive, {fx: "2026-11-08T09:30:00.000Z"}, scan.listing_name(_fx_root()))

    summary = sync.library_sync(run_id="y-1")

    [edited] = catalog.edited
    assert edited.drive_file_id == fx
    assert (edited.tags.title, edited.tags.genre) == ("Honesty (Bow Cut)", "Neo Soul")
    fx_part = summary.roots[1][1]
    assert fx_part.written == [] and fx_part.fx_kept == [f"RnB/{NAMES[fmt]} (artist)"]
    assert drive.items[fx].data == data
    assert drive.writes(except_folder=STATE) == []


def test_a_root_that_fails_does_not_stop_the_other(
    two_roots, wired, drive, catalog, posted, monkeypatch: pytest.MonkeyPatch
) -> None:
    fx_entry = uuid.uuid4()
    catalog.displays[fx_entry] = _display(fx_entry)
    fx = _linked(drive, catalog, "bow.mp3", b"", fx_entry, synced=None, root=FX)
    real = catalog.list_library_files_needing_sync

    def needing(**kwargs: Any) -> list[LibraryFile]:
        if kwargs["library_root_id"] == ROOT:
            raise RuntimeError("API down")
        return real(**kwargs)

    monkeypatch.setattr(catalog, "list_library_files_needing_sync", needing)

    with pytest.raises(RuntimeError, match="API down"):
        sync.library_sync(run_id="y-1")

    assert [i for m in catalog.marked for i in m.drive_file_ids] == [fx]
    assert "Main library: the run over this root failed" in _text(posted)


def _fx_root() -> backfill.LibraryRoot:
    settings, _ = backfill.BackfillSettings.from_env()
    assert settings is not None
    return settings.roots[1]


# -- a difference the file already had at its last sync is not an edit -------


def _junk_titled(drive: FakeDrive, catalog: FakeCatalog, entry: uuid.UUID) -> str:
    """A file whose old title tag already differed from its entry when the
    sync recorded the entry's values for it, and a pipeline write of
    another field (its genre) after that."""
    held = _song(entry, "Honesty Pink Sweat$", "Pink Sweat$ feat. Kehlani", bpm="95.5")
    drive_id = _linked(drive, catalog, "honesty.mp3", held, entry)
    drive.items[drive_id].revision_times[0] = _BEFORE_SYNC
    now = _song(
        entry, "Honesty Pink Sweat$", "Pink Sweat$ feat. Kehlani", "Neo Soul", "95.5"
    )
    drive.rewrite(drive_id, now, _AFTER_SYNC)
    _listed(drive, {drive_id: _AFTER_SYNC})
    return drive_id


def test_a_field_the_file_already_held_at_its_last_sync_is_not_sent_up(
    settings, wired, drive, catalog, posted
) -> None:
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    _junk_titled(drive, catalog, entry)

    summary = sync.library_sync(run_id="y-1")

    [edited] = catalog.edited
    assert edited.tags.title == "Honesty"  # the recorded value: no change
    assert edited.tags.genre == "Neo Soul"  # changed since the sync: goes up
    assert summary.already_differed == [
        "RnB/honesty.mp3: title 'Honesty Pink Sweat$' (recorded 'Honesty'; "
        "the file already held it at its last sync)"
    ]
    assert "not_an_edit_already_differed_at_last_sync" in str(posted.call_args_list)


def test_a_field_changed_since_the_last_sync_still_goes_up(
    settings, wired, drive, catalog
) -> None:
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    now = _song(entry, "Honesty", "Pink Sweat$ feat. Kehlani", "Neo Soul", "95.5")
    _changed(drive, catalog, entry, now, "honesty.mp3")
    _listed(drive, {catalog.rows[0].drive_file_id: _AFTER_SYNC})

    summary = sync.library_sync(run_id="y-1")

    [edited] = catalog.edited
    assert edited.tags.genre == "Neo Soul"
    assert summary.already_differed == [] and summary.held_for_review == []


# -- a title or artist edit waits for the owner -----------------------------


def test_a_title_or_artist_a_file_changed_is_left_for_the_review(
    settings, wired, drive, catalog, posted
) -> None:
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    now = _song(entry, "Honesty (Live)", "Pink Sweat$", "Neo Soul", "95.5")
    _changed(drive, catalog, entry, now, "honesty.mp3")
    _listed(drive, {catalog.rows[0].drive_file_id: _AFTER_SYNC})

    summary = sync.library_sync(run_id="y-1")

    [edited] = catalog.edited
    # The recorded values go up in their place: no change to the entry.
    assert edited.tags.title == "Honesty"
    assert edited.tags.artist == "Pink Sweat$ feat. Kehlani"
    assert edited.tags.genre == "Neo Soul"  # the other fields go up as before
    assert summary.held_for_review == [
        "RnB/honesty.mp3: title 'Honesty' -> 'Honesty (Live)'",
        "RnB/honesty.mp3: artist 'Pink Sweat$ feat. Kehlani' -> 'Pink Sweat$'",
    ]
    assert "title_or_artist_edit_left_for_review" in str(posted.call_args_list)


def test_with_no_revision_from_the_last_sync_the_field_goes_up_as_before(
    settings, wired, drive, catalog
) -> None:
    entry = uuid.uuid4()
    catalog.displays[entry] = _display(entry)
    drive_id = _junk_titled(drive, catalog, entry)
    drive.items[drive_id].revision_times[0] = None  # Drive kept none that old

    summary = sync.library_sync(run_id="y-1")

    [edited] = catalog.edited
    # Not proved to predate the sync, so an edit: a title, left for the review.
    assert edited.tags.title == "Honesty"
    assert summary.already_differed == []
    assert summary.held_for_review == [
        "RnB/honesty.mp3: title 'Honesty' -> 'Honesty Pink Sweat$'"
    ]
