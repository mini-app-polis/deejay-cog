"""The library-reconcile step: each difference between a linked file and
its catalog entry is filed under one of the twelve kinds, and the run reads
only, stepping through the library from its own cursor."""

from __future__ import annotations

import datetime as dt
import json
import uuid
from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from mini_app_polis import pipeline_status
from mini_app_polis.api.contract import (
    CatalogDisplay,
    CatalogDisplayRequest,
    LibraryFile,
)
from music_fakes import DriveItem, FakeDrive, mp3
from mutagen.id3 import COMM, TBPM, TCON, TDRC, TIT2, TPE1, TPUB, TXXX

from deejay_cog import library_backfill as backfill
from deejay_cog import library_reconcile as rec
from deejay_cog.music_safety import FlowMode

ROOT, STATE = "root", "state"
_NOW = dt.datetime(2026, 11, 6, tzinfo=dt.UTC)


# ---------------------------------------------------------------------------
# The twelve kinds
# ---------------------------------------------------------------------------


def _tags(**kw: str) -> dict[str, str]:
    base = {
        "title": "Attention (Lash Remix)",
        "artist": "Charlie Puth",
        "year": "2017",
        "genre": "Pop",
        "bpm": "100",
        "label": "Atlantic",
    }
    return {**base, **kw}


def _kinds(file: dict[str, str], catalog: dict[str, str]) -> dict[str, str]:
    return {d.field: d.kind for d in rec.classify("a.mp3", "f1", file, catalog)}


def test_a_file_that_says_what_the_catalog_says_has_no_difference() -> None:
    assert rec.classify("a.mp3", "f1", _tags(), _tags()) == []


@pytest.mark.parametrize(
    ("field_name", "file_value", "catalog_value"),
    [
        ("title", "attention (lash remix)", "Attention (Lash Remix)"),
        ("title", "Attention  (Lash Remix)", "Attention (Lash Remix)"),
        ("artist", "charlie puth", "Charlie Puth"),
        ("genre", "pop", "Pop"),
        ("label", "ATLANTIC", "Atlantic"),
    ],
)
def test_case_or_punctuation_only(field_name, file_value, catalog_value) -> None:
    kinds = _kinds(
        _tags(**{field_name: file_value}), _tags(**{field_name: catalog_value})
    )
    assert kinds == {field_name: "case_punctuation"}


def test_a_featured_artist_in_the_title_is_moved() -> None:
    file = _tags(title="Honesty (feat. Kehlani)", artist="Pink Sweat$")
    catalog = _tags(title="Honesty", artist="Pink Sweat$ feat. Kehlani")
    assert _kinds(file, catalog) == {
        "title": "featured_moved",
        "artist": "featured_moved",
    }


def test_a_trailing_ft_in_the_title_is_moved() -> None:
    file = _tags(title="Honesty ft. Kehlani", artist="Pink Sweat$")
    catalog = _tags(title="Honesty", artist="Pink Sweat$ feat. Kehlani")
    assert _kinds(file, catalog) == {
        "title": "featured_moved",
        "artist": "featured_moved",
    }


def test_artists_joined_differently() -> None:
    file = _tags(artist="Kelly Clarkson & Dolly Parton")
    catalog = _tags(artist="Kelly Clarkson, Dolly Parton")
    assert _kinds(file, catalog) == {"artist": "artist_joiner"}


def test_a_version_in_other_brackets() -> None:
    for title in ("Attention - Lash Remix", "Attention [Lash Remix]"):
        assert _kinds(_tags(title=title), _tags()) == {"title": "version_brackets"}


def test_a_genre_the_catalog_has_mapped() -> None:
    assert _kinds(_tags(genre="Neo Soul"), _tags(genre="R&B/Soul")) == {
        "genre": "genre_remapped"
    }


def test_a_genre_filled_where_the_file_has_none() -> None:
    assert _kinds(_tags(genre=""), _tags()) == {"genre": "genre_filled"}


def test_a_different_year() -> None:
    assert _kinds(_tags(year="2016"), _tags()) == {"year": "year_differs"}
    assert _kinds(_tags(year=""), _tags()) == {"year": "year_differs"}


def test_bpm_half_a_beat_apart_is_rounding_and_more_is_different() -> None:
    assert _kinds(_tags(bpm="96"), _tags(bpm="95.5")) == {"bpm": "bpm_rounding"}
    assert _kinds(_tags(bpm="100"), _tags(bpm="100.0")) == {}
    assert _kinds(_tags(bpm="98"), _tags(bpm="95.5")) == {"bpm": "bpm_differs"}
    assert _kinds(_tags(bpm="fast"), _tags(bpm="95.5")) == {"bpm": "bpm_differs"}


def test_a_different_label() -> None:
    assert _kinds(_tags(label="Warner"), _tags()) == {"label": "label_differs"}


@pytest.mark.parametrize("field_name", ["year", "genre", "bpm", "label"])
def test_a_file_value_for_an_empty_catalog_field_would_go_up(field_name) -> None:
    assert _kinds(_tags(), _tags(**{field_name: ""})) == {field_name: "fill_up"}


def test_anything_else_is_other() -> None:
    assert _kinds(_tags(title="Something Else"), _tags()) == {"title": "other"}
    assert _kinds(_tags(artist="Someone Else"), _tags()) == {"artist": "other"}


def test_the_expected_title_carries_the_files_own_edit_label() -> None:
    display = _display(uuid.uuid4(), title="Baby What You Want Me To Do")
    row = _row("f1", display.id, edit_label="No Intro Edit")
    assert rec.expected(display, row)["title"] == (
        "Baby What You Want Me To Do (No Intro Edit)"
    )
    plain = _row("f2", display.id)
    assert rec.expected(display, plain)["title"] == "Baby What You Want Me To Do"


# ---------------------------------------------------------------------------
# A run
# ---------------------------------------------------------------------------


def _display(entry: uuid.UUID, **kw) -> CatalogDisplay:
    values = {
        "id": entry,
        "title": "Attention (Lash Remix)",
        "artist": "Charlie Puth",
        "release_year": 2017,
        "genre": "Pop",
        "bpm": 100.0,
        "label": "Atlantic",
        "changed_at": _NOW,
        **kw,
    }
    return CatalogDisplay(**values)


def _row(drive_id: str, entry: uuid.UUID, **kw) -> LibraryFile:
    values = {
        "id": uuid.uuid4(),
        "drive_file_id": drive_id,
        "library_root_id": ROOT,
        "path": f"{drive_id}.mp3",
        "identification_status": "identified",
        "catalog_id": entry,
        "present": True,
        "trash_requested": False,
        "first_seen": _NOW,
        "last_seen": _NOW,
        **kw,
    }
    return LibraryFile(**values)


def _song(entry: uuid.UUID, **kw: str) -> bytes:
    t = _tags(**kw)
    return mp3(
        TIT2(encoding=3, text=[t["title"]]),
        TPE1(encoding=3, text=[t["artist"]]),
        TDRC(encoding=3, text=[t["year"]]),
        TCON(encoding=3, text=[t["genre"]]),
        TBPM(encoding=3, text=[t["bpm"]]),
        TPUB(encoding=3, text=[t["label"]]),
        COMM(encoding=3, lang="eng", desc="", text=["VirtualDJ cue notes"]),
        TXXX(encoding=3, desc="CATALOG_ID", text=[str(entry)]),
    )


@dataclass
class FakeCatalog:
    rows: list[LibraryFile] = field(default_factory=list)
    displays: dict[uuid.UUID, CatalogDisplay] = field(default_factory=dict)
    pages: list[dict] = field(default_factory=list)

    def list_library_files(self, **kwargs) -> list[LibraryFile]:
        self.pages.append(kwargs)
        assert kwargs["identification_status"] == "identified"
        assert kwargs["present"] is True
        start = kwargs["offset"]
        return self.rows[start : start + kwargs["limit"]]

    def read_catalog_display(self, payload: CatalogDisplayRequest):
        return [self.displays.get(i) for i in payload.ids]

    def __getattr__(self, name: str):
        raise AssertionError(f"the reconcile must not call {name}")


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
    monkeypatch.setenv(rec.MODE_VAR, "dry-run")
    monkeypatch.delenv(rec.CHUNK_SIZE_VAR, raising=False)
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
    monkeypatch.setattr(rec, "GoogleAPI", google)
    monkeypatch.setattr(rec, "api_client", lambda **_: catalog)
    monkeypatch.setattr(rec, "shared_deadline_passed", lambda: False)


def _library(drive: FakeDrive, catalog: FakeCatalog, songs: list[dict]) -> list[str]:
    ids = []
    for n, kw in enumerate(songs):
        entry = uuid.uuid4()
        label = kw.pop("edit_label", None)
        drive_id = drive.add(f"{n}.mp3", ROOT, _song(entry, **kw))
        catalog.rows.append(_row(drive_id, entry, path=f"{n}.mp3", edit_label=label))
        catalog.displays[entry] = _display(entry)
        ids.append(drive_id)
    return ids


def _state(drive: FakeDrive, name: str) -> dict | None:
    found = [i for i in drive.in_folder(STATE) if i.name == name]
    return json.loads(found[0].data) if found else None


def _text(posted: MagicMock) -> str:
    return posted.call_args.kwargs["text"]


def test_off_reads_nothing(
    settings, wired, drive, catalog, posted, monkeypatch
) -> None:
    monkeypatch.delenv(rec.MODE_VAR)
    _library(drive, catalog, [{}])

    summary = rec.library_reconcile(run_id="r-1")

    assert summary.mode is FlowMode.OFF
    assert catalog.pages == []
    assert drive.calls == []
    posted.assert_not_called()


def test_a_dry_run_files_each_difference_and_writes_no_music(
    settings, wired, drive, catalog, posted
) -> None:
    _library(
        drive,
        catalog,
        [
            {},
            {"genre": "Neo Soul", "bpm": "100.4"},
            {"title": "Unrelated Song"},
            {"title": "Attention (Lash Remix) (Clean)", "edit_label": "Clean"},
        ],
    )

    summary = rec.library_reconcile(run_id="r-1")

    assert summary.compared == 4
    assert summary.matching == 2  # the plain match and the labelled copy
    assert [d.path for d in summary.differences["genre_remapped"]] == ["1.mp3"]
    assert [d.path for d in summary.differences["bpm_rounding"]] == ["1.mp3"]
    assert [d.path for d in summary.differences["other"]] == ["2.mp3"]
    assert drive.writes(except_folder=STATE) == []

    record = _state(drive, "library-reconcile-r-1.dry-run.json")
    assert record["counts"]["genre_remapped"] == 1
    assert record["differences"]["other"][0] == {
        "path": "2.mp3",
        "drive_file_id": summary.differences["other"][0].drive_file_id,
        "field": "title",
        "file": "Unrelated Song",
        "catalog": "Attention (Lash Remix)",
    }
    text = _text(posted)
    assert "2.mp3" in text and "genre_remapped" in text


def test_the_comment_frame_is_never_compared(settings, wired, drive, catalog) -> None:
    _library(drive, catalog, [{}])

    summary = rec.library_reconcile(run_id="r-1")

    assert summary.matching == 1
    assert all(d.field != "comment" for ds in summary.differences.values() for d in ds)


def test_each_run_takes_the_next_chunk_and_wraps_at_the_end(
    settings, wired, drive, catalog, monkeypatch
) -> None:
    monkeypatch.setenv(rec.CHUNK_SIZE_VAR, "2")
    _library(drive, catalog, [{}, {}, {}])

    first = rec.library_reconcile(run_id="r-1")
    second = rec.library_reconcile(run_id="r-2")
    third = rec.library_reconcile(run_id="r-3")

    assert (first.start, first.position) == (0, 2)
    assert (second.start, second.position) == (2, 3)
    assert third.wrapped
    assert (third.start, third.position) == (0, 2)
    cursor = _state(drive, rec.CURSOR_NAME)
    assert cursor["position"] == 2 and cursor["passes"] == 1
    # Its own cursor, apart from the backfill's.
    assert _state(drive, backfill.CURSOR_NAME) is None


def test_write_runs_as_a_dry_run_and_says_so(
    settings, wired, drive, catalog, posted, monkeypatch
) -> None:
    monkeypatch.setenv(rec.MODE_VAR, "write")
    _library(drive, catalog, [{"year": "1999"}])

    summary = rec.library_reconcile(run_id="r-1")

    assert summary.requested_write
    assert summary.mode is FlowMode.DRY_RUN
    assert drive.writes(except_folder=STATE) == []
    assert "write_not_built" in _text(posted)


def test_a_file_linked_to_another_entry_is_other(
    settings, wired, drive, catalog
) -> None:
    [drive_id] = _library(drive, catalog, [{}])
    other = uuid.uuid4()
    entry = catalog.rows[0].catalog_id
    drive.items[drive_id].data = _song(other)
    catalog.displays[entry] = _display(entry)

    summary = rec.library_reconcile(run_id="r-1")

    [diff] = summary.differences["other"]
    assert (diff.field, diff.file_value) == ("catalog_id", str(other))


def test_a_failing_file_is_named_and_the_run_carries_on(
    settings, wired, drive, catalog, monkeypatch
) -> None:
    ids = _library(drive, catalog, [{}, {}])
    real = drive.download_file_bytes

    def flaky(file_id: str):
        if file_id == ids[0]:
            raise OSError("drive hiccup")
        return real(file_id)

    monkeypatch.setattr(drive, "download_file_bytes", flaky)

    summary = rec.library_reconcile(run_id="r-1")

    assert summary.compared == 1
    assert summary.failed and "drive hiccup" in summary.failed[0]
    assert summary.position == 2


def test_a_bad_chunk_size_does_nothing(
    settings, wired, drive, catalog, posted, monkeypatch
) -> None:
    monkeypatch.setenv(rec.CHUNK_SIZE_VAR, "0")

    summary = rec.library_reconcile(run_id="r-1")

    assert summary.problems
    assert catalog.pages == []
    assert rec.CHUNK_SIZE_VAR in _text(posted)
