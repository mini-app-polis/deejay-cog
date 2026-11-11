"""The library-reconcile step: each difference between a linked file and
its catalog entry is filed under one of the twelve kinds, and the run reads
only, stepping through the library from its own cursor."""

from __future__ import annotations

import datetime as dt
import io
import json
import uuid
from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from mini_app_polis import pipeline_status
from mini_app_polis.api import KaianoApiError
from mini_app_polis.api.contract import (
    CatalogDisplay,
    CatalogDisplayRequest,
    CatalogFillUp,
    CatalogFillUpResult,
    LibraryFieldEdit,
    LibraryFile,
    LibraryFileEdited,
    LibraryFileEditResult,
    LibraryFileRegister,
    ReconcileApprovals,
    ReconcileReport,
    ReconcileReportResult,
)
from music_fakes import DriveItem, FakeDrive, mp3
from mutagen.id3 import COMM, TBPM, TCON, TDRC, TIT2, TPE1, TPUB, TXXX

from deejay_cog import library_backfill as backfill
from deejay_cog import library_reconcile as rec
from deejay_cog import library_scan as scan
from deejay_cog.library_backfill import StateFile
from deejay_cog.music_safety import FlowMode
from flag_fakes import FLAGS

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


def test_a_year_is_filled_earlier_or_later_by_which_is_older() -> None:
    assert _kinds(_tags(year=""), _tags()) == {"year": "year_filled"}
    assert _kinds(_tags(year="2023"), _tags()) == {"year": "year_earlier"}
    assert _kinds(_tags(year="2016"), _tags()) == {"year": "year_later"}
    assert _kinds(_tags(year="2018-03-02"), _tags()) == {"year": "year_earlier"}


def test_a_year_that_is_not_a_number_differs() -> None:
    assert _kinds(_tags(year="soon"), _tags()) == {"year": "year_differs"}
    assert _kinds(_tags(year="2017-01-01"), _tags()) == {"year": "case_punctuation"}


def test_bpm_under_one_and_a_half_beats_is_rounding_and_more_is_different() -> None:
    assert _kinds(_tags(bpm="96"), _tags(bpm="95.5")) == {"bpm": "bpm_rounding"}
    assert _kinds(_tags(bpm="100"), _tags(bpm="100.0")) == {}
    assert _kinds(_tags(bpm="98"), _tags(bpm="95.5")) == {"bpm": "bpm_differs"}
    # Less than 1.5 BPM is rounding; 1.5 or more, a different tempo.
    assert _kinds(_tags(bpm="112.84"), _tags(bpm="113.1")) == {"bpm": "bpm_rounding"}
    assert _kinds(_tags(bpm="100"), _tags(bpm="101.4")) == {"bpm": "bpm_rounding"}
    assert _kinds(_tags(bpm="100"), _tags(bpm="101.5")) == {"bpm": "bpm_differs"}
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
        listed = [r for r in self.rows if r.present]
        if "after" in kwargs:
            ids = [r.id for r in self.rows]
            if kwargs["after"] not in ids:
                raise KaianoApiError(404, "Library file not found", "/files")
            # The registry's own order: the rows after that one, wherever
            # it now stands.
            after = ids.index(kwargs["after"])
            listed = [r for r in self.rows[after + 1 :] if r.present]
            return listed[: kwargs["limit"]]
        start = kwargs["offset"]
        return listed[start : start + kwargs["limit"]]

    approvals: ReconcileApprovals = field(default_factory=ReconcileApprovals)
    reviews: list[ReconcileReport] = field(default_factory=list)
    review_fails: bool = False

    def read_catalog_display(self, payload: CatalogDisplayRequest):
        return [self.displays.get(i) for i in payload.ids]

    def read_reconcile_approvals(self) -> ReconcileApprovals:
        return self.approvals

    def report_reconcile_differences(self, payload: ReconcileReport):
        if self.review_fails:
            raise RuntimeError("API down")
        self.reviews.append(payload)
        return ReconcileReportResult(
            open_differences=len(payload.differences),
            files_with_differences=len({d.drive_file_id for d in payload.differences}),
        )


@dataclass
class WritingCatalog(FakeCatalog):
    """The catalog a write run may also fill up and register files with."""

    fills: list[tuple[str, CatalogFillUp]] = field(default_factory=list)
    registrations: list[LibraryFileRegister] = field(default_factory=list)
    kept: list[LibraryFileEdited] = field(default_factory=list)
    holds: dict[str, str] = field(default_factory=dict)

    def report_library_file_edited(self, payload: LibraryFileEdited):
        """The entry takes each kept field's file value, unless held."""
        self.kept.append(payload)
        row = next(r for r in self.rows if r.drive_file_id == payload.drive_file_id)
        assert row.catalog_id is not None
        entry = self.displays[row.catalog_id]
        tags = payload.tags.model_dump()
        fields, taken = [], {}
        for name in payload.keep_file:
            held = self.holds.get(name)
            fields.append(
                LibraryFieldEdit(
                    field=name,
                    file=tags[name],
                    outcome="held" if held else "sent_up",
                    why=held,
                )
            )
            if not held:
                taken[name] = tags[name]
        updates: dict = {}
        for name, value in taken.items():
            if name == "year":
                updates["release_year"] = int(value)
            elif name == "bpm":
                updates["bpm"] = float(value)
            else:
                updates[name] = value
        self.displays[row.catalog_id] = entry.model_copy(update=updates)
        return LibraryFileEditResult(
            drive_file_id=row.drive_file_id, catalog_id=row.catalog_id, fields=fields
        )

    def fill_up_catalog_entry(self, catalog_id, payload: CatalogFillUp):
        self.fills.append((str(catalog_id), payload))
        entry = self.displays[uuid.UUID(str(catalog_id))]
        filled = [k for k, v in payload.model_dump().items() if v is not None]
        return CatalogFillUpResult(entry=entry, filled=filled)

    def register_library_file(self, payload: LibraryFileRegister):
        self.registrations.append(payload)
        return payload

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
    monkeypatch.setitem(FLAGS, rec.MODE_FLAG, "dry-run")
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
    monkeypatch.delitem(FLAGS, rec.MODE_FLAG, raising=False)
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


def _forget_seen(drive: FakeDrive) -> None:
    """Empty the seen record, as ``reconcile_admin full --apply`` does."""
    store = StateFile(drive, STATE, rec.SEEN_NAME)
    store.load()
    store.save({"files": {}})


def _listed(drive: FakeDrive, modified: dict[str, str]) -> None:
    """The scan's listing, with when Drive last saw each file change."""
    found = [i for i in drive.in_folder(STATE) if i.name == scan.LISTING_NAME]
    data = json.dumps(
        {"files": [{"id": k, "modified": v} for k, v in modified.items()]}
    ).encode()
    if found:
        found[0].data = data
    else:
        drive.add(scan.LISTING_NAME, STATE, data, mime_type="application/json")


def test_each_run_takes_the_changed_files_a_chunk_at_a_time(
    settings, wired, drive, catalog, posted, monkeypatch
) -> None:
    monkeypatch.setenv(rec.CHUNK_SIZE_VAR, "2")
    ids = _library(drive, catalog, [{}, {}, {}])

    first = rec.library_reconcile(run_id="r-1")
    second = rec.library_reconcile(run_id="r-2")
    third = rec.library_reconcile(run_id="r-3")

    assert first.reviewed == ids[:2] and first.changed_left == 1
    assert second.reviewed == ids[2:] and second.changed_left == 0
    # Nothing changed since: nothing read, and the run stays quiet.
    assert third.compared == 0 and third.reviewed == []
    assert sorted(_state(drive, rec.SEEN_NAME)["files"]) == sorted(ids)


def test_a_write_run_with_nothing_changed_is_quiet(
    settings, writing, drive, posted
) -> None:
    _library(drive, writing, [{}])
    _approve(writing, kinds=["genre_remapped"])
    rec.library_reconcile(run_id="r-1")

    again = rec.library_reconcile(run_id="r-2")

    assert again.compared == 0
    assert _state(drive, "library-reconcile-r-2.differences.json") is None
    assert posted.call_args.kwargs["notable"] is False
    assert "Run complete" in _text(posted)  # no notices, nothing to name


def test_a_file_drive_saw_change_is_compared_again(
    settings, wired, drive, catalog
) -> None:
    ids = _library(drive, catalog, [{}, {}])
    _listed(drive, dict.fromkeys(ids, "2026-11-07T00:00:00.000Z"))
    rec.library_reconcile(run_id="r-1")
    _listed(
        drive, {ids[0]: "2026-11-07T00:00:00.000Z", ids[1]: "2026-11-08T09:30:00.000Z"}
    )

    again = rec.library_reconcile(run_id="r-2")

    assert again.reviewed == [ids[1]]


def test_an_entry_whose_display_changed_has_its_files_compared_again(
    settings, wired, drive, catalog
) -> None:
    ids = _library(drive, catalog, [{}, {}])
    rec.library_reconcile(run_id="r-1")
    entry = catalog.rows[0].catalog_id
    catalog.displays[entry] = catalog.displays[entry].model_copy(
        update={"changed_at": _NOW + dt.timedelta(days=1)}
    )

    again = rec.library_reconcile(run_id="r-2")

    assert again.reviewed == [ids[0]]


def test_a_file_new_to_the_library_is_compared(settings, wired, drive, catalog) -> None:
    ids = _library(drive, catalog, [{}])
    rec.library_reconcile(run_id="r-1")
    [new] = _library(drive, catalog, [{}])

    again = rec.library_reconcile(run_id="r-2")

    assert again.reviewed == [new] and new not in ids


def test_an_emptied_seen_record_compares_every_file_again(
    settings, wired, drive, catalog
) -> None:
    ids = _library(drive, catalog, [{}, {}])
    rec.library_reconcile(run_id="r-1")
    _forget_seen(drive)

    again = rec.library_reconcile(run_id="r-2")

    assert again.reviewed == ids


def test_a_file_gone_from_the_library_leaves_the_seen_record(
    settings, wired, drive, catalog
) -> None:
    ids = _library(drive, catalog, [{}, {}])
    rec.library_reconcile(run_id="r-1")
    catalog.rows[0] = catalog.rows[0].model_copy(update={"present": False})

    again = rec.library_reconcile(run_id="r-2")

    assert again.reviewed == []
    assert list(_state(drive, rec.SEEN_NAME)["files"]) == [ids[1]]


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
    # Not marked compared: the next run takes it again.
    assert list(_state(drive, rec.SEEN_NAME)["files"]) == [ids[1]]


def test_a_bad_chunk_size_does_nothing(
    settings, wired, drive, catalog, posted, monkeypatch
) -> None:
    monkeypatch.setenv(rec.CHUNK_SIZE_VAR, "0")

    summary = rec.library_reconcile(run_id="r-1")

    assert summary.problems
    assert catalog.pages == []
    assert rec.CHUNK_SIZE_VAR in _text(posted)


# ---------------------------------------------------------------------------
# The write run: only what the owner approved, catalog winning
# ---------------------------------------------------------------------------


@pytest.fixture
def writing(monkeypatch: pytest.MonkeyPatch, drive: FakeDrive) -> WritingCatalog:
    """A write run wired to a catalog that can fill up and register."""
    api = WritingCatalog()
    google = MagicMock()
    google.from_env.return_value = SimpleNamespace(drive=drive)
    monkeypatch.setattr(rec, "GoogleAPI", google)
    monkeypatch.setattr(rec, "api_client", lambda **_: api)
    monkeypatch.setattr(rec, "shared_deadline_passed", lambda: False)
    monkeypatch.setitem(FLAGS, rec.MODE_FLAG, "write")
    return api


def _approve(api: FakeCatalog, **approvals) -> None:
    api.approvals = ReconcileApprovals.model_validate(
        {**approvals, "updated_at": _NOW, "updated_by": "owner"}
    )


def _frames(data: bytes) -> dict[str, str]:
    from mutagen.id3 import ID3

    return {k: str(f.text[0]) for k, f in ID3(io.BytesIO(data)).items()}


def test_a_write_applies_only_approved_kinds_with_the_catalogs_value(
    settings, writing, drive, posted
) -> None:
    [drive_id] = _library(
        drive, writing, [{"genre": "Neo Soul", "year": "2016", "label": "ATLANTIC"}]
    )
    _approve(writing, kinds=["genre_remapped", "case_punctuation"])

    summary = rec.library_reconcile(run_id="r-1")

    frames = _frames(drive.items[drive_id].data)
    assert frames["TCON"] == "Pop"  # approved: genre_remapped
    assert frames["TPUB"] == "Atlantic"  # approved: case_punctuation
    assert frames["TDRC"] == "2016"  # year_later was not approved
    assert frames["COMM::eng"] == "VirtualDJ cue notes"  # never written
    assert summary.not_approved == 1
    assert summary.written == ["0.mp3 (genre, label)"]
    assert drive.items[drive_id].kept  # the revision before the write is kept
    record = _state(drive, "library-reconcile-r-1.json")
    assert record["entries"][0]["action"] == "in_place"
    [reg] = writing.registrations
    assert reg.synced_at is not None and reg.catalog_id == writing.rows[0].catalog_id
    # Nothing renamed or moved.
    assert drive.items[drive_id].name == "0.mp3"
    assert drive.items[drive_id].parents == [ROOT]


def test_after_a_write_a_dry_run_shows_no_approved_difference_left(
    settings, writing, drive, monkeypatch
) -> None:
    _library(drive, writing, [{"genre": "Neo Soul", "artist": "charlie puth"}])
    _approve(writing, kinds=["genre_remapped", "case_punctuation"])
    rec.library_reconcile(run_id="r-1")

    monkeypatch.setitem(FLAGS, rec.MODE_FLAG, "dry-run")
    _forget_seen(drive)  # compared again, as a full pass does
    again = rec.run_reconcile(run_id="r-2", mode=FlowMode.DRY_RUN, api=writing)

    assert again.compared == 1 and again.matching == 1


def test_an_other_difference_is_written_only_when_its_file_is_approved(
    settings, writing, drive
) -> None:
    ids = _library(
        drive, writing, [{"title": "Something Else"}, {"title": "Something Else"}]
    )
    _approve(writing, kinds=[], files={ids[1]: ["title"]})

    rec.library_reconcile(run_id="r-1")

    assert _frames(drive.items[ids[0]].data)["TIT2"] == "Something Else"
    assert _frames(drive.items[ids[1]].data)["TIT2"] == "Attention (Lash Remix)"


def test_an_approved_fill_up_goes_to_the_catalog_and_the_file_is_unchanged(
    settings, writing, drive
) -> None:
    [drive_id] = _library(drive, writing, [{}])
    entry = writing.rows[0].catalog_id
    writing.displays[entry] = _display(entry, label=None)
    before = drive.items[drive_id].data
    _approve(writing, kinds=["fill_up"])

    summary = rec.library_reconcile(run_id="r-1")

    [(catalog_id, fill)] = writing.fills
    assert catalog_id == str(entry) and fill.label == "Atlantic"
    assert summary.filled == ["0.mp3 (label)"]
    assert drive.items[drive_id].data == before


def test_a_kind_approved_for_the_file_goes_up_and_the_file_is_unchanged(
    settings, writing, drive, posted, monkeypatch
) -> None:
    [drive_id] = _library(drive, writing, [{"year": "2016", "bpm": "75.4"}])
    before = drive.items[drive_id].data
    _approve(writing, file_wins_kinds=["year_later", "bpm_differs"])

    summary = rec.library_reconcile(run_id="r-1")

    [kept] = writing.kept
    assert kept.drive_file_id == drive_id
    assert kept.keep_file == ["bpm", "year"]
    assert (kept.tags.year, kept.tags.bpm) == ("2016", "75.4")
    assert drive.items[drive_id].data == before  # the file is never written
    assert writing.registrations == []
    assert summary.kept == ["0.mp3 (bpm, year)"] and summary.written == []
    [review] = writing.reviews
    assert review.differences == []  # both settled
    assert "catalog took the file's value" in _text(posted)

    monkeypatch.setitem(FLAGS, rec.MODE_FLAG, "dry-run")
    _forget_seen(drive)
    again = rec.run_reconcile(run_id="r-2", mode=FlowMode.DRY_RUN, api=writing)
    assert again.compared == 1 and again.matching == 1


def test_an_other_difference_goes_up_only_for_the_file_approved_for_it(
    settings, writing, drive
) -> None:
    ids = _library(
        drive, writing, [{"title": "Something Else"}, {"title": "Something Else"}]
    )
    _approve(writing, file_wins_files={ids[1]: ["title"]})

    rec.library_reconcile(run_id="r-1")

    [kept] = writing.kept
    assert kept.drive_file_id == ids[1] and kept.keep_file == ["title"]
    assert _frames(drive.items[ids[1]].data)["TIT2"] == "Something Else"
    [review] = writing.reviews
    assert [(d.drive_file_id, d.field) for d in review.differences] == [
        (ids[0], "title")
    ]


def test_a_file_value_the_catalog_holds_stays_open_with_why(
    settings, writing, drive, posted
) -> None:
    [drive_id] = _library(drive, writing, [{"artist": "Charlie Puth & Kaskade"}])
    writing.holds["artist"] = "not split into parts cleanly"
    _approve(writing, file_wins_files={drive_id: ["artist"]})

    summary = rec.library_reconcile(run_id="r-1")

    assert summary.kept == []
    assert summary.kept_held == [
        "0.mp3: artist 'Charlie Puth & Kaskade' (held: not split into parts cleanly)"
    ]
    [review] = writing.reviews
    assert [d.field for d in review.differences] == ["artist"]
    assert "kept_held" in _text(posted)


def test_one_file_can_settle_some_fields_each_way(settings, writing, drive) -> None:
    [drive_id] = _library(drive, writing, [{"genre": "Neo Soul", "year": "2016"}])
    _approve(writing, kinds=["genre_remapped"], file_wins_kinds=["year_later"])

    summary = rec.library_reconcile(run_id="r-1")

    frames = _frames(drive.items[drive_id].data)
    assert frames["TCON"] == "Pop"  # the catalog's genre, written
    assert frames["TDRC"] == "2016"  # the file's year, kept and sent up
    assert [k.keep_file for k in writing.kept] == [["year"]]
    assert summary.written == ["0.mp3 (genre)"] and summary.kept == ["0.mp3 (year)"]
    assert writing.reviews[0].differences == []


def test_a_dry_run_sends_nothing_up_for_the_file(settings, wired, drive) -> None:
    writing = WritingCatalog()
    _library(drive, writing, [{"year": "2016"}])
    _approve(writing, file_wins_kinds=["year_later"])

    summary = rec.run_reconcile(run_id="r-1", mode=FlowMode.DRY_RUN, api=writing)

    assert writing.kept == [] and summary.kept == []


def test_without_approvals_a_write_run_writes_nothing(
    settings, writing, drive, posted
) -> None:
    [drive_id] = _library(drive, writing, [{"genre": "Neo Soul"}])
    before = drive.items[drive_id].data

    summary = rec.library_reconcile(run_id="r-1")

    assert summary.approvals_missing
    assert drive.items[drive_id].data == before
    assert writing.fills == [] and writing.registrations == []
    assert "no_approvals" in _text(posted)


def test_approvals_that_cannot_be_read_stop_the_run(
    settings, writing, drive, posted, monkeypatch
) -> None:
    [drive_id] = _library(drive, writing, [{"genre": "Neo Soul"}])
    before = drive.items[drive_id].data

    def down():
        raise RuntimeError("API down")

    monkeypatch.setattr(writing, "read_reconcile_approvals", down, raising=False)

    summary = rec.library_reconcile(run_id="r-1")

    assert summary.problems
    assert drive.items[drive_id].data == before
    assert writing.fills == [] and writing.registrations == []
    assert _state(drive, rec.SEEN_NAME) is None  # nothing marked compared


# ---------------------------------------------------------------------------
# The review: every run sends the files it compared and what is still open
# ---------------------------------------------------------------------------


def test_a_dry_run_sends_every_difference_to_the_review(
    settings, wired, drive, catalog
) -> None:
    ids = _library(drive, catalog, [{}, {"genre": "Neo Soul", "year": "2016"}])

    rec.library_reconcile(run_id="r-1")

    [review] = catalog.reviews
    assert review.run_id == "r-1"
    assert review.compared == ids
    assert {(d.drive_file_id, d.field, d.kind) for d in review.differences} == {
        (ids[1], "genre", "genre_remapped"),
        (ids[1], "year", "year_later"),
    }


def test_a_write_run_sends_only_what_it_left_open(settings, writing, drive) -> None:
    [drive_id] = _library(drive, writing, [{"genre": "Neo Soul", "year": "2016"}])
    _approve(writing, kinds=["genre_remapped"])

    rec.library_reconcile(run_id="r-1")

    [review] = writing.reviews
    assert review.compared == [drive_id]
    assert [(d.field, d.kind) for d in review.differences] == [("year", "year_later")]


def test_a_filled_up_difference_is_not_left_open(settings, writing, drive) -> None:
    [drive_id] = _library(drive, writing, [{}])
    entry = writing.rows[0].catalog_id
    writing.displays[entry] = _display(entry, label=None)
    _approve(writing, kinds=["fill_up"])

    rec.library_reconcile(run_id="r-1")

    [review] = writing.reviews
    assert review.compared == [drive_id] and review.differences == []


def test_a_review_that_cannot_be_sent_is_reported_and_the_run_carries_on(
    settings, wired, drive, catalog, posted
) -> None:
    _library(drive, catalog, [{"genre": "Neo Soul"}])
    catalog.review_fails = True

    summary = rec.library_reconcile(run_id="r-1")

    assert summary.compared == 1 and "API down" in summary.review_problem
    assert "review_not_sent" in _text(posted)


def test_audio_that_would_change_stops_the_write_and_keeps_the_cursor(
    settings, writing, drive, monkeypatch
) -> None:
    from deejay_cog import music_safety

    _library(drive, writing, [{"genre": "Neo Soul"}, {"genre": "Neo Soul"}])
    _approve(writing, kinds=["genre_remapped"])
    finding = MagicMock()
    monkeypatch.setattr(rec, "post_run_finding", finding)

    def refuse(original, candidate):
        raise music_safety.AudioChanged("payload differs")

    monkeypatch.setattr(music_safety, "check_audio_unchanged", refuse)

    summary = rec.library_reconcile(run_id="r-1")

    assert summary.aborted
    # Not marked compared: the next run takes it again first.
    assert _state(drive, rec.SEEN_NAME)["files"] == {}
    assert drive.writes(except_folder=STATE) == []
    assert finding.call_args.args[1] == "ERROR"


# ---------------------------------------------------------------------------
# Long-open differences
# ---------------------------------------------------------------------------


def _age_open(drive: FakeDrive, days: int) -> None:
    """Move every recorded difference's first sighting ``days`` back."""
    held = _state(drive, rec.OPEN_NAME)
    assert held is not None
    since = (dt.datetime.now(dt.UTC) - dt.timedelta(days=days)).isoformat()
    for value in held["open"].values():
        value["since"] = since
    store = StateFile(drive, STATE, rec.OPEN_NAME)
    store.load()  # the file it writes over
    store.save(held)


def _held_open(drive: FakeDrive, drive_id: str, name: str, kind: str) -> None:
    """An open difference an earlier run recorded on ``drive_id``."""
    StateFile(drive, STATE, rec.OPEN_NAME).save(
        {
            "open": {
                f"{drive_id}|{name}": {
                    "since": "2026-11-10T00:00:00+00:00",
                    "kind": kind,
                    "path": "x.mp3",
                    "file": "Neo Soul",
                    "catalog": "Pop",
                }
            }
        }
    )


def test_a_file_with_an_approved_difference_is_settled_first(
    settings, writing, drive, monkeypatch
) -> None:
    monkeypatch.setenv(rec.CHUNK_SIZE_VAR, "1")
    ids = _library(drive, writing, [{}, {}, {"genre": "Neo Soul"}])
    _held_open(drive, ids[2], "genre", "genre_remapped")
    _approve(writing, kinds=["genre_remapped"])

    summary = rec.library_reconcile(run_id="r-1")

    assert summary.approved_first == 1
    assert _frames(drive.items[ids[2]].data)["TCON"] == "Pop"
    # Then the changed files, a chunk's worth: the rest wait for later runs.
    assert summary.reviewed == [ids[2], ids[0]]
    assert summary.changed_left == 2
    assert f"{ids[2]}|genre" not in _state(drive, rec.OPEN_NAME)["open"]


def test_a_changed_file_already_settled_first_is_compared_once(
    settings, writing, drive, monkeypatch
) -> None:
    monkeypatch.setenv(rec.CHUNK_SIZE_VAR, "1")
    ids = _library(drive, writing, [{"genre": "Neo Soul"}, {}])
    _held_open(drive, ids[0], "genre", "genre_remapped")
    _approve(writing, kinds=["genre_remapped"])

    summary = rec.library_reconcile(run_id="r-1")

    assert summary.approved_first == 1 and summary.compared == 1
    assert summary.changed_left == 1


def test_an_approved_difference_that_stays_open_is_not_put_first_again(
    settings, writing, drive, monkeypatch
) -> None:
    monkeypatch.setenv(rec.CHUNK_SIZE_VAR, "1")
    ids = _library(drive, writing, [{}, {}, {"genre": "Neo Soul"}])
    _held_open(drive, ids[2], "genre", "genre_remapped")
    _approve(writing, kinds=["genre_remapped"])
    # The write can't land: the file keeps its value and the difference stays.
    monkeypatch.setattr(rec, "write_owned_fields", lambda *_: None)

    first = rec.library_reconcile(run_id="r-1")
    again = rec.library_reconcile(run_id="r-2")

    assert first.approved_first == 1
    assert _state(drive, rec.OPEN_NAME)["open"][f"{ids[2]}|genre"]["tried"]
    assert again.approved_first == 0


def test_a_refused_fill_up_is_not_counted_as_long_open(
    settings, wired, drive, catalog
) -> None:
    [drive_id] = _library(drive, catalog, [{}])
    StateFile(drive, STATE, rec.OPEN_NAME).save(
        {
            "open": {
                "other-file|genre": {
                    "since": "2026-01-01T00:00:00+00:00",
                    "kind": "fill_up",
                    "path": "x.mp3",
                    "file": "Girl Group",
                    "catalog": "",
                    "tried": "2026-01-02T00:00:00+00:00",
                }
            }
        }
    )

    summary = rec.library_reconcile(run_id="r-1")

    assert drive_id and summary.long_open == {} and not summary.long_open_warn


def test_an_unapproved_open_difference_waits_for_the_cursor(
    settings, writing, drive, monkeypatch
) -> None:
    monkeypatch.setenv(rec.CHUNK_SIZE_VAR, "1")
    ids = _library(drive, writing, [{}, {"genre": "Neo Soul"}])
    _held_open(drive, ids[1], "genre", "genre_remapped")
    _approve(writing, kinds=["case_punctuation"])

    summary = rec.library_reconcile(run_id="r-1")

    assert summary.approved_first == 0 and summary.reviewed == [ids[0]]


def test_each_open_difference_is_recorded_with_when_it_was_first_seen(
    settings, wired, drive, catalog
) -> None:
    [drive_id] = _library(drive, catalog, [{"genre": "Neo Soul"}])

    summary = rec.library_reconcile(run_id="r-1")

    held = _state(drive, rec.OPEN_NAME)
    assert list(held["open"]) == [f"{drive_id}|genre"]
    assert held["open"][f"{drive_id}|genre"]["kind"] == "genre_remapped"
    assert summary.long_open == {} and not summary.long_open_warn


def test_a_difference_open_over_a_week_warns_once(
    settings, wired, drive, catalog, posted
) -> None:
    _library(drive, catalog, [{"genre": "Neo Soul", "year": "2016"}])
    rec.library_reconcile(run_id="r-1")
    _age_open(drive, rec.LONG_OPEN_DAYS + 1)

    first = rec.library_reconcile(run_id="r-2")
    text = _text(posted)
    second = rec.library_reconcile(run_id="r-3")

    assert first.long_open == {"genre_remapped": 1, "year_later": 1}
    assert first.long_open_warn and "long_open" in text
    assert second.long_open == first.long_open
    assert not second.long_open_warn  # warned once, not every run


def test_a_difference_whose_values_change_starts_again(
    settings, wired, drive, catalog
) -> None:
    [drive_id] = _library(drive, catalog, [{"genre": "Neo Soul"}])
    rec.library_reconcile(run_id="r-1")
    _age_open(drive, rec.LONG_OPEN_DAYS + 1)
    entry = catalog.rows[0].catalog_id
    catalog.displays[entry] = _display(
        entry, genre="R&B/Soul", changed_at=_NOW + dt.timedelta(days=1)
    )

    summary = rec.library_reconcile(run_id="r-2")

    assert summary.long_open == {}


def test_a_settled_difference_leaves_the_record_and_the_warning_resets(
    settings, writing, drive, posted
) -> None:
    [drive_id] = _library(drive, writing, [{"genre": "Neo Soul"}])
    _approve(writing, kinds=[])
    rec.library_reconcile(run_id="r-1")
    _age_open(drive, rec.LONG_OPEN_DAYS + 1)
    assert rec.library_reconcile(run_id="r-2").long_open_warn

    _approve(writing, kinds=["genre_remapped"])
    settled = rec.library_reconcile(run_id="r-3")

    assert settled.long_open == {}
    assert _state(drive, rec.OPEN_NAME)["open"] == {}
    assert _state(drive, rec.OPEN_NAME)["warned"] is False


def test_a_tracker_that_fails_is_reported_and_the_run_carries_on(
    settings, wired, drive, catalog, posted, monkeypatch
) -> None:
    _library(drive, catalog, [{"genre": "Neo Soul"}])

    def broken(*_a, **_k):
        raise RuntimeError("state folder unreadable")

    monkeypatch.setattr(rec, "_open_key", broken)

    summary = rec.library_reconcile(run_id="r-1")

    assert summary.compared == 1 and "unreadable" in summary.open_problem
    assert "open_not_tracked" in _text(posted)
