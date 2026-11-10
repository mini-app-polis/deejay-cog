"""The library-scan step of the nightly library run: files added to and
removed from the library reach the registry, metadata only, and nothing
damaging happens when the listing looks wrong."""

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
    LibraryFile,
    LibraryFileRegister,
    LibraryFilesAbsentRequest,
    LibraryFilesAbsentResult,
)
from music_fakes import DriveItem, FakeDrive

from deejay_cog import library_backfill as backfill
from deejay_cog import library_scan as scan
from deejay_cog.music_safety import FlowMode
from flag_fakes import FLAGS

ROOT, STATE = "root", "state"
FOLDER = backfill.FOLDER_MIME_TYPE
_LINK = uuid.UUID("11111111-2222-3333-4444-555555555555")
_NOW = dt.datetime(2026, 11, 6, tzinfo=dt.UTC)


@dataclass
class FakeRegistry:
    """The registry by Drive id, as the API keeps it."""

    rows: dict[str, LibraryFile] = field(default_factory=dict)
    registrations: list[LibraryFileRegister] = field(default_factory=list)
    absent_calls: list[list[str]] = field(default_factory=list)

    def add(
        self, drive_id: str, path: str, *, present: bool = True, linked: bool = True
    ) -> None:
        self.rows[drive_id] = LibraryFile(
            id=uuid.uuid4(),
            drive_file_id=drive_id,
            library_root_id=ROOT,
            path=path,
            identification_status="identified" if linked else "unidentified",
            catalog_id=_LINK if linked else None,
            present=present,
            trash_requested=False,
            first_seen=_NOW,
            last_seen=_NOW,
        )

    def list_library_files(self, **kwargs) -> list[LibraryFile]:
        assert kwargs["library_root_id"] == ROOT
        rows = list(self.rows.values())
        start = kwargs["offset"]
        return rows[start : start + kwargs["limit"]]

    def register_library_file(self, payload: LibraryFileRegister) -> LibraryFile:
        self.registrations.append(payload)
        old = self.rows.get(payload.drive_file_id)
        self.rows[payload.drive_file_id] = LibraryFile(
            id=old.id if old else uuid.uuid4(),
            drive_file_id=payload.drive_file_id,
            library_root_id=payload.library_root_id,
            path=payload.path,
            identification_status=payload.identification_status,
            catalog_id=payload.catalog_id,
            present=True,
            trash_requested=False,
            first_seen=old.first_seen if old else _NOW,
            last_seen=_NOW,
        )
        return self.rows[payload.drive_file_id]

    def mark_library_files_absent(
        self, payload: LibraryFilesAbsentRequest
    ) -> LibraryFilesAbsentResult:
        self.absent_calls.append(list(payload.drive_file_ids))
        marked = 0
        for drive_id in payload.drive_file_ids:
            row = self.rows.get(drive_id)
            if row is not None and row.present:
                self.rows[drive_id] = row.model_copy(update={"present": False})
                marked += 1
        return LibraryFilesAbsentResult(marked=marked)


@pytest.fixture
def drive() -> FakeDrive:
    drive = FakeDrive()
    drive.items[STATE] = DriveItem(STATE, "state", ["elsewhere"], mime_type=FOLDER)
    return drive


@pytest.fixture
def registry() -> FakeRegistry:
    return FakeRegistry()


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(backfill.ROOT_FOLDER_VAR, ROOT)
    monkeypatch.setenv(backfill.STATE_FOLDER_VAR, STATE)
    monkeypatch.setitem(FLAGS, scan.MODE_FLAG, "write")
    monkeypatch.delenv(scan.LISTING_SECONDS_VAR, raising=False)
    monkeypatch.delenv(backfill.CHUNK_SIZE_VAR, raising=False)


@pytest.fixture
def posted(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    fake = MagicMock()
    monkeypatch.setattr(pipeline_status, "post_run_finding", fake)
    return fake


@pytest.fixture
def wired(
    monkeypatch: pytest.MonkeyPatch, drive: FakeDrive, registry: FakeRegistry
) -> None:
    google = MagicMock()
    google.from_env.return_value = SimpleNamespace(drive=drive)
    monkeypatch.setattr(scan, "GoogleAPI", google)
    monkeypatch.setattr(scan, "api_client", lambda **_: registry)
    monkeypatch.setattr(scan, "shared_deadline_passed", lambda: False)


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


def _backfill_done(drive: FakeDrive, file_ids: list[str], position: int) -> None:
    """A complete backfill manifest of ``file_ids``, its cursor at ``position``."""
    files = [
        {
            "id": i,
            "name": drive.items[i].name,
            "folder_id": ROOT,
            "mime_type": "audio/mpeg",
        }
        for i in file_ids
    ]
    manifest = {"generation": "g1", "complete": True, "files": files}
    cursor = {"generation": "g1", "position": position}
    drive.add(
        backfill.MANIFEST_NAME,
        STATE,
        json.dumps(manifest).encode(),
        mime_type="application/json",
    )
    drive.add(
        backfill.CURSOR_NAME,
        STATE,
        json.dumps(cursor).encode(),
        mime_type="application/json",
    )


def _listing(drive: FakeDrive) -> dict | None:
    found = [i for i in drive.in_folder(STATE) if i.name == scan.LISTING_NAME]
    return json.loads(found[0].data) if found else None


def _text(posted: MagicMock) -> str:
    return posted.call_args.kwargs["text"]


def _library_writes(drive: FakeDrive) -> list:
    return drive.writes(except_folder=STATE)


# ---------------------------------------------------------------------------
# Modes and settings
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", [None, "", "on"])
def test_off_reads_nothing_and_reports_nothing(
    settings, wired, drive, registry, posted, monkeypatch, value
) -> None:
    if value is None:
        monkeypatch.delitem(FLAGS, scan.MODE_FLAG, raising=False)
    else:
        monkeypatch.setitem(FLAGS, scan.MODE_FLAG, value)
    drive.add("a.mp3", ROOT)

    summary = scan.library_scan(run_id="s-1")

    assert summary.mode is FlowMode.OFF
    assert drive.calls == []
    assert registry.registrations == []
    posted.assert_not_called()


def test_a_bad_listing_budget_does_nothing(
    settings, wired, drive, posted, monkeypatch
) -> None:
    monkeypatch.setenv(scan.LISTING_SECONDS_VAR, "soon")
    drive.add("a.mp3", ROOT)

    summary = scan.library_scan(run_id="s-1")

    assert summary.problems
    assert _listing(drive) is None
    assert "LIBRARY_SCAN_LISTING_SECONDS" in _text(posted)


# ---------------------------------------------------------------------------
# What the comparison finds
# ---------------------------------------------------------------------------


def test_an_unchanged_library_changes_nothing(
    settings, wired, drive, registry, downloads, posted
) -> None:
    a = drive.add("a.mp3", ROOT)
    sub = drive.add("House", ROOT, mime_type=FOLDER)
    b = drive.add("b.mp3", sub)
    _backfill_done(drive, [a, b], position=2)
    registry.add(a, "a.mp3")
    registry.add(b, "House/b.mp3")

    summary = scan.library_scan(run_id="s-1")

    assert summary.compared
    assert summary.unchanged == 2
    assert registry.registrations == []
    assert registry.absent_calls == []
    assert downloads == []
    assert _library_writes(drive) == []


def test_a_new_file_is_registered_unidentified_with_its_path(
    settings, wired, drive, registry, downloads, posted
) -> None:
    a = drive.add("a.mp3", ROOT)
    _backfill_done(drive, [a], position=1)
    registry.add(a, "a.mp3")
    sub = drive.add("New", ROOT, mime_type=FOLDER)
    new = drive.add("fresh.mp3", sub)
    drive.add("cover.jpg", sub, mime_type="image/jpeg")

    summary = scan.library_scan(run_id="s-1")

    assert summary.added == ["New/fresh.mp3"]
    [payload] = registry.registrations
    assert payload.drive_file_id == new
    assert payload.path == "New/fresh.mp3"
    assert payload.identification_status == "unidentified"
    assert payload.catalog_id is None
    assert downloads == []
    assert "New/fresh.mp3" in _text(posted)


def test_a_file_gone_from_the_library_is_marked_not_present(
    settings, wired, drive, registry, posted
) -> None:
    a = drive.add("a.mp3", ROOT)
    b = drive.add("b.mp3", ROOT)
    c = drive.add("c.mp3", ROOT)
    _backfill_done(drive, [a, b, c], position=3)
    for drive_id, path in ((a, "a.mp3"), (b, "b.mp3"), (c, "c.mp3")):
        registry.add(drive_id, path)
    del drive.items[c]

    summary = scan.library_scan(run_id="s-1")

    assert summary.removed == ["c.mp3"]
    assert registry.absent_calls == [[c]]
    assert registry.rows[c].present is False
    assert registry.rows[c].catalog_id == _LINK  # the link stays
    assert "c.mp3" in _text(posted)


def test_a_file_in_the_trash_is_marked_not_present(
    settings, wired, drive, registry
) -> None:
    a = drive.add("a.mp3", ROOT)
    b = drive.add("b.mp3", ROOT)
    c = drive.add("c.mp3", ROOT)
    _backfill_done(drive, [a, b, c], position=3)
    for drive_id, path in ((a, "a.mp3"), (b, "b.mp3"), (c, "c.mp3")):
        registry.add(drive_id, path)
    drive.items[b].trashed = True

    scan.library_scan(run_id="s-1")

    assert registry.absent_calls == [[b]]
    assert registry.rows[b].present is False


def test_a_file_back_in_the_library_keeps_its_link(
    settings, wired, drive, registry
) -> None:
    a = drive.add("a.mp3", ROOT)
    b = drive.add("b.mp3", ROOT)
    _backfill_done(drive, [a, b], position=2)
    registry.add(a, "a.mp3")
    registry.add(b, "b.mp3", present=False)

    summary = scan.library_scan(run_id="s-1")

    assert summary.back == ["b.mp3"]
    [payload] = registry.registrations
    assert payload.catalog_id == _LINK
    assert payload.identification_status == "identified"
    assert registry.rows[b].present is True


def test_a_moved_file_keeps_its_link_and_gets_its_new_path(
    settings, wired, drive, registry
) -> None:
    sub = drive.add("Soul", ROOT, mime_type=FOLDER)
    a = drive.add("a.mp3", sub)
    _backfill_done(drive, [a], position=1)
    registry.add(a, "a.mp3")

    summary = scan.library_scan(run_id="s-1")

    assert summary.moved == ["a.mp3 -> Soul/a.mp3"]
    [payload] = registry.registrations
    assert (payload.path, payload.catalog_id) == ("Soul/a.mp3", _LINK)


def test_files_the_backfill_has_not_reached_are_left_to_it(
    settings, wired, drive, registry
) -> None:
    files = [drive.add(f"{n}.mp3", ROOT) for n in range(3)]
    _backfill_done(drive, files, position=1)
    registry.add(files[0], "0.mp3")

    summary = scan.library_scan(run_id="s-1")

    assert summary.left_for_backfill == 2
    assert registry.registrations == []


def test_no_file_is_added_while_the_backfill_is_still_listing(
    settings, wired, drive, registry, posted
) -> None:
    a = drive.add("a.mp3", ROOT)
    b = drive.add("b.mp3", ROOT)
    registry.add(a, "a.mp3")

    summary = scan.library_scan(run_id="s-1")

    assert summary.added == []
    assert summary.added_held
    assert registry.registrations == []
    assert b not in registry.rows


# ---------------------------------------------------------------------------
# Safety
# ---------------------------------------------------------------------------


def test_a_dry_run_reports_and_changes_nothing(
    settings, wired, drive, registry, posted, monkeypatch
) -> None:
    monkeypatch.setitem(FLAGS, scan.MODE_FLAG, "dry-run")
    a = drive.add("a.mp3", ROOT)
    b = drive.add("b.mp3", ROOT)
    gone = "gone-id"
    _backfill_done(drive, [a], position=1)
    registry.add(a, "a.mp3")
    registry.add(gone, "gone.mp3")
    registry.add("other", "other.mp3")
    registry.add("third", "third.mp3")
    drive.add("other.mp3", ROOT)  # different id: other's row is gone too
    del drive.items[b]
    drive.items["other"] = DriveItem("other", "other.mp3", [ROOT])
    drive.items["third"] = DriveItem("third", "third.mp3", [ROOT])

    summary = scan.library_scan(run_id="s-1")

    assert summary.compared
    assert summary.removed == ["gone.mp3"]
    assert registry.registrations == []
    assert registry.absent_calls == []
    text = _text(posted)
    assert "would_remove" in text or "gone.mp3" in text
    assert _library_writes(drive) == []


def test_an_empty_listing_marks_nothing_and_says_why(
    settings, wired, drive, registry, posted
) -> None:
    registry.add("x", "x.mp3")
    registry.add("y", "y.mp3")

    summary = scan.library_scan(run_id="s-1")

    assert summary.refused
    assert registry.absent_calls == []
    assert "no MP3s" in _text(posted)


def test_marking_more_than_half_the_library_absent_is_refused(
    settings, wired, drive, registry, posted
) -> None:
    a = drive.add("a.mp3", ROOT)
    _backfill_done(drive, [a], position=1)
    registry.add(a, "a.mp3")
    for n in range(3):
        registry.add(f"lost-{n}", f"lost-{n}.mp3")

    summary = scan.library_scan(run_id="s-1")

    assert summary.refused
    assert registry.absent_calls == []
    assert registry.registrations == []
    assert "3 of 4" in _text(posted)


# ---------------------------------------------------------------------------
# The listing's position
# ---------------------------------------------------------------------------


def test_a_listing_past_its_budget_carries_on_the_next_night(
    settings, wired, drive, registry, monkeypatch
) -> None:
    subs = [drive.add(f"Dir {n}", ROOT, mime_type=FOLDER) for n in range(3)]
    files = [drive.add(f"{n}.mp3", sub) for n, sub in enumerate(subs)]
    _backfill_done(drive, files, position=3)
    for n, drive_id in enumerate(files):
        registry.add(drive_id, f"Dir {n}/{n}.mp3")
    ticks = iter([0.0, 0.0, 0.0, 100.0])
    monkeypatch.setattr(scan, "monotonic", lambda: next(ticks, 1000.0))

    first = scan.run_scan(run_id="s-1", mode=FlowMode.WRITE)

    assert not first.listing_complete
    assert _listing(drive)["pending_folders"]
    assert registry.absent_calls == []  # never compared on half a listing

    monkeypatch.setattr(scan, "monotonic", lambda: 0.0)
    second = scan.run_scan(run_id="s-2", mode=FlowMode.WRITE)

    assert second.listing_complete
    assert second.unchanged == 3
    assert _listing(drive)["compared_at"]


def test_a_compared_listing_is_listed_afresh_the_next_night(
    settings, wired, drive, registry
) -> None:
    a = drive.add("a.mp3", ROOT)
    _backfill_done(drive, [a], position=1)
    registry.add(a, "a.mp3")
    scan.library_scan(run_id="s-1")
    generation = _listing(drive)["generation"]
    new = drive.add("b.mp3", ROOT)

    summary = scan.library_scan(run_id="s-2")

    assert _listing(drive)["generation"] != generation
    assert [p.drive_file_id for p in registry.registrations] == [new]
    assert summary.added == ["b.mp3"]
