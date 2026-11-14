"""The library's steps together, against one catalog: what a file's tags go
through between the reconcile and the sync, where a tool outside the
pipeline (VirtualDJ) writes into a linked file.

The steps' own tests each fake the API the way that step calls it. Here one
catalog answers both, with the API's rules for an edit made in a file, so a
change one step makes is what the other one sees. On 2026-11-13 titles made
from file names ("1973 James Blunt" for 1973_James Blunt.mp3) reached the
catalog this way while every step's own tests passed.
"""

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
    CatalogFillUp,
    CatalogFillUpResult,
    LibraryFieldEdit,
    LibraryFile,
    LibraryFileEdited,
    LibraryFileEditResult,
    LibraryFileRegister,
    LibraryFilesSynced,
    LibraryFilesSyncedResult,
    ReconcileApprovals,
    ReconcileReport,
    ReconcileReportResult,
)
from music_fakes import DriveItem, FakeDrive, mp3
from mutagen.id3 import ID3, TBPM, TCON, TDRC, TIT2, TPE1, TXXX

from deejay_cog import library_backfill as backfill
from deejay_cog import library_reconcile as rec
from deejay_cog import library_scan as scan
from deejay_cog import library_sync as sync
from deejay_cog.music_safety import FlowMode
from flag_fakes import FLAGS

ROOT, STATE = "root", "state"
OWNED = ("title", "artist", "year", "genre", "bpm", "label")


def _norm(name: str, value: str | None) -> str:
    text = (value or "").strip()
    return text[:4] if name == "year" else text


@dataclass
class Catalog:
    """The API as the reconcile and the sync both call it."""

    entries: dict[uuid.UUID, CatalogDisplay] = field(default_factory=dict)
    rows: dict[str, LibraryFile] = field(default_factory=dict)
    approvals: ReconcileApprovals = field(default_factory=ReconcileApprovals)
    #: Each field a file's edit sent up to its entry: (drive id, field, value).
    sent_up: list[tuple[str, str, str]] = field(default_factory=list)

    # -- the registry ------------------------------------------------------

    def list_library_files(self, **kwargs: Any) -> list[LibraryFile]:
        rows = [
            r
            for r in self.rows.values()
            if r.present and r.library_root_id == kwargs["library_root_id"]
        ]
        if "after" in kwargs:
            ids = [r.id for r in rows]
            rows = rows[ids.index(kwargs["after"]) + 1 :]
            return rows[: kwargs["limit"]]
        start = kwargs.get("offset", 0)
        return rows[start : start + kwargs["limit"]]

    def list_library_files_needing_sync(self, **kwargs: Any) -> list[LibraryFile]:
        waiting = [
            r
            for r in self.rows.values()
            if r.library_root_id == kwargs["library_root_id"]
            and r.catalog_id is not None
            and (
                r.last_synced is None
                or r.synced is None
                or self.entries[r.catalog_id].changed_at > r.last_synced
            )
        ]
        return waiting[: kwargs["limit"]]

    def mark_library_files_synced(
        self, payload: LibraryFilesSynced
    ) -> LibraryFilesSyncedResult:
        for drive_id in payload.drive_file_ids:
            row = self.rows[drive_id]
            update: dict[str, Any] = {"last_synced": payload.synced_at}
            if drive_id in payload.values:
                update["synced"] = payload.values[drive_id]
            self.rows[drive_id] = row.model_copy(update=update)
        return LibraryFilesSyncedResult(marked=len(payload.drive_file_ids))

    def register_library_file(self, payload: LibraryFileRegister) -> LibraryFile:
        """A write by the reconcile: the file's recorded values are cleared,
        so the next sync records them afresh."""
        row = self.rows[payload.drive_file_id]
        self.rows[payload.drive_file_id] = row.model_copy(
            update={"last_synced": payload.synced_at, "synced": None}
        )
        return self.rows[payload.drive_file_id]

    # -- the catalog -------------------------------------------------------

    def read_catalog_display(
        self, payload: CatalogDisplayRequest
    ) -> list[CatalogDisplay | None]:
        return [self.entries.get(i) for i in payload.ids]

    def fill_up_catalog_entry(
        self, catalog_id: Any, payload: CatalogFillUp
    ) -> CatalogFillUpResult:
        del payload  # no fill-up is approved in these scenarios
        entry = self.entries[uuid.UUID(str(catalog_id))]
        return CatalogFillUpResult(entry=entry, filled=[])

    def _entry_value(self, entry: CatalogDisplay, name: str) -> str:
        value = {
            "title": entry.title,
            "artist": entry.artist,
            "year": entry.release_year,
            "genre": entry.genre,
            "bpm": entry.bpm,
            "label": entry.label,
        }[name]
        if name == "bpm" and value is not None:
            return f"{float(value):g}"
        return "" if value is None else str(value)

    def report_library_file_edited(
        self, payload: LibraryFileEdited
    ) -> LibraryFileEditResult:
        """The API's rule: a field the file changed (against what its last
        sync recorded) while its entry did not goes up; one the owner
        approved for the file (``keep_file``) goes up whatever was recorded."""
        row = self.rows[payload.drive_file_id]
        assert row.catalog_id is not None
        entry = self.entries[row.catalog_id]
        synced = row.synced.model_dump() if row.synced is not None else {}
        tags = payload.tags.model_dump()
        fields: list[LibraryFieldEdit] = []
        updates: dict[str, Any] = {}
        for name in OWNED:
            now = tags.get(name)
            if not now:
                continue
            kept = name in payload.keep_file
            changed = _norm(name, now) != _norm(name, synced.get(name))
            if not kept and (row.synced is None or not changed):
                continue
            entry_value = self._entry_value(entry, name)
            if not kept and _norm(name, entry_value) != _norm(name, synced.get(name)):
                fields.append(
                    LibraryFieldEdit(field=name, file=now, outcome="overwritten")
                )
                continue
            fields.append(LibraryFieldEdit(field=name, file=now, outcome="sent_up"))
            if payload.preview:
                continue
            self.sent_up.append((payload.drive_file_id, name, now))
            key = {"year": "release_year"}.get(name, name)
            updates[key] = (
                int(now[:4]) if name == "year" else float(now) if name == "bpm" else now
            )
        if updates:
            self.entries[row.catalog_id] = entry.model_copy(
                update={**updates, "changed_at": dt.datetime.now(dt.UTC)}
            )
        return LibraryFileEditResult(
            drive_file_id=row.drive_file_id, catalog_id=row.catalog_id, fields=fields
        )

    # -- the reconcile's review --------------------------------------------

    def read_reconcile_approvals(self) -> ReconcileApprovals:
        return self.approvals

    def report_reconcile_differences(
        self, payload: ReconcileReport
    ) -> ReconcileReportResult:
        self.reported = payload
        return ReconcileReportResult(
            open_differences=len(payload.differences),
            files_with_differences=len({d.drive_file_id for d in payload.differences}),
        )


def _song(entry: uuid.UUID, title: str, artist: str, genre: str = "Pop") -> bytes:
    return mp3(
        TIT2(encoding=3, text=[title]),
        TPE1(encoding=3, text=[artist]),
        TDRC(encoding=3, text=["2016"]),
        TCON(encoding=3, text=[genre]),
        TBPM(encoding=3, text=["112"]),
        TXXX(encoding=3, desc="CATALOG_ID", text=[str(entry)]),
    )


def _title(drive: FakeDrive, drive_id: str) -> str:
    return str(ID3(io.BytesIO(drive.items[drive_id].data))["TIT2"].text[0])


def _at(minutes: int) -> str:
    """A Drive time ``minutes`` from now, as the scan's listing holds it."""
    when = dt.datetime.now(dt.UTC) + dt.timedelta(minutes=minutes)
    return when.isoformat()


@pytest.fixture
def drive() -> FakeDrive:
    drive = FakeDrive()
    drive.items[STATE] = DriveItem(STATE, "state", ["elsewhere"], mime_type="folder")
    return drive


@pytest.fixture
def catalog() -> Catalog:
    return Catalog()


@pytest.fixture
def wired(monkeypatch: pytest.MonkeyPatch, drive: FakeDrive, catalog: Catalog) -> None:
    monkeypatch.setenv(backfill.ROOT_FOLDER_VAR, ROOT)
    monkeypatch.setenv(backfill.STATE_FOLDER_VAR, STATE)
    monkeypatch.setitem(FLAGS, sync.MODE_FLAG, "write")
    monkeypatch.setitem(FLAGS, rec.MODE_FLAG, "write")
    google = MagicMock()
    google.from_env.return_value = SimpleNamespace(drive=drive)
    for module in (sync, rec):
        monkeypatch.setattr(module, "GoogleAPI", google)
        monkeypatch.setattr(module, "api_client", lambda **_: catalog)
        monkeypatch.setattr(module, "shared_deadline_passed", lambda: False)
    monkeypatch.setattr(pipeline_status, "post_run_finding", MagicMock())


def _listed(drive: FakeDrive, changed: dict[str, str]) -> None:
    """The scan's listing, with when Drive last saw each file change."""
    content = json.dumps(
        {
            "files": [
                {
                    "id": drive_id,
                    "name": drive.items[drive_id].name,
                    "folder_id": ROOT,
                    "mime_type": "audio/mpeg",
                    "modified": when,
                }
                for drive_id, when in changed.items()
            ]
        }
    ).encode()
    for item in drive.in_folder(STATE):
        if item.name == scan.LISTING_NAME:
            item.data = content
            return
    drive.add(scan.LISTING_NAME, STATE, content, mime_type="application/json")


def _linked(drive: FakeDrive, catalog: Catalog) -> tuple[uuid.UUID, str]:
    """1973 by James Blunt: an entry, and a New Music file linked to it that
    says what the entry says, taken in an hour ago."""
    entry = uuid.uuid4()
    catalog.entries[entry] = CatalogDisplay(
        id=entry,
        title="1973",
        artist="James Blunt",
        release_year=2016,
        genre="Pop",
        bpm=112.0,
        label=None,
        changed_at=dt.datetime.now(dt.UTC) - dt.timedelta(hours=2),
    )
    drive_id = drive.add(
        "1973_James Blunt.mp3", ROOT, _song(entry, "1973", "James Blunt")
    )
    drive.items[drive_id].revision_times[0] = _at(-60)
    when = dt.datetime.now(dt.UTC) - dt.timedelta(hours=1)
    catalog.rows[drive_id] = LibraryFile(
        id=uuid.uuid4(),
        drive_file_id=drive_id,
        library_root_id=ROOT,
        path="New Music/1973_James Blunt.mp3",
        identification_status="identified",
        catalog_id=entry,
        present=True,
        trash_requested=False,
        first_seen=when,
        last_seen=when,
    )
    _listed(drive, {drive_id: _at(-60)})
    return entry, drive_id


def test_a_title_virtualdj_makes_from_the_file_name_never_reaches_the_catalog(
    wired, drive: FakeDrive, catalog: Catalog
) -> None:
    entry, drive_id = _linked(drive, catalog)

    # The first sync records the entry's values for the newly linked file.
    sync.library_sync(run_id="s-1")
    assert catalog.rows[drive_id].synced is not None

    # VirtualDJ, with its write-tags option on, writes a title made from the
    # file name into the file after that sync.
    drive.rewrite(drive_id, _song(entry, "1973 James Blunt", "James Blunt"), _at(5))
    _listed(drive, {drive_id: _at(5)})

    summary = sync.library_sync(run_id="s-2")

    assert catalog.entries[entry].title == "1973"  # nothing reached the catalog
    assert catalog.sent_up == []
    assert summary.held_for_review == [
        "New Music/1973_James Blunt.mp3: title '1973' -> '1973 James Blunt'"
    ]

    # The reconcile files the difference; the owner has the catalog win.
    catalog.approvals = ReconcileApprovals.model_validate(
        {
            "kinds": [],
            "files": {drive_id: ["title"]},
            "updated_at": dt.datetime.now(dt.UTC),
            "updated_by": "owner",
        }
    )
    rec.run_reconcile(run_id="r-1", mode=FlowMode.WRITE)
    assert _title(drive, drive_id) == "1973"

    # The next sync sees the reconcile's write and sends nothing up.
    _listed(drive, {drive_id: _at(10)})
    sync.library_sync(run_id="s-3")
    sync.library_sync(run_id="s-4")

    assert catalog.entries[entry].title == "1973"
    assert catalog.sent_up == []
    assert _title(drive, drive_id) == "1973"


def test_a_genre_edited_in_virtualdj_still_goes_up(
    wired, drive: FakeDrive, catalog: Catalog
) -> None:
    entry, drive_id = _linked(drive, catalog)
    sync.library_sync(run_id="s-1")

    drive.rewrite(
        drive_id, _song(entry, "1973", "James Blunt", genre="Singer-Songwriter"), _at(5)
    )
    _listed(drive, {drive_id: _at(5)})
    sync.library_sync(run_id="s-2")

    assert catalog.entries[entry].genre == "Singer-Songwriter"
    assert catalog.sent_up == [(drive_id, "genre", "Singer-Songwriter")]
    assert catalog.entries[entry].title == "1973"


def test_an_approved_file_title_reaches_the_catalog(
    wired, drive: FakeDrive, catalog: Catalog
) -> None:
    entry, drive_id = _linked(drive, catalog)
    sync.library_sync(run_id="s-1")
    drive.rewrite(drive_id, _song(entry, "1973 (Live)", "James Blunt"), _at(5))
    _listed(drive, {drive_id: _at(5)})
    sync.library_sync(run_id="s-2")
    assert catalog.entries[entry].title == "1973"  # held for the owner

    # The owner has the file win for its title.
    catalog.approvals = ReconcileApprovals.model_validate(
        {
            "kinds": [],
            "file_wins_files": {drive_id: ["title"]},
            "updated_at": dt.datetime.now(dt.UTC),
            "updated_by": "owner",
        }
    )
    rec.run_reconcile(run_id="r-1", mode=FlowMode.WRITE)

    assert catalog.entries[entry].title == "1973 (Live)"
    assert _title(drive, drive_id) == "1973 (Live)"
