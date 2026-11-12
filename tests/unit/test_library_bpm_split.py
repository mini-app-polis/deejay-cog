"""Two edits of one song cut at different tempos share one catalog entry:
library-reconcile holds their BPM differences and library-sync writes
neither BPM, so the entry never hands one file's tempo to the other, run
after run."""

from __future__ import annotations

import datetime as dt
import io
import uuid
from dataclasses import dataclass, field
from typing import Any

import pytest
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
    LibraryFileTags,
    ReconcileApprovals,
    ReconcileReport,
    ReconcileReportResult,
)
from music_fakes import DriveItem, FakeDrive, mp3
from mutagen.id3 import ID3, TBPM, TCON, TDRC, TIT2, TPE1, TXXX

from deejay_cog import library_backfill as backfill
from deejay_cog import library_reconcile as rec
from deejay_cog import library_sync as sync
from deejay_cog.library_backfill import StateFile
from deejay_cog.music_safety import FlowMode

ROOT, STATE = "root", "state"
_NOW = dt.datetime(2026, 11, 11, tzinfo=dt.UTC)
ENTRY = uuid.UUID("11111111-2222-3333-4444-555555555555")


def _song(bpm: str) -> bytes:
    return mp3(
        TIT2(encoding=3, text=["Burning Bridges"]),
        TPE1(encoding=3, text=["Ella Vos"]),
        TDRC(encoding=3, text=["2017"]),
        TCON(encoding=3, text=["Pop"]),
        TBPM(encoding=3, text=[bpm]),
        TXXX(encoding=3, desc="CATALOG_ID", text=[str(ENTRY)]),
    )


def _bpm(data: bytes) -> str:
    return str(ID3(io.BytesIO(data))["TBPM"].text[0])


@dataclass
class Catalog:
    """The registry and the catalog as both steps see them: a value sent up
    moves the entry, and every file on a moved entry needs a sync."""

    rows: list[LibraryFile] = field(default_factory=list)
    display: CatalogDisplay | None = None
    needing_sync: set[str] = field(default_factory=set)
    approvals: ReconcileApprovals = field(default_factory=ReconcileApprovals)
    sent_up: list[LibraryFileEdited] = field(default_factory=list)
    fills: list[CatalogFillUp] = field(default_factory=list)

    def list_library_files(self, **kw: Any) -> list[LibraryFile]:
        rows = [
            r
            for r in self.rows
            if r.library_root_id == kw["library_root_id"]
            and r.present
            and r.identification_status
            == kw.get("identification_status", r.identification_status)
        ]
        return rows[kw.get("offset", 0) :][: kw["limit"]]

    def read_catalog_display(self, payload: CatalogDisplayRequest):
        return [self.display if i == ENTRY else None for i in payload.ids]

    def read_reconcile_approvals(self) -> ReconcileApprovals:
        return self.approvals

    def report_reconcile_differences(self, payload: ReconcileReport):
        return ReconcileReportResult(
            open_differences=len(payload.differences),
            files_with_differences=len({d.drive_file_id for d in payload.differences}),
        )

    def report_library_file_edited(self, payload: LibraryFileEdited):
        assert self.display is not None
        fields = []
        if payload.keep_file and not payload.preview:
            self.sent_up.append(payload)
            tags = payload.tags.model_dump()
            updates: dict[str, Any] = {"changed_at": self.display.changed_at}
            for name in payload.keep_file:
                fields.append(
                    LibraryFieldEdit(field=name, file=tags[name], outcome="sent_up")
                )
                if name == "bpm":
                    updates["bpm"] = float(tags[name])
            updates["changed_at"] += dt.timedelta(minutes=1)
            self.display = self.display.model_copy(update=updates)
            self.needing_sync |= {r.drive_file_id for r in self.rows}
        return LibraryFileEditResult(
            drive_file_id=payload.drive_file_id, catalog_id=ENTRY, fields=fields
        )

    def fill_up_catalog_entry(self, catalog_id, payload: CatalogFillUp):
        del catalog_id
        self.fills.append(payload)
        assert self.display is not None
        return CatalogFillUpResult(entry=self.display, filled=[])

    def register_library_file(self, payload: LibraryFileRegister):
        return payload

    def list_library_files_needing_sync(self, **kw: Any) -> list[LibraryFile]:
        return [r for r in self.rows if r.drive_file_id in self.needing_sync][
            : kw["limit"]
        ]

    def mark_library_files_synced(self, payload: LibraryFilesSynced):
        for n, row in enumerate(self.rows):
            if row.drive_file_id in payload.drive_file_ids:
                self.rows[n] = row.model_copy(
                    update={
                        "synced": payload.values[row.drive_file_id],
                        "last_synced": payload.synced_at,
                    }
                )
                self.needing_sync.discard(row.drive_file_id)
        return LibraryFilesSyncedResult(marked=len(payload.drive_file_ids))


@pytest.fixture
def library(monkeypatch: pytest.MonkeyPatch) -> tuple[FakeDrive, Catalog]:
    monkeypatch.setenv(backfill.ROOT_FOLDER_VAR, ROOT)
    monkeypatch.setenv(backfill.STATE_FOLDER_VAR, STATE)
    for name in (rec.CHUNK_SIZE_VAR, sync.CHUNK_SIZE_VAR, backfill.CHUNK_SIZE_VAR):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(rec, "shared_deadline_passed", lambda: False)
    monkeypatch.setattr(sync, "shared_deadline_passed", lambda: False)
    drive = FakeDrive()
    drive.items[STATE] = DriveItem(STATE, "state", ["elsewhere"], mime_type="folder")
    api = Catalog(
        display=CatalogDisplay(
            id=ENTRY,
            title="Burning Bridges",
            artist="Ella Vos",
            release_year=2017,
            genre="Pop",
            bpm=109.0,
            label=None,
            changed_at=_NOW,
        ),
        # The owner let the file's BPM win, and the catalog's win too.
        approvals=ReconcileApprovals.model_validate(
            {
                "kinds": ["bpm_rounding"],
                "file_wins_kinds": ["bpm_differs"],
                "updated_at": _NOW,
                "updated_by": "owner",
            }
        ),
    )
    for name, bpm in (
        ("burningbridges_ellavos.mp3", "99"),
        ("New Music/Burning Bridges_Ella Vos.mp3", "109"),
    ):
        drive_id = drive.add(name.rsplit("/", 1)[-1], ROOT, _song(bpm))
        api.rows.append(
            LibraryFile(
                id=uuid.uuid4(),
                drive_file_id=drive_id,
                library_root_id=ROOT,
                path=name,
                identification_status="identified",
                catalog_id=ENTRY,
                present=True,
                trash_requested=False,
                first_seen=_NOW,
                last_seen=_NOW,
                last_synced=_NOW,
                # Last synced when the entry said 99: its BPM has moved since.
                synced=LibraryFileTags(
                    title="Burning Bridges",
                    artist="Ella Vos",
                    year="2017",
                    genre="Pop",
                    bpm="99",
                ),
            )
        )
        api.needing_sync.add(drive_id)
    return drive, api


def _compare_all_again(drive: FakeDrive) -> None:
    """Forget what the reconcile compared, so every run compares both files."""
    store = StateFile(drive, STATE, rec.SEEN_NAME)
    store.load()
    store.save({"files": {}})


def test_two_edits_at_different_tempos_keep_their_bpms_run_after_run(
    library,
) -> None:
    drive, api = library
    first, second = (row.drive_file_id for row in api.rows)
    writes = []
    for n in range(3):
        _compare_all_again(drive)
        r = rec.run_reconcile(
            run_id=f"r-{n}", mode=FlowMode.WRITE, drive=drive, api=api
        )
        s = sync.run_sync(run_id=f"y-{n}", mode=FlowMode.WRITE, drive=drive, api=api)
        writes.append(drive.writes(except_folder=STATE))
        assert r.kept == [] and s.written == []

    # Nothing written after the first round (and nothing in it either).
    assert writes == [[], [], []]
    assert api.sent_up == [] and api.fills == []
    assert api.display is not None and api.display.bpm == 109.0
    assert (_bpm(drive.items[first].data), _bpm(drive.items[second].data)) == (
        "99",
        "109",
    )
    # The reconcile holds the difference as bpm_differs and names why.
    assert [d.path for d in r.differences["bpm_differs"]] == [
        "burningbridges_ellavos.mp3"
    ]
    assert r.bpm_split_held and "'99'" in r.bpm_split_held[0]


def test_the_sync_writes_neither_bpm_of_a_split_entry(library) -> None:
    drive, api = library

    summary = sync.run_sync(run_id="y-1", mode=FlowMode.WRITE, drive=drive, api=api)

    assert summary.written == [] and drive.writes(except_folder=STATE) == []
    assert len(summary.bpm_split_kept) == 2
    # Recorded as synced: the same move is not tried again.
    assert api.needing_sync == set()


def test_a_bpm_the_entrys_files_agree_on_is_still_written(library) -> None:
    drive, api = library
    second = api.rows[1].drive_file_id
    drive.items[second].data = _song("99.5")
    first = api.rows[0].drive_file_id

    summary = sync.run_sync(run_id="y-1", mode=FlowMode.WRITE, drive=drive, api=api)

    assert summary.bpm_split_kept == []
    assert _bpm(drive.items[first].data) == "109"
