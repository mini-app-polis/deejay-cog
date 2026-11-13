"""The library-trash step: only the files the registry marks trash-requested
go to Drive's trash, never deleted, and are marked not present; a dry run
changes nothing."""

from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest
from mini_app_polis.api.contract import (
    LibraryFile,
    LibraryFilesAbsentRequest,
    LibraryFilesAbsentResult,
)
from music_fakes import DriveItem, FakeDrive

from deejay_cog import library_backfill as backfill
from deejay_cog import library_trash as trash
from deejay_cog.music_safety import FlowMode

ROOT, STATE, FX = "root", "state", "fx"
_NOW = dt.datetime(2026, 11, 7, tzinfo=dt.UTC)


def _row(
    drive_id: str, *, requested: bool, present: bool = True, root: str = ROOT
) -> LibraryFile:
    return LibraryFile(
        id=uuid.uuid4(),
        drive_file_id=drive_id,
        library_root_id=root,
        path=f"House/{drive_id}.mp3",
        identification_status="identified",
        present=present,
        trash_requested=requested,
        first_seen=_NOW,
        last_seen=_NOW,
    )


@dataclass
class FakeCatalog:
    rows: list[LibraryFile] = field(default_factory=list)
    absent: list[str] = field(default_factory=list)
    asked: list[dict[str, Any]] = field(default_factory=list)

    def list_library_files(self, **kwargs: Any) -> list[LibraryFile]:
        self.asked.append(kwargs)
        rows = [
            r
            for r in self.rows
            if r.trash_requested == kwargs["trash_requested"]
            and r.present == kwargs["present"]
            and r.library_root_id == kwargs["library_root_id"]
        ]
        return rows[kwargs["offset"] : kwargs["offset"] + kwargs["limit"]]

    def mark_library_files_absent(self, payload: LibraryFilesAbsentRequest):
        self.absent.extend(payload.drive_file_ids)
        return LibraryFilesAbsentResult(marked=len(payload.drive_file_ids))


@pytest.fixture
def drive() -> FakeDrive:
    drive = FakeDrive()
    drive.items[STATE] = DriveItem(STATE, "state", ["elsewhere"], mime_type="folder")
    return drive


@pytest.fixture(autouse=True)
def settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(backfill.ROOT_FOLDER_VAR, ROOT)
    monkeypatch.setenv(backfill.STATE_FOLDER_VAR, STATE)
    monkeypatch.setattr(trash, "shared_deadline_passed", lambda: False)


def _library(drive: FakeDrive, catalog: FakeCatalog) -> tuple[str, str]:
    keep = drive.add("keep.mp3", ROOT, b"keep")
    copy = drive.add("copy.mp3", ROOT, b"copy")
    catalog.rows += [_row(keep, requested=False), _row(copy, requested=True)]
    return keep, copy


def test_off_reads_nothing(drive: FakeDrive) -> None:
    catalog = FakeCatalog()
    _library(drive, catalog)

    summary = trash.run_trash(run_id="t-0", mode=FlowMode.OFF, drive=drive, api=catalog)

    assert summary.trashed == [] and catalog.asked == [] and drive.calls == []


def test_a_trash_requested_file_goes_to_the_trash_and_nothing_else(
    drive: FakeDrive,
) -> None:
    catalog = FakeCatalog()
    keep, copy = _library(drive, catalog)

    summary = trash.run_trash(
        run_id="t-1", mode=FlowMode.WRITE, drive=drive, api=catalog
    )

    assert summary.trashed == [f"House/{copy}.mp3"]
    assert drive.items[copy].trashed is True
    assert drive.items[keep].trashed is False
    assert drive.writes() == [("trash_file", (copy,))]
    assert catalog.absent == [copy]
    assert summary.marked_absent == 1


def test_a_dry_run_changes_nothing(drive: FakeDrive) -> None:
    catalog = FakeCatalog()
    _, copy = _library(drive, catalog)

    summary = trash.run_trash(
        run_id="t-2", mode=FlowMode.DRY_RUN, drive=drive, api=catalog
    )

    assert summary.would_trash == [f"House/{copy}.mp3"]
    assert drive.writes() == [] and catalog.absent == []


def test_a_file_already_in_the_trash_is_only_marked_not_present(
    drive: FakeDrive,
) -> None:
    catalog = FakeCatalog()
    _, copy = _library(drive, catalog)
    drive.items[copy].trashed = True

    summary = trash.run_trash(
        run_id="t-3", mode=FlowMode.WRITE, drive=drive, api=catalog
    )

    assert summary.already_gone == [f"House/{copy}.mp3"]
    assert drive.writes() == []
    assert catalog.absent == [copy]


def test_a_file_gone_from_drive_is_only_marked_not_present(
    drive: FakeDrive, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog = FakeCatalog()
    catalog.rows.append(_row("gone", requested=True))

    class NotFound(Exception):
        resp = SimpleNamespace(status=404)

    def missing(_drive: Any, _file_id: str) -> dict:
        raise NotFound("File not found")

    monkeypatch.setattr(trash, "file_meta", missing)

    summary = trash.run_trash(
        run_id="t-4", mode=FlowMode.WRITE, drive=drive, api=catalog
    )

    assert summary.already_gone == ["House/gone.mp3"]
    assert catalog.absent == ["gone"]


def test_a_row_not_marked_is_never_trashed_whatever_the_api_sends(
    drive: FakeDrive,
) -> None:
    keep = drive.add("keep.mp3", ROOT, b"keep")

    class Careless(FakeCatalog):
        def list_library_files(self, **_: Any) -> list[LibraryFile]:
            return [_row(keep, requested=False)]

    catalog = Careless()

    summary = trash.run_trash(
        run_id="t-5", mode=FlowMode.WRITE, drive=drive, api=catalog
    )

    assert summary.requested == 0
    assert drive.writes() == [] and catalog.absent == []


def test_missing_settings_are_reported(
    drive: FakeDrive, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(backfill.ROOT_FOLDER_VAR)

    summary = trash.run_trash(
        run_id="t-6", mode=FlowMode.WRITE, drive=drive, api=FakeCatalog()
    )

    assert any(backfill.ROOT_FOLDER_VAR in p for p in summary.problems)


def test_each_root_is_trashed_on_its_own_and_named(
    drive: FakeDrive, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(backfill.FX_ROOT_FOLDER_VAR, FX)
    posted = []
    monkeypatch.setattr(
        "mini_app_polis.pipeline_status.post_run_finding",
        lambda *_, **kw: posted.append(kw.get("text", "")),
    )
    catalog = FakeCatalog()
    _, copy = _library(drive, catalog)
    fx_copy = drive.add("bow.mp3", FX, b"bow")
    catalog.rows.append(_row(fx_copy, requested=True, root=FX))

    summary = trash.run_trash(
        run_id="t-7", mode=FlowMode.WRITE, drive=drive, api=catalog
    )

    (_, main), (_, fx) = summary.roots
    assert main.trashed == [f"House/{copy}.mp3"]
    assert fx.trashed == [f"House/{fx_copy}.mp3"]
    assert catalog.absent == [copy, fx_copy]
    assert [a["library_root_id"] for a in catalog.asked] == [ROOT, FX]
    text = posted[-1]
    assert f"Main library: House/{copy}.mp3" in text
    assert f"FX Music: House/{fx_copy}.mp3" in text
    # Named as the owner calls the roots, not by their settings.
    assert backfill.ROOT_FOLDER_VAR not in text
    assert backfill.FX_ROOT_FOLDER_VAR not in text


def test_a_root_that_fails_does_not_stop_the_other(
    drive: FakeDrive, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(backfill.FX_ROOT_FOLDER_VAR, FX)
    fx_copy = drive.add("bow.mp3", FX, b"bow")

    class MainDown(FakeCatalog):
        def list_library_files(self, **kwargs: Any) -> list[LibraryFile]:
            if kwargs["library_root_id"] == ROOT:
                raise RuntimeError("API down")
            return super().list_library_files(**kwargs)

    catalog = MainDown(rows=[_row(fx_copy, requested=True, root=FX)])

    with pytest.raises(RuntimeError, match="API down"):
        trash.run_trash(run_id="t-8", mode=FlowMode.WRITE, drive=drive, api=catalog)

    assert drive.items[fx_copy].trashed is True
    assert catalog.absent == [fx_copy]
