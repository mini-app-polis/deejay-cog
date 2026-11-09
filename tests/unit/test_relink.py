"""Relinking: only files whose title has a version after an unbracketed
feat. credit are looked up again, only the link changes, and only on
--apply."""

from __future__ import annotations

import datetime as dt
import json
import uuid
from dataclasses import dataclass, field

from mini_app_polis.api.contract import (
    CatalogLookupRequest,
    CatalogMatch,
    LibraryFile,
    LibraryFileRegister,
)
from music_fakes import FakeDrive, mp3
from mutagen.id3 import TIT2, TPE1, TXXX

from deejay_cog import relink
from deejay_cog.music_safety import tag_snapshot

LIB, STATE = "library", "state"
ROOT = "root-folder"
ORIGINAL = uuid.UUID("11111111-1111-1111-1111-111111111111")
COVER = uuid.UUID("22222222-2222-2222-2222-222222222222")
NOW = dt.datetime(2026, 11, 8, tzinfo=dt.UTC)


def _match(entry: uuid.UUID, title: str) -> CatalogMatch:
    return CatalogMatch(id=entry, title=title, artist="The Weeknd")


@dataclass
class FakeApi:
    #: Lookup answers by the looked-up title, lower-cased.
    answers: dict[str, CatalogMatch | None] = field(default_factory=dict)
    rows: dict[str, LibraryFile] = field(default_factory=dict)
    registrations: list[LibraryFileRegister] = field(default_factory=list)
    lookups: list[str] = field(default_factory=list)

    def lookup_catalog(self, payload: CatalogLookupRequest):
        self.lookups += [t.title for t in payload.tracks]
        return [self.answers.get(t.title.lower()) for t in payload.tracks]

    def register_library_file(self, payload: LibraryFileRegister):
        self.registrations.append(payload)
        return payload

    def list_library_files(self, **params):
        rows = list(self.rows.values())
        offset, limit = params.get("offset", 0), params.get("limit", 100)
        return rows[offset : offset + limit]


def _file(
    drive: FakeDrive, api: FakeApi, name: str, title: str, link: uuid.UUID
) -> str:
    file_id = drive.add(
        name,
        LIB,
        mp3(
            TIT2(encoding=3, text=[title]),
            TPE1(encoding=3, text=["The Weeknd"]),
            TXXX(encoding=3, desc="CATALOG_ID", text=[str(link)]),
        ),
    )
    api.rows[file_id] = LibraryFile(
        id=uuid.uuid4(),
        drive_file_id=file_id,
        library_root_id=ROOT,
        path=name,
        identification_status="identified",
        catalog_id=link,
        present=True,
        trash_requested=False,
        first_seen=NOW,
        last_seen=NOW,
    )
    return file_id


def _backfill_record(drive: FakeDrive, files: dict[str, tuple[str, str]]) -> None:
    entries = [
        {
            "file_id": file_id,
            "original_name": name,
            "action": "in_place",
            "state": "done",
            "old_tags": {"title": title},
        }
        for file_id, (name, title) in files.items()
    ]
    record = {"flow": "library-backfill", "run_id": "b-1", "entries": entries}
    drive.add("library-backfill-b-1.json", STATE, json.dumps(record).encode())
    # A dry run's record is not read.
    dry = {**record, "run_id": "b-0"}
    drive.add("library-backfill-b-0.dry-run.json", STATE, json.dumps(dry).encode())


def _library(drive: FakeDrive, api: FakeApi) -> dict[str, str]:
    titles = {
        "cover": ("StarboyCover.mp3", "Starboy ft. Daft Punk (Alex Aiono Cover)"),
        "gone": ("StarboyAcoustic.mp3", "Starboy ft. Daft Punk - Acoustic"),
        "same": ("StarboyRadio.mp3", "Starboy ft. Daft Punk (Radio Edit)"),
        "plain": ("Starboy.mp3", "Starboy ft. Daft Punk"),
    }
    files = {
        key: _file(drive, api, name, title, ORIGINAL)
        for key, (name, title) in titles.items()
    }
    _backfill_record(drive, {files[k]: titles[k] for k in titles})
    api.answers = {
        titles["cover"][1].lower(): _match(COVER, "Starboy (Alex Aiono Cover)"),
        titles["gone"][1].lower(): None,
        titles["same"][1].lower(): _match(ORIGINAL, "Starboy"),
        titles["plain"][1].lower(): _match(ORIGINAL, "Starboy"),
    }
    return files


def _link(drive: FakeDrive, file_id: str) -> str:
    return tag_snapshot(drive.download_file_bytes(file_id).data)["catalog_id"]


def test_only_a_version_after_an_unbracketed_credit_is_a_candidate() -> None:
    assert relink.CREDIT_THEN_PART.search("Starboy ft. Daft Punk (Alex Aiono Cover)")
    assert relink.CREDIT_THEN_PART.search("Song feat. X - Lash Remix")
    assert not relink.CREDIT_THEN_PART.search("Starboy ft. Daft Punk")
    assert not relink.CREDIT_THEN_PART.search("Stay (Feat. Alessia Cara) (Remix)")
    assert not relink.CREDIT_THEN_PART.search("Temple (Niko The Kid Remix)")


def test_dry_run_plans_and_changes_nothing() -> None:
    drive, api = FakeDrive(), FakeApi()
    files = _library(drive, api)
    before = len(drive.writes(except_folder=STATE))

    _, planned = relink.relink(drive, api, STATE, apply=False, run_id="d-1")

    by_id = {r.file_id: r for r in planned}
    assert set(by_id) == {files["cover"], files["gone"], files["same"]}
    assert by_id[files["cover"]].result == "would link to another entry"
    assert by_id[files["gone"]].result.startswith("would unlink")
    assert by_id[files["same"]].result.startswith("unchanged")
    assert len(drive.writes(except_folder=STATE)) == before
    assert api.registrations == []
    assert _link(drive, files["cover"]) == str(ORIGINAL)


def test_apply_relinks_the_cover_and_unlinks_the_lost_one() -> None:
    drive, api = FakeDrive(), FakeApi()
    files = _library(drive, api)

    record_name, planned = relink.relink(drive, api, STATE, apply=True, run_id="w-1")

    by_id = {r.file_id: r for r in planned}
    assert by_id[files["cover"]].result == "linked to another entry"
    assert _link(drive, files["cover"]) == str(COVER)
    # Only the link changed.
    tags = tag_snapshot(drive.download_file_bytes(files["cover"]).data)
    assert tags["title"] == "Starboy ft. Daft Punk (Alex Aiono Cover)"
    assert by_id[files["gone"]].result.startswith("unlinked in the registry")
    assert _link(drive, files["gone"]) == str(ORIGINAL)  # a tag is never emptied
    assert _link(drive, files["same"]) == str(ORIGINAL)

    registered = {p.drive_file_id: p for p in api.registrations}
    assert set(registered) == {files["cover"], files["gone"]}
    assert registered[files["cover"]].catalog_id == COVER
    assert registered[files["cover"]].identification_status == "identified"
    assert registered[files["gone"]].catalog_id is None
    assert registered[files["gone"]].identification_status == "unidentified"
    assert registered[files["gone"]].path == "StarboyAcoustic.mp3"
    assert record_name == "library-relink-w-1.json"
    assert any(i.name == record_name for i in drive.in_folder(STATE))


def test_a_file_gone_from_the_registry_is_left_alone() -> None:
    drive, api = FakeDrive(), FakeApi()
    files = _library(drive, api)
    del api.rows[files["cover"]]

    _, planned = relink.relink(drive, api, STATE, apply=True, run_id="w-2")

    (cover,) = [r for r in planned if r.file_id == files["cover"]]
    assert cover.result.startswith("left alone")
    assert _link(drive, files["cover"]) == str(ORIGINAL)


def test_path_takes_the_named_files_whatever_their_title() -> None:
    drive, api = FakeDrive(), FakeApi()
    files = _library(drive, api)
    # "Starboy.mp3" has a plain title, so no backfill record names it.
    api.answers["starboy ft. daft punk"] = None

    _, planned = relink.relink(
        drive, api, STATE, apply=True, run_id="p-1", path="starboy.mp3"
    )

    (only,) = planned
    assert only.file_id == files["plain"]
    assert only.title == "Starboy ft. Daft Punk"
    assert only.result.startswith("unlinked in the registry")
    (row,) = api.registrations
    assert row.drive_file_id == files["plain"] and row.catalog_id is None
