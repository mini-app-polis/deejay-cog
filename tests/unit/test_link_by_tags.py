"""Linking by their own tags the files the fingerprint found no match for:
only those files, a dry run that changes nothing, and links marked as made
by the file's own tags."""

from __future__ import annotations

import datetime as dt
import io
import uuid
from dataclasses import dataclass, field

from mini_app_polis.api.contract import (
    CatalogDisplay,
    CatalogDisplayRequest,
    CatalogFillUp,
    CatalogFillUpResult,
    CatalogLookupRequest,
    CatalogMatch,
    LibraryEntryCreate,
    LibraryEntryCreated,
    LibraryFile,
    LibraryFileRegister,
    LibraryFileTags,
)
from music_fakes import FakeDrive, mp3
from mutagen.id3 import ID3, TIT2, TPE1

from deejay_cog import link_by_tags

LIB, STATE, ROOT = "library", "state", "root-folder"
NOW = dt.datetime(2026, 11, 9, tzinfo=dt.UTC)
KNOWN = uuid.UUID("11111111-1111-1111-1111-111111111111")
MADE = uuid.UUID("99999999-9999-9999-9999-999999999999")


def _display(entry: uuid.UUID, title: str, artist: str) -> CatalogDisplay:
    return CatalogDisplay(
        id=entry,
        title=title,
        artist=artist,
        release_year=None,
        genre=None,
        bpm=None,
        label=None,
        changed_at=NOW,
    )


@dataclass
class FakeApi:
    rows: list[LibraryFile] = field(default_factory=list)
    known: dict[str, CatalogMatch] = field(default_factory=dict)
    displays: dict[uuid.UUID, CatalogDisplay] = field(default_factory=dict)
    registrations: list[LibraryFileRegister] = field(default_factory=list)
    created: list[LibraryEntryCreate] = field(default_factory=list)

    def list_library_files(self, **params):
        assert params["identification_status"] == "unidentified"
        offset, limit = params["offset"], params["limit"]
        return self.rows[offset : offset + limit]

    def lookup_catalog(self, payload: CatalogLookupRequest):
        return [self.known.get(t.title.lower()) for t in payload.tracks]

    def read_catalog_display(self, payload: CatalogDisplayRequest):
        return [self.displays.get(i) for i in payload.ids]

    def fill_up_catalog_entry(self, catalog_id, payload: CatalogFillUp):
        del payload
        entry = self.displays[uuid.UUID(str(catalog_id))]
        return CatalogFillUpResult(entry=entry, filled=[])

    def register_library_file(self, payload: LibraryFileRegister):
        self.registrations.append(payload)
        return payload

    def create_library_entry(self, payload: LibraryEntryCreate):
        self.created.append(payload)
        self.displays[MADE] = _display(MADE, payload.title, payload.artist)
        return LibraryEntryCreated(
            entry=CatalogMatch(id=MADE, title=payload.title, artist=payload.artist),
            created=True,
        )


def _file(drive: FakeDrive, api: FakeApi, title: str, artist: str, note: str) -> str:
    file_id = drive.add(
        f"{title}.mp3",
        LIB,
        mp3(TIT2(encoding=3, text=[title]), TPE1(encoding=3, text=[artist])),
    )
    api.rows.append(
        LibraryFile(
            id=uuid.uuid4(),
            drive_file_id=file_id,
            library_root_id=ROOT,
            path=f"Pop/{title}.mp3",
            identification_status="unidentified",
            present=True,
            trash_requested=False,
            first_seen=NOW,
            last_seen=NOW,
            tags=LibraryFileTags(title=title, artist=artist),
            identify_note=note,
        )
    )
    return file_id


def _library() -> tuple[FakeDrive, FakeApi]:
    drive, api = FakeDrive(), FakeApi()
    api.known["temple"] = CatalogMatch(id=KNOWN, title="Temple", artist="Thomas Gold")
    api.displays[KNOWN] = _display(KNOWN, "Temple", "Thomas Gold")
    _file(drive, api, "Beast Mode", "Rokka", "fingerprint: no match")
    _file(
        drive,
        api,
        "Temple",
        "Thomas Gold",
        "fingerprint: no confident match (score 0.40)",
    )
    _file(drive, api, "Untitled", "Rokka", "fingerprint: no match")
    _file(drive, api, "Beggin'", "Madcon", "fingerprint said Madcon - Don't Worry")
    return drive, api


def test_only_files_the_fingerprint_found_no_match_for_are_taken() -> None:
    _, api = _library()

    found = link_by_tags.candidates(api)

    assert [c.row.path for c in found] == [
        "Pop/Beast Mode.mp3",
        "Pop/Temple.mp3",
        "Pop/Untitled.mp3",
    ]


def test_a_dry_run_plans_from_the_registry_and_changes_nothing() -> None:
    drive, api = _library()
    found = link_by_tags.candidates(api)

    link_by_tags.plan(api, found)

    assert [c.result for c in found] == [
        "would make a library entry",
        "would link to the catalog's entry",
        "left: its tags name no song",
    ]
    assert api.created == [] and api.registrations == []
    assert drive.writes() == []


def test_apply_links_each_file_and_marks_it_as_linked_by_its_tags() -> None:
    drive, api = _library()
    found = link_by_tags.candidates(api)

    link_by_tags.apply(drive, api, STATE, found)

    assert [c.result.split(";")[0] for c in found] == [
        "linked to a new library entry",
        "linked to the catalog's entry",
        "left: its tags name no song",
    ]
    [made] = api.created
    assert (made.title, made.artist) == ("Beast Mode", "Rokka")
    linked = {r.drive_file_id: r for r in api.registrations}
    beast, temple = found[0].row.drive_file_id, found[1].row.drive_file_id
    assert linked[beast].catalog_id == MADE
    assert linked[beast].identify_note == (
        "identified by its own tags (fingerprint: no match)"
    )
    assert linked[temple].catalog_id == KNOWN
    tags = ID3(io.BytesIO(drive.items[beast].data))
    assert str(tags["TXXX:CATALOG_ID"].text[0]) == str(MADE)
