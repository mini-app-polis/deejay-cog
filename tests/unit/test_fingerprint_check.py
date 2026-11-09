"""The fingerprint check: only fingerprint writes that retitled a file to
another song are listed, and --apply rolls them back and unlinks them."""

from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass, field

from mini_app_polis.api.contract import LibraryFile, LibraryFileRegister
from music_fakes import FakeDrive, mp3
from mutagen.id3 import TIT2, TPE1

from deejay_cog import fingerprint_check as check
from deejay_cog.music_safety import FlowMode, RunRecord, tag_snapshot, write_in_place
from deejay_cog.music_tags import tags_agree, write_owned_fields

LIB, STATE, ROOT = "library", "state", "root"
NOW = dt.datetime(2026, 11, 8, tzinfo=dt.UTC)


@dataclass
class FakeApi:
    rows: dict[str, LibraryFile] = field(default_factory=dict)
    registrations: list[LibraryFileRegister] = field(default_factory=list)

    def register_library_file(self, payload: LibraryFileRegister):
        self.registrations.append(payload)
        return payload

    def list_library_files(self, **params):
        rows = list(self.rows.values())
        offset, limit = params.get("offset", 0), params.get("limit", 100)
        return rows[offset : offset + limit]


def _linked(
    drive: FakeDrive, api: FakeApi, run_id: str, was: tuple, became: tuple, by: str
) -> str:
    file_id = drive.add(
        f"{was[0]}.mp3",
        LIB,
        mp3(TIT2(encoding=3, text=[was[0]]), TPE1(encoding=3, text=[was[1]])),
    )
    api.rows[file_id] = LibraryFile(
        id=uuid.uuid4(),
        drive_file_id=file_id,
        library_root_id=ROOT,
        path=f"{was[0]}.mp3",
        identification_status="identified",
        catalog_id=uuid.uuid4(),
        present=True,
        trash_requested=False,
        first_seen=NOW,
        last_seen=NOW,
    )
    record = RunRecord("library-identify", run_id, FlowMode.WRITE).attach(drive, STATE)
    data = drive.download_file_bytes(file_id).data
    tagged = write_owned_fields(
        data, {"title": became[0], "artist": became[1], "catalog_id": run_id}
    )
    assert tagged is not None
    write_in_place(
        drive,
        record,
        file_id=file_id,
        name=f"{was[0]}.mp3",
        original=data,
        tagged=tagged,
        identified_by=by,
    )
    record.finish("complete")
    return file_id


def _title(drive: FakeDrive, file_id: str) -> str:
    return tag_snapshot(drive.download_file_bytes(file_id).data)["title"]


def test_tags_agree_on_versions_credits_and_punctuation() -> None:
    assert tags_agree(
        "Temple (Radio Edit)", "Thomas Gold feat. X", "Temple", "Thomas Gold"
    )
    assert tags_agree("Don't Worry", "Madcon", "Don’t Worry", "Madcon feat. Ray Dalton")
    assert tags_agree("Rock & Roll", "A", "Rock and Roll", "A")
    assert not tags_agree("Beggin", "Madcon", "Don't Worry", "Madcon")
    assert not tags_agree("Fast Car", "Daniel Hanson", "Fast Car", "Tracy Chapman")


def test_tags_agree_only_on_the_same_version() -> None:
    # The original found for a remix, or a remix for the original, does not agree.
    assert not tags_agree(
        "go with u (Louis La Roche Remix)", "BAYNK", "go with u", "BAYNK"
    )
    assert not tags_agree(
        "Temple", "Thomas Gold", "Temple (Niko The Kid Remix)", "Thomas Gold"
    )
    assert tags_agree(
        "Temple - Niko The Kid Remix",
        "Thomas Gold",
        "Temple (Niko the Kid Remix)",
        "Thomas Gold",
    )
    # Credits, release labels and remasters are not versions.
    assert tags_agree("Temple (Radio Edit)", "Thomas Gold", "Temple", "Thomas Gold")
    assert tags_agree("Temple (feat. X)", "Thomas Gold", "Temple", "Thomas Gold")
    assert tags_agree("Drive (2017 Remaster)", "The Cars", "Drive", "The Cars")


def _library(drive: FakeDrive, api: FakeApi) -> dict[str, str]:
    return {
        "wrong": _linked(
            drive,
            api,
            "fp-1",
            ("Beggin", "Madcon"),
            ("Don't Worry", "Madcon"),
            "fingerprint (0.97) + catalog",
        ),
        "right": _linked(
            drive,
            api,
            "fp-2",
            ("Temple", "Thomas Gold"),
            ("Temple", "Thomas Gold"),
            "fingerprint (0.99) + catalog",
        ),
        "by_tags": _linked(
            drive, api, "id-1", ("Song", "A"), ("Other", "B"), "tags + catalog"
        ),
    }


def test_dry_run_lists_only_the_retitled_fingerprint_write() -> None:
    drive, api = FakeDrive(), FakeApi()
    files = _library(drive, api)

    found = check.check(drive, api, STATE, apply=False)

    assert [(m.file_id, m.run_id) for m in found] == [(files["wrong"], "fp-1")]
    assert found[0].was == "Madcon - Beggin"
    assert api.registrations == []
    assert _title(drive, files["wrong"]) == "Don't Worry"


def test_apply_rolls_it_back_and_unlinks_it() -> None:
    drive, api = FakeDrive(), FakeApi()
    files = _library(drive, api)

    [m] = check.check(drive, api, STATE, apply=True)

    assert m.result == "rolled back and unlinked"
    assert _title(drive, files["wrong"]) == "Beggin"
    [row] = api.registrations
    assert row.drive_file_id == files["wrong"]
    assert row.identification_status == "unidentified" and row.catalog_id is None
    assert "the file says Madcon - Beggin" in (row.identify_note or "")
    assert _title(drive, files["right"]) == "Temple"


def test_a_kept_run_is_listed_but_left_alone() -> None:
    drive, api = FakeDrive(), FakeApi()
    files = _library(drive, api)

    [m] = check.check(drive, api, STATE, apply=True, keep=frozenset({"fp-1"}))

    assert m.result == "kept: named in --keep"
    assert api.registrations == []
    assert _title(drive, files["wrong"]) == "Don't Worry"
