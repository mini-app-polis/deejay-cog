"""Clearing one library file's catalog link: dry by default, found by its
id, path or a name only it has."""

from __future__ import annotations

import datetime as dt
import uuid
from unittest.mock import MagicMock

import pytest
from mini_app_polis.api.contract import LibraryFile, LibraryFileRegister

from deejay_cog import unlink as ul

NOW = dt.datetime(2026, 11, 13, tzinfo=dt.UTC)
WRONG = uuid.uuid4()


def _row(file_id: str, path: str, catalog_id: uuid.UUID | None = WRONG) -> LibraryFile:
    return LibraryFile(
        id=uuid.uuid4(),
        drive_file_id=file_id,
        library_root_id="root",
        path=path,
        identification_status="identified" if catalog_id else "unidentified",
        catalog_id=catalog_id,
        present=True,
        trash_requested=False,
        first_seen=NOW,
        last_seen=NOW,
    )


class FakeApi:
    def __init__(self, rows: list[LibraryFile]) -> None:
        self.rows = rows
        self.registered: list[LibraryFileRegister] = []

    def list_library_files(
        self,
        *,
        present: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[LibraryFile]:
        assert present is True
        start = offset or 0
        return self.rows[start : start + (limit or len(self.rows))]

    def register_library_file(self, payload: LibraryFileRegister) -> LibraryFile:
        self.registered.append(payload)
        return self.rows[0]


def _api() -> FakeApi:
    return FakeApi(
        [
            _row("f1", "New Music/American Girl_Tom Petty.mp3"),
            _row("f2", "New Music/Same.mp3"),
            _row("f3", "Old/Same.mp3"),
            _row("f4", "New Music/Unlinked.mp3", catalog_id=None),
        ]
    )


def test_a_dry_run_changes_nothing() -> None:
    api = _api()

    said = ul.unlink(api, "American Girl_Tom Petty.mp3", apply=False)

    assert "would unlink" in said and str(WRONG) in said
    assert api.registered == []


@pytest.mark.parametrize(
    "name",
    ["f1", "New Music/American Girl_Tom Petty.mp3", "American Girl_Tom Petty.mp3"],
)
def test_the_link_is_cleared_with_a_note(name: str) -> None:
    api = _api()

    said = ul.unlink(api, name, apply=True, note="wrong song")

    assert said.endswith("unlinked")
    [sent] = api.registered
    assert sent.drive_file_id == "f1"
    assert sent.path == "New Music/American Girl_Tom Petty.mp3"
    assert sent.identification_status == "unidentified"
    assert sent.catalog_id is None
    assert sent.identify_note == "wrong song"


def test_a_name_two_files_have_is_refused() -> None:
    api = _api()

    with pytest.raises(ul.UnlinkError, match="more than one"):
        ul.unlink(api, "Same.mp3", apply=True)

    assert api.registered == []


def test_a_path_picks_one_of_two_files_with_a_name() -> None:
    api = _api()

    ul.unlink(api, "Old/Same.mp3", apply=True)

    assert [p.drive_file_id for p in api.registered] == ["f3"]


def test_an_unknown_file_is_refused() -> None:
    with pytest.raises(ul.UnlinkError, match="no present"):
        ul.unlink(_api(), "Nope.mp3", apply=True)


def test_a_file_with_no_link_is_left_alone() -> None:
    api = _api()

    said = ul.unlink(api, "Unlinked.mp3", apply=True)

    assert said.endswith("no link to clear")
    assert api.registered == []


def test_the_command_is_dry_unless_told_to_apply(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    api = _api()
    monkeypatch.setattr(ul, "api_client", MagicMock(return_value=api))

    assert ul.main(["American Girl_Tom Petty.mp3"]) == 0
    assert "Nothing was changed" in capsys.readouterr().out
    assert api.registered == []

    assert ul.main(["American Girl_Tom Petty.mp3", "--apply"]) == 0
    assert len(api.registered) == 1
    assert ul.main(["Same.mp3"]) == 2
