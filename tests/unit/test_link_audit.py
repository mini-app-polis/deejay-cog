"""The link audit: files whose names disagree with their entries are
listed, in two groups, and nothing is changed."""

from __future__ import annotations

import datetime as dt
import uuid
from unittest.mock import MagicMock

import pytest
from mini_app_polis.api.contract import CatalogDisplay, LibraryFile

from deejay_cog import link_audit as la

NOW = dt.datetime(2026, 11, 13, tzinfo=dt.UTC)


def _row(path: str, entry: uuid.UUID | None, status: str = "identified") -> LibraryFile:
    return LibraryFile(
        id=uuid.uuid4(),
        drive_file_id=f"d-{path}",
        library_root_id="root",
        path=path,
        identification_status=status,
        catalog_id=entry,
        present=True,
        trash_requested=False,
        first_seen=NOW,
        last_seen=NOW,
    )


def _display(entry: uuid.UUID, title: str, artist: str) -> CatalogDisplay:
    return CatalogDisplay(id=entry, title=title, artist=artist, changed_at=NOW)


@pytest.mark.parametrize(
    ("path", "parts"),
    [
        ("/New Music/Despacito_Luis Fonsi.mp3", ("Despacito", "Luis Fonsi")),
        ("Pop/Happy_PharrellWilliams_91.mp3", ("Happy", "PharrellWilliams")),
        ("Pop/2step_edsheeran.mp3", ("2step", "edsheeran")),
        ("Pop/Untitled.mp3", ("Untitled", "")),
    ],
)
def test_a_name_gives_its_title_and_artist_without_a_tempo(path, parts) -> None:
    assert la.name_parts(path) == parts


def _audit(cases: list[tuple[str, str, str]]) -> list[la.Suspect]:
    rows, displays = [], {}
    for path, title, artist in cases:
        entry = uuid.uuid4()
        rows.append(_row(path, entry))
        displays[entry] = _display(entry, title, artist)
    return la.audit(rows, displays)


def test_a_file_linked_to_another_song_is_likely_another_song() -> None:
    [s] = _audit(
        [("/New Music/Despacito_Luis Fonsi.mp3", "California Gurls", "Katy Perry")]
    )

    assert s.both
    assert s.line.startswith(
        "/New Music/Despacito_Luis Fonsi.mp3  ->  California Gurls — Katy Perry  ["
    )


def test_spelling_versions_and_old_names_agree() -> None:
    assert (
        _audit(
            [
                ("Pop/Happy_PharrellWilliams_91.mp3", "Happy", "Pharrell Williams"),
                (
                    "Pop/Cheerleader (Remix)_OMI.mp3",
                    "Cheerleader (Felix Jaehn remix)",
                    "OMI",
                ),
                ("Pop/2step_edsheeran.mp3", "2step (Long Edit)", "Ed Sheeran"),
                ("Pop/10 35_Tiësto.mp3", "10:35", "Tiësto"),
                ("Pop/Big (feat. Gunna)_Rita Ora.mp3", "Big", "Rita Ora, David Guetta"),
            ]
        )
        == []
    )


def test_another_title_by_the_same_artist_is_listed_apart() -> None:
    [s] = _audit(
        [("/New Music/Shake It_Metro Station.mp3", "Control", "Metro Station")]
    )

    assert not s.both


def test_a_name_with_no_artist_is_judged_by_its_title() -> None:
    [s] = _audit([("/New Music/Feelin_Unknown.mp3", "Flowers", "Miley Cyrus")])

    assert s.both


def test_unidentified_files_and_entries_the_catalog_lacks_are_left_out() -> None:
    entry = uuid.uuid4()
    rows = [
        _row("/New Music/A_B.mp3", None, status="unidentified"),
        _row("/New Music/C_D.mp3", entry),
    ]

    assert la.audit(rows, {}) == []


def test_the_report_groups_and_counts_and_changes_nothing(capsys) -> None:
    entry, other = uuid.uuid4(), uuid.uuid4()
    rows = [
        _row("/New Music/Despacito_Luis Fonsi.mp3", entry),
        _row("/New Music/Shake It_Metro Station.mp3", other),
    ]
    api = MagicMock()
    api.list_library_files.return_value = rows
    api.read_catalog_display.return_value = [
        _display(entry, "California Gurls", "Katy Perry"),
        _display(other, "Control", "Metro Station"),
    ]

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(la, "api_client", MagicMock(return_value=api))
        assert la.main([]) == 0

    out = capsys.readouterr().out
    assert "LIKELY ANOTHER SONG (1)" in out and "TITLE DIFFERS, SAME ARTIST (1)" in out
    assert "2 linked files compared; nothing was changed." in out
    api.register_library_file.assert_not_called()
