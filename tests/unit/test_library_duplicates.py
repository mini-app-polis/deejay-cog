"""The library-duplicates report: files that look like copies of one song
are grouped by md5, catalog entry, title and artist, or name in a folder;
different edits and release labels are kept on purpose; the groups are the
same every run; nothing is written but the API's groups in write mode."""

from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock

import pytest
from mini_app_polis.api.contract import (
    CatalogDisplay,
    CatalogDisplayRequest,
    DuplicateGroupReport,
    DuplicateReport,
    DuplicateReportResult,
    LibraryFile,
)
from music_fakes import DriveItem, FakeDrive

from deejay_cog import library_backfill as backfill
from deejay_cog import library_duplicates as dup
from deejay_cog.music_safety import FlowMode

ROOT, STATE = "root", "state"
_NOW = dt.datetime(2026, 11, 7, tzinfo=dt.UTC)


def _file(
    drive_id: str,
    path: str,
    *,
    catalog_id: uuid.UUID | None = None,
    md5: str | None = None,
    secs: float | None = 200.0,
    label: str | None = None,
    requested: bool = False,
) -> LibraryFile:
    return LibraryFile(
        id=uuid.uuid4(),
        drive_file_id=drive_id,
        library_root_id=ROOT,
        path=path,
        md5=md5,
        duration_secs=secs,
        size_bytes=1000,
        catalog_id=catalog_id,
        edit_label=label,
        identification_status="identified" if catalog_id else "unidentified",
        present=True,
        trash_requested=requested,
        first_seen=_NOW,
        last_seen=_NOW,
    )


def _display(
    entry: uuid.UUID, title: str, artist: str, *, owner_edit: bool = False
) -> CatalogDisplay:
    return CatalogDisplay(
        id=entry,
        title=title,
        artist=artist,
        release_year=None,
        genre=None,
        bpm=None,
        label=None,
        owner_edit=owner_edit,
        changed_at=_NOW,
    )


def _members(group: DuplicateGroupReport) -> dict[str, str | None]:
    """Each file: None for a duplicate, why for one kept on purpose."""
    return {m.drive_file_id: m.why_kept for m in group.files}


# ── Grouping ──────────────────────────────────────────────────────────────


def test_exact_copies_are_grouped_by_md5() -> None:
    files = [
        _file("a", "House/x.mp3", md5="m1"),
        _file("b", "Pop/y.mp3", md5="m1"),
        _file("c", "Pop/z.mp3", md5="m2"),
    ]

    [group] = dup.find_groups(files, {})

    assert group.reasons == ["same_md5"]
    assert _members(group) == {"a": None, "b": None}
    assert group.key.startswith("same_md5:")


def test_files_linked_to_one_entry_are_grouped() -> None:
    entry = uuid.uuid4()
    files = [
        _file("a", "Pop/9to5_kellyclarkson.mp3", catalog_id=entry),
        _file("b", "Pop/9to5_kellyclarkson (1).mp3", catalog_id=entry),
        _file("c", "Pop/9to5_kellyclarkson (2).mp3", catalog_id=entry),
    ]

    [group] = dup.find_groups(files, {})

    assert group.reasons == ["same_catalog_id", "same_name"]
    assert set(_members(group)) == {"a", "b", "c"}


def test_entries_shown_with_the_same_title_and_artist_are_grouped() -> None:
    one, two = uuid.uuid4(), uuid.uuid4()
    files = [
        _file("a", "House/temple.mp3", catalog_id=one),
        _file("b", "Dance/temple_gold.mp3", catalog_id=two),
    ]
    shown = {
        one: _display(one, "Temple", "Thomas Gold"),
        two: _display(two, "temple ", "Thomas  Gold!"),
    }

    [group] = dup.find_groups(files, shown)

    assert group.reasons == ["same_title_artist"]


def test_the_same_name_twice_in_one_folder_is_grouped() -> None:
    files = [
        _file("a", "Pop/2step_edsheeran.mp3"),
        _file("b", "Pop/2step_edsheeran.mp3"),
        _file("c", "Other/2step_edsheeran.mp3"),
    ]

    [group] = dup.find_groups(files, {})

    assert group.reasons == ["same_name"]
    assert set(_members(group)) == {"a", "b"}


def test_files_with_nothing_in_common_are_not_grouped() -> None:
    files = [
        _file("a", "Pop/one.mp3", catalog_id=uuid.uuid4(), md5="1"),
        _file("b", "Pop/two.mp3", catalog_id=uuid.uuid4(), md5="2"),
    ]

    assert dup.find_groups(files, {}) == []


# ── Kept on purpose ───────────────────────────────────────────────────────


def test_a_no_intro_edit_is_sent_kept_on_purpose_not_left_out() -> None:
    entry = uuid.uuid4()
    files = [
        _file("full", "Blues/baby_ettajames.mp3", catalog_id=entry, secs=150.0),
        _file(
            "cut", "Blues/baby_nointroedit_ettajames.mp3", catalog_id=entry, secs=132.0
        ),
    ]

    [group] = dup.find_groups(files, {})

    assert _members(group) == {
        "cut": None,
        "full": "a different edit (2:30 vs 2:12)",
    }


def test_another_edit_is_shown_kept_in_the_group_of_real_copies() -> None:
    entry = uuid.uuid4()
    files = [
        _file("full", "Blues/baby.mp3", catalog_id=entry, secs=150.0),
        _file("full2", "Blues/baby (1).mp3", catalog_id=entry, secs=151.5),
        _file("cut", "Blues/baby_nointro.mp3", catalog_id=entry, secs=132.0),
    ]

    [group] = dup.find_groups(files, {})

    members = _members(group)
    assert members["full"] is None and members["full2"] is None
    assert members["cut"] == "a different edit (2:12 vs 2:31)"
    kept = [m for m in group.files if m.kept_on_purpose]
    assert [m.drive_file_id for m in kept] == ["cut"]


def test_clean_and_explicit_copies_are_kept_on_purpose() -> None:
    entry = uuid.uuid4()
    files = [
        _file("x1", "Pop/song.mp3", catalog_id=entry, label="Explicit"),
        _file("x2", "Pop/song (1).mp3", catalog_id=entry, label="Explicit"),
        _file("c", "Pop/song_clean.mp3", catalog_id=entry, label="Clean"),
    ]

    [group] = dup.find_groups(files, {})

    members = _members(group)
    assert members["x1"] is None and members["x2"] is None
    assert members["c"] == "a different release label (Clean)"


def test_one_of_each_label_is_one_group_with_the_others_kept_on_purpose() -> None:
    entry = uuid.uuid4()
    files = [
        _file("x", "Pop/song.mp3", catalog_id=entry, label="Explicit"),
        _file("c", "Pop/song_clean.mp3", catalog_id=entry, label="Clean"),
        _file("r", "Pop/song_radio.mp3", catalog_id=entry, label="Radio Edit"),
    ]

    [group] = dup.find_groups(files, {})

    assert _members(group) == {
        "c": None,
        "r": "a different release label (Radio Edit)",
        "x": "a different release label (Explicit)",
    }
    assert group.reasons == ["same_catalog_id"]


def test_an_entry_with_a_clean_and_an_explicit_file_is_sent() -> None:
    # Gravity [Clean] by Kill J: two files on one entry, one of each label.
    entry = uuid.uuid4()
    files = [
        _file("clean", "Pop/gravity_clean.mp3", catalog_id=entry, label="Clean"),
        _file("explicit", "New Music/gravity.mp3", catalog_id=entry, secs=203.0),
    ]

    [group] = dup.find_groups(files, {})

    assert _members(group) == {
        "explicit": None,
        "clean": "a different release label (Clean)",
    }


def test_every_entry_with_two_or_more_files_is_sent() -> None:
    entries = [uuid.uuid4() for _ in range(4)]
    files = [
        # Lengths more than three seconds apart.
        _file("a1", "Pop/2step.mp3", catalog_id=entries[0], secs=150.0),
        _file("a2", "Pop/2step_radio.mp3", catalog_id=entries[0], secs=170.0),
        # Another release label.
        _file("b1", "Pop/worse.mp3", catalog_id=entries[1], label="Explicit"),
        _file("b2", "Pop/worse_clean.mp3", catalog_id=entries[1], label="Clean"),
        # Real copies beside another edit.
        _file("c1", "Pop/jelly.mp3", catalog_id=entries[2], md5="j"),
        _file("c2", "Pop/jelly (1).mp3", catalog_id=entries[2], md5="j"),
        _file("c3", "Pop/jelly_long.mp3", catalog_id=entries[2], secs=260.0),
        # One file: not a group.
        _file("d1", "Pop/alone.mp3", catalog_id=entries[3]),
    ]

    groups = dup.find_groups(files, {})

    assert sorted(sorted(_members(g)) for g in groups) == [
        ["a1", "a2"],
        ["b1", "b2"],
        ["c1", "c2", "c3"],
    ]
    for group in groups:
        offered = [m for m in group.files if not m.kept_on_purpose]
        assert offered, "every group offers a file"
        assert all(m.why_kept for m in group.files if m.kept_on_purpose)


def test_a_song_of_different_edits_gives_the_same_group_every_run() -> None:
    entry = uuid.uuid4()
    files = [
        _file("x", "Pop/song.mp3", catalog_id=entry, label="Explicit"),
        _file("c", "Pop/song_clean.mp3", catalog_id=entry, label="Clean"),
    ]

    [first] = dup.find_groups(files, {})
    [second] = dup.find_groups(list(reversed(files)), {})
    # A file joining the song keeps the key and adds a file, so the API
    # reopens a group the owner kept together only when its files change.
    [grown] = dup.find_groups(
        [*files, _file("r", "Pop/song_radio.mp3", catalog_id=entry, label="Radio")],
        {},
    )

    assert first == second
    assert grown.key == first.key
    assert set(_members(grown)) == {"x", "c", "r"}


def test_a_file_of_unknown_length_joins_the_largest_edit() -> None:
    entry = uuid.uuid4()
    files = [
        _file("a", "Pop/s.mp3", catalog_id=entry, secs=200.0),
        _file("b", "Pop/s (1).mp3", catalog_id=entry, secs=None),
    ]

    [group] = dup.find_groups(files, {})

    assert _members(group) == {"a": None, "b": None}


# ── Owner edits ───────────────────────────────────────────────────────────

_SONG, _CUT, _CUT2 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
_SHOWN = {
    _SONG: _display(_SONG, "Baby What You Want Me To Do", "Etta James"),
    _CUT: _display(
        _CUT, "Baby What You Want Me To Do <Intro Cut>", "Etta James", owner_edit=True
    ),
    _CUT2: _display(
        _CUT2,
        "Baby What You Want Me To Do <Self Censored>",
        "Etta James",
        owner_edit=True,
    ),
}


def test_an_owner_edit_is_never_a_duplicate_of_the_published_song_even_by_md5() -> None:
    files = [
        _file("song", "Blues/baby.mp3", catalog_id=_SONG, md5="m"),
        _file("cut", "Blues/baby (1).mp3", catalog_id=_CUT, md5="m"),
    ]

    [group] = dup.find_groups(files, _SHOWN)

    assert _members(group) == {
        "song": None,
        "cut": "an owner edit (its own entry, not the published song)",
    }


def test_an_owner_edit_beside_the_published_songs_copies_is_kept_as_an_owner_edit() -> (
    None
):
    files = [
        _file("song", "Blues/baby.mp3", catalog_id=_SONG, md5="m"),
        _file("song2", "Blues/baby (1).mp3", catalog_id=_SONG, md5="m"),
        _file("cut", "Blues/baby (2).mp3", catalog_id=_CUT, md5="m"),
    ]

    [group] = dup.find_groups(files, _SHOWN)

    members = _members(group)
    assert members["song"] is None and members["song2"] is None
    why = members["cut"]
    assert why is not None and "owner edit" in why
    assert "release label" not in why and "different edit" not in why
    [kept] = [m for m in group.files if m.kept_on_purpose]
    assert kept.drive_file_id == "cut"


def test_two_files_on_one_owner_edit_entry_are_copies_of_it() -> None:
    files = [
        _file("cut", "Blues/baby_cut.mp3", catalog_id=_CUT, md5="c"),
        _file("cut2", "Fx/baby_cut.mp3", catalog_id=_CUT, md5="c"),
        _file("song", "Blues/baby.mp3", catalog_id=_SONG, md5="c"),
    ]

    [group] = dup.find_groups(files, _SHOWN)

    members = _members(group)
    assert members["cut"] is None and members["cut2"] is None
    # The published song's file is shown kept, never offered for the trash.
    assert members["song"] == "the published song (this group is of an owner edit)"
    assert [m.drive_file_id for m in group.files if not m.kept_on_purpose] == [
        "cut",
        "cut2",
    ]


def test_two_owner_edits_of_one_song_are_not_copies_of_each_other() -> None:
    files = [
        _file("cut", "Blues/baby_cut.mp3", catalog_id=_CUT, md5="c"),
        _file("censored", "Blues/baby_censored.mp3", catalog_id=_CUT2, md5="c"),
    ]

    [group] = dup.find_groups(files, _SHOWN)

    offered = [m.drive_file_id for m in group.files if not m.kept_on_purpose]
    assert len(offered) == 1
    [kept] = [m for m in group.files if m.kept_on_purpose]
    assert "owner edit" in (kept.why_kept or "")


def test_a_file_kept_as_an_owner_edit_is_never_sent_for_the_trash() -> None:
    files = [
        _file("song", "Blues/baby.mp3", catalog_id=_SONG, md5="m"),
        _file("song2", "Blues/baby (1).mp3", catalog_id=_SONG, md5="m"),
        _file("cut", "Blues/baby (2).mp3", catalog_id=_CUT, md5="m"),
        _file("cut2", "Blues/baby (3).mp3", catalog_id=_CUT, md5="m"),
    ]

    groups = dup.find_groups(files, _SHOWN)

    assert len(groups) == 2
    for group in groups:
        offered = {m.drive_file_id for m in group.files if not m.kept_on_purpose}
        assert offered in ({"song", "song2"}, {"cut", "cut2"})
        for m in group.files:
            if m.drive_file_id not in offered:
                assert m.kept_on_purpose and m.why_kept


def test_unflagged_entries_group_as_before() -> None:
    files = [
        _file("song", "Blues/baby.mp3", catalog_id=_SONG, md5="m"),
        _file("other", "Blues/baby (1).mp3", catalog_id=uuid.uuid4(), md5="m"),
    ]
    shown = {_SONG: _SHOWN[_SONG]}

    [group] = dup.find_groups(files, shown)

    assert _members(group) == {"song": None, "other": None}


def test_the_run_reads_owner_edits_from_the_catalog_and_keeps_them_apart() -> None:
    catalog = FakeCatalog(
        rows=[
            _file("song", "Blues/baby.mp3", catalog_id=_SONG, md5="m"),
            _file("song2", "Blues/baby (1).mp3", catalog_id=_SONG, md5="m"),
            _file("cut", "Blues/baby (2).mp3", catalog_id=_CUT, md5="m"),
        ],
        displays=dict(_SHOWN),
    )

    dup.run_duplicates(run_id="d-1", mode=FlowMode.WRITE, api=catalog)

    [report] = catalog.reports
    [group] = report.groups
    assert {m.drive_file_id for m in group.files if not m.kept_on_purpose} == {
        "song",
        "song2",
    }
    assert "owner edit" in (_members(group)["cut"] or "")


# ── Stability ─────────────────────────────────────────────────────────────


def test_the_same_library_gives_the_same_groups() -> None:
    entry = uuid.uuid4()
    files = [
        _file("a", "Pop/s.mp3", catalog_id=entry),
        _file("b", "Pop/s (1).mp3", catalog_id=entry),
        _file("c", "House/x.mp3", md5="m"),
        _file("d", "House/y.mp3", md5="m"),
    ]

    first = dup.find_groups(files, {})
    second = dup.find_groups(list(reversed(files)), {})

    assert first == second
    assert len({g.key for g in first}) == 2


# ── The run ───────────────────────────────────────────────────────────────


@dataclass
class FakeCatalog:
    rows: list[LibraryFile] = field(default_factory=list)
    displays: dict[uuid.UUID, CatalogDisplay] = field(default_factory=dict)
    reports: list[DuplicateReport] = field(default_factory=list)

    def list_library_files(self, **kwargs: Any) -> list[LibraryFile]:
        assert kwargs["library_root_id"] == ROOT and kwargs["present"] is True
        return self.rows[kwargs["offset"] : kwargs["offset"] + kwargs["limit"]]

    def read_catalog_display(self, payload: CatalogDisplayRequest):
        return [self.displays.get(i) for i in payload.ids]

    def report_duplicate_groups(self, payload: DuplicateReport):
        self.reports.append(payload)
        return DuplicateReportResult(
            open_groups=len(payload.groups), added=len(payload.groups), dropped=0
        )


@pytest.fixture(autouse=True)
def settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(backfill.ROOT_FOLDER_VAR, ROOT)
    monkeypatch.setenv(backfill.STATE_FOLDER_VAR, STATE)


def _catalog() -> FakeCatalog:
    entry = uuid.uuid4()
    return FakeCatalog(
        rows=[
            _file("a", "Pop/s.mp3", catalog_id=entry),
            _file("b", "Pop/s (1).mp3", catalog_id=entry),
            _file("t", "Pop/s (2).mp3", catalog_id=entry, requested=True),
        ]
    )


def test_write_sends_the_groups_to_the_api() -> None:
    catalog = _catalog()

    summary = dup.run_duplicates(run_id="d-1", mode=FlowMode.WRITE, api=catalog)

    [report] = catalog.reports
    assert report.library_root_id == ROOT
    [group] = report.groups
    assert set(_members(group)) == {"a", "b"}
    assert summary.result is not None and summary.result.open_groups == 1


def test_write_sends_an_entry_of_different_edits_marked_kept_on_purpose() -> None:
    entry = uuid.uuid4()
    catalog = FakeCatalog(
        rows=[
            _file("x", "Pop/song.mp3", catalog_id=entry, label="Explicit"),
            _file("c", "Pop/song_clean.mp3", catalog_id=entry, label="Clean"),
        ]
    )

    dup.run_duplicates(run_id="d-1", mode=FlowMode.WRITE, api=catalog)

    [report] = catalog.reports
    [group] = report.groups
    assert [(m.drive_file_id, m.kept_on_purpose) for m in group.files] == [
        ("c", False),
        ("x", True),
    ]


def test_a_file_already_marked_for_the_trash_is_left_out() -> None:
    catalog = _catalog()

    summary = dup.run_duplicates(run_id="d-2", mode=FlowMode.WRITE, api=catalog)

    assert "t" not in _members(summary.groups[0])


def test_a_dry_run_sends_nothing() -> None:
    catalog = _catalog()

    summary = dup.run_duplicates(run_id="d-3", mode=FlowMode.DRY_RUN, api=catalog)

    assert len(summary.groups) == 1 and catalog.reports == []


def test_off_reads_nothing() -> None:
    catalog = _catalog()

    class Untouchable(FakeCatalog):
        def list_library_files(self, **_: Any) -> list[LibraryFile]:
            raise AssertionError("off must not read")

    summary = dup.run_duplicates(run_id="d-4", mode=FlowMode.OFF, api=Untouchable())

    assert summary.groups == [] and catalog.reports == []


def test_a_name_key_drops_the_extension_and_copy_suffix() -> None:
    assert dup.name_key("Pop/9to5 (1).mp3") == dup.name_key("pop/9to5.MP3")
    assert dup.name_key("Pop/9to5.mp3") != dup.name_key("Rock/9to5.mp3")


@pytest.mark.parametrize(
    "copy",
    [
        "_MCIntro/01 - Peru [Explicit] copy.mp3",
        "_MCIntro/01 - Peru [Explicit] - Copy.mp3",
        "_MCIntro/01 - Peru [Explicit] copy 2.mp3",
    ],
)
def test_a_name_key_drops_a_file_manager_copy_suffix(copy: str) -> None:
    assert dup.name_key(copy) == dup.name_key("_MCIntro/01 - Peru [Explicit].mp3")


def test_a_name_key_keeps_copy_inside_a_title() -> None:
    assert dup.name_key("Pop/copycat.mp3") != dup.name_key("Pop/cat.mp3")
    assert dup.name_key("Pop/Copy That.mp3") != dup.name_key("Pop/That.mp3")


def _grouped(notices: list[Any]) -> list[tuple[str, str]]:
    """The run report's lines naming a group, by section, from the report
    mock's ``notice`` calls."""
    titles = (dup.COPIES_SECTION.casefold(), dup.VERSIONS_SECTION.casefold())
    return [
        (c.args[0], c.args[1]) for c in notices if c.args[0].casefold().endswith(titles)
    ]


def test_open_groups_unchanged_since_last_run_are_not_named_again() -> None:
    catalog = _catalog()
    summary = dup.run_duplicates(run_id="d-4", mode=FlowMode.WRITE, api=catalog)
    summary.result = DuplicateReportResult(open_groups=1, added=0, dropped=0)
    summary.roots[0].result = summary.result
    report = MagicMock()

    dup._record(report, summary, 1.0)

    assert _grouped(report.notice.call_args_list) == []


def test_a_new_group_is_named() -> None:
    catalog = _catalog()
    summary = dup.run_duplicates(run_id="d-5", mode=FlowMode.WRITE, api=catalog)
    report = MagicMock()

    dup._record(report, summary, 1.0)

    assert _grouped(report.notice.call_args_list)


# ── The run report ────────────────────────────────────────────────────────


def _record_dry(rows: list[LibraryFile], **kw: Any) -> MagicMock:
    catalog = FakeCatalog(rows=rows, **kw)
    summary = dup.run_duplicates(run_id="r-1", mode=FlowMode.DRY_RUN, api=catalog)
    report = MagicMock()
    dup._record(report, summary, 1.0)
    return report


def test_copies_and_versions_are_reported_in_their_own_sections() -> None:
    copy, versions = uuid.uuid4(), uuid.uuid4()
    report = _record_dry(
        [
            # Cold Call: a copy in New Music of a main-library file.
            _file("a", "coldcall_boulevards.mp3", catalog_id=copy),
            _file("b", "/New Music/Cold Call_Boulevards.mp3", catalog_id=copy),
            # Lay Low: one file of each release label, nothing to resolve.
            _file("c", "LayLow_ORKID_86.mp3", catalog_id=versions, secs=190),
            _file(
                "d",
                "LayLow_Explicit_ORKID_86.mp3",
                catalog_id=versions,
                label="Explicit",
                secs=190,
            ),
        ]
    )

    assert _grouped(report.notice.call_args_list) == [
        (
            dup.COPIES_SECTION,
            "same catalog entry: /coldcall_boulevards.mp3"
            " · /New Music/Cold Call_Boulevards.mp3",
        ),
        (
            dup.VERSIONS_SECTION,
            "/LayLow_Explicit_ORKID_86.mp3 (Explicit, 3:10)"
            " · /LayLow_ORKID_86.mp3 (3:10)",
        ),
    ]
    counts = {c.args[0]: c.args[1] for c in report.count.call_args_list}
    assert counts["copies"] == 1 and counts["versions"] == 1


def test_a_copy_line_names_the_versions_kept_beside_it() -> None:
    entry = uuid.uuid4()
    report = _record_dry(
        [
            _file("a", "Pop/song.mp3", catalog_id=entry, secs=200),
            _file("b", "Pop/song (1).mp3", catalog_id=entry, secs=201),
            _file("c", "Pop/song_clean.mp3", catalog_id=entry, label="Clean"),
        ]
    )

    [(_, line)] = _grouped(report.notice.call_args_list)
    assert line == (
        "same catalog entry, same file name: /Pop/song.mp3 · /Pop/song (1).mp3"
        " — also kept: /Pop/song_clean.mp3 (Clean, 3:20)"
    )


def test_two_copies_at_one_path_are_told_apart() -> None:
    report = _record_dry(
        [
            _file("aaaaaaaaaa1", "boyfriend_justinbieber.mp3", secs=170),
            _file("bbbbbbbbbb2", "boyfriend_justinbieber.mp3", secs=171),
            _file("ccccccccc3", "2step_edsheeran.mp3", md5="m"),
            _file("ddddddddd4", "2step_edsheeran.mp3", md5="m"),
        ]
    )

    lines = [line for _, line in _grouped(report.notice.call_args_list)]
    assert (
        "identical file, same file name: /2step_edsheeran.mp3 (id cccccccc)"
        " · /2step_edsheeran.mp3 (id dddddddd)"
    ) in lines
    assert (
        "same file name: /boyfriend_justinbieber.mp3 (2:50)"
        " · /boyfriend_justinbieber.mp3 (2:51)"
    ) in lines


def test_the_headline_counts_files_checked_and_counters_are_in_words() -> None:
    report = _record_dry(_catalog().rows)

    report.ok.assert_called_once_with(2)  # the file marked for the trash is out
    labels = {c.args[0]: c.kwargs.get("label") for c in report.count.call_args_list}
    assert labels["groups"] == dup.GROUPS_LABEL
    assert all(label for label in labels.values())


def test_the_api_counts_are_in_words(roots) -> None:
    catalog = RootsCatalog(rows=[_fx("c", "Bows/y.mp3", md5="m")])
    summary = dup.run_duplicates(run_id="r-1", mode=FlowMode.WRITE, api=catalog)
    summary.result = DuplicateReportResult(open_groups=2, added=1, dropped=1)
    report = MagicMock()

    dup._record(report, summary, 1.0)

    labels = {c.args[0]: c.kwargs.get("label") for c in report.count.call_args_list}
    assert labels["open_groups"] == dup.OPEN_LABEL
    assert labels["dropped"] == dup.DROPPED_LABEL
    assert labels["groups_main"] == "groups in Main library"


# ── Per library root ──────────────────────────────────────────────────────

FX, NEW = "fx-root", "new-music"
#: How a report names FX Music: by the setting naming its folder.
FX_LABEL = backfill.FX_ROOT_FOLDER_VAR


def _fx(drive_id: str, path: str, **kw: Any) -> LibraryFile:
    return _file(drive_id, path, **kw).model_copy(update={"library_root_id": FX})


@dataclass
class RootsCatalog(FakeCatalog):
    """A registry holding files under more than one root."""

    broken: set[str] = field(default_factory=set)

    def list_library_files(self, **kwargs: Any) -> list[LibraryFile]:
        root = kwargs["library_root_id"]
        if root in self.broken:
            raise RuntimeError("registry down")
        rows = [r for r in self.rows if r.library_root_id == root]
        return rows[kwargs["offset"] : kwargs["offset"] + kwargs["limit"]]


@pytest.fixture
def roots(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(backfill.NEW_MUSIC_FOLDER_VAR, NEW)
    monkeypatch.setenv(backfill.FX_ROOT_FOLDER_VAR, FX)


def test_an_fx_file_and_a_main_library_file_are_never_grouped(roots) -> None:
    entry = uuid.uuid4()
    catalog = RootsCatalog(
        rows=[
            _file("main", "Pop/walk.mp3", md5="m1", catalog_id=entry),
            _fx("fx", "Walk-ons/walk.mp3", md5="m1", catalog_id=entry),
        ]
    )

    summary = dup.run_duplicates(run_id="d-1", mode=FlowMode.WRITE, api=catalog)

    assert summary.groups == []
    # Each root's groups replace that root's own open groups.
    assert [(r.library_root_id, r.groups) for r in catalog.reports] == [
        (ROOT, []),
        (FX, []),
    ]


def test_the_main_library_groups_across_its_folder_and_new_music(roots) -> None:
    entry = uuid.uuid4()
    catalog = RootsCatalog(
        rows=[
            _file("a", "Pop/s.mp3", catalog_id=entry),
            _file("b", "/New Music/s.mp3", catalog_id=entry),
            _file("c", "/New Music/t.mp3", md5="m2"),
            _file("d", "/New Music/t (1).mp3", md5="m2"),
        ]
    )

    summary = dup.run_duplicates(run_id="d-1", mode=FlowMode.WRITE, api=catalog)

    assert sorted(set(_members(g)) for g in summary.groups) == [
        {"a", "b"},
        {"c", "d"},
    ]
    [main, fx] = catalog.reports
    assert main.library_root_id == ROOT and len(main.groups) == 2
    assert fx.library_root_id == FX and fx.groups == []


def test_fx_copies_in_one_folder_are_a_group_unless_they_are_different_cuts(
    roots,
) -> None:
    entry, other = uuid.uuid4(), uuid.uuid4()
    catalog = RootsCatalog(
        rows=[
            _fx("a", "Bows/bow.mp3", md5="m1", catalog_id=entry),
            _fx("b", "Bows/bow (1).mp3", md5="m1", catalog_id=entry),
            # A shorter cut of the same song, in the same folder.
            _fx("c", "Bows/bow short.mp3", catalog_id=entry, secs=30.0),
            # Two cuts and nothing else: not a group.
            _fx("d", "Awards/fanfare.mp3", catalog_id=other, secs=20.0),
            _fx("e", "Awards/fanfare long.mp3", catalog_id=other, secs=60.0),
        ]
    )

    summary = dup.run_duplicates(run_id="d-1", mode=FlowMode.WRITE, api=catalog)

    [group] = summary.groups
    assert _members(group) == {
        "a": None,
        "b": None,
        "c": "a different edit (0:30 vs 3:20)",
    }
    [_, fx] = catalog.reports
    assert fx.library_root_id == FX and fx.groups == [group]


def test_fx_copies_in_different_folders_are_listed_for_the_owner_not_grouped(
    roots,
) -> None:
    entry = uuid.uuid4()
    catalog = RootsCatalog(
        rows=[
            _fx("a", "Walk-ons/song.mp3", md5="m1", catalog_id=entry),
            _fx("b", "Events/MADjam 2026/song.mp3", md5="m1", catalog_id=entry),
            _fx("c", "Rotation/other.mp3", md5="m9"),
        ]
    )
    report = MagicMock()

    summary = dup.run_duplicates(run_id="d-1", mode=FlowMode.WRITE, api=catalog)
    dup._record(report, summary, 1.0)

    assert summary.groups == []
    [_, fx_report] = catalog.reports
    assert fx_report.groups == []
    [_, fx] = summary.roots
    assert fx.across_folders == [["Events/MADjam 2026/song.mp3", "Walk-ons/song.mp3"]]
    assert (
        f"FX Music: {dup.ACROSS_SECTION[0].lower()}{dup.ACROSS_SECTION[1:]}",
        "/Events/MADjam 2026/song.mp3 · /Walk-ons/song.mp3",
    ) in [c.args for c in report.notice.call_args_list]


def test_an_fx_owner_edit_in_one_folder_is_not_a_copy_of_the_published_song(
    roots,
) -> None:
    catalog = RootsCatalog(
        rows=[
            _fx("song", "Bow/baby.mp3", catalog_id=_SONG, md5="m"),
            _fx("cut", "Bow/baby (1).mp3", catalog_id=_CUT, md5="m"),
        ],
        displays=dict(_SHOWN),
    )

    summary = dup.run_duplicates(run_id="d-1", mode=FlowMode.DRY_RUN, api=catalog)

    assert summary.groups == []


def test_an_fx_owner_edit_in_another_folder_is_not_listed_as_a_copy(roots) -> None:
    catalog = RootsCatalog(
        rows=[
            _fx("song", "Bow/baby.mp3", catalog_id=_SONG, md5="m"),
            _fx("cut", "Walk On/baby.mp3", catalog_id=_CUT, md5="m"),
            _fx("cut2", "Award/baby.mp3", catalog_id=_CUT, md5="m"),
        ],
        displays=dict(_SHOWN),
    )

    summary = dup.run_duplicates(run_id="d-1", mode=FlowMode.DRY_RUN, api=catalog)

    [fx] = [r for r in summary.roots if r.root.key == "fx"]
    assert fx.across_folders == [["Award/baby.mp3", "Walk On/baby.mp3"]]


def test_with_two_roots_each_group_names_its_root(roots) -> None:
    catalog = RootsCatalog(
        rows=[
            _file("a", "Pop/x.mp3", md5="m1"),
            _file("b", "/New Music/x.mp3", md5="m1"),
            _fx("c", "Bows/y.mp3", md5="m2"),
            _fx("d", "Bows/y (1).mp3", md5="m2"),
        ]
    )
    report = MagicMock()

    summary = dup.run_duplicates(run_id="d-1", mode=FlowMode.DRY_RUN, api=catalog)
    dup._record(report, summary, 1.0)

    assert _grouped(report.notice.call_args_list) == [
        (
            "Main library: copies to resolve",
            "identical file: /Pop/x.mp3 · /New Music/x.mp3",
        ),
        (
            "FX Music: copies to resolve",
            "identical file, same file name: /Bows/y.mp3 · /Bows/y (1).mp3",
        ),
    ]
    assert catalog.reports == []  # a dry run sends nothing


def test_a_root_that_fails_does_not_stop_the_other(roots) -> None:
    catalog = RootsCatalog(
        rows=[_fx("c", "Bows/y.mp3", md5="m2"), _fx("d", "Bows/y (1).mp3", md5="m2")],
        broken={ROOT},
    )
    report = MagicMock()

    summary = dup.run_duplicates(run_id="d-1", mode=FlowMode.WRITE, api=catalog)
    dup._record(report, summary, 1.0)

    [fx] = catalog.reports
    assert fx.library_root_id == FX and len(fx.groups) == 1
    assert "registry down" in summary.roots[0].failed
    issues = [c.args for c in report.issue.call_args_list]
    assert issues[0][0] == "failed" and issues[0][1].startswith("Main library: ")


@pytest.mark.parametrize(
    ("registered", "shown"),
    [
        (
            "ItRunsThroughMe_FeatDeLaSoul_TomMisch_97.mp3",
            "/ItRunsThroughMe_FeatDeLaSoul_TomMisch_97.mp3",
        ),
        ("House/x.mp3", "/House/x.mp3"),
        ("/New Music/x.mp3", "/New Music/x.mp3"),
    ],
)
def test_a_registered_path_is_shown_from_its_root(registered: str, shown: str) -> None:
    assert dup.shown_path(registered) == shown


# ── Reported when the groups change ───────────────────────────────────────


@dataclass
class KeyedCatalog(FakeCatalog):
    """The API's own count: a group is added when its key is new to the
    root's open groups, dropped when a report leaves it out."""

    open_keys: set[str] = field(default_factory=set)

    def report_duplicate_groups(self, payload: DuplicateReport):
        self.reports.append(payload)
        keys = {g.key for g in payload.groups}
        result = DuplicateReportResult(
            open_groups=len(keys),
            added=len(keys - self.open_keys),
            dropped=len(self.open_keys - keys),
        )
        self.open_keys = keys
        return result


@pytest.fixture
def posted(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    from mini_app_polis import pipeline_status

    fake = MagicMock()
    monkeypatch.setattr(pipeline_status, "post_run_finding", fake)
    return fake


@pytest.fixture
def state_drive() -> FakeDrive:
    drive = FakeDrive()
    drive.items[STATE] = DriveItem(STATE, "state", ["elsewhere"], mime_type="folder")
    return drive


def _run(catalog, drive, n: int, mode: FlowMode = FlowMode.WRITE):
    return dup.run_duplicates(run_id=f"d-{n}", mode=mode, api=catalog, drive=drive)


def _posted_a_report(posted: MagicMock) -> bool:
    """Whether the run's report goes to Discord: a SUCCESS goes only when
    notable."""
    call = posted.call_args
    return call.args[1] != "SUCCESS" or bool(call.kwargs["notable"])


def test_the_same_groups_as_the_last_run_post_no_report(posted, state_drive) -> None:
    catalog = KeyedCatalog(rows=_catalog().rows)
    _run(catalog, state_drive, 1)
    assert _posted_a_report(posted)

    again = _run(catalog, state_drive, 2)

    assert again.unchanged and len(again.groups) == 1
    assert not _posted_a_report(posted)
    # The run is still recorded: its counts, and the groups sent to the API.
    assert posted.call_args.kwargs["groups"] == 1 and len(catalog.reports) == 2
    assert [i.name for i in state_drive.in_folder(STATE)] == [dup.SEEN_NAME]


def test_a_dry_run_with_the_same_groups_posts_no_report(posted, state_drive) -> None:
    catalog = KeyedCatalog(rows=_catalog().rows)
    _run(catalog, state_drive, 1, FlowMode.DRY_RUN)

    again = _run(catalog, state_drive, 2, FlowMode.DRY_RUN)

    assert again.unchanged and not _posted_a_report(posted)
    assert catalog.reports == []


def test_a_group_added_is_reported(posted, state_drive) -> None:
    catalog = KeyedCatalog(rows=_catalog().rows)
    _run(catalog, state_drive, 1)
    _run(catalog, state_drive, 2)
    catalog.rows += [
        _file("c", "Rock/x.mp3", md5="same"),
        _file("d", "Rock/y.mp3", md5="same"),
    ]

    changed = _run(catalog, state_drive, 3)

    assert not changed.unchanged and len(changed.groups) == 2
    assert _posted_a_report(posted)
    assert "Rock/x.mp3" in posted.call_args.kwargs["text"]


def test_a_group_dropped_is_reported(posted, state_drive) -> None:
    catalog = KeyedCatalog(rows=_catalog().rows)
    catalog.rows += [
        _file("c", "Rock/x.mp3", md5="same"),
        _file("d", "Rock/y.mp3", md5="same"),
    ]
    _run(catalog, state_drive, 1)
    _run(catalog, state_drive, 2)
    catalog.rows = [r for r in catalog.rows if r.drive_file_id != "d"]

    changed = _run(catalog, state_drive, 3)

    assert not changed.unchanged and len(changed.groups) == 1
    assert _posted_a_report(posted)


def test_fx_sets_across_folders_unchanged_post_no_report(
    roots, posted, state_drive
) -> None:
    entry = uuid.uuid4()
    catalog = RootsCatalog(
        rows=[
            _fx("a", "Walk-ons/song.mp3", md5="m1", catalog_id=entry),
            _fx("b", "Events/MADjam 2026/song.mp3", md5="m1", catalog_id=entry),
        ]
    )
    _run(catalog, state_drive, 1)
    assert _posted_a_report(posted)

    again = _run(catalog, state_drive, 2)

    assert again.unchanged and not _posted_a_report(posted)
