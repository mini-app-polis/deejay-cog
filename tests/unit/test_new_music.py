"""The new-music worker mode: each path a file can take, the modes, the
folder settings, the audio check and the run record."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from mini_app_polis import pipeline_status
from mini_app_polis.lambda_worker import RunOutOfTime
from music_fakes import AUDIO, FakeDrive, mp3
from mutagen.id3 import TALB, TIT2, TPE1

from deejay_cog import library_backfill as backfill
from deejay_cog import new_music as flow
from deejay_cog.music_safety import (
    SOURCE_PROPERTY,
    audio_payload,
    md5,
    tag_snapshot,
)
from flag_fakes import FLAGS

IN, DEST, REVIEW, ORIG, STATE = "in", "dest", "review", "orig", "state"
_NAME = "Temple_Thomas Gold.mp3"


def _tagged(title: str = "Temple", artist: str = "Thomas Gold") -> bytes:
    return mp3(
        TIT2(encoding=3, text=[title]),
        TPE1(encoding=3, text=[artist]),
        TALB(encoding=3, text=["Some Album"]),
    )


@dataclass
class FakeApi:
    """The API as intake sees it: only the library registry, read for names."""

    #: Paths in the library registry; listing raises this while it is set.
    library: list[str] = field(default_factory=list)
    registry_down: Exception | None = None
    listings: list[dict] = field(default_factory=list)
    #: Registry rows with more than a path (md5, root, trash), after those.
    rows: list[SimpleNamespace] = field(default_factory=list)

    def lookup_catalog(self, _payload):
        raise AssertionError("intake never looks a file up")

    def create_library_entry(self, _payload):
        raise AssertionError("intake never makes a catalog entry")

    def list_library_files(self, *, present=None, limit=None, offset=None):
        self.listings.append({"present": present, "limit": limit, "offset": offset})
        if self.registry_down is not None:
            raise self.registry_down
        start = offset or 0
        every = [SimpleNamespace(path=p) for p in self.library] + self.rows
        return every[start : start + (limit or 100)]


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch) -> None:
    for role, value in {
        "input": IN,
        "destination": DEST,
        "review": REVIEW,
        "originals": ORIG,
        "state": STATE,
    }.items():
        monkeypatch.setenv(flow.FOLDER_VARS[role], value)
    monkeypatch.setitem(FLAGS, flow.MODE_FLAG, "write")
    monkeypatch.delenv(flow.MAX_FILES_VAR, raising=False)


@pytest.fixture
def posted(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    fake = MagicMock()
    monkeypatch.setattr(pipeline_status, "post_run_finding", fake)
    return fake


@pytest.fixture
def errors(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    fake = MagicMock()
    monkeypatch.setattr(flow, "post_run_finding", fake)
    return fake


@pytest.fixture
def api() -> FakeApi:
    return FakeApi()


@pytest.fixture
def wired(monkeypatch: pytest.MonkeyPatch, drive: FakeDrive, api: FakeApi) -> MagicMock:
    """The real flow on the fake Drive and API."""
    google = MagicMock()
    google.from_env.return_value = SimpleNamespace(drive=drive)
    monkeypatch.setattr(flow, "GoogleAPI", google)
    monkeypatch.setattr(flow, "api_client", lambda **_: api)
    monkeypatch.setattr(flow, "shared_deadline_passed", lambda: False)
    return google


def _record(drive: FakeDrive) -> dict:
    (item,) = drive.in_folder(STATE)
    return json.loads(item.data)


# ---------------------------------------------------------------------------
# Modes and settings
# ---------------------------------------------------------------------------


def test_the_worker_runs_this_mode() -> None:
    from deejay_cog import worker

    assert worker.MODES["new-music"] is flow.new_music
    assert worker.FLOW_NAMES["new-music"] == "new-music"


@pytest.mark.parametrize("value", [None, "", "on", "WRITE!", "yes"])
def test_unset_or_unknown_mode_is_off_and_nothing_is_touched(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch, value
) -> None:
    drive.add("a.mp3", IN, _tagged())
    if value is None:
        monkeypatch.delitem(FLAGS, flow.MODE_FLAG, raising=False)
    else:
        monkeypatch.setitem(FLAGS, flow.MODE_FLAG, value)

    summary = flow.new_music(run_id="m-1")

    assert summary.scanned == 0
    wired.from_env.assert_not_called()
    assert drive.calls == []
    assert "is off" in posted.call_args.kwargs["text"]


@pytest.mark.parametrize(
    "role", ["input", "destination", "review", "originals", "state"]
)
def test_a_missing_folder_setting_does_nothing(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch, role
) -> None:
    drive.add("a.mp3", IN, _tagged())
    monkeypatch.delenv(flow.FOLDER_VARS[role])

    summary = flow.new_music(run_id="m-1")

    assert summary.scanned == 0
    assert drive.calls == []
    assert posted.call_args.args[1] == "WARN"
    assert flow.FOLDER_VARS[role] in posted.call_args.kwargs["text"]


def test_two_roles_on_one_folder_does_nothing(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    drive.add("a.mp3", IN, _tagged())
    monkeypatch.setenv(flow.FOLDER_VARS["destination"], IN)

    flow.new_music(run_id="m-1")

    assert drive.calls == []
    assert "must all be different" in posted.call_args.kwargs["text"]


# ---------------------------------------------------------------------------
# Taken in: an unchanged copy, named by its own tags
# ---------------------------------------------------------------------------


def test_a_file_is_copied_unchanged_and_named_by_its_own_tags(
    settings, wired, posted, drive
) -> None:
    original = _tagged()
    file_id = drive.add("track01.mp3", IN, original)

    summary = flow.new_music(run_id="m-1")

    assert summary.identified == [f"track01.mp3 -> {_NAME}"]
    (copy,) = drive.in_folder(DEST)
    assert copy.name == _NAME
    assert copy.app_properties == {SOURCE_PROPERTY: file_id}
    # Tags and audio exactly as they came in: the library steps do the rest.
    assert copy.data == original
    assert audio_payload(copy.data) == AUDIO
    # The original: moved, never changed, never renamed.
    item = drive.items[file_id]
    assert item.parents == [ORIG]
    assert item.data == original
    assert item.name == "track01.mp3"
    assert not any(m == "update_file" and a[0] == file_id for m, a in drive.calls)

    record = _record(drive)
    assert record["flow"] == "new-music"
    assert record["run_id"] == "m-1"
    assert record["dry_run"] is False
    assert record["status"] == "complete"
    (entry,) = record["entries"]
    assert entry["file_id"] == file_id
    assert entry["original_name"] == "track01.mp3"
    assert entry["identified_by"] == "own tags"
    assert entry["state"] == "done"
    assert entry["old_tags"] == entry["new_tags"]
    assert entry["copy"] == {"id": copy.id, "folder_id": DEST, "name": copy.name}
    assert entry["move"] == {"from": IN, "to": ORIG}
    assert posted.call_args.args[1] == "SUCCESS"
    assert posted.call_args.kwargs["identified"] == 1


def test_dry_run_writes_nothing_but_its_record(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(FLAGS, flow.MODE_FLAG, "dry-run")
    drive.add("track01.mp3", IN, _tagged())
    drive.add("clip.m4a", IN, b"m4a", mime_type="audio/mp4")
    drive.add(_NAME, DEST, b"x")

    summary = flow.new_music(run_id="m-1")

    assert drive.writes(except_folder=STATE) == []
    # Only one write at all: the would-be record, created once.
    assert [m for m, _ in drive.writes()] == ["upload_bytes"]
    (item,) = drive.in_folder(STATE)
    assert item.name == "new-music-m-1.dry-run.json"
    record = json.loads(item.data)
    assert record["dry_run"] is True
    assert {e["state"] for e in record["entries"]} == {"planned"}
    assert [e["action"] for e in record["entries"]] == ["review", "review"]
    assert summary.collisions and summary.needs_review
    text = posted.call_args.kwargs["text"]
    assert "dry-run" in text


def test_dry_run_reports_what_it_would_write(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(FLAGS, flow.MODE_FLAG, "dry-run")
    file_id = drive.add("track01.mp3", IN, _tagged())

    summary = flow.new_music(run_id="m-1")

    assert drive.writes(except_folder=STATE) == []
    assert drive.items[file_id].parents == [IN]
    (entry,) = _record(drive)["entries"]
    assert entry["action"] == "deliver"
    assert entry["new_name"] == _NAME
    assert entry["new_tags"]["title"] == "Temple"
    assert entry["copy"] is None and entry["move"] is None
    assert summary.identified
    assert posted.call_args.kwargs["not_written_dry_run"] == 1


@pytest.mark.parametrize(
    ("title", "name"),
    [
        ("Temple [Clean]", "Temple [Clean]_Thomas Gold.mp3"),
        ("Temple (Explicit)", "Temple (Explicit)_Thomas Gold.mp3"),
    ],
)
def test_a_release_label_stays_in_the_title_and_name(
    settings, wired, drive, title, name
) -> None:
    original = _tagged(title)
    drive.add("track01.mp3", IN, original)

    summary = flow.new_music(run_id="m-1")

    assert summary.identified == [f"track01.mp3 -> {name}"]
    (copy,) = drive.in_folder(DEST)
    assert copy.name == name
    assert copy.data == original


def test_a_file_with_no_usable_title_or_artist_goes_to_review(
    settings, wired, drive
) -> None:
    file_id = drive.add("new.mp3", IN, mp3(TALB(encoding=3, text=["Album only"])))

    summary = flow.new_music(run_id="m-1")

    assert summary.needs_review == ["new.mp3 (no usable title or artist)"]
    assert drive.items[file_id].parents == [REVIEW]
    assert drive.in_folder(DEST) == []


# ---------------------------------------------------------------------------
# Needs review and collisions
# ---------------------------------------------------------------------------


def test_a_file_that_is_not_an_mp3_goes_to_review_unchanged(
    settings, wired, drive
) -> None:
    file_id = drive.add("clip.m4a", IN, b"m4a bytes", mime_type="audio/mp4")

    summary = flow.new_music(run_id="m-1")

    assert summary.needs_review
    assert drive.items[file_id].parents == [REVIEW]
    assert drive.items[file_id].data == b"m4a bytes"
    assert drive.in_folder(DEST) == []


def test_a_name_already_in_the_destination_is_not_overwritten(
    settings, wired, posted, drive
) -> None:
    existing = drive.add(_NAME, DEST, b"mine")
    original = _tagged()
    file_id = drive.add("track01.mp3", IN, original)

    summary = flow.new_music(run_id="m-1")

    assert summary.collisions == [f"track01.mp3 -> {_NAME}"]
    assert [i.id for i in drive.in_folder(DEST)] == [existing]
    assert drive.items[existing].data == b"mine"
    assert drive.items[file_id].parents == [REVIEW]
    assert drive.items[file_id].data == original
    assert not any(m == "upload_bytes" and a[0] == DEST for m, a in drive.calls)
    assert posted.call_args.args[1] == "WARN"
    assert "track01.mp3" in posted.call_args.kwargs["text"]


def test_a_copy_an_earlier_run_made_is_used_not_made_twice(
    settings, wired, drive
) -> None:
    file_id = drive.add("track01.mp3", IN, _tagged())
    # As an earlier run stopped after its upload, before the move.
    earlier = drive.add(
        _NAME,
        DEST,
        mp3(TIT2(encoding=3, text=["Temple"]), TPE1(encoding=3, text=["T"])),
        app_properties={SOURCE_PROPERTY: file_id},
    )

    flow.new_music(run_id="m-2")

    assert [i.id for i in drive.in_folder(DEST)] == [earlier]
    assert drive.items[file_id].parents == [ORIG]
    (entry,) = _record(drive)["entries"]
    assert entry["copy"]["made_by"] == "an earlier run"


@pytest.mark.parametrize(
    ("tags", "name"),
    [
        ({"title": "A/B: C?", "artist": "D\\E"}, "A B C_D E.mp3"),
        ({"title": "  Hi  there ", "artist": "x\x00y"}, "Hi there_x y.mp3"),
        ({"title": "", "artist": "x"}, None),
        ({"title": "...", "artist": "x"}, None),
    ],
)
def test_names_are_sanitized(tags, name) -> None:
    assert flow.delivery_name(tags, "orig.MP3") == name


# ---------------------------------------------------------------------------
# The audio check stops the run
# ---------------------------------------------------------------------------


def test_audio_changed_by_the_upload_trashes_the_copy_and_stops(
    settings, wired, errors, drive
) -> None:
    drive.corrupt_uploads = True
    first = drive.add("a.mp3", IN, _tagged())
    second = drive.add("b.mp3", IN, _tagged())

    summary = flow.new_music(run_id="m-1")

    assert summary.aborted
    assert drive.in_folder(DEST) == []  # the bad copy is in the trash
    trashed = [i for i in drive.items.values() if i.trashed]
    assert len(trashed) == 1 and DEST in trashed[0].parents
    assert drive.items[first].parents == [IN]
    assert drive.items[second].parents == [IN]
    record = _record(drive)
    assert record["status"] == "aborted"
    assert record["entries"][0]["state"] == "failed"
    assert record["entries"][0]["copy"]["trashed"] == "true"
    errors.assert_called_once()
    assert not any(m == "delete_file" for m, _ in drive.calls)


# ---------------------------------------------------------------------------
# Cap, deadline, report
# ---------------------------------------------------------------------------


def test_the_cap_limits_a_run(
    settings, wired, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(flow.MAX_FILES_VAR, "1")
    drive.add("a.mp3", IN, _tagged())
    drive.add("b.mp3", IN, _tagged("brand new", "x"))

    summary = flow.new_music(run_id="m-1")

    assert summary.scanned == 1


def test_the_deadline_stops_between_files_and_the_record_survives(
    settings, wired, posted, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    drive.add("a.mp3", IN, _tagged())
    drive.add("b.mp3", IN, _tagged())
    ticks = iter([False, True])
    monkeypatch.setattr(flow, "shared_deadline_passed", lambda: next(ticks))

    with pytest.raises(RunOutOfTime):
        flow.new_music(run_id="m-1")

    record = _record(drive)
    assert record["status"] == "stopped"
    assert len(record["entries"]) == 1
    assert record["entries"][0]["state"] == "done"
    assert "Stopped early" in posted.call_args.kwargs["text"]


def test_a_failing_file_does_not_end_the_sweep(
    settings, wired, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    drive.add("a.mp3", IN, _tagged())
    drive.add("b.mp3", IN, _tagged("Other", "Someone"))
    calls = iter([RuntimeError("tags unreadable")])
    real = flow.read_tag_row

    def flaky(data):
        exc = next(calls, None)
        if exc:
            raise exc
        return real(data)

    monkeypatch.setattr(flow, "read_tag_row", flaky)

    summary = flow.new_music(run_id="m-1")

    assert len(summary.failed) == 1
    assert len(summary.identified) == 1


def test_a_record_that_cannot_be_saved_stops_before_any_write(
    settings, wired, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    file_id = drive.add("track01.mp3", IN, _tagged())

    def refuse(**_):
        raise RuntimeError("state folder not writable")

    monkeypatch.setattr(drive, "upload_bytes", refuse)

    summary = flow.new_music(run_id="m-1")

    assert summary.failed
    assert drive.items[file_id].parents == [IN]
    assert drive.in_folder(DEST) == []


def test_a_file_that_left_the_input_folder_is_never_moved(
    settings, wired, drive, monkeypatch: pytest.MonkeyPatch
) -> None:
    file_id = drive.add("track01.mp3", IN, _tagged())
    real_upload = drive.upload_bytes

    def upload_then_owner_files_it(**kw):
        new_id = real_upload(**kw)
        if kw["parent_id"] == DEST:
            drive.items[file_id].parents = [IN, "library"]
        return new_id

    monkeypatch.setattr(drive, "upload_bytes", upload_then_owner_files_it)

    summary = flow.new_music(run_id="m-1")

    assert summary.failed and "no longer only in the input folder" in summary.failed[0]
    assert drive.items[file_id].parents == [IN, "library"]
    assert not any(m == "move_file" for m, _ in drive.calls)


# ---------------------------------------------------------------------------
# Names: the main artist, featured artists only to avoid a clash
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("artist", "main", "featured"),
    [
        ("Emotional Oranges", ["Emotional Oranges"], []),
        ("Pink Sweat$ feat. Kehlani", ["Pink Sweat$"], ["Kehlani"]),
        ("Pink Sweat$ ft. Kehlani", ["Pink Sweat$"], ["Kehlani"]),
        ("Pink Sweat$ (featuring Kehlani)", ["Pink Sweat$"], ["Kehlani"]),
        ("Wale feat. Usher, Rick Ross", ["Wale"], ["Usher", "Rick Ross"]),
        ("Kelly Clarkson, Dolly Parton", ["Kelly Clarkson", "Dolly Parton"], []),
    ],
)
def test_an_artist_tag_splits_into_main_and_featured(
    artist: str, main: list[str], featured: list[str]
) -> None:
    assert flow.split_artist(artist) == (main, featured)


def test_a_plain_song_is_named_by_its_title_and_artist() -> None:
    assert flow.delivery_names(
        {"title": "Sundays", "artist": "Emotional Oranges"}, "a.mp3"
    ) == ["Sundays_Emotional Oranges.mp3"]


def test_a_song_with_features_is_named_by_its_main_artist_first() -> None:
    assert flow.delivery_names(
        {"title": "At My Worst", "artist": "Pink Sweat$ feat. Kehlani"}, "a.mp3"
    ) == [
        "At My Worst_Pink Sweat$.mp3",
        "At My Worst_Pink Sweat$ feat. Kehlani.mp3",
    ]


def _featured() -> bytes:
    return _tagged("At My Worst", "Pink Sweat$ feat. Kehlani")


def test_a_song_with_features_is_delivered_under_its_main_artist(
    settings, wired, drive
) -> None:
    drive.add("track01.mp3", IN, _featured())

    summary = flow.new_music(run_id="m-1")

    assert summary.identified == ["track01.mp3 -> At My Worst_Pink Sweat$.mp3"]
    assert [i.name for i in drive.in_folder(DEST)] == ["At My Worst_Pink Sweat$.mp3"]


def test_a_name_taken_in_the_library_adds_the_featured_artists(
    settings, wired, drive, api: FakeApi
) -> None:
    api.library = ["R&B/At My Worst_Pink Sweat$.mp3"]
    drive.add("track01.mp3", IN, _featured())

    summary = flow.new_music(run_id="m-1")

    assert summary.identified == [
        "track01.mp3 -> At My Worst_Pink Sweat$ feat. Kehlani.mp3"
    ]
    assert api.listings[0]["present"] is True


def test_a_name_taken_even_with_the_features_goes_to_review(
    settings, wired, drive, api: FakeApi
) -> None:
    api.library = ["a/At My Worst_Pink Sweat$.mp3"]
    drive.add("At My Worst_Pink Sweat$ feat. Kehlani.mp3", DEST, b"mine")
    file_id = drive.add("track01.mp3", IN, _featured())

    summary = flow.new_music(run_id="m-1")

    assert summary.identified == []
    assert len(summary.collisions) == 1
    assert drive.items[file_id].parents == [REVIEW]
    (entry,) = _record(drive)["entries"]
    assert "name collision" in entry["reason"]


def test_a_name_taken_with_no_features_to_add_goes_to_review(
    settings, wired, drive, api: FakeApi
) -> None:
    api.library = [f"House/{_NAME}"]
    file_id = drive.add("track01.mp3", IN, _tagged())

    summary = flow.new_music(run_id="m-1")

    assert summary.identified == []
    assert drive.items[file_id].parents == [REVIEW]


def test_a_registry_that_cannot_be_read_sends_the_file_to_review(
    settings, wired, drive, api: FakeApi
) -> None:
    api.registry_down = RuntimeError("API down")
    file_id = drive.add("track01.mp3", IN, _tagged())

    summary = flow.new_music(run_id="m-1")

    assert summary.identified == []
    assert drive.items[file_id].parents == [REVIEW]
    assert "library registry could not be read" in summary.needs_review[0]


def test_the_registry_is_read_once_a_run_and_paged(
    settings, wired, drive, api: FakeApi, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(flow.LibraryNames, "PAGE", 2)
    api.library = ["a/1.mp3", "a/2.mp3", "a/3.mp3"]
    drive.add("track01.mp3", IN, _tagged())
    drive.add("track02.mp3", IN, _tagged("temple", "thomas gold"))

    flow.new_music(run_id="m-1")

    assert [c["offset"] for c in api.listings] == [0, 2]


# ---------------------------------------------------------------------------
# Owner edits: a version in < > in the file's title
# ---------------------------------------------------------------------------

_BABY = "Baby What You Want Me To Do"


def test_an_owner_edit_is_copied_unchanged_and_named_without_brackets(
    settings, wired, drive
) -> None:
    title = f"{_BABY} <Intro Cut>"
    original = _tagged(title, "Etta James")
    drive.add("cut.mp3", IN, original)

    summary = flow.new_music(run_id="m-1")

    (copy,) = drive.in_folder(DEST)
    assert copy.name == f"{_BABY} Intro Cut_Etta James.mp3"
    assert "<" not in copy.name and ">" not in copy.name
    assert copy.data == original
    assert tag_snapshot(copy.data)["title"] == title
    assert summary.identified == [f"cut.mp3 -> {copy.name}"]


def test_names_never_carry_the_owner_edit_brackets() -> None:
    tags = {"title": f"{_BABY} <Intro Cut>", "artist": "Etta James"}
    assert flow.delivery_name(tags, "x.mp3") == f"{_BABY} Intro Cut_Etta James.mp3"


# ---------------------------------------------------------------------------
# A title tag that is an old file name
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("title", "artist", "check"),
    [
        # The title tags behind three names seen on 2026-11-11.
        (
            # BoxingUnderwater featCailinRusso Golding featCailinRusso 95_Golding.mp3
            "BoxingUnderwater featCailinRusso Golding featCailinRusso 95",
            "Golding",
            "a trailing BPM number",
        ),
        (
            # Burning (Bridges) (Sistek) (Remix) (Ella)_Vos.mp3
            "Burning (Bridges) (Sistek) (Remix) (Ella)",
            "Vos",
            "words in brackets one by one",
        ),
        (
            # CandyCleanKahlo113 Kahlo 113_Kah-lo.mp3
            "CandyCleanKahlo113 Kahlo 113",
            "Kah-lo",
            "a trailing BPM number",
        ),
        ("burning_bridges", "Ella Vos", "underscores"),
        ("BoxingUnderwater", "Golding", "run-together words"),
        ("CandyCleanKahlo113", "Kah-lo", "run-together words"),
        ("Kahlo Candy Clean", "Kah-lo", "the artist repeated in the title"),
        ("Golding Boxing Underwater", "Golding", "the artist repeated in the title"),
        ("Burning Bridges Ella Vos", "Ella Vos", "the artist repeated in the title"),
    ],
)
def test_a_title_that_looks_like_a_file_name_is_named(title, artist, check) -> None:
    assert flow.file_name_title(title, artist) == check


@pytest.mark.parametrize(
    ("title", "artist"),
    [
        ("Ain't No Sunshine (Explicit)", "Bill Withers"),
        ("2 AM", "Adrian Marcel"),
        ("Dernière danse", "Indila"),
        ("Alive (M4SONIC Remix)", "Krewella"),
        ("Alive (M4SONIC Remix)", "M4SONIC"),
        ("Be Like That", "Kane Brown"),
        ("Temple", "Thomas Gold"),
        ("Honesty (feat. Kehlani)", "Pink Sweat$ feat. Kehlani"),
        ("Attention (Lash Remix) (Clean)", "Charlie Puth"),
        ("Baby What You Want Me To Do <Intro Cut>", "Etta James"),
        ("Route 66", "Chuck Berry"),
        ("McDonald's Theme", "DeBarge"),
        ("Bad Company", "Bad Company"),
    ],
)
def test_an_ordinary_title_reads_as_a_title(title, artist) -> None:
    assert flow.file_name_title(title, artist) is None


def test_a_file_name_title_goes_to_review_unrenamed(settings, wired, drive) -> None:
    data = _tagged("CandyCleanKahlo113 Kahlo 113", "Kah-lo")
    file_id = drive.add("CandyCleanKahlo113.mp3", IN, data)
    ordinary = drive.add("be.mp3", IN, _tagged("Be Like That", "Kane Brown"))

    summary = flow.new_music(run_id="m-1")

    assert summary.needs_review == [
        "CandyCleanKahlo113.mp3 (its title tag looks like a file name (a trailing "
        "BPM number): 'CandyCleanKahlo113 Kahlo 113')"
    ]
    item = drive.items[file_id]
    assert item.parents == [REVIEW] and item.name == "CandyCleanKahlo113.mp3"
    assert item.data == data
    assert summary.identified == ["be.mp3 -> Be Like That_Kane Brown.mp3"]
    assert drive.items[ordinary].parents == [ORIG]
    [review, deliver] = _record(drive)["entries"]
    assert review["action"] == "review" and review["file_id"] == file_id
    assert review["move"] == {"from": IN, "to": REVIEW}
    assert "a trailing BPM number" in review["reason"]
    assert deliver["action"] == "deliver"


def test_a_dry_run_moves_no_file_name_title(
    settings, wired, drive, monkeypatch
) -> None:
    file_id = drive.add("x.mp3", IN, _tagged("burning_bridges", "Ella Vos"))
    monkeypatch.setitem(FLAGS, flow.MODE_FLAG, "dry-run")

    summary = flow.new_music(run_id="m-1")

    assert summary.needs_review and drive.items[file_id].parents == [IN]
    assert drive.writes(except_folder=STATE) == []
    [entry] = _record(drive)["entries"]
    assert entry["action"] == "review" and entry["state"] == "planned"


# ---------------------------------------------------------------------------
# A byte-identical copy of a library file
# ---------------------------------------------------------------------------

LIBRARY_ROOT, FX_ROOT = "library-root", "fx-root"


def _row(path: str, data: bytes | None, **kw) -> SimpleNamespace:
    values = {
        "drive_file_id": f"lib-{path}",
        "path": path,
        "md5": md5(data) if data is not None else None,
        "library_root_id": LIBRARY_ROOT,
        "trash_requested": False,
        **kw,
    }
    return SimpleNamespace(**values)


@pytest.fixture
def fx(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(backfill.FX_ROOT_FOLDER_VAR, FX_ROOT)


def test_an_exact_copy_of_a_library_file_goes_to_review(
    settings, wired, drive, api: FakeApi, fx
) -> None:
    data = _tagged()
    api.rows = [_row("/New Music/Temple_Thomas Gold.mp3", data)]
    file_id = drive.add("temple copy.mp3", IN, data)

    summary = flow.new_music(run_id="m-1")

    assert summary.needs_review == [
        "temple copy.mp3 (exact copy of a library file "
        "(/New Music/Temple_Thomas Gold.mp3))"
    ]
    item = drive.items[file_id]
    assert item.parents == [REVIEW] and item.name == "temple copy.mp3"
    assert item.data == data and not item.trashed
    assert drive.in_folder(DEST) == []
    [entry] = _record(drive)["entries"]
    assert entry["action"] == "review" and entry["move"] == {"from": IN, "to": REVIEW}


def test_a_file_no_library_file_matches_is_taken_in(
    settings, wired, drive, api: FakeApi, fx
) -> None:
    api.rows = [
        _row("other.mp3", _tagged("Other", "Someone")),
        _row("no-md5.mp3", None),
    ]
    drive.add("track01.mp3", IN, _tagged())

    summary = flow.new_music(run_id="m-1")

    assert summary.identified == [f"track01.mp3 -> {_NAME}"]
    assert summary.needs_review == []


def test_a_copy_of_a_file_marked_for_the_trash_is_taken_in(
    settings, wired, drive, api: FakeApi, fx
) -> None:
    data = _tagged()
    api.rows = [_row("old/Temple.mp3", data, trash_requested=True)]
    drive.add("track01.mp3", IN, data)

    summary = flow.new_music(run_id="m-1")

    assert summary.identified == [f"track01.mp3 -> {_NAME}"]


def test_a_copy_of_an_fx_music_file_only_is_taken_in(
    settings, wired, drive, api: FakeApi, fx
) -> None:
    data = _tagged()
    api.rows = [_row("Walk-ons/Temple.mp3", data, library_root_id=FX_ROOT)]
    drive.add("track01.mp3", IN, data)

    summary = flow.new_music(run_id="m-1")

    assert summary.identified == [f"track01.mp3 -> {_NAME}"]


def test_an_unreadable_registry_sends_the_file_to_review_before_copying(
    settings, wired, drive, api: FakeApi, fx
) -> None:
    api.registry_down = RuntimeError("API down")
    file_id = drive.add("track01.mp3", IN, _tagged())

    summary = flow.new_music(run_id="m-1")

    assert summary.needs_review == [
        "track01.mp3 (the library registry could not be read to check for an "
        "exact copy (RuntimeError: API down))"
    ]
    assert drive.items[file_id].parents == [REVIEW]
    assert drive.in_folder(DEST) == []


def test_the_copy_an_earlier_run_made_is_not_an_exact_copy(
    settings, wired, drive, api: FakeApi, fx
) -> None:
    data = _tagged()
    file_id = drive.add("track01.mp3", IN, data)
    copy_id = drive.add(_NAME, DEST, data, app_properties={SOURCE_PROPERTY: file_id})
    # The scan registered that copy before this run.
    api.rows = [_row(f"/New Music/{_NAME}", data, drive_file_id=copy_id)]

    summary = flow.new_music(run_id="m-1")

    assert summary.identified == [f"track01.mp3 -> {_NAME}"]
    assert drive.items[file_id].parents == [ORIG]


def test_a_dry_run_moves_no_exact_copy(
    settings, wired, drive, api: FakeApi, fx, monkeypatch
) -> None:
    data = _tagged()
    api.rows = [_row("Temple.mp3", data)]
    file_id = drive.add("track01.mp3", IN, data)
    monkeypatch.setitem(FLAGS, flow.MODE_FLAG, "dry-run")

    summary = flow.new_music(run_id="m-1")

    assert summary.needs_review and drive.items[file_id].parents == [IN]
    assert drive.writes(except_folder=STATE) == []
