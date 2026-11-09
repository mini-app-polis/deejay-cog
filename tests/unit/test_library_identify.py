"""The library-identify step: unidentified library files are identified by
their tags or sent to the fingerprint function, linked to the catalog and
written with its values in place, safely; and the fingerprint function's
library-identify flow, which writes no MusicBrainz tags."""

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
    CatalogLookupRequest,
    CatalogMatch,
    LibraryEntryCreate,
    LibraryEntryCreated,
    LibraryFile,
    LibraryFileRegister,
)
from music_fakes import DriveItem, FakeDrive, mp3
from mutagen.id3 import COMM, ID3, TBPM, TCON, TDRC, TIT2, TPE1

from deejay_cog import fingerprint as fp
from deejay_cog import library_backfill as backfill
from deejay_cog import library_identify as ident
from deejay_cog.fingerprint_jobs import FingerprintJobMessage
from deejay_cog.music_safety import FlowMode, RunRecord

ROOT, STATE = "root", "state"
_NOW = dt.datetime(2026, 11, 6, tzinfo=dt.UTC)
_ENTRY = uuid.UUID("11111111-2222-3333-4444-555555555555")


def _song(title: str = "temple", artist: str = "thomas gold", **kw: Any) -> bytes:
    frames = [TIT2(encoding=3, text=[title]), TPE1(encoding=3, text=[artist])]
    if kw.get("year"):
        frames.append(TDRC(encoding=3, text=[kw["year"]]))
    if kw.get("genre"):
        frames.append(TCON(encoding=3, text=[kw["genre"]]))
    if kw.get("bpm"):
        frames.append(TBPM(encoding=3, text=[kw["bpm"]]))
    frames.append(COMM(encoding=3, lang="eng", desc="", text=["cue: drop at 1:02"]))
    return mp3(*frames)


def _no_tags() -> bytes:
    return mp3(COMM(encoding=3, lang="eng", desc="", text=["cue"]))


def _frames(data: bytes) -> dict[str, str]:
    tags = ID3(io.BytesIO(data))
    return {key: str(frame.text[0]) for key, frame in tags.items()}


def _display(entry: uuid.UUID = _ENTRY, **kw: Any) -> CatalogDisplay:
    values = {
        "id": entry,
        "title": "Temple",
        "artist": "Thomas Gold",
        "release_year": 2019,
        "genre": "Dance/Electronic",
        "bpm": 125.5,
        "label": None,
        "changed_at": _NOW,
        **kw,
    }
    return CatalogDisplay(**values)


def _match(entry: uuid.UUID = _ENTRY) -> CatalogMatch:
    return CatalogMatch(id=entry, title="Temple", remix=None, artist="Thomas Gold")


@dataclass
class FakeApi:
    rows: list[LibraryFile] = field(default_factory=list)
    known: dict[str, CatalogMatch] = field(default_factory=dict)
    displays: dict[uuid.UUID, CatalogDisplay] = field(default_factory=dict)
    fills: list[tuple[str, CatalogFillUp]] = field(default_factory=list)
    registrations: list[LibraryFileRegister] = field(default_factory=list)
    created: list[LibraryEntryCreate] = field(default_factory=list)

    def list_library_files(self, **kwargs) -> list[LibraryFile]:
        assert kwargs["identification_status"] == "unidentified"
        assert kwargs["present"] is True
        start = kwargs["offset"]
        return self.rows[start : start + kwargs["limit"]]

    def lookup_catalog(self, payload: CatalogLookupRequest):
        return [self.known.get(t.title.lower()) for t in payload.tracks]

    def read_catalog_display(self, payload: CatalogDisplayRequest):
        return [self.displays.get(i) for i in payload.ids]

    def fill_up_catalog_entry(self, catalog_id, payload: CatalogFillUp):
        self.fills.append((str(catalog_id), payload))
        filled = [k for k, v in payload.model_dump().items() if v is not None]
        return CatalogFillUpResult(
            entry=self.displays[uuid.UUID(str(catalog_id))], filled=filled
        )

    def register_library_file(self, payload: LibraryFileRegister):
        self.registrations.append(payload)
        return payload

    def create_library_entry(self, payload: LibraryEntryCreate):
        self.created.append(payload)
        entry = uuid.UUID("99999999-2222-3333-4444-555555555555")
        self.displays[entry] = _display(
            entry, title=payload.title, artist=payload.artist
        )
        return LibraryEntryCreated(
            entry=CatalogMatch(id=entry, title=payload.title, artist=payload.artist),
            created=True,
        )


@dataclass
class FakeJobs:
    sent: list[dict[str, Any]] = field(default_factory=list)

    def send(self, file_id: str, name: str, **kw: Any) -> None:
        self.sent.append({"file_id": file_id, "name": name, **kw})


@pytest.fixture
def drive() -> FakeDrive:
    drive = FakeDrive()
    drive.items[STATE] = DriveItem(STATE, "state", ["elsewhere"], mime_type="folder")
    return drive


@pytest.fixture
def api() -> FakeApi:
    return FakeApi(known={"temple": _match()}, displays={_ENTRY: _display()})


@pytest.fixture
def jobs() -> FakeJobs:
    return FakeJobs()


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(backfill.ROOT_FOLDER_VAR, ROOT)
    monkeypatch.setenv(backfill.STATE_FOLDER_VAR, STATE)
    monkeypatch.setenv(ident.MODE_VAR, "write")
    monkeypatch.delenv(ident.CHUNK_SIZE_VAR, raising=False)
    monkeypatch.delenv(backfill.CHUNK_SIZE_VAR, raising=False)


@pytest.fixture
def posted(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    fake = MagicMock()
    monkeypatch.setattr(pipeline_status, "post_run_finding", fake)
    return fake


@pytest.fixture
def wired(
    monkeypatch: pytest.MonkeyPatch, drive: FakeDrive, api: FakeApi, jobs: FakeJobs
) -> None:
    google = MagicMock()
    google.from_env.return_value = SimpleNamespace(drive=drive)
    monkeypatch.setattr(ident, "GoogleAPI", google)
    monkeypatch.setattr(ident, "api_client", lambda **_: api)
    monkeypatch.setattr(ident, "fingerprint_jobs_from_env", lambda: jobs)
    monkeypatch.setattr(ident, "shared_deadline_passed", lambda: False)


def _unidentified(drive: FakeDrive, api: FakeApi, name: str, data: bytes) -> str:
    drive_id = drive.add(name, ROOT, data)
    api.rows.append(
        LibraryFile(
            id=uuid.uuid4(),
            drive_file_id=drive_id,
            library_root_id=ROOT,
            path=f"New/{name}",
            identification_status="unidentified",
            present=True,
            trash_requested=False,
            first_seen=_NOW,
            last_seen=_NOW,
        )
    )
    return drive_id


def _state(drive: FakeDrive, name: str) -> dict | None:
    found = [i for i in drive.in_folder(STATE) if i.name == name]
    return json.loads(found[0].data) if found else None


def _text(posted: MagicMock) -> str:
    return posted.call_args.kwargs["text"]


# ---------------------------------------------------------------------------
# By its tags
# ---------------------------------------------------------------------------


def test_off_reads_nothing(
    settings, wired, drive, api, jobs, posted, monkeypatch
) -> None:
    monkeypatch.delenv(ident.MODE_VAR)
    _unidentified(drive, api, "temple.mp3", _song())

    summary = ident.library_identify(run_id="i-1")

    assert summary.mode is FlowMode.OFF
    assert drive.calls == []
    assert api.registrations == [] and jobs.sent == []
    posted.assert_not_called()


def test_a_file_the_catalog_knows_is_linked_written_and_registered(
    settings, wired, drive, api, posted
) -> None:
    drive_id = _unidentified(
        drive,
        api,
        "temple.mp3",
        _song("temple", "thomas gold", genre="House", bpm="125"),
    )

    summary = ident.library_identify(run_id="i-1")

    assert summary.linked and summary.written == ["New/temple.mp3"]
    frames = _frames(drive.items[drive_id].data)
    assert frames["TIT2"] == "Temple"
    assert frames["TPE1"] == "Thomas Gold"
    assert frames["TCON"] == "Dance/Electronic"
    assert frames["TBPM"] == "125.5"  # one decimal, as the display rules round it
    assert frames["TXXX:CATALOG_ID"] == str(_ENTRY)
    assert frames["COMM::eng"] == "cue: drop at 1:02"  # the comment is kept
    # Fill-up first, with the file's own values.
    [(catalog_id, fill)] = api.fills
    assert catalog_id == str(_ENTRY) and fill.genre == "House" and fill.bpm == 125.0
    [reg] = api.registrations
    assert reg.identification_status == "identified" and reg.catalog_id == _ENTRY
    assert reg.comment_backup == "cue: drop at 1:02"
    assert reg.synced_at is not None
    assert reg.md5 is not None
    # Written safely: revision kept, run record saved.
    assert drive.items[drive_id].kept
    record = _state(drive, "library-identify-i-1.json")
    assert record["entries"][0]["action"] == "in_place"
    assert _state(drive, ident.TRIED_NAME)["files"][drive_id]["outcome"] == "linked"
    # Never renamed, moved or trashed.
    assert drive.items[drive_id].name == "temple.mp3"
    assert drive.items[drive_id].parents == [ROOT]


def test_the_files_own_release_label_stays_in_its_title(
    settings, wired, drive, api
) -> None:
    drive_id = _unidentified(
        drive, api, "temple.mp3", _song("Temple (No Intro Edit)", "Thomas Gold")
    )
    api.known["temple (no intro edit)"] = _match()

    ident.library_identify(run_id="i-1")

    assert _frames(drive.items[drive_id].data)["TIT2"] == "Temple (No Intro Edit)"
    assert api.registrations[0].edit_label == "No Intro Edit"


def test_garbled_tags_are_read_repaired(settings, wired, drive, api) -> None:
    garbled = "Beyoncé".encode().decode("latin-1")
    data = mp3(TIT2(encoding=0, text=["temple"]), TPE1(encoding=0, text=[garbled]))
    _unidentified(drive, api, "temple.mp3", data)
    seen: list[str] = []
    real = api.lookup_catalog

    def lookup(payload):
        seen.extend(t.artist for t in payload.tracks)
        return real(payload)

    api.lookup_catalog = lookup  # type: ignore[method-assign]

    ident.library_identify(run_id="i-1")

    assert seen == ["Beyoncé"]


# ---------------------------------------------------------------------------
# On to the fingerprint
# ---------------------------------------------------------------------------


def test_a_file_without_usable_tags_goes_to_the_fingerprint_queue(
    settings, wired, drive, api, jobs
) -> None:
    drive_id = _unidentified(drive, api, "track01.mp3", _no_tags())

    summary = ident.library_identify(run_id="i-1")

    [job] = jobs.sent
    assert job["file_id"] == drive_id and job["flow"] == "library-identify"
    assert (job["library_root_id"], job["path"], job["mode"]) == (
        ROOT,
        "New/track01.mp3",
        "write",
    )
    assert summary.to_fingerprint
    [note] = api.registrations
    assert note.identification_status == "unidentified" and note.catalog_id is None
    assert note.identify_note == (
        "its tags did not identify it (no usable title or artist); "
        "sent to fingerprinting"
    )
    assert note.tags is not None
    assert _state(drive, ident.TRIED_NAME)["files"][drive_id]["outcome"] == (
        "sent_to_fingerprint"
    )
    assert drive.writes(except_folder=STATE) == []


def test_tags_the_catalog_does_not_know_go_to_the_fingerprint_first(
    settings, wired, drive, api, jobs
) -> None:
    _unidentified(drive, api, "new.mp3", _song("brand new song", "someone"))

    ident.library_identify(run_id="i-1")

    assert len(jobs.sent) == 1
    assert api.created == []  # no entry from tags alone


def test_without_a_queue_the_file_is_named(
    settings, wired, drive, api, posted, monkeypatch
) -> None:
    monkeypatch.setattr(ident, "fingerprint_jobs_from_env", lambda: None)
    _unidentified(drive, api, "track01.mp3", _no_tags())

    summary = ident.library_identify(run_id="i-1")

    assert summary.no_queue
    assert "no_fingerprint_queue" in _text(posted)


# ---------------------------------------------------------------------------
# Position and safety
# ---------------------------------------------------------------------------


def test_a_tried_file_is_not_tried_again(settings, wired, drive, api, jobs) -> None:
    _unidentified(drive, api, "track01.mp3", _no_tags())
    ident.library_identify(run_id="i-1")

    summary = ident.library_identify(run_id="i-2")

    assert summary.tried == 0 and summary.waiting == 0
    assert len(jobs.sent) == 1


def test_each_run_takes_the_next_chunk(
    settings, wired, drive, api, jobs, monkeypatch
) -> None:
    monkeypatch.setenv(ident.CHUNK_SIZE_VAR, "2")
    for n in range(3):
        _unidentified(drive, api, f"{n}.mp3", _no_tags())

    first = ident.library_identify(run_id="i-1")
    second = ident.library_identify(run_id="i-2")

    assert (first.tried, second.tried) == (2, 1)
    assert len(jobs.sent) == 3


def test_a_dry_run_changes_nothing_and_tries_the_same_files_again(
    settings, wired, drive, api, jobs, posted, monkeypatch
) -> None:
    monkeypatch.setenv(ident.MODE_VAR, "dry-run")
    known = _unidentified(drive, api, "temple.mp3", _song(genre="House"))
    _unidentified(drive, api, "track01.mp3", _no_tags())
    before = drive.items[known].data

    first = ident.library_identify(run_id="i-1")
    second = ident.library_identify(run_id="i-2")

    assert first.would_write == ["New/temple.mp3"]
    assert first.to_fingerprint and second.tried == 2
    assert drive.items[known].data == before
    assert drive.writes(except_folder=STATE) == []
    assert api.fills == [] and api.registrations == [] and jobs.sent == []
    assert _state(drive, ident.TRIED_NAME) is None
    assert _state(drive, "library-identify-i-1.dry-run.json") is not None
    assert "would_link" in _text(posted)


def test_a_file_in_the_trash_is_left_to_the_scan(
    settings, wired, drive, api, jobs
) -> None:
    drive_id = _unidentified(drive, api, "temple.mp3", _song())
    drive.items[drive_id].trashed = True

    summary = ident.library_identify(run_id="i-1")

    assert summary.skipped and api.registrations == [] and jobs.sent == []


def test_audio_that_would_change_stops_the_run(
    settings, wired, drive, api, monkeypatch
) -> None:
    from deejay_cog import music_safety

    _unidentified(drive, api, "temple.mp3", _song())
    _unidentified(drive, api, "temple2.mp3", _song())
    finding = MagicMock()
    monkeypatch.setattr(ident, "post_run_finding", finding)

    def refuse(original, candidate):
        raise music_safety.AudioChanged("payload differs")

    monkeypatch.setattr(music_safety, "check_audio_unchanged", refuse)

    summary = ident.library_identify(run_id="i-1")

    assert summary.aborted and summary.tried == 1
    assert drive.writes(except_folder=STATE) == []
    assert finding.call_args.args[1] == "ERROR"


# ---------------------------------------------------------------------------
# The fingerprint function's library-identify flow
# ---------------------------------------------------------------------------


@dataclass
class FakeIdentifier:
    score: float = 0.95
    metadata: dict[str, Any] = field(
        default_factory=lambda: {
            "title": "Temple",
            "artist": "Thomas Gold",
            "year": "2019",
        }
    )

    def identify(self, path: str, *, fetch_metadata: bool = True):
        del path, fetch_metadata
        return SimpleNamespace(
            chosen=SimpleNamespace(confidence=self.score), metadata=self.metadata
        )


def _record(drive: FakeDrive, mode: FlowMode = FlowMode.WRITE) -> RunRecord:
    return RunRecord("library-identify", "m-1", mode).attach(drive, STATE)


def test_a_confident_fingerprint_in_the_catalog_is_linked_without_musicbrainz_tags(
    drive, api
) -> None:
    drive_id = drive.add("track01.mp3", ROOT, _no_tags())
    api.known["temple"] = _match()

    result = fp.identify_library_file(
        drive,
        api,
        FakeIdentifier(),
        _record(drive),
        file_id=drive_id,
        root=ROOT,
        path="New/track01.mp3",
        threshold=0.9,
    )

    assert result.outcome == "linked" and not result.entry_created
    frames = _frames(drive.items[drive_id].data)
    assert frames["TXXX:CATALOG_ID"] == str(_ENTRY)
    assert not any(k.startswith("TXXX:MusicBrainz") for k in frames)
    assert api.registrations[0].catalog_id == _ENTRY


def test_a_confident_fingerprint_the_catalog_lacks_gets_a_library_entry(
    drive, api
) -> None:
    drive_id = drive.add("track01.mp3", ROOT, _no_tags())
    api.known.clear()

    result = fp.identify_library_file(
        drive,
        api,
        FakeIdentifier(),
        _record(drive),
        file_id=drive_id,
        root=ROOT,
        path="New/track01.mp3",
        threshold=0.9,
    )

    assert result.entry_created
    [made] = api.created
    assert (made.title, made.artist, made.release_year) == (
        "Temple",
        "Thomas Gold",
        2019,
    )
    assert api.registrations[0].identification_status == "identified"


def test_an_unconfident_fingerprint_only_records_why(drive, api) -> None:
    drive_id = drive.add("track01.mp3", ROOT, _no_tags())

    result = fp.identify_library_file(
        drive,
        api,
        FakeIdentifier(score=0.5),
        _record(drive),
        file_id=drive_id,
        root=ROOT,
        path="New/track01.mp3",
        threshold=0.9,
    )

    assert result.outcome == "below_confidence"
    assert drive.writes(except_folder=STATE) == []
    assert api.created == []
    [note] = api.registrations
    assert note.identification_status == "unidentified" and note.catalog_id is None
    assert note.identify_note == (
        "fingerprint: no confident match (best guess Thomas Gold - Temple, score 0.50)"
    )


def test_a_recording_musicbrainz_does_not_serve_only_records_why(drive, api) -> None:
    drive_id = drive.add("track01.mp3", ROOT, _no_tags())
    gone = {"_provider": "musicbrainz", "_mbid": "mbid-1", fp.NOT_SERVED: True}

    result = fp.identify_library_file(
        drive,
        api,
        FakeIdentifier(metadata=gone),
        _record(drive),
        file_id=drive_id,
        root=ROOT,
        path="New/track01.mp3",
        threshold=0.9,
    )

    assert result.outcome == "recording_not_served"
    assert drive.writes(except_folder=STATE) == []
    assert api.created == []
    [note] = api.registrations
    assert note.identification_status == "unidentified" and note.catalog_id is None
    assert note.identify_note == (
        "fingerprint: matched recording mbid-1 (score 0.95), which MusicBrainz "
        "does not serve; left for review"
    )


def test_a_file_whose_audio_cannot_be_read_only_records_why(drive, api) -> None:
    drive_id = drive.add("track01.mp3", ROOT, _no_tags())

    class Unreadable:
        def identify(self, path: str, *, fetch_metadata: bool = True):
            del path, fetch_metadata
            raise fp.AudioUnreadable("fpcalc exited with status 3: ERROR")

    result = fp.identify_library_file(
        drive,
        api,
        Unreadable(),
        _record(drive),
        file_id=drive_id,
        root=ROOT,
        path="New/track01.mp3",
        threshold=0.9,
    )

    assert result.outcome == "audio_unreadable" and result.score is None
    assert drive.writes(except_folder=STATE) == []
    [note] = api.registrations
    assert note.identification_status == "unidentified" and note.catalog_id is None
    assert note.identify_note == (
        "fingerprint: the audio could not be read (fpcalc exited with status 3: "
        "ERROR); left for review"
    )


def _no_fingerprint(drive, api, data: bytes, mode: FlowMode = FlowMode.WRITE):
    drive_id = drive.add("track01.mp3", ROOT, data)
    result = fp.identify_library_file(
        drive,
        api,
        FakeIdentifier(score=0.0, metadata={}),
        _record(drive, mode),
        file_id=drive_id,
        root=ROOT,
        path="New/track01.mp3",
        threshold=0.9,
    )
    return drive_id, result


def test_with_no_fingerprint_match_its_own_tags_make_its_entry(drive, api) -> None:
    drive_id, result = _no_fingerprint(
        drive, api, _song("Beast Mode", "Rokka", year="2023", bpm="120")
    )

    assert result.outcome == "linked_by_tags" and result.entry_created
    [made] = api.created
    assert (made.title, made.artist, made.release_year) == ("Beast Mode", "Rokka", 2023)
    [row] = api.registrations
    assert row.identification_status == "identified"
    assert row.catalog_id == uuid.UUID(result.catalog_id)
    assert row.identify_note == (
        "identified by its own tags (fingerprint: no confident match (score 0.00))"
    )
    assert _frames(drive.items[drive_id].data)["TXXX:CATALOG_ID"] == result.catalog_id


def test_with_no_fingerprint_match_tags_the_catalog_knows_link_to_it(
    drive, api
) -> None:
    api.known["temple"] = _match()

    _, result = _no_fingerprint(drive, api, _song())

    assert result.outcome == "linked_by_tags" and not result.entry_created
    assert result.catalog_id == str(_ENTRY)
    assert api.created == []


def test_tags_that_name_no_song_are_left_for_the_owner(drive, api) -> None:
    _, result = _no_fingerprint(drive, api, _song("Beast Mode", "Unknown Artist"))

    assert result.outcome == "below_confidence"
    assert api.created == []
    [note] = api.registrations
    assert note.identification_status == "unidentified"


def test_a_dry_run_names_the_entry_its_tags_would_make(drive, api) -> None:
    _, result = _no_fingerprint(
        drive, api, _song("Beast Mode", "Rokka"), FlowMode.DRY_RUN
    )

    assert result.outcome == "would_create_entry_from_tags"
    assert api.created == [] and api.registrations == []


@pytest.mark.parametrize(
    ("title", "artist", "usable"),
    [
        ("Beast Mode", "Rokka", True),
        ("Beast Mode", "Various Artists", False),
        ("Beast Mode", "unknown", False),
        ("Track 3", "Rokka", False),
        ("Untitled", "Rokka", False),
        ("", "Rokka", False),
        ("7 Rings", "Ariana Grande", True),
    ],
)
def test_usable_tags_name_a_song(title: str, artist: str, usable: bool) -> None:
    tags = {"title": title, "artist": artist, "year": "", "bpm": "", "genre": ""}
    assert (ident.usable_track(tags) is not None) is usable


def test_a_dry_run_unconfident_fingerprint_records_nothing(drive, api) -> None:
    drive_id = drive.add("track01.mp3", ROOT, _no_tags())

    result = fp.identify_library_file(
        drive,
        api,
        FakeIdentifier(score=0.5),
        _record(drive, FlowMode.DRY_RUN),
        file_id=drive_id,
        root=ROOT,
        path="New/track01.mp3",
        threshold=0.9,
    )

    assert result.outcome == "below_confidence"
    assert api.registrations == [] and api.created == []


def test_a_dry_run_fingerprint_makes_no_entry(drive, api) -> None:
    drive_id = drive.add("track01.mp3", ROOT, _no_tags())
    api.known.clear()

    result = fp.identify_library_file(
        drive,
        api,
        FakeIdentifier(),
        _record(drive, FlowMode.DRY_RUN),
        file_id=drive_id,
        root=ROOT,
        path="New/track01.mp3",
        threshold=0.9,
    )

    assert result.outcome == "would_create_entry"
    assert api.created == [] and api.registrations == []


def test_a_library_identify_message_runs_the_library_flow(monkeypatch) -> None:
    seen: dict[str, Any] = {}
    monkeypatch.setattr(
        fp, "library_identify_by_fingerprint", lambda *a, **kw: seen.update(kw, args=a)
    )
    body = FingerprintJobMessage(
        payload={
            "drive_file_id": "f1",
            "name": "track01.mp3",
            "flow": "library-identify",
            "mode": "write",
            "library_root_id": ROOT,
            "path": "New/track01.mp3",
        }
    ).model_dump_json()

    fp.process_message(body, run_id="m-1")

    assert seen["args"] == ("f1", "track01.mp3")
    assert (seen["root"], seen["path"], seen["sender_mode"]) == (
        ROOT,
        "New/track01.mp3",
        FlowMode.WRITE,
    )


def test_the_library_flow_follows_the_stricter_mode(monkeypatch, drive, api) -> None:
    monkeypatch.setenv(ident.MODE_VAR, "off")

    result = fp.library_identify_by_fingerprint(
        "f1", root=ROOT, path="p", sender_mode=FlowMode.WRITE, drive=drive, api=api
    )

    assert result is None and drive.calls == []


def test_the_report_counts_fingerprinted_files_still_unidentified(
    settings, wired, drive, api, jobs, monkeypatch
) -> None:
    linked = _unidentified(drive, api, "a.mp3", _no_tags())
    _unidentified(drive, api, "b.mp3", _no_tags())
    ident.library_identify(run_id="i-1")
    assert len(jobs.sent) == 2
    # The fingerprint function linked one of the two: only the other is
    # among the unidentified rows.
    api.rows = [r for r in api.rows if r.drive_file_id != linked]

    summary = ident.library_identify(run_id="i-2")

    assert summary.fingerprinted_unlinked == 1 and summary.waiting == 0


def test_a_fingerprint_run_that_links_its_file_is_not_sent(
    settings, drive, api, posted
) -> None:
    drive_id = drive.add("track01.mp3", ROOT, _no_tags())

    result = fp.library_identify_by_fingerprint(
        drive_id,
        root=ROOT,
        path="New/track01.mp3",
        sender_mode=FlowMode.WRITE,
        drive=drive,
        api=api,
        identifier=FakeIdentifier(),
    )

    assert result is not None and result.outcome == "linked"
    kwargs = posted.call_args.kwargs
    assert kwargs["notable"] is False
    assert posted.call_args.args[1] == "SUCCESS"


def test_a_fingerprint_that_names_another_song_than_the_tags_is_held(
    drive, api
) -> None:
    drive_id = drive.add("Beggin_Madcon.mp3", ROOT, _song("Beggin", "Madcon"))
    api.known.clear()

    result = fp.identify_library_file(
        drive,
        api,
        FakeIdentifier(metadata={"title": "Don't Worry", "artist": "Madcon"}),
        _record(drive),
        file_id=drive_id,
        root=ROOT,
        path="Beggin_Madcon.mp3",
        threshold=0.9,
    )

    assert result.outcome == fp.DISAGREES_WITH_TAGS
    assert drive.writes(except_folder=STATE) == []
    assert api.created == []
    [note] = api.registrations
    assert note.identification_status == "unidentified"
    assert "disagrees with the file's own tags (Madcon - Beggin)" in (
        note.identify_note or ""
    )


def test_a_fingerprint_that_agrees_with_the_tags_is_linked(drive, api) -> None:
    drive_id = drive.add(
        "temple.mp3", ROOT, _song("Temple (Radio Edit)", "Thomas Gold feat. X")
    )
    api.known["temple"] = _match()

    result = fp.identify_library_file(
        drive,
        api,
        FakeIdentifier(),
        _record(drive),
        file_id=drive_id,
        root=ROOT,
        path="temple.mp3",
        threshold=0.9,
    )

    assert result.outcome == "linked"
