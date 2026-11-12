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
from audio_fixtures import NAMES, OTHER_FORMATS, tagged
from mini_app_polis import pipeline_status
from mini_app_polis.api import OtherLibraryError
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
from deejay_cog.audio_formats import other_format
from deejay_cog.fingerprint_jobs import FingerprintJobMessage, FingerprintJobs
from deejay_cog.music_safety import FlowMode, RunRecord
from deejay_cog.music_tags import linked_catalog_id, read_tag_row
from deejay_cog.second_opinion import Heard, Recording
from flag_fakes import FLAGS

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
        rows = [r for r in self.rows if r.library_root_id == kwargs["library_root_id"]]
        start = kwargs["offset"]
        return rows[start : start + kwargs["limit"]]

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
    monkeypatch.setitem(FLAGS, ident.MODE_FLAG, "write")
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


def _unidentified(
    drive: FakeDrive, api: FakeApi, name: str, data: bytes, root: str = ROOT
) -> str:
    drive_id = drive.add(name, root, data)
    api.rows.append(
        LibraryFile(
            id=uuid.uuid4(),
            drive_file_id=drive_id,
            library_root_id=root,
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
    monkeypatch.delitem(FLAGS, ident.MODE_FLAG, raising=False)
    _unidentified(drive, api, "temple.mp3", _song())

    summary = ident.library_identify(run_id="i-1")

    assert summary.mode is FlowMode.OFF
    assert drive.calls == []
    assert api.registrations == [] and jobs.sent == []
    posted.assert_not_called()


def test_a_file_the_catalog_knows_is_sent_to_the_fingerprint_not_linked(
    settings, wired, drive, api, jobs
) -> None:
    data = _song("temple", "thomas gold", genre="House", bpm="125")
    drive_id = _unidentified(drive, api, "temple.mp3", data)

    summary = ident.library_identify(run_id="i-1")

    # Its tags are a starting point: nothing is linked or written here.
    assert summary.linked == [] and summary.written == []
    assert drive.items[drive_id].data == data
    assert api.fills == []
    [job] = jobs.sent
    assert job["file_id"] == drive_id and job["flow"] == "library-identify"
    assert job["expected_catalog_id"] == str(_ENTRY)
    assert (job["library_root_id"], job["path"], job["mode"]) == (
        ROOT,
        "New/temple.mp3",
        "write",
    )
    [note] = api.registrations
    assert note.identification_status == "unidentified" and note.catalog_id is None
    assert note.identify_note == (
        "its tags match Thomas Gold - Temple in the catalog; sent to "
        "fingerprinting to confirm it"
    )
    assert summary.to_fingerprint == [
        "New/temple.mp3 (its tags match Thomas Gold - Temple; the fingerprint "
        "is to confirm it)"
    ]
    assert _state(drive, ident.TRIED_NAME)["files"][drive_id]["outcome"] == (
        "sent_to_fingerprint"
    )


def test_a_tag_match_sent_to_the_fingerprint_is_not_sent_again(
    settings, wired, drive, api, jobs
) -> None:
    _unidentified(drive, api, "temple.mp3", _song())
    ident.library_identify(run_id="i-1")

    summary = ident.library_identify(run_id="i-2")

    assert summary.tried == 0 and len(jobs.sent) == 1


def test_a_file_already_identified_is_not_sent(
    settings, wired, drive, api, jobs
) -> None:
    _unidentified(drive, api, "temple.mp3", _song())
    api.rows[0] = api.rows[0].model_copy(update={"identification_status": "identified"})
    api.list_library_files = lambda **kw: [  # type: ignore[method-assign]
        r for r in api.rows if r.identification_status == kw["identification_status"]
    ]

    summary = ident.library_identify(run_id="i-1")

    assert summary.tried == 0 and jobs.sent == []


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


def test_a_file_held_before_the_second_opinion_is_tried_once_more(
    settings, wired, drive, api, jobs
) -> None:
    drive_id = _unidentified(drive, api, "Beggin_Madcon.mp3", _song("Beggin", "Madcon"))
    held = _unidentified(drive, api, "x.mp3", _song("Sunny", "Boogie Pimps"))
    api.known.clear()
    api.rows[0] = api.rows[0].model_copy(
        update={
            "identify_note": "fingerprint: Madcon - Don't Worry (score 0.95) "
            "disagrees with the file's own tags (Madcon - Beggin); left for review"
        }
    )
    api.rows[1] = api.rows[1].model_copy(
        update={"identify_note": "fingerprint: no match"}
    )
    state = {
        "flow": ident.FLOW_NAME,
        "files": {
            drive_id: {"outcome": "sent_to_fingerprint", "at": "2026-11-08"},
            held: {"outcome": "sent_to_fingerprint", "at": "2026-11-08"},
        },
    }
    backfill.StateFile(drive, STATE, ident.TRIED_NAME).save(state)

    first = ident.library_identify(run_id="i-1")
    second = ident.library_identify(run_id="i-2")

    assert first.tried == 1 and [j["file_id"] for j in jobs.sent] == [drive_id]
    tried = _state(drive, ident.TRIED_NAME)["files"]
    assert tried[drive_id]["second_opinion"] is True
    assert "second_opinion" not in tried[held]
    assert second.tried == 0


def test_a_file_whose_second_opinion_could_not_ask_spotify_is_tried_again(
    settings, wired, drive, api, jobs
) -> None:
    drive_id = _unidentified(drive, api, "Demons.mp3", _song("Demons", "Alonzo Holt"))
    api.known.clear()
    api.rows[0] = api.rows[0].model_copy(
        update={
            "identify_note": "fingerprint: Kurt Hugo Schneider - Demons (score 0.98) "
            "disagrees with the file's own tags (Alonzo Holt - Demons); "
            "no second opinion: Spotify is not available; left for review"
        }
    )
    state = {
        "flow": ident.FLOW_NAME,
        "files": {
            drive_id: {
                "outcome": "sent_to_fingerprint",
                "at": "2026-11-09",
                "second_opinion": True,
            },
        },
    }
    backfill.StateFile(drive, STATE, ident.TRIED_NAME).save(state)

    summary = ident.library_identify(run_id="i-1")

    assert summary.tried == 1 and [j["file_id"] for j in jobs.sent] == [drive_id]


def test_a_file_whose_second_opinion_backed_neither_is_not_tried_again(
    settings, wired, drive, api, jobs
) -> None:
    drive_id = _unidentified(drive, api, "Waves.mp3", _song("Waves", "Dean Lewis"))
    api.known.clear()
    api.rows[0] = api.rows[0].model_copy(
        update={
            "identify_note": "fingerprint: Dean Lewis - Waves (acoustic) (score 0.97) "
            "disagrees with the file's own tags (Dean Lewis - Waves); "
            "Spotify has neither song at the file's length (4:08); left for review"
        }
    )
    state = {
        "flow": ident.FLOW_NAME,
        "files": {
            drive_id: {
                "outcome": "sent_to_fingerprint",
                "at": "2026-11-09",
                "second_opinion": True,
            },
        },
    }
    backfill.StateFile(drive, STATE, ident.TRIED_NAME).save(state)

    summary = ident.library_identify(run_id="i-1")

    assert summary.tried == 0 and jobs.sent == []


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
    monkeypatch.setitem(FLAGS, ident.MODE_FLAG, "dry-run")
    known = _unidentified(drive, api, "temple.mp3", _song(genre="House"))
    _unidentified(drive, api, "track01.mp3", _no_tags())
    before = drive.items[known].data

    first = ident.library_identify(run_id="i-1")
    second = ident.library_identify(run_id="i-2")

    assert first.would_write == [] and first.linked == []
    assert len(first.to_fingerprint) == 2 and second.tried == 2
    assert drive.items[known].data == before
    assert drive.writes(except_folder=STATE) == []
    assert api.fills == [] and api.registrations == [] and jobs.sent == []
    assert _state(drive, ident.TRIED_NAME) is None
    assert "would_fingerprint" in _text(posted)


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

    # An owner-edit entry its tags find is linked here, so it is written.
    api.known["temple"] = _match().model_copy(update={"owner_edit": True})
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
    #: What AcoustID heard, for a second opinion; None: nothing heard.
    heard: Heard | None = None

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


@pytest.mark.parametrize(
    ("title", "name", "usable"),
    [
        ("FindAWayBayje112 Bayje 112", "Pop/FindAWayBayje112_Bayje_112.mp3", False),
        ("Find A Way", "Pop/FindAWayBayje112_Bayje_112.mp3", True),
        ("Happy", "Pop/Happy.mp3", True),
    ],
)
def test_a_title_that_is_only_the_file_name_names_no_song(
    title: str, name: str, usable: bool
) -> None:
    tags = {"title": title, "artist": "Bayje", "year": "", "bpm": "", "genre": ""}
    assert (ident.usable_track(tags, name) is not None) is usable


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
    monkeypatch.setitem(FLAGS, ident.MODE_FLAG, "off")

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


def _disagreeing(drive, api, heard: Heard | None, search: Any = None):
    drive_id = drive.add("Beggin_Madcon.mp3", ROOT, _song("Beggin", "Madcon"))
    api.known.clear()
    result = fp.identify_library_file(
        drive,
        api,
        FakeIdentifier(
            metadata={"title": "Don't Worry", "artist": "Madcon"}, heard=heard
        ),
        _record(drive),
        file_id=drive_id,
        root=ROOT,
        path="Beggin_Madcon.mp3",
        threshold=0.9,
        search=search,
    )
    return drive_id, result


@dataclass
class FakeSearch:
    tracks_by: dict[str, list[Recording]] = field(default_factory=dict)

    def tracks(self, artist: str, title: str) -> list[Recording]:
        del artist  # the fake answers by title
        return self.tracks_by.get(title, [])


def test_another_acoustid_recording_of_its_song_links_it_by_its_tags(
    drive, api
) -> None:
    heard = Heard(
        214.0, (Recording("Don't Worry", "Madcon"), Recording("Beggin'", "Madcon"))
    )

    drive_id, result = _disagreeing(drive, api, heard)

    assert result.outcome == "linked_by_tags" and result.entry_created
    [made] = api.created
    assert (made.title, made.artist) == ("Beggin", "Madcon")
    [row] = api.registrations
    assert row.identification_status == "identified"
    assert "AcoustID also links this audio to Madcon - Beggin'" in (
        row.identify_note or ""
    )
    assert _frames(drive.items[drive_id].data)["TXXX:CATALOG_ID"] == result.catalog_id


def test_spotify_at_the_files_length_can_back_the_fingerprint(drive, api) -> None:
    search = FakeSearch({"Don't Worry": [Recording("Don't Worry", "Madcon", 214.5)]})

    _, result = _disagreeing(drive, api, Heard(214.0), search)

    assert result.outcome == "linked" and result.entry_created
    [made] = api.created
    assert (made.title, made.artist) == ("Don't Worry", "Madcon")


def test_a_second_opinion_that_backs_neither_side_leaves_it_for_the_owner(
    drive, api
) -> None:
    _, result = _disagreeing(drive, api, Heard(214.0), FakeSearch())

    assert result.outcome == fp.DISAGREES_WITH_TAGS
    assert drive.writes(except_folder=STATE) == []
    [note] = api.registrations
    assert note.identify_note == (
        "fingerprint: Madcon - Don't Worry (score 0.95) disagrees with the file's "
        "own tags (Madcon - Beggin); Spotify has neither song at the file's length "
        "(3:34); left for review"
    )


def test_a_spotify_client_that_cannot_start_is_no_second_opinion(monkeypatch) -> None:
    from deejay_cog import spotify_sync

    class _Broken:
        @property
        def client(self):
            raise ValueError("Missing one or more required Spotify credentials.")

    monkeypatch.setattr(spotify_sync, "get_spotify_client", lambda: _Broken())

    assert fp.build_track_search() is None


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


# ---------------------------------------------------------------------------
# The fingerprint checks a tag match before it is linked
# ---------------------------------------------------------------------------


def _confirm(
    drive,
    api,
    data: bytes,
    identifier: Any = None,
    *,
    mode: FlowMode = FlowMode.WRITE,
    search: Any = None,
    name: str = "temple.mp3",
):
    """The fingerprint run for a file whose tags found ``_ENTRY``."""
    drive_id = drive.add(name, ROOT, data)
    result = fp.identify_library_file(
        drive,
        api,
        identifier if identifier is not None else FakeIdentifier(),
        _record(drive, mode),
        file_id=drive_id,
        root=ROOT,
        path=f"New/{name}",
        threshold=0.9,
        search=search,
        expected=str(_ENTRY),
    )
    return drive_id, result


def test_a_fingerprint_that_confirms_the_tag_match_links_it_written_and_registered(
    drive, api
) -> None:
    drive_id, result = _confirm(
        drive, api, _song("temple", "thomas gold", genre="House", bpm="125")
    )

    assert result.outcome == "linked" and result.catalog_id == str(_ENTRY)
    assert not result.entry_created and api.created == []
    frames = _frames(drive.items[drive_id].data)
    # Only the link is written; every owned field is the reconcile's.
    assert frames["TIT2"] == "temple"
    assert frames["TPE1"] == "thomas gold"
    assert frames["TCON"] == "House"
    assert frames["TBPM"] == "125"
    assert frames["TXXX:CATALOG_ID"] == str(_ENTRY)
    assert frames["COMM::eng"] == "cue: drop at 1:02"  # the comment is kept
    assert not any(k.startswith("TXXX:MusicBrainz") for k in frames)
    # Fill-up first, with the file's own values.
    [(catalog_id, fill)] = api.fills
    assert catalog_id == str(_ENTRY) and fill.genre == "House" and fill.bpm == 125.0
    [reg] = api.registrations
    assert reg.identification_status == "identified" and reg.catalog_id == _ENTRY
    assert reg.identify_note == (
        "identified by its own tags, confirmed by the fingerprint (score 0.95)"
    )
    assert reg.comment_backup == "cue: drop at 1:02"
    assert reg.synced_at is not None and reg.md5 is not None
    # Written safely: revision kept, run record saved; never renamed or moved.
    assert drive.items[drive_id].kept
    record = _state(drive, "library-identify-m-1.json")
    assert record["entries"][0]["action"] == "in_place"
    assert drive.items[drive_id].name == "temple.mp3"
    assert drive.items[drive_id].parents == [ROOT]


def test_a_confirmed_link_writes_only_the_link_when_the_owned_fields_differ(
    drive, api
) -> None:
    song = _song("temple", "thomas gold", genre="House", bpm="140")
    before = _frames(song)
    api.displays[_ENTRY] = _display(artist="Thomas  GOLD", release_year=2021, bpm=125.5)

    drive_id, result = _confirm(drive, api, song)

    assert result.written
    after = _frames(drive.items[drive_id].data)
    assert after.pop("TXXX:CATALOG_ID") == str(_ENTRY)
    assert after == before
    assert len(api.fills) == 1 and len(api.registrations) == 1


def test_a_confirmed_files_own_release_label_stays_in_its_title(drive, api) -> None:
    drive_id, result = _confirm(
        drive, api, _song("Temple (No Intro Edit)", "Thomas Gold")
    )

    assert result.outcome == "linked"
    assert _frames(drive.items[drive_id].data)["TIT2"] == "Temple (No Intro Edit)"
    assert api.registrations[0].edit_label == "No Intro Edit"


def test_a_recording_the_catalog_holds_as_the_tag_match_confirms_it(drive, api) -> None:
    # Credited differently, so the tags and the recording do not agree by
    # name; the catalog names the recording as the very entry the tags found.
    identifier = FakeIdentifier(metadata={"title": "Temple", "artist": "T. Gold"})

    _, result = _confirm(drive, api, _song("Temple", "Thomas Gold"), identifier)

    assert result.outcome == "linked" and result.catalog_id == str(_ENTRY)
    assert api.registrations[0].identify_note == (
        "identified by its own tags, confirmed by the fingerprint (score 0.95)"
    )


@pytest.mark.parametrize(
    ("identifier", "why"),
    [
        (
            FakeIdentifier(score=0.0, metadata={}),
            "fingerprint: no confident match (score 0.00)",
        ),
        (
            FakeIdentifier(score=0.5),
            "fingerprint: no confident match (best guess Thomas Gold - Temple, "
            "score 0.50)",
        ),
        (
            FakeIdentifier(
                metadata={
                    "_provider": "musicbrainz",
                    "_mbid": "mbid-1",
                    fp.NOT_SERVED: True,
                }
            ),
            "fingerprint: matched recording mbid-1 (score 0.95), which MusicBrainz "
            "does not serve",
        ),
    ],
)
def test_no_fingerprint_result_links_the_tag_match_by_its_tags(
    drive, api, identifier, why: str
) -> None:
    drive_id, result = _confirm(drive, api, _song(), identifier)

    assert result.outcome == fp.LINKED_BY_TAGS and result.catalog_id == str(_ENTRY)
    assert api.created == []
    [reg] = api.registrations
    assert reg.identification_status == "identified" and reg.catalog_id == _ENTRY
    assert reg.identify_note == f"identified by its own tags ({why})"
    assert _frames(drive.items[drive_id].data)["TXXX:CATALOG_ID"] == str(_ENTRY)


def test_unreadable_audio_links_the_tag_match_by_its_tags(drive, api) -> None:
    class Unreadable:
        def identify(self, path: str, *, fetch_metadata: bool = True):
            del path, fetch_metadata
            raise fp.AudioUnreadable("fpcalc exited with status 3: ERROR")

    _, result = _confirm(drive, api, _song(), Unreadable())

    assert result.outcome == fp.LINKED_BY_TAGS and result.catalog_id == str(_ENTRY)
    [reg] = api.registrations
    assert reg.catalog_id == _ENTRY
    assert reg.identify_note == (
        "identified by its own tags (fingerprint: the audio could not be read "
        "(fpcalc exited with status 3: ERROR))"
    )


def _another_song(drive, api, heard: Heard | None, search: Any = None):
    """A file whose tags found ``_ENTRY`` (Madcon - Beggin) and whose
    fingerprint names another song."""
    api.known.clear()
    api.known["beggin"] = CatalogMatch(id=_ENTRY, title="Beggin", artist="Madcon")
    return _confirm(
        drive,
        api,
        _song("Beggin", "Madcon"),
        FakeIdentifier(
            metadata={"title": "Don't Worry", "artist": "Madcon"}, heard=heard
        ),
        search=search,
        name="Beggin_Madcon.mp3",
    )


def test_a_confident_match_to_another_song_held_when_no_second_source_decides(
    drive, api
) -> None:
    _, result = _another_song(drive, api, Heard(214.0), FakeSearch())

    assert result.outcome == fp.DISAGREES_WITH_TAGS
    assert drive.writes(except_folder=STATE) == []
    [note] = api.registrations
    assert note.identification_status == "unidentified" and note.catalog_id is None
    assert "disagrees with the file's own tags (Madcon - Beggin)" in (
        note.identify_note or ""
    )


def test_a_second_opinion_for_the_tags_links_the_tag_match(drive, api) -> None:
    heard = Heard(
        214.0, (Recording("Don't Worry", "Madcon"), Recording("Beggin'", "Madcon"))
    )

    _, result = _another_song(drive, api, heard)

    assert result.outcome == fp.LINKED_BY_TAGS and result.catalog_id == str(_ENTRY)
    assert api.created == []
    [row] = api.registrations
    assert row.catalog_id == _ENTRY
    assert "AcoustID also links this audio to Madcon - Beggin'" in (
        row.identify_note or ""
    )


def test_a_second_opinion_for_the_fingerprint_links_its_song_with_why(
    drive, api
) -> None:
    search = FakeSearch({"Don't Worry": [Recording("Don't Worry", "Madcon", 214.5)]})

    _, result = _another_song(drive, api, Heard(214.0), search)

    assert result.outcome == "linked" and result.entry_created
    assert result.catalog_id != str(_ENTRY)
    [made] = api.created
    assert (made.title, made.artist) == ("Don't Worry", "Madcon")
    [row] = api.registrations
    assert (row.identify_note or "").startswith(
        "identified by its fingerprint (fingerprint: Madcon - Don't Worry"
    )


def test_a_dry_run_confirmation_changes_nothing(drive, api) -> None:
    data = _song()

    drive_id, result = _confirm(drive, api, data, mode=FlowMode.DRY_RUN)

    assert result.outcome == "linked" and result.would_write
    assert drive.items[drive_id].data == data
    assert api.registrations == [] and api.fills == [] and api.created == []


def test_a_message_carries_the_entry_the_tags_found(monkeypatch) -> None:
    seen: dict[str, Any] = {}
    monkeypatch.setattr(
        fp, "library_identify_by_fingerprint", lambda *a, **kw: seen.update(kw, args=a)
    )
    sqs = MagicMock()
    FingerprintJobs("https://sqs.test/q.fifo", sqs).send(
        "f1",
        "temple.mp3",
        flow="library-identify",
        mode="write",
        library_root_id=ROOT,
        path="New/temple.mp3",
        expected_catalog_id=str(_ENTRY),
    )

    fp.process_message(sqs.send_message.call_args.kwargs["MessageBody"], run_id="m-1")

    assert seen["expected"] == str(_ENTRY)


# ---------------------------------------------------------------------------
# Owner edits: a version in < >, set by the owner in the file's tags
# ---------------------------------------------------------------------------

_BABY = "Baby What You Want Me To Do"
_CUT = uuid.UUID("0e0e0e0e-0000-4000-8000-000000000002")


class OwnerEditApi(FakeApi):
    """A catalog whose new entries for a title in < > are owner edits."""

    def create_library_entry(self, payload: LibraryEntryCreate):
        self.created.append(payload)
        owner_edit = "<" in payload.title
        self.displays[_CUT] = _display(
            _CUT, title=payload.title, artist=payload.artist, owner_edit=owner_edit
        )
        return LibraryEntryCreated(
            entry=CatalogMatch(
                id=_CUT,
                title=payload.title,
                artist=payload.artist,
                owner_edit=owner_edit,
            ),
            created=not payload.preview,
            previewed=payload.preview,
        )


@pytest.fixture
def owner_edit_api(monkeypatch: pytest.MonkeyPatch, wired) -> OwnerEditApi:
    api = OwnerEditApi(
        known={
            _BABY.lower(): CatalogMatch(id=_ENTRY, title=_BABY, artist="Etta James")
        },
        displays={_ENTRY: _display(title=_BABY, artist="Etta James")},
    )
    monkeypatch.setattr(ident, "api_client", lambda **_: api)
    return api


def test_an_owner_edit_gets_an_owner_edit_entry_of_its_own(
    settings, owner_edit_api, drive, jobs
) -> None:
    api = owner_edit_api
    title = f"{_BABY} <Intro Cut>"
    drive_id = _unidentified(drive, api, "baby.mp3", _song(title, "Etta James"))

    summary = ident.library_identify(run_id="i-1")

    [made] = api.created
    assert made.title == title and made.artist == "Etta James"
    assert made.preview is False
    frames = _frames(drive.items[drive_id].data)
    assert frames["TXXX:CATALOG_ID"] == str(_CUT)  # not the published song's
    assert frames["TIT2"] == title
    assert frames["COMM::eng"] == "cue: drop at 1:02"
    [reg] = api.registrations
    assert reg.catalog_id == _CUT and reg.edit_label is None
    assert reg.identify_note == ident.OWNER_EDIT_NOTE
    # The owner's tags identify it; once linked, the fingerprint is asked
    # for a hint only (what it was cut from).
    [hint] = jobs.sent
    assert hint["file_id"] == drive_id and hint["flow"] == "library-identify"
    assert summary.owner_edit_hints == ["New/baby.mp3"]
    assert summary.owner_edits == [f"New/baby.mp3 -> Etta James - {title}"]
    assert _state(drive, ident.TRIED_NAME)["files"][drive_id]["outcome"] == "linked"


def test_an_owner_edit_is_linked_to_the_owner_edit_entry_the_catalog_has(
    settings, owner_edit_api, drive, jobs
) -> None:
    api = owner_edit_api
    title = f"{_BABY} <Intro Cut> (Clean)"
    existing = uuid.uuid4()
    api.known[title.lower()] = CatalogMatch(
        id=existing, title=f"{_BABY} <Intro Cut>", artist="Etta James", owner_edit=True
    )
    api.displays[existing] = _display(existing, owner_edit=True)
    drive_id = _unidentified(drive, api, "baby.mp3", _song(title, "Etta James"))

    ident.library_identify(run_id="i-1")

    assert api.created == []
    assert [s["file_id"] for s in jobs.sent] == [drive_id]  # a hint only
    assert jobs.sent[0].get("expected_catalog_id") is None
    assert _frames(drive.items[drive_id].data)["TXXX:CATALOG_ID"] == str(existing)
    [reg] = api.registrations
    assert reg.catalog_id == existing and reg.edit_label == "Clean"


def test_an_owner_edit_is_never_linked_to_the_published_songs_entry(
    settings, owner_edit_api, drive
) -> None:
    api = owner_edit_api
    title = f"{_BABY} <Intro Cut>"
    # A catalog answering with the published song is not taken.
    api.known[title.lower()] = api.known[_BABY.lower()]
    drive_id = _unidentified(drive, api, "baby.mp3", _song(title, "Etta James"))

    ident.library_identify(run_id="i-1")

    assert _frames(drive.items[drive_id].data)["TXXX:CATALOG_ID"] == str(_CUT)


def test_a_dry_run_previews_an_owner_edit_entry_and_changes_nothing(
    settings, owner_edit_api, drive, jobs, monkeypatch
) -> None:
    monkeypatch.setitem(FLAGS, ident.MODE_FLAG, "dry-run")
    api = owner_edit_api
    data = _song(f"{_BABY} <Intro Cut>", "Etta James")
    drive_id = _unidentified(drive, api, "baby.mp3", data)

    summary = ident.library_identify(run_id="i-1")

    assert [c.preview for c in api.created] == [True]
    assert drive.items[drive_id].data == data
    assert api.registrations == [] and jobs.sent == []
    assert len(summary.owner_edits) == 1


def test_an_unmarked_file_is_sent_to_confirm_the_published_song(
    settings, owner_edit_api, drive, jobs
) -> None:
    api = owner_edit_api
    _unidentified(drive, api, "baby.mp3", _song(_BABY, "Etta James"))

    summary = ident.library_identify(run_id="i-1")

    assert api.created == [] and summary.owner_edits == []
    [job] = jobs.sent
    assert job["expected_catalog_id"] == str(_ENTRY)


# ---------------------------------------------------------------------------
# FX Music: its files link only to fx entries, and main files never do
# ---------------------------------------------------------------------------

FX = "fx-root"
_FX_ENTRY = uuid.UUID("0f0f0f0f-0000-4000-8000-000000000003")


class FxApi(FakeApi):
    """A catalog that keeps fx entries apart: a new entry is of the library
    asked for, and a link across libraries is refused."""

    fx_known: dict[str, uuid.UUID]

    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.fx_known = {}
        self.library_of: dict[uuid.UUID, str] = {_ENTRY: "main"}

    def create_library_entry(self, payload: LibraryEntryCreate):
        self.created.append(payload)
        if payload.library != "fx":
            return super().create_library_entry(payload)
        key = payload.title.lower()
        existing = key in self.fx_known
        entry = self.fx_known.get(key, _FX_ENTRY)
        if not payload.preview:
            self.fx_known[key] = entry
            self.library_of[entry] = "fx"
            self.displays[entry] = _display(
                entry, title=payload.title, artist=payload.artist
            )
        return LibraryEntryCreated(
            entry=CatalogMatch(
                id=entry, title=payload.title, artist=payload.artist, library="fx"
            ),
            created=not existing and not payload.preview,
            previewed=not existing and payload.preview,
        )

    def register_library_file(self, payload: LibraryFileRegister):
        if payload.catalog_id is not None:
            library = self.library_of.get(payload.catalog_id, "main")
            root_library = "fx" if payload.library_root_id == FX else "main"
            if library != root_library:
                raise OtherLibraryError(
                    status_code=409, message="other_library", path="/files"
                )
        return super().register_library_file(payload)


@pytest.fixture
def fx_api(monkeypatch: pytest.MonkeyPatch, wired) -> FxApi:
    monkeypatch.setenv(backfill.FX_ROOT_FOLDER_VAR, FX)
    api = FxApi(known={"temple": _match()}, displays={_ENTRY: _display()})
    monkeypatch.setattr(ident, "api_client", lambda **_: api)
    return api


def test_an_fx_file_gets_an_fx_entry_even_when_its_tags_match_a_main_entry(
    settings, fx_api, drive, jobs
) -> None:
    api = fx_api
    # "temple" is a main-library song the lookup knows.
    drive_id = _unidentified(drive, api, "temple.mp3", _song(), root=FX)

    summary = ident.library_identify(run_id="i-1")

    [made] = api.created
    assert made.library == "fx" and made.title == "temple"
    frames = _frames(drive.items[drive_id].data)
    assert frames["TXXX:CATALOG_ID"] == str(_FX_ENTRY)  # not the main entry's
    # Only the link is written: the FX file's own tags win (ADR-0010).
    assert frames["TIT2"] == "temple" and frames["TPE1"] == "thomas gold"
    [reg] = api.registrations
    assert reg.catalog_id == _FX_ENTRY and reg.library_root_id == FX
    assert reg.identify_note == ident.FX_NOTE
    assert jobs.sent == []  # never fingerprinted
    assert summary.fx_entries == ["New/temple.mp3 -> thomas gold - temple"]
    assert _state(drive, ident.TRIED_NAME)["files"][drive_id]["outcome"] == "linked"


def test_an_fx_file_links_to_the_fx_entry_the_catalog_already_has(
    settings, fx_api, drive
) -> None:
    api = fx_api
    api.fx_known["temple"] = _FX_ENTRY
    api.library_of[_FX_ENTRY] = "fx"
    api.displays[_FX_ENTRY] = _display(_FX_ENTRY)
    drive_id = _unidentified(drive, api, "temple.mp3", _song(), root=FX)

    summary = ident.library_identify(run_id="i-1")

    assert _frames(drive.items[drive_id].data)["TXXX:CATALOG_ID"] == str(_FX_ENTRY)
    assert summary.fx_entries == [] and len(summary.linked) == 1


def test_an_fx_file_the_fingerprint_would_match_is_never_fingerprinted(
    settings, fx_api, drive, jobs
) -> None:
    api = fx_api
    drive_id = _unidentified(drive, api, "clip.mp3", _no_tags(), root=FX)

    summary = ident.library_identify(run_id="i-1")

    assert jobs.sent == [] and api.created == []
    assert "TXXX:CATALOG_ID" not in _frames(drive.items[drive_id].data)
    [reg] = api.registrations
    assert reg.identification_status == "unidentified" and "FX Music" in (
        reg.identify_note or ""
    )
    assert summary.skipped


@pytest.mark.parametrize("fmt", OTHER_FORMATS, ids=lambda f: f.value)
def test_an_fx_file_in_any_supported_format_is_read_and_linked(
    settings, fx_api, drive, jobs, fmt
) -> None:
    api = fx_api
    data = tagged(fmt, title="temple", artist="thomas gold", year="2016")
    drive_id = _unidentified(drive, api, NAMES[fmt], data, root=FX)

    ident.library_identify(run_id="i-1")

    [made] = api.created
    assert (made.library, made.title, made.release_year) == ("fx", "temple", 2016)
    written = drive.items[drive_id].data
    assert linked_catalog_id(written) == str(_FX_ENTRY)
    # Only the link: the file keeps its format and its own tags.
    assert other_format(written) is fmt
    assert read_tag_row(written) == read_tag_row(data)
    [reg] = api.registrations
    assert reg.catalog_id == _FX_ENTRY and reg.duration_secs
    assert jobs.sent == []


def test_an_fx_file_the_shared_tagger_cannot_tag_is_left_for_the_owner(
    settings, fx_api, drive, jobs
) -> None:
    api = fx_api
    broken = b"fLaC" + b"\x00" * 40
    drive_id = _unidentified(drive, api, "bow.flac", broken, root=FX)

    summary = ident.library_identify(run_id="i-1")

    assert drive.items[drive_id].data == broken
    assert api.created == [] and jobs.sent == []
    [reg] = api.registrations
    assert "cannot tag" in (reg.identify_note or "")
    assert _state(drive, ident.TRIED_NAME)["files"][drive_id]["outcome"] == (
        "fx_untaggable"
    )
    assert summary.skipped


def test_a_dry_run_names_the_fx_entry_it_would_make_and_changes_nothing(
    settings, fx_api, drive, jobs, posted, monkeypatch
) -> None:
    monkeypatch.setitem(FLAGS, ident.MODE_FLAG, "dry-run")
    api = fx_api
    data = _song()
    drive_id = _unidentified(drive, api, "temple.mp3", data, root=FX)

    summary = ident.library_identify(run_id="i-1")

    assert [(c.library, c.preview) for c in api.created] == [("fx", True)]
    assert drive.items[drive_id].data == data
    assert api.registrations == [] and jobs.sent == []
    assert summary.fx_entries == ["New/temple.mp3 -> thomas gold - temple"]
    assert "would_make_fx_entry" in _text(posted)


def test_the_report_names_fx_files_given_new_entries(
    settings, fx_api, drive, posted
) -> None:
    _unidentified(drive, fx_api, "temple.mp3", _song(), root=FX)

    ident.library_identify(run_id="i-1")

    assert "New/temple.mp3 -> thomas gold - temple" in _text(posted)


def test_a_main_file_never_links_to_an_fx_entry(settings, fx_api, drive, jobs) -> None:
    api = fx_api
    # Were the lookup ever to answer with an fx entry, it is not taken.
    api.known["temple"] = CatalogMatch(
        id=_FX_ENTRY, title="Temple", artist="Thomas Gold", library="fx"
    )
    drive_id = _unidentified(drive, api, "temple.mp3", _song())

    ident.library_identify(run_id="i-1")

    assert "TXXX:CATALOG_ID" not in _frames(drive.items[drive_id].data)
    assert [s["file_id"] for s in jobs.sent] == [drive_id]


def test_main_and_fx_files_each_link_within_their_own_library(
    settings, fx_api, drive, jobs
) -> None:
    api = fx_api
    main_id = _unidentified(drive, api, "temple.mp3", _song())
    fx_id = _unidentified(drive, api, "walkon.mp3", _song(), root=FX)

    ident.library_identify(run_id="i-1")

    # The main file's tag match goes to the fingerprint to confirm; the FX
    # file is never fingerprinted and links to its fx entry.
    [job] = jobs.sent
    assert job["file_id"] == main_id and job["expected_catalog_id"] == str(_ENTRY)
    assert _frames(drive.items[fx_id].data)["TXXX:CATALOG_ID"] == str(_FX_ENTRY)
    assert {(r.library_root_id, r.catalog_id) for r in api.registrations} == {
        (ROOT, None),
        (FX, _FX_ENTRY),
    }


def test_a_link_the_catalog_refuses_as_the_other_librarys_is_reported_not_raised(
    settings, fx_api, drive, posted
) -> None:
    api = fx_api
    # A main-library entry the catalog holds as fx: the link is refused. An
    # owner-edit entry, so this step links it rather than the fingerprint.
    api.library_of[_ENTRY] = "fx"
    api.known["temple"] = _match().model_copy(update={"owner_edit": True})
    _unidentified(drive, api, "temple.mp3", _song())
    other = _unidentified(drive, api, "clip.mp3", _song(), root=FX)

    summary = ident.library_identify(run_id="i-1")

    assert summary.not_linkable and summary.not_linkable[0].startswith("New/temple.mp3")
    assert summary.failed == [] and not summary.aborted
    # The run carries on to the next file.
    assert _frames(drive.items[other].data)["TXXX:CATALOG_ID"] == str(_FX_ENTRY)
    assert "not_linkable" in _text(posted)


def test_the_fingerprint_reports_a_link_to_the_other_library_as_not_linkable(
    drive,
) -> None:
    api = FxApi(known={"temple": _match()}, displays={_ENTRY: _display()})
    api.library_of[_ENTRY] = "fx"
    drive_id = drive.add("temple.mp3", ROOT, _no_tags())
    identifier = FakeIdentifier()

    result = fp.identify_library_file(
        drive,
        api,
        identifier,
        _record(drive),
        file_id=drive_id,
        root=ROOT,
        path="temple.mp3",
        threshold=0.5,
    )

    assert result.outcome == fp.NOT_LINKABLE


# ---------------------------------------------------------------------------
# Owner edits: the fingerprint is a hint at what the edit was cut from
# ---------------------------------------------------------------------------


def _linked_edit(drive: FakeDrive, title: str = f"{_BABY} <Intro Cut>") -> str:
    from mutagen.id3 import TXXX

    data = mp3(
        TIT2(encoding=3, text=[title]),
        TPE1(encoding=3, text=["Etta James"]),
        TXXX(encoding=3, desc="CATALOG_ID", text=[str(_CUT)]),
    )
    return drive.add("baby.mp3", ROOT, data)


class SpySearch:
    """A Spotify search that must never be asked."""

    def tracks(self, artist: str, title: str) -> list[Recording]:
        raise AssertionError(f"Spotify asked for {artist} - {title}")


@pytest.fixture
def no_second_opinion(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("an owner edit was given a second opinion")

    monkeypatch.setattr(fp, "second_opinion", refuse)


def _hint(drive, api, identifier, mode: FlowMode = FlowMode.WRITE):
    drive_id = _linked_edit(drive)
    before = drive.items[drive_id].data
    result = fp.identify_library_file(
        drive,
        api,
        identifier,
        _record(drive, mode),
        file_id=drive_id,
        root=ROOT,
        path="Blues/baby.mp3",
        threshold=0.9,
        search=SpySearch(),
    )
    # Never retitled, relinked or written in any way.
    assert drive.items[drive_id].data == before
    assert drive.writes(except_folder=STATE) == []
    return drive_id, result


def _baby_api() -> FakeApi:
    return FakeApi(
        known={
            _BABY.lower(): CatalogMatch(id=_ENTRY, title=_BABY, artist="Etta James")
        },
        displays={_ENTRY: _display(title=_BABY, artist="Etta James")},
    )


def test_an_owner_edits_fingerprint_records_the_published_song_it_was_cut_from(
    drive, no_second_opinion
) -> None:
    api = _baby_api()
    identifier = FakeIdentifier(metadata={"title": _BABY, "artist": "Etta James"})

    drive_id, result = _hint(drive, api, identifier)

    assert result.outcome == fp.OWNER_EDIT_CUT_FROM
    assert result.catalog_id == str(_ENTRY)
    assert str(_ENTRY) in result.cut_from and _BABY in result.cut_from
    # Recorded on the file's registry row; its link and status unchanged.
    [reg] = api.registrations
    assert reg.drive_file_id == drive_id
    assert reg.catalog_id == _CUT and reg.identification_status == "identified"
    assert "cut from" in (reg.identify_note or "")
    assert str(_ENTRY) in (reg.identify_note or "")
    assert reg.synced_at is None and reg.tags is None
    # Nothing else changes: no entry made, nothing filled up.
    assert api.created == [] and api.fills == []


def test_an_owner_edit_with_no_fingerprint_match_changes_nothing(
    drive, no_second_opinion
) -> None:
    api = _baby_api()

    _, result = _hint(drive, api, FakeIdentifier(score=0.2))

    assert result.outcome == fp.OWNER_EDIT_NO_HINT
    # Not recorded as unidentified, not held for review.
    assert api.registrations == [] and api.created == []


def test_an_owner_edit_matched_to_another_recording_is_not_held_or_relinked(
    drive, no_second_opinion
) -> None:
    api = _baby_api()
    api.known["don't worry"] = _match()
    identifier = FakeIdentifier(metadata={"title": "Don't Worry", "artist": "Madcon"})

    _, result = _hint(drive, api, identifier)

    assert result.outcome == fp.OWNER_EDIT_NO_HINT
    assert api.registrations == [] and api.created == []


def test_an_owner_edit_matched_to_its_own_entry_is_no_hint(
    drive, no_second_opinion
) -> None:
    api = _baby_api()
    title = f"{_BABY} <Intro Cut>"
    api.known[_BABY.lower()] = CatalogMatch(
        id=_CUT, title=title, artist="Etta James", owner_edit=True
    )
    identifier = FakeIdentifier(metadata={"title": _BABY, "artist": "Etta James"})

    _, result = _hint(drive, api, identifier)

    assert result.outcome == fp.OWNER_EDIT_NO_HINT
    assert api.registrations == []


def test_a_dry_run_owner_edit_hint_records_nothing(drive, no_second_opinion) -> None:
    api = _baby_api()
    identifier = FakeIdentifier(metadata={"title": _BABY, "artist": "Etta James"})

    _, result = _hint(drive, api, identifier, FlowMode.DRY_RUN)

    assert result.outcome == fp.OWNER_EDIT_CUT_FROM
    assert api.registrations == []


def test_an_owner_edit_is_never_given_a_second_opinion() -> None:
    from deejay_cog import second_opinion

    verdict = second_opinion.decide(
        f"{_BABY} <Intro Cut>",
        "Etta James",
        "Don't Worry",
        "Madcon",
        Heard(150.0, ()),
        SpySearch(),
    )

    assert verdict.side is None
    assert verdict.why == second_opinion.OWNER_EDIT_NO_OPINION


def test_the_cut_from_hint_is_named_in_the_run_report(
    monkeypatch, drive, posted
) -> None:
    monkeypatch.setitem(FLAGS, ident.MODE_FLAG, "write")
    monkeypatch.setenv(backfill.STATE_FOLDER_VAR, STATE)
    api = _baby_api()
    drive_id = _linked_edit(drive)

    fp.library_identify_by_fingerprint(
        drive_id,
        "baby.mp3",
        root=ROOT,
        path="Blues/baby.mp3",
        run_id="m-1",
        sender_mode=FlowMode.WRITE,
        drive=drive,
        api=api,
        identifier=FakeIdentifier(metadata={"title": _BABY, "artist": "Etta James"}),
        search=SpySearch(),
    )

    text = _text(posted)
    assert "owner_edit_cut_from" in text and str(_ENTRY) in text


def test_an_unflagged_file_still_gets_a_second_opinion(drive) -> None:
    api = FakeApi()
    _, result = _disagreeing(drive, api, Heard(150.0, ()), FakeSearch())

    assert result.outcome == fp.DISAGREES_WITH_TAGS


def test_a_dry_run_names_the_owner_edit_it_would_send_for_a_hint(
    settings, owner_edit_api, drive, jobs, monkeypatch
) -> None:
    monkeypatch.setitem(FLAGS, ident.MODE_FLAG, "dry-run")
    api = owner_edit_api
    title = f"{_BABY} <Intro Cut>"
    existing = uuid.uuid4()
    api.known[title.lower()] = CatalogMatch(
        id=existing, title=title, artist="Etta James", owner_edit=True
    )
    api.displays[existing] = _display(existing, owner_edit=True)
    _unidentified(drive, api, "baby.mp3", _song(title, "Etta James"))

    summary = ident.library_identify(run_id="i-1")

    assert jobs.sent == []
    assert summary.owner_edit_hints == ["New/baby.mp3"]
