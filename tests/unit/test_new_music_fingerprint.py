"""The fingerprint function's new-music path: confident matches are taken in
with the new-music steps, the rest go to needs-review, and the stricter of
the sender's mode and the function's own applies."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from unittest.mock import MagicMock

import pytest
from mini_app_polis import pipeline_status
from mini_app_polis.api.contract import CatalogLookupRequest, CatalogMatch
from mini_app_polis.mp3.identify import IdentificationResult, TrackId
from music_fakes import AUDIO, FakeDrive, mp3
from mutagen.id3 import TALB

from deejay_cog import fingerprint as fp
from deejay_cog import new_music
from deejay_cog.fingerprint_jobs import FingerprintJobMessage, FingerprintJobPayload
from deejay_cog.music_safety import FlowMode, audio_payload, tag_snapshot

IN, DEST, REVIEW, ORIG, STATE = "in", "dest", "review", "orig", "state"


@dataclass
class FakeIdentifier:
    score: float = 0.97
    metadata: dict = field(
        default_factory=lambda: {
            "title": "Brand New",
            "artist": "Someone",
            "year": "2026",
        }
    )

    def identify(
        self, path: str, *, fetch_metadata: bool = True
    ) -> IdentificationResult:
        del fetch_metadata
        chosen = TrackId(provider="musicbrainz", id="mbid", confidence=self.score)
        return IdentificationResult(
            path=path, candidates=[chosen], chosen=chosen, metadata=self.metadata
        )


@dataclass
class FakeApi:
    answer: CatalogMatch | None = None

    def lookup_catalog(self, payload: CatalogLookupRequest):
        return [self.answer for _ in payload.tracks]

    def list_library_files(self, **_):
        return []  # an empty library registry


@pytest.fixture(autouse=True)
def settings(monkeypatch: pytest.MonkeyPatch) -> None:
    for role, value in {
        "input": IN,
        "destination": DEST,
        "review": REVIEW,
        "originals": ORIG,
        "state": STATE,
    }.items():
        monkeypatch.setenv(new_music.FOLDER_VARS[role], value)
    monkeypatch.setenv(new_music.MODE_VAR, "write")
    monkeypatch.setenv("RETAG_MIN_CONFIDENCE", "0.9")
    monkeypatch.setattr(pipeline_status, "post_run_finding", MagicMock())


def _run(drive: FakeDrive, file_id: str, *, sender=FlowMode.WRITE, **kw):
    return fp.intake_by_fingerprint(
        file_id,
        "x.mp3",
        run_id="fp-1",
        sender_mode=sender,
        drive=drive,
        api=kw.pop("api", FakeApi()),
        identifier=kw.pop("identifier", FakeIdentifier()),
    )


def _untagged() -> bytes:
    return mp3(TALB(encoding=3, text=["Album"]))


def test_a_confident_match_is_taken_in_with_musicbrainz_tags(drive: FakeDrive) -> None:
    original = _untagged()
    file_id = drive.add("x.mp3", IN, original)

    summary = _run(drive, file_id)

    assert summary.identified == ["x.mp3 -> Brand New_Someone.mp3"]
    (copy,) = drive.in_folder(DEST)
    assert copy.name == "Brand New_Someone.mp3"
    assert tag_snapshot(copy.data)["title"] == "Brand New"
    assert audio_payload(copy.data) == AUDIO
    assert drive.items[file_id].parents == [ORIG]
    assert drive.items[file_id].data == original
    record = json.loads(drive.in_folder(STATE)[0].data)
    assert record["stage"] == "fingerprint"
    assert "MusicBrainz" in record["entries"][0]["identified_by"]


def test_a_confident_match_the_catalog_knows_gets_the_catalog_tags(
    drive: FakeDrive,
) -> None:
    file_id = drive.add("x.mp3", IN, _untagged())
    match = CatalogMatch(
        id=uuid.UUID(int=1), title="Brand New", remix=None, artist="Someone Else"
    )

    _run(drive, file_id, api=FakeApi(match))

    (copy,) = drive.in_folder(DEST)
    assert copy.name == "Brand New_Someone Else.mp3"


def test_low_confidence_goes_to_review_unchanged(drive: FakeDrive) -> None:
    original = _untagged()
    file_id = drive.add("x.mp3", IN, original)

    summary = _run(drive, file_id, identifier=FakeIdentifier(score=0.5))

    assert summary.needs_review and "0.50" in summary.needs_review[0]
    assert drive.items[file_id].parents == [REVIEW]
    assert drive.items[file_id].data == original
    assert drive.in_folder(DEST) == []


def test_a_file_no_longer_in_the_input_folder_is_left_alone(drive: FakeDrive) -> None:
    file_id = drive.add("x.mp3", ORIG, _untagged())

    summary = _run(drive, file_id)

    assert summary.no_longer_in_input == ["x.mp3"]
    assert drive.writes() == []


@pytest.mark.parametrize(
    ("sender", "env", "writes"),
    [
        (FlowMode.WRITE, "dry-run", False),
        (FlowMode.DRY_RUN, "write", False),
        (FlowMode.WRITE, "off", False),
        (FlowMode.OFF, "write", False),
        (FlowMode.WRITE, "write", True),
    ],
)
def test_the_stricter_mode_applies(
    drive: FakeDrive, monkeypatch: pytest.MonkeyPatch, sender, env, writes
) -> None:
    monkeypatch.setenv(new_music.MODE_VAR, env)
    file_id = drive.add("x.mp3", IN, _untagged())

    _run(drive, file_id, sender=sender)

    assert bool(drive.writes(except_folder=STATE)) is writes
    assert (drive.items[file_id].parents == [ORIG]) is writes


def test_a_new_music_message_takes_the_new_music_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    intake = MagicMock()
    plain = MagicMock()
    monkeypatch.setattr(fp, "intake_by_fingerprint", intake)
    monkeypatch.setattr(fp, "fingerprint", plain)
    body = FingerprintJobMessage(
        payload=FingerprintJobPayload(
            drive_file_id="f1", name="x.mp3", flow="new-music", mode="dry-run"
        )
    ).model_dump_json()

    fp.process_message(body, run_id="m-1")

    plain.assert_not_called()
    assert intake.call_args.kwargs["sender_mode"] is FlowMode.DRY_RUN


def test_an_old_message_without_a_flow_is_retag_music(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    intake = MagicMock()
    plain = MagicMock()
    monkeypatch.setattr(fp, "intake_by_fingerprint", intake)
    monkeypatch.setattr(fp, "fingerprint", plain)
    body = json.dumps(
        {
            "type": "deejay.fingerprint",
            "version": 1,
            "payload": {"drive_file_id": "f1", "name": "x.mp3"},
        }
    )

    fp.process_message(body, run_id="m-1")

    intake.assert_not_called()
    plain.assert_called_once()


def test_a_new_music_message_without_a_mode_is_off(drive: FakeDrive) -> None:
    file_id = drive.add("x.mp3", IN, _untagged())
    payload = FingerprintJobPayload(drive_file_id=file_id, flow="new-music")
    assert payload.mode is None

    summary = _run(drive, file_id, sender=fp.parse_mode(payload.mode))

    assert summary.mode is FlowMode.OFF
    assert drive.calls == []
