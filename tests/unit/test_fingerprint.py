"""The fingerprint function: fpcalc, AcoustID and MusicBrainz, the confidence
gate, the two ways a confident match is written, the run report, the
AcoustID rate limit and the Lambda entrypoint."""

from __future__ import annotations

import io
import json
import stat
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from mini_app_polis import pipeline_status
from mini_app_polis.api.contract import CatalogLookupRequest, CatalogMatch
from mini_app_polis.mp3.identify import (
    IdentificationPolicy,
    Mp3Identifier,
    TrackId,
)
from mutagen.id3 import ID3, TALB

from deejay_cog import fingerprint as fp
from deejay_cog.fingerprint import Outcome
from deejay_cog.fingerprint_jobs import FingerprintJobMessage

#: One MPEG-1 Layer III frame, repeated: enough audio for mutagen.
_AUDIO = (b"\xff\xfb\x90\x64" + b"\x00" * 413) * 8
_CATALOG_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_MBID = "9f1e2d3c-0000-4000-8000-000000000001"


def _mp3_with_album() -> bytes:
    """An MP3 with an album and no title or artist: what stage 1 hands on."""
    tags = ID3()
    tags.add(TALB(encoding=3, text=["Some Album"]))
    buffer = io.BytesIO(_AUDIO)
    tags.save(buffer, v2_version=4)
    return buffer.getvalue()


def _tags(data: bytes) -> ID3:
    return ID3(io.BytesIO(data))


def _text(tags: ID3, key: str) -> str | None:
    frame = tags.get(key)
    return str(frame.text[0]) if frame is not None else None


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


@dataclass
class _Downloaded:
    name: str
    data: bytes
    mime_type: str = "audio/mpeg"


@dataclass
class FakeDrive:
    name: str = "untitled.mp3"
    data: bytes = field(default_factory=_mp3_with_album)
    mime_type: str = "audio/mpeg"
    updates: list[tuple[str, bytes, str | None]] = field(default_factory=list)

    def download_file_bytes(self, file_id: str) -> _Downloaded:
        assert file_id
        return _Downloaded(self.name, self.data, self.mime_type)

    def update_file(
        self, file_id: str, filepath: str, *, mime_type: str | None = None
    ) -> None:
        self.updates.append((file_id, Path(filepath).read_bytes(), mime_type))

    # Anything that renames, moves or deletes fails the test loudly.
    def __getattr__(self, name: str):
        raise AssertionError(f"unexpected Drive call: {name}")


@dataclass
class FakeApi:
    answer: CatalogMatch | None = None
    requests: list[CatalogLookupRequest] = field(default_factory=list)

    def lookup_catalog(
        self, payload: CatalogLookupRequest
    ) -> list[CatalogMatch | None]:
        self.requests.append(payload)
        return [self.answer for _ in payload.tracks]


@dataclass
class FakeMusicBrainz:
    metadata: dict[str, Any] = field(
        default_factory=lambda: {
            "title": "Temple",
            "artist": "Thomas Gold",
            "year": "2019-05-01",
        }
    )
    fetched: list[str] = field(default_factory=list)

    def fetch(self, track_id: TrackId) -> dict[str, Any]:
        self.fetched.append(track_id.id)
        return dict(self.metadata)


@dataclass
class FakeAcoustId:
    """AcoustID's lookup endpoint: answers in turn, then the last again."""

    answers: list[dict[str, Any]]
    calls: list[tuple[str, str, float, Any]] = field(default_factory=list)

    def __call__(
        self, apikey: str, fingerprint: str, duration: float, meta: Any = None
    ) -> dict[str, Any]:
        self.calls.append((apikey, fingerprint, duration, meta))
        index = min(len(self.calls), len(self.answers)) - 1
        return self.answers[index]


def _ok(score: float, *recording_ids: str) -> dict[str, Any]:
    return {
        "status": "ok",
        "results": [
            {
                "score": score,
                "id": "a",
                "recordings": [{"id": r} for r in recording_ids],
            }
        ],
    }


_RATE_LIMITED = {
    "status": "error",
    "error": {"code": 14, "message": "rate limit (3 requests per second) exceeded"},
}


@pytest.fixture
def fake_fpcalc(tmp_path: Path) -> str:
    """An fpcalc that prints what the real one prints for ``-json``."""
    script = tmp_path / "bin" / "fpcalc"
    script.parent.mkdir()
    script.write_text(
        "#!/bin/sh\n"
        'test "$1" = "-json" || exit 2\n'
        'test -f "$2" || exit 3\n'
        'echo \'{"duration": 215.4, "fingerprint": "AQADtFKSRUmS"}\'\n'
    )
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    return str(script)


def _identifier(
    fpcalc: str, acoustid: FakeAcoustId, mb: FakeMusicBrainz, threshold: float = 0.9
) -> Mp3Identifier:
    """The real identifier on a fake fpcalc, AcoustID and MusicBrainz."""
    return Mp3Identifier(
        acoustid_identifier=fp.AcoustIdLookup(
            "key", fpcalc=fpcalc, lookup=acoustid, sleep=lambda _s: None
        ),
        musicbrainz_provider=mb,
        policy=IdentificationPolicy(
            min_confidence=threshold,
            max_candidates=fp.MAX_CANDIDATES,
            fetch_metadata_min_confidence=threshold,
        ),
    )


def _match(**overrides: Any) -> CatalogMatch:
    fields: dict[str, Any] = {
        "id": _CATALOG_ID,
        "title": "Temple",
        "remix": "Niko The Kid Remix",
        "artist": "Thomas Gold",
        "label": "Armada",
        "genre": "House",
        "bpm": 124.0,
        "release_year": 2019,
    }
    fields.update(overrides)
    return CatalogMatch(**fields)


# ---------------------------------------------------------------------------
# fpcalc
# ---------------------------------------------------------------------------


def test_fpcalc_on_path_is_used(
    fake_fpcalc: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PATH", str(Path(fake_fpcalc).parent))
    assert fp.find_fpcalc() == fake_fpcalc


def test_the_layers_fpcalc_is_used_when_it_is_not_on_path(
    fake_fpcalc: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PATH", str(tmp_path / "empty"))
    monkeypatch.setattr(fp, "FPCALC_LAYER_PATH", fake_fpcalc)
    assert fp.find_fpcalc() == fake_fpcalc


def test_no_fpcalc_anywhere_fails_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PATH", str(tmp_path / "empty"))
    monkeypatch.setattr(fp, "FPCALC_LAYER_PATH", str(tmp_path / "missing"))
    with pytest.raises(fp.FpcalcNotFound):
        fp.find_fpcalc()


def test_fpcalc_output_is_read(fake_fpcalc: str, tmp_path: Path) -> None:
    audio = tmp_path / "a.mp3"
    audio.write_bytes(_AUDIO)
    assert fp.run_fpcalc(str(audio), fpcalc=fake_fpcalc) == (215.4, "AQADtFKSRUmS")


def test_a_failing_fpcalc_raises(fake_fpcalc: str, tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="status 3"):
        fp.run_fpcalc(str(tmp_path / "missing.mp3"), fpcalc=fake_fpcalc)


# ---------------------------------------------------------------------------
# AcoustID, and its rate limit
# ---------------------------------------------------------------------------


def test_acoustid_candidates_carry_their_scores_best_first(
    fake_fpcalc: str, tmp_path: Path
) -> None:
    audio = tmp_path / "a.mp3"
    audio.write_bytes(_AUDIO)
    acoustid = FakeAcoustId(
        [
            {
                "status": "ok",
                "results": [
                    {"score": 0.5, "recordings": [{"id": "low"}]},
                    {"score": 0.97, "recordings": [{"id": "high"}, {}]},
                    {"score": 0.99},
                ],
            }
        ]
    )
    lookup = fp.AcoustIdLookup("key", fpcalc=fake_fpcalc, lookup=acoustid)

    candidates = lookup.identify(str(audio))

    assert [(c.id, c.confidence) for c in candidates] == [
        ("high", 0.97),
        ("low", 0.5),
    ]
    assert acoustid.calls == [("key", "AQADtFKSRUmS", 215.4, "recordings")]


def test_a_rate_limited_lookup_is_retried_after_a_pause(
    fake_fpcalc: str, tmp_path: Path
) -> None:
    audio = tmp_path / "a.mp3"
    audio.write_bytes(_AUDIO)
    acoustid = FakeAcoustId([_RATE_LIMITED, _RATE_LIMITED, _ok(0.95, _MBID)])
    pauses: list[float] = []
    lookup = fp.AcoustIdLookup(
        "key", fpcalc=fake_fpcalc, lookup=acoustid, sleep=pauses.append
    )

    candidates = lookup.identify(str(audio))

    assert [c.id for c in candidates] == [_MBID]
    assert len(acoustid.calls) == 3
    assert pauses == [1.0, 2.0]


def test_a_lookup_rate_limited_every_time_raises(
    fake_fpcalc: str, tmp_path: Path
) -> None:
    audio = tmp_path / "a.mp3"
    audio.write_bytes(_AUDIO)
    acoustid = FakeAcoustId([_RATE_LIMITED])
    lookup = fp.AcoustIdLookup(
        "key", fpcalc=fake_fpcalc, lookup=acoustid, sleep=lambda _s: None
    )

    with pytest.raises(fp.AcoustIdRateLimited):
        lookup.identify(str(audio))
    assert len(acoustid.calls) == fp.RATE_LIMIT_RETRIES + 1


def test_a_rate_limited_file_is_not_written_and_its_message_comes_back(
    fake_fpcalc: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rate limited is a retry, never a file reported as unidentified."""
    posted = MagicMock()
    monkeypatch.setattr(pipeline_status, "post_run_finding", posted)
    drive = FakeDrive()
    identifier = _identifier(
        fake_fpcalc, FakeAcoustId([_RATE_LIMITED]), FakeMusicBrainz()
    )
    monkeypatch.setattr(
        fp, "GoogleAPI", MagicMock(**{"from_env.return_value.drive": drive})
    )
    monkeypatch.setattr(fp, "api_client", lambda **_kw: FakeApi())
    monkeypatch.setattr(fp, "build_identifier", lambda **_kw: identifier)

    result = fp.lambda_handler(
        {"Records": [{"messageId": "m-1", "body": _body(), "attributes": {}}]}, None
    )

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    assert drive.updates == []
    posted.assert_called_once()
    assert posted.call_args.kwargs["left_untouched"] == 0
    assert "AcoustIdRateLimited" in posted.call_args.kwargs["text"]


def test_any_other_acoustid_error_raises(fake_fpcalc: str, tmp_path: Path) -> None:
    audio = tmp_path / "a.mp3"
    audio.write_bytes(_AUDIO)
    acoustid = FakeAcoustId(
        [{"status": "error", "error": {"code": 4, "message": "invalid API key"}}]
    )
    lookup = fp.AcoustIdLookup("key", fpcalc=fake_fpcalc, lookup=acoustid)

    with pytest.raises(fp.AcoustIdError, match="invalid API key"):
        lookup.identify(str(audio))
    assert len(acoustid.calls) == 1


# ---------------------------------------------------------------------------
# One file
# ---------------------------------------------------------------------------


def test_a_confident_match_in_the_catalog_is_written_with_the_catalogs_tags(
    fake_fpcalc: str,
) -> None:
    drive, api, mb = FakeDrive(), FakeApi(answer=_match()), FakeMusicBrainz()
    identifier = _identifier(fake_fpcalc, FakeAcoustId([_ok(0.95, _MBID)]), mb)

    result = fp.identify_file(drive, api, identifier, "f1", threshold=0.9)

    assert result.outcome is Outcome.TAGGED_FROM_CATALOG
    assert result.score == 0.95
    assert result.match is not None
    # The recording went through the same IngestTrack lookup as stage 1.
    (track,) = api.requests[0].tracks
    assert (track.title, track.artist, track.release_year) == (
        "Temple",
        "Thomas Gold",
        2019,
    )
    assert mb.fetched == [_MBID]
    ((file_id, data, mime_type),) = drive.updates
    assert (file_id, mime_type) == ("f1", "audio/mpeg")
    tags = _tags(data)
    assert _text(tags, "TIT2") == "Temple (Niko The Kid Remix)"
    assert _text(tags, "TPE1") == "Thomas Gold"
    assert _text(tags, "TXXX:CATALOG_ID") == str(_CATALOG_ID)
    assert _text(tags, "TALB") == "Some Album"


def test_a_confident_match_not_in_the_catalog_gets_musicbrainzs_tags(
    fake_fpcalc: str,
) -> None:
    drive, api = FakeDrive(), FakeApi(answer=None)
    identifier = _identifier(
        fake_fpcalc, FakeAcoustId([_ok(0.92, _MBID)]), FakeMusicBrainz()
    )

    result = fp.identify_file(drive, api, identifier, "f1", threshold=0.9)

    assert result.outcome is Outcome.TAGGED_FROM_MUSICBRAINZ
    ((_, data, _),) = drive.updates
    tags = _tags(data)
    assert _text(tags, "TIT2") == "Temple"
    assert _text(tags, "TPE1") == "Thomas Gold"
    assert _text(tags, "TDRC") == "2019"
    assert tags.get("TXXX:CATALOG_ID") is None
    assert _text(tags, "TALB") == "Some Album"


def test_a_match_below_the_threshold_leaves_the_file_unchanged(
    fake_fpcalc: str,
) -> None:
    drive, api, mb = FakeDrive(), FakeApi(answer=_match()), FakeMusicBrainz()
    identifier = _identifier(fake_fpcalc, FakeAcoustId([_ok(0.89, _MBID)]), mb)

    result = fp.identify_file(drive, api, identifier, "f1", threshold=0.9)

    assert result.outcome is Outcome.BELOW_CONFIDENCE
    assert result.score == 0.89
    assert drive.updates == []
    assert api.requests == []
    assert mb.fetched == []


def test_no_acoustid_match_leaves_the_file_unchanged(fake_fpcalc: str) -> None:
    drive = FakeDrive()
    identifier = _identifier(
        fake_fpcalc, FakeAcoustId([{"status": "ok", "results": []}]), FakeMusicBrainz()
    )

    result = fp.identify_file(drive, FakeApi(), identifier, "f1", threshold=0.9)

    assert result.outcome is Outcome.BELOW_CONFIDENCE
    assert result.score == 0.0
    assert drive.updates == []


def test_a_recording_without_title_or_artist_leaves_the_file_unchanged(
    fake_fpcalc: str,
) -> None:
    drive, api = FakeDrive(), FakeApi()
    mb = FakeMusicBrainz(metadata={"title": "Temple", "artist": None})
    identifier = _identifier(fake_fpcalc, FakeAcoustId([_ok(0.99, _MBID)]), mb)

    result = fp.identify_file(drive, api, identifier, "f1", threshold=0.9)

    assert result.outcome is Outcome.NO_RECORDING_TAGS
    assert drive.updates == []
    assert api.requests == []


def test_a_file_that_is_not_an_mp3_is_not_fingerprinted() -> None:
    drive = FakeDrive(name="song.m4a", mime_type="audio/mp4")
    identifier = MagicMock()

    result = fp.identify_file(drive, FakeApi(), identifier, "f1", threshold=0.9)

    assert result.outcome is Outcome.NOT_MP3
    identifier.identify.assert_not_called()
    assert drive.updates == []


def test_a_file_already_carrying_the_tags_is_not_written_again(
    fake_fpcalc: str,
) -> None:
    drive = FakeDrive()
    identifier = _identifier(
        fake_fpcalc, FakeAcoustId([_ok(0.95, _MBID)]), FakeMusicBrainz()
    )
    fp.identify_file(drive, FakeApi(), identifier, "f1", threshold=0.9)
    drive.data = drive.updates[0][1]
    drive.updates.clear()

    result = fp.identify_file(drive, FakeApi(), identifier, "f1", threshold=0.9)

    assert result.outcome is Outcome.ALREADY_TAGGED
    assert drive.updates == []


def test_the_threshold_comes_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RETAG_MIN_CONFIDENCE", raising=False)
    assert fp.min_confidence() == fp.DEFAULT_MIN_CONFIDENCE
    monkeypatch.setenv("RETAG_MIN_CONFIDENCE", "0.75")
    assert fp.min_confidence() == 0.75


def test_no_acoustid_key_fails_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ACOUSTID_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="ACOUSTID_API_KEY"):
        fp.build_identifier(threshold=0.9)


# ---------------------------------------------------------------------------
# The run report
# ---------------------------------------------------------------------------


@pytest.fixture
def posted(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    fake = MagicMock()
    monkeypatch.setattr(pipeline_status, "post_run_finding", fake)
    return fake


def test_the_report_names_an_untouched_file_and_its_score(
    fake_fpcalc: str, posted: MagicMock
) -> None:
    identifier = _identifier(
        fake_fpcalc, FakeAcoustId([_ok(0.42, _MBID)]), FakeMusicBrainz()
    )

    result = fp.fingerprint(
        "f1",
        "untitled.mp3",
        run_id="m-1",
        drive=FakeDrive(),
        api=FakeApi(),
        identifier=identifier,
    )

    assert result is not None and result.outcome is Outcome.BELOW_CONFIDENCE
    posted.assert_called_once()
    args, kwargs = posted.call_args
    assert args[0] == "deejay-fingerprint"
    assert kwargs["run_id"] == "m-1"
    assert kwargs["notable"] is True
    assert kwargs["scanned"] == 1
    assert kwargs["left_untouched"] == 1
    assert kwargs["tagged"] == 0
    assert "untitled.mp3 (best score 0.42)" in kwargs["text"]


def test_the_report_counts_a_file_tagged_from_the_catalog(
    fake_fpcalc: str, posted: MagicMock
) -> None:
    identifier = _identifier(
        fake_fpcalc, FakeAcoustId([_ok(0.95, _MBID)]), FakeMusicBrainz()
    )

    fp.fingerprint(
        "f1",
        run_id="m-1",
        drive=FakeDrive(),
        api=FakeApi(answer=_match()),
        identifier=identifier,
    )

    args, kwargs = posted.call_args
    assert args[1] == "SUCCESS"
    assert kwargs["matched_by_fingerprint"] == 1
    assert kwargs["matched_in_catalog"] == 1
    assert kwargs["not_in_catalog"] == 0
    assert kwargs["tagged"] == 1
    assert kwargs["left_untouched"] == 0


def test_the_report_counts_a_file_tagged_from_musicbrainz(
    fake_fpcalc: str, posted: MagicMock
) -> None:
    identifier = _identifier(
        fake_fpcalc, FakeAcoustId([_ok(0.95, _MBID)]), FakeMusicBrainz()
    )

    fp.fingerprint(
        "f1", run_id="m-1", drive=FakeDrive(), api=FakeApi(), identifier=identifier
    )

    kwargs = posted.call_args.kwargs
    assert kwargs["matched_in_catalog"] == 0
    assert kwargs["not_in_catalog"] == 1
    assert kwargs["tagged"] == 1


# ---------------------------------------------------------------------------
# The Lambda entrypoint
# ---------------------------------------------------------------------------


def _body(**overrides: Any) -> str:
    return FingerprintJobMessage.model_validate(
        {
            "type": "deejay.fingerprint",
            "payload": {"drive_file_id": "f1", "name": "untitled.mp3"},
            **overrides,
        }
    ).model_dump_json()


def _event(body: str, receive: str = "1") -> dict[str, Any]:
    return {
        "Records": [
            {
                "messageId": "m-1",
                "body": body,
                "attributes": {"ApproximateReceiveCount": receive},
            }
        ]
    }


def test_a_message_runs_its_file_under_the_message_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = MagicMock()
    monkeypatch.setattr(fp, "fingerprint", run)

    assert fp.lambda_handler(_event(_body()), None) == {"batchItemFailures": []}
    run.assert_called_once_with("f1", "untitled.mp3", run_id="m-1")


def test_a_failed_run_comes_back(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fp, "fingerprint", MagicMock(side_effect=RuntimeError("x")))

    result = fp.lambda_handler(_event(_body()), None)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}


@pytest.mark.parametrize(
    "body",
    [
        "not json",
        json.dumps({"type": "deejay.run", "version": 1, "payload": {"mode": "x"}}),
        json.dumps({"type": "deejay.fingerprint", "version": 1, "payload": {}}),
    ],
)
def test_an_unprocessable_message_comes_back_and_is_reported_once(
    monkeypatch: pytest.MonkeyPatch, body: str
) -> None:
    reported = MagicMock()
    monkeypatch.setattr(fp, "post_run_finding", reported)
    run = MagicMock()
    monkeypatch.setattr(fp, "fingerprint", run)

    first = fp.lambda_handler(_event(body, "1"), None)
    fp.lambda_handler(_event(body, "2"), None)

    assert first == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    run.assert_not_called()
    reported.assert_called_once()
    assert reported.call_args.args[:2] == ("deejay-fingerprint", "ERROR")


def test_settings_that_cannot_load_send_every_record_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fp, "load_secrets", MagicMock(side_effect=RuntimeError("ssm")))
    run = MagicMock()
    monkeypatch.setattr(fp, "fingerprint", run)

    result = fp.lambda_handler(_event(_body()), None)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    run.assert_not_called()
