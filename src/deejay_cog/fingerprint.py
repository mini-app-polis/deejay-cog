"""The fingerprint function: identify one music file by its sound
(ADR-0007, stage 2).

retag-music sends each file with no usable title or artist here, one
message per file (:mod:`deejay_cog.fingerprint_jobs`). For each:

1. ``fpcalc``, from the function's layer, fingerprints the file; it is
   found on ``PATH`` or at ``/opt/bin/fpcalc``.
2. AcoustID gives the MusicBrainz recordings that fingerprint belongs to,
   and MusicBrainz the best one's title, artist and year, through
   common-python-utils' ``Mp3Identifier`` and its MusicBrainz provider.
3. Only when the AcoustID score reaches ``RETAG_MIN_CONFIDENCE``, the
   recording is built into an ``IngestTrack`` and looked up in the catalog
   exactly as stage 1 looks up a file by its tags. A match is written with
   the catalog's tags; with no match, MusicBrainz's title, artist and year
   are written, so the file joins the catalog once it is played.

Below the score the file is not changed and is named in the run report, to
be tagged by hand. So is a file that is not an MP3, since only ID3 tags are
written. A file is never renamed, moved or deleted: a write replaces its
contents in place, as stage 1's does.

**AcoustID allows three requests a second.** The function runs one file at
a time (concurrency 1, one FIFO message group) and makes one lookup per
file, through pyacoustid, which spaces its requests to that rate. A lookup
AcoustID still refuses as over the limit is retried after a pause; one
refused every time raises, so the message comes back to the queue rather
than the file being reported as unidentified. MusicBrainz is the same: a
server or network error raises, while a recording it refuses (not found,
the same answer every time) leaves the file as it is, for review
(:class:`MusicBrainzRecordings`).

Each message is one run with its own report, under the message id, as the
deejay worker's runs are.

**New music.** A message marked ``"flow": "new-music"`` comes from the
new-music mode instead. Such a file is identified the same way, but is then
taken in with the new-music steps (:class:`deejay_cog.new_music.Intake`): a
tagged copy under its new name in the destination folder and the original
moved to processed-originals on a confident match, the original moved to
needs-review otherwise. That run is reported under ``new-music`` and follows
the stricter of the message's mode and ``flags.deejay_cog.new_music``.

**Library files.** A message marked ``"flow": "library-identify"`` comes
from the nightly library run (:mod:`deejay_cog.library_identify`). The file
is identified the same way but **no MusicBrainz tags are written**: a
confident match is looked up in the catalog, or made a library entry when
the catalog has no song for it, and the file is then linked to that entry
(fill-up, the catalog's values written in place, registered as identified).
Below the score, or with no match, the file is linked by its own tags when
they name a song (:func:`deejay_cog.library_identify.link_by_own_tags`), and
otherwise left unchanged and unidentified. A confident match that disagrees
with the file's own tags goes to a second opinion
(:mod:`deejay_cog.second_opinion`): the other recordings AcoustID links to
the audio, then Spotify's length for each side. The file is linked by
whichever side a second source backs, and left for the owner when none
does or both are. That run
is reported under ``library-identify`` and follows the stricter of the
message's mode and ``flags.deejay_cog.library_identify``.
"""

from __future__ import annotations

import enum
import json
import os
import shutil
import subprocess
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from time import monotonic
from typing import Any, Protocol

import acoustid
import musicbrainzngs
from googleapiclient.http import DEFAULT_HTTP_TIMEOUT_SEC
from mini_app_polis import load_secrets, timing
from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import (
    CatalogLookupRequest,
    CatalogMatch,
    LibraryEntryCreate,
)
from mini_app_polis.deadline import RunDeadlineReached
from mini_app_polis.environment import api_base_url, log_settings
from mini_app_polis.google import GoogleAPI
from mini_app_polis.lambda_worker import (
    RunOutOfTime,
    UnprocessableMessage,
    all_failed,
    deadline,
    parse_message,
    process_records,
)
from mini_app_polis.mp3.identify import (
    IdentificationPolicy,
    IdentificationResult,
    Mp3Identifier,
    TrackId,
)
from mini_app_polis.mp3.identify.providers.musicbrainz_provider import (
    MusicBrainzRecordingProvider,
)
from mini_app_polis.pipeline_status import run_report
from mini_app_polis.sentry import init_sentry

from deejay_cog import library_identify, new_music
from deejay_cog._pipeline_eval import REPO, post_run_finding
from deejay_cog._version import __version__
from deejay_cog.api_client import api_client
from deejay_cog.fingerprint_jobs import FingerprintJobMessage
from deejay_cog.music_safety import (
    STATE_FOLDER_VAR,
    AudioChanged,
    FlowMode,
    RunRecord,
    TagEmptied,
    check_audio_unchanged,
    file_meta,
    flow_mode,
    parse_mode,
    stricter,
)
from deejay_cog.music_tags import (
    MP3_MIME_TYPE,
    CatalogLookup,
    DriveFiles,
    ingest_track,
    read_tag_row,
    replace_contents,
    tags_agree,
    write_catalog_tags,
    write_recording_tags,
)
from deejay_cog.second_opinion import (
    Heard,
    SpotifyTrackSearch,
    TrackSearch,
    acoustid_recordings,
)
from deejay_cog.second_opinion import decide as second_opinion

log = logger_mod.get_logger()

FLOW_NAME = "deejay-fingerprint"

#: The AcoustID score a match must reach when ``RETAG_MIN_CONFIDENCE`` is unset.
DEFAULT_MIN_CONFIDENCE = 0.90

#: AcoustID candidates kept per file; the best is the one used.
MAX_CANDIDATES = 5

#: Where the layer puts fpcalc, for when ``/opt/bin`` is not on ``PATH``.
FPCALC_LAYER_PATH = "/opt/bin/fpcalc"

#: How long one fpcalc run may take.
FPCALC_TIMEOUT_SECONDS = 120

#: AcoustID's error code for a request over its rate limit.
ACOUSTID_RATE_LIMITED = 14

#: Further attempts after a lookup AcoustID refuses as over the limit, and
#: the pause before the first of them (doubled for each after).
RATE_LIMIT_RETRIES = 2
RATE_LIMIT_BACKOFF_SECONDS = 1.0

#: Who MusicBrainz is told is asking.
MUSICBRAINZ_CONTACT = "https://github.com/mini-app-polis/deejay-cog"

#: As the deejay worker: stop a margin before the function's timeout so the
#: report still goes out, and stop the shared library's retries a request's
#: timeout earlier still.
DEADLINE_MARGIN_SECONDS = 30
REQUEST_HEADROOM_SECONDS = DEFAULT_HTTP_TIMEOUT_SEC

# At import, once per cold start; the service tag is the deejay worker's,
# since this function is the same cog.
init_sentry("deejay-cog")


class FpcalcNotFound(RuntimeError):  # noqa: N818 — named for what happened
    """fpcalc is neither on PATH nor where the layer puts it."""


class AudioUnreadable(RuntimeError):  # noqa: N818 — named for what happened
    """fpcalc could not open or decode the file's audio (its exit status 2
    or 3). The same bytes give the same answer every time, so the file is
    left for review rather than tried again."""


#: fpcalc's exit statuses for audio it cannot open (2) or decode (3).
FPCALC_UNREADABLE = frozenset({2, 3})


class AcoustIdError(RuntimeError):
    """AcoustID refused a lookup."""


class AcoustIdRateLimited(AcoustIdError):  # noqa: N818 — named for what happened
    """AcoustID refused a lookup as over its rate limit, every time it was tried."""


#: A library file whose fingerprint names another song than its own tags do.
DISAGREES_WITH_TAGS = "disagrees_with_tags"


class Outcome(enum.Enum):
    """What happened to one file."""

    TAGGED_FROM_CATALOG = "tagged_from_catalog"
    """Matched with confidence, found in the catalog, and written with its tags."""

    TAGGED_FROM_MUSICBRAINZ = "tagged_from_musicbrainz"
    """Matched with confidence, not in the catalog, and written with
    MusicBrainz's title, artist and year."""

    ALREADY_TAGGED = "already_tagged"
    """Matched with confidence, and its tags already said what would be written."""

    BELOW_CONFIDENCE = "below_confidence"
    """No AcoustID match reached the minimum score. Not changed."""

    NO_RECORDING_TAGS = "no_recording_tags"
    """Matched, but MusicBrainz gave no title or artist. Not changed."""

    RECORDING_NOT_SERVED = "recording_not_served"
    """Matched, but MusicBrainz refuses the recording AcoustID named (not
    found: merged or deleted since AcoustID linked it). Not changed."""

    NOT_MP3 = "not_mp3"
    """Not an MP3, so it has no ID3 tags to write. Not changed."""

    AUDIO_UNREADABLE = "audio_unreadable"
    """fpcalc could not read the audio (a damaged or mislabeled file). Not
    changed."""


#: The outcomes that leave a file as it was, to be tagged by hand.
UNTOUCHED = frozenset(
    {
        Outcome.BELOW_CONFIDENCE,
        Outcome.NO_RECORDING_TAGS,
        Outcome.RECORDING_NOT_SERVED,
        Outcome.NOT_MP3,
        Outcome.AUDIO_UNREADABLE,
    }
)


@dataclass(frozen=True)
class FingerprintResult:
    """One file's outcome, its best AcoustID score, and its catalog entry."""

    file_id: str
    name: str
    outcome: Outcome
    score: float | None = None
    match: CatalogMatch | None = None


class Identifier(Protocol):
    """What :func:`identify_file` asks of ``Mp3Identifier``."""

    def identify(
        self, path: str, *, fetch_metadata: bool = True
    ) -> IdentificationResult:
        """Identify the audio file at ``path``."""
        ...


# ---------------------------------------------------------------------------
# fpcalc and AcoustID
# ---------------------------------------------------------------------------


def find_fpcalc() -> str:
    """The fpcalc to run: the one on PATH, else the layer's."""
    found = shutil.which("fpcalc")
    if found:
        return found
    if os.access(FPCALC_LAYER_PATH, os.X_OK):
        return FPCALC_LAYER_PATH
    raise FpcalcNotFound(f"fpcalc is not on PATH or at {FPCALC_LAYER_PATH}")


def run_fpcalc(path: str, *, fpcalc: str) -> tuple[float, str]:
    """The file's duration in seconds and its Chromaprint fingerprint."""
    proc = subprocess.run(
        [fpcalc, "-json", path],
        capture_output=True,
        text=True,
        check=False,
        timeout=FPCALC_TIMEOUT_SECONDS,
    )
    if proc.returncode != 0:
        message = f"fpcalc exited with status {proc.returncode}: {proc.stderr.strip()}"
        if proc.returncode in FPCALC_UNREADABLE:
            raise AudioUnreadable(message)
        raise RuntimeError(message)
    try:
        parsed = json.loads(proc.stdout)
        return float(parsed["duration"]), str(parsed["fingerprint"])
    except (ValueError, KeyError, TypeError) as exc:
        raise RuntimeError(f"fpcalc output not understood: {exc}") from exc


class AcoustIdLookup:
    """The AcoustID half of ``Mp3Identifier``: fingerprint a file with fpcalc
    and return the MusicBrainz recordings AcoustID matches it to, best first.

    Every candidate is returned with its score, whatever the score;
    ``Mp3Identifier`` picks the best, and the caller decides whether it is
    good enough. A lookup AcoustID refuses raises rather than looking like
    a file with no match.
    """

    def __init__(
        self,
        api_key: str,
        *,
        fpcalc: str,
        lookup: Callable[..., Any] = acoustid.lookup,
        sleep: Callable[[float], None] = time.sleep,
        max_candidates: int = MAX_CANDIDATES,
    ) -> None:
        self._api_key = api_key
        self._fpcalc = fpcalc
        # pyacoustid's lookup spaces its requests to AcoustID's rate limit.
        self._lookup = lookup
        self._sleep = sleep
        self._max_candidates = max_candidates
        #: The last file's length and every titled recording AcoustID linked
        #: to its audio, for a second opinion (:mod:`deejay_cog.second_opinion`).
        self.heard: Heard | None = None

    def identify(self, path: str) -> list[TrackId]:
        """The MusicBrainz recordings AcoustID matches ``path`` to, best first."""
        self.heard = None
        duration, fingerprint = run_fpcalc(path, fpcalc=self._fpcalc)
        data = self._lookup_answer(fingerprint, duration)
        self.heard = Heard(duration, acoustid_recordings(data))
        candidates: list[TrackId] = []
        for result in data.get("results") or []:
            score = float(result.get("score") or 0.0)
            for recording in result.get("recordings") or []:
                recording_id = recording.get("id")
                if recording_id:
                    candidates.append(
                        TrackId(
                            provider="musicbrainz",
                            id=str(recording_id),
                            confidence=score,
                        )
                    )
        candidates.sort(key=lambda c: c.confidence, reverse=True)
        return candidates[: self._max_candidates]

    def _lookup_answer(self, fingerprint: str, duration: float) -> dict[str, Any]:
        """AcoustID's answer, retrying one refused as over the rate limit."""
        for attempt in range(RATE_LIMIT_RETRIES + 1):
            data = self._lookup(self._api_key, fingerprint, duration, meta="recordings")
            if not isinstance(data, dict):
                raise AcoustIdError(f"AcoustID answered {data!r}")
            if data.get("status") == "ok":
                return data
            error = data.get("error") or {}
            message = error.get("message") or data.get("status")
            if error.get("code") != ACOUSTID_RATE_LIMITED:
                raise AcoustIdError(f"AcoustID lookup failed: {message}")
            if attempt < RATE_LIMIT_RETRIES:
                pause = RATE_LIMIT_BACKOFF_SECONDS * (2**attempt)
                log.warning(
                    "deejay-fingerprint: AcoustID rate limit; retrying in %ss", pause
                )
                self._sleep(pause)
        raise AcoustIdRateLimited(
            f"AcoustID refused {RATE_LIMIT_RETRIES + 1} lookups as over its rate limit"
        )


def min_confidence() -> float:
    """The lowest match score accepted: ``RETAG_MIN_CONFIDENCE``, else
    ``DEFAULT_MIN_CONFIDENCE``."""
    raw = os.getenv("RETAG_MIN_CONFIDENCE", "").strip()
    return float(raw) if raw else DEFAULT_MIN_CONFIDENCE


#: Set in a match's metadata when MusicBrainz refuses its recording.
NOT_SERVED = "_musicbrainz_not_served"


class MusicBrainzRecordings(MusicBrainzRecordingProvider):
    """The MusicBrainz provider, with a refused recording as an answer.

    MusicBrainz answers a recording it does not have (404) or a request it
    will not take (400) with ``musicbrainzngs.ResponseError``, the same way
    every time, so asking again cannot help: that gives metadata marked
    :data:`NOT_SERVED`, and the file is left as it is, for review. A server
    or network error (``NetworkError``) still raises, so the message comes
    back to the queue and is tried again.
    """

    def fetch(self, track_id: TrackId) -> dict[str, Any]:
        """The recording's metadata, or metadata marked :data:`NOT_SERVED`
        when MusicBrainz refuses it."""
        try:
            return super().fetch(track_id)
        except RuntimeError as exc:
            if not isinstance(exc.__cause__, musicbrainzngs.ResponseError):
                raise
            log.warning(
                "deejay-fingerprint: MusicBrainz refuses recording %s: %r",
                track_id.id,
                exc.__cause__,
            )
            return {"_provider": "musicbrainz", "_mbid": track_id.id, NOT_SERVED: True}


class FingerprintIdentifier:
    """``Mp3Identifier`` with what its AcoustID lookup heard on the last
    file (``heard``), for a second opinion."""

    def __init__(self, identifier: Mp3Identifier, lookup: AcoustIdLookup) -> None:
        self._identifier = identifier
        self._lookup = lookup

    def identify(
        self, path: str, *, fetch_metadata: bool = True
    ) -> IdentificationResult:
        """Identify the audio file at ``path``."""
        return self._identifier.identify(path, fetch_metadata=fetch_metadata)

    @property
    def heard(self) -> Heard | None:
        """The last file's length and the recordings linked to its audio."""
        return self._lookup.heard


def build_identifier(*, threshold: float) -> FingerprintIdentifier:
    """An ``Mp3Identifier`` on fpcalc, AcoustID and MusicBrainz, with
    MusicBrainz asked only for a match that reaches ``threshold``."""
    api_key = os.getenv("ACOUSTID_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("ACOUSTID_API_KEY is not set")
    lookup = AcoustIdLookup(api_key, fpcalc=find_fpcalc())
    return FingerprintIdentifier(
        Mp3Identifier(
            acoustid_identifier=lookup,
            musicbrainz_provider=MusicBrainzRecordings(
                app_name=REPO, app_version=__version__, contact=MUSICBRAINZ_CONTACT
            ),
            policy=IdentificationPolicy(
                min_confidence=threshold,
                max_candidates=MAX_CANDIDATES,
                fetch_metadata_min_confidence=threshold,
            ),
        ),
        lookup,
    )


def build_track_search() -> TrackSearch | None:
    """Spotify's search for a second opinion, or None without Spotify's
    credentials (the second opinion then rests on AcoustID alone)."""
    from deejay_cog.spotify_sync import get_spotify_client

    spotify = get_spotify_client()
    if spotify is None:
        return None
    try:
        client = spotify.client
    except Exception as exc:  # noqa: BLE001 — no second opinion, not a failed file
        log.warning("second opinion: Spotify could not start: %s", exc)
        return None
    return SpotifyTrackSearch(client)


# ---------------------------------------------------------------------------
# One file
# ---------------------------------------------------------------------------


def _is_mp3(downloaded: Any, name: str) -> bool:
    mime_type = str(getattr(downloaded, "mime_type", "") or "")
    return mime_type == MP3_MIME_TYPE or name.lower().endswith(".mp3")


def _identify(identifier: Identifier, name: str, data: bytes) -> IdentificationResult:
    """Identify the file's bytes, from a temporary file fpcalc can read."""
    suffix = os.path.splitext(name)[1] or ".mp3"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as handle:
        handle.write(data)
        path = handle.name
    try:
        return identifier.identify(path, fetch_metadata=True)
    finally:
        os.unlink(path)


def _year(value: Any) -> str:
    year = str(value or "").strip()[:4]
    return year if year.isdigit() else ""


def identify_file(
    drive: DriveFiles,
    api: CatalogLookup,
    identifier: Identifier,
    file_id: str,
    *,
    threshold: float,
) -> FingerprintResult:
    """Identify one Drive file by its fingerprint and, on a confident match,
    write its tags back over the same file.

    Never renames, moves or deletes the file, and never writes one whose
    best match scores below ``threshold``.
    """
    downloaded = drive.download_file_bytes(file_id)
    name = str(getattr(downloaded, "name", "") or file_id)
    data: bytes = downloaded.data
    if not _is_mp3(downloaded, name):
        return FingerprintResult(file_id, name, Outcome.NOT_MP3)

    try:
        result = _identify(identifier, name, data)
    except AudioUnreadable:
        return FingerprintResult(file_id, name, Outcome.AUDIO_UNREADABLE)
    chosen = result.chosen
    score = float(chosen.confidence or 0.0) if chosen is not None else 0.0
    if chosen is None or score < threshold:
        return FingerprintResult(file_id, name, Outcome.BELOW_CONFIDENCE, score)

    metadata = result.metadata or {}
    if metadata.get(NOT_SERVED):
        return FingerprintResult(file_id, name, Outcome.RECORDING_NOT_SERVED, score)
    title = str(metadata.get("title") or "").strip()
    artist = str(metadata.get("artist") or "").strip()
    year = _year(metadata.get("year"))
    track = ingest_track({"title": title, "artist": artist, "year": year})
    if track is None:
        return FingerprintResult(file_id, name, Outcome.NO_RECORDING_TAGS, score)

    match = api.lookup_catalog(CatalogLookupRequest(tracks=[track]))[0]
    if match is not None:
        tagged = write_catalog_tags(data, match)
        outcome = Outcome.TAGGED_FROM_CATALOG
    else:
        tagged = write_recording_tags(data, title=title, artist=artist, year=year)
        outcome = Outcome.TAGGED_FROM_MUSICBRAINZ
    if tagged is None:
        return FingerprintResult(file_id, name, Outcome.ALREADY_TAGGED, score, match)

    check_audio_unchanged(data, tagged)
    replace_contents(drive, file_id, name, tagged)
    return FingerprintResult(file_id, name, outcome, score, match)


# ---------------------------------------------------------------------------
# One run
# ---------------------------------------------------------------------------


def fingerprint(
    file_id: str,
    name: str = "",
    *,
    run_id: str | None = None,
    drive: DriveFiles | None = None,
    api: CatalogLookup | None = None,
    identifier: Identifier | None = None,
) -> FingerprintResult | None:
    """Identify one file and report the run.

    ``drive``, ``api`` and ``identifier`` default to the real ones, built
    from the environment. Returns None when the run raised before a result.
    """
    started_at = monotonic()
    result: FingerprintResult | None = None
    with run_report(
        FLOW_NAME,
        repo=REPO,
        # A triggered run that changed nothing is still worth saying so.
        notable=True,
        run_id=run_id,
        version=__version__,
    ) as report:
        try:
            threshold = min_confidence()
            result = identify_file(
                drive if drive is not None else GoogleAPI.from_env().drive,
                api
                if api is not None
                else api_client(base_url=api_base_url().strip() or None),
                identifier
                if identifier is not None
                else build_identifier(threshold=threshold),
                file_id,
                threshold=threshold,
            )
        finally:
            _record(report, result, monotonic() - started_at)
    log.info(
        "deejay-fingerprint: %s -> %s (score %s)",
        result.name if result else name or file_id,
        result.outcome.value if result else "failed",
        f"{result.score:.3f}" if result and result.score is not None else "n/a",
    )
    return result


def _record(report: Any, result: FingerprintResult | None, duration_sec: float) -> None:
    report.duration_sec = duration_sec
    outcome = result.outcome if result else None
    tagged = outcome in (Outcome.TAGGED_FROM_CATALOG, Outcome.TAGGED_FROM_MUSICBRAINZ)
    matched = outcome is not None and outcome not in UNTOUCHED
    report.ok(1 if tagged else 0)
    report.count("scanned", 1)
    report.count("matched_by_fingerprint", 1 if matched else 0)
    report.count("matched_in_catalog", 1 if result and result.match else 0)
    report.count("not_in_catalog", 1 if matched and result and not result.match else 0)
    report.count("tagged", 1 if tagged else 0)
    report.count("left_untouched", 1 if outcome in UNTOUCHED else 0)
    if result is not None and outcome in UNTOUCHED:
        item = result.name
        if result.score is not None:
            item = f"{item} (best score {result.score:.2f})"
        report.notice(result.outcome.value, item)


# ---------------------------------------------------------------------------
# New music (the new-music flow's files)
# ---------------------------------------------------------------------------


def intake_fingerprinted(
    intake: new_music.Intake,
    api: CatalogLookup,
    identifier: Identifier,
    file_id: str,
    *,
    threshold: float,
) -> None:
    """Identify one new-music file by its fingerprint and, on a confident
    match, take it in with the new-music steps; otherwise send it, unchanged,
    to needs-review.

    A file that is not in the input folder (already taken in, or moved by
    hand) is left alone.
    """
    summary = intake.summary
    meta = file_meta(intake.drive, file_id)
    name = str(meta.get("name") or file_id)
    if meta.get("trashed") or intake.folders.input not in (meta.get("parents") or []):
        summary.no_longer_in_input.append(name)
        return

    downloaded = intake.drive.download_file_bytes(file_id)
    name = str(getattr(downloaded, "name", "") or name)
    data: bytes = downloaded.data
    if not new_music.is_mp3(downloaded, name):
        intake.review(file_id, name, None, "not an MP3: only ID3 tags are written")
        return

    try:
        result = _identify(identifier, name, data)
    except AudioUnreadable as exc:
        intake.review(file_id, name, data, f"fpcalc could not read the audio: {exc}")
        return
    chosen = result.chosen
    score = float(chosen.confidence or 0.0) if chosen is not None else 0.0
    if chosen is None or score < threshold:
        intake.review(
            file_id,
            name,
            data,
            f"fingerprint below confidence (best score {score:.2f}, "
            f"needs {threshold:.2f})",
        )
        return

    metadata = result.metadata or {}
    if metadata.get(NOT_SERVED):
        intake.review(
            file_id,
            name,
            data,
            f"MusicBrainz does not serve the matched recording {metadata.get('_mbid')}",
        )
        return
    title = str(metadata.get("title") or "").strip()
    artist = str(metadata.get("artist") or "").strip()
    year = _year(metadata.get("year"))
    track = ingest_track({"title": title, "artist": artist, "year": year})
    if track is None:
        intake.review(file_id, name, data, "MusicBrainz gave no title or artist")
        return

    match = api.lookup_catalog(CatalogLookupRequest(tracks=[track]))[0]
    if match is not None:
        tagged = write_catalog_tags(data, match)
        by = f"fingerprint ({score:.2f}) + catalog"
    else:
        tagged = write_recording_tags(data, title=title, artist=artist, year=year)
        by = f"fingerprint ({score:.2f}) + MusicBrainz"
    intake.deliver(file_id, name, data, tagged or data, identified_by=by)


def intake_by_fingerprint(
    file_id: str,
    name: str = "",
    *,
    run_id: str | None = None,
    sender_mode: FlowMode = FlowMode.OFF,
    drive: Any | None = None,
    api: CatalogLookup | None = None,
    identifier: Identifier | None = None,
) -> new_music.IntakeSummary:
    """One new-music file's fingerprint run, reported under ``new-music``.

    Runs in the stricter of the sender's mode and ``flags.deejay_cog.new_music``,
    so turning the flow off (or to dry-run) also stops files already queued.
    """
    mode = stricter(sender_mode, flow_mode(new_music.MODE_FLAG))
    log.info("deejay-fingerprint: new-music file %s in %s", name or file_id, mode.value)

    client = (
        api if api is not None else api_client(base_url=api_base_url().strip() or None)
    )

    def work(intake: new_music.Intake) -> None:
        intake.summary.scanned = 1
        threshold = min_confidence()
        try:
            intake_fingerprinted(
                intake,
                client,
                identifier
                if identifier is not None
                else build_identifier(threshold=threshold),
                file_id,
                threshold=threshold,
            )
        except AudioChanged:
            intake.summary.failed.append(name or file_id)
            raise
        except TagEmptied as exc:
            intake.summary.failed.append(f"{name or file_id} ({exc})")

    return new_music.run_intake(
        stage="fingerprint",
        run_id=run_id,
        mode=mode,
        work=work,
        drive=drive,
        library=client,  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# Library files (the nightly library run's unidentified files)
# ---------------------------------------------------------------------------


#: A library file the fingerprint could not identify, linked by its own tags.
LINKED_BY_TAGS = "linked_by_tags"


@dataclass
class LibraryIdentifyResult:
    """One library file's outcome."""

    path: str
    outcome: str
    score: float | None = None
    catalog_id: str = ""
    entry_created: bool = False
    written: bool = False
    would_write: bool = False
    filled: tuple[str, ...] = ()


def identify_library_file(
    drive: Any,
    api: Any,
    identifier: Identifier,
    record: RunRecord,
    *,
    file_id: str,
    root: str,
    path: str,
    threshold: float,
    search: TrackSearch | None = None,
) -> LibraryIdentifyResult:
    """Identify one library file by its fingerprint and, on a confident
    match, link it to its catalog entry (made a library entry when the
    catalog has no song for it). No MusicBrainz tags are written; below
    ``threshold`` nothing changes. A match that disagrees with the file's
    own tags is linked by whichever side a second opinion backs
    (:mod:`deejay_cog.second_opinion`, with ``search`` as its Spotify), and
    otherwise left for the owner."""
    meta = file_meta(drive, file_id)
    if meta.get("trashed"):
        return LibraryIdentifyResult(path, "in_trash")
    downloaded = drive.download_file_bytes(file_id)
    name = str(meta.get("name") or getattr(downloaded, "name", "") or file_id)
    data: bytes = downloaded.data
    if not _is_mp3(downloaded, name):
        return LibraryIdentifyResult(path, Outcome.NOT_MP3.value)

    # The best match's score, once there is one; read when a note is made.
    score: float | None = None

    def by_own_tags(why: str) -> LibraryIdentifyResult | None:
        """Link the file by its own tags; None when they name no song."""
        by_tags = library_identify.link_by_own_tags(
            drive,
            api,
            record,
            file_id=file_id,
            name=name,
            root=root,
            path=path,
            data=data,
            why=why,
        )
        if by_tags is None:
            return None
        if by_tags.linked is None:
            return LibraryIdentifyResult(path, "would_create_entry_from_tags", score)
        return LibraryIdentifyResult(
            path,
            LINKED_BY_TAGS,
            score,
            by_tags.catalog_id,
            by_tags.entry_created,
            by_tags.linked.written,
            by_tags.linked.would_write,
            tuple(by_tags.linked.filled),
        )

    def unidentified(outcome: str, note: str) -> LibraryIdentifyResult:
        if record.mode.writes:
            library_identify.record_unidentified(
                api, file_id=file_id, root=root, path=path, data=data, note=note
            )
        return LibraryIdentifyResult(path, outcome, score)

    try:
        result = _identify(identifier, name, data)
    except AudioUnreadable as exc:
        return unidentified(
            Outcome.AUDIO_UNREADABLE.value,
            f"fingerprint: the audio could not be read ({exc}); left for review",
        )
    chosen = result.chosen
    score = float(chosen.confidence or 0.0) if chosen is not None else 0.0
    metadata = result.metadata or {}
    title = str(metadata.get("title") or "").strip()
    artist = str(metadata.get("artist") or "").strip()

    if chosen is None or score < threshold:
        guess = f"{artist} - {title}" if title and artist else ""
        if chosen is None:
            note = "fingerprint: no match"
        elif guess:
            note = f"fingerprint: no confident match (best guess {guess}, score {score:.2f})"
        else:
            note = f"fingerprint: no confident match (score {score:.2f})"
        # Nothing says the file's own tags are wrong, and the fingerprint
        # never will: they make its entry when they name a song.
        by_tags = by_own_tags(note)
        if by_tags is None:
            return unidentified(Outcome.BELOW_CONFIDENCE.value, note)
        return by_tags
    if metadata.get(NOT_SERVED):
        return unidentified(
            Outcome.RECORDING_NOT_SERVED.value,
            f"fingerprint: matched recording {metadata.get('_mbid')} (score {score:.2f}), "
            "which MusicBrainz does not serve; left for review",
        )
    year = _year(metadata.get("year"))
    track = ingest_track({"title": title, "artist": artist, "year": year})
    if track is None:
        return unidentified(
            Outcome.NO_RECORDING_TAGS.value,
            f"fingerprint: matched (score {score:.2f}) but the recording has no "
            "title or artist",
        )

    # A file whose own tags name a song is linked only when the recording
    # found is that song: AcoustID's links to recordings are crowd-sourced
    # and can be wrong, and its answer must not retitle a file that already
    # said what it is.
    own = read_tag_row(data)
    identified_by = f"fingerprint ({score:.2f}) + catalog"
    if ingest_track(own) is not None and not tags_agree(
        own["title"], own["artist"], title, artist
    ):
        disagreement = (
            f"fingerprint: {artist} - {title} (score {score:.2f}) disagrees with "
            f"the file's own tags ({own['artist']} - {own['title']})"
        )
        # Neither side wins by itself: a second source decides, or the owner.
        verdict = second_opinion(
            own["title"],
            own["artist"],
            title,
            artist,
            getattr(identifier, "heard", None),
            search,
        )
        if verdict.side == "tags":
            by_tags = by_own_tags(f"{disagreement}; {verdict.why}")
            if by_tags is not None:
                return by_tags
        if verdict.side != "fingerprint":
            return unidentified(
                DISAGREES_WITH_TAGS,
                f"{disagreement}; {verdict.why}; left for review",
            )
        identified_by = f"fingerprint ({score:.2f}) + catalog; {verdict.why}"

    match = api.lookup_catalog(CatalogLookupRequest(tracks=[track]))[0]
    created = False
    if match is not None:
        catalog_id = str(match.id)
    elif record.mode.writes:
        made = api.create_library_entry(
            LibraryEntryCreate(
                title=title,
                artist=artist,
                release_year=int(year) if year else None,
            )
        )
        catalog_id, created = str(made.entry.id), made.created
    else:
        return LibraryIdentifyResult(path, "would_create_entry", score)

    linked = library_identify.link_and_write(
        drive,
        api,
        record,
        file_id=file_id,
        name=name,
        root=root,
        path=path,
        data=data,
        catalog_id=catalog_id,
        identified_by=identified_by,
    )
    return LibraryIdentifyResult(
        path,
        "linked",
        score,
        catalog_id,
        created,
        linked.written,
        linked.would_write,
        tuple(linked.filled),
    )


def library_identify_by_fingerprint(
    file_id: str,
    name: str = "",
    *,
    root: str,
    path: str,
    run_id: str | None = None,
    sender_mode: FlowMode = FlowMode.OFF,
    drive: Any | None = None,
    api: Any | None = None,
    identifier: Identifier | None = None,
    search: TrackSearch | None = None,
) -> LibraryIdentifyResult | None:
    """One library file's fingerprint run, reported under
    ``library-identify``, in the stricter of the sender's mode and
    ``flags.deejay_cog.library_identify``."""
    mode = stricter(sender_mode, flow_mode(library_identify.MODE_FLAG))
    started_at = monotonic()
    result: LibraryIdentifyResult | None = None
    if mode is FlowMode.OFF:
        log.info("deejay-fingerprint: library file %s skipped: off", path or file_id)
        return None
    # Not notable: a run that went as expected is logged, not sent (see
    # _record_library); an issue still sends.
    with run_report(
        library_identify.FLOW_NAME,
        repo=REPO,
        run_id=run_id,
        version=__version__,
    ) as report:
        record: RunRecord | None = None
        try:
            state = os.getenv(STATE_FOLDER_VAR, "").strip()
            if not state:
                report.issue("setting", f"{STATE_FOLDER_VAR} is not set")
                return None
            if drive is None:
                drive = GoogleAPI.from_env().drive
            client = (
                api
                if api is not None
                else api_client(base_url=api_base_url().strip() or None)
            )
            threshold = min_confidence()
            record = RunRecord(
                library_identify.FLOW_NAME,
                run_id or file_id,
                mode,
                stage="fingerprint",
                folders={"root": root, "state": state},
            ).attach(drive, state)
            result = identify_library_file(
                drive,
                client,
                identifier
                if identifier is not None
                else build_identifier(threshold=threshold),
                record,
                file_id=file_id,
                root=root,
                path=path or name or file_id,
                threshold=threshold,
                search=search if search is not None else build_track_search(),
            )
        except AudioChanged as exc:
            report.issue("audio_changed", f"{path}: {exc}")
            raise
        finally:
            if record is not None and record.entries:
                record.finish("complete" if result else "failed")
            _record_library(report, mode, result, monotonic() - started_at)
    return result


def _record_library(
    report: Any,
    mode: FlowMode,
    result: LibraryIdentifyResult | None,
    duration_sec: float,
) -> None:
    """Count one library file's fingerprint run, and log what it did.

    The library identify step sends a whole chunk to fingerprinting at once,
    one run here per file, so a run that went as expected records counts
    only and is not sent: its file is in the run record, and the next
    nightly identify report counts what is still unidentified
    (``fingerprinted_unlinked``). Only an issue (a missing setting, a
    changed audio check, an exception) reaches the channel.
    """
    report.duration_sec = duration_sec
    if result is None:
        return
    item = result.path
    if result.score is not None:
        item = f"{item} (score {result.score:.2f})"
    report.count("scanned", 1)
    report.count("linked", 1 if result.outcome == "linked" else 0)
    report.count("linked_by_tags", 1 if result.outcome == LINKED_BY_TAGS else 0)
    report.count("entry_created", 1 if result.entry_created else 0)
    report.count("tags_written", 1 if result.written else 0)
    details = [result.outcome]
    if mode is FlowMode.DRY_RUN:
        details.append("dry run")
    if result.entry_created:
        details.append("library entry created")
    if result.written:
        details.append("tags written")
    if result.filled:
        details.append(f"catalog filled up ({', '.join(result.filled)})")
    log.info("library-identify fingerprint: %s: %s", item, "; ".join(details))


# ---------------------------------------------------------------------------
# The Lambda entrypoint
# ---------------------------------------------------------------------------


def effective_settings() -> dict[str, Any]:
    """Every setting this function reads, by name, as resolved."""
    return {
        "RETAG_MIN_CONFIDENCE": min_confidence(),
        "ACOUSTID_API_KEY": os.getenv("ACOUSTID_API_KEY", "").strip(),
        **{var: os.getenv(var, "").strip() for var in new_music.FOLDER_VARS.values()},
    }


# Once per cold start: the settings in effect, with a value only for the
# names in SETTINGS_ALLOWLIST. A setting that will not parse fails its run
# as before, not the cold start.
try:
    log_settings(effective_settings())
except Exception:  # noqa: BLE001 — the report is not the job
    log.exception("deejay-fingerprint: could not log the effective settings")


def process_message(body: str, *, run_id: str) -> None:
    """Run the file one message names. Raises if it must be redelivered.

    A redelivered file is identified again from the start. That is safe: a
    file already written is not written again (its tags already say so).
    """
    message = parse_message(body, FingerprintJobMessage)
    payload = message.payload
    if payload.flow == "new-music":
        intake_by_fingerprint(
            payload.drive_file_id,
            payload.name,
            run_id=run_id,
            sender_mode=parse_mode(payload.mode, setting="message mode"),
        )
        return
    if payload.flow == "library-identify":
        if not payload.library_root_id or not payload.path:
            raise UnprocessableMessage(
                "a library-identify message names its library_root_id and path"
            )
        library_identify_by_fingerprint(
            payload.drive_file_id,
            payload.name,
            root=payload.library_root_id,
            path=payload.path,
            run_id=run_id,
            sender_mode=parse_mode(payload.mode, setting="message mode"),
        )
        return
    fingerprint(payload.drive_file_id, payload.name, run_id=run_id)


def _attempt(record: dict[str, Any]) -> str:
    """Which receive of this message this is, or "?" when unknown."""
    return str((record.get("attributes") or {}).get("ApproximateReceiveCount", "?"))


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Run each record's file, and name the records that must come back.

    Never raises; anything left out of ``batchItemFailures`` is deleted by
    the event source mapping, so a record whose run raised is named there.
    """
    # Settings as Doppler holds them now, as the deejay worker reads them.
    try:
        load_secrets(refresh=True)
    except Exception:  # noqa: BLE001 — nothing can run without its settings
        log.exception("deejay-fingerprint: could not refresh settings from SSM")
        return all_failed(event)

    def handle(record: dict[str, Any]) -> None:
        message_id = str(record.get("messageId") or "")
        body = record.get("body") or ""
        attempt = _attempt(record)
        with timing.invocation(
            cog="deejay", mode="fingerprint", attempt=attempt
        ) as timed:
            try:
                with deadline(
                    context,
                    margin_seconds=DEADLINE_MARGIN_SECONDS,
                    request_headroom_seconds=REQUEST_HEADROOM_SECONDS,
                ):
                    process_message(body, run_id=message_id)
            except UnprocessableMessage:
                timed.label(outcome="unprocessable")
                raise
            except (RunOutOfTime, RunDeadlineReached):
                # The run's report already says it stopped early.
                timed.label(outcome="deadline")
                log.error(
                    "deejay-fingerprint: stopped at the deadline (attempt %s)", attempt
                )
                raise
            except Exception:
                # The run's report already carries the exception.
                timed.label(outcome="failed")
                log.exception("deejay-fingerprint: run failed (attempt %s)", attempt)
                raise
            timed.label(outcome="ok")

    def on_unprocessable(exc: UnprocessableMessage, record: dict[str, Any]) -> None:
        # Once, on the first receive; the dead-letter queue has its own alarm.
        if _attempt(record) not in ("1", "?"):
            return
        try:
            post_run_finding(
                FLOW_NAME,
                "ERROR",
                f"an unprocessable message failed: {exc}",
                source="queue_consumer",
                run_id=str(record.get("messageId") or ""),
            )
        except Exception:  # noqa: BLE001 — the notification is not the job
            log.exception("deejay-fingerprint: could not report the failure")

    return process_records(event, handle, on_unprocessable=on_unprocessable)
