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
than the file being reported as unidentified.

Each message is one run with its own report, under the message id, as the
deejay worker's runs are.
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
from googleapiclient.http import DEFAULT_HTTP_TIMEOUT_SEC
from mini_app_polis import load_secrets, timing
from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import CatalogLookupRequest, CatalogMatch
from mini_app_polis.deadline import RunDeadlineReached
from mini_app_polis.environment import api_base_url
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

from deejay_cog._pipeline_eval import REPO, post_run_finding
from deejay_cog._version import __version__
from deejay_cog.api_client import api_client
from deejay_cog.fingerprint_jobs import FingerprintJobMessage
from deejay_cog.music_tags import (
    MP3_MIME_TYPE,
    CatalogLookup,
    DriveFiles,
    ingest_track,
    replace_contents,
    write_catalog_tags,
    write_recording_tags,
)

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


class AcoustIdError(RuntimeError):
    """AcoustID refused a lookup."""


class AcoustIdRateLimited(AcoustIdError):  # noqa: N818 — named for what happened
    """AcoustID refused a lookup as over its rate limit, every time it was tried."""


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

    NOT_MP3 = "not_mp3"
    """Not an MP3, so it has no ID3 tags to write. Not changed."""


#: The outcomes that leave a file as it was, to be tagged by hand.
UNTOUCHED = frozenset(
    {Outcome.BELOW_CONFIDENCE, Outcome.NO_RECORDING_TAGS, Outcome.NOT_MP3}
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
    ) -> IdentificationResult: ...


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
        raise RuntimeError(
            f"fpcalc exited with status {proc.returncode}: {proc.stderr.strip()}"
        )
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

    def identify(self, path: str) -> list[TrackId]:
        duration, fingerprint = run_fpcalc(path, fpcalc=self._fpcalc)
        data = self._lookup_answer(fingerprint, duration)
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
    raw = os.getenv("RETAG_MIN_CONFIDENCE", "").strip()
    return float(raw) if raw else DEFAULT_MIN_CONFIDENCE


def build_identifier(*, threshold: float) -> Mp3Identifier:
    """An ``Mp3Identifier`` on fpcalc, AcoustID and MusicBrainz, with
    MusicBrainz asked only for a match that reaches ``threshold``."""
    api_key = os.getenv("ACOUSTID_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("ACOUSTID_API_KEY is not set")
    return Mp3Identifier(
        acoustid_identifier=AcoustIdLookup(api_key, fpcalc=find_fpcalc()),
        musicbrainz_provider=MusicBrainzRecordingProvider(
            app_name=REPO, app_version=__version__, contact=MUSICBRAINZ_CONTACT
        ),
        policy=IdentificationPolicy(
            min_confidence=threshold,
            max_candidates=MAX_CANDIDATES,
            fetch_metadata_min_confidence=threshold,
        ),
    )


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

    result = _identify(identifier, name, data)
    chosen = result.chosen
    score = float(chosen.confidence or 0.0) if chosen is not None else 0.0
    if chosen is None or score < threshold:
        return FingerprintResult(file_id, name, Outcome.BELOW_CONFIDENCE, score)

    metadata = result.metadata or {}
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
# The Lambda entrypoint
# ---------------------------------------------------------------------------


def process_message(body: str, *, run_id: str) -> None:
    """Run the file one message names. Raises if it must be redelivered.

    A redelivered file is identified again from the start. That is safe: a
    file already written is not written again (its tags already say so).
    """
    message = parse_message(body, FingerprintJobMessage)
    fingerprint(message.payload.drive_file_id, message.payload.name, run_id=run_id)


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
