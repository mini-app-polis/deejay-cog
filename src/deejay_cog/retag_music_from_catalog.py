"""The ``retag-music`` worker mode: tag the upload folder's files from the
catalog (ADR-0007, stage 1).

Lists the audio files in ``MUSIC_UPLOAD_SOURCE_FOLDER_ID`` and runs
:func:`deejay_cog.music_tags.tag_file` on each, at most
``MAX_UPLOADS_PER_RUN`` a run. A file the catalog has no song for is left
as it is and named in the run report. A file with no usable title or artist
is sent to the fingerprint function (ADR-0007, stage 2), one message per
file on ``DEEJAY_FINGERPRINT_QUEUE_URL``; with no queue configured it is
named in the report instead.

Between files the run checks the shared deadline and stops with
``RunOutOfTime``, as the other modes do, so the report still goes out and
the redelivery sweeps what is left (a tagged file is not written again).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from time import monotonic
from typing import Any

from mini_app_polis import logger as logger_mod
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI
from mini_app_polis.lambda_worker import RunOutOfTime
from mini_app_polis.pipeline_status import run_report

import deejay_cog.config as config
from deejay_cog._pipeline_eval import REPO
from deejay_cog._shared_deadline import shared_deadline_passed
from deejay_cog._version import __version__
from deejay_cog.api_client import api_client
from deejay_cog.fingerprint_jobs import FingerprintJobs, fingerprint_jobs_from_env
from deejay_cog.music_tags import CatalogLookup, DriveFiles, Outcome, tag_file

log = logger_mod.get_logger()

FLOW_NAME = "retag-music"

#: Files per run when ``MAX_UPLOADS_PER_RUN`` is unset; 0 means no cap.
DEFAULT_MAX_UPLOADS_PER_RUN = 200

_AUDIO_MIME_TYPES = (
    "audio/mpeg",
    "audio/mp4",
    "audio/x-m4a",
    "audio/wav",
    "audio/x-wav",
    "audio/flac",
    "audio/aac",
    "audio/ogg",
    "audio/x-aiff",
    "audio/aiff",
)


@dataclass
class RetagSummary:
    """What one run did, file by file."""

    scanned: int = 0
    tagged: int = 0
    already_tagged: int = 0
    not_in_catalog: list[str] = field(default_factory=list)
    needs_fingerprinting: list[str] = field(default_factory=list)
    handed_to_fingerprinting: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)

    @property
    def matched_in_catalog(self) -> int:
        """Files found in the catalog: tagged now or already tagged."""
        return self.tagged + self.already_tagged


def _list_audio_files(g: GoogleAPI, folder_id: str) -> list[Any]:
    """The audio files directly in a Drive folder, each once."""
    files: list[Any] = []
    seen: set[str] = set()
    for mime_type in _AUDIO_MIME_TYPES:
        for f in g.drive.list_files(
            parent_id=folder_id, mime_type=mime_type, trashed=False
        ):
            file_id = getattr(f, "id", None)
            if file_id and file_id not in seen:
                seen.add(file_id)
                files.append(f)
    return files


def _max_uploads_per_run() -> int:
    raw = os.getenv("MAX_UPLOADS_PER_RUN", "").strip()
    return int(raw) if raw else DEFAULT_MAX_UPLOADS_PER_RUN


def tag_files(
    drive: DriveFiles,
    api: CatalogLookup,
    files: list[Any],
    summary: RetagSummary,
    *,
    max_files: int,
    fingerprint_jobs: FingerprintJobs | None = None,
) -> None:
    """Tag up to ``max_files`` of ``files`` (all of them when 0), recording
    into ``summary`` as it goes. A file with no usable tags is sent to
    ``fingerprint_jobs`` when given.

    Raises ``RunOutOfTime`` between files once the shared deadline has
    passed; ``summary`` then holds what was done. A file that raises is
    counted as failed and the sweep goes on.
    """
    for f in files:
        if max_files > 0 and summary.scanned >= max_files:
            log.info("retag-music: reached %d files for this run", max_files)
            break
        name = str(getattr(f, "name", "") or getattr(f, "id", ""))
        # Between files: past the deadline every Drive call is refused, and
        # the handler below would count each refusal as that file's failure.
        if shared_deadline_passed():
            raise RunOutOfTime(f"run deadline reached before {name}")
        summary.scanned += 1
        try:
            result = tag_file(drive, api, f.id)
        except Exception as exc:  # noqa: BLE001 — one file must not end the sweep
            if shared_deadline_passed():
                raise RunOutOfTime(f"run deadline reached processing {name}") from exc
            log.exception("retag-music: failed on %s", name)
            summary.failed.append(name)
            continue
        name = result.name
        if result.outcome is Outcome.TAGGED:
            summary.tagged += 1
        elif result.outcome is Outcome.ALREADY_TAGGED:
            summary.already_tagged += 1
        elif result.outcome is Outcome.NOT_IN_CATALOG:
            summary.not_in_catalog.append(name)
        elif fingerprint_jobs is None:
            summary.needs_fingerprinting.append(name)
        else:
            try:
                fingerprint_jobs.send(result.file_id, name)
            except Exception:  # noqa: BLE001 — one file must not end the sweep
                log.exception(
                    "retag-music: could not queue %s for fingerprinting", name
                )
                summary.failed.append(name)
                continue
            summary.handed_to_fingerprinting.append(name)


def retag_music(*, run_id: str | None = None) -> RetagSummary:
    """Tag the upload folder's files from the catalog and report the run.

    ``run_id`` is the queue message id when the Lambda worker runs this.
    """
    started_at = monotonic()
    summary = RetagSummary()
    with run_report(
        FLOW_NAME,
        repo=REPO,
        # A triggered run that found nothing is still worth saying so.
        notable=True,
        run_id=run_id,
        version=__version__,
    ) as report:
        try:
            g = GoogleAPI.from_env()
            files = _list_audio_files(g, config.MUSIC_UPLOAD_SOURCE_FOLDER_ID)
            tag_files(
                g.drive,
                api_client(base_url=api_base_url().strip() or None),
                files,
                summary,
                max_files=_max_uploads_per_run(),
                fingerprint_jobs=fingerprint_jobs_from_env(),
            )
        finally:
            _record(report, summary, monotonic() - started_at)
    return summary


def _record(report: Any, summary: RetagSummary, duration_sec: float) -> None:
    report.duration_sec = duration_sec
    report.ok(summary.tagged)
    report.count("scanned", summary.scanned)
    report.count("matched_in_catalog", summary.matched_in_catalog)
    report.count("not_in_catalog", len(summary.not_in_catalog))
    report.count("tagged", summary.tagged)
    report.count("needing_fingerprinting", len(summary.needs_fingerprinting))
    report.count("handed_to_fingerprinting", len(summary.handed_to_fingerprinting))
    report.count("failed", len(summary.failed))
    for name in summary.not_in_catalog:
        report.notice("not_in_catalog", name)
    for name in summary.needs_fingerprinting:
        report.notice("needs_fingerprinting", name)
    for name in summary.failed:
        report.issue("failed", name)


if __name__ == "__main__":
    retag_music()
