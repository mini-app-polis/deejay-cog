"""
retag_music — WORK IN PROGRESS, LOCAL ONLY

Not served by main.py. Blocked on system dependencies (ffmpeg, fpcalc
from libchromaprint-tools) which are not currently available in the
Railway deploy environment. See module body docstring for details.

Run locally: uv run python -m deejay_cog.retag_music
(requires ffmpeg and fpcalc binaries on PATH)

Findings from this module are never posted to pipeline_evaluations.
The failure hook fires and logs locally, but the production_only=False
flag passed to the pipeline_eval helpers prevents any API call.

---

Prefect flow: retag-music

Downloads audio files from a source Google Drive folder, identifies them via
AcoustID → MusicBrainz, writes corrected tags, renames, then uploads to a
destination folder (or updates in place on low-confidence matches).

Env vars (see config.py for defaults):
  MUSIC_UPLOAD_SOURCE_FOLDER_ID   – Drive folder to read from
  MUSIC_TAGGING_OUTPUT_FOLDER_ID  – Drive folder to write identified files to
  ACOUSTID_API_KEY                – AcoustID application API key
  MAX_UPLOADS_PER_RUN             – optional ceiling (default 200)

System dependencies (must be present in the runtime environment):
  ffmpeg   – required by pyacoustid for audio decoding
  fpcalc   – required by pyacoustid for audio fingerprinting
             (provided by the chromaprint / libchromaprint-tools package)

  These are NOT standard Railway/Python deps and must be installed separately.
  On Ubuntu/Debian: sudo apt-get install -y ffmpeg libchromaprint-tools
  When registering the retag-music deployment, confirm both binaries are
  available in the Railway environment (e.g. via a custom Dockerfile or
  nixpacks config). The other deejay-cog flows do not require these.
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, field
from typing import Any

from mini_app_polis import logger as logger_mod
from mini_app_polis.google import GoogleAPI
from mini_app_polis.mp3.identify import IdentificationPolicy, Mp3Identifier
from mini_app_polis.mp3.rename import Mp3Renamer
from mini_app_polis.mp3.tag import Mp3Tagger
from prefect import flow, task

import deejay_cog.config as config
from deejay_cog._pipeline_eval import (
    get_prefect_logger,
    make_failure_hook,
    post_run_finding,
)

log = logger_mod.get_logger()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _print_all_tags(logger, tagger: Mp3Tagger, path: str) -> None:
    printed = tagger.dump(path)
    if not printed:
        return
    logger.info("[FILE] %s", os.path.basename(path))
    for k in sorted(printed.keys()):
        v = printed.get(k, "") or ""
        logger.info("  [TAG] %s = %s", k, v)


def _list_music_files(g: GoogleAPI, folder_id: str) -> list[Any]:
    """List likely-audio files in a Drive folder."""
    mime_types = [
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
    ]
    files: list[Any] = []
    seen: set[str] = set()
    for mt in mime_types:
        for f in g.drive.list_files(parent_id=folder_id, mime_type=mt, trashed=False):
            fid = getattr(f, "id", None)
            if not fid or fid in seen:
                continue
            seen.add(fid)
            files.append(f)

    if not files:
        files = g.drive.list_files(parent_id=folder_id, trashed=False)

    return files


def _format_candidate_summary(candidate: Any) -> str:
    confidence = getattr(candidate, "confidence", None)
    confidence_str = f"{confidence:.3f}" if confidence is not None else "N/A"
    mbid = getattr(candidate, "mbid", "") or getattr(candidate, "recording_id", "")
    title = getattr(candidate, "title", "") or ""
    artist = getattr(candidate, "artist", "") or ""
    parts = []
    if mbid:
        parts.append(f"id={mbid}")
    if title:
        parts.append(f"title={title}")
    if artist:
        parts.append(f"artist={artist}")
    info = ", ".join(parts)
    return f"confidence={confidence_str}" + (f", {info}" if info else "")


def _format_metadata_summary(metadata: Any) -> str:
    parts = []
    for attr in ("title", "artist", "year"):
        val = getattr(metadata, attr, "") or ""
        if val:
            parts.append(f"{attr}={val}")
    return ", ".join(parts)


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------


@dataclass
class RetagSummary:
    """TODO: describe this class."""
    scanned: int = 0
    downloaded: int = 0
    identified: int = 0
    tagged: int = 0
    uploaded: int = 0
    deleted: int = 0
    failed: int = 0
    skipped: int = field(default=0)


@task(name="retag-music-file", retries=2, retry_delay_seconds=15)
def retag_music_file(
    g: GoogleAPI,
    file: Any,
    *,
    identifier: Mp3Identifier,
    tagger: Mp3Tagger,
    renamer: Mp3Renamer,
    dest_folder_id: str,
    min_confidence: float,
) -> dict[str, int]:
    """
    Process a single audio file:
      - download → identify → tag → rename → upload/update-in-place

    Returns a delta dict compatible with RetagSummary fields.
    """
    logger = get_prefect_logger()
    delta = {
        "downloaded": 0,
        "identified": 0,
        "tagged": 0,
        "uploaded": 0,
        "deleted": 0,
        "failed": 0,
    }

    file_id = getattr(file, "id", None)
    name = getattr(file, "name", "unknown")

    if not file_id:
        logger.info("[SKIP] Missing file id for %r", name)
        return delta

    temp_path = os.path.join(tempfile.gettempdir(), f"{file_id}_{name}")
    path_out = temp_path

    try:
        logger.info("[DOWNLOAD] %s (%s) -> %s", name, file_id, temp_path)
        g.drive.download_file(file_id, temp_path)
        delta["downloaded"] += 1

        logger.info("[PRE-EXISTING-TAGS]------------------")
        _print_all_tags(logger, tagger, temp_path)

        id_result = identifier.identify(temp_path, fetch_metadata=True)

        candidates = getattr(id_result, "candidates", []) or []
        chosen = getattr(id_result, "chosen", None)
        chosen_summary = _format_candidate_summary(chosen) if chosen else "None"
        metadata_present = bool(getattr(id_result, "metadata", None))
        logger.info(
            "[IDENTIFY] candidates=%d, chosen=(%s), metadata_fetched=%s",
            len(candidates),
            chosen_summary,
            metadata_present,
        )

        chosen_conf = (
            float(getattr(chosen, "confidence", 0.0)) if chosen is not None else 0.0
        )
        identified = chosen is not None and chosen_conf >= float(min_confidence)

        desired_filename = os.path.basename(temp_path)

        if id_result.metadata:
            metadata_summary = _format_metadata_summary(id_result.metadata)
            logger.info(
                "[TAGGING] confidence=%.3f, metadata=(%s)",
                chosen_conf,
                metadata_summary,
            )
            tagger.write(path_out, id_result.metadata, ensure_virtualdj_compat=True)
            logger.info("[TAGGING-DONE]")
            _print_all_tags(logger, tagger, temp_path)
            delta["tagged"] += 1

            rename_result = renamer.apply(path_out, metadata=id_result.metadata)
            old_basename = os.path.basename(path_out)
            path_out = rename_result.dest_path
            desired_filename = rename_result.dest_name
            logger.info("[RENAME] %s -> %s", old_basename, os.path.basename(path_out))

        if not identified:
            reason = (
                "no_candidates"
                if chosen is None
                else f"low_confidence:{chosen_conf:.3f}"
            )
            logger.info(
                "[DECISION] update_in_place reason=%s chosen_conf=%.3f",
                reason,
                chosen_conf,
            )
            g.drive.update_file(file_id, path_out)
            delta["uploaded"] += 1
            logger.info(
                "[UPLOAD-SOURCE] Updated in place file_id=%s (%s)", file_id, name
            )
            return delta

        delta["identified"] += 1
        logger.info(
            "[DECISION] move_to_dest chosen_conf=%.3f dest_folder_id=%s",
            chosen_conf,
            dest_folder_id,
        )
        g.drive.upload_file(
            path_out, parent_id=dest_folder_id, dest_name=desired_filename
        )
        delta["uploaded"] += 1
        logger.info(
            "[UPLOAD] %s -> dest_folder_id=%s", desired_filename, dest_folder_id
        )

        g.drive.delete_file(file_id)
        delta["deleted"] += 1
        logger.info("[DELETE] Deleted source file_id=%s (%s)", file_id, name)

    except Exception as e:
        delta["failed"] += 1
        logger.error("[ERROR] %s (%s): %s", name, file_id, e, exc_info=True)
    finally:
        try:
            paths = {temp_path}
            if path_out and path_out != temp_path:
                paths.add(path_out)
            for p in paths:
                if p and os.path.exists(p):
                    os.remove(p)
        except Exception:
            pass

    return delta


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(
    name="retag-music",
    description="Download audio files from Drive, identify via AcoustID/MusicBrainz, write tags, and upload.",
    on_failure=[make_failure_hook("retag-music", production_only=False)],
    on_crashed=[make_failure_hook("retag-music", production_only=False)],
)
def retag_music_flow() -> RetagSummary:
    """
    For each audio file in MUSIC_UPLOAD_SOURCE_FOLDER_ID:
      - identify via AcoustID → MusicBrainz
      - write corrected tags and rename
      - move to MUSIC_TAGGING_OUTPUT_FOLDER_ID on high confidence
      - update in place on low/no confidence
    IMPORTANT: This flow requires system binaries `ffmpeg` and `fpcalc`; see module docstring.
    """
    logger = get_prefect_logger()

    acoustid_api_key = os.getenv("ACOUSTID_API_KEY", "").strip()
    if not acoustid_api_key:
        logger.warning("ACOUSTID_API_KEY not set — skipping retag run")
        summary = RetagSummary()
        post_run_finding(
            flow_name="retag-music",
            severity="SUCCESS",
            production_only=False,
            sets_imported=summary.uploaded,
            sets_failed=summary.failed,
            sets_skipped=summary.skipped,
            total_tracks=summary.scanned,
            scanned=summary.scanned,
            downloaded=summary.downloaded,
            identified=summary.identified,
            tagged=summary.tagged,
            uploaded=summary.uploaded,
            deleted=summary.deleted,
            failed=summary.failed,
        )
        return summary

    source_folder_id = config.MUSIC_UPLOAD_SOURCE_FOLDER_ID
    dest_folder_id = config.MUSIC_TAGGING_OUTPUT_FOLDER_ID
    min_confidence: float = float(os.getenv("RETAG_MIN_CONFIDENCE", "0.90"))
    max_candidates: int = int(os.getenv("RETAG_MAX_CANDIDATES", "5"))
    max_uploads_per_run: int = int(os.getenv("MAX_UPLOADS_PER_RUN", "200"))

    g = GoogleAPI.from_env()

    policy = IdentificationPolicy(
        min_confidence=min_confidence,
        max_candidates=max_candidates,
        fetch_metadata_min_confidence=min_confidence,
    )
    identifier = Mp3Identifier.from_env(
        acoustid_api_key=acoustid_api_key, policy=policy
    )
    tagger = Mp3Tagger()
    renamer = Mp3Renamer()

    music_files = _list_music_files(g, source_folder_id)
    logger.info(
        "[START] Found %d music files in source folder (max_uploads_per_run=%d)",
        len(music_files),
        max_uploads_per_run,
    )

    summary = RetagSummary(scanned=0)

    for file in music_files:
        if max_uploads_per_run > 0 and summary.uploaded >= max_uploads_per_run:
            logger.info(
                "[STOP] Reached max uploads per run (%d). Stopping.",
                max_uploads_per_run,
            )
            remaining = len(music_files) - summary.scanned
            summary.skipped += remaining
            break

        summary.scanned += 1

        delta = retag_music_file(
            g,
            file,
            identifier=identifier,
            tagger=tagger,
            renamer=renamer,
            dest_folder_id=dest_folder_id,
            min_confidence=min_confidence,
        )

        for key, val in delta.items():
            setattr(summary, key, getattr(summary, key) + val)

    logger.info(
        "[DONE] scanned=%d downloaded=%d identified=%d tagged=%d uploaded=%d deleted=%d failed=%d",
        summary.scanned,
        summary.downloaded,
        summary.identified,
        summary.tagged,
        summary.uploaded,
        summary.deleted,
        summary.failed,
    )

    _post_kwargs = {
        "flow_name": "retag-music",
        "production_only": False,
        "sets_imported": summary.uploaded,
        "sets_failed": summary.failed,
        "sets_skipped": summary.skipped,
        "total_tracks": summary.scanned,
        "scanned": summary.scanned,
        "downloaded": summary.downloaded,
        "identified": summary.identified,
        "tagged": summary.tagged,
        "uploaded": summary.uploaded,
        "deleted": summary.deleted,
        "failed": summary.failed,
    }
    if summary.failed == 0:
        post_run_finding(severity="SUCCESS", **_post_kwargs)
    else:
        post_run_finding(
            severity="WARN",
            text=(
                f"Completed with issues: failed={summary.failed} "
                f"of scanned={summary.scanned}. "
                "Check AcoustID/MusicBrainz logs for identification failures."
            ),
            **_post_kwargs,
        )
    return summary


if __name__ == "__main__":
    retag_music_flow()
