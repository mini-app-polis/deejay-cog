import contextlib
import os
import re
import sys
from dataclasses import dataclass, field

from mini_app_polis import logger as logger_mod
from mini_app_polis.google import GoogleAPI
from prefect import flow, task

import deejay_cog.config as config
from deejay_cog._pipeline_eval import (
    get_prefect_logger,
    make_failure_hook,
    post_run_finding,
)
from deejay_cog.ingest_to_api import (
    build_ingest_payload,
    read_tracks_from_sheet,
)
from deejay_cog.spotify_sync import (
    get_spotify_client,
    push_playlists_to_api,
    sync_set_to_spotify,
)

log = logger_mod.get_logger()

os.environ.setdefault("CSV_SOURCE_FOLDER_ID", "1t4d_8lMC3ZJfSyainbpwInoDta7n69hC")
os.environ.setdefault("DJ_SETS_FOLDER_ID", "1A0tKQ2DBXI1Bt9h--olFwnBNne3am-rL")

# Retry backoff: zero delay under pytest so retries do not slow the suite.
# Checking sys.modules is reliable at import time; the previously-used
# PYTEST_CURRENT_TEST env var is only set while a test function is
# running, not when this module is first imported during collection.
_INGEST_TO_API_RETRY_DELAY = 0 if "pytest" in sys.modules else 30


@dataclass
class CsvPipelineStats:
    """Counters for a process_new_files run (used for AI evaluation)."""

    sets_attempted: int = 0
    sets_imported: int = 0
    sets_failed: int = 0
    sets_skipped_non_csv: int = 0
    skipped_bad_filename: int = 0
    duplicate_csv: int = 0
    total_tracks: int = 0
    failed_set_labels: list[str] = field(default_factory=list)
    ingest_attempted: int = 0
    ingest_failed: int = 0
    spotify_failed: int = 0
    bad_filename_in_file: int = 0  # post-import path: could not extract date/venue
    ingest_skipped_no_tracks: int = 0
    ingest_skipped_env_missing: int = 0
    #: The API client could not be imported or constructed. Distinct from
    #: ingest_skipped_env_missing, which is a configuration gap, and from
    #: ingest_failed, which means the API answered and said no.
    ingest_client_unavailable: int = 0
    #: Raised before the POST was attempted — reading the sheet or
    #: building the payload. Previously uncounted, because the guard on
    #: the handler below only fires once ingest_attempted has moved.
    ingest_prepare_failed: int = 0
    track_read_failed: int = 0
    #: The Archive move failed after a successful upload. The set is
    #: imported; its CSV is still sitting in the source folder, where the
    #: next run will treat it as a duplicate.
    archive_move_failed: int = 0
    #: Something escaped the ingest or Spotify calls after the set had
    #: already uploaded and been counted. The set IS imported — this is
    #: not sets_failed, and conflating the two is what renamed a
    #: correctly imported CSV to FAILED_.
    post_import_failed: int = 0
    #: A non-CSV file could not be moved out of the input folder, so it
    #: will be seen again every run until someone moves it by hand.
    non_csv_move_failed: int = 0
    #: The duplicate check could not read the destination folder, so
    #: "is this already imported" has no answer this run.
    duplicate_check_failed: int = 0


def _real_issue(stats: CsvPipelineStats) -> bool:
    """Whether this run produced anything a human should look at.

    A set that was never offered to the API is not a set that succeeded.
    ``ingest_skipped_env_missing`` and ``ingest_client_unavailable`` both
    mean the whole ingest path was a no-op for this run, which is
    precisely the state that must not be reported as SUCCESS:
    ``ingest_failed`` stays 0 because nothing was ever attempted, and a
    severity derived only from failures reads that as a clean run.
    """
    return (
        stats.sets_failed > 0
        or stats.ingest_failed > 0
        or stats.ingest_prepare_failed > 0
        or stats.ingest_skipped_env_missing > 0
        or stats.ingest_client_unavailable > 0
        or stats.spotify_failed > 0
        or stats.bad_filename_in_file > 0
        or stats.track_read_failed > 0
        or stats.archive_move_failed > 0
        or stats.non_csv_move_failed > 0
        or stats.duplicate_check_failed > 0
        or stats.post_import_failed > 0
    )


def _warn_parts(stats: CsvPipelineStats) -> list[str]:
    """The ``k=v`` fragments naming what went wrong, in a fixed order."""
    parts: list[str] = []
    if stats.sets_failed:
        parts.append(f"sets_failed={stats.sets_failed}")
    if stats.ingest_failed:
        parts.append(f"ingest_failed={stats.ingest_failed}")
    if stats.ingest_prepare_failed:
        parts.append(f"ingest_prepare_failed={stats.ingest_prepare_failed}")
    if stats.ingest_skipped_env_missing:
        parts.append(
            f"ingest_skipped_env_missing={stats.ingest_skipped_env_missing} "
            "(KAIANO_API_BASE_URL unset — nothing was sent)"
        )
    if stats.ingest_client_unavailable:
        parts.append(
            f"ingest_client_unavailable={stats.ingest_client_unavailable} "
            "(API client could not be built — nothing was sent)"
        )
    if stats.spotify_failed:
        parts.append(f"spotify_failed={stats.spotify_failed}")
    if stats.bad_filename_in_file:
        parts.append(f"bad_filename_in_file={stats.bad_filename_in_file}")
    if stats.track_read_failed:
        parts.append(f"track_read_failed={stats.track_read_failed}")
    if stats.archive_move_failed:
        parts.append(
            f"archive_move_failed={stats.archive_move_failed} "
            "(CSV left in the source folder — next run will see it as a duplicate)"
        )
    if stats.non_csv_move_failed:
        parts.append(
            f"non_csv_move_failed={stats.non_csv_move_failed} "
            "(file left in the input folder and will be retried every run)"
        )
    if stats.duplicate_check_failed:
        parts.append(
            f"duplicate_check_failed={stats.duplicate_check_failed} "
            "(flagged possible_duplicate_ rather than risk a double import)"
        )
    if stats.post_import_failed:
        parts.append(
            f"post_import_failed={stats.post_import_failed} "
            "(set imported; ingest or Spotify raised afterwards)"
        )
    return parts


def _common_eval(stats: CsvPipelineStats) -> dict:
    """The counters carried on the run report, whatever its severity."""
    return {
        "sets_imported": stats.sets_imported,
        "sets_failed": stats.sets_failed,
        "sets_skipped": stats.sets_skipped_non_csv,
        "total_tracks": stats.total_tracks,
        "failed_set_labels": list(stats.failed_set_labels),
        # Not "nothing failed" — "something was attempted and none of it
        # failed". Zero failures out of zero attempts is not a success.
        "api_ingest_success": (
            stats.ingest_attempted > 0
            and stats.ingest_failed == 0
            and stats.ingest_prepare_failed == 0
        ),
        "sets_attempted": stats.sets_attempted,
        "collection_update": False,
        "unrecognized_filename_skips": stats.skipped_bad_filename,
        "duplicate_csv_count": stats.duplicate_csv,
        "ingest_skipped_no_tracks": stats.ingest_skipped_no_tracks,
        "ingest_skipped_env_missing": stats.ingest_skipped_env_missing,
        "ingest_client_unavailable": stats.ingest_client_unavailable,
        "ingest_prepare_failed": stats.ingest_prepare_failed,
        "ingest_attempted": stats.ingest_attempted,
        "ingest_failed": stats.ingest_failed,
        "spotify_failed": stats.spotify_failed,
        "bad_filename_in_file": stats.bad_filename_in_file,
        "track_read_failed": stats.track_read_failed,
        "archive_move_failed": stats.archive_move_failed,
        "non_csv_move_failed": stats.non_csv_move_failed,
        "duplicate_check_failed": stats.duplicate_check_failed,
        "post_import_failed": stats.post_import_failed,
    }


def normalize_prefixes_in_source(drive) -> None:
    """Remove leading status prefixes from files in the CSV source folder.

    If a file name starts with 'FAILED_', 'possible_duplicate_', or 'Copy of ' (case-insensitive),
    this will attempt to rename it to the stripped base name, but only if that target name does
    not already exist in the source folder.

    This function expects the new Drive facade / DriveFacade interface.
    """

    FAILED_PREFIX = "FAILED_"
    POSSIBLE_DUPLICATE_PREFIX = "possible_duplicate_"
    COPY_OF_PREFIX = "Copy of "

    try:
        log.debug("normalize_prefixes_in_source: listing source folder files")
        files = drive.list_files(
            config.CSV_SOURCE_FOLDER_ID, include_folders=False, trashed=False
        )
        log.info(f"normalize_prefixes_in_source: found {len(files)} files to inspect")

        # Build a quick lookup of names already present in the folder
        existing_names = {f.name for f in files if f.name}

        for f in files:
            original_name = f.name or ""
            lower = original_name.lower()
            prefix = None

            if lower.startswith(FAILED_PREFIX.lower()):
                prefix = original_name[: len(FAILED_PREFIX)]
            elif lower.startswith(POSSIBLE_DUPLICATE_PREFIX.lower()):
                prefix = original_name[: len(POSSIBLE_DUPLICATE_PREFIX)]
            elif lower.startswith(COPY_OF_PREFIX.lower()):
                prefix = original_name[: len(COPY_OF_PREFIX)]

            if not prefix:
                continue

            new_name = original_name[len(prefix) :]
            if not new_name:
                log.warning(
                    f"normalize_prefixes_in_source: derived empty new name for {original_name}, skipping"
                )
                continue

            if new_name in existing_names:
                log.info(
                    f"normalize_prefixes_in_source: target name '{new_name}' already exists in source folder — leaving '{original_name}' as-is"
                )
                continue

            try:
                log.info(
                    f"normalize_prefixes_in_source: renaming '{original_name}' -> '{new_name}'"
                )
                drive.rename_file(f.id, new_name)
                # Keep our local set consistent for subsequent checks in this run
                existing_names.discard(original_name)
                existing_names.add(new_name)
            except Exception as e:
                log.error(
                    f"normalize_prefixes_in_source: failed to rename {original_name}: {e}"
                )

    except Exception as e:
        log.error(f"normalize_prefixes_in_source: unexpected error: {e}")


# --- Utility: remove summary file for a given year ---
def remove_summary_file_for_year(g: GoogleAPI, year: str) -> None:
    """Remove the summary sheet for the given year from Drive if it exists."""
    try:
        summary_folder_id = g.drive.ensure_folder(config.DJ_SETS_FOLDER_ID, "Summary")
        summary_name = f"{year} Summary"

        # List files in the Summary folder and delete any exact name matches
        files = g.drive.list_files(
            summary_folder_id, include_folders=False, trashed=False
        )
        for f in files:
            if (f.name or "") == summary_name:
                g.drive.delete_file(f.id)
                log.info(
                    f"🗑️ Deleted existing summary file '{summary_name}' for year {year}"
                )
    except Exception as e:
        log.error(f"Failed to remove summary file for year {year}: {e}")


# --- Utility: check for duplicate base filename in a folder ---
def file_exists_with_base_name(
    g: GoogleAPI, folder_id: str, base_name: str
) -> bool | None:
    """Whether a file with this base name exists — or None if unknown.

    Three answers, not two. Returning False on a failed listing made a
    Drive rate limit indistinguishable from an empty folder, and the
    caller then imported a set it already had. None says "I could not
    find out", which is a different thing from "it is not there" and the
    caller has to decide what to do about it.
    """
    try:
        candidates = g.drive.list_files(folder_id, include_folders=False, trashed=False)
    except Exception as e:
        log.error(f"Error checking for duplicates in folder {folder_id}: {e}")
        return None
    return any(os.path.splitext(f.name or "")[0] == base_name for f in candidates)


def rename_file_as_duplicate(g: GoogleAPI, file_id: str, filename: str) -> None:
    """Rename a file with a possible_duplicate_ prefix to flag it for review."""
    try:
        new_name = f"possible_duplicate_{filename}"
        g.drive.rename_file(file_id, new_name)
        log.info(f"✏️ Renamed original to '{new_name}'")
    except Exception as rename_exc:
        log.error(f"Failed to rename original to possible_duplicate_: {rename_exc}")


def process_non_csv_file(
    g: GoogleAPI,
    file_metadata: dict,
    year: str,
    stats: CsvPipelineStats | None = None,
) -> None:
    """Move a non-CSV file that starts with a year into the correct year folder."""
    filename = file_metadata["name"]
    file_id = file_metadata["id"]
    log.info(f"\n📄 Moving non-CSV file that starts with year: {filename}")
    try:
        year_folder_id = g.drive.ensure_folder(config.DJ_SETS_FOLDER_ID, year)
        base_name = os.path.splitext(filename)[0]
        exists = file_exists_with_base_name(g, year_folder_id, base_name)
        if exists is None and stats is not None:
            stats.duplicate_check_failed += 1
        if exists is not False:
            rename_file_as_duplicate(g, file_id, filename)
            if stats is not None:
                stats.sets_skipped_non_csv += 1
            return

        g.drive.move_file(
            file_id, new_parent_id=year_folder_id, remove_from_parents=True
        )
        log.info(f"📦 Moved original file to {year} subfolder: {filename}")
        remove_summary_file_for_year(g, year)
        if stats is not None:
            stats.sets_skipped_non_csv += 1
    except Exception as e:
        log.error(f"Failed to move non-CSV file {filename}: {e}")
        if stats is not None:
            stats.non_csv_move_failed += 1


def _extract_year_from_filename(filename: str) -> str | None:
    log.debug(f"extract_year_from_filename called with filename: {filename}")
    match = re.match(r"(\d{4})[-_]", filename)
    year = match.group(1) if match else None
    log.debug(f"Extracted year: {year} from filename: {filename}")
    return year


def _extract_date_and_venue(
    base_name: str,
) -> tuple[str, str] | tuple[None, None]:
    """
    Extract date and venue from a base filename.
    Returns (date_str, venue_str) or (None, None) if no match.
    e.g. "2024-01-15 MADjam" → ("2024-01-15", "MADjam")
    """
    match = re.match(r"^(\d{4}-\d{2}-\d{2})\s+(.+)$", base_name.strip())
    if not match:
        return None, None
    return match.group(1), match.group(2)


@task(name="normalize-csv")
def _normalize_csv(file_path: str) -> None:
    """
    Normalize a CSV file before upload.

    This does the following, in order:
    1) Removes a leading `sep=...` line if present (case-insensitive).
    2) Drops empty / whitespace-only lines.
    3) Normalizes runs of whitespace at a text level.

    NOTE: This does NOT parse CSV structure.
    """
    logger = get_prefect_logger()
    logger.debug(f"normalize_csv called with file_path: {file_path} - reading file")

    with open(file_path) as f:
        lines = f.readlines()

    cleaned_lines: list[str] = []
    for i, line in enumerate(lines):
        # Strip whitespace and any UTF-8 BOM (Excel often writes BOM + sep=,)
        raw = line.strip().lstrip("\ufeff")

        # Drop empty lines
        if not raw:
            continue

        # Drop Excel-style separator hint (e.g. "sep=,") if it appears as the first line
        if i == 0 and raw.lower().startswith("sep="):
            logger.info(f"Removed CSV separator hint line: {raw}")
            continue

        cleaned = re.sub(r"\s+", " ", raw)
        cleaned_lines.append(cleaned)

    logger.debug(f"Lines after cleaning: {len(cleaned_lines)}")

    with open(file_path, "w") as f:
        f.write("\n".join(cleaned_lines))

    logger.debug(f"✅ Normalized: {file_path}")


@task(name="upload-to-sheets")
def _upload_csv_to_sheets(
    g: GoogleAPI,
    temp_path: str,
    year_folder_id: str,
    year: str,
    filename: str,
) -> str:
    """Upload normalized CSV as a Google Sheet, apply formatting, invalidate summary."""
    logger = get_prefect_logger()
    sheet_id = g.drive.upload_csv_as_google_sheet(temp_path, parent_id=year_folder_id)
    logger.debug("Uploaded sheet ID: %s", sheet_id)
    g.sheets.formatter.apply_formatting_to_sheet(sheet_id)
    remove_summary_file_for_year(g, year)
    logger.debug("upload-to-sheets complete for %s", filename)
    return sheet_id


@task(
    name="ingest-to-api",
    retries=2,
    retry_delay_seconds=_INGEST_TO_API_RETRY_DELAY,
)
def _ingest_set_to_api(
    spreadsheet_id: str,
    set_date: str,
    venue: str,
    label: str,
    g: GoogleAPI,
    stats: CsvPipelineStats | None = None,
) -> None:
    """
    Send a single newly processed set to deejay-marvel-api.
    Skips gracefully if KAIANO_API_BASE_URL is not set.
    Logs success or failure but never raises — pipeline must continue.
    """
    import os as _os

    logger = get_prefect_logger()

    if not _os.environ.get("KAIANO_API_BASE_URL"):
        logger.warning(
            "KAIANO_API_BASE_URL not set — skipping API ingest for %s", label
        )
        if stats is not None:
            stats.ingest_skipped_env_missing += 1
        return

    try:
        from mini_app_polis.api.errors import KaianoApiError  # type: ignore

        from .api_client import api_client
    except Exception as e:
        logger.warning(
            "⚠️ API client not available; skipping ingest for %s: %s", label, e
        )
        if stats is not None:
            stats.ingest_client_unavailable += 1
        return

    try:
        tracks = read_tracks_from_sheet(g, spreadsheet_id)
        payload = build_ingest_payload(
            set_date=set_date,
            venue=venue,
            source_file=label,
            tracks=tracks,
        )
        final_tracks = payload.get("tracks") or []
        if not final_tracks:
            logger.warning("⚠️ No tracks to ingest for %s", label)
            if stats is not None:
                stats.ingest_skipped_no_tracks += 1
            return

        if stats is not None:
            stats.ingest_attempted += 1
        client = api_client()
        client.post("/v1/ingest", payload)
        logger.info("✅ Ingested to API: %s (%d tracks)", label, len(final_tracks))
    except KaianoApiError as e:
        if stats is not None:
            stats.ingest_failed += 1
        logger.error("❌ API ingest failed for %s: %s", label, e)
    except Exception as e:
        if stats is not None:
            # Which side of the POST this landed on decides which counter
            # moves, but both are failures. The old guard counted only the
            # first case and dropped the second on the floor.
            if stats.ingest_attempted > 0:
                stats.ingest_failed += 1
            else:
                stats.ingest_prepare_failed += 1
        logger.error("❌ Unexpected error during API ingest for %s: %s", label, e)


@task(name="sync-to-spotify")
def _sync_set_to_spotify(
    sheet_id: str,
    set_name: str,
    label: str,
    g: GoogleAPI,
    stats: CsvPipelineStats | None = None,
) -> None:
    logger = get_prefect_logger()

    spotify_env_ok = all(
        os.environ.get(name)
        for name in (
            "SPOTIPY_CLIENT_ID",
            "SPOTIPY_CLIENT_SECRET",
            "SPOTIPY_REFRESH_TOKEN",
        )
    )
    if not spotify_env_ok:
        logger.warning(
            "Spotify credentials incomplete (need SPOTIPY_CLIENT_ID, "
            "SPOTIPY_CLIENT_SECRET, SPOTIPY_REFRESH_TOKEN) — skipping Spotify sync for %s",
            label,
        )
        return

    try:
        sp = get_spotify_client()
        if sp is None:
            return

        tracks = read_tracks_from_sheet(g, sheet_id)
        outcome = sync_set_to_spotify(sp, set_name, tracks)
        if not outcome.ok:
            logger.error(
                "❌ Spotify sync failed for %s: %s",
                label,
                outcome.detail,
            )
            if stats is not None:
                stats.spotify_failed += 1
        push_playlists_to_api(sp)
    except Exception as e:
        logger.error("❌ Spotify sync failed for %s: %s", label, e)
        if stats is not None:
            stats.spotify_failed += 1


def _file_already_in_folder(g: GoogleAPI, file_id: str, folder_id: str) -> bool:
    """Return True if file_id is currently parented under folder_id.

    Used to make archive-step moves idempotent: if a prior run of
    process_csv_file archived the file before a later task failed,
    the retry must not crash on "file missing from source parent."

    Best-effort — on any metadata-fetch error, returns False so the
    caller falls through to the normal move path (which will surface
    any real Drive API error).
    """
    try:
        meta = g.drive.service.files().get(fileId=file_id, fields="parents").execute()
        parents = meta.get("parents", []) or []
        return folder_id in parents
    except Exception as exc:
        log.warning(
            "Could not read Drive parents for file_id=%s: %s. Falling through to move.",
            file_id,
            exc,
        )
        return False


@task(name="process-csv-file")
def process_csv_file(
    g: GoogleAPI,
    file_metadata: dict,
    year: str,
    stats: CsvPipelineStats | None = None,
) -> str:
    """Process one CSV. Returns imported | failed | duplicate."""
    logger = get_prefect_logger()
    filename = file_metadata["name"]
    file_id = file_metadata["id"]
    logger.info(f"\n🚧 Processing: {filename}")
    temp_path = os.path.join("/tmp", filename)

    try:
        g.drive.download_file(file_id, temp_path)
        _normalize_csv(temp_path)
        logger.info(f"Downloaded and normalized file: {filename}")

        year_folder_id = g.drive.ensure_folder(config.DJ_SETS_FOLDER_ID, year)
        base_name = os.path.splitext(filename)[0]
        exists = file_exists_with_base_name(g, year_folder_id, base_name)
        if exists is None and stats is not None:
            stats.duplicate_check_failed += 1
        if exists is not False:
            logger.warning(
                f"⚠️ Destination already contains file with base name '{base_name}' in year folder {year_folder_id}. Marking original as possible duplicate and skipping."
            )
            rename_file_as_duplicate(g, file_id, filename)
            if stats is not None:
                stats.duplicate_csv += 1
            return "duplicate"

        sheet_id = _upload_csv_to_sheets(g, temp_path, year_folder_id, year, filename)

        if stats is not None:
            stats.sets_imported += 1
            try:
                tracks = read_tracks_from_sheet(g, sheet_id)
                stats.total_tracks += len(tracks or [])
            except Exception as track_exc:
                logger.warning(
                    "Could not read tracks from new sheet for stats: %s", track_exc
                )
                if stats is not None:
                    stats.track_read_failed += 1

        # The archive move and the ingest are independent. They were in
        # one try, so a Drive 5xx on the move also skipped the ingest and
        # the Spotify sync — and the function still returned "imported"
        # with sets_imported already incremented. The sheet existed, was
        # never POSTed, and the CSV was still in the source folder, so
        # the next run flagged it possible_duplicate_ and it was never
        # ingested at all. Under a SUCCESS report.
        try:
            archive_folder_id = g.drive.ensure_folder(year_folder_id, "Archive")
            if _file_already_in_folder(g, file_id, archive_folder_id):
                logger.info(
                    f"📦 Already archived: {filename} "
                    f"(file_id={file_id}). Skipping move — "
                    "likely a retry after partial-failure."
                )
            else:
                g.drive.move_file(
                    file_id,
                    new_parent_id=archive_folder_id,
                    remove_from_parents=True,
                )
                logger.info(f"📦 Moved original file to Archive subfolder: {filename}")
        except Exception as move_exc:
            logger.error(
                f"Failed to move original file to Archive subfolder: {move_exc}"
            )
            if stats is not None:
                stats.archive_move_failed += 1

        base_name = os.path.splitext(filename)[0]
        set_date, venue = _extract_date_and_venue(base_name)
        if set_date and venue:
            # Contained deliberately. By this point the sheet is uploaded,
            # sets_imported is incremented and the CSV is archived — the
            # set IS imported. Letting anything from here reach the outer
            # handler would count it in sets_failed as well, append it to
            # failed_set_labels, and rename the archived file FAILED_.
            # Both calls swallow their own exceptions and record their own
            # counters, so what lands here is the Prefect task machinery
            # around them: a timeout, a result-persistence error. Rare,
            # and destructive if it re-brands a good import.
            try:
                _ingest_set_to_api(
                    spreadsheet_id=sheet_id,
                    set_date=set_date,
                    venue=venue,
                    label=base_name,
                    g=g,
                    stats=stats,
                )
                _sync_set_to_spotify(
                    sheet_id=sheet_id,
                    set_name=base_name,
                    label=base_name,
                    g=g,
                    stats=stats,
                )
            except Exception as post_exc:
                logger.error(
                    "Post-import step raised for %s (set is imported): %s",
                    base_name,
                    post_exc,
                    exc_info=True,
                )
                if stats is not None:
                    stats.post_import_failed += 1
        else:
            logger.warning(
                "Could not extract date/venue from filename; skipping API ingest for %s",
                base_name,
            )
            if stats is not None:
                stats.bad_filename_in_file += 1

        return "imported"

    except Exception as e:
        logger.error(f"❌ Failed to upload or format {filename}: {e}")
        # Counted before the rename, not after. Whatever broke the upload
        # — a Drive quota, an auth expiry — usually breaks the rename
        # too, and the old order meant the set's failure was recorded
        # only if the cleanup succeeded. A failed set that reports SUCCESS
        # is worse than a file left unrenamed.
        if stats is not None:
            stats.sets_failed += 1
            stats.failed_set_labels.append(os.path.splitext(filename)[0])
        try:
            failed_name = f"FAILED_{filename}"
            g.drive.rename_file(file_id, failed_name)
            logger.info(f"✏️ Renamed original to '{failed_name}'")
        except Exception as rename_exc:
            logger.error(f"Failed to rename original to FAILED_: {rename_exc}")
        return "failed"
    finally:
        if os.path.exists(temp_path):
            with contextlib.suppress(Exception):
                os.remove(temp_path)


@flow(
    name="process-new-csv-files",
    description="Normalize new DJ set CSVs, upload to "
    "Google Sheets, archive (idempotent), and ingest to API.",
    on_failure=[make_failure_hook("process-new-csv-files")],
    on_crashed=[make_failure_hook("process-new-csv-files")],
)
def process_new_csv_files_flow() -> None:
    """TODO: describe this function."""
    logger = get_prefect_logger()
    logger.info("Starting main process")
    g = GoogleAPI.from_env()

    # Normalize any leftover status prefixes before processing
    normalize_prefixes_in_source(g.drive)

    files = g.drive.list_files(
        config.CSV_SOURCE_FOLDER_ID, include_folders=False, trashed=False
    )
    files = [{"id": f.id, "name": f.name} for f in files]
    logger.info(f"Found {len(files)} files in source folder")

    stats = CsvPipelineStats()

    for file_metadata in files:
        filename = file_metadata["name"]
        logger.debug(f"Processing file: {filename}")

        year = _extract_year_from_filename(filename)
        if not year:
            logger.warning(f"⚠️ Skipping unrecognized filename format: {filename}")
            stats.skipped_bad_filename += 1
            continue

        # If the file is not a CSV but starts with a year, move it straight to the year folder
        if not filename.lower().endswith(".csv"):
            process_non_csv_file(g, file_metadata, year, stats)
            continue

        # At this point we only process CSVs
        stats.sets_attempted += 1
        # Read before the call so the handler below can tell which side of
        # the import the failure landed on. process_csv_file mutates this
        # same stats object, so once it has incremented sets_imported the
        # set IS imported — counting it in sets_failed as well would report
        # one file as both, and put a label in failed_set_labels for a set
        # that is sitting correctly in the Archive folder.
        imported_before = stats.sets_imported
        try:
            process_csv_file(g, file_metadata, year, stats)
        except Exception as e:
            if stats.sets_imported > imported_before:
                logger.error(
                    "❌ Post-import step raised for %s (set is imported): %s",
                    filename,
                    e,
                    exc_info=True,
                )
                stats.post_import_failed += 1
            else:
                logger.error(
                    "❌ Unexpected error processing %s — continuing to next file: %s",
                    filename,
                    e,
                )
                stats.sets_failed += 1

    logger.info(
        "✅ Done: %d CSVs, %d non-CSV files, %d skipped.",
        stats.sets_attempted,
        stats.sets_skipped_non_csv,
        stats.skipped_bad_filename,
    )

    # Always sync Spotify playlists regardless of whether new files were processed
    spotify_env_ok = all(
        os.environ.get(name)
        for name in (
            "SPOTIPY_CLIENT_ID",
            "SPOTIPY_CLIENT_SECRET",
            "SPOTIPY_REFRESH_TOKEN",
        )
    )
    if spotify_env_ok:
        try:
            sp = get_spotify_client()
            if sp is not None:
                pushed = push_playlists_to_api(sp)
                logger.info(
                    "✅ Spotify playlist sync complete: %s playlists pushed",
                    pushed,
                )
        except Exception as e:
            logger.error("❌ Spotify playlist sync failed: %s", e)
            stats.spotify_failed += 1

    real_issue = _real_issue(stats)
    warn_parts = _warn_parts(stats)
    common_eval = _common_eval(stats)

    # Whether this run had anything in front of it at all. A scheduled
    # sweep over an empty folder is an idle tick; a sweep that saw files
    # reports even when every one of them was skipped, because "there were
    # four files and nothing was imported" is the case that hides a bug.
    saw_input = bool(
        stats.sets_attempted
        or stats.sets_skipped_non_csv
        or stats.skipped_bad_filename
        or stats.duplicate_csv
    )

    if real_issue:
        post_run_finding(
            flow_name="process-new-csv-files",
            severity="WARN",
            text="Completed with issues: " + "; ".join(warn_parts),
            production_only=True,
            **common_eval,
        )
    else:
        post_run_finding(
            flow_name="process-new-csv-files",
            severity="SUCCESS",
            # Spelled out here because the counters below are absorbed by
            # the cog shim and never reach the message text.
            text=(
                f"Imported {stats.sets_imported} set(s) from "
                f"{stats.sets_attempted} file(s); "
                f"{stats.total_tracks} track(s)"
            ),
            production_only=True,
            notable=saw_input,
            **common_eval,
        )


# Backwards-compatible alias for tests and callers that import main
main = process_new_csv_files_flow


if __name__ == "__main__":
    process_new_csv_files_flow()
