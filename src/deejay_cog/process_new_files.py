import contextlib
import os
import re
from dataclasses import dataclass, field
from time import monotonic
from typing import Literal

from mini_app_polis import logger as logger_mod
from mini_app_polis.environment import api_base_url, env_var_name
from mini_app_polis.google import GoogleAPI

import deejay_cog.config as config
from deejay_cog._deadline import RunOutOfTime
from deejay_cog._pipeline_eval import (
    REPO,
    RunReport,
    get_prefect_logger,
)
from deejay_cog.ingest_to_api import (
    build_ingest_payload,
    read_tracks_from_sheet,
)
from deejay_cog.spotify_sync import (
    get_spotify_client,
    missing_spotify_credentials,
    push_playlists_to_api,
    sync_set_to_spotify,
)

log = logger_mod.get_logger()

os.environ.setdefault("CSV_SOURCE_FOLDER_ID", "1t4d_8lMC3ZJfSyainbpwInoDta7n69hC")
os.environ.setdefault("DJ_SETS_FOLDER_ID", "1A0tKQ2DBXI1Bt9h--olFwnBNne3am-rL")


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
    #: The sets this run actually imported, by filename. The counter
    #: beside it says how many; only this says which, and "3 sets
    #: imported" is not something anyone can go and look at.
    imported_set_labels: list[str] = field(default_factory=list)
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
    #: The ingest failed and the sheet uploaded for it could not be
    #: deleted. The CSV is FAILED_ and will be retried, but the leftover
    #: sheet makes the retry see a duplicate.
    rollback_failed: int = 0


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
        or stats.rollback_failed > 0
    )


#: What can go wrong, in the order it is reported: the counter's name on
#: :class:`CsvPipelineStats`, and the note that says why it matters where
#: the name alone does not. One list so the run report and the warning
#: text cannot drift — they were two hand-written sequences of the same
#: twelve facts, and only one of them ever got a new entry.
_ISSUE_FIELDS: tuple[tuple[str, str | None], ...] = (
    ("sets_failed", None),
    ("ingest_failed", None),
    ("ingest_prepare_failed", None),
    ("ingest_skipped_env_missing", "KAIANO_API_BASE_URL unset — nothing was sent"),
    ("ingest_client_unavailable", "API client could not be built — nothing was sent"),
    ("spotify_failed", None),
    ("bad_filename_in_file", None),
    ("track_read_failed", None),
    (
        "archive_move_failed",
        "CSV left in the source folder — next run will see it as a duplicate",
    ),
    (
        "non_csv_move_failed",
        "file left in the input folder and will be retried every run",
    ),
    (
        "duplicate_check_failed",
        "flagged possible_duplicate_ rather than risk a double import",
    ),
    ("post_import_failed", "set imported; Spotify sync raised afterwards"),
    (
        "rollback_failed",
        "sheet left in the year folder after a failed ingest — delete it by "
        "hand, or the retry will flag the CSV possible_duplicate_",
    ),
)


def _issue_counts(stats: CsvPipelineStats) -> list[tuple[str, int, str | None]]:
    """Every non-zero problem this run had, as ``(reason, count, note)``."""
    out: list[tuple[str, int, str | None]] = []
    for name, note in _ISSUE_FIELDS:
        count = int(getattr(stats, name, 0) or 0)
        if count:
            out.append((name, count, note))
    return out


def _warn_parts(stats: CsvPipelineStats) -> list[str]:
    """The ``k=v`` fragments naming what went wrong, in a fixed order."""
    return [
        f"{reason}={count}" + (f" ({note})" if note else "")
        for reason, count, note in _issue_counts(stats)
    ]


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
        "rollback_failed": stats.rollback_failed,
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


IngestOutcome = Literal["sent", "skipped", "failed"]


def _ingest_set_to_api(
    spreadsheet_id: str,
    set_date: str,
    venue: str,
    label: str,
    g: GoogleAPI,
    stats: CsvPipelineStats | None = None,
) -> IngestOutcome:
    """Send one newly uploaded set to api-kaianolevine-com.

    Never raises. Returns what happened, because the caller archives the
    CSV only when the API has the set: ``"sent"``; ``"skipped"`` when there
    was nothing to send (no tracks); ``"failed"`` when the set should have
    reached the API and did not — including no base URL or no client,
    which send nothing just as surely as a 5xx does.
    """
    logger = get_prefect_logger()

    # The same variable the client below will read: _DEV outside
    # production. Gating on the unsuffixed name skipped every ingest in a
    # correctly configured development run.
    if not api_base_url():
        logger.warning(
            "%s not set — skipping API ingest for %s",
            env_var_name("KAIANO_API_BASE_URL"),
            label,
        )
        if stats is not None:
            stats.ingest_skipped_env_missing += 1
        return "failed"

    try:
        from mini_app_polis.api.errors import KaianoApiError

        from .api_client import api_client
    except Exception as e:
        logger.warning(
            "⚠️ API client not available; skipping ingest for %s: %s", label, e
        )
        if stats is not None:
            stats.ingest_client_unavailable += 1
        return "failed"

    # Per call, not read off the run-wide counter: once any earlier set had
    # been attempted, that counter made this set's pre-POST failure look
    # like a rejected POST.
    attempted = False
    try:
        tracks = read_tracks_from_sheet(g, spreadsheet_id)
        payload = build_ingest_payload(
            set_date=set_date,
            venue=venue,
            source_file=label,
            tracks=tracks,
        )
        final_tracks = payload.tracks
        if not final_tracks:
            logger.warning("⚠️ No tracks to ingest for %s", label)
            if stats is not None:
                stats.ingest_skipped_no_tracks += 1
            return "skipped"

        attempted = True
        if stats is not None:
            stats.ingest_attempted += 1
        client = api_client()
        client.ingest(payload)
        logger.info("✅ Ingested to API: %s (%d tracks)", label, len(final_tracks))
        return "sent"
    except KaianoApiError as e:
        if stats is not None:
            stats.ingest_failed += 1
        logger.error("❌ API ingest failed for %s: %s", label, e)
        return "failed"
    except Exception as e:
        if stats is not None:
            # Which side of the POST this landed on decides which counter
            # moves, but both are failures.
            if attempted:
                stats.ingest_failed += 1
            else:
                stats.ingest_prepare_failed += 1
        logger.error("❌ Unexpected error during API ingest for %s: %s", label, e)
        return "failed"


def _sync_set_to_spotify(
    sheet_id: str,
    set_name: str,
    label: str,
    g: GoogleAPI,
    stats: CsvPipelineStats | None = None,
) -> None:
    logger = get_prefect_logger()

    missing = missing_spotify_credentials()
    if missing:
        logger.warning(
            "Spotify credentials incomplete (%s not set) — "
            "skipping Spotify sync for %s",
            ", ".join(missing),
            label,
        )
        return

    try:
        sp = get_spotify_client()
        if sp is None:
            # Credentials are present, so this is a client that would not
            # build. This used to return with no log line at all: the only
            # message came from get_spotify_client's module logger, which
            # does not reach the Prefect run logger.
            logger.error(
                "❌ Spotify client could not be initialized — "
                "skipping Spotify sync for %s",
                label,
            )
            if stats is not None:
                stats.spotify_failed += 1
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
        # The full playlist snapshot used to be pushed here as well as at
        # the end of the flow: N+1 enumerations and N+1 POSTs for N files,
        # with a push failure logged as that CSV's sync failing. The
        # flow-level push runs unconditionally and covers this.
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


def _mark_failed(
    g: GoogleAPI,
    file_id: str,
    filename: str,
    stats: CsvPipelineStats | None,
) -> None:
    """Count a set as failed and rename its CSV FAILED_ for the next run.

    Counted before the rename, not after. Whatever broke the set — a Drive
    quota, an auth expiry — usually breaks the rename too, and a failed set
    that reports SUCCESS is worse than a file left unrenamed.
    """
    logger = get_prefect_logger()
    if stats is not None:
        stats.sets_failed += 1
        stats.failed_set_labels.append(os.path.splitext(filename)[0])
    try:
        failed_name = f"FAILED_{filename}"
        g.drive.rename_file(file_id, failed_name)
        logger.info(f"✏️ Renamed original to '{failed_name}'")
    except Exception as rename_exc:
        logger.error(f"Failed to rename original to FAILED_: {rename_exc}")


def _roll_back_upload(
    g: GoogleAPI,
    sheet_id: str,
    filename: str,
    stats: CsvPipelineStats | None,
) -> None:
    """Delete a sheet this run uploaded for a set it did not finish.

    The sheet is this run's own derivative of the CSV. Left behind, the
    retry would find it, take the CSV for a duplicate and never ingest the
    set — so a set is either uploaded, ingested and archived, or none of
    them.
    """
    logger = get_prefect_logger()
    try:
        g.drive.delete_file(sheet_id)
        logger.info("↩️ Removed the sheet uploaded for %s", filename)
    except Exception as exc:
        logger.error(
            "Could not remove the sheet uploaded for %s (sheet_id=%s): %s",
            filename,
            sheet_id,
            exc,
        )
        if stats is not None:
            stats.rollback_failed += 1


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

        # Ingest before archive. The CSV leaves the drop zone only once the
        # API has the set: archived first, a rejected POST left the set in
        # Sheets and nowhere else, with nothing that would ever retry it.
        try:
            set_date, venue = _extract_date_and_venue(base_name)
            if set_date and venue:
                outcome = _ingest_set_to_api(
                    spreadsheet_id=sheet_id,
                    set_date=set_date,
                    venue=venue,
                    label=base_name,
                    g=g,
                    stats=stats,
                )
            else:
                logger.warning(
                    "Could not extract date/venue from filename; "
                    "skipping API ingest for %s",
                    base_name,
                )
                if stats is not None:
                    stats.bad_filename_in_file += 1
                outcome = "skipped"
        except RunOutOfTime:
            # Stopped between upload and archive. Undo the upload so the
            # redelivery finds the CSV, not a duplicate of it.
            _roll_back_upload(g, sheet_id, filename, stats)
            raise

        if outcome == "failed":
            _roll_back_upload(g, sheet_id, filename, stats)
            _mark_failed(g, file_id, filename, stats)
            return "failed"

        if stats is not None:
            stats.sets_imported += 1
            try:
                tracks = read_tracks_from_sheet(g, sheet_id)
                stats.total_tracks += len(tracks or [])
            except Exception as track_exc:
                logger.warning(
                    "Could not read tracks from new sheet for stats: %s", track_exc
                )
                stats.track_read_failed += 1

        # The set is in the API now; a failed move costs a duplicate flag
        # on the next run, not the set.
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

        if set_date and venue:
            # Contained deliberately: the set is imported, so nothing from
            # here may reach the outer handler and re-brand it FAILED_.
            try:
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

        return "imported"

    except Exception as e:
        logger.error(f"❌ Failed to upload or format {filename}: {e}")
        _mark_failed(g, file_id, filename, stats)
        return "failed"
    finally:
        if os.path.exists(temp_path):
            with contextlib.suppress(Exception):
                os.remove(temp_path)


def process_new_csv_files_flow(*, run_id: str | None = None) -> None:
    """Normalize new DJ set CSVs, upload to Google Sheets, archive, ingest to API.

    A sweep of the source folder, not one file: whatever is there is
    processed, so a trigger that arrives after its files were already
    handled finds nothing and reports an idle run.

    ``run_id`` is the queue message id when the Lambda worker runs this.
    Passed rather than resolved: ``get_run_id()`` only knows Prefect's ids,
    so without it every report would arrive as ``"local-run"``.
    """
    started_at = monotonic()
    logger = get_prefect_logger()
    logger.info("Starting main process")
    g = GoogleAPI.from_env()

    # Normalize any leftover status prefixes before processing
    normalize_prefixes_in_source(g.drive)

    listed = g.drive.list_files(
        config.CSV_SOURCE_FOLDER_ID, include_folders=False, trashed=False
    )
    files = [{"id": f.id, "name": f.name} for f in listed]
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

        # Recorded here rather than inside process_csv_file, because this
        # is where the run already decides which side of the import a
        # file landed on. Both paths pass through: a set whose
        # post-import step raised is still imported, and still belongs in
        # the report as something this run created.
        if stats.sets_imported > imported_before:
            stats.imported_set_labels.append(filename)

    logger.info(
        "✅ Done: %d CSVs, %d non-CSV files, %d skipped.",
        stats.sets_attempted,
        stats.sets_skipped_non_csv,
        stats.skipped_bad_filename,
    )

    # Always sync Spotify playlists regardless of whether new files were processed
    missing_credentials = missing_spotify_credentials()
    if not missing_credentials:
        try:
            sp = get_spotify_client()
            if sp is None:
                logger.error(
                    "❌ Spotify client could not be initialized — "
                    "playlist snapshot not pushed",
                )
                stats.spotify_failed += 1
            else:
                push = push_playlists_to_api(sp)
                if push is None:
                    # A skip is not a push of None playlists.
                    logger.warning(
                        "⚠️ Spotify playlist push skipped — KAIANO_API_BASE_URL not set",
                    )
                else:
                    upserted, unchanged = push
                    logger.info(
                        "✅ Spotify playlist sync complete: %s upserted, %s unchanged",
                        upserted,
                        unchanged,
                    )
        except Exception as e:
            logger.error("❌ Spotify playlist sync failed: %s", e)
            stats.spotify_failed += 1
    else:
        logger.warning(
            "Spotify credentials incomplete (%s not set) — "
            "playlist snapshot not pushed",
            ", ".join(missing_credentials),
        )

    # Whether this run had anything in front of it at all. A scheduled
    # sweep over an empty folder is an idle tick; a sweep that saw files
    # reports even when every one of them was skipped, because "there were
    # four files and nothing was imported" is the case that hides a bug.
    # A run that imported something is notable on its own — the outcomes
    # below say so — but this covers the run that saw four files and
    # imported none, which has no outcome to speak for it.
    saw_input = bool(
        stats.sets_attempted
        or stats.sets_skipped_non_csv
        or stats.skipped_bad_filename
        or stats.duplicate_csv
    )

    # Assembled rather than hand-written. The severity is derived from
    # the issues recorded below instead of chosen by ``_real_issue``,
    # which stays as the tested predicate other code reads; a run with
    # any issue is a WARN, which is the same answer.
    report = RunReport(
        flow_name="process-new-csv-files",
        repo=REPO,
        duration_sec=monotonic() - started_at,
        run_id=run_id,
    )
    report.ok(stats.sets_imported)
    for label in stats.imported_set_labels:
        report.created("dj set", label)
    report.count("tracks", stats.total_tracks)
    report.count("attempted", stats.sets_attempted)
    if stats.duplicate_csv:
        report.note("duplicate_csv")
    if stats.skipped_bad_filename:
        report.note("unrecognized_filename")

    for reason, count, note in _issue_counts(stats):
        for index in range(count):
            # The note rides on the first one only: it explains the
            # reason, not the instance, and repeating it once per count
            # is how one bad run fills the channel.
            report.issue(reason, note if index == 0 else None)

    report.send(notable=saw_input)


# Backwards-compatible alias for tests and callers that import main
main = process_new_csv_files_flow


if __name__ == "__main__":
    process_new_csv_files_flow()
