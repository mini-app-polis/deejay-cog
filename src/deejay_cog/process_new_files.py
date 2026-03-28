import contextlib
import os
import re
from dataclasses import dataclass, field

import mini_app_polis.config as config
from evaluator_cog.flows.pipeline_eval import evaluate_pipeline_run
from mini_app_polis import logger as logger_mod
from mini_app_polis.google import GoogleAPI
from prefect import flow, get_run_logger, task

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

def _prefect_logger():
    try:
        return get_run_logger()
    except Exception:
        return log


def _handle_flow_failure(flow, flow_run, state) -> None:
    """
    Prefect failure/crash hook: write a direct evaluation finding.
    Never raises.
    """
    logger = _prefect_logger()
    try:
        state_name = str(getattr(state, "name", "FAILED"))
        state_type = str(getattr(state, "type", "")).upper()
        severity = (
            "ERROR" if state_type == "CRASHED" or state_name == "Crashed" else "WARN"
        )
        run_id = str(getattr(flow_run, "id", "") or os.environ.get("GITHUB_RUN_ID", ""))
        if not run_id:
            run_id = "prefect-unknown-run"

        logger.error("Flow failure hook fired: run_id=%s state=%s", run_id, state_name)
        evaluate_pipeline_run(
            run_id=run_id,
            repo="deejay-cog",
            flow_name=flow.name,
            sets_imported=0,
            sets_failed=0,
            sets_skipped=0,
            total_tracks=0,
            failed_set_labels=[],
            api_ingest_success=True,
            sets_attempted=0,
            collection_update=False,
            direct_finding_text=f"Flow entered {state_name} unexpectedly",
            direct_severity=severity,
        )
    except Exception:
        logger.exception("Flow failure hook failed unexpectedly")


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
    track_read_failed: int = 0


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
def file_exists_with_base_name(g: GoogleAPI, folder_id: str, base_name: str) -> bool:
    try:
        candidates = g.drive.list_files(folder_id, include_folders=False, trashed=False)
        for f in candidates:
            if os.path.splitext(f.name or "")[0] == base_name:
                return True
    except Exception as e:
        log.error(f"Error checking for duplicates in folder {folder_id}: {e}")
    return False


def rename_file_as_duplicate(g: GoogleAPI, file_id: str, filename: str) -> None:
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
    filename = file_metadata["name"]
    file_id = file_metadata["id"]
    log.info(f"\n📄 Moving non-CSV file that starts with year: {filename}")
    try:
        year_folder_id = g.drive.ensure_folder(config.DJ_SETS_FOLDER_ID, year)
        base_name = os.path.splitext(filename)[0]
        if file_exists_with_base_name(g, year_folder_id, base_name):
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
    logger = _prefect_logger()
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
    logger = _prefect_logger()
    sheet_id = g.drive.upload_csv_as_google_sheet(temp_path, parent_id=year_folder_id)
    logger.debug("Uploaded sheet ID: %s", sheet_id)
    g.sheets.formatter.apply_formatting_to_sheet(sheet_id)
    remove_summary_file_for_year(g, year)
    logger.debug("upload-to-sheets complete for %s", filename)
    return sheet_id


@task(
    name="ingest-to-api",
    retries=2,
    retry_delay_seconds=30,
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

    logger = _prefect_logger()

    if not _os.environ.get("KAIANO_API_BASE_URL"):
        logger.warning(
            "KAIANO_API_BASE_URL not set — skipping API ingest for %s", label
        )
        if stats is not None:
            stats.ingest_skipped_env_missing += 1
        return

    try:
        from mini_app_polis.api import KaianoApiClient  # type: ignore
        from mini_app_polis.api.errors import KaianoApiError  # type: ignore
    except Exception as e:
        logger.error(
            "❌ API client not available; skipping ingest for %s: %s", label, e
        )
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
        client = KaianoApiClient.from_env()
        client.post("/v1/ingest", payload)
        logger.info("✅ Ingested to API: %s (%d tracks)", label, len(final_tracks))
    except KaianoApiError as e:
        if stats is not None:
            stats.ingest_failed += 1
        logger.error("❌ API ingest failed for %s: %s", label, e)
    except Exception as e:
        if stats is not None and stats.ingest_attempted > 0:
            stats.ingest_failed += 1
        logger.error("❌ Unexpected error during API ingest for %s: %s", label, e)


@task(name="sync-to-spotify")
def _sync_set_to_spotify(
    sheet_id: str,
    set_name: str,
    label: str,
    g: GoogleAPI,
    stats: CsvPipelineStats | None = None,
) -> None:
    logger = _prefect_logger()

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
        sync_set_to_spotify(sp, set_name, tracks)
        push_playlists_to_api(sp)
    except Exception as e:
        logger.error("❌ Spotify sync failed for %s: %s", label, e)
        if stats is not None:
            stats.spotify_failed += 1


@task(name="process-csv-file")
def process_csv_file(
    g: GoogleAPI,
    file_metadata: dict,
    year: str,
    stats: CsvPipelineStats | None = None,
) -> str:
    """Process one CSV. Returns imported | failed | duplicate."""
    logger = _prefect_logger()
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
        if file_exists_with_base_name(g, year_folder_id, base_name):
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

        try:
            archive_folder_id = g.drive.ensure_folder(year_folder_id, "Archive")
            g.drive.move_file(
                file_id, new_parent_id=archive_folder_id, remove_from_parents=True
            )
            logger.info(f"📦 Moved original file to Archive subfolder: {filename}")

            base_name = os.path.splitext(filename)[0]
            set_date, venue = _extract_date_and_venue(base_name)
            if set_date and venue:
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
            else:
                logger.warning(
                    "Could not extract date/venue from filename; skipping API ingest for %s",
                    base_name,
                )
                if stats is not None:
                    stats.bad_filename_in_file += 1
        except Exception as move_exc:
            logger.error(
                f"Failed to move original file to Archive subfolder: {move_exc}"
            )

        return "imported"

    except Exception as e:
        logger.error(f"❌ Failed to upload or format {filename}: {e}")
        try:
            failed_name = f"FAILED_{filename}"
            g.drive.rename_file(file_id, failed_name)
            logger.info(f"✏️ Renamed original to '{failed_name}'")
            if stats is not None:
                stats.sets_failed += 1
                stats.failed_set_labels.append(os.path.splitext(filename)[0])
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
    "Google Sheets, archive, and ingest to API.",
    on_failure=[_handle_flow_failure],
    on_crashed=[_handle_flow_failure],
)
def process_new_csv_files_flow() -> None:
    logger = _prefect_logger()
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
        process_csv_file(g, file_metadata, year, stats)

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

    if os.environ.get("ANTHROPIC_API_KEY") and os.environ.get("KAIANO_API_BASE_URL"):
        run_id = os.environ.get("GITHUB_RUN_ID", "local-run")
        try:
            evaluate_pipeline_run(
                run_id=run_id,
                repo="deejay-cog",
                flow_name="process-new-csv-files",
                sets_imported=stats.sets_imported,
                sets_failed=stats.sets_failed,
                sets_skipped=stats.sets_skipped_non_csv,
                total_tracks=stats.total_tracks,
                failed_set_labels=list(stats.failed_set_labels),
                api_ingest_success=(stats.ingest_failed == 0),
                sets_attempted=stats.sets_attempted,
                collection_update=False,
                unrecognized_filename_skips=stats.skipped_bad_filename,
                duplicate_csv_count=stats.duplicate_csv,
            )
            aux_parts: list[str] = []
            if stats.spotify_failed:
                aux_parts.append(f"spotify_failed={stats.spotify_failed}")
            if stats.bad_filename_in_file:
                aux_parts.append(f"bad_filename_in_file={stats.bad_filename_in_file}")
            if stats.ingest_skipped_no_tracks:
                aux_parts.append(
                    f"ingest_skipped_no_tracks={stats.ingest_skipped_no_tracks}"
                )
            if stats.ingest_skipped_env_missing:
                aux_parts.append(
                    f"ingest_skipped_env_missing={stats.ingest_skipped_env_missing}"
                )
            if stats.track_read_failed:
                aux_parts.append(f"track_read_failed={stats.track_read_failed}")
            if aux_parts:
                evaluate_pipeline_run(
                    run_id=run_id,
                    repo="deejay-cog",
                    flow_name="process-new-csv-files",
                    sets_imported=0,
                    sets_failed=0,
                    sets_skipped=0,
                    total_tracks=0,
                    failed_set_labels=[],
                    api_ingest_success=True,
                    sets_attempted=0,
                    collection_update=False,
                    direct_finding_text=(
                        "CSV pipeline auxiliary signals: " + "; ".join(aux_parts)
                    ),
                    direct_severity="WARN",
                )
        except Exception:
            logger.exception(
                "Pipeline evaluation raised unexpectedly (should be best-effort)"
            )


# Backwards-compatible alias for tests and callers that import main
main = process_new_csv_files_flow


if __name__ == "__main__":
    process_new_csv_files_flow()
