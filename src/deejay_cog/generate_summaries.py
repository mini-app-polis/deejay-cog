"""
generate_summaries — LOCAL ONLY

Not served by main.py in production. Retained for ad-hoc local runs
during the PostgreSQL-as-source-of-truth transition. Will be retired
when that cutover completes.

Run locally: uv run python -m deejay_cog.generate_summaries

Findings from this module are never posted to pipeline_evaluations.
The failure hook fires and logs locally, but the production_only=False
flag passed to the pipeline_eval helpers prevents any API call.
"""

from mini_app_polis import logger as logger_mod
from mini_app_polis.google import GoogleAPI
from prefect import flow

import deejay_cog.config as config
import deejay_cog.deduplicate_summary as deduplication
from deejay_cog._pipeline_eval import (
    get_prefect_logger,
    make_failure_hook,
    post_run_finding,
)

log = logger_mod.get_logger()


@flow(
    name="generate-summaries",
    description="Generates per-year summary sheets. "
    "Validation layer — will be deprecated once "
    "PostgreSQL is confirmed as source of truth.",
    on_failure=[make_failure_hook("generate-summaries", production_only=False)],
    on_crashed=[make_failure_hook("generate-summaries", production_only=False)],
)
def generate_summaries_flow() -> None:
    """Generate the next missing summary for a year."""
    logger = get_prefect_logger()

    logger.info("🚀 Starting generate_next_missing_summary()")
    g = GoogleAPI.from_env()

    summary_folder_id = g.drive.ensure_folder(
        config.DJ_SETS_FOLDER_ID, config.SUMMARY_FOLDER_NAME
    )
    logger.debug(f"Summary folder: {summary_folder_id}")

    year_folders = g.drive.list_files(
        config.DJ_SETS_FOLDER_ID,
        mime_type="application/vnd.google-apps.folder",
        trashed=False,
        include_folders=True,
    )

    logger.debug(f"Year folders found: {[f.name for f in year_folders]}")

    years_processed = 0
    summaries_generated = 0
    summaries_skipped_no_canonical = 0
    dedup_runs = 0

    for folder in year_folders:
        year = folder.name
        if (year or "").lower() == "summary":
            continue

        years_processed += 1

        summary_name = f"{year} Summary"

        # Find existing summaries for this year in the Summary folder (contains match)
        all_summary_files = g.drive.list_files(
            summary_folder_id, trashed=False, include_folders=False
        )
        existing_summaries = [
            f for f in all_summary_files if f.name and summary_name in f.name
        ]
        existing_names = [f.name for f in existing_summaries]
        logger.debug(f"Found existing summaries for {year}: {existing_names}")

        canonical = next(
            (f for f in existing_summaries if f.name == summary_name), None
        )
        if canonical:
            logger.info(
                f"✅ Summary already exists for {year} — running dedup on '{summary_name}' and continuing"
            )
            deduplication.deduplicate_summary(canonical.id, g=g)
            dedup_runs += 1
            continue

        if existing_summaries:
            logger.warning(
                f"⚠️ Found summary-like files for {year} but no exact '{summary_name}' match. "
                f"Skipping dedup to avoid modifying the wrong file. Matches: {existing_names}"
            )
            summaries_skipped_no_canonical += 1
            continue

        logger.debug(f"Getting files for year {year}")
        files = g.drive.list_files(
            folder.id,
            mime_type="application/vnd.google-apps.spreadsheet",
            trashed=False,
            include_folders=False,
        )

        if any(
            (f.name or "").startswith("FAILED_") or "_Cleaned" in (f.name or "")
            for f in files
        ):
            logger.info(f"⛔ Skipping year {year} — unready files found")
            continue

        logger.debug(f"Files to process for {year}: {[f.name for f in files]}")
        logger.info(f"🔧 Generating summary for {year}...")

        if generate_summary_for_folder(g, files, summary_folder_id, year):
            summaries_generated += 1

    post_run_finding(
        flow_name="generate-summaries",
        severity="SUCCESS",
        production_only=False,
        years_processed=years_processed,
        summaries_generated=summaries_generated,
        summaries_skipped_no_canonical_match=summaries_skipped_no_canonical,
        dedup_runs=dedup_runs,
    )


# Backwards-compatible name for callers and tests
generate_next_missing_summary = generate_summaries_flow


def generate_summary_for_folder(
    g: GoogleAPI,
    files,
    summary_folder_id: str,
    year: str,
) -> bool:
    log.debug(
        f"Starting generate_summary_for_folder for year {year} with {len(files)} files"
    )

    def _trim_cell(v: str) -> str:
        return str(v).strip() if v is not None else ""

    def _canon_header(h: str) -> str:
        return _trim_cell(h).lower()

    combined_name = f"_TestingOnly_{year}"
    summary_name = f"{year} Summary"

    all_headers: set[str] = set()
    sheet_data: list[tuple[list[str], list[list[str]]]] = []

    for f in files:
        file_name = f.name or ""
        log.info(f"🔍 Reading {file_name}")

        sheets_metadata = g.sheets.get_metadata(
            f.id, fields="sheets(properties(title))"
        )

        sheets = sheets_metadata.get("sheets", [])
        if not sheets:
            log.warning(
                f"⚠️ No sheets found in spreadsheet {file_name} ({f.id}); skipping"
            )
            continue

        for sheet in sheets:
            sheet_title = sheet.get("properties", {}).get("title")
            if not sheet_title:
                log.debug(
                    f"Skipping sheet with missing title in spreadsheet {file_name}"
                )
                continue

            values = g.sheets.read_values(f.id, f"{sheet_title}!A:Z")

            if not values or len(values) < 2:
                log.warning(f"⚠️ No data in {file_name} - sheet '{sheet_title}'")
                continue

            header = [_trim_cell(h) for h in values[0]]
            rows = [[_trim_cell(c) for c in r] for r in values[1:]]

            lower_header = [_canon_header(h) for h in header]

            allowed = {str(h).strip().lower() for h in config.ALLOWED_HEADERS}
            keep_indices = [i for i, h in enumerate(lower_header) if h in allowed]

            if not keep_indices:
                continue

            filtered_header = [lower_header[i] for i in keep_indices]
            filtered_rows: list[list[str]] = []

            for row in rows:
                if not any((cell or "").strip() for cell in row):
                    continue
                padded = row + [""] * (max(keep_indices) + 1 - len(row))
                filtered_rows.append([padded[i] for i in keep_indices])

            log.debug(
                f"Filtered header for sheet '{sheet_title}': {filtered_header}, rows: {len(filtered_rows)}"
            )

            if filtered_rows:
                all_headers.update(filtered_header)
                sheet_data.append((filtered_header, filtered_rows))

    if not sheet_data:
        log.info(f"📭 No valid data found in folder: {year}")
        return False

    desired_display = [str(c).strip() for c in config.desiredOrder]
    desired_canon = [_canon_header(c) for c in desired_display]
    desired_map = dict(zip(desired_canon, desired_display, strict=True))

    ordered_header = [c for c in desired_canon if c in all_headers]
    unordered_header = sorted([c for c in all_headers if c not in set(desired_canon)])

    final_header_canon = ordered_header + unordered_header
    final_header = [desired_map.get(c, c) for c in final_header_canon] + ["Count"]

    final_rows: list[list[str | int]] = []
    for header, rows in sheet_data:
        idx_map = {h: i for i, h in enumerate(header)}
        for row in rows:
            aligned = [
                row[idx_map[h]] if h in idx_map else "" for h in final_header_canon
            ]
            final_rows.append(aligned + [1])

    log.debug(
        f"Final header for year {year}: {final_header}, total rows: {len(final_rows)}"
    )

    title_canon = _canon_header("Title")
    if title_canon in final_header_canon:
        title_index = final_header_canon.index(title_canon)
        final_rows.sort(key=lambda r: str(r[title_index]))
    else:
        final_rows.sort(key=lambda r: [str(x) for x in r])

    ss_id = g.drive.create_spreadsheet_in_folder(combined_name, summary_folder_id)
    log.debug(f"Created spreadsheet ID for {combined_name}: {ss_id}")

    g.sheets.ensure_sheet_exists(ss_id, "Summary")

    log.info(f"Deleting all sheets except 'Summary' in spreadsheet {ss_id}")
    g.sheets.clear_all_except_one_sheet(ss_id, "Summary")

    log.info(f"Writing summary data to 'Summary' sheet with {len(final_rows)} rows")
    rows_to_write = [final_header] + [list(r) for r in final_rows]
    g.sheets.insert_rows(
        ss_id,
        "Summary",
        rows_to_write,
        value_input_option="RAW",
    )

    # Apply common formatting once the data is written.
    fmt = g.sheets.formatter
    fmt.apply_formatting_to_sheet(ss_id)

    # Copy the generated combined summary to the year summary name
    year_summary_id = g.drive.copy_file(
        ss_id,
        parent_folder_id=summary_folder_id,
        name=summary_name,
    )

    log.info(f"Combined summary spreadsheet ID: {ss_id}")
    log.info(
        f"Year summary spreadsheet ID with name '{summary_name}': {year_summary_id}"
    )

    deduplication.deduplicate_summary(year_summary_id, g=g)
    return True


if __name__ == "__main__":
    generate_summaries_flow()
