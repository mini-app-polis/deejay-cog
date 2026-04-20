"""
update_deejay_set_collection — LOCAL ONLY

Not served by main.py in production. Retained for ad-hoc local runs
during the PostgreSQL-as-source-of-truth transition. Will be retired
when that cutover completes.

Run locally: uv run python -m deejay_cog.update_deejay_set_collection

Findings from this module are never posted to pipeline_evaluations.
The failure hook fires and logs locally, but the production_only=False
flag passed to the pipeline_eval helpers prevents any API call.
"""

import json
import pathlib
import re

from mini_app_polis import logger as logger_mod
from mini_app_polis.google import GoogleAPI
from prefect import flow

import deejay_cog.config as config
from deejay_cog._pipeline_eval import (
    get_prefect_logger,
    make_failure_hook,
    post_run_finding,
)

log = logger_mod.get_logger()


def _create_collection_snapshot(key: str) -> dict:
    """Return an empty snapshot dict with the given top-level key."""
    return {key: []}


def _write_json_snapshot(data: dict, path: str) -> None:
    """Write data as formatted JSON to path, creating parent dirs as needed."""
    out = pathlib.Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=2), encoding="utf-8")


@flow(
    name="update-dj-set-collection",
    description="Rebuilds master DJ set collection spreadsheet "
    "and JSON snapshot. Validation layer — will be "
    "deprecated once PostgreSQL is confirmed as "
    "source of truth.",
    on_failure=[make_failure_hook("update-dj-set-collection", production_only=False)],
    on_crashed=[make_failure_hook("update-dj-set-collection", production_only=False)],
)
def generate_dj_set_collection():
    """TODO: describe this function."""
    logger = get_prefect_logger()

    logger.info("🚀 Starting generate_dj_set_collection")
    g = GoogleAPI.from_env()
    fmt = g.sheets.formatter

    # Locate DJ_SETS folder (we assume the constant ID points to the shared drive folder or folder in shared drive)
    parent_folder_id = config.DJ_SETS_FOLDER_ID
    logger.info(f"📁 Using DJ_SETS folder: {parent_folder_id}")

    # Check for existing file or create new (create directly in the shared drive parent)
    spreadsheet_id = g.drive.find_or_create_spreadsheet(
        parent_folder_id=parent_folder_id,
        name=config.OUTPUT_NAME,
    )
    logger.info(f"📄 Spreadsheet ID: {spreadsheet_id}")

    # Ensure there's exactly one temp sheet to start from
    g.sheets.clear_all_except_one_sheet(spreadsheet_id, config.TEMP_TAB_NAME)

    # Enumerate subfolders in DJ_SETS
    subfolders = g.drive.get_all_subfolders(parent_folder_id)
    logger.debug(f"Retrieved {len(subfolders)} subfolders")
    subfolders.sort(key=lambda f: f.name, reverse=True)

    tabs_to_add: list[str] = []

    # Build a JSON snapshot alongside the Google Sheet output.
    collection_snapshot = _create_collection_snapshot("folders")

    for folder in subfolders:
        name = folder.name
        folder_id = folder.id
        logger.info(f"📁 Processing folder: {name} (id: {folder_id})")

        if name.lower() == "archive":
            logger.info(f"⏭️ Skipping folder: {name} (archive folder)")
            continue

        folder_snapshot = {
            "name": name,
            "folder_id": folder_id,
            "items": [],
        }
        collection_snapshot["folders"].append(folder_snapshot)

        files = g.drive.get_files_in_folder(folder_id, include_folders=False)
        logger.debug(f"Found {len(files)} files in folder '{name}'")
        rows = []

        for f in files:
            file_name = f.name or ""
            mime_type = f.mime_type or ""
            file_url = f"https://docs.google.com/spreadsheets/d/{f.id}"
            logger.debug(
                f"Processing file: Name='{file_name}', MIME='{mime_type}', URL='{file_url}'"
            )

            if mime_type != "application/vnd.google-apps.spreadsheet":
                continue

            if name.lower() == "summary":
                folder_snapshot["items"].append(
                    {
                        "label": file_name,
                        "url": file_url,
                        "spreadsheet_id": f.id,
                    }
                )
                rows.append([f'=HYPERLINK("{file_url}", "{file_name}")', file_name])
            else:
                date, title = _extract_date_and_title(file_name)
                folder_snapshot["items"].append(
                    {
                        "date": date,
                        "title": title,
                        "label": file_name,
                        "url": file_url,
                        "spreadsheet_id": f.id,
                    }
                )
                date_cell = f"'{date}" if date else ""
                title_cell = f"'{title}" if title else ""
                rows.append(
                    [date_cell, title_cell, f'=HYPERLINK("{file_url}", "{file_name}")']
                )

        if name.lower() == "summary":
            if rows:
                all_rows = sorted(rows, key=lambda r: r[1], reverse=True)
                logger.debug(f"Adding Summary sheet with {len(all_rows)} rows")
                # add Summary sheet
                logger.info("➕ Adding Summary sheet")
                logger.info("Inserting rows into Summary sheet")
                g.sheets.insert_rows(
                    spreadsheet_id,
                    config.SUMMARY_TAB_NAME,
                    [["Link"]] + [[r[0]] for r in all_rows],
                    value_input_option="USER_ENTERED",
                )
        elif rows:
            rows.sort(key=lambda r: r[0], reverse=True)
            logger.debug(f"Adding sheet for folder '{name}' with {len(rows)} rows")
            logger.info(f"➕ Adding sheet for folder '{name}'")
            logger.info(f"Inserting rows into sheet '{name}'")
            g.sheets.insert_rows(
                spreadsheet_id,
                name,
                [["Date", "Name", "Link"]] + rows,
                value_input_option="USER_ENTERED",
            )
            # Keep Date/Name as plain text; Link remains a formula.
            fmt.set_column_text_formatting(spreadsheet_id, name, [0, 1])
            tabs_to_add.append(name)

    # Sort snapshot folders and items to match the spreadsheet ordering.
    for folder_snapshot in collection_snapshot["folders"]:
        if folder_snapshot["name"].lower() == "summary":
            folder_snapshot["items"].sort(
                key=lambda x: x.get("label", ""), reverse=True
            )
        else:
            # Prefer date sorting when present; fall back to label.
            folder_snapshot["items"].sort(
                key=lambda x: (x.get("date", ""), x.get("label", "")),
                reverse=True,
            )

    # Determine output path for the JSON snapshot.
    json_output_path = (
        getattr(config, "DEEJAY_SET_COLLECTION_JSON_PATH", None)
        or "v1/deejay-sets/deejay_set_collection.json"
    )
    json_snapshot_written = False
    try:
        _write_json_snapshot(collection_snapshot, json_output_path)
        json_snapshot_written = True
        logger.info(f"🧾 Wrote DJ set collection JSON snapshot to: {json_output_path}")
    except Exception:
        logger.exception(
            f"Failed to write DJ set collection JSON snapshot to: {json_output_path}"
        )

    # Clean up temp sheets if any
    logger.info(
        f"Deleting temp sheets: {config.TEMP_TAB_NAME} and 'Sheet1' if they exist"
    )
    g.sheets.delete_sheet_by_name(spreadsheet_id, config.TEMP_TAB_NAME)
    g.sheets.delete_sheet_by_name(spreadsheet_id, "Sheet1")

    logger.info("Setting column formatting for spreadsheet")
    fmt.apply_formatting_to_sheet(spreadsheet_id)

    # Reorder sheets: tabs_to_add then Summary
    logger.info(
        f"Reordering sheets with order: {tabs_to_add + [config.SUMMARY_TAB_NAME]}"
    )
    metadata = g.sheets.get_metadata(spreadsheet_id)
    fmt.reorder_sheets(
        spreadsheet_id,
        tabs_to_add + [config.SUMMARY_TAB_NAME],
        metadata,
    )
    logger.info("Completed reordering sheets")
    logger.info("✅ Finished generate_dj_set_collection")

    post_run_finding(
        flow_name="update-dj-set-collection",
        severity="SUCCESS",
        production_only=False,
        collection_update=True,
        folders_processed=len(subfolders),
        tabs_written=len(tabs_to_add),
        total_sets=sum(
            len(fs["items"])
            for fs in collection_snapshot["folders"]
            if fs["name"].lower() != "summary"
        ),
        json_snapshot_written=json_snapshot_written,
        folder_names=[f.name for f in subfolders],
    )


def _extract_date_and_title(file_name: str) -> tuple[str, str]:
    match = re.match(r"^(\d{4}-\d{2}-\d{2})(.*)", file_name)
    if not match:
        return ("", file_name)
    date = match[1]
    title = match[2].lstrip("-_ ")
    return (date, title)


if __name__ == "__main__":
    generate_dj_set_collection()
