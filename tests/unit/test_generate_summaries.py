from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import deejay_cog.generate_summaries as generate_summaries


def _make_file(name: str, fid: str):
    return SimpleNamespace(name=name, id=fid)


def test_generate_summary_filters_columns_and_orders_by_desired_order():
    g = SimpleNamespace()
    sheets = SimpleNamespace(
        get_metadata=MagicMock(
            return_value={"sheets": [{"properties": {"sheetId": 1, "title": "Sheet1"}}]}
        ),
        read_values=MagicMock(
            return_value=[
                ["Title", "Artist", "Ignored", "Genre"],
                [" A ", " X ", "foo", " House "],
                [" B ", " Y ", "bar", " Techno "],
            ]
        ),
        ensure_sheet_exists=MagicMock(),
        clear_all_except_one_sheet=MagicMock(),
        insert_rows=MagicMock(),
        formatter=SimpleNamespace(apply_formatting_to_sheet=MagicMock()),
    )
    g.sheets = sheets
    g.drive = SimpleNamespace(
        create_spreadsheet_in_folder=MagicMock(return_value="ss-id"),
        copy_file=MagicMock(return_value="year-summary-id"),
    )

    files = [_make_file("2024-01-01_Set", "file-1")]

    with (
        patch.object(generate_summaries, "config") as mock_config,
        patch.object(generate_summaries, "log"),
        patch.object(generate_summaries, "deduplication"),
    ):
        mock_config.ALLOWED_HEADERS = ["title", "artist", "genre"]
        mock_config.desiredOrder = ["Title", "Genre", "Artist"]

        generate_summaries.generate_summary_for_folder(
            g, files, summary_folder_id="summary-folder", year="2024"
        )

    sheets.get_metadata.assert_called_once()
    sheets.read_values.assert_called_once()
    sheets.insert_rows.assert_called_once()
    args, kwargs = sheets.insert_rows.call_args
    _, sheet_name, rows, *_ = args

    assert sheet_name == "Summary"
    header = rows[0]
    assert header == ["Title", "Genre", "Artist", "Count"]
    data_rows = rows[1:]
    assert [r[0] for r in data_rows] == ["A", "B"]


def test_generate_next_missing_summary_skips_existing_canonical_and_unready_years():
    summary_folder = SimpleNamespace(id="summary-folder")
    year_2023 = SimpleNamespace(id="folder-2023", name="2023")
    year_2024 = SimpleNamespace(id="folder-2024", name="2024")

    g = SimpleNamespace()
    g.drive = SimpleNamespace(
        ensure_folder=MagicMock(return_value=summary_folder.id),
        list_files=MagicMock(),
    )

    def list_files_side_effect(folder_id, **_):
        if folder_id == "dj-sets-folder":
            return [year_2023, year_2024]
        if folder_id == "summary-folder":
            return [
                SimpleNamespace(name="2023 Summary", id="sum-2023"),
            ]
        if folder_id == "folder-2023":
            return [
                SimpleNamespace(name="2023-01-01_Set", id="set-2023-1"),
            ]
        if folder_id == "folder-2024":
            return [
                SimpleNamespace(name="FAILED_2024-01-01_Set", id="set-2024-bad"),
            ]
        return []

    g.drive.list_files.side_effect = list_files_side_effect

    with (
        patch.object(generate_summaries, "GoogleAPI") as mock_google_api,
        patch.object(generate_summaries, "deduplication") as mock_dedup,
        patch.object(generate_summaries, "log"),
        patch.object(
            generate_summaries, "generate_summary_for_folder"
        ) as mock_generate_folder,
        patch.object(generate_summaries, "config") as mock_config,
    ):
        mock_google_api.from_env.return_value = g
        mock_config.DJ_SETS_FOLDER_ID = "dj-sets-folder"
        mock_config.SUMMARY_FOLDER_NAME = "Summary"

        generate_summaries.generate_summaries_flow()

    g.drive.ensure_folder.assert_called_once()
    assert mock_dedup.deduplicate_summary.call_count == 1
    mock_dedup.deduplicate_summary.assert_called_with("sum-2023", g=g)
    mock_generate_folder.assert_not_called()
