import contextlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import deejay_cog.deduplicate_summary as dedup_mod


def _make_g_for_sheet(data):
    sheets_api = SimpleNamespace(
        get_metadata=MagicMock(
            return_value={
                "sheets": [{"properties": {"sheetId": 1, "title": "Summary"}}]
            }
        ),
        read_values=MagicMock(return_value=data),
        clear=MagicMock(),
        write_values=MagicMock(),
        formatter=SimpleNamespace(apply_formatting_to_sheet=MagicMock()),
    )
    g = SimpleNamespace(
        sheets=sheets_api,
    )
    return g


def test_deduplicate_summary_merges_duplicates_and_sums_count():
    header = ["Title", "Artist"]
    row1 = ["Song A", "Artist 1"]
    row2 = ["Song A ", " Artist 1"]  # duplicate after normalization
    row3 = ["Song B", "Artist 2"]
    data = [header, row1, row2, row3]

    g = _make_g_for_sheet(data)

    with (
        patch.object(dedup_mod, "GoogleAPI") as mock_google_api,
        patch.object(dedup_mod, "log"),
    ):
        mock_google_api.from_env.return_value = g
        dedup_mod.deduplicate_summary("spreadsheet-id")

    g.sheets.get_metadata.assert_called_once()
    g.sheets.read_values.assert_called_once()
    g.sheets.clear.assert_called_once()
    g.sheets.write_values.assert_called_once()
    args, kwargs = g.sheets.write_values.call_args
    _, _, final_data, *_ = args

    out_header = final_data[0]
    assert "Count" in out_header
    count_idx = out_header.index("Count")
    out_rows = final_data[1:]
    titles = [r[0] for r in out_rows]
    assert titles == ["Song A", "Song B"]
    counts = [int(r[count_idx]) for r in out_rows]
    assert counts == [2, 1]


def test_deduplicate_summary_is_idempotent_for_clean_sheet():
    header = ["Title", "Artist", "Count"]
    row1 = ["Song A", "Artist 1", "1"]
    row2 = ["Song B", "Artist 2", "1"]
    data = [header, row1, row2]

    g = _make_g_for_sheet(data)

    with (
        patch.object(dedup_mod, "GoogleAPI") as mock_google_api,
        patch.object(dedup_mod, "log"),
    ):
        mock_google_api.from_env.return_value = g
        dedup_mod.deduplicate_summary("spreadsheet-id")

    g.sheets.read_values.assert_called_once()
    g.sheets.write_values.assert_called_once()
    args, _ = g.sheets.write_values.call_args
    _, _, final_data, *_ = args
    assert final_data[0] == header
    assert final_data[1:] == [row1, row2]


def test_deduplicate_summary_handles_read_failure_without_raising():
    sheets_api = SimpleNamespace(
        get_metadata=MagicMock(
            return_value={
                "sheets": [{"properties": {"sheetId": 1, "title": "Summary"}}]
            }
        ),
        read_values=MagicMock(side_effect=RuntimeError("boom")),
        clear=MagicMock(),
        write_values=MagicMock(),
        formatter=SimpleNamespace(apply_formatting_to_sheet=MagicMock()),
    )
    g = SimpleNamespace(sheets=sheets_api)

    with (
        patch.object(dedup_mod, "GoogleAPI") as mock_google_api,
        patch.object(dedup_mod, "log"),
    ):
        mock_google_api.from_env.return_value = g
        with contextlib.suppress(RuntimeError):
            dedup_mod.deduplicate_summary("spreadsheet-id")

    g.sheets.get_metadata.assert_called_once()
    g.sheets.read_values.assert_called_once()
