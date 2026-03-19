import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import deejay_set_processor.process_new_files as process_new_files

# --- Normalization tests -----------------------------------------------------


def _write_and_normalize(tmp_path, contents: str) -> str:
    path = tmp_path / "input.csv"
    path.write_text(contents)
    process_new_files._normalize_csv(str(path))
    return path.read_text()


def test_normalize_csv_leaves_clean_csv_unchanged(tmp_path):
    original = "a,b,c\n1,2,3\n4,5,6\n"
    normalized = _write_and_normalize(tmp_path, original)
    assert normalized == original.strip("\n")


def test_normalize_csv_removes_leading_sep_line(tmp_path):
    original = "sep=,\na,b\n1,2\n"
    normalized = _write_and_normalize(tmp_path, original)
    assert normalized == "a,b\n1,2"


def test_normalize_csv_strips_utf8_bom_from_first_line(tmp_path):
    original = "\ufeffa,b\n1,2\n"
    normalized = _write_and_normalize(tmp_path, original)
    assert normalized == "a,b\n1,2"


def test_normalize_csv_drops_empty_and_whitespace_only_lines(tmp_path):
    original = "\n\na,b\n\n1,2\n\n\n3,4\n"
    normalized = _write_and_normalize(tmp_path, original)
    assert normalized == "a,b\n1,2\n3,4"


def test_normalize_csv_collapses_internal_whitespace(tmp_path):
    original = "a,   b,   c\n1,\t2,\t3\n"
    normalized = _write_and_normalize(tmp_path, original)
    assert normalized == "a, b, c\n1, 2, 3"


def test_normalize_csv_handles_all_rules_together(tmp_path):
    original = "\ufeffsep=,\n\na,   b\n\n1,\t 2\n"
    normalized = _write_and_normalize(tmp_path, original)
    assert normalized == "a, b\n1, 2"


# --- Deduplication tests -----------------------------------------------------


def _make_fake_g_for_exists(files_in_folder):
    drive = SimpleNamespace(list_files=MagicMock(return_value=files_in_folder))
    return SimpleNamespace(drive=drive)


def test_file_exists_with_base_name_matches_by_base_name_only():
    existing_files = [
        SimpleNamespace(name="track_one.csv"),
        SimpleNamespace(name="other.txt"),
    ]
    g = _make_fake_g_for_exists(existing_files)

    assert process_new_files.file_exists_with_base_name(g, "folder-id", "track_one")
    assert not process_new_files.file_exists_with_base_name(
        g, "folder-id", "track_one (1)"
    )
    assert not process_new_files.file_exists_with_base_name(g, "folder-id", "missing")


def test_duplicate_csv_marks_source_as_possible_duplicate_and_skips_upload(tmp_path):
    g = SimpleNamespace()
    g.drive = SimpleNamespace(
        download_file=MagicMock(),
        ensure_folder=MagicMock(return_value="year-folder"),
        upload_csv_as_google_sheet=MagicMock(),
        move_file=MagicMock(),
        rename_file=MagicMock(),
    )
    g.sheets = SimpleNamespace(
        formatter=SimpleNamespace(apply_formatting_to_sheet=MagicMock())
    )

    file_meta = {"id": "file-1", "name": "2024-01-01_My_Set.csv"}

    with (
        patch.object(
            process_new_files, "file_exists_with_base_name", return_value=True
        ) as mock_exists,
        patch.object(process_new_files, "_normalize_csv") as mock_normalize,
    ):
        process_new_files.process_csv_file(g, file_meta, "2024")

    g.drive.download_file.assert_called_once()
    mock_normalize.assert_called_once()
    mock_exists.assert_called_once()
    g.drive.upload_csv_as_google_sheet.assert_not_called()
    g.drive.move_file.assert_not_called()
    g.drive.rename_file.assert_called_once_with(
        "file-1", "possible_duplicate_2024-01-01_My_Set.csv"
    )


def test_non_duplicate_csv_uploads_normally(tmp_path):
    g = SimpleNamespace()
    g.drive = SimpleNamespace(
        download_file=MagicMock(),
        ensure_folder=MagicMock(return_value="year-folder"),
        upload_csv_as_google_sheet=MagicMock(return_value="sheet-1"),
        move_file=MagicMock(),
        rename_file=MagicMock(),
    )
    g.sheets = SimpleNamespace(
        formatter=SimpleNamespace(apply_formatting_to_sheet=MagicMock())
    )

    file_meta = {"id": "file-2", "name": "2024-01-02_Other_Set.csv"}

    with (
        patch.object(
            process_new_files, "file_exists_with_base_name", return_value=False
        ) as mock_exists,
        patch.object(process_new_files, "_normalize_csv") as mock_normalize,
    ):
        process_new_files.process_csv_file(g, file_meta, "2024")

    g.drive.download_file.assert_called_once()
    mock_normalize.assert_called_once()
    mock_exists.assert_called_once()
    g.drive.upload_csv_as_google_sheet.assert_called_once()
    g.drive.rename_file.assert_not_called()


# --- Failure-path tests ------------------------------------------------------


def _fake_drive_for_failure(tmp_path):
    def download_file(file_id: str, dest: str) -> None:
        with open(dest, "w") as f:
            f.write("a,b\n1,2\n")

    drive = SimpleNamespace(
        download_file=MagicMock(side_effect=download_file),
        ensure_folder=MagicMock(return_value="year-folder"),
        upload_csv_as_google_sheet=MagicMock(),
        move_file=MagicMock(),
        rename_file=MagicMock(),
    )
    sheets = SimpleNamespace(
        formatter=SimpleNamespace(apply_formatting_to_sheet=MagicMock())
    )
    return SimpleNamespace(drive=drive, sheets=sheets)


def test_failure_on_one_file_marks_failed_and_continues_to_next(tmp_path):
    g = _fake_drive_for_failure(tmp_path)

    failing_meta = {"id": "file-fail", "name": "2024-01-01_Failing.csv"}
    succeeding_meta = {"id": "file-ok", "name": "2024-01-02_Ok.csv"}

    def upload_side_effect(temp_path: str, parent_id: str):
        if "Failing" in temp_path:
            raise RuntimeError("upload failed")
        return "sheet-ok"

    g.drive.upload_csv_as_google_sheet.side_effect = upload_side_effect

    with patch.object(
        process_new_files, "file_exists_with_base_name", return_value=False
    ) as mock_exists:
        process_new_files.process_csv_file(g, failing_meta, "2024")
        process_new_files.process_csv_file(g, succeeding_meta, "2024")

    mock_exists.assert_called()
    g.drive.rename_file.assert_any_call("file-fail", "FAILED_2024-01-01_Failing.csv")
    assert any(
        call_args[0][0].endswith("2024-01-02_Ok.csv")
        for call_args in g.drive.upload_csv_as_google_sheet.call_args_list
    )


def test_temp_file_is_removed_in_all_cases(tmp_path):
    g = _fake_drive_for_failure(tmp_path)
    file_meta = {"id": "file-temp", "name": "2024-01-03_WithTemp.csv"}

    g.drive.upload_csv_as_google_sheet.side_effect = RuntimeError("boom")

    process_new_files.process_csv_file(g, file_meta, "2024")

    temp_path = os.path.join("/tmp", file_meta["name"])
    assert not os.path.exists(temp_path)


# --- API ingest integration ---------------------------------------------------


def test_ingest_set_to_api_skips_when_base_url_not_set(monkeypatch):
    monkeypatch.delenv("KAIANO_API_BASE_URL", raising=False)
    g = SimpleNamespace()

    with patch.object(process_new_files, "log") as mock_log:
        process_new_files._ingest_set_to_api(
            spreadsheet_id="ssid",
            set_date="2024-01-01",
            venue="Venue",
            label="2024-01-01 Venue",
            g=g,
        )
        mock_log.warning.assert_called()


def test_ingest_set_to_api_posts_payload(monkeypatch):
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://example.test")

    # Provide fake kaiano.api modules for import inside _ingest_set_to_api.
    class FakeApiError(Exception):
        pass

    client = SimpleNamespace(post=MagicMock())

    class FakeClient:
        @classmethod
        def from_env(cls):
            return client

    sys.modules["kaiano.api"] = SimpleNamespace(KaianoApiClient=FakeClient)
    sys.modules["kaiano.api.errors"] = SimpleNamespace(KaianoApiError=FakeApiError)

    g = SimpleNamespace()

    with (
        patch.object(process_new_files, "read_tracks_from_sheet") as mock_read_tracks,
        patch.object(process_new_files, "build_ingest_payload") as mock_build,
    ):
        mock_read_tracks.return_value = [
            {"play_order": 1, "title": "Song", "artist": "Artist", "length": "01:00"}
        ]
        mock_build.return_value = {
            "set_date": "2024-01-01",
            "venue": "Venue",
            "source_file": "2024-01-01 Venue",
            "tracks": [{"play_order": 1, "title": "Song", "artist": "Artist"}],
        }

        process_new_files._ingest_set_to_api(
            spreadsheet_id="ssid",
            set_date="2024-01-01",
            venue="Venue",
            label="2024-01-01 Venue",
            g=g,
        )

    mock_read_tracks.assert_called_once_with(g, "ssid")
    mock_build.assert_called_once()
    client.post.assert_called_once()
    path, payload = client.post.call_args.args
    assert path == "/v1/ingest"
    assert payload["set_date"] == "2024-01-01"
    assert payload["venue"] == "Venue"
    assert payload["source_file"] == "2024-01-01 Venue"
    assert payload["tracks"]


def test_ingest_set_to_api_logs_error_on_api_error(monkeypatch):
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://example.test")

    class FakeApiError(Exception):
        pass

    class FakeClient:
        @classmethod
        def from_env(cls):
            return cls()

        def post(self, *_args, **_kwargs):
            raise FakeApiError("nope")

    sys.modules["kaiano.api"] = SimpleNamespace(KaianoApiClient=FakeClient)
    sys.modules["kaiano.api.errors"] = SimpleNamespace(KaianoApiError=FakeApiError)

    g = SimpleNamespace()

    with (
        patch.object(
            process_new_files,
            "read_tracks_from_sheet",
            return_value=[{"title": "t", "artist": "a"}],
        ),
        patch.object(
            process_new_files,
            "build_ingest_payload",
            return_value={"tracks": [{"title": "t", "artist": "a"}]},
        ),
        patch.object(process_new_files, "log") as mock_log,
    ):
        process_new_files._ingest_set_to_api(
            spreadsheet_id="ssid",
            set_date="2024-01-01",
            venue="Venue",
            label="label",
            g=g,
        )

        mock_log.error.assert_called()
