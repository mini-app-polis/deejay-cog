import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from prefect.testing.utilities import prefect_test_harness

import deejay_cog.process_new_files as process_new_files


def test_main_calls_evaluate_when_llm_and_api_configured(monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-anthropic")
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    monkeypatch.setenv("GITHUB_RUN_ID", "42")

    fake_file = SimpleNamespace(id="f1", name="2024-01-02_My Venue.csv")
    drive = SimpleNamespace(list_files=MagicMock(return_value=[fake_file]))
    g = SimpleNamespace(drive=drive)

    def _fake_process_csv(g_api, meta, year, stats):
        stats.sets_imported += 1
        stats.total_tracks += 11
        return "imported"

    with (
        patch.object(process_new_files.GoogleAPI, "from_env", return_value=g),
        patch.object(process_new_files, "normalize_prefixes_in_source"),
        patch.object(
            process_new_files,
            "process_csv_file",
            side_effect=_fake_process_csv,
        ),
        patch.object(process_new_files, "evaluate_pipeline_run") as mock_eval,
        patch.object(process_new_files, "config") as mock_cfg,
    ):
        mock_cfg.CSV_SOURCE_FOLDER_ID = "src-folder"
        with prefect_test_harness():
            process_new_files.main()

    mock_eval.assert_called_once()
    kw = mock_eval.call_args.kwargs
    assert kw["run_id"] == "42"
    assert kw["sets_attempted"] == 1
    assert kw["sets_imported"] == 1
    assert kw["total_tracks"] == 11
    assert kw["collection_update"] is False


def test_main_skips_evaluate_without_anthropic(monkeypatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")

    drive = SimpleNamespace(list_files=MagicMock(return_value=[]))
    g = SimpleNamespace(drive=drive)

    with (
        patch.object(process_new_files.GoogleAPI, "from_env", return_value=g),
        patch.object(process_new_files, "normalize_prefixes_in_source"),
        patch.object(process_new_files, "evaluate_pipeline_run") as mock_eval,
        patch.object(process_new_files, "config") as mock_cfg,
    ):
        mock_cfg.CSV_SOURCE_FOLDER_ID = "src-folder"
        with prefect_test_harness():
            process_new_files.main()

    mock_eval.assert_not_called()


# --- Normalization tests -----------------------------------------------------


def _write_and_normalize(tmp_path, contents: str) -> str:
    path = tmp_path / "input.csv"
    path.write_text(contents)
    process_new_files._normalize_csv.fn(str(path))
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


def test_temp_file_is_removed_in_all_cases(tmp_path):
    g = _fake_drive_for_failure(tmp_path)
    file_meta = {"id": "file-temp", "name": "2024-01-03_WithTemp.csv"}

    g.drive.upload_csv_as_google_sheet.side_effect = RuntimeError("boom")

    process_new_files.process_csv_file.fn(g, file_meta, "2024")

    temp_path = os.path.join("/tmp", file_meta["name"])
    assert not os.path.exists(temp_path)


# --- API ingest integration ---------------------------------------------------


def test_ingest_set_to_api_skips_when_base_url_not_set(monkeypatch):
    monkeypatch.delenv("KAIANO_API_BASE_URL", raising=False)
    g = SimpleNamespace()

    mock_log = MagicMock()
    with patch.object(process_new_files, "_prefect_logger", return_value=mock_log):
        process_new_files._ingest_set_to_api.fn(
            spreadsheet_id="ssid",
            set_date="2024-01-01",
            venue="Venue",
            label="2024-01-01 Venue",
            g=g,
        )
        mock_log.warning.assert_called()


def test_ingest_set_to_api_posts_payload(monkeypatch):
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://example.test")

    # Provide fake mini_app_polis.api modules for import inside _ingest_set_to_api.
    class FakeApiError(Exception):
        pass

    client = SimpleNamespace(post=MagicMock())

    class FakeClient:
        @classmethod
        def from_env(cls):
            return client

    sys.modules["mini_app_polis.api"] = SimpleNamespace(KaianoApiClient=FakeClient)
    sys.modules["mini_app_polis.api.errors"] = SimpleNamespace(
        KaianoApiError=FakeApiError
    )

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

        process_new_files._ingest_set_to_api.fn(
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

    sys.modules["mini_app_polis.api"] = SimpleNamespace(KaianoApiClient=FakeClient)
    sys.modules["mini_app_polis.api.errors"] = SimpleNamespace(
        KaianoApiError=FakeApiError
    )

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
    ):
        mock_log = MagicMock()
        with patch.object(process_new_files, "_prefect_logger", return_value=mock_log):
            process_new_files._ingest_set_to_api.fn(
                spreadsheet_id="ssid",
                set_date="2024-01-01",
                venue="Venue",
                label="label",
                g=g,
            )

        mock_log.error.assert_called()


# -- _extract_year_from_filename ----------------------------------------------


def test_extract_year_from_filename_returns_year_from_valid_name():
    assert (
        process_new_files._extract_year_from_filename("2024-01-15_My Set.csv") == "2024"
    )


def test_extract_year_from_filename_returns_none_when_no_year():
    assert process_new_files._extract_year_from_filename("My Set.csv") is None


def test_extract_year_from_filename_returns_none_for_empty_string():
    assert process_new_files._extract_year_from_filename("") is None


# -- _extract_date_and_venue ---------------------------------------------------


def test_extract_date_and_venue_parses_standard_name():
    date, venue = process_new_files._extract_date_and_venue("2024-01-15 MADjam WCS")
    assert date == "2024-01-15"
    assert venue == "MADjam WCS"


def test_extract_date_and_venue_returns_none_when_no_date():
    date, venue = process_new_files._extract_date_and_venue("No Date")
    assert date is None
    assert venue is None


# -- rename_file_as_duplicate --------------------------------------------------


def test_rename_file_as_duplicate_calls_drive_rename():
    mock_drive = SimpleNamespace(rename_file=MagicMock())
    g = SimpleNamespace(drive=mock_drive)
    process_new_files.rename_file_as_duplicate(g, "file-id", "2024-01-01_Set.csv")
    mock_drive.rename_file.assert_called_once_with(
        "file-id", "possible_duplicate_2024-01-01_Set.csv"
    )


# -- process_non_csv_file ------------------------------------------------------


def test_process_non_csv_file_moves_to_year_folder():
    mock_drive = SimpleNamespace(
        ensure_folder=MagicMock(return_value="year-folder-id"),
        move_file=MagicMock(),
        list_files=MagicMock(return_value=[]),
    )
    g = SimpleNamespace(drive=mock_drive)
    file_meta = {"id": "file-1", "name": "2024-01-15_flyer.pdf"}

    with patch.object(process_new_files, "config") as mock_cfg:
        mock_cfg.DJ_SETS_FOLDER_ID = "dj-sets-folder"
        process_new_files.process_non_csv_file(g, file_meta, "2024")

    mock_drive.move_file.assert_called_once_with(
        "file-1", new_parent_id="year-folder-id", remove_from_parents=True
    )
