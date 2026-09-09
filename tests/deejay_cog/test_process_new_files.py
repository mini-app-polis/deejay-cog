import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import deejay_cog.process_new_files as process_new_files


def test_main_posts_single_success_finding_when_llm_and_api_configured(
    monkeypatch,
    prefect_test_harness,
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-anthropic")
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")

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
        patch.object(process_new_files, "post_run_finding") as mock_post,
        patch.object(process_new_files, "config") as mock_cfg,
    ):
        mock_cfg.CSV_SOURCE_FOLDER_ID = "src-folder"
        process_new_files.main()

    mock_post.assert_called_once()
    kw = mock_post.call_args.kwargs
    assert kw["flow_name"] == "process-new-csv-files"
    assert kw["severity"] == "SUCCESS"
    assert kw["production_only"] is True
    assert kw["sets_attempted"] == 1
    assert kw["sets_imported"] == 1
    assert kw["total_tracks"] == 11
    assert kw["collection_update"] is False


def test_main_skips_evaluate_without_anthropic(
    monkeypatch, prefect_test_harness
) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")

    drive = SimpleNamespace(list_files=MagicMock(return_value=[]))
    g = SimpleNamespace(drive=drive)

    with (
        patch.object(process_new_files.GoogleAPI, "from_env", return_value=g),
        patch.object(process_new_files, "normalize_prefixes_in_source"),
        patch.object(process_new_files, "post_run_finding") as mock_post,
        patch.object(process_new_files, "config") as mock_cfg,
    ):
        mock_cfg.CSV_SOURCE_FOLDER_ID = "src-folder"
        process_new_files.main()

    mock_post.assert_called_once()


def test_main_posts_single_warn_finding_when_sets_failed(
    monkeypatch, prefect_test_harness
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-anthropic")
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")

    fake_file = SimpleNamespace(id="f1", name="2024-01-02_My Venue.csv")
    drive = SimpleNamespace(list_files=MagicMock(return_value=[fake_file]))
    g = SimpleNamespace(drive=drive)

    def _fake_process_csv(g_api, meta, year, stats):
        stats.sets_imported += 1
        stats.sets_failed += 1
        return "imported"

    with (
        patch.object(process_new_files.GoogleAPI, "from_env", return_value=g),
        patch.object(process_new_files, "normalize_prefixes_in_source"),
        patch.object(
            process_new_files,
            "process_csv_file",
            side_effect=_fake_process_csv,
        ),
        patch.object(process_new_files, "post_run_finding") as mock_post,
        patch.object(process_new_files, "config") as mock_cfg,
    ):
        mock_cfg.CSV_SOURCE_FOLDER_ID = "src-folder"
        process_new_files.main()

    mock_post.assert_called_once()
    assert mock_post.call_args.kwargs["severity"] == "WARN"
    assert "sets_failed=1" in mock_post.call_args.kwargs["text"]


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


def test_file_already_in_folder_returns_true_when_parent_matches():
    from deejay_cog.process_new_files import _file_already_in_folder

    g = MagicMock()
    g.drive.service.files().get().execute.return_value = {
        "parents": ["archive-folder-id", "some-other-folder"],
    }

    assert _file_already_in_folder(g, "file-1", "archive-folder-id") is True


def test_file_already_in_folder_returns_false_when_parent_missing():
    from deejay_cog.process_new_files import _file_already_in_folder

    g = MagicMock()
    g.drive.service.files().get().execute.return_value = {
        "parents": ["year-folder-id"],
    }

    assert _file_already_in_folder(g, "file-1", "archive-folder-id") is False


def test_file_already_in_folder_returns_false_on_metadata_error():
    """Best-effort: a metadata-fetch failure must not crash the archive step."""
    from deejay_cog.process_new_files import _file_already_in_folder

    g = MagicMock()
    g.drive.service.files().get().execute.side_effect = RuntimeError("drive 500")

    assert _file_already_in_folder(g, "file-1", "archive-folder-id") is False


def test_process_csv_file_skips_archive_move_when_already_archived():
    """PIPE-013 idempotency: if file_id is already under Archive, the
    archive step does not call move_file a second time. Prevents retries
    from crashing on files that completed their archive step before a
    later task errored."""
    file_meta = {"id": "file-123", "name": "2024-01-03_Venue.csv"}

    def download_file(file_id: str, dest: str) -> None:
        with open(dest, "w") as f:
            f.write("Title,Artist\nSong A,Artist A\n")

    drive = SimpleNamespace(
        download_file=MagicMock(side_effect=download_file),
        ensure_folder=MagicMock(
            side_effect=lambda _parent, name: {
                "Archive": "archive-folder-id",
            }.get(name, "year-folder-id")
        ),
        upload_csv_as_google_sheet=MagicMock(return_value="sheet-id"),
        move_file=MagicMock(),
        rename_file=MagicMock(),
        list_files=MagicMock(return_value=[]),
        service=MagicMock(),
    )
    drive.service.files().get().execute.return_value = {
        "parents": ["archive-folder-id"],
    }
    sheets = SimpleNamespace(
        formatter=SimpleNamespace(apply_formatting_to_sheet=MagicMock())
    )
    g = SimpleNamespace(drive=drive, sheets=sheets)

    with patch.object(process_new_files, "read_tracks_from_sheet", return_value=[]):
        process_new_files.process_csv_file.fn(g, file_meta, "2024")

    drive.move_file.assert_not_called()


# --- API ingest integration ---------------------------------------------------


def test_ingest_set_to_api_skips_when_base_url_not_set(monkeypatch):
    monkeypatch.delenv("KAIANO_API_BASE_URL", raising=False)
    g = SimpleNamespace()

    mock_log = MagicMock()
    with patch.object(process_new_files, "get_prefect_logger", return_value=mock_log):
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
    # The cog builds its client through deejay_cog.api_client so every call
    # site presents the same machine identity; that is what to intercept.
    sys.modules["deejay_cog.api_client"] = SimpleNamespace(
        api_client=lambda *_a, **_k: client
    )
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
    # The cog builds its client through deejay_cog.api_client so every call
    # site presents the same machine identity; that is what to intercept.
    sys.modules["deejay_cog.api_client"] = SimpleNamespace(
        api_client=lambda *_a, **_k: FakeClient()
    )
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
        with patch.object(
            process_new_files, "get_prefect_logger", return_value=mock_log
        ):
            process_new_files._ingest_set_to_api.fn(
                spreadsheet_id="ssid",
                set_date="2024-01-01",
                venue="Venue",
                label="label",
                g=g,
            )

        mock_log.error.assert_called()


# -- severity when the ingest never happened -----------------------------------
#
# Four exits from _ingest_set_to_api, only two of which used to be visible
# to the run's severity. A delivery path that becomes a no-op because a
# variable is missing, while every instrument reports green, is the shape
# these pin against.


def test_missing_base_url_is_a_warn_not_a_success():
    """A run that sent nothing because a variable was unset is not green."""
    stats = process_new_files.CsvPipelineStats()
    stats.sets_imported = 3
    stats.ingest_skipped_env_missing = 3

    assert process_new_files._real_issue(stats) is True
    parts = " ".join(process_new_files._warn_parts(stats))
    assert "ingest_skipped_env_missing=3" in parts
    assert "KAIANO_API_BASE_URL" in parts


def test_unavailable_client_is_a_warn_not_a_success():
    """The same no-op, with nothing at all to read before this."""
    stats = process_new_files.CsvPipelineStats()
    stats.sets_imported = 2
    stats.ingest_client_unavailable = 2

    assert process_new_files._real_issue(stats) is True
    assert "ingest_client_unavailable=2" in " ".join(
        process_new_files._warn_parts(stats)
    )


def test_a_clean_run_is_still_a_success():
    """The fix must not turn a working run WARN."""
    stats = process_new_files.CsvPipelineStats()
    stats.sets_attempted = 2
    stats.sets_imported = 2
    stats.ingest_attempted = 2

    assert process_new_files._real_issue(stats) is False
    assert process_new_files._warn_parts(stats) == []
    assert process_new_files._common_eval(stats)["api_ingest_success"] is True


def test_unimportable_client_is_counted(monkeypatch):
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.test")
    stats = process_new_files.CsvPipelineStats()

    # An api_client module that raises on attribute access is what an
    # unimportable client looks like from inside the try block.
    with patch.dict(sys.modules, {"deejay_cog.api_client": None}):
        process_new_files._ingest_set_to_api.fn(
            spreadsheet_id="ssid",
            set_date="2026-01-03",
            venue="Venue",
            label="label",
            g=SimpleNamespace(),
            stats=stats,
        )

    assert stats.ingest_client_unavailable == 1
    assert stats.ingest_attempted == 0
    assert stats.ingest_failed == 0


def test_failure_before_the_post_is_counted(monkeypatch):
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.test")
    stats = process_new_files.CsvPipelineStats()

    sys.modules["deejay_cog.api_client"] = SimpleNamespace(
        api_client=lambda *_a, **_k: SimpleNamespace(post=MagicMock())
    )
    sys.modules["mini_app_polis.api.errors"] = SimpleNamespace(
        KaianoApiError=type("FakeApiError", (Exception,), {})
    )

    with patch.object(
        process_new_files,
        "read_tracks_from_sheet",
        side_effect=RuntimeError("sheet unreadable"),
    ):
        process_new_files._ingest_set_to_api.fn(
            spreadsheet_id="ssid",
            set_date="2026-01-03",
            venue="Venue",
            label="label",
            g=SimpleNamespace(),
            stats=stats,
        )

    assert stats.ingest_prepare_failed == 1
    assert stats.ingest_failed == 0
    assert process_new_files._real_issue(stats) is True


def test_api_ingest_success_is_false_when_nothing_was_attempted():
    """Zero failures out of zero attempts is not a success."""
    stats = process_new_files.CsvPipelineStats()
    stats.ingest_skipped_env_missing = 2

    assert stats.ingest_attempted == 0
    assert process_new_files._common_eval(stats)["api_ingest_success"] is False


def test_api_ingest_success_is_false_when_preparation_failed():
    stats = process_new_files.CsvPipelineStats()
    stats.ingest_prepare_failed = 1

    assert process_new_files._common_eval(stats)["api_ingest_success"] is False


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


def test_process_csv_file_skips_when_duplicate_exists_in_year_folder():
    """TEST-002: end-to-end dedup — a CSV whose base name already exists
    in the year folder is renamed with possible_duplicate_ prefix and
    skipped (no upload, no archive). Exercises the row-level dedup
    guard documented in evaluator.yaml via the flow entry point."""
    filename = "2024-01-03_Duplicate Venue.csv"
    file_meta = {"id": "file-dup", "name": filename}

    def download_file(file_id: str, dest: str) -> None:
        with open(dest, "w") as f:
            f.write("Title,Artist\nSong,Artist\n")

    existing_file = SimpleNamespace(
        name="2024-01-03_Duplicate Venue.csv",
        id="existing-file",
    )

    drive = SimpleNamespace(
        download_file=MagicMock(side_effect=download_file),
        ensure_folder=MagicMock(return_value="year-folder-id"),
        list_files=MagicMock(return_value=[existing_file]),
        rename_file=MagicMock(),
        move_file=MagicMock(),
        upload_csv_as_google_sheet=MagicMock(),
    )
    sheets = SimpleNamespace(
        formatter=SimpleNamespace(apply_formatting_to_sheet=MagicMock())
    )
    g = SimpleNamespace(drive=drive, sheets=sheets)

    stats = process_new_files.CsvPipelineStats()
    result = process_new_files.process_csv_file.fn(g, file_meta, "2024", stats)

    assert result == "duplicate"

    drive.rename_file.assert_called_once()
    new_name_arg = drive.rename_file.call_args[0][1]
    assert new_name_arg == f"possible_duplicate_{filename}"

    drive.upload_csv_as_google_sheet.assert_not_called()
    drive.move_file.assert_not_called()

    assert stats.duplicate_csv == 1
    assert stats.sets_imported == 0

    assert not os.path.exists(os.path.join("/tmp", filename))


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


# ── Failure path (TEST-003) ───────────────────────────────────────────────────


def test_main_flow_continues_after_single_file_failure(
    monkeypatch, prefect_test_harness
) -> None:
    """Main pipeline loop does not abort when process_csv_file raises — continues to next."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "")
    monkeypatch.setenv("KAIANO_API_BASE_URL", "")

    file_1 = SimpleNamespace(id="f-1", name="2024-01-01_Failing Venue.csv")
    file_2 = SimpleNamespace(id="f-2", name="2024-02-01_Good Venue.csv")

    drive = SimpleNamespace(list_files=MagicMock(return_value=[file_1, file_2]))
    g = SimpleNamespace(drive=drive)

    call_count = 0

    def _fake_process_csv(g_api, meta, year, stats):
        nonlocal call_count
        call_count += 1
        if meta["id"] == "f-1":
            raise RuntimeError("simulated failure")
        stats.sets_imported += 1
        return "imported"

    with (
        patch.object(process_new_files.GoogleAPI, "from_env", return_value=g),
        patch.object(process_new_files, "normalize_prefixes_in_source"),
        patch.object(
            process_new_files,
            "process_csv_file",
            side_effect=_fake_process_csv,
        ),
        patch.object(process_new_files, "config") as mock_cfg,
    ):
        mock_cfg.CSV_SOURCE_FOLDER_ID = "src-folder"
        process_new_files.main()

    # Both valid files were attempted — the failing one did not abort the loop
    assert call_count == 2


# ── Post-upload failures under a SUCCESS report ───────────────────────────────
#
# Each of these is a way a set could go missing while the run reported
# green: the archive move taking the ingest down with it, the failure
# counter sitting behind cleanup that fails too, an unmovable file that
# is retried forever, and a duplicate check that could not read the
# folder answering "no duplicate".


def _drive_for_post_upload(*, on_move=None, on_list=None):
    """A drive that gets one CSV as far as an uploaded sheet."""

    def download_file(_file_id: str, dest: str) -> None:
        with open(dest, "w") as f:
            f.write("Title,Artist\nSong,Artist\n")

    drive = SimpleNamespace(
        download_file=MagicMock(side_effect=download_file),
        ensure_folder=MagicMock(
            side_effect=lambda _parent, name: (
                "archive-folder-id" if name == "Archive" else "year-folder-id"
            )
        ),
        list_files=MagicMock(return_value=[], side_effect=on_list),
        upload_csv_as_google_sheet=MagicMock(return_value="sheet-id"),
        move_file=MagicMock(side_effect=on_move),
        rename_file=MagicMock(),
        service=MagicMock(),
    )
    drive.service.files().get().execute.return_value = {"parents": ["source-folder"]}
    sheets = SimpleNamespace(
        formatter=SimpleNamespace(apply_formatting_to_sheet=MagicMock())
    )
    return SimpleNamespace(drive=drive, sheets=sheets)


def test_archive_move_failure_still_ingests():
    """A Drive failure on the move must not skip the POST."""
    g = _drive_for_post_upload(on_move=RuntimeError("drive 503"))
    file_meta = {"id": "file-1", "name": "2024-01-03 Venue.csv"}
    stats = process_new_files.CsvPipelineStats()

    with (
        patch.object(process_new_files, "read_tracks_from_sheet", return_value=[]),
        patch.object(process_new_files, "_ingest_set_to_api") as mock_ingest,
        patch.object(process_new_files, "_sync_set_to_spotify") as mock_sync,
    ):
        result = process_new_files.process_csv_file.fn(g, file_meta, "2024", stats)

    assert result == "imported"
    mock_ingest.assert_called_once()
    assert mock_ingest.call_args.kwargs["spreadsheet_id"] == "sheet-id"
    mock_sync.assert_called_once()
    assert stats.archive_move_failed == 1
    assert process_new_files._real_issue(stats) is True


def test_archive_move_failure_is_a_warn():
    stats = process_new_files.CsvPipelineStats(archive_move_failed=1)

    assert process_new_files._real_issue(stats) is True
    assert "archive_move_failed=1" in " ".join(process_new_files._warn_parts(stats))
    assert process_new_files._common_eval(stats)["archive_move_failed"] == 1


def test_failed_set_is_counted_even_when_the_rename_fails():
    """sets_failed moves before the cleanup that can fail with it."""
    g = _drive_for_post_upload()
    g.drive.upload_csv_as_google_sheet.side_effect = RuntimeError("drive quota")
    g.drive.rename_file.side_effect = RuntimeError("drive quota")
    file_meta = {"id": "file-2", "name": "2024-01-04 Venue.csv"}
    stats = process_new_files.CsvPipelineStats()

    result = process_new_files.process_csv_file.fn(g, file_meta, "2024", stats)

    assert result == "failed"
    assert stats.sets_failed == 1
    assert stats.failed_set_labels == ["2024-01-04 Venue"]
    assert process_new_files._real_issue(stats) is True


def test_unknown_duplicate_check_flags_rather_than_imports():
    """None from file_exists_with_base_name takes the duplicate branch."""
    g = _drive_for_post_upload(on_list=RuntimeError("rate limit exceeded"))
    filename = "2024-01-05 Venue.csv"
    file_meta = {"id": "file-3", "name": filename}
    stats = process_new_files.CsvPipelineStats()

    assert (
        process_new_files.file_exists_with_base_name(g, "year-folder-id", "anything")
        is None
    )

    result = process_new_files.process_csv_file.fn(g, file_meta, "2024", stats)

    assert result == "duplicate"
    g.drive.upload_csv_as_google_sheet.assert_not_called()
    g.drive.rename_file.assert_called_once_with(
        "file-3", f"possible_duplicate_{filename}"
    )
    assert stats.duplicate_check_failed == 1
    assert stats.sets_imported == 0
    assert process_new_files._real_issue(stats) is True


def test_unmovable_non_csv_file_is_counted():
    drive = SimpleNamespace(
        ensure_folder=MagicMock(return_value="year-folder-id"),
        list_files=MagicMock(return_value=[]),
        move_file=MagicMock(side_effect=RuntimeError("drive 503")),
        rename_file=MagicMock(),
    )
    g = SimpleNamespace(drive=drive)
    file_meta = {"id": "file-4", "name": "2024-01-15_flyer.pdf"}
    stats = process_new_files.CsvPipelineStats()

    with patch.object(process_new_files, "config") as mock_cfg:
        mock_cfg.DJ_SETS_FOLDER_ID = "dj-sets-folder"
        process_new_files.process_non_csv_file(g, file_meta, "2024", stats)

    assert stats.non_csv_move_failed == 1
    assert stats.sets_skipped_non_csv == 0
    assert process_new_files._real_issue(stats) is True
