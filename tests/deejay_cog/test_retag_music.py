from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from prefect.testing.utilities import prefect_test_harness

import deejay_cog.retag_music as retag

# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------


def _make_file(file_id: str = "file-1", name: str = "track.mp3") -> SimpleNamespace:
    return SimpleNamespace(id=file_id, name=name)


def _make_drive(files: list | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        list_files=MagicMock(return_value=files or []),
        download_file=MagicMock(),
        update_file=MagicMock(),
        upload_file=MagicMock(),
        delete_file=MagicMock(),
    )


def _make_id_result(
    chosen_conf: float | None = None,
    metadata: object | None = None,
) -> SimpleNamespace:
    chosen = (
        SimpleNamespace(
            confidence=chosen_conf,
            mbid="mbid-123",
            title="Track Title",
            artist="Track Artist",
        )
        if chosen_conf is not None
        else None
    )
    return SimpleNamespace(
        chosen=chosen, candidates=[chosen] if chosen else [], metadata=metadata
    )


def _make_metadata(
    title: str = "Track Title", artist: str = "Artist", year: str = "2020"
) -> SimpleNamespace:
    return SimpleNamespace(title=title, artist=artist, year=year)


def _make_rename_result(dest_path: str, dest_name: str) -> SimpleNamespace:
    return SimpleNamespace(dest_path=dest_path, dest_name=dest_name)


# ---------------------------------------------------------------------------
# _list_music_files
# ---------------------------------------------------------------------------


def test_list_music_files_deduplicates_across_mime_types() -> None:
    file_a = _make_file("id-a", "a.mp3")
    file_b = _make_file("id-b", "b.flac")

    drive = SimpleNamespace(
        list_files=MagicMock(
            side_effect=[
                [file_a],  # audio/mpeg
                [],  # audio/mp4
                [],  # audio/x-m4a
                [file_b],  # audio/wav  (new file)
                [],  # audio/x-wav
                [],  # audio/flac
                [],  # audio/aac
                [],  # audio/ogg
                [],  # audio/x-aiff
                [],  # audio/aiff
            ]
        )
    )
    g = SimpleNamespace(drive=drive)
    result = retag._list_music_files(g, "folder-id")
    assert len(result) == 2
    ids = {f.id for f in result}
    assert ids == {"id-a", "id-b"}


def test_list_music_files_falls_back_when_no_mime_matches() -> None:
    fallback_file = _make_file("id-x", "mystery.ogg")
    drive = SimpleNamespace(
        list_files=MagicMock(side_effect=([[] for _ in range(10)] + [[fallback_file]]))
    )
    g = SimpleNamespace(drive=drive)
    result = retag._list_music_files(g, "folder-id")
    assert len(result) == 1
    assert result[0].id == "id-x"


# ---------------------------------------------------------------------------
# _format_candidate_summary / _format_metadata_summary
# ---------------------------------------------------------------------------


def test_format_candidate_summary_with_full_candidate() -> None:
    candidate = SimpleNamespace(confidence=0.95, mbid="abc", title="T", artist="A")
    summary = retag._format_candidate_summary(candidate)
    assert "confidence=0.950" in summary
    assert "id=abc" in summary
    assert "title=T" in summary
    assert "artist=A" in summary


def test_format_candidate_summary_no_confidence() -> None:
    candidate = SimpleNamespace(confidence=None, mbid="", title="", artist="")
    summary = retag._format_candidate_summary(candidate)
    assert "confidence=N/A" in summary


def test_format_metadata_summary_partial() -> None:
    metadata = SimpleNamespace(title="Song", artist="", year="2021")
    summary = retag._format_metadata_summary(metadata)
    assert "title=Song" in summary
    assert "year=2021" in summary
    assert "artist" not in summary


# ---------------------------------------------------------------------------
# retag_music_file task
# ---------------------------------------------------------------------------


def test_retag_music_file_skips_missing_file_id() -> None:
    g = SimpleNamespace(drive=_make_drive())
    file = SimpleNamespace(id=None, name="bad.mp3")
    delta = retag.retag_music_file.fn(
        g,
        file,
        identifier=MagicMock(),
        tagger=MagicMock(),
        renamer=MagicMock(),
        dest_folder_id="dest",
        min_confidence=0.90,
    )
    assert delta["downloaded"] == 0
    assert delta["failed"] == 0
    g.drive.download_file.assert_not_called()


def test_retag_music_file_update_in_place_on_no_candidates(tmp_path) -> None:
    drive = _make_drive()
    g = SimpleNamespace(drive=drive)
    file = _make_file("fid-1", "track.mp3")

    id_result = _make_id_result(chosen_conf=None, metadata=None)
    identifier = MagicMock()
    identifier.identify.return_value = id_result

    tagger = MagicMock()
    tagger.dump.return_value = {}
    renamer = MagicMock()

    with patch(
        "deejay_cog.retag_music.tempfile.gettempdir", return_value=str(tmp_path)
    ):
        delta = retag.retag_music_file.fn(
            g,
            file,
            identifier=identifier,
            tagger=tagger,
            renamer=renamer,
            dest_folder_id="dest-folder",
            min_confidence=0.90,
        )

    assert delta["downloaded"] == 1
    assert delta["uploaded"] == 1
    assert delta["identified"] == 0
    assert delta["tagged"] == 0
    assert delta["deleted"] == 0
    drive.update_file.assert_called_once()
    drive.upload_file.assert_not_called()
    drive.delete_file.assert_not_called()


def test_retag_music_file_update_in_place_on_low_confidence(tmp_path) -> None:
    drive = _make_drive()
    g = SimpleNamespace(drive=drive)
    file = _make_file("fid-2", "track.mp3")

    id_result = _make_id_result(chosen_conf=0.50, metadata=None)
    identifier = MagicMock()
    identifier.identify.return_value = id_result

    tagger = MagicMock()
    tagger.dump.return_value = {}
    renamer = MagicMock()

    with patch(
        "deejay_cog.retag_music.tempfile.gettempdir", return_value=str(tmp_path)
    ):
        delta = retag.retag_music_file.fn(
            g,
            file,
            identifier=identifier,
            tagger=tagger,
            renamer=renamer,
            dest_folder_id="dest-folder",
            min_confidence=0.90,
        )

    assert delta["uploaded"] == 1
    assert delta["identified"] == 0
    drive.update_file.assert_called_once()
    drive.upload_file.assert_not_called()


def test_retag_music_file_moves_to_dest_on_high_confidence(tmp_path) -> None:
    drive = _make_drive()
    g = SimpleNamespace(drive=drive)
    file = _make_file("fid-3", "track.mp3")

    metadata = _make_metadata()
    id_result = _make_id_result(chosen_conf=0.95, metadata=metadata)
    identifier = MagicMock()
    identifier.identify.return_value = id_result

    tagger = MagicMock()
    tagger.dump.return_value = {}

    renamed_path = str(tmp_path / "Artist - Track Title.mp3")
    renamer = MagicMock()
    renamer.apply.return_value = _make_rename_result(
        renamed_path, "Artist - Track Title.mp3"
    )

    with patch(
        "deejay_cog.retag_music.tempfile.gettempdir", return_value=str(tmp_path)
    ):
        delta = retag.retag_music_file.fn(
            g,
            file,
            identifier=identifier,
            tagger=tagger,
            renamer=renamer,
            dest_folder_id="dest-folder",
            min_confidence=0.90,
        )

    assert delta["downloaded"] == 1
    assert delta["tagged"] == 1
    assert delta["identified"] == 1
    assert delta["uploaded"] == 1
    assert delta["deleted"] == 1
    tagger.write.assert_called_once_with(
        str(tmp_path / "fid-3_track.mp3"), metadata, ensure_virtualdj_compat=True
    )
    renamer.apply.assert_called_once()
    drive.upload_file.assert_called_once_with(
        renamed_path, parent_id="dest-folder", dest_name="Artist - Track Title.mp3"
    )
    drive.delete_file.assert_called_once_with("fid-3")
    drive.update_file.assert_not_called()


def test_retag_music_file_tags_but_updates_in_place_when_metadata_but_low_confidence(
    tmp_path,
) -> None:
    """Metadata present (tags/rename applied) but confidence below threshold → update in place."""
    drive = _make_drive()
    g = SimpleNamespace(drive=drive)
    file = _make_file("fid-4", "track.mp3")

    metadata = _make_metadata()
    id_result = _make_id_result(chosen_conf=0.70, metadata=metadata)
    identifier = MagicMock()
    identifier.identify.return_value = id_result

    tagger = MagicMock()
    tagger.dump.return_value = {}

    renamed_path = str(tmp_path / "Artist - Track Title.mp3")
    renamer = MagicMock()
    renamer.apply.return_value = _make_rename_result(
        renamed_path, "Artist - Track Title.mp3"
    )

    with patch(
        "deejay_cog.retag_music.tempfile.gettempdir", return_value=str(tmp_path)
    ):
        delta = retag.retag_music_file.fn(
            g,
            file,
            identifier=identifier,
            tagger=tagger,
            renamer=renamer,
            dest_folder_id="dest-folder",
            min_confidence=0.90,
        )

    assert delta["tagged"] == 1
    assert delta["identified"] == 0
    assert delta["uploaded"] == 1
    assert delta["deleted"] == 0
    drive.update_file.assert_called_once()
    drive.upload_file.assert_not_called()


def test_retag_music_file_records_failure_on_exception(tmp_path) -> None:
    drive = MagicMock()
    drive.download_file.side_effect = RuntimeError("Drive error")
    g = SimpleNamespace(drive=drive)
    file = _make_file("fid-5", "bad.mp3")

    with patch(
        "deejay_cog.retag_music.tempfile.gettempdir", return_value=str(tmp_path)
    ):
        delta = retag.retag_music_file.fn(
            g,
            file,
            identifier=MagicMock(),
            tagger=MagicMock(),
            renamer=MagicMock(),
            dest_folder_id="dest",
            min_confidence=0.90,
        )

    assert delta["failed"] == 1
    assert delta["downloaded"] == 0


# ---------------------------------------------------------------------------
# retag_music_flow
# ---------------------------------------------------------------------------


def test_retag_music_flow_skips_when_no_acoustid_key(monkeypatch) -> None:
    monkeypatch.delenv("ACOUSTID_API_KEY", raising=False)

    with (
        patch.object(retag, "evaluate_pipeline_run") as mock_eval,
        prefect_test_harness(),
    ):
        summary = retag.retag_music_flow.fn()

    assert summary.scanned == 0
    assert summary.uploaded == 0
    mock_eval.assert_called_once()


def test_retag_music_flow_processes_files_and_returns_summary(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.setenv("ACOUSTID_API_KEY", "test-key")
    monkeypatch.setenv("MAX_UPLOADS_PER_RUN", "10")

    file_a = _make_file("id-a", "a.mp3")
    file_b = _make_file("id-b", "b.mp3")
    drive = _make_drive(files=[file_a, file_b])
    g = SimpleNamespace(drive=drive)

    # Both files: high confidence, with metadata
    metadata = _make_metadata()
    id_result = _make_id_result(chosen_conf=0.95, metadata=metadata)
    identifier = MagicMock()
    identifier.identify.return_value = id_result

    tagger = MagicMock()
    tagger.dump.return_value = {}

    renamed_path = str(tmp_path / "Artist - Track Title.mp3")
    renamer = MagicMock()
    renamer.apply.return_value = _make_rename_result(
        renamed_path, "Artist - Track Title.mp3"
    )

    with (
        patch.object(retag.GoogleAPI, "from_env", return_value=g),
        patch.object(retag, "Mp3Identifier") as mock_identifier_cls,
        patch.object(retag, "Mp3Tagger", return_value=tagger),
        patch.object(retag, "Mp3Renamer", return_value=renamer),
        patch.object(retag, "evaluate_pipeline_run") as mock_eval,
        patch.object(retag, "_list_music_files", return_value=[file_a, file_b]),
        patch("deejay_cog.retag_music.tempfile.gettempdir", return_value=str(tmp_path)),
        prefect_test_harness(),
    ):
        mock_identifier_cls.from_env.return_value = identifier
        summary = retag.retag_music_flow.fn()

    assert summary.scanned == 2
    assert summary.downloaded == 2
    assert summary.tagged == 2
    assert summary.identified == 2
    assert summary.uploaded == 2
    assert summary.deleted == 2
    assert summary.failed == 0
    mock_eval.assert_called_once()


def test_retag_music_flow_respects_max_uploads_per_run(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("ACOUSTID_API_KEY", "test-key")
    monkeypatch.setenv("MAX_UPLOADS_PER_RUN", "1")

    files = [_make_file(f"id-{i}", f"track{i}.mp3") for i in range(3)]

    metadata = _make_metadata()
    id_result = _make_id_result(chosen_conf=0.95, metadata=metadata)
    identifier = MagicMock()
    identifier.identify.return_value = id_result

    tagger = MagicMock()
    tagger.dump.return_value = {}

    renamed_path = str(tmp_path / "Artist - Track Title.mp3")
    renamer = MagicMock()
    renamer.apply.return_value = _make_rename_result(
        renamed_path, "Artist - Track Title.mp3"
    )

    with (
        patch.object(
            retag.GoogleAPI,
            "from_env",
            return_value=SimpleNamespace(drive=_make_drive()),
        ),
        patch.object(retag, "Mp3Identifier") as mock_identifier_cls,
        patch.object(retag, "Mp3Tagger", return_value=tagger),
        patch.object(retag, "Mp3Renamer", return_value=renamer),
        patch.object(retag, "evaluate_pipeline_run"),
        patch.object(retag, "_list_music_files", return_value=files),
        patch("deejay_cog.retag_music.tempfile.gettempdir", return_value=str(tmp_path)),
        prefect_test_harness(),
    ):
        mock_identifier_cls.from_env.return_value = identifier
        summary = retag.retag_music_flow.fn()

    assert summary.scanned == 1
    assert summary.uploaded == 1
    assert summary.skipped == 2
