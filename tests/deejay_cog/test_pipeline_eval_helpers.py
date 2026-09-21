"""Tests for the deejay-cog shim around mini_app_polis.pipeline_status.

The library is exhaustively tested in common-python-utils. These tests
cover the two shim-specific responsibilities:

1. ``repo="deejay-cog"`` is pre-bound on every outbound call.
2. Counters in ``_DEEJAY_ABSORBED_KWARGS`` are dropped before being
   forwarded, so they don't appear in the finding text suffix.

Other deejay-cog code (``process_new_files``, ``ingest_live_history``)
patches ``post_run_finding`` directly at its import site; those tests
live alongside the flow modules and don't need to know about the
underlying library.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import mini_app_polis.pipeline_status as ps

import deejay_cog._pipeline_eval as pe


def test_reexports_library_helpers() -> None:
    """The shim re-exports get_run_id / get_prefect_logger unchanged."""
    assert pe.get_run_id is ps.get_run_id
    assert pe.get_prefect_logger is ps.get_prefect_logger


def test_post_run_finding_binds_repo(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    with (
        patch.object(ps, "_deliver", return_value=True),
        patch.object(ps, "_build_message", wraps=ps._build_message) as post,
    ):
        pe.post_run_finding("my-flow", "SUCCESS", production_only=True, notable=True)
    payload = post.call_args.kwargs
    assert payload["repo"] == "deejay-cog"
    assert payload["flow_name"] == "my-flow"
    assert payload["severity"] == "SUCCESS"
    # Use ``in`` rather than ``==`` because the library appends
    # ``(processor=X.Y.Z)`` whenever the cog's distribution is installed
    # — which it is in editable test environments. The processor suffix
    # is the library's contract (tested in common-python-utils); this
    # shim's contract is the repo/flow/severity binding and the default
    # success text. Don't couple the two.
    assert "Run completed successfully." in payload["text"]


def test_post_run_finding_drops_absorbed_kwargs_from_text(monkeypatch) -> None:
    """sets_imported, total_tracks etc. must not surface in the finding text."""
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    with (
        patch.object(ps, "_deliver", return_value=True),
        patch.object(ps, "_build_message", wraps=ps._build_message) as post,
    ):
        pe.post_run_finding(
            "my-flow",
            "SUCCESS",
            production_only=True,
            notable=True,
            sets_imported=3,
            total_tracks=42,
            ingest_attempted=1,
        )
    finding = post.call_args.kwargs["text"]
    assert "sets_imported" not in finding
    assert "total_tracks" not in finding
    # Non-absorbed counter does surface
    assert "ingest_attempted=1" in finding


def test_post_run_finding_production_only_false_no_post(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    with (
        patch.object(ps, "_deliver", return_value=True),
        patch.object(ps, "_build_message", wraps=ps._build_message) as post,
    ):
        pe.post_run_finding("f", "SUCCESS", production_only=False)
    post.assert_not_called()


def test_post_run_finding_preserves_success_severity(monkeypatch) -> None:
    """Regression: SUCCESS must not be downgraded."""
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    with (
        patch.object(ps, "_deliver", return_value=True),
        patch.object(ps, "_build_message", wraps=ps._build_message) as post,
    ):
        pe.post_run_finding("f", "SUCCESS", production_only=True, notable=True)
    assert post.call_args.kwargs["severity"] == "SUCCESS"


def test_post_run_finding_goes_to_notify_not_evaluations(monkeypatch) -> None:
    """Regression: run status is a notification, not a finding.

    deejay-cog's runs are not graded against the standards catalog, so
    they must not land in the evaluations table — that conflation is why
    the API had to null out standards_version on those rows.
    """
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    client = MagicMock()
    with patch(
        "mini_app_polis.api.KaianoApiClient.from_env", return_value=client
    ) as from_env:
        pe.post_run_finding("f", "SUCCESS", production_only=True, notable=True)
    from_env.assert_called_once_with(machine_name="deejay-cog")
    client.notify.assert_called_once()
    client.post.assert_not_called()


def test_post_run_finding_explicit_source_is_forwarded(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    with (
        patch.object(ps, "_deliver", return_value=True),
        patch.object(ps, "_build_message", wraps=ps._build_message) as post,
    ):
        pe.post_run_finding(
            "f", "WARN", text="bad", production_only=True, source="flow_hook"
        )
    assert post.call_args.kwargs["source"] == "flow_hook"


def test_post_run_finding_warn_includes_extras(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    with (
        patch.object(ps, "_deliver", return_value=True),
        patch.object(ps, "_build_message", wraps=ps._build_message) as post,
    ):
        pe.post_run_finding(
            "f",
            "WARN",
            text="Completed with issues",
            production_only=True,
            spotify_failed=2,
        )
    # Counters follow the version stamp, so the last line is the metadata.
    assert post.call_args.kwargs["text"] == (
        f"Completed with issues\n{pe.VERSION_STAMP} spotify_failed=2"
    )


def test_make_failure_hook_binds_repo_and_emits_warn(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    hook = pe.make_failure_hook("fl", production_only=True)
    state = SimpleNamespace(name="Failed", type="FAILED")
    with (
        patch.object(ps, "_deliver", return_value=True),
        patch.object(ps, "_build_message", wraps=ps._build_message) as post,
    ):
        hook(None, None, state)
    payload = post.call_args.kwargs
    assert payload["repo"] == "deejay-cog"
    assert payload["severity"] == "WARN"
    assert payload["source"] == "flow_hook"


def test_make_failure_hook_crashed_emits_error(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    hook = pe.make_failure_hook("fl", production_only=True)
    state = SimpleNamespace(name="Crashed", type="CRASHED")
    with (
        patch.object(ps, "_deliver", return_value=True),
        patch.object(ps, "_build_message", wraps=ps._build_message) as post,
    ):
        hook(None, None, state)
    assert post.call_args.kwargs["severity"] == "ERROR"


def test_make_failure_hook_production_only_false_no_post(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    hook = pe.make_failure_hook("fl", production_only=False)
    state = SimpleNamespace(name="Failed", type="FAILED")
    with (
        patch.object(ps, "_deliver", return_value=True),
        patch.object(ps, "_build_message", wraps=ps._build_message) as post,
    ):
        hook(None, None, state)
    post.assert_not_called()


def test_post_run_finding_swallows_underlying_exceptions(monkeypatch) -> None:
    """The library is best-effort; the shim must not regress on that.

    Returning normally is the assertion that nothing propagated — a raise
    would surface as this test erroring. The report is checked too, so
    "swallowed it" cannot be confused with "never tried".
    """
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    with patch.object(ps, "_deliver", side_effect=RuntimeError("boom")) as deliver:
        result = pe.post_run_finding("f", "SUCCESS", production_only=True, notable=True)

    assert result.failed == 1
    assert result.sent == 0
    deliver.assert_called_once()


def test_make_failure_hook_swallows_post_exception(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    hook = pe.make_failure_hook("fl", production_only=True)
    state = SimpleNamespace(name="Failed", type="FAILED")
    mock_log = MagicMock()
    with (
        patch.object(ps, "post_run_finding", side_effect=RuntimeError("x")),
        patch.object(ps, "get_prefect_logger", return_value=mock_log),
    ):
        hook(None, None, state)
    mock_log.exception.assert_called()


def test_post_run_finding_source_in_kwargs_does_not_raise(monkeypatch) -> None:
    """Passing source=... as a kwarg is fine — it's a first-class param."""
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    with (
        patch.object(ps, "_deliver", return_value=True),
        patch.object(ps, "_build_message", wraps=ps._build_message) as post,
    ):
        pe.post_run_finding(
            "f", "SUCCESS", production_only=True, notable=True, source="flow_hook"
        )
    assert post.call_args.kwargs["source"] == "flow_hook"


def test_get_run_id_is_unattributable_without_prefect(monkeypatch) -> None:
    """Why the worker passes the message id as run_id.

    Both of get_run_id's resolution steps are Prefect's. With Prefect gone
    every run falls back to one id that identifies nothing, so a report
    that relied on it could not be joined to the run that sent it.
    """
    monkeypatch.delenv("PREFECT_FLOW_RUN_ID", raising=False)
    assert pe.get_run_id() == "local-run"


def test_post_run_finding_stamps_version_without_distribution(monkeypatch) -> None:
    """Regression: Lambda strips *.dist-info, so the library's own lookup
    finds nothing and stamps nothing. The shim stamps from _version.py."""
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    with (
        patch.object(ps, "_resolve_processor_version", return_value=None),
        patch.object(ps, "_deliver", return_value=True),
        patch.object(ps, "_build_message", wraps=ps._build_message) as post,
    ):
        pe.post_run_finding("f", "ERROR", text="worker failed", production_only=True)
    assert post.call_args.kwargs["text"] == f"worker failed\n{pe.VERSION_STAMP}"


def test_version_stamp_is_the_running_code() -> None:
    from deejay_cog import __version__

    assert f"(processor={__version__})" == pe.VERSION_STAMP


def test_post_run_finding_stamps_once_when_distribution_installed(
    monkeypatch,
) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    with (
        patch.object(ps, "_resolve_processor_version", return_value="9.9.9"),
        patch.object(ps, "_deliver", return_value=True),
        patch.object(ps, "_build_message", wraps=ps._build_message) as post,
    ):
        pe.post_run_finding("f", "SUCCESS", production_only=True, notable=True)
    text = post.call_args.kwargs["text"]
    assert text.count("(processor=") == 1
    assert pe.VERSION_STAMP in text


def test_stamp_version_leaves_empty_text_alone() -> None:
    assert pe.stamp_version("") == ""


def test_run_report_text_is_version_stamped() -> None:
    report = pe.RunReport(flow_name="f", repo=pe.REPO)
    report.ok(2)
    lines = report.text().splitlines()
    assert lines[0].startswith("Run complete in ")
    assert lines[-1] == pe.VERSION_STAMP


def test_run_report_send_carries_version_without_distribution(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    report = pe.RunReport(flow_name="f", repo=pe.REPO)
    report.ok()
    with (
        patch.object(ps, "_resolve_processor_version", return_value=None),
        patch.object(ps, "_deliver", return_value=True),
        patch.object(ps, "_build_message", wraps=ps._build_message) as post,
    ):
        report.send(notable=True)
    assert post.call_args.kwargs["text"].endswith(pe.VERSION_STAMP)
