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
    with patch.object(ps, "_post_evaluation") as post:
        pe.post_run_finding("my-flow", "SUCCESS", production_only=True)
    payload = post.call_args.args[0]
    assert payload["repo"] == "deejay-cog"
    assert payload["flow_name"] == "my-flow"
    assert payload["severity"] == "SUCCESS"
    assert payload["finding"] == "Run completed successfully."


def test_post_run_finding_drops_absorbed_kwargs_from_text(monkeypatch) -> None:
    """sets_imported, total_tracks etc. must not surface in the finding text."""
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    with patch.object(ps, "_post_evaluation") as post:
        pe.post_run_finding(
            "my-flow",
            "SUCCESS",
            production_only=True,
            sets_imported=3,
            total_tracks=42,
            ingest_attempted=1,
        )
    finding = post.call_args.args[0]["finding"]
    assert "sets_imported" not in finding
    assert "total_tracks" not in finding
    # Non-absorbed counter does surface
    assert "ingest_attempted=1" in finding


def test_post_run_finding_production_only_false_no_post(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    with patch.object(ps, "_post_evaluation") as post:
        pe.post_run_finding("f", "SUCCESS", production_only=False)
    post.assert_not_called()


def test_post_run_finding_preserves_success_severity(monkeypatch) -> None:
    """Regression: SUCCESS must not be downgraded."""
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    with patch.object(ps, "_post_evaluation") as post:
        pe.post_run_finding("f", "SUCCESS", production_only=True)
    assert post.call_args.args[0]["severity"] == "SUCCESS"


def test_post_run_finding_omits_standards_version(monkeypatch) -> None:
    """Regression: standards_version must not appear in self-reported posts."""
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    monkeypatch.setenv("STANDARDS_VERSION", "6.0")
    with patch.object(ps, "_post_evaluation") as post:
        pe.post_run_finding("f", "SUCCESS", production_only=True)
    assert "standards_version" not in post.call_args.args[0]


def test_post_run_finding_explicit_source_is_forwarded(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    with patch.object(ps, "_post_evaluation") as post:
        pe.post_run_finding(
            "f", "WARN", text="bad", production_only=True, source="flow_hook"
        )
    assert post.call_args.args[0]["source"] == "flow_hook"


def test_post_run_finding_warn_includes_extras(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    with patch.object(ps, "_post_evaluation") as post:
        pe.post_run_finding(
            "f",
            "WARN",
            text="Completed with issues",
            production_only=True,
            spotify_failed=2,
        )
    assert post.call_args.args[0]["finding"] == (
        "Completed with issues spotify_failed=2"
    )


def test_make_failure_hook_binds_repo_and_emits_warn(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    hook = pe.make_failure_hook("fl", production_only=True)
    state = SimpleNamespace(name="Failed", type="FAILED")
    with patch.object(ps, "_post_evaluation") as post:
        hook(None, None, state)
    payload = post.call_args.args[0]
    assert payload["repo"] == "deejay-cog"
    assert payload["severity"] == "WARN"
    assert payload["source"] == "flow_hook"


def test_make_failure_hook_crashed_emits_error(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    hook = pe.make_failure_hook("fl", production_only=True)
    state = SimpleNamespace(name="Crashed", type="CRASHED")
    with patch.object(ps, "_post_evaluation") as post:
        hook(None, None, state)
    assert post.call_args.args[0]["severity"] == "ERROR"


def test_make_failure_hook_production_only_false_no_post(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    hook = pe.make_failure_hook("fl", production_only=False)
    state = SimpleNamespace(name="Failed", type="FAILED")
    with patch.object(ps, "_post_evaluation") as post:
        hook(None, None, state)
    post.assert_not_called()


def test_post_run_finding_swallows_underlying_exceptions(monkeypatch) -> None:
    """The library is best-effort; the shim must not regress on that."""
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    with patch.object(ps, "_post_evaluation", side_effect=RuntimeError("boom")):
        # Must not raise.
        pe.post_run_finding("f", "SUCCESS", production_only=True)


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
    with patch.object(ps, "_post_evaluation") as post:
        pe.post_run_finding("f", "SUCCESS", production_only=True, source="flow_hook")
    assert post.call_args.args[0]["source"] == "flow_hook"


def test_get_run_id_local_run_when_no_runtime_or_env(monkeypatch) -> None:
    monkeypatch.delenv("PREFECT_FLOW_RUN_ID", raising=False)
    with patch("prefect.runtime.flow_run.id", None):
        assert pe.get_run_id() == "local-run"


def test_get_run_id_prefers_runtime_id_over_env(monkeypatch) -> None:
    monkeypatch.setenv("PREFECT_FLOW_RUN_ID", "env-id")
    with patch("prefect.runtime.flow_run.id", "runtime-id"):
        assert pe.get_run_id() == "runtime-id"
