from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import deejay_cog._pipeline_eval as pe


def test_get_run_id_local_run_when_no_runtime_or_env(monkeypatch) -> None:
    monkeypatch.delenv("PREFECT_FLOW_RUN_ID", raising=False)
    with patch("prefect.runtime.flow_run.id", None):
        assert pe.get_run_id() == "local-run"


def test_get_run_id_prefers_prefect_env_when_no_runtime_id(monkeypatch) -> None:
    monkeypatch.setenv("PREFECT_FLOW_RUN_ID", "run-from-env")
    with patch("prefect.runtime.flow_run.id", None):
        assert pe.get_run_id() == "run-from-env"


def test_get_run_id_prefers_runtime_id_over_env(monkeypatch) -> None:
    monkeypatch.setenv("PREFECT_FLOW_RUN_ID", "env-id")
    with patch("prefect.runtime.flow_run.id", "runtime-id"):
        assert pe.get_run_id() == "runtime-id"


def test_get_run_id_ignores_github_run_id(monkeypatch) -> None:
    monkeypatch.delenv("PREFECT_FLOW_RUN_ID", raising=False)
    monkeypatch.setenv("GITHUB_RUN_ID", "gha-999")
    with patch("prefect.runtime.flow_run.id", None):
        assert pe.get_run_id() == "local-run"


def test_post_run_finding_production_only_false_never_calls_evaluate(
    monkeypatch,
) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
    with patch("evaluator_cog.flows.pipeline_eval.evaluate_pipeline_run") as ev:
        pe.post_run_finding(
            "test-flow",
            "SUCCESS",
            production_only=False,
            sets_imported=1,
        )
    ev.assert_not_called()


def test_post_run_finding_production_true_noop_without_env(monkeypatch) -> None:
    monkeypatch.delenv("KAIANO_API_BASE_URL", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with patch("evaluator_cog.flows.pipeline_eval.evaluate_pipeline_run") as ev:
        pe.post_run_finding("f", "SUCCESS", production_only=True)
    ev.assert_not_called()


def test_post_run_finding_production_posts_when_env_set(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
    with patch("evaluator_cog.flows.pipeline_eval.evaluate_pipeline_run") as ev:
        pe.post_run_finding(
            "my-flow",
            "WARN",
            text="Something",
            production_only=True,
            sets_imported=2,
        )
    ev.assert_called_once()
    kw = ev.call_args.kwargs
    assert kw["repo"] == "deejay-cog"
    assert kw["flow_name"] == "my-flow"
    assert kw["direct_finding_text"] == "Something"
    assert kw["direct_severity"] == "WARN"
    assert kw["sets_imported"] == 2


def test_post_run_finding_default_source_is_flow_inline(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
    with patch("evaluator_cog.flows.pipeline_eval.evaluate_pipeline_run") as ev:
        pe.post_run_finding("f", "SUCCESS", production_only=True)
    assert ev.call_args.kwargs["source"] == "flow_inline"


def test_post_run_finding_explicit_source_is_forwarded(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
    with patch("evaluator_cog.flows.pipeline_eval.evaluate_pipeline_run") as ev:
        pe.post_run_finding("f", "WARN", production_only=True, source="flow_hook")
    assert ev.call_args.kwargs["source"] == "flow_hook"


def test_post_run_finding_success_default_text(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
    with patch("evaluator_cog.flows.pipeline_eval.evaluate_pipeline_run") as ev:
        pe.post_run_finding("f", "SUCCESS", production_only=True)
    assert ev.call_args.kwargs["direct_finding_text"] == "Run completed successfully."
    assert ev.call_args.kwargs["direct_severity"] == "SUCCESS"


def test_post_run_finding_swallows_evaluate_exception(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
    with patch(
        "evaluator_cog.flows.pipeline_eval.evaluate_pipeline_run",
        side_effect=RuntimeError("boom"),
    ):
        pe.post_run_finding("f", "SUCCESS", production_only=True)


def test_make_failure_hook_crashed_posts_error(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
    hook = pe.make_failure_hook("fl", production_only=True)
    state = SimpleNamespace(name="Crashed", type="CRASHED")
    with patch("evaluator_cog.flows.pipeline_eval.evaluate_pipeline_run") as ev:
        hook(None, None, state)
    assert ev.call_args.kwargs["direct_severity"] == "ERROR"


def test_make_failure_hook_failed_posts_warn(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
    hook = pe.make_failure_hook("fl", production_only=True)
    state = SimpleNamespace(name="Failed", type="FAILED")
    with patch("evaluator_cog.flows.pipeline_eval.evaluate_pipeline_run") as ev:
        hook(None, None, state)
    assert ev.call_args.kwargs["direct_severity"] == "WARN"


def test_make_failure_hook_posts_source_flow_hook(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
    hook = pe.make_failure_hook("fl", production_only=True)
    state = SimpleNamespace(name="Failed", type="FAILED")
    with patch("evaluator_cog.flows.pipeline_eval.evaluate_pipeline_run") as ev:
        hook(None, None, state)
    assert ev.call_args.kwargs["source"] == "flow_hook"


def test_make_failure_hook_production_only_false_no_post(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
    hook = pe.make_failure_hook("fl", production_only=False)
    state = SimpleNamespace(name="Failed", type="FAILED")
    with patch("evaluator_cog.flows.pipeline_eval.evaluate_pipeline_run") as ev:
        hook(None, None, state)
    ev.assert_not_called()


def test_make_failure_hook_swallows_post_run_finding_exception(monkeypatch) -> None:
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
    hook = pe.make_failure_hook("fl", production_only=True)
    state = SimpleNamespace(name="Failed", type="FAILED")
    mock_log = MagicMock()
    with (
        patch.object(pe, "post_run_finding", side_effect=RuntimeError("x")),
        patch.object(pe, "get_prefect_logger", return_value=mock_log),
    ):
        hook(None, None, state)
    mock_log.exception.assert_called()


def test_post_run_finding_source_in_counters_does_not_raise(monkeypatch) -> None:
    """If a caller mistakenly passes source= via **counters, no TypeError is raised.

    source is a first-class parameter and no longer in passthrough kwargs.
    """
    monkeypatch.setenv("KAIANO_API_BASE_URL", "https://api.example")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
    with patch("evaluator_cog.flows.pipeline_eval.evaluate_pipeline_run") as ev:
        pe.post_run_finding(
            "f",
            "SUCCESS",
            production_only=True,
            source="flow_hook",
        )
    assert ev.call_args.kwargs["source"] == "flow_hook"
