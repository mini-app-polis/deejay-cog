from unittest.mock import patch

from deejay_set_processor.evaluation_webhook import handle_prefect_flow_run_event


def test_crashed_flow_posts_error_finding() -> None:
    payload = {
        "flow_run_id": "run-1",
        "flow_name": "process-new-csv-files",
        "state_name": "Crashed",
        "state_type": "CRASHED",
        "start_time": "2026-03-19T10:00:00Z",
        "end_time": "2026-03-19T10:01:00Z",
    }

    with patch(
        "deejay_set_processor.evaluation_webhook.evaluate_pipeline_run"
    ) as mock_eval:
        handle_prefect_flow_run_event(payload)

    mock_eval.assert_called_once()
    kw = mock_eval.call_args.kwargs
    assert kw["run_id"] == "run-1"
    assert kw["repo"] == "deejay-set-processor-dev"
    assert kw["collection_update"] is False
    assert kw["direct_severity"] == "ERROR"
    assert "entered Crashed state" in kw["direct_finding_text"]


def test_failed_flow_posts_warn_finding() -> None:
    payload = {
        "flow_run_id": "run-2",
        "flow_name": "update-dj-set-collection",
        "state_name": "Failed",
        "state_type": "FAILED",
        "start_time": "2026-03-19T10:00:00Z",
        "end_time": "2026-03-19T10:01:00Z",
    }

    with patch(
        "deejay_set_processor.evaluation_webhook.evaluate_pipeline_run"
    ) as mock_eval:
        handle_prefect_flow_run_event(payload)

    mock_eval.assert_called_once()
    kw = mock_eval.call_args.kwargs
    assert kw["run_id"] == "run-2"
    assert kw["collection_update"] is True
    assert kw["direct_severity"] == "WARN"
    assert "entered Failed state" in kw["direct_finding_text"]


def test_completed_flow_calls_evaluator_normally() -> None:
    payload = {
        "flow_run_id": "run-3",
        "flow_name": "update-dj-set-collection",
        "state_name": "Completed",
        "state_type": "COMPLETED",
        "start_time": "2026-03-19T10:00:00Z",
        "end_time": "2026-03-19T10:01:00Z",
    }

    with patch(
        "deejay_set_processor.evaluation_webhook.evaluate_pipeline_run"
    ) as mock_eval:
        handle_prefect_flow_run_event(payload)

    mock_eval.assert_called_once()
    kw = mock_eval.call_args.kwargs
    assert kw["run_id"] == "run-3"
    assert kw["collection_update"] is True
    assert kw.get("direct_finding_text") is None
    assert kw.get("direct_severity") is None
