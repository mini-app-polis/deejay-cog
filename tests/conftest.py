import sys
import types

import pytest


def _install_mini_app_polis_stubs() -> None:
    """
    Inject stub submodules into the real `mini_app_polis` package for
    attributes/submodules that don't exist in the installed version of
    common-python-utils, or to provide test-safe config values.
    """
    import mini_app_polis as _real  # noqa: F401

    # ── mini_app_polis.config ────────────────────────────────────────────────
    # Patch missing config attributes onto the real config module so tests
    # that rely on them at import time don't crash.
    import mini_app_polis.config as _config  # noqa: F401

    for name in [
        "DJ_SETS_FOLDER_ID",
        "OUTPUT_NAME",
        "TEMP_TAB_NAME",
        "SUMMARY_TAB_NAME",
        "CSV_SOURCE_FOLDER_ID",
        "SUMMARY_FOLDER_NAME",
        "DEEJAY_SET_COLLECTION_JSON_PATH",
        "TIMEZONE",
    ]:
        if not hasattr(_config, name):
            setattr(_config, name, "")

    # ── mini_app_polis.logger ────────────────────────────────────────────────
    # Provide a no-op logger stub if the real one doesn't expose get_logger.
    import mini_app_polis.logger as _logger_mod  # noqa: F401

    if not hasattr(_logger_mod, "get_logger"):

        class _DummyLogger:
            def info(self, *_args, **_kwargs) -> None:
                return None

            def debug(self, *_args, **_kwargs) -> None:
                return None

            def warning(self, *_args, **_kwargs) -> None:
                return None

            def error(self, *_args, **_kwargs) -> None:
                return None

            def exception(self, *_args, **_kwargs) -> None:
                return None

        _logger_mod.get_logger = lambda: _DummyLogger()  # type: ignore[attr-defined]

    # ── mini_app_polis.google ────────────────────────────────────────────────
    import mini_app_polis.google as _google_mod  # noqa: F401

    if not hasattr(_google_mod, "GoogleAPI"):

        class GoogleAPI:  # pragma: no cover
            pass

        _google_mod.GoogleAPI = GoogleAPI  # type: ignore[attr-defined]

    # ── mini_app_polis.api ───────────────────────────────────────────────────
    import mini_app_polis.api as _api_mod  # noqa: F401

    if not hasattr(_api_mod, "KaianoApiClient"):

        class KaianoApiError(Exception):
            pass

        class KaianoApiClient:
            def __init__(self, *_args, **_kwargs) -> None:
                return None

            def post(self, *_args, **_kwargs) -> dict:
                return {}

            @classmethod
            def from_env(cls) -> "KaianoApiClient":
                return cls()

        _api_mod.KaianoApiClient = KaianoApiClient  # type: ignore[attr-defined]
        _api_mod.KaianoApiError = KaianoApiError  # type: ignore[attr-defined]

    if "mini_app_polis.api.errors" not in sys.modules:
        api_errors_mod = types.ModuleType("mini_app_polis.api.errors")
        if hasattr(_api_mod, "KaianoApiError"):
            api_errors_mod.KaianoApiError = _api_mod.KaianoApiError  # type: ignore[attr-defined]
        sys.modules["mini_app_polis.api.errors"] = api_errors_mod


_install_mini_app_polis_stubs()


@pytest.fixture(scope="session")
def prefect_test_harness():
    """Session-scoped Prefect test harness.

    Each call to ``prefect.testing.utilities.prefect_test_harness`` boots
    an in-memory Prefect server (sqlite migrations, event subsystem, the
    works). Doing that per-test costs ~1–2 s each, which dominated the
    deejay-cog suite. This fixture spins one up once for the whole
    session and yields control to every test that asks for it.

    Tests that previously did::

        with prefect_test_harness():
            my_flow()

    should now consume the fixture and call the flow directly::

        def test_x(prefect_test_harness):
            my_flow()
    """
    # Imported lazily so test collections that don't touch Prefect don't
    # pay the import cost.
    from prefect.testing.utilities import (
        prefect_test_harness as _real_prefect_test_harness,
    )

    with _real_prefect_test_harness():
        yield


@pytest.fixture(autouse=True)
def _production_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the environment so assertions do not depend on the host shell.

    Effect gates and the Discord title prefix both resolve from the
    environment, and an unset one resolves to local. Left to inherit
    whatever ENVIRONMENT the launching shell carries, this suite asserts
    different rendered titles on a laptop than in CI — and the lenient
    run is the one that hides the regression. Tests that want the
    non-production path set ENVIRONMENT themselves.
    """
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.delenv("PREFECT_TRIGGER_ENABLED", raising=False)
    monkeypatch.delenv("HEALTHCHECKS_ENABLED", raising=False)
