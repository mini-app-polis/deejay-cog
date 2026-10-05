import pytest


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


@pytest.fixture
def typed_client():
    """A real KaianoApiClient whose ``post`` is the mock you pass in.

    The cog calls the API through the shared client's typed methods, which
    build the body from the request model and validate the answer against
    the response model. Only the HTTP call is replaced, so that path runs
    exactly as it does in production and a test can still assert on the
    path and body that were posted.
    """
    # From the defining module, not the package: some process_new_files
    # tests swap sys.modules["mini_app_polis.api"] for a fake while they
    # run, and this must be the real client.
    from mini_app_polis.api.client import KaianoApiClient

    def make(post):
        client = KaianoApiClient(base_url="https://example.test", api_key="k")
        client.post = post  # type: ignore[method-assign]
        return client

    return make


@pytest.fixture
def envelope():
    """Wrap ``data`` the way every Kaiano API response is wrapped."""

    def wrap(data):
        return {"data": data, "meta": {"count": 1, "total": 1, "version": "test"}}

    return wrap
