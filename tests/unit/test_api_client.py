"""This cog's identity — exercised through the path production actually uses.

These previously asserted on a local ``api_key()`` helper that read the
variable directly. The shared client derives it from the machine name instead,
so that helper was a second path only the tests took — it would have kept
passing while the real one broke.
"""

from __future__ import annotations

import pytest

from deejay_cog.api_client import MACHINE_NAME, api_client


def test_presents_this_cogs_own_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEEJAY_COG_API_KEY", "k_deejay")
    client = api_client()
    assert client.api_key == "k_deejay"
    assert client._headers()["Authorization"] == "Bearer k_deejay"


def test_does_not_pick_up_another_cogs_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """One shared Doppler config holds every cog's key; take only ours."""
    monkeypatch.delenv("DEEJAY_COG_API_KEY", raising=False)
    monkeypatch.delenv("KAIANO_API_KEY", raising=False)
    monkeypatch.setenv("EVALUATOR_COG_API_KEY", "k_evaluator")
    assert api_client().api_key is None


def test_blank_key_is_treated_as_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """An existing but empty variable must fall back, not send an empty key."""
    monkeypatch.setenv("DEEJAY_COG_API_KEY", "   ")
    monkeypatch.delenv("KAIANO_API_KEY", raising=False)
    assert api_client().api_key is None


def test_machine_name_matches_the_api_declaration() -> None:
    """The API derives DEEJAY_COG_API_KEY from this exact string."""
    assert MACHINE_NAME == "deejay-cog"


def test_base_url_is_passed_through(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEEJAY_COG_API_KEY", "k_deejay")
    assert api_client(base_url="https://api.example/").base_url == "https://api.example"
