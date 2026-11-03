"""Fixtures for the music-tagging tests."""

from __future__ import annotations

import pytest
from music_fakes import FakeDrive


@pytest.fixture
def drive() -> FakeDrive:
    """An in-memory Drive that records every call that would change it."""
    return FakeDrive()
