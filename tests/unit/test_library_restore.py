"""Putting the library's tags back to what each file held at a time."""

from __future__ import annotations

import pytest

from deejay_cog import library_restore as lr


def test_only_fields_that_differ_come_back_with_the_old_values() -> None:
    before = {"title": "Show Me", "artist": "Usher", "year": "2012", "genre": "R&B"}
    now = {"title": "Show Me", "artist": "Usher", "year": "2024", "genre": "R&B/Soul"}
    assert lr.fields_to_restore(before, now) == {"year": "2012", "genre": "R&B"}


def test_a_field_the_old_revision_leaves_empty_is_not_emptied() -> None:
    assert lr.fields_to_restore({"label": ""}, {"label": "C"}) == {}


def test_the_catalog_link_is_never_put_back() -> None:
    assert lr.fields_to_restore({"catalog_id": "a"}, {"catalog_id": "b"}) == {}


def test_a_time_without_an_offset_is_refused(
    capsys: pytest.CaptureFixture,
) -> None:
    assert lr.main(["2026-11-09T18:30:00"]) == 2
    assert "offset" in capsys.readouterr().err
