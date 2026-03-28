import json

from deejay_cog.update_deejay_set_collection import (
    _create_collection_snapshot,
    _extract_date_and_title,
    _write_json_snapshot,
)

# -- _extract_date_and_title ---------------------------------------------------


def test_extract_date_and_title_parses_standard_filename():
    date, title = _extract_date_and_title("2024-03-15_MADjam_WCS.csv")
    assert date == "2024-03-15"
    assert title == "MADjam_WCS.csv"


def test_extract_date_and_title_strips_leading_separators():
    date, title = _extract_date_and_title("2024-03-15 - My Set")
    assert date == "2024-03-15"
    assert title == "My Set"


def test_extract_date_and_title_returns_empty_date_when_no_match():
    date, title = _extract_date_and_title("No Date Here")
    assert date == ""
    assert title == "No Date Here"


def test_extract_date_and_title_handles_date_only():
    date, title = _extract_date_and_title("2024-01-01")
    assert date == "2024-01-01"
    assert title == ""


# -- _create_collection_snapshot ----------------------------------------------


def test_create_collection_snapshot_returns_dict_with_key():
    result = _create_collection_snapshot("folders")
    assert result == {"folders": []}


def test_create_collection_snapshot_uses_given_key():
    result = _create_collection_snapshot("items")
    assert "items" in result
    assert result["items"] == []


# -- _write_json_snapshot ------------------------------------------------------


def test_write_json_snapshot_writes_valid_json(tmp_path):
    data = {"folders": [{"name": "2024", "items": []}]}
    out = tmp_path / "output.json"
    _write_json_snapshot(data, str(out))
    assert out.exists()
    loaded = json.loads(out.read_text(encoding="utf-8"))
    assert loaded == data


def test_write_json_snapshot_creates_parent_dirs(tmp_path):
    data = {"folders": []}
    out = tmp_path / "nested" / "deep" / "snapshot.json"
    _write_json_snapshot(data, str(out))
    assert out.exists()


def test_write_json_snapshot_writes_formatted_json(tmp_path):
    data = {"folders": [{"name": "2024"}]}
    out = tmp_path / "snap.json"
    _write_json_snapshot(data, str(out))
    content = out.read_text(encoding="utf-8")
    assert "\n" in content  # formatted, not single-line
