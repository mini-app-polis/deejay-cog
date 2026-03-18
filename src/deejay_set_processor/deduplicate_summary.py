import argparse
import sys
from typing import Any

from kaiano import logger as logger_mod
from kaiano.google import GoogleAPI

log = logger_mod.get_logger()


def deduplicate_summary(spreadsheet_id: str, g: GoogleAPI | None = None) -> None:
    log.info(f"🚀 Starting deduplicate_summary for spreadsheet: {spreadsheet_id}")
    if g is None:
        g = GoogleAPI.from_env()
    fmt = g.sheets.formatter
    spreadsheet = g.sheets.get_metadata(
        spreadsheet_id, fields="sheets(properties(sheetId,title))"
    )
    sheets = spreadsheet.get("sheets", [])

    for sheet in sheets:
        sheet_props = sheet["properties"]
        sheet_id = sheet_props["sheetId"]
        sheet_name = sheet_props["title"]
        log.debug(f"Processing sheet '{sheet_name}' (ID: {sheet_id})")

        data = g.sheets.read_values(spreadsheet_id, f"{sheet_name}!A:Z")
        if not data or len(data) < 2:
            log.warning(f"⚠️ Skipping empty or header-only sheet: {sheet_name}")
            continue

        header = [_strip_cell_value(h) for h in data[0]]
        rows = data[1:]

        # Ensure 'Count' column exists (case-insensitive)
        count_index = _find_column_index_ci(header, "Count")
        if count_index is None:
            header.append("Count")
            rows = [row + ["1"] for row in rows]
            count_index = len(header) - 1

        title_index = _find_column_index_ci(header, "Title")

        # Normalize row lengths
        for i, row in enumerate(rows):
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            elif len(row) > len(header):
                row = row[: len(header)]

            # Always strip leading/trailing whitespace on ALL cells to avoid whitespace-only duplicates.
            row = [_strip_cell_value(v) for v in row]
            rows[i] = row

        # Build a non-adjacent dedup map keyed by row values (excluding Count).
        length_index = _find_column_index_ci(header, "Length")
        # Placeholder for future BPM normalization if desired (not applied for now).
        bpm_index = _find_column_index_ci(header, "BPM")

        comment_index = _find_column_index_ci(header, "Comment")
        genre_index = _find_column_index_ci(header, "Genre")
        year_index = _find_column_index_ci(header, "Year")

        optional_indices = [
            i
            for i in [comment_index, genre_index, year_index, length_index, bpm_index]
            if i is not None
        ]

        # Group rows by an identity key that excludes Count and the optional match columns.
        # Within each identity key, merge rows when optional columns are compatible:
        # - empty vs non-empty is allowed (and we fill template with the non-empty value)
        # - non-empty vs different non-empty is NOT allowed
        identity_to_entries: dict[tuple[str, ...], list[dict[str, Any]]] = {}

        def _norm_optional(
            col_i: int,
            cell: Any,
            *,
            _len_idx: int | None = length_index,
            _bpm_idx: int | None = bpm_index,
        ) -> str:
            s = _normalize_key_cell(cell)
            if not s:
                return ""
            if _len_idx is not None and col_i == _len_idx:
                return _normalize_length(s)
            if _bpm_idx is not None and col_i == _bpm_idx:
                return _normalize_bpm(s)
            return s

        for row in rows:
            try:
                row_count = int(_strip_cell_value(row[count_index]))
            except Exception:
                row_count = 0

            identity_parts: list[str] = []
            for col_i, cell in enumerate(row):
                if col_i == count_index:
                    continue
                if col_i in optional_indices:
                    continue
                norm = _normalize_key_cell(cell)
                if title_index is not None and col_i == title_index:
                    # For title key comparison only: remove all non-alphanumeric characters, including whitespace
                    norm = "".join(ch for ch in norm if ch.isalnum())
                identity_parts.append(norm.lower())

            identity_key = tuple(identity_parts)

            # Compute normalized optional values for compatibility checks
            opt_norm: dict[int, str] = {
                i: _norm_optional(i, row[i]) for i in optional_indices
            }

            entries = identity_to_entries.setdefault(identity_key, [])

            matched_entry = None
            for entry in entries:
                entry_opt: dict[int, str] = entry["opt_norm"]
                compatible = True
                for i in optional_indices:
                    a = entry_opt.get(i, "")
                    b = opt_norm.get(i, "")
                    if a and b and a != b:
                        compatible = False
                        break
                if compatible:
                    matched_entry = entry
                    break

            if matched_entry is None:
                # New distinct entry under this identity
                template_row = row.copy()
                # Ensure Count cell is string
                template_row[count_index] = str(row_count)
                entries.append(
                    {
                        "row": template_row,
                        "count": row_count,
                        "opt_norm": opt_norm,
                    }
                )
            else:
                # Merge into the matched entry
                matched_entry["count"] += row_count
                matched_entry["row"][count_index] = str(matched_entry["count"])

                # Fill missing optional values from incoming row (preserve original text)
                for i in optional_indices:
                    existing_norm = matched_entry["opt_norm"].get(i, "")
                    incoming_norm = opt_norm.get(i, "")
                    if not existing_norm and incoming_norm:
                        matched_entry["opt_norm"][i] = incoming_norm
                        if i < len(matched_entry["row"]) and i < len(row):
                            matched_entry["row"][i] = row[i]

        deduped_rows: list[list[str]] = []
        total_count_sum = 0
        for _, entries in identity_to_entries.items():
            for entry in entries:
                deduped_rows.append(entry["row"])
                total_count_sum += entry["count"]

        log.debug(
            f"Sheet '{sheet_name}': original rows={len(rows)}, deduplicated rows={len(deduped_rows)}, total count={total_count_sum}"
        )

        final_data = [header] + deduped_rows
        g.sheets.clear(spreadsheet_id, f"{sheet_name}!A:Z")
        g.sheets.write_values(
            spreadsheet_id, f"{sheet_name}!A1", final_data, value_input_option="RAW"
        )

    log.info(f"✅ Applying formatting for spreadsheet: {spreadsheet_id}")
    try:
        fmt.apply_formatting_to_sheet(spreadsheet_id)
    except Exception as e:
        log.warning(f"⚠️ Formatting failed (continuing without formatting): {e}")
    log.info(f"✅ Finished deduplicate_summary for spreadsheet: {spreadsheet_id}")


def _find_column_index_ci(header: list[str], target: str) -> int | None:
    for i, h in enumerate(header):
        if _normalize_key_cell(h).lower() == _normalize_key_cell(target).lower():
            return i
    return None


def _normalize_key_cell(value: Any) -> str:
    """Normalize cell text for deduplication key comparisons.

    Accented characters are folded to their base letters (e.g., 'Beyoncé' == 'Beyonce').

    This is intentionally *more aggressive* than what we write back to the sheet.
    It strips invisible unicode format characters (e.g. zero-width space, BOM),
    normalizes unicode width/compat forms, and collapses whitespace.

    NOTE: We only apply this to the *key*, not to the stored template row.
    """
    s = "" if value is None else str(value)

    # Normalize non-breaking spaces and common whitespace to plain spaces
    s = s.replace("\u00a0", " ")  # NBSP
    s = s.replace("\t", " ").replace("\r", " ").replace("\n", " ")

    try:
        import unicodedata

        # First decompose characters so accents become combining marks
        # e.g. "é" -> "e" + "́"
        s = unicodedata.normalize("NFKD", s)

        # Remove combining marks (accents/diacritics)
        s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")

        # Re-compose to a stable form
        s = unicodedata.normalize("NFKC", s)

        # Remove invisible/format characters (category Cf), e.g. \u200b, \ufeff
        s = "".join(ch for ch in s if unicodedata.category(ch) != "Cf")
    except Exception:
        pass

    # Collapse runs of whitespace and trim
    s = " ".join(s.split())
    return s


def _strip_cell_value(value: Any) -> str:
    """Strip leading/trailing whitespace from a cell value.

    This mutates what we write back to Sheets (unlike `_normalize_key_cell`, which is key-only).
    We keep internal whitespace intact; we only remove leading/trailing whitespace and convert
    NBSP to a normal space first.
    """
    if value is None:
        return ""
    s = str(value)
    s = s.replace("\u00a0", " ")  # NBSP → space
    return s.strip()


def _normalize_length(value: str) -> str:
    """Normalize length values so equivalent time formats match (MM:SS and H:MM:SS).

    Supports both MM:SS and H:MM:SS time formats and ignores leading zero hours for key comparisons.
    Examples: '00:2:54' == '0:02:54' == '2:54'.
    """
    if value is None:
        return ""

    s = _normalize_key_cell(value)
    if not s:
        return ""

    parts = [p.strip() for p in s.split(":") if p.strip() != ""]

    # Accept common formats:
    # - MM:SS
    # - H:MM:SS (or 0:MM:SS)
    # We normalize to:
    # - M:SS when hours == 0
    # - H:MM:SS when hours > 0
    if len(parts) == 2:
        mm_raw, ss_raw = parts
        try:
            h = 0
            m = int(mm_raw) if mm_raw else 0
            sec = int(ss_raw) if ss_raw else 0
        except Exception:
            return s
    elif len(parts) == 3:
        hh_raw, mm_raw, ss_raw = parts
        try:
            h = int(hh_raw) if hh_raw else 0
            m = int(mm_raw) if mm_raw else 0
            sec = int(ss_raw) if ss_raw else 0
        except Exception:
            return s
    else:
        # Not a recognized time format; leave as-is.
        return s

    # Basic sanity checks
    if h < 0 or m < 0 or sec < 0 or sec >= 60 or m >= 60:
        return s

    if h == 0:
        return f"{m}:{sec:02d}"

    return f"{h}:{m:02d}:{sec:02d}"


def _normalize_bpm(value: Any) -> str:
    """Normalize BPM values for deduplication key comparisons.

    Treats numeric equivalents as equal (e.g., '100' == '100.0').
    Leaves non-numeric BPM text unchanged.

    NOTE: This is key-only; we do not mutate what gets written back.
    """
    s = _normalize_key_cell(value)
    if not s:
        return ""

    # Common case: int-like or float-like strings
    try:
        f = float(s)
    except Exception:
        return s

    # If it's effectively an integer, drop the .0
    if abs(f - round(f)) < 1e-9:
        return str(int(round(f)))

    # Otherwise keep a stable string form (trim trailing zeros)
    # e.g. 100.50 -> '100.5'
    out = f"{f:f}".rstrip("0").rstrip(".")
    return out


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Deduplicate one or more Google Sheets spreadsheets in-place by combining duplicate rows "
            "(summing the Count column) across all tabs."
        )
    )
    parser.add_argument(
        "spreadsheet_ids",
        nargs="+",
        help=(
            "One or more Google Sheets spreadsheet IDs to deduplicate (in-place). "
            "Example: 174AK9BTKpRhf4_uUSR5GtWarTSxEiVUdhvrn6Jk-OMA"
        ),
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args(sys.argv[1:])
    exit_code = 0
    for ss_id in args.spreadsheet_ids:
        try:
            deduplicate_summary(ss_id)
        except Exception as e:
            log.error(f"❌ Dedup failed for spreadsheet {ss_id}: {e}")
            exit_code = 1
    raise SystemExit(exit_code)
