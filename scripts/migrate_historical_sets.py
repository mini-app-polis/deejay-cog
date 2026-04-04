# ruff: noqa: E402
#!/usr/bin/env python3

from __future__ import annotations

"""
Historical DJ set migration script.

Reads DJ_Sets_V3.zip (xlsx files), parses every set, and POSTs
each to POST /v1/ingest on api-kaianolevine-com.

Usage:
    uv run --with openpyxl migrate_historical_sets.py --zip DJ_Sets_V3.zip

Required env vars (or .env file):
    KAIANO_API_BASE_URL   e.g. https://api.kaianolevine.com
    KAIANO_API_OWNER_ID   your owner ID

Optional:
    DRY_RUN=1             parse and report without posting
"""

# Local migration script — not part of the deployed service.
# Run manually: uv run python scripts/migrate_historical_sets.py
# Not included in CI or pipeline execution.

"""
========================================================
HISTORICAL MIGRATION REPORT
========================================================
SUMMARY
  Total sets in source:            443
  Sets imported this run:          440
  Sets skipped (empty):              3
  Sets failed:                       0
  Total tracks sent this run:    13590

BY YEAR
  Year   | Expected | Imported | Skipped | Failed | Tracks
  -------+----------+----------+---------+--------+-------
  2016   |       64 |       64 |       0 |      0 |   1221
  2017   |       40 |       40 |       0 |      0 |    915
  2018   |       37 |       37 |       0 |      0 |    870
  2019   |       50 |       50 |       0 |      0 |   1363
  2020   |        8 |        8 |       0 |      0 |    212
  2021   |       28 |       28 |       0 |      0 |   1062
  2022   |       77 |       75 |       2 |      0 |   2705
  2023   |       49 |       48 |       1 |      0 |   1994
  2024   |       42 |       42 |       0 |      0 |   1519
  2025   |       35 |       35 |       0 |      0 |   1234
  2026   |       13 |       13 |       0 |      0 |    495

FAILURES
  None

========================================================
04/04/2026
"""

import argparse
import datetime
import io
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

try:
    import openpyxl
except ImportError:
    print(
        "ERROR: openpyxl is required. Run with: uv run --with openpyxl migrate_historical_sets.py ..."
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = os.getenv("KAIANO_API_BASE_URL", "https://api.kaianolevine.com").rstrip("/")
OWNER_ID = (
    os.getenv("KAIANO_API_OWNER_ID")
    or os.getenv("OWNER_ID", "kaiano_admin_01J9K2X7M4N8P3Q6R5T0V2W8Y1")
).strip()
DRY_RUN = os.getenv("DRY_RUN", "0").strip() in ("1", "true", "yes")

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


# ---------------------------------------------------------------------------
# Value parsers — handle both string and native xlsx types
# ---------------------------------------------------------------------------


def _parse_length_secs(value) -> int | None:
    """Accept MM:SS / HH:MM:SS strings, datetime.time, or timedelta objects.

    openpyxl returns duration cells as:
      - datetime.timedelta  (e.g. 2021 era) — already total seconds
      - datetime.time(MM, SS) (e.g. 2026 era) — hour=minutes, minute=seconds
      - str '00:2:52'       (e.g. 2020 era) — HH:MM:SS with loose padding
    """
    if value is None:
        return None
    # timedelta — already in seconds
    if isinstance(value, datetime.timedelta):
        return int(value.total_seconds())
    # datetime.time — openpyxl reads MM:SS duration cells as time(MM, SS, 0)
    # hour=minutes, minute=seconds (NOT a wall-clock time)
    if isinstance(value, datetime.time):
        return value.hour * 60 + value.minute
    s = str(value).strip()
    if not s:
        return None
    # HH:MM:SS or H:M:SS (e.g. '00:2:52')
    m = re.match(r"^(\d{1,2}):(\d{1,2}):(\d{2})$", s)
    if m:
        hh, mm, ss = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return hh * 3600 + mm * 60 + ss
    # MM:SS
    m = re.match(r"^(\d{1,3}):(\d{2})$", s)
    if m:
        mm, ss = int(m.group(1)), int(m.group(2))
        return None if ss >= 60 else mm * 60 + ss
    return None


def _parse_play_time(value) -> str | None:
    """Accept 'HH:MM AM/PM', 'HH:MM', 'HH:MM:SS' strings or datetime.time objects."""
    if value is None:
        return None
    if isinstance(value, datetime.time):
        return value.strftime("%H:%M:%S")
    s = str(value).strip()
    if not s:
        return None
    if re.match(r"^\d{1,2}:\d{2}(:\d{2})?$", s):
        return s
    m = re.match(r"^(\d{1,2}):(\d{2})\s*(AM|PM)$", s, re.IGNORECASE)
    if m:
        hh, mm, meridiem = int(m.group(1)), int(m.group(2)), m.group(3).upper()
        if meridiem == "PM" and hh != 12:
            hh += 12
        elif meridiem == "AM" and hh == 12:
            hh = 0
        return f"{hh:02d}:{mm:02d}"
    return None


def _parse_bpm(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value).strip()
    try:
        return float(raw) if raw else None
    except ValueError:
        return None


def _parse_year(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    raw = str(value).strip()
    try:
        return int(float(raw)) if raw else None
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# xlsx parsing
# ---------------------------------------------------------------------------

KNOWN_COLUMNS = {
    "label",
    "title",
    "remix",
    "artist",
    "comment",
    "genre",
    "length",
    "bpm",
    "year",
    "play time",
}

HEADER_ALIASES = {
    "play time": "play time",
    "playtime": "play time",
    "play_time": "play time",
    "length": "length",
    "duration": "length",
    "bpm": "bpm",
    "year": "year",
    "release year": "year",
}


def _canon(h: str) -> str:
    return str(h).strip().lower()


def parse_xlsx_bytes(data: bytes) -> list[dict]:
    """Parse raw xlsx bytes into a list of raw row dicts."""
    wb = openpyxl.load_workbook(io.BytesIO(data), data_only=True)
    ws = wb.active

    rows_iter = ws.iter_rows(values_only=True)
    header_row = next(rows_iter, None)
    if header_row is None:
        wb.close()
        return []

    col_index: dict[str, int] = {}
    for i, h in enumerate(header_row):
        if h is None:
            continue
        c = _canon(h)
        alias = HEADER_ALIASES.get(c, c)
        if alias in KNOWN_COLUMNS:
            col_index[alias] = i

    raw_tracks: list[dict] = []
    for play_order, row in enumerate(rows_iter, start=1):

        def get(col: str, _row=row):
            i = col_index.get(col)
            if i is None or i >= len(_row):
                return None
            return _row[i]

        raw_tracks.append(
            {
                "play_order": play_order,
                "label": get("label"),
                "title": get("title"),
                "remix": get("remix"),
                "artist": get("artist"),
                "comment": get("comment"),
                "genre": get("genre"),
                "length": get("length"),
                "bpm": get("bpm"),
                "year": get("year"),
                "play time": get("play time"),
            }
        )

    wb.close()
    return raw_tracks


def build_tracks(raw_rows: list[dict]) -> list[dict]:
    """Convert raw rows into ingest-ready track dicts, skipping rows without title+artist."""
    tracks = []
    for t in raw_rows:
        title = str(t.get("title") or "").strip()
        artist = str(t.get("artist") or "").strip()
        if not title or not artist:
            continue
        tracks.append(
            {
                "play_order": t.get("play_order", len(tracks) + 1),
                "label": (str(t.get("label") or "").strip() or None),
                "title": title,
                "remix": (str(t.get("remix") or "").strip() or None),
                "artist": artist,
                "comment": (str(t.get("comment") or "").strip() or None),
                "genre": (str(t.get("genre") or "").strip() or None),
                "length_secs": _parse_length_secs(t.get("length")),
                "bpm": _parse_bpm(t.get("bpm")),
                "release_year": _parse_year(t.get("year")),
                "play_time": _parse_play_time(t.get("play time")),
            }
        )
    return tracks


# ---------------------------------------------------------------------------
# Zip traversal
# ---------------------------------------------------------------------------

FILENAME_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\s*(.*?)\.xlsx$", re.IGNORECASE)


@dataclass
class SetEntry:
    zip_path: str
    set_date: str
    venue: str
    source_file: str
    year: int


def discover_sets(zf: zipfile.ZipFile) -> list[SetEntry]:
    entries = []
    for info in zf.infolist():
        if info.is_dir():
            continue
        if "__MACOSX" in info.filename or "/Summary/" in info.filename:
            continue
        name = Path(info.filename).name
        if "DJ Set Collection" in name:
            continue
        m = FILENAME_RE.match(name)
        if not m:
            continue
        date_str = m.group(1)
        venue_raw = m.group(2).strip().lstrip("-_ ")
        venue = venue_raw if venue_raw else "Unknown"
        try:
            year = int(date_str[:4])
        except ValueError:
            continue
        entries.append(
            SetEntry(
                zip_path=info.filename,
                set_date=date_str,
                venue=venue,
                source_file=Path(name).stem,
                year=year,
            )
        )
    entries.sort(key=lambda e: e.set_date)
    return entries


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------


def post_ingest(payload: dict, timeout: int = 60) -> dict:
    url = f"{BASE_URL}/v1/ingest"
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Owner-Id": OWNER_ID,
            "User-Agent": "deejay-cog-migration/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def post_with_retry(payload: dict) -> dict:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return post_ingest(payload)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {e.code}: {body}") from e
        except (urllib.error.URLError, TimeoutError, ConnectionResetError) as e:
            last_err = e
            if attempt < MAX_RETRIES:
                print(
                    f"      ↻ Connection error (attempt {attempt}/{MAX_RETRIES}): {e} — retrying in {RETRY_DELAY}s"
                )
                time.sleep(RETRY_DELAY)
    raise RuntimeError(f"Failed after {MAX_RETRIES} attempts: {last_err}") from last_err


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


@dataclass
class YearStats:
    expected: int = 0
    imported: int = 0
    skipped: int = 0
    failed: int = 0
    tracks: int = 0


@dataclass
class MigrationReport:
    year_stats: dict[int, YearStats] = field(default_factory=dict)
    failures: list[dict] = field(default_factory=list)
    total_tracks: int = 0

    def stat(self, year: int) -> YearStats:
        if year not in self.year_stats:
            self.year_stats[year] = YearStats()
        return self.year_stats[year]


def print_report(report: MigrationReport, total_in_source: int) -> None:
    total_imported = sum(s.imported for s in report.year_stats.values())
    total_skipped = sum(s.skipped for s in report.year_stats.values())
    total_failed = sum(s.failed for s in report.year_stats.values())

    print()
    print("=" * 56)
    print("HISTORICAL MIGRATION REPORT")
    print("=" * 56)
    print("SUMMARY")
    print(f"  Total sets in source:         {total_in_source:>6}")
    print(f"  Sets imported this run:       {total_imported:>6}")
    print(f"  Sets skipped (empty):         {total_skipped:>6}")
    print(f"  Sets failed:                  {total_failed:>6}")
    print(f"  Total tracks sent this run:   {report.total_tracks:>6}")
    print()
    print("BY YEAR")
    print(
        f"  {'Year':<6} | {'Expected':>8} | {'Imported':>8} | {'Skipped':>7} | {'Failed':>6} | {'Tracks':>6}"
    )
    print(f"  {'-' * 6}-+-{'-' * 8}-+-{'-' * 8}-+-{'-' * 7}-+-{'-' * 6}-+-{'-' * 6}")
    for year in sorted(report.year_stats):
        s = report.year_stats[year]
        print(
            f"  {year:<6} | {s.expected:>8} | {s.imported:>8} | {s.skipped:>7} | {s.failed:>6} | {s.tracks:>6}"
        )
    print()
    if report.failures:
        print("FAILURES")
        for f in report.failures:
            print(f"  ❌ {f['source_file']}")
            print(f"     {f['error']}")
    else:
        print("FAILURES")
        print("  None")
    print()
    print("=" * 56)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate historical DJ sets from zip to API."
    )
    parser.add_argument("--zip", required=True, help="Path to DJ_Sets_V3.zip")
    parser.add_argument(
        "--dry-run", action="store_true", help="Parse only, do not POST"
    )
    parser.add_argument("--year", type=int, help="Only migrate sets from this year")
    args = parser.parse_args()

    dry_run = DRY_RUN or args.dry_run

    if not dry_run:
        if not BASE_URL:
            print("ERROR: KAIANO_API_BASE_URL is not set.", file=sys.stderr)
            sys.exit(1)
        if not OWNER_ID:
            print(
                "ERROR: KAIANO_API_OWNER_ID (or OWNER_ID) is not set.", file=sys.stderr
            )
            sys.exit(1)

    zip_path = Path(args.zip)
    if not zip_path.exists():
        print(f"ERROR: Zip not found: {zip_path}", file=sys.stderr)
        sys.exit(1)

    print(f"📦 Opening {zip_path.name}...")
    if dry_run:
        print("🔍 DRY RUN — no data will be posted\n")
    else:
        print(f"🎯 Target: {BASE_URL}\n")

    with zipfile.ZipFile(zip_path) as zf:
        all_sets = discover_sets(zf)

        if args.year:
            all_sets = [s for s in all_sets if s.year == args.year]
            print(f"🔎 Filtered to year {args.year}: {len(all_sets)} sets\n")

        report = MigrationReport()
        for entry in all_sets:
            report.stat(entry.year).expected += 1

        print(f"Found {len(all_sets)} sets to process.\n")

        for i, entry in enumerate(all_sets, start=1):
            prefix = f"[{i:>3}/{len(all_sets)}]"

            try:
                raw_bytes = zf.read(entry.zip_path)
            except Exception as e:
                print(f"{prefix} ❌ Cannot read: {entry.source_file} — {e}")
                report.stat(entry.year).failed += 1
                report.failures.append(
                    {"source_file": entry.source_file, "error": str(e)}
                )
                continue

            raw_rows = parse_xlsx_bytes(raw_bytes)
            tracks = build_tracks(raw_rows)

            if not tracks:
                print(f"{prefix} ⏭️  Empty — skipping: {entry.source_file}")
                report.stat(entry.year).skipped += 1
                continue

            payload = {
                "set_date": entry.set_date,
                "venue": entry.venue,
                "source_file": entry.source_file,
                "owner_id": OWNER_ID,
                "tracks": tracks,
            }

            if dry_run:
                print(f"{prefix} 🔍 {entry.source_file} ({len(tracks)} tracks)")
                report.stat(entry.year).imported += 1
                report.stat(entry.year).tracks += len(tracks)
                report.total_tracks += len(tracks)
                continue

            print(
                f"{prefix} 🚀 {entry.source_file} ({len(tracks)} tracks)...",
                end=" ",
                flush=True,
            )
            try:
                post_with_retry(payload)
                print("✅")
                report.stat(entry.year).imported += 1
                report.stat(entry.year).tracks += len(tracks)
                report.total_tracks += len(tracks)
            except Exception as e:
                print(f"❌ {e}")
                report.stat(entry.year).failed += 1
                report.failures.append(
                    {"source_file": entry.source_file, "error": str(e)}
                )

    print_report(report, total_in_source=len(all_sets))

    if any(s.failed > 0 for s in report.year_stats.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
