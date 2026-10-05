"""The repair pass: finish the follow-up work earlier runs left undone.

One CSV fans out into several jobs — a sheet, an API set, a Spotify
playlist — and the CSV's move to ``Archive`` is a single bit standing for
all of them. A set whose ingest or playlist failed after that move was never
looked at again.

This pass asks the systems themselves rather than keeping a record. For
every set sheet dated within :data:`~deejay_cog.config.REPAIR_LOOKBACK_DAYS`:

- **ingested** means ``GET /v1/sets`` lists a set whose ``source_file`` is
  the sheet's name;
- **published** means the Spotify account has a playlist of that name.

Whatever is missing is redone from the sheet, newest set first, at most
:data:`~deejay_cog.config.REPAIR_MAX_PER_RUN` jobs per run so a backlog
cannot run a sweep into its deadline. A recreated playlist feeds the radio
playlist exactly as a new one does; a radio update that fails after the
playlist exists is reported and, by design, not retried.

Nothing is stored, so there is nothing to backfill or to drift: the first
run finds everything ever missed inside the window, and a playlist deleted
by hand is rebuilt.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Collection
from dataclasses import dataclass, field
from typing import Any
from zoneinfo import ZoneInfo

from mini_app_polis import logger as logger_mod
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI

import deejay_cog.config as config

from .api_client import api_client
from .ingest_to_api import build_ingest_payload, read_tracks_from_sheet
from .spotify_sync import fetch_all_playlists, sync_set_to_spotify

log = logger_mod.get_logger()

SHEET_MIME = "application/vnd.google-apps.spreadsheet"
FOLDER_MIME = "application/vnd.google-apps.folder"

#: Largest page GET /v1/sets serves.
_SETS_PAGE = 200


@dataclass(frozen=True)
class SetSheet:
    """A set's Google Sheet, with what its name says about the set."""

    sheet_id: str
    #: The name the set goes by everywhere: the sheet's name without
    #: ".csv". It is the API's ``source_file`` and the playlist's name.
    name: str
    set_date: dt.date
    venue: str


@dataclass
class RepairResult:
    """What a repair pass did, and what it still owes."""

    ingested: list[str] = field(default_factory=list)
    published: list[str] = field(default_factory=list)
    #: ``(set name, what failed)``, one per failed job.
    failed: list[tuple[str, str]] = field(default_factory=list)
    #: Jobs found missing but left for a later run by the per-run cap.
    pending: int = 0


def set_name(sheet_name: str) -> str:
    """A sheet's set name: its name without a trailing ".csv".

    Not ``os.path.splitext``, which would cut "2026-03-01 St. Paul Swing"
    at the dot in the venue.
    """
    name = sheet_name.strip()
    return name[:-4] if name.lower().endswith(".csv") else name


def _parse_set_name(name: str) -> tuple[dt.date, str] | None:
    """``(date, venue)`` from "YYYY-MM-DD Venue", or None."""
    date_part, _, venue = name.partition(" ")
    if not venue.strip():
        return None
    try:
        return dt.date.fromisoformat(date_part), venue.strip()
    except ValueError:
        return None


def today() -> dt.date:
    """Today where the sets are played, not in UTC."""
    return dt.datetime.now(ZoneInfo(config.TIMEZONE)).date()


def recent_set_sheets(g: GoogleAPI, since: dt.date) -> list[SetSheet]:
    """Set sheets in the year folders dated ``since`` or later, newest first."""
    years = [
        f
        for f in g.drive.list_files(config.DJ_SETS_FOLDER_ID, include_folders=True)
        if f.mime_type == FOLDER_MIME
        and (f.name or "").isdigit()
        and int(f.name) >= since.year
    ]
    sheets: list[SetSheet] = []
    for year in years:
        for f in g.drive.list_files(year.id, include_folders=False):
            if f.mime_type != SHEET_MIME:
                continue
            name = set_name(f.name or "")
            parsed = _parse_set_name(name)
            if parsed is None or parsed[0] < since:
                continue
            sheets.append(SetSheet(f.id, name, parsed[0], parsed[1]))
    sheets.sort(key=lambda s: (s.set_date, s.name), reverse=True)
    return sheets


def _ingested_names(since: dt.date) -> set[str]:
    """``source_file`` of every set the API holds dated ``since`` or later."""
    client = api_client()
    names: set[str] = set()
    offset = 0
    while True:
        page = client.list_sets(date_from=since, limit=_SETS_PAGE, offset=offset)
        names.update(item.source_file for item in page if item.source_file)
        if len(page) < _SETS_PAGE:
            return names
        offset += _SETS_PAGE


def _ingest(g: GoogleAPI, sheet: SetSheet) -> None:
    """Send one set to the API from its sheet. Raises on any failure."""
    tracks = read_tracks_from_sheet(g, sheet.sheet_id)
    payload = build_ingest_payload(
        set_date=sheet.set_date.isoformat(),
        venue=sheet.venue,
        source_file=sheet.name,
        tracks=tracks,
    )
    if not payload.tracks:
        raise ValueError("the sheet has no tracks to send")
    api_client().ingest(payload)


def repair_recent_sets(
    g: GoogleAPI,
    *,
    sp: Any | None,
    skip: Collection[str] = (),
    on: dt.date | None = None,
) -> RepairResult:
    """Redo the missing ingests and playlists of recent sets.

    ``sp`` is the Spotify client, or None to leave playlists alone (no
    credentials, or a client that would not build). ``skip`` names sets
    this run already handled. Never raises for a failed check or job — the
    run's own work is done, and a repair that fails is reported and tried
    again next run.
    """
    result = RepairResult()
    since = (on or today()) - dt.timedelta(days=config.REPAIR_LOOKBACK_DAYS)

    try:
        sheets = [s for s in recent_set_sheets(g, since) if s.name not in skip]
    except Exception as exc:
        log.error("repair: could not list the set sheets: %s", exc)
        result.failed.append(("set sheets", f"listing: {exc}"))
        return result
    if not sheets:
        return result

    owed: list[tuple[SetSheet, str]] = []
    if api_base_url():
        try:
            have = _ingested_names(since)
            owed += [(s, "ingest") for s in sheets if s.name not in have]
        except Exception as exc:
            log.error("repair: could not list the API's sets: %s", exc)
            result.failed.append(("api sets", f"listing: {exc}"))
    if sp is not None:
        try:
            playlists = {p.get("name") for p in fetch_all_playlists(sp)}
            owed += [(s, "playlist") for s in sheets if s.name not in playlists]
        except Exception as exc:
            log.error("repair: could not list the Spotify playlists: %s", exc)
            result.failed.append(("spotify playlists", f"listing: {exc}"))

    # Newest set first; for one set, its ingest before its playlist.
    owed.sort(key=lambda job: (job[0].set_date, job[0].name, job[1] == "ingest"))
    owed.reverse()
    cap = max(config.REPAIR_MAX_PER_RUN, 0)
    result.pending = max(len(owed) - cap, 0)

    for sheet, job in owed[:cap]:
        log.info("repair: %s for %s", job, sheet.name)
        if job == "ingest":
            try:
                _ingest(g, sheet)
                result.ingested.append(sheet.name)
            except Exception as exc:
                log.error("repair: ingest failed for %s: %s", sheet.name, exc)
                result.failed.append((sheet.name, f"ingest: {exc}"))
            continue

        assert sp is not None  # playlist jobs are only owed with a client
        try:
            tracks = read_tracks_from_sheet(g, sheet.sheet_id)
            outcome = sync_set_to_spotify(sp, sheet.name, tracks)
        except Exception as exc:
            log.error("repair: playlist failed for %s: %s", sheet.name, exc)
            result.failed.append((sheet.name, f"playlist: {exc}"))
            continue
        if outcome.created:
            result.published.append(sheet.name)
        if not outcome.ok:
            result.failed.append((sheet.name, f"playlist: {outcome.detail}"))

    return result
