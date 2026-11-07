"""The ``library-reconcile`` step of the nightly library run: compare each
linked library file's owned fields with what its catalog entry says they
should be, and group every difference by kind (ADR-0008, step 3 and
decision 15).

**Reads only.** Nothing is written to any music file, and nothing in the
catalog or the registry changes: the run writes only its cursor and its
record in ``MUSIC_TAGGING_STATE_FOLDER_ID``. The write run that applies the
owner's approved kinds, with the catalog winning, is a separate step.

**Which files.** Registered files under ``LIBRARY_ROOT_FOLDER_ID`` that are
present and identified (linked to a catalog entry), in the registry's own
order (``GET /v1/catalog/library/files``, oldest first). Each run takes the
next ``LIBRARY_RECONCILE_CHUNK_SIZE`` files (default 50) after its own
cursor, ``library-reconcile-cursor.json``, kept apart from the backfill's.
A dry run moves this cursor: repeated dry runs step through the whole
library, and after the last file the next run starts again from the first.
The cursor counts files in that order, so a file marked not present between
runs shifts the rest by one: a dry run may then pass over or repeat a file,
which the next pass catches.
Past the run's deadline the run stops between files and saves where it got
to.

**What is compared.** The owned fields (title with its version, artist,
year, genre, BPM, label) as the file's ID3 tags hold them, against the
entry as the display rules write it out (``POST /v1/catalog/display``). The
file's expected title is the entry's title followed by the file's own edit
label in parentheses, as the registry records it ("Song (No Intro Edit)").
The comment frame and every frame VirtualDJ owns are never read for the
comparison, and never written.

**The twelve kinds** a difference is filed under:

- ``case_punctuation`` — the same text but for case, spacing or punctuation
- ``featured_moved`` — a featured artist carried in the title rather than
  the artist (or the other way round)
- ``artist_joiner`` — the same artists in the same order, joined
  differently ("A & B" against "A, B")
- ``version_brackets`` — the same title and version, written with other
  brackets or a dash ("Song - Lash Remix" against "Song (Lash Remix)")
- ``genre_remapped`` — the file's genre is not the catalog's (the catalog's
  genres are the mapped vocabulary, so the file's is an older value or one
  placed differently)
- ``genre_filled`` — the file has no genre and the catalog has one
- ``year_differs``, ``bpm_rounding`` (0.5 BPM or less apart),
  ``bpm_differs``, ``label_differs``
- ``fill_up`` — the file has a value for a field the catalog leaves empty,
  which would go up to the catalog
- ``other`` — anything else, listed file by file

**Record:** ``library-reconcile-<run id>.dry-run.json`` in the state folder,
with each kind's count and every difference under it (file, field, the
file's value, the catalog's). The run report gives each kind's count and a
few examples, and names every ``other`` file.

**Safety:** ``LIBRARY_RECONCILE_MODE`` is ``off`` unless ``dry-run``. A
``write`` setting runs as a dry run and says so: writing is not built here.
"""

from __future__ import annotations

import os
import re
import unicodedata
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import monotonic
from typing import Any, Protocol, cast

from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import (
    CATALOG_DISPLAY_MAX_IDS,
    CatalogDisplay,
    CatalogDisplayRequest,
    LibraryFile,
)
from mini_app_polis.deadline import without_run_deadline
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI
from mini_app_polis.pipeline_status import run_report

from deejay_cog._pipeline_eval import REPO
from deejay_cog._shared_deadline import shared_deadline_passed
from deejay_cog._version import __version__
from deejay_cog.api_client import api_client
from deejay_cog.library_backfill import (
    BackfillSettings,
    StateFile,
    StateProblem,
)
from deejay_cog.music_safety import (
    FlowMode,
    file_meta,
    flow_mode,
    local_run_id,
)
from deejay_cog.music_tags import linked_catalog_id, read_tag_row

log = logger_mod.get_logger()

FLOW_NAME = "library-reconcile"

#: off | dry-run; ``write`` runs as a dry run. Unset or unknown is off.
MODE_VAR = "LIBRARY_RECONCILE_MODE"

#: Files per run; a whole number of at least 1.
CHUNK_SIZE_VAR = "LIBRARY_RECONCILE_CHUNK_SIZE"
DEFAULT_CHUNK_SIZE = 50

CURSOR_NAME = f"{FLOW_NAME}-cursor.json"

#: The kinds a difference is filed under, in the order they are reported.
KINDS = (
    "case_punctuation",
    "featured_moved",
    "artist_joiner",
    "version_brackets",
    "genre_remapped",
    "genre_filled",
    "year_differs",
    "bpm_rounding",
    "bpm_differs",
    "label_differs",
    "fill_up",
    "other",
)

#: The owned fields, as ``read_tag_row`` names them.
FIELDS = ("title", "artist", "year", "genre", "bpm", "label")

#: How many examples of each kind the run report shows (the record has all).
REPORT_EXAMPLES = 5

#: BPM this close is a rounding difference, not a different tempo.
BPM_ROUNDING = 0.5


class Catalog(Protocol):
    """The part of the API client the reconcile calls."""

    def list_library_files(self, **kwargs: Any) -> list[LibraryFile]:
        """One page of registered files."""
        ...

    def read_catalog_display(
        self, payload: CatalogDisplayRequest
    ) -> list[CatalogDisplay | None]:
        """Each entry's owned fields as a file should carry them."""
        ...


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def chunk_size_setting() -> int | str:
    """The chunk size as the worker logs it: the number, or the raw text
    when it will not parse."""
    raw = os.getenv(CHUNK_SIZE_VAR, "").strip()
    if not raw:
        return DEFAULT_CHUNK_SIZE
    try:
        return int(raw)
    except ValueError:
        return raw


# ---------------------------------------------------------------------------
# Classifying one difference
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Difference:
    """One owned field where a file and its catalog entry differ."""

    path: str
    drive_file_id: str
    field: str
    file_value: str
    catalog_value: str
    kind: str

    def as_dict(self) -> dict[str, str]:
        return {
            "path": self.path,
            "drive_file_id": self.drive_file_id,
            "field": self.field,
            "file": self.file_value,
            "catalog": self.catalog_value,
        }

    def example(self) -> str:
        return (
            f"{self.path}: {self.field} {self.file_value!r} -> {self.catalog_value!r}"
        )


def _fold(text: str) -> str:
    """Text with case, accents' compatibility forms, spacing and every
    symbol left out: what is left is the letters and digits."""
    text = unicodedata.normalize("NFKC", text).casefold()
    return "".join(ch for ch in text if ch.isalnum())


_FEATURING = r"(?:feat\.?|ft\.?|featuring)"
#: "(feat. X)", "[ft. X]" or a trailing "feat. X" in a title.
_TITLE_FEATURE = re.compile(
    rf"\s*(?:[\[(]\s*{_FEATURING}\s+([^\])]+)[\])]|\s+{_FEATURING}\s+(.+)$)",
    re.IGNORECASE,
)
#: What joins artists: commas, ampersands, "and", "x", "with", "vs", "feat.".
_JOINERS = re.compile(
    rf"\s*(?:,|&|\+|/|\band\b|\bx\b|\bwith\b|\bvs\.?|{_FEATURING})\s*",
    re.IGNORECASE,
)


def _artists(text: str) -> list[str]:
    """The artists in an artist (or featuring) text, in order, folded."""
    return [_fold(part) for part in _JOINERS.split(text) if _fold(part)]


def _without_feature(title: str) -> tuple[str, list[str]]:
    """A title with its featuring part taken out, and the artists it named."""
    found: list[str] = []

    def take(match: re.Match[str]) -> str:
        found.extend(_artists(match.group(1) or match.group(2) or ""))
        return ""

    return _TITLE_FEATURE.sub(take, title).strip(), found


_BRACKETS = re.compile(r"[\[\]()]|\s-\s")


def _bracket_shape(title: str) -> list[str]:
    """The brackets and " - " separators in a title, in order."""
    return [part.strip() for part in _BRACKETS.findall(title)]


def _number(text: str) -> float | None:
    try:
        return float(text)
    except ValueError:
        return None


def _shown(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:g}"
    return str(value).strip()


def expected(display: CatalogDisplay, row: LibraryFile) -> dict[str, str]:
    """The owned fields as this file should carry them."""
    title = display.title
    if row.edit_label:
        title = f"{title} ({row.edit_label})"
    return {
        "title": title,
        "artist": display.artist,
        "year": _shown(display.release_year),
        "genre": _shown(display.genre),
        "bpm": _shown(display.bpm),
        "label": _shown(display.label),
    }


def classify(
    path: str, drive_file_id: str, tags: dict[str, str], wanted: dict[str, str]
) -> list[Difference]:
    """Every owned field where ``tags`` (the file) and ``wanted`` (the
    catalog) differ, each filed under one kind."""
    diffs: list[Difference] = []

    def add(name: str, kind: str) -> None:
        diffs.append(
            Difference(path, drive_file_id, name, tags[name], wanted[name], kind)
        )

    title_done = artist_done = False
    # A featured artist on the wrong side: the title's featuring part, put in
    # the artist, makes both match (or the catalog's title carries it).
    if tags["title"] != wanted["title"] or tags["artist"] != wanted["artist"]:
        file_title, file_featured = _without_feature(tags["title"])
        want_title, want_featured = _without_feature(wanted["title"])
        file_names = _artists(tags["artist"]) + file_featured
        want_names = _artists(wanted["artist"]) + want_featured
        if (
            (file_featured or want_featured)
            and file_featured != want_featured
            and _fold(file_title) == _fold(want_title)
            and file_names == want_names
        ):
            if tags["title"] != wanted["title"]:
                add("title", "featured_moved")
            if tags["artist"] != wanted["artist"]:
                add("artist", "featured_moved")
            title_done = artist_done = True

    if not title_done and tags["title"] != wanted["title"]:
        if _fold(tags["title"]) != _fold(wanted["title"]):
            add("title", "other")
        elif _bracket_shape(tags["title"]) != _bracket_shape(wanted["title"]):
            add("title", "version_brackets")
        else:
            add("title", "case_punctuation")

    if not artist_done and tags["artist"] != wanted["artist"]:
        if _artists(tags["artist"]) == _artists(wanted["artist"]):
            same_joins = _JOINERS.findall(tags["artist"].lower()) == _JOINERS.findall(
                wanted["artist"].lower()
            )
            add("artist", "case_punctuation" if same_joins else "artist_joiner")
        elif _fold(tags["artist"]) == _fold(wanted["artist"]):
            add("artist", "case_punctuation")
        else:
            add("artist", "other")

    for name in ("year", "genre", "bpm", "label"):
        have, want = tags[name], wanted[name]
        if not have and not want:
            continue
        if not want:
            add(name, "fill_up")
            continue
        if name == "bpm":
            have_n, want_n = _number(have), _number(want)
            if have_n is not None and want_n is not None:
                if have_n == want_n:
                    continue
                gap = abs(have_n - want_n)
                add(name, "bpm_rounding" if gap <= BPM_ROUNDING else "bpm_differs")
            else:
                add(name, "bpm_differs")
            continue
        if have == want:
            continue
        if name == "genre":
            if not have:
                add(name, "genre_filled")
            elif _fold(have) == _fold(want):
                add(name, "case_punctuation")
            else:
                add(name, "genre_remapped")
        elif name == "year":
            add(name, "year_differs")
        elif _fold(have) == _fold(want):
            add(name, "case_punctuation")
        else:
            add(name, "label_differs")
    return diffs


# ---------------------------------------------------------------------------
# One run
# ---------------------------------------------------------------------------


@dataclass
class ReconcileSummary:
    """What one run compared and found."""

    mode: FlowMode
    requested_write: bool = False
    problems: list[str] = field(default_factory=list)
    record_name: str = ""
    start: int = 0
    position: int = 0
    total: int = 0
    wrapped: bool = False
    compared: int = 0
    matching: int = 0
    stopped: str = ""
    differences: dict[str, list[Difference]] = field(
        default_factory=lambda: {kind: [] for kind in KINDS}
    )
    failed: list[str] = field(default_factory=list)

    @property
    def files_with_differences(self) -> int:
        return len(
            {d.drive_file_id for diffs in self.differences.values() for d in diffs}
        )


def _settings() -> tuple[BackfillSettings | None, int, list[str]]:
    settings, problems = BackfillSettings.from_env()
    size = chunk_size_setting()
    if not isinstance(size, int) or size < 1:
        problems.append(
            f"{CHUNK_SIZE_VAR}={size!r} is not a whole number of at least 1"
        )
        size = 0
    return settings, size, problems


def _page(api: Catalog, root: str, offset: int, limit: int) -> list[LibraryFile]:
    return api.list_library_files(
        library_root_id=root,
        identification_status="identified",
        present=True,
        offset=offset,
        limit=limit,
    )


def _displays(api: Catalog, rows: list[LibraryFile]) -> dict[uuid.UUID, CatalogDisplay]:
    ids = list(dict.fromkeys(r.catalog_id for r in rows if r.catalog_id))
    found: dict[uuid.UUID, CatalogDisplay] = {}
    for start in range(0, len(ids), CATALOG_DISPLAY_MAX_IDS):
        batch = ids[start : start + CATALOG_DISPLAY_MAX_IDS]
        answers = api.read_catalog_display(CatalogDisplayRequest(ids=batch))
        for display in answers:
            if display is not None:
                found[display.id] = display
    return found


def _compare_one(
    drive: Any,
    row: LibraryFile,
    display: CatalogDisplay | None,
    summary: ReconcileSummary,
) -> None:
    if display is None:
        summary.failed.append(
            f"{row.path} (its catalog entry {row.catalog_id} is gone)"
        )
        return
    meta = file_meta(drive, row.drive_file_id)
    if meta.get("trashed"):
        summary.failed.append(f"{row.path} (in the trash; the scan will mark it)")
        return
    data: bytes = drive.download_file_bytes(row.drive_file_id).data
    tags = read_tag_row(data)
    diffs = classify(row.path, row.drive_file_id, tags, expected(display, row))
    link = linked_catalog_id(data)
    if link and link != str(row.catalog_id):
        diffs.append(
            Difference(
                row.path,
                row.drive_file_id,
                "catalog_id",
                link,
                str(row.catalog_id),
                "other",
            )
        )
    summary.compared += 1
    if not diffs:
        summary.matching += 1
    for diff in diffs:
        summary.differences[diff.kind].append(diff)


def _reconcile(
    drive: Any,
    api: Catalog,
    settings: BackfillSettings,
    chunk_size: int,
    run_id: str,
    summary: ReconcileSummary,
) -> None:
    cursor_store = StateFile(drive, settings.state, CURSOR_NAME)
    try:
        cursor = cursor_store.load() or {}
    except StateProblem as exc:
        summary.problems.append(str(exc))
        return
    if cursor.get("root_folder_id") not in (None, settings.root):
        cursor = {}
    position = int(cursor.get("position", 0))
    rows = _page(api, settings.root, position, chunk_size)
    if not rows and position:
        # Past the last file: the next pass starts from the first.
        summary.wrapped = True
        position = 0
        rows = _page(api, settings.root, 0, chunk_size)
    summary.start = summary.position = position
    displays = _displays(api, rows) if rows else {}

    for row in rows:
        if shared_deadline_passed():
            summary.stopped = f"run deadline reached before {row.path}"
            break
        try:
            display = displays.get(row.catalog_id) if row.catalog_id else None
            _compare_one(drive, row, display, summary)
        except Exception as exc:  # noqa: BLE001 — one file must not end the run
            if shared_deadline_passed():
                summary.stopped = f"run deadline reached reading {row.path}"
                break
            log.exception("library-reconcile: failed on %s", row.path)
            summary.failed.append(f"{row.path} ({type(exc).__name__}: {exc})")
        summary.position += 1

    # Saved even past the deadline: the next run must carry on from here.
    with without_run_deadline():
        _save_state(cursor_store, cursor, drive, settings, run_id, summary)


def _save_state(
    cursor_store: StateFile,
    cursor: dict[str, Any],
    drive: Any,
    settings: BackfillSettings,
    run_id: str,
    summary: ReconcileSummary,
) -> None:
    cursor_store.save(
        {
            "flow": FLOW_NAME,
            "version": 1,
            "root_folder_id": settings.root,
            "position": summary.position,
            "passes": int(cursor.get("passes", 0)) + (1 if summary.wrapped else 0),
            "updated_at": _now(),
        }
    )
    _save_record(drive, settings.state, run_id, summary)


def _save_record(
    drive: Any, state: str, run_id: str, summary: ReconcileSummary
) -> None:
    name = f"{FLOW_NAME}-{run_id}.dry-run.json"
    record = {
        "flow": FLOW_NAME,
        "run_id": run_id,
        "mode": "dry-run",
        "saved_at": _now(),
        "files": f"{summary.start + 1}-{summary.position} of the identified files",
        "compared": summary.compared,
        "matching": summary.matching,
        "counts": {kind: len(summary.differences[kind]) for kind in KINDS},
        "differences": {
            kind: [d.as_dict() for d in summary.differences[kind]] for kind in KINDS
        },
        "failed": summary.failed,
    }
    store = StateFile(drive, state, name)
    store.load()  # a redelivered run writes over its own record
    store.save(record)
    summary.record_name = name


def run_reconcile(
    *,
    run_id: str | None,
    mode: FlowMode,
    drive: Any | None = None,
    api: Catalog | None = None,
) -> ReconcileSummary:
    """One run: the mode, settings, cursor, comparison, record and report."""
    started_at = monotonic()
    run_id = run_id or local_run_id()
    summary = ReconcileSummary(mode, requested_write=mode is FlowMode.WRITE)
    if mode is FlowMode.OFF:
        return summary
    summary.mode = FlowMode.DRY_RUN
    with run_report(
        FLOW_NAME,
        repo=REPO,
        notable=True,
        run_id=run_id,
        version=__version__,
        list_all=True,
    ) as report:
        try:
            settings, chunk_size, problems = _settings()
            if settings is None or problems:
                summary.problems.extend(problems)
                return summary
            if drive is None:
                drive = GoogleAPI.from_env().drive
            client = api or cast(
                Catalog, api_client(base_url=api_base_url().strip() or None)
            )
            _reconcile(drive, client, settings, chunk_size, run_id, summary)
        finally:
            _record(report, summary, monotonic() - started_at)
    return summary


def library_reconcile(*, run_id: str | None = None) -> ReconcileSummary:
    """Compare the next chunk of linked library files with the catalog and
    report the differences by kind."""
    return run_reconcile(run_id=run_id, mode=flow_mode(MODE_VAR))


def _record(report: Any, summary: ReconcileSummary, duration_sec: float) -> None:
    report.duration_sec = duration_sec
    for problem in summary.problems:
        report.issue("setting", problem)
    if summary.problems:
        return
    if summary.requested_write:
        report.notice(
            "write_not_built",
            f"{MODE_VAR}=write runs as a dry run: the reconcile write is not built yet",
        )
    report.notice(
        "dry_run",
        "reads only: no music, catalog or registry value was changed; "
        f"record {summary.record_name or '(none)'}",
    )
    if summary.wrapped:
        report.notice(
            "pass_complete", "every identified file was compared; starting again"
        )
    if summary.stopped:
        report.notice("stopped", summary.stopped)
    report.count("compared", summary.compared)
    report.count("matching", summary.matching)
    report.count("with_differences", summary.files_with_differences)
    for kind in KINDS:
        report.count(kind, len(summary.differences[kind]))
    report.count("failed", len(summary.failed))
    for kind in KINDS:
        diffs = summary.differences[kind]
        shown = diffs if kind == "other" else diffs[:REPORT_EXAMPLES]
        for diff in shown:
            report.notice(kind, diff.example())
    for failure in summary.failed:
        report.issue("failed", failure)
    report.ok(summary.compared)
