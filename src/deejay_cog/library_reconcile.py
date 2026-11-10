"""The ``library-reconcile`` step of the nightly library run: compare each
linked library file's owned fields with what its catalog entry says they
should be, and group every difference by kind (ADR-0008, step 3 and
decision 15).

**A dry run reads only.** Nothing is written to any music file, and
nothing in the catalog or the registry changes: the run writes only its
cursor and its record in ``MUSIC_TAGGING_STATE_FOLDER_ID``, and sends its
differences to the owner's review (below). A **write** run
compares the same way, then applies only what the owner approved (below),
with the catalog winning.

**Which files.** Registered files under ``LIBRARY_ROOT_FOLDER_ID`` that are
present and identified (linked to a catalog entry), in the registry's own
order (``GET /v1/catalog/library/files``, oldest first). Each run takes the
next ``LIBRARY_RECONCILE_CHUNK_SIZE`` files (default 50) after its own
cursor, ``library-reconcile-cursor.json``, kept apart from the backfill's.
A dry run moves this cursor: repeated dry runs step through the whole
library, and after the last file the next run starts again from the first.
The cursor names the last file compared (``after``, its registry id), and
the next run asks for the files after that one. So a file linked, unlinked
or marked not present between runs, earlier in the order, neither repeats
nor skips a file; ``position`` only counts the files compared this pass,
for the report. A cursor with no ``after`` (moved by hand) starts at its
``position`` once; one naming a file the registry does not have starts
the next pass.
Past the run's deadline the run stops between files and saves where it got
to.

**What is compared.** The owned fields (title with its version, artist,
year, genre, BPM, label) as the file's ID3 tags hold them, against the
entry as the display rules write it out (``POST /v1/catalog/display``). The
file's expected title is the entry's title followed by the file's own edit
label in parentheses, as the registry records it ("Song (No Intro Edit)").
The comment frame and every frame VirtualDJ owns are never read for the
comparison, and never written.

**The kinds** a difference is filed under:

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
- ``year_filled`` — the file has no year and the catalog has one
- ``year_earlier`` — the catalog's year is older than the file's
- ``year_later`` — the file's year is older than the catalog's (the older
  year is kept: leave this kind unapproved and the file keeps its year)
- ``year_differs`` — a year that is not a number, so neither is older
- ``bpm_rounding`` (less than ``BPM_ROUNDING`` apart), ``bpm_differs``,
  ``label_differs``
- ``fill_up`` — the file has a value for a field the catalog leaves empty,
  which would go up to the catalog
- ``other`` — anything else, listed file by file

**Record:** ``library-reconcile-<run id>.dry-run.json`` in the state folder
(``.differences.json`` for a write run), with each kind's count and every
difference under it (file, field, the file's value, the catalog's). The run
report gives each kind's count and a few examples, and names every
``other`` file.

**Review.** Every run (dry or write) sends the API the files it compared
and the differences still open on them (``POST
/v1/catalog/library/reconcile/differences``): all of them in a dry run, and
in a write run those it did not write or fill up. For those files they
replace what the API held, so the owner's review page on the WCS site always
shows each file's latest. A run that cannot send them says so in its report
and carries on; the next pass sends them again.

**Approvals** are the owner's, set on the review page and read from the API
by each write run (``GET /v1/catalog/library/reconcile/approvals``)::

    {"kinds": ["case_punctuation", "genre_remapped", "fill_up"],
     "files": {"<drive file id>": ["title"]},
     "file_wins_kinds": ["year_later", "bpm_differs"],
     "file_wins_files": {"<drive file id>": ["artist"]}}

Each approval says which side wins, so every difference has a way to
close. ``kinds`` and ``files`` approve it for the catalog: its value is
written over the file. ``file_wins_kinds`` and ``file_wins_files`` approve
it for the file: its value goes up to the catalog entry (``POST
/v1/catalog/library/files/edited`` with ``keep_file``) and the file is not
written. ``kinds`` names whole kinds (any but ``other``); ``files`` approves
``other`` differences one file and field at a time, and the same for the
file's side. Approvals given before
the API held them were kept in ``library-reconcile-approvals.json`` in the
state folder; ``python -m deejay_cog.reconcile_admin seed`` carries that file
over once, and nothing reads it after. A write run writes each
approved difference with the catalog's value over the same file, in place,
through ``write_in_place`` (head revision kept, audio checked before and
after, every write in ``library-reconcile-<run id>.json``, which
``python -m deejay_cog.music_rollback <run id>`` undoes), registers the file
as synced, and sends approved ``fill_up`` values up to the catalog
(``POST /v1/catalog/{id}/fill-up``) without writing the file. A difference
approved for the file goes up to the entry without writing the file; one
the API holds (a title or artist that does not split into parts cleanly, a
genre off the vocabulary) stays open on the review page, with the reason in
the report. Anything not approved is left as it is and counted. Until any approvals are set a write
run writes nothing. Genre is written; subgenre is not (no frame for it is
decided). An audio mismatch stops the run with an ERROR finding and leaves
the cursor on that file.

**Safety:** ``flags.deejay_cog.library_reconcile`` is ``off`` unless ``dry-run`` or
``write``. The comment frame and VirtualDJ's frames are never written.
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
from mini_app_polis.api import ApiUnavailable, KaianoApiError
from mini_app_polis.api.contract import (
    CATALOG_DISPLAY_MAX_IDS,
    CatalogDisplay,
    CatalogDisplayRequest,
    CatalogFillUp,
    CatalogFillUpResult,
    LibraryFieldEdit,
    LibraryFile,
    LibraryFileEdited,
    LibraryFileEditResult,
    LibraryFileRegister,
    LibraryFileTags,
    ReconcileApprovals,
    ReconcileDifference,
    ReconcileReport,
    ReconcileReportResult,
)
from mini_app_polis.deadline import without_run_deadline
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI
from mini_app_polis.pipeline_status import run_report

from deejay_cog._pipeline_eval import REPO, post_run_finding
from deejay_cog._shared_deadline import shared_deadline_passed
from deejay_cog._version import __version__
from deejay_cog.api_client import api_client
from deejay_cog.library_backfill import (
    BackfillSettings,
    StateFile,
    StateProblem,
)
from deejay_cog.music_safety import (
    AudioChanged,
    FlowMode,
    RunRecord,
    file_meta,
    flow_mode,
    local_run_id,
    md5,
    write_in_place,
)
from deejay_cog.music_tags import (
    duration_secs,
    linked_catalog_id,
    read_tag_row,
    write_owned_fields,
)

log = logger_mod.get_logger()

FLOW_NAME = "library-reconcile"

#: The feature flag holding this step's mode: off | dry-run | write; a
#: missing flag or an unknown value is off.
MODE_FLAG = "flags.deejay_cog.library_reconcile"

#: Files per run; a whole number of at least 1.
CHUNK_SIZE_VAR = "LIBRARY_RECONCILE_CHUNK_SIZE"
DEFAULT_CHUNK_SIZE = 50

CURSOR_NAME = f"{FLOW_NAME}-cursor.json"
APPROVALS_NAME = f"{FLOW_NAME}-approvals.json"

#: The kinds a difference is filed under, in the order they are reported.
KINDS = (
    "case_punctuation",
    "featured_moved",
    "artist_joiner",
    "version_brackets",
    "genre_remapped",
    "genre_filled",
    "year_filled",
    "year_earlier",
    "year_later",
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

#: BPM closer than this is a rounding difference, not a different tempo:
#: versions at different tempos are further apart.
BPM_ROUNDING = 1.5


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

    def fill_up_catalog_entry(
        self, catalog_id: str | uuid.UUID, payload: CatalogFillUp
    ) -> CatalogFillUpResult:
        """Fill the entry's empty fields from a file."""
        ...

    def register_library_file(self, payload: LibraryFileRegister) -> LibraryFile:
        """Register a library file, or bring its row up to date."""
        ...

    def report_library_file_edited(
        self, payload: LibraryFileEdited
    ) -> LibraryFileEditResult:
        """Send a file's fields up to its entry (``keep_file``: the file wins)."""
        ...

    def read_reconcile_approvals(self) -> ReconcileApprovals:
        """The owner's approvals for write runs."""
        ...

    def report_reconcile_differences(
        self, payload: ReconcileReport
    ) -> ReconcileReportResult:
        """The files a run compared and their open differences."""
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
        """The difference as the run record stores it."""
        return {
            "path": self.path,
            "drive_file_id": self.drive_file_id,
            "field": self.field,
            "file": self.file_value,
            "catalog": self.catalog_value,
        }

    def for_review(self) -> ReconcileDifference:
        """The difference as the API's review holds it."""
        return ReconcileDifference.model_validate(
            {
                "drive_file_id": self.drive_file_id,
                "path": self.path,
                "field": self.field,
                "kind": self.kind,
                "file_value": self.file_value,
                "catalog_value": self.catalog_value,
            }
        )

    def example(self) -> str:
        """One line naming the file, the field and both values, for reports."""
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


_YEAR = re.compile(r"^\s*(\d{4})")


def _year(text: str) -> int | None:
    """The year a tag starts with ("2016" or "2016-05-12"), or None."""
    match = _YEAR.match(text)
    return int(match.group(1)) if match else None


def _year_kind(have: str, want: str) -> str:
    """Which year kind a differing year is: filled, earlier or later by the
    catalog's year against the file's, or differs when either will not
    read as a year."""
    if not have:
        return "year_filled"
    have_y, want_y = _year(have), _year(want)
    if have_y is None or want_y is None:
        return "year_differs"
    if want_y == have_y:
        return "case_punctuation"
    return "year_earlier" if want_y < have_y else "year_later"


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
                add(name, "bpm_rounding" if gap < BPM_ROUNDING else "bpm_differs")
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
            add(name, _year_kind(have, want))
        elif _fold(have) == _fold(want):
            add(name, "case_punctuation")
        else:
            add(name, "label_differs")
    return diffs


# ---------------------------------------------------------------------------
# Approvals
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Approvals:
    """What the owner approved, for the catalog (``kinds``, ``files``) or for
    the file (``file_kinds``, ``file_files``): whole kinds, and ``other``
    differences one file and field at a time."""

    kinds: frozenset[str] = frozenset()
    files: dict[str, frozenset[str]] = field(default_factory=dict)
    file_kinds: frozenset[str] = frozenset()
    file_files: dict[str, frozenset[str]] = field(default_factory=dict)

    def side(self, diff: Difference) -> str | None:
        """Which value wins for ``diff``: ``catalog``, ``file``, or None when
        the owner has not approved it."""
        if diff.kind == "other":
            if diff.field in self.files.get(diff.drive_file_id, frozenset()):
                return "catalog"
            if diff.field in self.file_files.get(diff.drive_file_id, frozenset()):
                return "file"
            return None
        if diff.kind in self.kinds:
            return "catalog"
        if diff.kind in self.file_kinds:
            return "file"
        return None

    def approves(self, diff: Difference) -> bool:
        """Whether the owner approved ``diff``, for either side."""
        return self.side(diff) is not None


def parse_approvals(data: dict[str, Any]) -> tuple[Approvals, list[str]]:
    """The approvals in ``data``, and what is wrong with it."""
    problems: list[str] = []
    kinds = data.get("kinds") or []
    files = data.get("files") or {}
    if not isinstance(kinds, list) or not isinstance(files, dict):
        return Approvals(), [f"{APPROVALS_NAME}: kinds must be a list, files an object"]
    for kind in kinds:
        if kind == "other":
            problems.append(
                f"{APPROVALS_NAME}: 'other' is approved file by file, under files"
            )
        elif kind not in KINDS:
            problems.append(f"{APPROVALS_NAME}: unknown kind {kind!r}")
    allowed = set(FIELDS) | {"catalog_id"}
    for file_id, names in files.items():
        if not isinstance(names, list) or not set(names) <= allowed:
            problems.append(
                f"{APPROVALS_NAME}: files[{file_id!r}] must list fields from "
                f"{sorted(allowed)}"
            )
    if problems:
        return Approvals(), problems
    return (
        Approvals(frozenset(kinds), {str(k): frozenset(v) for k, v in files.items()}),
        [],
    )


def from_api(approvals: ReconcileApprovals) -> Approvals | None:
    """The API's approvals, or None when none have been set yet."""
    if approvals.updated_at is None:
        return None
    return Approvals(
        frozenset(approvals.kinds),
        {str(k): frozenset(v) for k, v in approvals.files.items()},
        frozenset(approvals.file_wins_kinds),
        {str(k): frozenset(v) for k, v in approvals.file_wins_files.items()},
    )


#: The catalog field a fill-up sets, by the difference's field.
_FILLED_FIELD = {
    "year": "release_year",
    "genre": "genre",
    "bpm": "bpm",
    "label": "label",
}


def _fill_up(diffs: list[Difference]) -> CatalogFillUp | None:
    values: dict[str, Any] = {}
    for diff in diffs:
        if diff.field == "year" and diff.file_value.isdigit():
            values["release_year"] = int(diff.file_value)
        elif diff.field == "bpm":
            number = _number(diff.file_value)
            if number is not None and number > 0:
                values["bpm"] = number
        elif diff.field in ("genre", "label"):
            values[diff.field] = diff.file_value
    return CatalogFillUp(**values) if values else None


# ---------------------------------------------------------------------------
# One run
# ---------------------------------------------------------------------------


@dataclass
class ReconcileSummary:
    """What one run compared and found."""

    mode: FlowMode
    problems: list[str] = field(default_factory=list)
    record_name: str = ""
    start: int = 0
    position: int = 0
    after: str = ""
    total: int = 0
    wrapped: bool = False
    compared: int = 0
    matching: int = 0
    stopped: str = ""
    differences: dict[str, list[Difference]] = field(
        default_factory=lambda: {kind: [] for kind in KINDS}
    )
    failed: list[str] = field(default_factory=list)
    approvals_missing: bool = False
    written: list[str] = field(default_factory=list)
    would_write: list[str] = field(default_factory=list)
    filled: list[str] = field(default_factory=list)
    kept: list[str] = field(default_factory=list)
    kept_held: list[str] = field(default_factory=list)
    not_approved: int = 0
    aborted: str = ""
    reviewed: list[str] = field(default_factory=list)
    open: list[Difference] = field(default_factory=list)
    review_problem: str = ""

    @property
    def files_with_differences(self) -> int:
        """How many files compared had at least one difference."""
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


def _page(
    api: Catalog, root: str, limit: int, *, offset: int = 0, after: str = ""
) -> list[LibraryFile]:
    paging: dict[str, Any] = (
        {"after": uuid.UUID(after)} if after else {"offset": offset}
    )
    return api.list_library_files(
        library_root_id=root,
        identification_status="identified",
        present=True,
        limit=limit,
        **paging,
    )


def _resume(
    api: Catalog, root: str, limit: int, cursor: dict[str, Any]
) -> tuple[list[LibraryFile], bool]:
    """The next chunk after the cursor, and whether it wrapped to the first
    file: past the last file, or after a file the registry does not have."""
    after = str(cursor.get("after") or "")
    position = int(cursor.get("position", 0))
    try:
        rows = _page(api, root, limit, offset=position, after=after)
    except KaianoApiError as exc:
        if not after or isinstance(exc, ApiUnavailable) or exc.status_code != 404:
            raise
        rows = []
    if rows or not (after or position):
        return rows, False
    return _page(api, root, limit), True


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
) -> tuple[str, bytes, list[Difference], dict[str, str]] | None:
    """Compare one file; return its name, bytes, differences and expected
    values, or None when it could not be compared."""
    if display is None:
        summary.failed.append(
            f"{row.path} (its catalog entry {row.catalog_id} is gone)"
        )
        return None
    meta = file_meta(drive, row.drive_file_id)
    if meta.get("trashed"):
        summary.failed.append(f"{row.path} (in the trash; the scan will mark it)")
        return None
    name = str(meta.get("name") or row.path.rsplit("/", 1)[-1])
    data: bytes = drive.download_file_bytes(row.drive_file_id).data
    tags = read_tag_row(data)
    wanted = expected(display, row)
    diffs = classify(row.path, row.drive_file_id, tags, wanted)
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
    return name, data, diffs, {**wanted, "catalog_id": str(row.catalog_id)}


def _apply_one(
    drive: Any,
    api: Catalog,
    record: RunRecord,
    settings: BackfillSettings,
    row: LibraryFile,
    compared: tuple[str, bytes, list[Difference], dict[str, str]],
    approvals: Approvals,
    summary: ReconcileSummary,
) -> set[str]:
    """Settle one file's approved differences: send those approved for the
    file up to its entry, send its approved fill-ups to the catalog, and
    write those approved for the catalog over the file. Return the fields it
    settled. Raises :class:`AudioChanged`."""
    name, data, diffs, wanted = compared
    approved = [d for d in diffs if approvals.approves(d)]
    summary.not_approved += len(diffs) - len(approved)
    settled: set[str] = set()
    kept = [d for d in approved if approvals.side(d) == "file"]
    if kept and record.mode.writes:
        settled |= _keep_file(api, row, data, kept, summary)
    approved = [d for d in approved if approvals.side(d) == "catalog"]
    fills = [d for d in approved if d.kind == "fill_up"]
    fill = _fill_up(fills)
    to_write = {d.field: wanted[d.field] for d in approved if d.kind != "fill_up"}
    if fill is not None and record.mode.writes and row.catalog_id:
        filled = api.fill_up_catalog_entry(row.catalog_id, fill).filled
        if filled:
            summary.filled.append(f"{row.path} ({', '.join(filled)})")
        settled |= {d.field for d in fills if _FILLED_FIELD.get(d.field) in filled}
    if not to_write:
        return settled
    tagged = write_owned_fields(data, to_write)
    if tagged is None:
        return settled
    entry = write_in_place(
        drive,
        record,
        file_id=row.drive_file_id,
        name=name,
        original=data,
        tagged=tagged,
        identified_by="reconcile (catalog wins)",
    )
    label = f"{row.path} ({', '.join(sorted(to_write))})"
    if entry.state != "done":
        summary.would_write.append(label)
        return settled
    summary.written.append(label)
    settled |= set(to_write)
    api.register_library_file(
        LibraryFileRegister(
            drive_file_id=row.drive_file_id,
            library_root_id=settings.root,
            path=row.path,
            size_bytes=len(tagged),
            md5=md5(tagged),
            duration_secs=duration_secs(tagged),
            identification_status="identified",
            catalog_id=row.catalog_id,
            synced_at=datetime.now(UTC),
        )
    )
    return settled


def _keep_file(
    api: Catalog,
    row: LibraryFile,
    data: bytes,
    kept: list[Difference],
    summary: ReconcileSummary,
) -> set[str]:
    """Send the fields approved for the file up to its entry; return those
    the entry took. The file is not written. A field the API holds stays
    open, with why."""
    fields = sorted({d.field for d in kept if d.field in FIELDS})
    if not fields:
        return set()
    tags = {name: value or None for name, value in read_tag_row(data).items()}
    result = api.report_library_file_edited(
        LibraryFileEdited(
            drive_file_id=row.drive_file_id,
            tags=LibraryFileTags(**tags),
            keep_file=fields,  # type: ignore[arg-type]
        )
    )
    outcomes: dict[str, LibraryFieldEdit] = {f.field: f for f in result.fields}
    taken: set[str] = set()
    for name in fields:
        edit = outcomes.get(name)
        if edit is None:
            # The API already shows the file's value; the next pass compares
            # again, and this says why it may still differ.
            summary.kept_held.append(
                f"{row.path}: {name} (the catalog already shows the file's value)"
            )
            continue
        if edit.outcome == "sent_up":
            taken.add(name)
        else:
            summary.kept_held.append(
                f"{row.path}: {name} {edit.file!r} ({edit.outcome}"
                f"{': ' + edit.why if edit.why else ''})"
            )
    if taken:
        summary.kept.append(f"{row.path} ({', '.join(sorted(taken))})")
    return taken


def _send_review(api: Catalog, run_id: str, summary: ReconcileSummary) -> None:
    """Send the files compared and their open differences to the review;
    a failure is reported, never raised."""
    if not summary.reviewed:
        return
    try:
        api.report_reconcile_differences(
            ReconcileReport(
                run_id=run_id,
                compared=list(dict.fromkeys(summary.reviewed)),
                differences=[d.for_review() for d in summary.open],
            )
        )
    except Exception as exc:  # noqa: BLE001 — the review is not the run
        log.exception("library-reconcile: could not send the review")
        summary.review_problem = f"{type(exc).__name__}: {exc}"


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
    rows, summary.wrapped = _resume(api, settings.root, chunk_size, cursor)
    if summary.wrapped:
        # Past the last file: the next pass starts from the first.
        position = 0
    else:
        summary.after = str(cursor.get("after") or "")
    summary.start = summary.position = position
    approvals = Approvals()
    record: RunRecord | None = None
    if summary.mode.writes:
        try:
            given = from_api(api.read_reconcile_approvals())
        except Exception as exc:  # noqa: BLE001 — no approvals, no writes
            summary.problems.append(
                f"could not read the approvals from the API: {type(exc).__name__}: {exc}"
            )
            return
        if given is None:
            summary.approvals_missing = True
        else:
            approvals = given
        record = RunRecord(
            FLOW_NAME,
            run_id,
            summary.mode,
            stage=f"identified files from {position + 1}",
            folders={"root": settings.root, "state": settings.state},
        ).attach(drive, settings.state)
    displays = _displays(api, rows) if rows else {}

    status = "complete"
    try:
        for row in rows:
            if shared_deadline_passed():
                summary.stopped = f"run deadline reached before {row.path}"
                status = "stopped"
                break
            try:
                display = displays.get(row.catalog_id) if row.catalog_id else None
                compared = _compare_one(drive, row, display, summary)
                settled: set[str] = set()
                if compared is not None and record is not None:
                    settled = _apply_one(
                        drive, api, record, settings, row, compared, approvals, summary
                    )
                if compared is not None:
                    summary.reviewed.append(row.drive_file_id)
                    summary.open.extend(
                        d for d in compared[2] if d.field not in settled
                    )
            except AudioChanged as exc:
                # The cursor stays on this file: the run stops here.
                summary.aborted = f"{row.path}: {exc}"
                status = "aborted"
                break
            except Exception as exc:  # noqa: BLE001 — one file must not end the run
                if shared_deadline_passed():
                    summary.stopped = f"run deadline reached reading {row.path}"
                    status = "stopped"
                    break
                log.exception("library-reconcile: failed on %s", row.path)
                summary.failed.append(f"{row.path} ({type(exc).__name__}: {exc})")
            summary.position += 1
            summary.after = str(row.id)
    finally:
        # Saved even past the deadline: the next run must carry on from here.
        with without_run_deadline():
            if record is not None and record.entries:
                record.finish(status)
            _save_state(cursor_store, cursor, drive, settings, run_id, summary)
            _send_review(api, run_id, summary)
    if summary.aborted:
        _report_abort(run_id, summary)


def _report_abort(run_id: str, summary: ReconcileSummary) -> None:
    try:
        post_run_finding(
            FLOW_NAME,
            "ERROR",
            "library-reconcile stopped: a file's audio would not stay "
            f"byte-identical ({summary.aborted}). The file is unchanged (or "
            "restored); the cursor stays on it.",
            run_id=run_id,
        )
    except Exception:  # noqa: BLE001 — the notification is not the job
        log.exception("library-reconcile: could not report the stop")


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
            "after": summary.after,
            "passes": int(cursor.get("passes", 0)) + (1 if summary.wrapped else 0),
            "updated_at": _now(),
        }
    )
    _save_record(drive, settings.state, run_id, summary)


def _save_record(
    drive: Any, state: str, run_id: str, summary: ReconcileSummary
) -> None:
    writes = summary.mode.writes
    name = f"{FLOW_NAME}-{run_id}.{'differences' if writes else 'dry-run'}.json"
    record = {
        "flow": FLOW_NAME,
        "run_id": run_id,
        "mode": summary.mode.value,
        "saved_at": _now(),
        "files": f"{summary.start + 1}-{summary.position} of the identified files",
        "compared": summary.compared,
        "matching": summary.matching,
        "counts": {kind: len(summary.differences[kind]) for kind in KINDS},
        "differences": {
            kind: [d.as_dict() for d in summary.differences[kind]] for kind in KINDS
        },
        "failed": summary.failed,
        "written": summary.written,
        "filled": summary.filled,
        "kept": summary.kept,
        "kept_held": summary.kept_held,
        "not_approved": summary.not_approved,
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
    summary = ReconcileSummary(mode)
    if mode is FlowMode.OFF:
        return summary
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


def library_reconcile(
    *, run_id: str | None = None, mode: FlowMode | None = None
) -> ReconcileSummary:
    """Compare the next chunk of linked library files with the catalog and
    report the differences by kind, in ``mode`` or, without one, the mode
    the step's feature flag sets."""
    return run_reconcile(
        run_id=run_id, mode=flow_mode(MODE_FLAG) if mode is None else mode
    )


def _record(report: Any, summary: ReconcileSummary, duration_sec: float) -> None:
    report.duration_sec = duration_sec
    for problem in summary.problems:
        report.issue("setting", problem)
    if summary.problems:
        return
    if summary.mode.writes:
        if summary.approvals_missing:
            report.notice(
                "no_approvals",
                "no approvals are set in the API yet: nothing was written",
            )
    else:
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
    if summary.review_problem:
        report.issue(
            "review_not_sent",
            "the differences did not reach the review page "
            f"({summary.review_problem}); the next pass sends them again",
        )
    report.count("compared", summary.compared)
    report.count("matching", summary.matching)
    report.count("with_differences", summary.files_with_differences)
    for kind in KINDS:
        report.count(kind, len(summary.differences[kind]))
    report.count("failed", len(summary.failed))
    if summary.mode.writes:
        report.count("written", len(summary.written))
        report.count("filled_up", len(summary.filled))
        report.count("kept_from_file", len(summary.kept))
        report.count("not_approved", summary.not_approved)
        for item in summary.written:
            report.created("tags written", item)
        for item in summary.filled:
            report.updated("catalog filled up", item)
        for item in summary.kept:
            report.updated("catalog took the file's value", item)
        for item in summary.kept_held:
            report.notice("kept_held", item)
    if summary.aborted:
        report.issue("audio_changed_run_stopped", summary.aborted)
    for kind in KINDS:
        diffs = summary.differences[kind]
        shown = diffs if kind == "other" else diffs[:REPORT_EXAMPLES]
        for diff in shown:
            report.notice(kind, diff.example())
    for failure in summary.failed:
        report.issue("failed", failure)
    report.ok(summary.compared)
