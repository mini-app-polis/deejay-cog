"""The ``library-reconcile`` worker mode, on its own schedule: compare each
linked library file's owned fields with what its catalog entry says they
should be, and group every difference by kind (ADR-0008, step 3 and
decision 15).

**A dry run reads only.** Nothing is written to any music file, and
nothing in the catalog or the registry changes: the run writes only its
what it compared and its record in ``MUSIC_TAGGING_STATE_FOLDER_ID``, and sends its
differences to the owner's review (below). A **write** run
compares the same way, then applies only what the owner approved (below),
with the catalog winning.

**Which files: only those that changed.** Registered files under
``LIBRARY_ROOT_FOLDER_ID`` that are present and identified (linked to a
catalog entry), in the registry's own order (``GET
/v1/catalog/library/files``). ``library-reconcile-seen.json`` in the state
folder keeps, for each file, what it was when it was last compared: when
Drive last saw it change (from the scan's listing,
``library-scan-listing.json``), when its entry's display last changed
(``changed_at`` from ``POST /v1/catalog/display``, which a display-rule
change moves too) and the entry it was linked to. A run compares a file only
when one of those has moved since, or it was never compared; up to
``LIBRARY_RECONCILE_CHUNK_SIZE`` of them, the rest on the next run
(``changed_left``). A library where nothing changed is not read at all. To
compare every file again (after a change to the reconcile's own rules),
trash ``library-reconcile-seen.json``: every file is then new to it, and the
runs work through the library a chunk at a time. Past the run's deadline the
run stops between files and saves what it compared.

**Approved files first.** A write run first compares the files that have
an open difference the owner has approved (from ``library-reconcile-open.json``
and the approvals), up to ``LIBRARY_RECONCILE_CHUNK_SIZE`` of them, so an
approval settles on the next run. An approved difference a write run tried
and left open (a fill-up the catalog refused, a value the API held) is
marked ``tried`` and not put first again; the file is compared again when it
changes.

**What is compared.** The owned fields (title with its version, artist,
year, genre, BPM, label) as the file's ID3 tags hold them, against the
entry as the display rules write it out (``POST /v1/catalog/display``). The
file's expected title is the entry's title followed by the file's own edit
label in parentheses, as the registry records it ("Song (No Intro Edit)").
The comment frame and every frame VirtualDJ owns are never read for the
comparison, and never written.
The tags are read from the start and end of the file (a ranged Drive
download); the whole file is downloaded only when its ID3v2 tag runs past
the start read, or when an approved difference is written over it.

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

**Long-open differences.** ``library-reconcile-open.json`` in the state
folder keeps when each open difference was first seen (a difference whose
values change starts again). Every run counts those open longer than
``LONG_OPEN_DAYS`` (``long_open``), leaving out an approved fill-up a run
tried and the catalog refused (the file keeps that value on purpose): by
then each has had the chance to settle, so each has no decision or one
that cannot apply. The first run to find any warns (``long_open``, with the count by kind); it warns
again only after the count has been back to zero.

**Owner edits.** A file whose title the owner retitled with a version in
``< >`` ("Song <Intro Cut>": a cut he made himself) while its entry is the
published song's, or another owner edit, is not compared: the retitle is the
owner's choice and goes up to the catalog (ADR-0008, decided 2026-11-11).
The file is linked to its owner-edit entry, the one the catalog's matching
finds or one made from its title, artist and year, with only
``TXXX:CATALOG_ID`` written and the file registered on the new entry. The
entry it leaves keeps its plays and its other files. This needs no approval;
a dry run looks up and previews the entry and changes nothing. The next run
compares the file with its new entry.

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
decided). An audio mismatch stops the run with an ERROR finding; that file
is not marked compared, so the next run takes it again.

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
from mini_app_polis.api.contract import (
    CATALOG_DISPLAY_MAX_IDS,
    CatalogDisplay,
    CatalogDisplayRequest,
    CatalogFillUp,
    CatalogFillUpResult,
    CatalogLookupRequest,
    CatalogMatch,
    LibraryEntryCreate,
    LibraryEntryCreated,
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
from deejay_cog.library_scan import LISTING_NAME as SCAN_LISTING_NAME
from deejay_cog.music_safety import (
    AudioChanged,
    FlowMode,
    RunRecord,
    TagBytes,
    file_meta,
    flow_mode,
    local_run_id,
    md5,
    read_tag_bytes,
    whole_bytes,
    write_in_place,
)
from deejay_cog.music_tags import (
    duration_secs,
    edit_label,
    ingest_track,
    linked_catalog_id,
    needs_owner_edit_entry,
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

#: What each file was when it was last compared: its Drive modified time
#: (from the scan's listing) and when its entry's display last changed.
SEEN_NAME = f"{FLOW_NAME}-seen.json"
#: Every open difference and when it was first seen open, across runs.
OPEN_NAME = f"{FLOW_NAME}-open.json"

#: Registry rows asked for per page when finding the approved files.
REGISTRY_PAGE = 500

#: A difference open this long has had a full pass to close and did not:
#: it has no decision, or its decision cannot apply. The report warns once.
LONG_OPEN_DAYS = 7
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

    def lookup_catalog(
        self, payload: CatalogLookupRequest
    ) -> list[CatalogMatch | None]:
        """The catalog entry each track is a play of."""
        ...

    def create_library_entry(self, payload: LibraryEntryCreate) -> LibraryEntryCreated:
        """The song's entry, made from the library when the catalog lacks it."""
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
            if diff.field == "catalog_id":
                # The row's id is the truth (a merge repoints rows only).
                return "catalog"
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
    #: Files that changed since they were last compared, beyond this run's
    #: chunk: compared on a later run.
    changed_left: int = 0
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
    long_open: dict[str, int] = field(default_factory=dict)
    long_open_warn: bool = False
    open_problem: str = ""
    #: Files compared first because they had an approved open difference.
    approved_first: int = 0
    #: Open-difference keys a write run tried to settle and left open (a
    #: fill-up the catalog refused, a value the API held): not put first again.
    tried: set[str] = field(default_factory=set)
    #: Files the owner retitled as an owner edit, moved to an owner-edit
    #: entry (or, in a dry run, that would be): "path: old -> new (title)".
    owner_edits: list[str] = field(default_factory=list)

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


def _approved_open(
    drive: Any,
    api: Catalog,
    settings: BackfillSettings,
    approvals: Approvals,
    limit: int,
) -> list[LibraryFile]:
    """The registry rows of the files with an open difference the owner has
    approved, in the registry's order, at most ``limit``. Empty when the
    open-differences file cannot be read (the run goes on with the changed files)."""
    try:
        held = StateFile(drive, settings.state, OPEN_NAME).load() or {}
    except StateProblem:
        log.exception("library-reconcile: open differences not read")
        return []
    ids: set[str] = set()
    for key, value in (held.get("open") or {}).items():
        file_id, _, name = str(key).partition("|")
        diff = Difference(
            str(value.get("path") or ""),
            file_id,
            name,
            str(value.get("file") or ""),
            str(value.get("catalog") or ""),
            str(value.get("kind") or "other"),
        )
        if value.get("tried"):
            # A run already tried it and it stayed open: compared again on a change.
            continue
        if approvals.approves(diff):
            ids.add(file_id)
    if not ids:
        return []
    found: list[LibraryFile] = []
    offset = 0
    while len(found) < limit:
        page = _page(api, settings.root, REGISTRY_PAGE, offset=offset)
        found.extend(row for row in page if row.drive_file_id in ids)
        if len(page) < REGISTRY_PAGE:
            break
        offset += len(page)
    return found[:limit]


def _all_rows(api: Catalog, root: str) -> list[LibraryFile]:
    """Every present, identified registry row under the root, in order."""
    rows: list[LibraryFile] = []
    while True:
        page = _page(api, root, REGISTRY_PAGE, offset=len(rows))
        rows.extend(page)
        if len(page) < REGISTRY_PAGE:
            return rows


def _drive_modified(drive: Any, settings: BackfillSettings) -> dict[str, str]:
    """When Drive last saw each library file change, from the scan's listing."""
    listing = StateFile(drive, settings.state, SCAN_LISTING_NAME).load() or {}
    return {
        str(item["id"]): str(item.get("modified") or "")
        for item in listing.get("files") or []
    }


def _stamp(modified: dict[str, str], row: LibraryFile, display: Any) -> dict[str, str]:
    """What a file is now, as the seen record keeps it."""
    shown = getattr(display, "changed_at", None)
    return {
        "modified": modified.get(row.drive_file_id, ""),
        "shown": shown.isoformat() if shown is not None else "",
        "catalog_id": str(row.catalog_id or ""),
    }


def baseline(drive: Any, api: Catalog, settings: BackfillSettings) -> int:
    """Record every present, identified file as compared as it is now,
    without reading one, so the next runs compare only what changes after.
    For a library just reconciled in full. Returns how many files it
    recorded."""
    every = _all_rows(api, settings.root)
    shown = _displays(api, every) if every else {}
    modified = _drive_modified(drive, settings)
    store = StateFile(drive, settings.state, SEEN_NAME)
    store.load()
    store.save(
        {
            "flow": FLOW_NAME,
            "version": 1,
            "root_folder_id": settings.root,
            "files": {
                row.drive_file_id: _stamp(
                    modified, row, shown.get(row.catalog_id) if row.catalog_id else None
                )
                for row in every
            },
            "updated_at": _now(),
        }
    )
    return len(every)


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


@dataclass(frozen=True)
class OwnerEditRetitle:
    """A linked file whose title names an owner edit its entry is not: the
    owner's choice, which goes up to the catalog as a link to an owner-edit
    entry rather than as differences."""

    name: str
    read: TagBytes
    tags: dict[str, str]


def _compare_one(
    drive: Any,
    row: LibraryFile,
    display: CatalogDisplay | None,
    summary: ReconcileSummary,
    *,
    owner_edits: bool = True,
) -> tuple[str, TagBytes, list[Difference], dict[str, str]] | OwnerEditRetitle | None:
    """Compare one file; return its name, the bytes its tags were read from
    (its start and end, or the whole file when its tag runs past the start
    read), differences and expected values, or None when it could not be
    compared. With ``owner_edits``, a file retitled as an owner edit its
    entry is not is returned as an :class:`OwnerEditRetitle`, uncompared."""
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
    read = read_tag_bytes(drive, row.drive_file_id)
    tags = read_tag_row(read.data)
    if owner_edits and needs_owner_edit_entry(tags["title"], display):
        return OwnerEditRetitle(name, read, tags)
    wanted = expected(display, row)
    diffs = classify(row.path, row.drive_file_id, tags, wanted)
    link = linked_catalog_id(read.data)
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
    return name, read, diffs, {**wanted, "catalog_id": str(row.catalog_id)}


def _apply_one(
    drive: Any,
    api: Catalog,
    record: RunRecord,
    settings: BackfillSettings,
    row: LibraryFile,
    compared: tuple[str, TagBytes, list[Difference], dict[str, str]],
    approvals: Approvals,
    summary: ReconcileSummary,
) -> set[str]:
    """Settle one file's approved differences: send those approved for the
    file up to its entry, send its approved fill-ups to the catalog, and
    write those approved for the catalog over the file. Return the fields it
    settled. A write is built from the whole file, downloaded here when the
    comparison read only its start and end. Raises :class:`AudioChanged`."""
    name, read, diffs, wanted = compared
    approved = [d for d in diffs if approvals.approves(d)]
    summary.not_approved += len(diffs) - len(approved)
    settled: set[str] = set()
    kept = [d for d in approved if approvals.side(d) == "file"]
    if kept and record.mode.writes:
        settled |= _keep_file(api, row, read.data, kept, summary)
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
    data = whole_bytes(drive, row.drive_file_id, read)
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


def _entry_year(text: str) -> int | None:
    """The year a new entry may take from a file's year tag, or None."""
    year = _year(text)
    return year if year is not None and year >= 1000 else None


def _take_owner_edit(
    drive: Any,
    api: Catalog,
    record: RunRecord | None,
    settings: BackfillSettings,
    row: LibraryFile,
    retitled: OwnerEditRetitle,
    summary: ReconcileSummary,
) -> bool:
    """Move a file the owner retitled as an owner edit to its owner-edit
    entry: the catalog's, by the set import's matching, or one made from the
    file's title, artist and year. Only the link (``TXXX:CATALOG_ID``) is
    written to the file, and it is registered on the new entry. The entry it
    leaves keeps its plays and its other files. A dry run (no ``record``)
    looks up and previews, and changes nothing.

    Returns False when the catalog's answer is the file's own entry: the
    file is then compared as any other."""
    tags = retitled.tags
    track = ingest_track(tags)
    if track is None:
        return False
    match = api.lookup_catalog(CatalogLookupRequest(tracks=[track]))[0]
    if match is not None and not match.owner_edit:
        match = None
    made = "an owner-edit entry it has"
    if match is None:
        created = api.create_library_entry(
            LibraryEntryCreate(
                title=tags["title"].strip(),
                artist=tags["artist"].strip(),
                release_year=_entry_year(tags.get("year") or ""),
                preview=record is None,
            )
        )
        if not created.entry.owner_edit:
            summary.failed.append(
                f"{row.path} (the catalog did not take {tags['title']!r} as an "
                "owner edit; the file is left on its entry)"
            )
            return True
        match = created.entry
        if created.created or created.previewed:
            made = "a new owner-edit entry"
    if match.id == row.catalog_id:
        return False
    line = (
        f"{row.path}: {row.catalog_id} -> {match.id} "
        f"({match.artist} - {match.title}, {made})"
    )
    if record is None:
        summary.owner_edits.append(line)
        return True
    data = whole_bytes(drive, row.drive_file_id, retitled.read)
    current = data
    tagged = write_owned_fields(data, {"catalog_id": str(match.id)})
    if tagged is not None:
        entry = write_in_place(
            drive,
            record,
            file_id=row.drive_file_id,
            name=retitled.name,
            original=data,
            tagged=tagged,
            identified_by="reconcile (owner edit, linked to its own entry)",
        )
        if entry.state != "done":
            summary.failed.append(f"{row.path} (its link was not written)")
            return True
        current = tagged
    api.register_library_file(
        LibraryFileRegister(
            drive_file_id=row.drive_file_id,
            library_root_id=settings.root,
            path=row.path,
            size_bytes=len(current),
            md5=md5(current),
            duration_secs=duration_secs(current),
            identification_status="identified",
            catalog_id=match.id,
            edit_label=edit_label(tags["title"]),
            synced_at=datetime.now(UTC),
        )
    )
    summary.owner_edits.append(line)
    # The review drops what it held for the file under its old entry.
    summary.reviewed.append(row.drive_file_id)
    return True


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
    seen_store = StateFile(drive, settings.state, SEEN_NAME)
    try:
        held = seen_store.load() or {}
        modified = _drive_modified(drive, settings)
    except StateProblem as exc:
        summary.problems.append(str(exc))
        return
    if held.get("root_folder_id") not in (None, settings.root):
        held = {}
    seen: dict[str, dict[str, str]] = dict(held.get("files") or {})
    every = _all_rows(api, settings.root)
    shown = _displays(api, every) if every else {}
    changed = [
        row
        for row in every
        if seen.get(row.drive_file_id)
        != _stamp(modified, row, shown.get(row.catalog_id) if row.catalog_id else None)
    ]
    rows = changed[:chunk_size]
    summary.changed_left = len(changed) - len(rows)
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
            stage=f"{len(rows)} changed files",
            folders={"root": settings.root, "state": settings.state},
        ).attach(drive, settings.state)
    first: list[LibraryFile] = []
    if record is not None and approvals != Approvals():
        first = _approved_open(drive, api, settings, approvals, chunk_size)
    summary.approved_first = len(first)
    done_first = {row.drive_file_id for row in first}
    queue = [(row, True) for row in first] + [
        (row, False) for row in rows if row.drive_file_id not in done_first
    ]
    displays = shown

    status = "complete"
    try:
        for row, _is_first in queue:
            if shared_deadline_passed():
                summary.stopped = f"run deadline reached before {row.path}"
                status = "stopped"
                break
            try:
                display = displays.get(row.catalog_id) if row.catalog_id else None
                compared = _compare_one(drive, row, display, summary)
                if isinstance(compared, OwnerEditRetitle):
                    if _take_owner_edit(
                        drive, api, record, settings, row, compared, summary
                    ):
                        # Not marked compared: the next run compares the file
                        # with its new entry (or, after a dry run, links it).
                        continue
                    compared = _compare_one(
                        drive, row, display, summary, owner_edits=False
                    )
                    if isinstance(compared, OwnerEditRetitle):  # not with them off
                        continue
                settled: set[str] = set()
                if compared is not None and record is not None:
                    settled = _apply_one(
                        drive, api, record, settings, row, compared, approvals, summary
                    )
                if compared is not None and record is not None:
                    summary.tried |= {
                        _open_key(d)
                        for d in compared[2]
                        if approvals.approves(d) and d.field not in settled
                    }
                if compared is not None:
                    display = displays.get(row.catalog_id) if row.catalog_id else None
                    seen[row.drive_file_id] = _stamp(modified, row, display)
                    summary.reviewed.append(row.drive_file_id)
                    summary.open.extend(
                        d for d in compared[2] if d.field not in settled
                    )
            except AudioChanged as exc:
                # Not marked seen: the next run compares it again first.
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
    finally:
        # Saved even past the deadline: the next run must carry on from here.
        with without_run_deadline():
            if record is not None and record.entries:
                record.finish(status)
            present = {row.drive_file_id for row in every}
            seen_store.save(
                {
                    "flow": FLOW_NAME,
                    "version": 1,
                    "root_folder_id": settings.root,
                    "files": {k: v for k, v in seen.items() if k in present},
                    "updated_at": _now(),
                }
            )
            if summary.compared or summary.failed or summary.owner_edits:
                # A run that compared nothing leaves no record.
                _save_record(drive, settings.state, run_id, summary)
            _send_review(api, run_id, summary)
            _track_open(drive, settings, summary)
    if summary.aborted:
        _report_abort(run_id, summary)


def _report_abort(run_id: str, summary: ReconcileSummary) -> None:
    try:
        post_run_finding(
            FLOW_NAME,
            "ERROR",
            "library-reconcile stopped: a file's audio would not stay "
            f"byte-identical ({summary.aborted}). The file is unchanged (or "
            "restored) and is compared again first next run.",
            run_id=run_id,
        )
    except Exception:  # noqa: BLE001 — the notification is not the job
        log.exception("library-reconcile: could not report the stop")


def _open_key(diff: Difference) -> str:
    return f"{diff.drive_file_id}|{diff.field}"


def _track_open(
    drive: Any, settings: BackfillSettings, summary: ReconcileSummary
) -> None:
    """Keep when each open difference was first seen, and count those open
    longer than ``LONG_OPEN_DAYS`` by kind.

    For every file this run compared, its entries are replaced by what is
    open on it now; a difference whose values changed starts again. A run
    warns once when the count first goes above zero, and again only after
    it has been back to zero. A failure is reported, never raised."""
    store = StateFile(drive, settings.state, OPEN_NAME)
    try:
        held = store.load() or {}
        entries: dict[str, dict[str, str]] = dict(held.get("open") or {})
        compared = set(summary.reviewed)
        now = datetime.now(UTC)
        stamp = now.isoformat(timespec="seconds")
        kept = {k: v for k, v in entries.items() if k.split("|", 1)[0] not in compared}
        for diff in summary.open:
            key = _open_key(diff)
            was = entries.get(key)
            same = (
                was is not None
                and was.get("file") == diff.file_value
                and was.get("catalog") == diff.catalog_value
            )
            kept[key] = {
                "since": was["since"] if same and was else stamp,
                "kind": diff.kind,
                "path": diff.path,
                "file": diff.file_value,
                "catalog": diff.catalog_value,
            }
            tried = stamp if key in summary.tried else (was or {}).get("tried")
            if tried and (same or key in summary.tried):
                kept[key]["tried"] = tried
        cutoff = now.timestamp() - LONG_OPEN_DAYS * 86400
        long_open: dict[str, int] = {}
        for value in kept.values():
            if value.get("kind") == "fill_up" and value.get("tried"):
                # Approved and refused by the catalog: the file keeps its
                # value on purpose, so it never closes and is not "long open".
                continue
            if datetime.fromisoformat(value["since"]).timestamp() <= cutoff:
                long_open[value["kind"]] = long_open.get(value["kind"], 0) + 1
        warned = bool(held.get("warned"))
        summary.long_open = long_open
        summary.long_open_warn = bool(long_open) and not warned
        store.save(
            {
                "flow": FLOW_NAME,
                "version": 1,
                "open": kept,
                "warned": bool(long_open),
                "updated_at": stamp,
            }
        )
    except Exception as exc:  # noqa: BLE001 — the tracker is not the run
        log.exception("library-reconcile: could not track open differences")
        summary.open_problem = f"{type(exc).__name__}: {exc}"


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
        "changed_left": summary.changed_left,
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
        "owner_edits": summary.owner_edits,
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
    """One run: the mode, settings, changed files, comparison, record and report."""
    started_at = monotonic()
    run_id = run_id or local_run_id()
    summary = ReconcileSummary(mode)
    if mode is FlowMode.OFF:
        return summary
    with run_report(
        FLOW_NAME,
        repo=REPO,
        notable=False,
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
    if summary.stopped:
        report.notice("stopped", summary.stopped)
    if summary.review_problem:
        report.issue(
            "review_not_sent",
            "the differences did not reach the review page "
            f"({summary.review_problem}); the next pass sends them again",
        )
    if summary.open_problem:
        report.issue(
            "open_not_tracked",
            f"how long differences have been open was not recorded "
            f"({summary.open_problem})",
        )
    long_open = sum(summary.long_open.values())
    if summary.long_open_warn:
        kinds = ", ".join(f"{k} {n}" for k, n in sorted(summary.long_open.items()))
        report.issue(
            "long_open",
            f"{long_open} differences have been open over {LONG_OPEN_DAYS} days "
            f"({kinds}): decide them on the review page",
        )
    report.count("long_open", long_open)
    report.count("compared", summary.compared)
    report.count("changed_left", summary.changed_left)
    report.count("approved_first", summary.approved_first)
    report.count("matching", summary.matching)
    report.count("with_differences", summary.files_with_differences)
    refused = (
        {
            _open_key(d)
            for d in summary.differences["fill_up"]
            if _open_key(d) in summary.tried
        }
        if summary.tried
        else set()
    )
    refused_count = len(refused)
    for kind in KINDS:
        n = len(summary.differences[kind])
        report.count(kind, n - refused_count if kind == "fill_up" else n)
    report.count("fill_up_kept_by_file", refused_count)
    report.count("failed", len(summary.failed))
    report.count("owner_edits", len(summary.owner_edits))
    for item in summary.owner_edits:
        if summary.mode.writes:
            report.updated("owner edit linked to its own entry", item)
        else:
            report.notice("would_link_owner_edit", item)
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
        if refused:
            diffs = [d for d in diffs if _open_key(d) not in refused]
        shown = diffs if kind == "other" else diffs[:REPORT_EXAMPLES]
        for diff in shown:
            report.notice(kind, diff.example())
    for failure in summary.failed:
        report.issue("failed", failure)
    report.ok(summary.compared)
