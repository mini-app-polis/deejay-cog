"""The ``library-sync`` step of the library job: write catalog
edits and display-rule changes to the library files they affect (ADR-0008,
step 4 and decision 5, edit).

Once the library is reconciled, the catalog is where a song's owned fields
change: an edit on the WCS site, an edit made in a file (below), or a
change to the display rules. The API
lists the present, linked files whose entry or rules changed after the
catalog's values were last written to them, or that never had them written
(``GET /v1/catalog/library/files/needing-sync``). Each run this step takes
the next ``LIBRARY_SYNC_CHUNK_SIZE`` of them (default 50) and, for each:

1. reads its entry as the display rules write it out
   (``POST /v1/catalog/display``): the owned fields, the title with the
   file's own release label, artist, year, genre, BPM and label;
2. compares them with the values recorded at the file's last sync (its
   ``synced``) and writes, over the same file and in place, only the fields
   whose value moved (ADR-0008, 2026-11-09:
   the sync writes only what moved). A field that did not move is
   never written, so a value the file keeps on purpose (a BPM, year or
   spelling the owner held in the reconcile) survives an edit to another
   field of its entry. A field the catalog empties keeps the file's value;
3. records the file as synced (``POST /v1/catalog/library/files/synced``)
   with the values it now records, whether a write was needed, the file
   already said so, or nothing moved.

A file with no recorded values (never synced since the change, or
registered again by identify or the reconcile, which clears them) gets the
entry's current values recorded and nothing written: the reconcile, not the
sync, is what brings a file in step with its entry. Neither that nor a file
whose fields did not move reads the file from Drive.

**Edits made in a file go up first.** Before the catalog's changes come
down, a file changed in Drive after its last sync (the scan's listing has
when Drive last saw each file change) is read, and its owned fields are sent
to the API (``POST /v1/catalog/library/files/edited``; ADR-0008, 2026-11-09:
an edit made in VirtualDJ goes up). A field the file changed and the catalog
did not goes up to its entry, and the sync below then carries it to the
entry's other files; one the catalog changed too stays the catalog's and is
written back; a BPM a player re-measured is left; a title or artist that
does not split into the catalog's parts cleanly, and the like, is held and
named in the report. A file with no recorded values is not read. Its tags
are read from the start and end of the file (a ranged Drive download); the
whole file is downloaded only when its ID3v2 tag runs past the start read,
or when a stale field is put back.

**An owner edit is the reconcile's.** A file the owner retitled with a
version in ``< >`` ("Song <Intro Cut>") while its entry is the published
song's (or another owner edit) is not an edit to that entry: nothing of it
goes up, nothing is written over it, and it is not marked synced; the
reconcile links it to its owner-edit entry (ADR-0008, decided 2026-11-11).
An owner-edit entry's displayed title carries its version in ``< >``, and
the sync writes it so.

**A stale write-back is told by its value.** A changed field that now holds
a value the pipeline replaced in that file (the old tags of a done, in-place
write in any library run record) was written back over the pipeline's
value, not edited: it is put back with what the file held at its last sync
(its Drive revision then, never the catalog's value) and not sent up.
Every other change goes up, however many files changed. Only past the
``LIBRARY_SYNC_EDIT_FLOOD`` backstop (default 1000) is the whole change
held, nothing sent up and nothing written.

**The edits step leaves the sync step half the run.** Reading changed files
stops once ``EDITS_SHARE`` of the run's time is spent; the files not read
are counted in the report (``changed_in_drive_left``) and read on a later
run. The sync step runs on the rest, but skips those files: writing one and
marking it synced would hide its unread edit from the next run.

The write follows ADR-0007: never renamed, moved or deleted, the head
revision kept, the audio checked before and after, and every write in the
run record (``library-sync-<run id>.json``), which
``python -m deejay_cog.music_rollback <run id>`` undoes. The comment and
every frame VirtualDJ owns are never written. Genre is written; subgenre
is not, until a frame for it is decided.

**The report** says how many files each entry's change reached, so an edit
or a rule change can be followed to the files it touched.

**Roots** (ADR-0009). Each library root is synced on its own, the main
library first, from its own scan listing and its own registry rows; a root
that fails does not stop the other (its failure is raised once the other has
run). With more than one root, every line of the report names its root.
**FX Music's files win** (ADR-0010): an edit made in an FX file goes up to
its fx entry as above (an owner-edit retitle included: an FX file's title is
its own), but the catalog's values are never written over an FX file. One
whose entry moved keeps what it holds, is recorded as synced with the values
it already had recorded, and is named in the report (``fx_file_wins``); the
reconcile then sends its values up to the entry.

**Safety:** ``flags.deejay_cog.library_sync`` is ``off`` unless ``dry-run`` or
``write``. It was kept off until the library was reconciled, so the
reconcile, with the owner's approvals, decided the first writes. A dry run
reads and plans, writes no music, and records nothing as synced, so the
same files come up again. An audio mismatch stops the run with an ERROR
finding; that file is not marked synced, so it comes up again first.
"""

from __future__ import annotations

import os
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import monotonic
from typing import Any, Protocol, cast

from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import (
    CATALOG_DISPLAY_MAX_IDS,
    LIBRARY_ABSENT_MAX_FILES,
    CatalogDisplay,
    CatalogDisplayRequest,
    LibraryFile,
    LibraryFileEdited,
    LibraryFileEditResult,
    LibraryFilesSynced,
    LibraryFilesSyncedResult,
    LibraryFileTags,
)
from mini_app_polis.deadline import remaining_seconds, without_run_deadline
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI

from deejay_cog._pipeline_eval import REPO, post_run_finding, run_report
from deejay_cog._shared_deadline import shared_deadline_passed
from deejay_cog._version import __version__
from deejay_cog.api_client import api_client
from deejay_cog.library_backfill import (
    BackfillSettings,
    LibraryRoot,
    StateFile,
    StateProblem,
    each_root,
    record_roots,
)
from deejay_cog.library_scan import listing_name
from deejay_cog.music_safety import (
    AudioChanged,
    FlowMode,
    RunRecord,
    TagBytes,
    TagEmptied,
    file_meta,
    flow_mode,
    local_run_id,
    parse_record,
    read_tag_bytes,
    revision_at,
    revision_bytes,
    tag_snapshot,
    whole_bytes,
    write_in_place,
)
from deejay_cog.music_tags import (
    display_fields,
    is_owner_edit,
    needs_owner_edit_entry,
    read_tag_row,
    write_owned_fields,
)

log = logger_mod.get_logger()

FLOW_NAME = "library-sync"

#: The feature flag holding this step's mode: off | dry-run | write; a
#: missing flag or an unknown value is off.
MODE_FLAG = "flags.deejay_cog.library_sync"

#: Files written per run; a whole number of at least 1.
CHUNK_SIZE_VAR = "LIBRARY_SYNC_CHUNK_SIZE"
DEFAULT_CHUNK_SIZE = 50

#: A backstop, well above any editing session: more files changed outside
#: the pipeline than this, in one run, is a bulk write the value check below
#: did not catch: nothing goes up and nothing is written; the files keep
#: what they hold and the report says so.
EDIT_FLOOD_VAR = "LIBRARY_SYNC_EDIT_FLOOD"
DEFAULT_EDIT_FLOOD = 1000

#: The owned fields a write-back can make stale (never the catalog link).
OWNED_FIELDS = ("title", "artist", "year", "genre", "bpm", "label")

#: How a run record marks a field put back after a stale write-back.
STALE_PUT_BACK = "sync (stale write-back put back)"

#: The state folder's files that are not run records.
_STATE_SUFFIXES = (
    "-manifest.json",
    "-cursor.json",
    "-tried.json",
    "-approvals.json",
    "-listing.json",
    "-index.json",
    "-open.json",
    "-seen.json",
)

#: The state file holding the values pipeline writes replaced, by file and
#: field, and which run records it has read (by Drive modified time).
REPLACED_INDEX_NAME = "library-sync-replaced-index.json"

#: A BPM this close to the catalog's is a re-measure, not a change: left.
BPM_REMEASURE = 1.5

#: Registry rows asked for per page when looking for files changed in Drive.
REGISTRY_PAGE = 500

#: The share of the run's time the edits step may spend before it stops, so
#: the sync step always gets the rest.
EDITS_SHARE = 0.5


class Catalog(Protocol):
    """The parts of the API client the sync calls."""

    def list_library_files_needing_sync(self, **kwargs: Any) -> list[LibraryFile]:
        """One page of the files a sync has not caught up with."""
        ...

    def read_catalog_display(
        self, payload: CatalogDisplayRequest
    ) -> list[CatalogDisplay | None]:
        """Each entry's owned fields as a file should carry them."""
        ...

    def mark_library_files_synced(
        self, payload: LibraryFilesSynced
    ) -> LibraryFilesSyncedResult:
        """Record that files now carry the catalog's values."""
        ...

    def list_library_files(self, **kwargs: Any) -> list[LibraryFile]:
        """One page of registered files."""
        ...

    def report_library_file_edited(
        self, payload: LibraryFileEdited
    ) -> LibraryFileEditResult:
        """Send up the fields a file changed outside the pipeline."""
        ...


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


def edit_flood_setting() -> int | str:
    """The bulk-change limit as the worker logs it: the number, or the raw
    text when it will not parse."""
    raw = os.getenv(EDIT_FLOOD_VAR, "").strip()
    if not raw:
        return DEFAULT_EDIT_FLOOD
    try:
        return int(raw)
    except ValueError:
        return raw


@dataclass
class SyncSummary:
    """What one run wrote, or in a dry run would write, file by file."""

    mode: FlowMode
    problems: list[str] = field(default_factory=list)
    record_name: str = ""
    waiting: int = 0
    written: list[str] = field(default_factory=list)
    would_write: list[str] = field(default_factory=list)
    already: list[str] = field(default_factory=list)
    #: Files with no recorded values: recorded now, nothing written.
    recorded: list[str] = field(default_factory=list)
    #: Files whose entry changed but none of whose fields moved.
    unmoved: list[str] = field(default_factory=list)
    #: Files read because they changed in Drive after their last sync.
    edits_read: int = 0
    #: Drive ids of changed files not read: the edits step's share ran out.
    edits_left: list[str] = field(default_factory=list)
    #: Run records read for the values the pipeline replaced, and how long.
    run_records_read: int = 0
    run_records_sec: float = 0.0
    #: Run records not read yet: the edits step's share ran out first.
    run_records_left: int = 0
    #: Each field a file changed, by outcome: "path: field 'a' -> 'b'".
    edits: dict[str, list[str]] = field(default_factory=dict)
    #: Files changed outside the pipeline when that was a bulk change.
    flood: int = 0
    #: Each stale field put back (or that would be): "path: field 'a' -> 'b'".
    put_back: list[str] = field(default_factory=list)
    #: Stale fields left as they are, and why.
    stale_left: list[str] = field(default_factory=list)
    #: Files the owner retitled as an owner edit their entry is not: left
    #: for the reconcile, which links each to its owner-edit entry.
    owner_edits: list[str] = field(default_factory=list)
    #: Their Drive ids: not sent up, written or marked synced in this run.
    owner_edit_ids: set[str] = field(default_factory=set)
    synced: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    #: Files written (or that would be) per entry, as the entry is shown.
    reached: Counter[str] = field(default_factory=Counter)
    aborted: str = ""
    stopped: str = ""
    #: FX Music files whose entry moved: the file wins, nothing is written.
    fx_kept: list[str] = field(default_factory=list)
    #: With more than one root, each root's own summary, in order.
    roots: list[tuple[LibraryRoot, SyncSummary]] = field(default_factory=list)


def _settings() -> tuple[BackfillSettings | None, int, int, list[str]]:
    settings, problems = BackfillSettings.from_env()
    size = chunk_size_setting()
    if not isinstance(size, int) or size < 1:
        problems.append(
            f"{CHUNK_SIZE_VAR}={size!r} is not a whole number of at least 1"
        )
        size = 0
    flood = edit_flood_setting()
    if not isinstance(flood, int) or flood < 1:
        problems.append(
            f"{EDIT_FLOOD_VAR}={flood!r} is not a whole number of at least 1"
        )
        flood = 0
    return settings, size, flood, problems


def _displays(api: Catalog, rows: list[LibraryFile]) -> dict[uuid.UUID, CatalogDisplay]:
    ids = list(dict.fromkeys(r.catalog_id for r in rows if r.catalog_id))
    found: dict[uuid.UUID, CatalogDisplay] = {}
    for start in range(0, len(ids), CATALOG_DISPLAY_MAX_IDS):
        batch = ids[start : start + CATALOG_DISPLAY_MAX_IDS]
        for display in api.read_catalog_display(CatalogDisplayRequest(ids=batch)):
            if display is not None:
                found[display.id] = display
    return found


def _moved(now: dict[str, str | None], synced: LibraryFileTags) -> dict[str, str]:
    """The fields whose value moved since the last sync. A field the
    catalog now leaves empty is not one: the file keeps its value."""
    return {
        name: value
        for name, value in now.items()
        if value and value != getattr(synced, name)
    }


def _sync_one(
    drive: Any,
    record: RunRecord,
    row: LibraryFile,
    display: CatalogDisplay,
    summary: SyncSummary,
) -> LibraryFileTags | None:
    """Write one file's moved fields from its entry. The values to record
    when it now carries them (written, already so, or nothing moved), None
    when not. Raises :class:`AudioChanged`."""
    now = display_fields(display, label=row.edit_label)
    values = LibraryFileTags(**now)
    if row.synced is None:
        summary.recorded.append(row.path)
        return values
    moved = _moved(now, row.synced)
    if not moved:
        summary.unmoved.append(row.path)
        return values
    meta = file_meta(drive, row.drive_file_id)
    if meta.get("trashed"):
        summary.failed.append(f"{row.path} (in the trash; the scan will mark it)")
        return None
    name = str(meta.get("name") or row.path.rsplit("/", 1)[-1])
    data: bytes = drive.download_file_bytes(row.drive_file_id).data
    tagged = write_owned_fields(data, moved)
    if tagged is None:
        summary.already.append(row.path)
        return values
    entry = write_in_place(
        drive,
        record,
        file_id=row.drive_file_id,
        name=name,
        original=data,
        tagged=tagged,
        identified_by="sync (catalog wins on the fields that moved)",
        head_revision=meta.get("headRevisionId"),
    )
    summary.reached[f"{display.artist} - {display.title}"] += 1
    label = f"{row.path} ({', '.join(sorted(moved))})"
    if entry.state == "done":
        summary.written.append(label)
        return values
    summary.would_write.append(label)
    return None


def _fx_one(
    row: LibraryFile, display: CatalogDisplay, summary: SyncSummary
) -> LibraryFileTags:
    """An FX Music file: its tags win (ADR-0010), so the catalog's values
    are never written over it. The values to record: the entry's for a file
    with none recorded yet, as for any file, and otherwise those it already
    had recorded, since it still holds them; the reconcile sends its values
    up to the entry."""
    now = display_fields(display, label=row.edit_label)
    if row.synced is None:
        summary.recorded.append(row.path)
        return LibraryFileTags(**now)
    moved = _moved(now, row.synced)
    if moved:
        summary.fx_kept.append(f"{row.path} ({', '.join(sorted(moved))})")
    else:
        summary.unmoved.append(row.path)
    return row.synced


def _mark(api: Catalog, done: dict[str, LibraryFileTags]) -> None:
    when = datetime.now(UTC)
    ids = list(done)
    for start in range(0, len(ids), LIBRARY_ABSENT_MAX_FILES):
        batch = ids[start : start + LIBRARY_ABSENT_MAX_FILES]
        api.mark_library_files_synced(
            LibraryFilesSynced(
                drive_file_ids=batch,
                synced_at=when,
                values={i: done[i] for i in batch},
            )
        )


def _edits_cutoff() -> float | None:
    """The seconds left at which the edits step stops: ``EDITS_SHARE`` of
    what is left now, which the sync step then has. None with no deadline."""
    remaining = remaining_seconds()
    if remaining is None:
        return None
    return remaining * (1 - EDITS_SHARE)


def _edits_share_spent(cutoff: float | None) -> bool:
    """Whether the edits step has used its share of the run."""
    if cutoff is None:
        return False
    remaining = remaining_seconds()
    return remaining is not None and remaining <= cutoff


def _changed_since_sync(
    drive: Any, settings: BackfillSettings, root: LibraryRoot
) -> dict[str, datetime]:
    """When Drive last saw each listed file change, from the scan's listing
    of ``root``."""
    listing = StateFile(drive, settings.state, listing_name(root)).load() or {}
    changed: dict[str, datetime] = {}
    for item in listing.get("files") or []:
        when = item.get("modified")
        if when:
            changed[str(item["id"])] = datetime.fromisoformat(str(when))
    return changed


def _present_files(api: Catalog, root: str) -> list[LibraryFile]:
    """Every present registry row under ``root``."""
    rows: list[LibraryFile] = []
    while True:
        page = api.list_library_files(
            library_root_id=root, present=True, limit=REGISTRY_PAGE, offset=len(rows)
        )
        rows.extend(page)
        if len(page) < REGISTRY_PAGE:
            return rows


def _edit_line(row: LibraryFile, edit: Any) -> str:
    why = f" ({edit.why})" if edit.why else ""
    return f"{row.path}: {edit.field} {edit.synced!r} -> {edit.file!r}{why}"


def _bpm_close(a: str | None, b: str | None) -> bool:
    try:
        return abs(float(a or "") - float(b or "")) < BPM_REMEASURE
    except ValueError:
        return False


def _norm(name: str, value: str | None) -> str:
    """A field's text as compared: stripped, and a year by its first four."""
    text = (value or "").strip()
    return text[:4] if name == "year" else text


def _replaced_values(
    drive: Any,
    state: str,
    file_ids: set[str],
    summary: SyncSummary,
    cutoff: float | None = None,
) -> dict[str, dict[str, set[str]]] | None:
    """Per file and owned field, every value a pipeline write replaced in
    that file: the old tags of each in-place write the library flows' run
    records show was done. A field that now holds one of these was written
    back over the pipeline's value, not edited.

    The values are kept in ``REPLACED_INDEX_NAME`` with the Drive modified
    time each record was read at, so a run reads only the records that are
    new or changed since. The index is saved as records are read, so a run
    that runs out of its share keeps its progress and the next one goes on.
    None when not every record could be read in this run (its share or the
    run deadline ran out): no file's fields can be judged until they are."""
    store = StateFile(drive, state, REPLACED_INDEX_NAME)
    index = store.load() or {}
    seen: dict[str, Any] = dict(index.get("records") or {})
    replaced: dict[str, dict[str, list[str]]] = {
        file_id: {name: list(values) for name, values in fields.items()}
        for file_id, fields in (index.get("replaced") or {}).items()
    }
    pending = [
        f
        for f in drive.list_files(
            parent_id=state, name_contains="library-", include_folders=False
        )
        if _is_run_record(str(f.name))
        and (str(f.id) not in seen or seen[str(f.id)] != f.modified_time)
    ]
    read = 0
    try:
        for f in pending:
            if shared_deadline_passed():
                summary.stopped = "run deadline reached reading the run records"
                break
            if _edits_share_spent(cutoff):
                break
            try:
                raw = drive.download_file_bytes(str(f.id)).data
            except Exception:
                if shared_deadline_passed():
                    summary.stopped = "run deadline reached reading the run records"
                    break
                raise
            summary.run_records_read += 1
            read += 1
            seen[str(f.id)] = f.modified_time
            try:
                record = parse_record(raw)
            except ValueError:
                continue
            for file_id, name, was in _replaced_in(record):
                values = replaced.setdefault(file_id, {}).setdefault(name, [])
                if was not in values:
                    values.append(was)
    finally:
        summary.run_records_left = len(pending) - read
        if read:
            with without_run_deadline():
                store.save({"records": seen, "replaced": replaced})
    if summary.run_records_left:
        return None
    return {
        file_id: {name: set(values) for name, values in fields.items()}
        for file_id, fields in replaced.items()
        if file_id in file_ids
    }


def _is_run_record(name: str) -> bool:
    """Whether a state folder file is a library flow's (writing) run record."""
    return (
        name.startswith("library-")
        and name.endswith(".json")
        and not name.endswith(".dry-run.json")
        and not name.endswith(_STATE_SUFFIXES)
    )


def _replaced_in(record: dict[str, Any]) -> list[tuple[str, str, str]]:
    """(file id, field, old value) for each owned field a done, in-place
    write in ``record`` changed."""
    found: list[tuple[str, str, str]] = []
    for entry in record.get("entries") or []:
        file_id = str(entry.get("file_id") or "")
        if (
            not file_id
            or entry.get("action") != "in_place"
            or entry.get("state") != "done"
        ):
            continue
        old = entry.get("old_tags") or {}
        new = entry.get("new_tags") or {}
        for field_name in OWNED_FIELDS:
            was = _norm(field_name, old.get(field_name))
            if was and was != _norm(field_name, new.get(field_name)):
                found.append((file_id, field_name, was))
    return found


def _put_back_stale(
    drive: Any,
    record: RunRecord,
    row: LibraryFile,
    read: TagBytes,
    replaced: dict[str, set[str]],
    summary: SyncSummary,
) -> tuple[bytes, bool]:
    """Put back each field of ``row``'s file that now holds a value the
    pipeline replaced in it, with what the file held at its last sync (its
    Drive revision then, never the catalog's value). Returns the file's
    bytes as they are after it (the put-back's in a dry run too, so the
    edits sent up are the file's real ones), and False when a stale field
    had to be left, so the file's fields must not go up. ``read`` may hold
    only the file's start and end; a put-back is written from the whole
    file, downloaded here. Raises :class:`AudioChanged`."""
    data = read.data
    now = tag_snapshot(data)
    synced = row.synced.model_dump() if row.synced is not None else {}
    stale = sorted(
        f
        for f in OWNED_FIELDS
        if _norm(f, now.get(f)) in replaced.get(f, set())
        and _norm(f, now.get(f)) != _norm(f, synced.get(f))
    )
    if not stale:
        return data, True
    rev = (
        revision_at(drive, row.drive_file_id, row.last_synced)
        if row.last_synced is not None
        else None
    )
    if rev is None:
        summary.stale_left.append(
            f"{row.path}: {', '.join(stale)} left as they are (Drive keeps no "
            "revision from its last sync)"
        )
        return data, False
    before = tag_snapshot(revision_bytes(drive, row.drive_file_id, rev))
    back: dict[str, str] = {}
    left = False
    for f in stale:
        was = before.get(f) or ""
        if not was or _norm(f, was) in replaced.get(f, set()):
            # Empty then, or itself a value the pipeline replaced: no value
            # the file held is safe to put back.
            summary.stale_left.append(
                f"{row.path}: {f} {now.get(f)!r} left as it is (its last-sync "
                f"value {was!r} is not one to put back)"
            )
            left = True
            continue
        back[f] = was
    if not back:
        return data, not left
    data = whole_bytes(drive, row.drive_file_id, read)
    tagged = write_owned_fields(data, back)
    if tagged is None:
        return data, not left
    meta = file_meta(drive, row.drive_file_id)
    name = str(meta.get("name") or row.path.rsplit("/", 1)[-1])
    write_in_place(
        drive,
        record,
        file_id=row.drive_file_id,
        name=name,
        original=data,
        tagged=tagged,
        identified_by=STALE_PUT_BACK,
        head_revision=meta.get("headRevisionId"),
    )
    for f in sorted(back):
        summary.put_back.append(f"{row.path}: {f} {now.get(f)!r} -> {back[f]!r}")
    return tagged, not left


def _hold(api: Catalog, rows: list[LibraryFile], summary: SyncSummary) -> None:
    """A bulk change: hold it. Nothing goes up to the catalog and nothing is
    written to the files — no automatic answer can tell a stale write-back
    from the state a repair should restore, so the files keep what they hold
    and the report says so. Each is recorded as synced with the values it
    already had recorded, so the same change is reported once."""
    done = {row.drive_file_id: row.synced for row in rows if row.synced is not None}
    if summary.mode.writes and done:
        _mark(api, done)
        summary.synced = list(done)


def _edits(
    drive: Any,
    api: Catalog,
    settings: BackfillSettings,
    root: LibraryRoot,
    flood: int,
    run_id: str,
    summary: SyncSummary,
    cutoff: float | None = None,
) -> None:
    """Send up what files changed outside the pipeline since their last
    sync. A field that now holds a value the pipeline replaced in that file
    is a stale write-back, put back first and not sent up; every other
    change is an edit and goes up, however many files changed. Past the
    ``flood`` backstop, the whole change is held instead."""
    try:
        changed = _changed_since_sync(drive, settings, root)
    except StateProblem as exc:
        summary.failed.append(f"edits made in files not read: {exc}")
        return
    if not changed:
        return
    rows: list[LibraryFile] = []
    for row in _present_files(api, root.root_id):
        when = changed.get(row.drive_file_id)
        if (
            when is None
            or row.synced is None
            or row.last_synced is None
            or row.identification_status != "identified"
            or when <= row.last_synced
        ):
            continue
        rows.append(row)
    if len(rows) > flood:
        summary.flood = len(rows)
        _hold(api, rows, summary)
        return
    if not rows:
        return
    started = monotonic()
    try:
        found = _replaced_values(
            drive, settings.state, {row.drive_file_id for row in rows}, summary, cutoff
        )
    except StateProblem as exc:
        summary.failed.append(f"edits made in files not read: {exc}")
        found = None
    summary.run_records_sec = monotonic() - started
    if found is None or _edits_share_spent(cutoff):
        if not summary.stopped:
            summary.edits_left = [row.drive_file_id for row in rows]
        return
    replaced = found
    record = RunRecord(
        FLOW_NAME,
        run_id,
        summary.mode,
        stage="stale write-backs put back",
        folders={"root": root.root_id, "state": settings.state},
    ).attach(drive, settings.state)
    try:
        _read_edits(
            drive,
            api,
            record,
            rows,
            replaced,
            summary,
            cutoff,
            owner_edits=root.key != "fx",
        )
    finally:
        with without_run_deadline():
            if record.entries:
                status = (
                    "aborted"
                    if summary.aborted
                    else "stopped"
                    if summary.stopped
                    else "complete"
                )
                record.finish(status)
                summary.record_name = record.file_name


def _read_edits(
    drive: Any,
    api: Catalog,
    record: RunRecord,
    rows: list[LibraryFile],
    replaced: dict[str, dict[str, set[str]]],
    summary: SyncSummary,
    cutoff: float | None,
    *,
    owner_edits: bool = True,
) -> None:
    """Read each changed file and send its edits up. With ``owner_edits``
    off (FX Music, whose files win), a retitle as an owner edit goes up
    like any other edit."""
    for index, row in enumerate(rows):
        if shared_deadline_passed():
            summary.stopped = f"run deadline reached before reading {row.path}"
            return
        if _edits_share_spent(cutoff):
            summary.edits_left = [r.drive_file_id for r in rows[index:]]
            return
        summary.edits_read += 1
        try:
            read = read_tag_bytes(drive, row.drive_file_id)
            data, clean = _put_back_stale(
                drive, record, row, read, replaced.get(row.drive_file_id, {}), summary
            )
            if not clean:
                # A stale field that could not be put back must not go up.
                continue
            tags = {k: v or None for k, v in read_tag_row(data).items()}
            if owner_edits and _retitled_as_owner_edit(
                api, row, tags.get("title") or ""
            ):
                summary.owner_edits.append(
                    f"{row.path}: title {tags.get('title')!r} (an owner edit: "
                    "the reconcile links it to its own entry)"
                )
                summary.owner_edit_ids.add(row.drive_file_id)
                continue
            result = api.report_library_file_edited(
                LibraryFileEdited(
                    drive_file_id=row.drive_file_id,
                    tags=LibraryFileTags(**tags),
                    preview=not summary.mode.writes,
                )
            )
        except AudioChanged as exc:
            summary.aborted = f"{row.path}: {exc}"
            return
        except Exception as exc:  # noqa: BLE001 — one file must not end the run
            if shared_deadline_passed():
                summary.stopped = f"run deadline reached reading {row.path}"
                return
            log.exception("library-sync: reading edits in %s failed", row.path)
            summary.failed.append(
                f"{row.path} (edits not read: {type(exc).__name__}: {exc})"
            )
            continue
        for edit in result.fields:
            summary.edits.setdefault(edit.outcome, []).append(_edit_line(row, edit))


def _retitled_as_owner_edit(api: Catalog, row: LibraryFile, title: str) -> bool:
    """Whether the file's title now names an owner edit its entry is not.
    That retitle is the owner's choice of another entry, not an edit to this
    one, so it does not go up, and the catalog's title is not written over
    it (ADR-0008, decided 2026-11-11)."""
    if not is_owner_edit(title) or row.catalog_id is None:
        return False
    [display] = api.read_catalog_display(CatalogDisplayRequest(ids=[row.catalog_id]))
    return display is not None and needs_owner_edit_entry(title, display)


def _sync(
    drive: Any,
    api: Catalog,
    settings: BackfillSettings,
    root: LibraryRoot,
    chunk_size: int,
    flood: int,
    run_id: str,
    summary: SyncSummary,
) -> None:
    _edits(drive, api, settings, root, flood, run_id, summary, _edits_cutoff())
    if summary.stopped or summary.aborted:
        if summary.aborted:
            _report_abort(run_id, summary)
        return
    rows = api.list_library_files_needing_sync(
        library_root_id=root.root_id, limit=chunk_size, offset=0
    )
    left = set(summary.edits_left) | summary.owner_edit_ids
    rows = [row for row in rows if row.drive_file_id not in left]
    summary.waiting = len(rows)
    if not rows:
        return
    displays = _displays(api, rows)
    record = RunRecord(
        FLOW_NAME,
        run_id,
        summary.mode,
        stage=f"{len(rows)} files needing a sync",
        folders={"root": root.root_id, "state": settings.state},
    ).attach(drive, settings.state)
    summary.record_name = record.file_name
    done: dict[str, LibraryFileTags] = {}
    status = "complete"
    try:
        for row in rows:
            if shared_deadline_passed():
                summary.stopped = f"run deadline reached before {row.path}"
                status = "stopped"
                break
            display = displays.get(row.catalog_id) if row.catalog_id else None
            if display is None:
                summary.failed.append(
                    f"{row.path} (its catalog entry {row.catalog_id} is gone)"
                )
                continue
            try:
                values = (
                    _fx_one(row, display, summary)
                    if root.key == "fx"
                    else _sync_one(drive, record, row, display, summary)
                )
                if values is not None:
                    done[row.drive_file_id] = values
            except AudioChanged as exc:
                summary.aborted = f"{row.path}: {exc}"
                status = "aborted"
                break
            except TagEmptied as exc:
                summary.failed.append(f"{row.path} ({exc})")
            except Exception as exc:  # noqa: BLE001 — one file must not end the run
                if shared_deadline_passed():
                    summary.stopped = f"run deadline reached reading {row.path}"
                    status = "stopped"
                    break
                log.exception("library-sync: failed on %s", row.path)
                summary.failed.append(f"{row.path} ({type(exc).__name__}: {exc})")
    finally:
        with without_run_deadline():
            if record.entries:
                record.finish(status)
            if summary.mode.writes and done:
                _mark(api, done)
                summary.synced = list(done)
    if summary.aborted:
        _report_abort(run_id, summary)


def _report_abort(run_id: str, summary: SyncSummary) -> None:
    try:
        post_run_finding(
            FLOW_NAME,
            "ERROR",
            "library-sync stopped: a file's audio would not stay "
            f"byte-identical ({summary.aborted}). The file is unchanged (or "
            "restored) and not marked synced, so it comes up again first.",
            run_id=run_id,
        )
    except Exception:  # noqa: BLE001 — the notification is not the job
        log.exception("library-sync: could not report the stop")


def run_sync(
    *,
    run_id: str | None,
    mode: FlowMode,
    drive: Any | None = None,
    api: Catalog | None = None,
) -> SyncSummary:
    """One run: the mode, settings, the next chunk, the record and report."""
    started_at = monotonic()
    run_id = run_id or local_run_id()
    summary = SyncSummary(mode)
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
            settings, chunk_size, flood, problems = _settings()
            if settings is None or problems:
                summary.problems.extend(problems)
                return summary
            if drive is None:
                drive = GoogleAPI.from_env().drive
            client = api or cast(
                Catalog, api_client(base_url=api_base_url().strip() or None)
            )
            if len(settings.roots) == 1:
                root = settings.roots[0]
                _sync(drive, client, settings, root, chunk_size, flood, run_id, summary)
            else:
                _sync_roots(drive, client, settings, chunk_size, flood, run_id, summary)
        finally:
            if summary.roots:
                record_roots(report, summary.roots, _record, monotonic() - started_at)
            else:
                _record(report, summary, monotonic() - started_at)
    return summary


def _sync_roots(
    drive: Any,
    api: Catalog,
    settings: BackfillSettings,
    chunk_size: int,
    flood: int,
    run_id: str,
    summary: SyncSummary,
) -> None:
    """Each root on its own, the main library first (ADR-0009). An audio
    mismatch or the run's deadline stops the run, whatever the root."""

    def run(root: LibraryRoot) -> bool:
        part = SyncSummary(summary.mode)
        summary.roots.append((root, part))
        _sync(drive, api, settings, root, chunk_size, flood, run_id, part)
        return not (part.aborted or part.stopped)

    def failed(_root: LibraryRoot, exc: Exception) -> None:
        summary.roots[-1][1].failed.append(
            f"the run over this root failed ({type(exc).__name__}: {exc})"
        )

    each_root(settings.roots, run, failed)


def library_sync(*, run_id: str | None = None) -> SyncSummary:
    """Write catalog changes to the next chunk of linked files and report."""
    return run_sync(run_id=run_id, mode=flow_mode(MODE_FLAG))


def _record(report: Any, summary: SyncSummary, duration_sec: float) -> None:
    report.duration_sec = duration_sec
    for problem in summary.problems:
        report.issue("setting", problem)
    if summary.problems:
        return
    if summary.mode is FlowMode.DRY_RUN:
        report.notice(
            "dry_run",
            f"{MODE_FLAG} is dry-run: no music was written and nothing was "
            f"marked synced; record {summary.record_name or '(none)'}",
        )
    if summary.stopped:
        report.notice("stopped", summary.stopped)
    report.count("waiting", summary.waiting)
    report.count("written", len(summary.written))
    report.count("would_write", len(summary.would_write))
    report.count("already_in_step", len(summary.already))
    report.count("nothing_moved", len(summary.unmoved))
    report.count("values_recorded", len(summary.recorded))
    report.count("changed_in_drive_read", summary.edits_read)
    report.count("changed_in_drive_left", len(summary.edits_left))
    report.count("run_records_read", summary.run_records_read)
    report.count("run_records_sec", round(summary.run_records_sec))
    report.count("run_records_left", summary.run_records_left)
    if summary.run_records_left:
        report.notice(
            "run_records_left",
            f"{summary.run_records_left} run records not read yet for the values "
            "the pipeline replaced; what was read is kept, and the next run "
            "reads the rest before any file's edits are judged.",
        )
    if summary.edits_left:
        report.notice(
            "changed_in_drive_left",
            f"{len(summary.edits_left)} files changed in Drive were not read: the "
            f"edits step stopped at {EDITS_SHARE:.0%} of the run so the sync step "
            "could run. A later run reads them; they were not synced in this one.",
        )
    if summary.flood:
        report.issue(
            "bulk_change_held",
            f"{summary.flood} files changed outside the pipeline since their last "
            f"sync, more than the {EDIT_FLOOD_VAR} backstop allows: nothing was "
            "sent up to the catalog and nothing was written; the files keep what "
            "they hold. If the change was a stale write-back, repair it with "
            "deejay_cog.library_restore.",
        )
    report.count("stale_fields_put_back", len(summary.put_back))
    for line in summary.put_back:
        if summary.mode.writes:
            report.updated("stale write-back put back", line)
        else:
            report.notice("would_put_back", line)
    for line in summary.stale_left:
        report.issue("stale_write_back_left", line)
    for outcome, lines in sorted(summary.edits.items()):
        report.count(f"edits_{outcome}", len(lines))
    for line in summary.edits.get("sent_up", []):
        if summary.mode.writes:
            report.updated("edit made in a file sent up to the catalog", line)
        else:
            report.notice("would_send_up", line)
    for line in summary.edits.get("overwritten", []):
        report.notice("edit_overwritten_by_the_catalog", line)
    for line in summary.edits.get("held", []):
        report.notice("edit_held", line)
    report.count("owner_edits_left", len(summary.owner_edits))
    for line in summary.owner_edits:
        report.notice("owner_edit_left_for_reconcile", line)
    if summary.fx_kept:
        report.count("fx_file_wins", len(summary.fx_kept))
    for line in summary.fx_kept:
        report.notice(
            "fx_file_wins",
            f"{line}: its entry moved; the FX file keeps its own values",
        )
    report.count("marked_synced", len(summary.synced))
    report.count("failed", len(summary.failed))
    for shown, files in sorted(summary.reached.items()):
        noun = "file" if files == 1 else "files"
        if summary.mode.writes:
            report.updated("entry written to files", f"{shown}: {files} {noun}")
        else:
            report.notice("would_reach", f"{shown}: {files} {noun}")
    for path in summary.written:
        report.created("tags written", path)
    for path in summary.would_write:
        report.notice("would_write", path)
    for item in summary.failed:
        report.issue("failed", item)
    if summary.aborted:
        report.issue("audio_changed_run_stopped", summary.aborted)
    report.ok(len(summary.written))
