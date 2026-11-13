"""The ``library-backfill`` worker mode: retag the music library from the
catalog, in place, a chunk a run (ADR-0007, amended 2026-11-03).

The library under ``LIBRARY_ROOT_FOLDER_ID`` is what VirtualDJ plays from,
by path. So a library file is **never renamed, moved or deleted**: only its
tags are rewritten from the catalog, over the same Drive file (same name,
folder and file id), through :func:`deejay_cog.music_safety.write_in_place`.

**Position.** The library is thousands of files, so it is never rescanned to
find where to carry on. Two JSON files in ``MUSIC_TAGGING_STATE_FOLDER_ID``
hold the position:

- ``library-backfill-manifest.json`` — the library listed once, recursively,
  metadata only (no downloads): every file's id, name and folder, in a fixed
  order (breadth first from the root; within a folder by name, then id).
  The listing is resumable: the folders still to list are kept in the
  manifest, saved every few folders and when the run stops, so a listing cut
  off by the deadline carries on where it stopped. Until it is complete no
  file is processed. Built in ``dry-run`` too: listing reads nothing but
  metadata, and only the state folder is written.
- ``library-backfill-cursor.json`` — how far through that manifest the
  backfill has got (the next file's index), tied to the manifest it counts
  through by the manifest's ``generation``. Saved only by a ``write`` run,
  after each file's outcome is in the run record, so the cursor never moves
  past a file whose outcome is not recorded.

Each run takes the next ``LIBRARY_BACKFILL_CHUNK_SIZE`` files (default 10)
after the cursor, and starts no file with less than
``LIBRARY_BACKFILL_RESERVE_SECONDS`` (default 120) left before the run
deadline, so the library job's steps after it still run; the next run carries on
from the cursor. The record and cursor are saved after each file even past
the deadline, so a file written is never done twice. A redelivery of a run stopped by the deadline (the same
queue message id) finishes that run's chunk rather than start a new one.
Once the cursor reaches the end, every run reports the library done and does
nothing more. **To start over** (files added to the library since, or a new
root), move ``library-backfill-manifest.json`` to the Drive trash: the next
run lists the library again and, the generation having changed, starts from
the first file.

**Roots** (ADR-0009). Each library root has a manifest and cursor of its own
and is run on its own, the main library first: the main library's are the
names above (its own folder and, when set, New Music, under the main root's
id), and FX Music's (``FX_LIBRARY_ROOT_FOLDER_ID``) are
``library-backfill-fx-manifest.json`` and ``library-backfill-fx-cursor.json``.
An FX Music file links only to an fx entry, found by its own tags
(``POST /v1/catalog/library/entries`` with library ``fx`` and ``preview``,
so the backfill never makes one: identify does), and, as for every file,
only the link is written, never a catalog value (ADR-0010: the file wins).
A root whose run fails, or whose state is refused, does not stop or change
the other; the failure is raised once the other has run. An audio mismatch
stops the run, whatever the root. With more than one root, every line of
the report names its root.

**Per file:** not an MP3 (by its listed type or name) → skipped, named; in
FX Music, not audio in a format the shared tagger supports (ADR-0010
decision 2: FLAC, Ogg Vorbis, Ogg Opus, M4A, WAV and AIFF as well), or a
partial download. No
longer in the folder the manifest has it in, or in the trash → skipped,
named. No usable title or artist → skipped and named as needing
fingerprinting (library files are **not** sent to the fingerprint function:
that writes MusicBrainz tags). Not in the catalog → skipped, named; so is
an owner edit (a version in ``< >`` in its title) the catalog has no
owner-edit entry for, which identify then takes in. In the
catalog and already carrying its tags → nothing written. Otherwise written
in place, writing only ``TXXX:CATALOG_ID`` (no other frame changes, even an
empty field), with the head revision kept forever, the audio checked before
and after, and the original restored if the check after fails.

**Registry:** every MP3 the run reads, in ``dry-run`` and ``write``, is
registered in the API's library registry (``POST /v1/catalog/library/files``):
its Drive id, library root, path, size, md5, duration, the catalog entry it
is linked to (identified) or none (unidentified), the release label its title
carries, and its comment frame as a backup. Registering never changes a music
file, and a re-run updates the same row. A registration that fails does not
stop the run: the file is named in the report and, in a ``write`` run, kept in
the cursor's ``registry_retry`` and registered again at the start of the next.

**Safety:** ``flags.deejay_cog.library_backfill`` is ``off`` unless ``dry-run`` or
``write``. A dry run writes nothing to any music file, does not move the
cursor (so the same chunk can be reviewed and then written) and saves the
would-be run record. An audio mismatch stops the run with an ERROR finding
and leaves the cursor **on** that file: the next run tries it again first
(a mismatch found before the write writes nothing), so the backfill does not
pass a file it could not safely write until the owner has looked. To pass
one, put its id in ``LIBRARY_BACKFILL_SKIP_FILE_IDS``. Every write run's
record is undone by ``python -m deejay_cog.music_rollback <run id>``.
"""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime
from time import monotonic
from typing import Any, Protocol

from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import (
    CatalogLookupRequest,
    CatalogMatch,
    LibraryEntryCreate,
    LibraryEntryCreated,
    LibraryFile,
    LibraryFileRegister,
)
from mini_app_polis.deadline import (
    RunDeadlineReached,
    remaining_seconds,
    without_run_deadline,
)
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI
from mini_app_polis.lambda_worker import RunOutOfTime
from mini_app_polis.mp3.tag import MAIN as MAIN_PROFILE
from mini_app_polis.mp3.tag import RuleProfile, Winner, get_profile
from mini_app_polis.mp3.tag.audio_tagger import PARTIAL_DOWNLOAD_SUFFIXES

from deejay_cog._pipeline_eval import REPO, post_run_finding, run_report
from deejay_cog._shared_deadline import shared_deadline_passed
from deejay_cog._version import __version__
from deejay_cog.api_client import api_client
from deejay_cog.music_safety import (
    MP3_MIME_TYPE,
    RECORD_MIME_TYPE,
    STATE_FOLDER_VAR,
    AudioChanged,
    FlowMode,
    RecordEntry,
    RunRecord,
    TagEmptied,
    check_audio_unchanged,
    file_meta,
    flow_mode,
    local_run_id,
    md5,
    tag_snapshot,
    update_contents,
    write_in_place,
)
from deejay_cog.music_tags import (
    CatalogLookup,
    comment_text,
    duration_secs,
    edit_label,
    ingest_track,
    linked_catalog_id,
    owner_edit_fits,
    read_tag_row,
    write_catalog_tags,
)

log = logger_mod.get_logger()

FLOW_NAME = "library-backfill"

#: The feature flag holding this step's mode: off | dry-run | write; a
#: missing flag or an unknown value is off.
MODE_FLAG = "flags.deejay_cog.library_backfill"

#: The library's root folder, walked recursively.
ROOT_FOLDER_VAR = "LIBRARY_ROOT_FOLDER_ID"

#: New Music, the folder new-music intake writes to: listed with the main
#: library, under its root (ADR-0009).
NEW_MUSIC_FOLDER_VAR = "NEW_MUSIC_DESTINATION_FOLDER_ID"

#: FX Music, a library root of its own when set (ADR-0009).
FX_ROOT_FOLDER_VAR = "FX_LIBRARY_ROOT_FOLDER_ID"

#: The path New Music's files are registered under, in the main library.
#: A path in the main folder never starts with "/": a top-level file's path
#: is its bare name, and a name holding "/" can't sync to a local disk.
NEW_MUSIC_PATH_PREFIX = "/New Music"

#: Files per run; a whole number of at least 1.
CHUNK_SIZE_VAR = "LIBRARY_BACKFILL_CHUNK_SIZE"
DEFAULT_CHUNK_SIZE = 10

#: Comma-separated Drive file ids the backfill passes over (named in the
#: report): the way past a file whose audio check fails.
SKIP_IDS_VAR = "LIBRARY_BACKFILL_SKIP_FILE_IDS"

#: Seconds of the run the backfill leaves for the library job's steps after it:
#: it starts no file with less than this left before the run deadline.
RESERVE_SECONDS_VAR = "LIBRARY_BACKFILL_RESERVE_SECONDS"
DEFAULT_RESERVE_SECONDS = 120

MANIFEST_NAME = f"{FLOW_NAME}-manifest.json"
CURSOR_NAME = f"{FLOW_NAME}-cursor.json"


def root_state_name(flow: str, name: str, root: LibraryRoot) -> str:
    """A step's state file ``name`` (it starts with ``flow``) for ``root``:
    the name itself for the main library, and the root's key after the
    flow's name for any other ("library-backfill-manifest.json" ->
    "library-backfill-fx-manifest.json")."""
    if root.key == "main":
        return name
    return f"{flow}-{root.key}{name[len(flow) :]}"


FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
SHORTCUT_MIME_TYPE = "application/vnd.google-apps.shortcut"

#: How many folders are listed between saves of a manifest in progress.
LISTING_SAVE_EVERY = 25

#: The outcomes a file can have, as the report and the cursor count them.
OUTCOMES = (
    "written",
    "would_write",
    "already_tagged",
    "not_in_catalog",
    "needs_fingerprinting",
    "not_mp3",
    "moved_since",
    "skipped_by_setting",
    "failed",
)


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LibraryFolder:
    """One Drive folder a library root covers, listed recursively."""

    folder_id: str
    #: The setting naming it.
    setting: str
    #: The path its files are registered under; "" for the root's own folder.
    path: str = ""


#: Each library root's tagging rule profile, by the root's key (deejay-cog
#: ADR-0010 decision 1): the main library (its folder and New Music) follows
#: ``main``, the catalog winning; FX Music follows ``fx``, the file winning.
#: The profiles themselves live in common-python-utils.
ROOT_PROFILES: dict[str, str] = {"main": "main", "fx": "fx"}

#: Each library root's name in a report a person reads, by the root's key.
ROOT_NAMES: dict[str, str] = {"main": "Main library", "fx": "FX Music"}


def root_profile(key: str) -> RuleProfile:
    """The rule profile the root called ``key`` follows."""
    return get_profile(ROOT_PROFILES[key])


@dataclass(frozen=True)
class LibraryRoot:
    """One library: its ``library_root_id``, the folders listed under it, and
    the tagging rule profile its files follow (ADR-0010 decision 1)."""

    #: "main" or "fx".
    key: str
    root_id: str
    folders: tuple[LibraryFolder, ...]
    profile: RuleProfile = MAIN_PROFILE

    @property
    def file_wins(self) -> bool:
        """Whether the file's values stand over the catalog's (the fx
        profile): its values go up, and the catalog's are never written over
        it."""
        return self.profile.winner is Winner.FILE

    @property
    def every_format(self) -> bool:
        """Whether its files are tagged in every format the shared tagger
        supports, not MP3 only: the main profile stays MP3-only (ADR-0010
        decision 2)."""
        return self.profile.name != MAIN_PROFILE.name

    def lists(self, item: dict[str, Any]) -> bool:
        """Whether a listed file is one of this root's files to tag: an MP3
        for the main library, audio in any supported format for the others;
        never a partial download."""
        if self.every_format:
            return is_audio_listing(item)
        return is_mp3_listing(item)

    def untaggable(self, item: dict[str, Any]) -> bool:
        """Whether a listed file is audio this root cannot tag, to report: a
        format the shared tagger does not support. The main library reports
        none; partial downloads and files that are not audio are ignored."""
        if not self.every_format or self.lists(item) or _is_partial(item):
            return False
        name = str(item.get("name", "")).lower()
        return str(item.get("mime_type", "")).startswith("audio/") or name.endswith(
            OTHER_AUDIO_SUFFIXES
        )

    @property
    def folder_ids(self) -> list[str]:
        """Its folders' ids, the root's own first."""
        return [folder.folder_id for folder in self.folders]

    @property
    def label(self) -> str:
        """The settings naming its folders, for a report."""
        return " and ".join(folder.setting for folder in self.folders)

    @property
    def name(self) -> str:
        """What the owner calls it, for a report a person reads."""
        return ROOT_NAMES.get(self.key, self.key)


@dataclass(frozen=True)
class BackfillSettings:
    """The backfill's settings, read from the environment at the start of a run."""

    root: str
    state: str
    chunk_size: int
    skip_ids: frozenset[str]
    reserve_seconds: int = DEFAULT_RESERVE_SECONDS
    #: New Music, listed with the main library; "" when unset.
    new_music: str = ""
    #: FX Music's root; "" when unset.
    fx_root: str = ""

    @property
    def roots(self) -> tuple[LibraryRoot, ...]:
        """The library roots: the main library (its folder, and New Music
        when set), then FX Music when set."""
        main = [LibraryFolder(self.root, ROOT_FOLDER_VAR)]
        if self.new_music:
            main.append(
                LibraryFolder(
                    self.new_music, NEW_MUSIC_FOLDER_VAR, NEW_MUSIC_PATH_PREFIX
                )
            )
        roots = [LibraryRoot("main", self.root, tuple(main), root_profile("main"))]
        if self.fx_root:
            fx = LibraryFolder(self.fx_root, FX_ROOT_FOLDER_VAR)
            roots.append(LibraryRoot("fx", self.fx_root, (fx,), root_profile("fx")))
        return tuple(roots)

    @classmethod
    def from_env(cls) -> tuple[BackfillSettings | None, list[str]]:
        """The settings, or None and why not."""
        root = os.getenv(ROOT_FOLDER_VAR, "").strip()
        state = os.getenv(STATE_FOLDER_VAR, "").strip()
        problems = [
            f"{var} is not set"
            for var, value in ((ROOT_FOLDER_VAR, root), (STATE_FOLDER_VAR, state))
            if not value
        ]
        if root and root == state:
            problems.append(
                f"{ROOT_FOLDER_VAR} and {STATE_FOLDER_VAR} must be different folders"
            )
        new_music = os.getenv(NEW_MUSIC_FOLDER_VAR, "").strip()
        fx_root = os.getenv(FX_ROOT_FOLDER_VAR, "").strip()
        named = [(ROOT_FOLDER_VAR, root), (STATE_FOLDER_VAR, state)]
        for var, value in (
            (NEW_MUSIC_FOLDER_VAR, new_music),
            (FX_ROOT_FOLDER_VAR, fx_root),
        ):
            if not value:
                continue
            for other, other_value in named:
                if value == other_value:
                    problems.append(f"{var} and {other} must be different folders")
            named.append((var, value))
        raw = os.getenv(CHUNK_SIZE_VAR, "").strip()
        chunk_size = DEFAULT_CHUNK_SIZE
        if raw:
            try:
                chunk_size = int(raw)
            except ValueError:
                chunk_size = 0
            if chunk_size < 1:
                problems.append(
                    f"{CHUNK_SIZE_VAR}={raw!r} is not a whole number of at least 1"
                )
        skip_ids = frozenset(
            part.strip()
            for part in os.getenv(SKIP_IDS_VAR, "").split(",")
            if part.strip()
        )
        raw = os.getenv(RESERVE_SECONDS_VAR, "").strip()
        reserve = DEFAULT_RESERVE_SECONDS
        if raw:
            try:
                reserve = int(raw)
            except ValueError:
                reserve = -1
            if reserve < 0:
                problems.append(
                    f"{RESERVE_SECONDS_VAR}={raw!r} is not a whole number of seconds"
                )
        if problems:
            return None, problems
        return cls(
            root,
            state,
            chunk_size,
            skip_ids,
            reserve,
            new_music=new_music,
            fx_root=fx_root,
        ), []


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
# More than one root
# ---------------------------------------------------------------------------


def each_root(
    roots: tuple[LibraryRoot, ...],
    run: Callable[[LibraryRoot], bool],
    on_error: Callable[[LibraryRoot, Exception], None],
) -> None:
    """Run ``run`` for every root, in order, until one returns False (an
    audio mismatch: the run stops). With more than one root, a root that
    raises is handed to ``on_error`` and the next root still runs; the
    first such failure is raised once every root has run, so the run still
    fails. The run's deadline is raised at once. With one root, nothing is
    caught."""
    first: Exception | None = None
    for root in roots:
        try:
            if not run(root):
                break
        except (RunOutOfTime, RunDeadlineReached):
            raise
        except Exception as exc:
            if len(roots) == 1:
                raise
            log.exception("%s failed; the other library roots carry on", root.label)
            on_error(root, exc)
            first = first or exc
    if first is not None:
        raise first


class NamedReport:
    """A run report for one of several library roots: every line it is
    given names the root (``named``, "<the root's settings>: "), and its
    counts are added up across the roots in ``totals``, to be set on the
    report once every root is recorded (:func:`set_totals`)."""

    def __init__(self, report: Any, named: str, totals: dict[str, Any]) -> None:
        self.report = report
        self.named = named
        self.totals = totals
        self.duration_sec: float = 0.0

    def _item(self, item: str | None) -> str | None:
        return item if item is None else f"{self.named}{item}"

    def count(self, key: str, value: Any) -> None:
        """Add ``value`` to the run's count of ``key``."""
        self.totals[key] = self.totals.get(key, 0) + value

    def ok(self, n: int = 1) -> None:
        """Items handled cleanly, for the whole run."""
        self.report.ok(n)

    def notice(self, reason: str, item: str | None = None) -> None:
        """A notice naming the root."""
        self.report.notice(reason, self._item(item))

    def issue(self, reason: str, item: str | None = None, **kwargs: Any) -> None:
        """An issue naming the root."""
        self.report.issue(reason, self._item(item), **kwargs)

    def created(self, kind: str, item: str | None = None, **kwargs: Any) -> None:
        """Something created, naming the root."""
        self.report.created(kind, self._item(item), **kwargs)

    def updated(self, kind: str, item: str | None = None, **kwargs: Any) -> None:
        """Something updated, naming the root."""
        self.report.updated(kind, self._item(item), **kwargs)

    def removed(self, kind: str, item: str | None = None, **kwargs: Any) -> None:
        """Something removed, naming the root."""
        self.report.removed(kind, self._item(item), **kwargs)


def set_totals(report: Any, totals: dict[str, Any]) -> None:
    """Set the counts added up across the roots on the run's report."""
    for key, value in totals.items():
        report.count(key, round(value, 1) if isinstance(value, float) else value)


def record_roots(
    report: Any,
    roots: list[tuple[LibraryRoot, Any]],
    record: Callable[[Any, Any, float], None],
    duration_sec: float,
) -> None:
    """Record each root's summary with ``record`` (a step's own report
    writer), every line naming its root as the owner calls it (Main
    library, FX Music) and the counts added up."""
    report.duration_sec = duration_sec
    totals: dict[str, Any] = {}
    for root, part in roots:
        record(NamedReport(report, f"{root.name}: ", totals), part, duration_sec)
    set_totals(report, totals)


# ---------------------------------------------------------------------------
# State files in the state folder
# ---------------------------------------------------------------------------


class LibraryRegistry(Protocol):
    """The part of the API client the registry calls use."""

    def register_library_file(self, payload: LibraryFileRegister) -> LibraryFile:
        """Register a library file, or bring its row up to date."""
        ...


class LibraryEntryCreator(Protocol):
    """The part of the API client that finds (or makes) a library entry."""

    def create_library_entry(self, payload: LibraryEntryCreate) -> LibraryEntryCreated:
        """The song's entry in the library sent; with preview, nothing is made."""
        ...


class StateProblem(Exception):  # noqa: N818 — named for what it is
    """The state folder holds something the backfill will not guess about."""


@dataclass
class StateFile:
    """One JSON file in the state folder: found by exact name, created on
    the first save, written over in place after that."""

    drive: Any
    folder: str
    name: str
    file_id: str | None = None

    def load(self) -> dict[str, Any] | None:
        """Read the state file, or None when the state folder has none."""
        files = self.drive.list_files(
            parent_id=self.folder, name_contains=self.name, include_folders=False
        )
        matches = [f for f in files if str(f.name) == self.name]
        if len(matches) > 1:
            raise StateProblem(
                f"{len(matches)} files named {self.name} in the state folder; "
                "trash all but one"
            )
        if not matches:
            return None
        self.file_id = str(matches[0].id)
        raw = self.drive.download_file_bytes(self.file_id).data
        try:
            data = json.loads(bytes(raw).decode("utf-8"))
        except ValueError as exc:
            raise StateProblem(f"{self.name} is not valid JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise StateProblem(f"{self.name} is not a JSON object")
        return data

    def save(self, data: dict[str, Any]) -> None:
        """Write ``data`` to the state file, creating it on the first save."""
        content = json.dumps(data, indent=1).encode()
        if self.file_id is None:
            self.file_id = self.drive.upload_bytes(
                parent_id=self.folder,
                filename=self.name,
                content=content,
                mime_type=RECORD_MIME_TYPE,
            )
            return
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as handle:
            handle.write(content)
            path = handle.name
        try:
            self.drive.update_file(self.file_id, path, mime_type=RECORD_MIME_TYPE)
        finally:
            os.unlink(path)


def new_manifest(root: str) -> dict[str, Any]:
    """A manifest about to be listed: only the root is queued."""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return {
        "flow": FLOW_NAME,
        "version": 1,
        "generation": f"{stamp}-{os.urandom(3).hex()}",
        "root_folder_id": root,
        "started_at": _now(),
        "completed_at": None,
        "complete": False,
        "pending_folders": [{"id": root, "path": ""}],
        "visited_folders": [],
        "folders": {root: ""},
        "shortcuts_skipped": 0,
        "files": [],
    }


def new_root_manifest(root: LibraryRoot) -> dict[str, Any]:
    """A manifest of ``root`` about to be listed: each of its folders
    queued at the path its files are registered under."""
    manifest = new_manifest(root.root_id)
    for folder in root.folders[1:]:
        manifest["pending_folders"].append(
            {"id": folder.folder_id, "path": folder.path}
        )
        manifest["folders"][folder.folder_id] = folder.path
    return manifest


def _sort_key(item: Any) -> tuple[str, str]:
    return (str(getattr(item, "name", "")).casefold(), str(getattr(item, "id", "")))


def list_one_folder(drive: Any, manifest: dict[str, Any], *, exclude: set[str]) -> None:
    """List the next queued folder into ``manifest``: its files appended in
    name order, its subfolders queued in name order. Only metadata is read."""
    folder = manifest["pending_folders"][0]
    folder_id, path = str(folder["id"]), str(folder["path"])
    children = sorted(
        drive.list_files(parent_id=folder_id, trashed=False, include_folders=True),
        key=_sort_key,
    )
    visited = set(manifest["visited_folders"])
    queued = {str(f["id"]) for f in manifest["pending_folders"]}
    for child in children:
        child_id = str(child.id)
        mime_type = str(getattr(child, "mime_type", "") or "")
        if mime_type == FOLDER_MIME_TYPE:
            if child_id in visited or child_id in queued or child_id in exclude:
                continue
            child_path = f"{path}/{child.name}" if path else str(child.name)
            manifest["pending_folders"].append({"id": child_id, "path": child_path})
            manifest["folders"][child_id] = child_path
            queued.add(child_id)
        elif mime_type == SHORTCUT_MIME_TYPE:
            # A shortcut points elsewhere, maybe outside the library.
            manifest["shortcuts_skipped"] += 1
        else:
            item = {
                "id": child_id,
                "name": str(child.name),
                "folder_id": folder_id,
                "mime_type": mime_type,
            }
            modified = getattr(child, "modified_time", None)
            if modified:
                # When Drive last saw it change: the sync reads a file
                # changed after its last sync for edits made in it.
                item["modified"] = str(modified)
            manifest["files"].append(item)
    manifest["pending_folders"].pop(0)
    manifest["visited_folders"].append(folder_id)


def build_manifest(
    drive: Any, store: StateFile, manifest: dict[str, Any], *, exclude: set[str]
) -> None:
    """List folders until none are left, saving every few and at the end.

    Past the deadline it saves what it has and raises ``RunOutOfTime``; the
    next run carries on from the saved queue.
    """
    listed = 0
    while manifest["pending_folders"]:
        if shared_deadline_passed():
            with without_run_deadline():
                store.save(manifest)
            raise RunOutOfTime(
                f"run deadline reached listing the library "
                f"({len(manifest['files'])} files so far)"
            )
        list_one_folder(drive, manifest, exclude=exclude)
        listed += 1
        if listed % LISTING_SAVE_EVERY == 0 and manifest["pending_folders"]:
            store.save(manifest)
    manifest["complete"] = True
    manifest["completed_at"] = _now()
    store.save(manifest)


def new_cursor(generation: str, total: int) -> dict[str, Any]:
    """A cursor at the first file of the manifest named by ``generation``."""
    return {
        "flow": FLOW_NAME,
        "version": 1,
        "generation": generation,
        "position": 0,
        "total": total,
        "run": None,
        "counts": dict.fromkeys(OUTCOMES, 0),
        "failed": [],
        "done_at": None,
        "updated_at": _now(),
    }


# ---------------------------------------------------------------------------
# One run
# ---------------------------------------------------------------------------


@dataclass
class BackfillSummary:
    """What one run did, or in a dry run would do, file by file."""

    mode: FlowMode
    record_name: str = ""
    problems: list[str] = field(default_factory=list)
    listed: int = 0
    listing_finished: bool = False
    listing_pending_folders: int = 0
    total: int = 0
    start: int = 0
    position: int = 0
    scanned: int = 0
    outcomes: dict[str, list[str]] = field(
        default_factory=lambda: {outcome: [] for outcome in OUTCOMES}
    )
    restarted: bool = False
    done: bool = False
    aborted: str = ""
    #: Why the run stopped before its chunk's end to leave time for the
    #: steps after it; empty when it did not.
    paused: str = ""
    registered: int = 0
    registry_failed: list[str] = field(default_factory=list)
    #: File ids whose registration failed this run, to try again.
    registry_retry: set[str] = field(default_factory=set)
    #: The manifest this summary is of (each root has its own).
    manifest_name: str = MANIFEST_NAME
    #: With more than one root, each root's own summary, in order; this
    #: summary then holds only the run's setting problems.
    roots: list[tuple[LibraryRoot, BackfillSummary]] = field(default_factory=list)
    #: A root's run that failed (with more than one root), and why.
    error: str = ""

    def add(self, outcome: str, item: str) -> None:
        """Count ``item`` under ``outcome``."""
        self.outcomes[outcome].append(item)

    @property
    def remaining(self) -> int:
        """Files in the manifest after this run's position."""
        return max(self.total - self.position, 0)

    @property
    def matched(self) -> int:
        """Files the catalog had tags for: written, would write, or already tagged."""
        return sum(
            len(self.outcomes[o]) for o in ("written", "would_write", "already_tagged")
        )


def is_mp3_listing(item: dict[str, Any]) -> bool:
    """Whether a listed file is an MP3, by its type or name: only ID3 tags
    are written."""
    return item.get("mime_type") == MP3_MIME_TYPE or str(
        item.get("name", "")
    ).lower().endswith(".mp3")


#: The name endings of the formats the shared tagger supports.
AUDIO_SUFFIXES = (
    ".mp3",
    ".flac",
    ".ogg",
    ".oga",
    ".opus",
    ".m4a",
    ".wav",
    ".aif",
    ".aiff",
    ".aifc",
)
#: The types Drive lists those formats under.
AUDIO_MIME_TYPES = frozenset(
    {
        MP3_MIME_TYPE,
        "audio/flac",
        "audio/x-flac",
        "audio/ogg",
        "audio/opus",
        "audio/mp4",
        "audio/m4a",
        "audio/x-m4a",
        "audio/wav",
        "audio/x-wav",
        "audio/wave",
        "audio/vnd.wave",
        "audio/aiff",
        "audio/x-aiff",
    }
)
#: Name endings of audio the shared tagger does not tag, reported when a
#: root lists one (Drive does not always give such a file an audio type).
OTHER_AUDIO_SUFFIXES = (
    ".wma",
    ".aac",
    ".ac3",
    ".alac",
    ".amr",
    ".ape",
    ".dsf",
    ".m4b",
    ".m4p",
    ".mid",
    ".midi",
    ".mka",
    ".mp2",
    ".wv",
)


def _is_partial(item: dict[str, Any]) -> bool:
    return str(item.get("name", "")).lower().endswith(PARTIAL_DOWNLOAD_SUFFIXES)


def is_audio_listing(item: dict[str, Any]) -> bool:
    """Whether a listed file is audio in a format the shared tagger
    supports, by its type or name, and not a partial download. Its
    contents decide in the end: one the tagger cannot read is reported when
    it is read, and never written."""
    if _is_partial(item):
        return False
    return item.get("mime_type") in AUDIO_MIME_TYPES or str(
        item.get("name", "")
    ).lower().endswith(AUDIO_SUFFIXES)


class Backfill:
    """One run over one chunk of the manifest."""

    def __init__(
        self,
        drive: Any,
        api: CatalogLookup,
        settings: BackfillSettings,
        record: RunRecord,
        summary: BackfillSummary,
        manifest: dict[str, Any],
        root: LibraryRoot | None = None,
    ) -> None:
        self.drive = drive
        self.api = api
        # The same API client: it registers files as well as looking them up.
        self.registry: LibraryRegistry = api  # type: ignore[assignment]
        self.settings = settings
        self.record = record
        self.summary = summary
        self.manifest = manifest
        #: The library root this run is over.
        self.root = root or settings.roots[0]

    @property
    def mode(self) -> FlowMode:
        """The mode this run was started in."""
        return self.record.mode

    def label(self, item: dict[str, Any], name: str | None = None) -> str:
        """The file's path under the library root, for reports."""
        path = self.manifest["folders"].get(item["folder_id"], "")
        name = name or str(item["name"])
        return f"{path}/{name}" if path else name

    def _skip(
        self,
        item: dict[str, Any],
        outcome: str,
        reason: str,
        name: str | None = None,
        *,
        old_tags: dict[str, str] | None = None,
        detail: str = "",
    ) -> None:
        self.record.add(
            RecordEntry(
                file_id=str(item["id"]),
                original_name=name or str(item["name"]),
                action="skip",
                state="skipped",
                folder=str(item["folder_id"]),
                reason=reason,
                old_tags=old_tags or {},
            )
        )
        label = self.label(item, name)
        if outcome == "failed":
            label = f"{label} ({reason})"
        elif detail:
            label = f"{label} ({detail})"
        self.summary.add(outcome, label)

    def register(
        self,
        item: dict[str, Any],
        name: str,
        meta: dict[str, Any],
        data: bytes,
        catalog_id: str | None,
    ) -> None:
        """Register the file in the library registry. A failure is reported
        and kept to try again; it never stops the run."""
        file_id = str(item["id"])
        row = read_tag_row(data)
        comment = comment_text(data)
        try:
            link = uuid.UUID(catalog_id) if catalog_id else None
        except ValueError:
            link = None
        payload = LibraryFileRegister(
            drive_file_id=file_id,
            library_root_id=self.root.root_id,
            path=self.label(item, name),
            size_bytes=len(data),
            md5=str(meta.get("md5Checksum") or "") or None,
            duration_secs=duration_secs(data),
            identification_status="identified" if link else "unidentified",
            catalog_id=link,
            edit_label=edit_label(row["title"]),
            comment_backup=comment or None,
        )
        try:
            self.registry.register_library_file(payload)
        except Exception as exc:  # noqa: BLE001 — registering must not stop the run
            if shared_deadline_passed():
                raise
            log.warning("library-backfill: registering %s failed: %s", name, exc)
            self.summary.registry_failed.append(
                f"{self.label(item, name)} ({type(exc).__name__}: {exc})"
            )
            self.summary.registry_retry.add(file_id)
            return
        self.summary.registered += 1
        self.summary.registry_retry.discard(file_id)

    def retry_registrations(self, file_ids: list[str]) -> None:
        """Register again the files an earlier run could not register,
        reading each one's link from its own ``TXXX:CATALOG_ID``."""
        by_id = {str(item["id"]): item for item in self.manifest["files"]}
        for file_id in file_ids:
            item = by_id.get(file_id)
            if item is None:
                continue
            meta = file_meta(self.drive, file_id)
            if meta.get("trashed"):
                continue
            name = str(meta.get("name") or item["name"])
            data: bytes = self.drive.download_file_bytes(file_id).data
            self.register(item, name, meta, data, linked_catalog_id(data) or None)

    def one(self, item: dict[str, Any]) -> str:
        """Process one listed file; return its outcome. Raises
        :class:`AudioChanged` (the run stops) and lets anything unexpected
        out to :func:`_process_chunk`, which records it as failed."""
        file_id = str(item["id"])
        if file_id in self.settings.skip_ids:
            self._skip(item, "skipped_by_setting", f"listed in {SKIP_IDS_VAR}")
            return "skipped_by_setting"
        if not self.root.lists(item):
            reason = (
                "not audio the shared tagger supports"
                if self.root.every_format
                else "not an MP3: only ID3 tags are written"
            )
            self._skip(item, "not_mp3", reason)
            return "not_mp3"
        meta = file_meta(self.drive, file_id)
        name = str(meta.get("name") or item["name"])
        if meta.get("trashed") or item["folder_id"] not in (meta.get("parents") or []):
            self._skip(item, "moved_since", "no longer where the manifest has it", name)
            return "moved_since"

        data: bytes = self.drive.download_file_bytes(file_id).data
        row = read_tag_row(data)
        track = ingest_track(row)
        if track is None:
            self._skip(
                item,
                "needs_fingerprinting",
                "no usable title or artist",
                name,
                old_tags=tag_snapshot(data),
            )
            self.register(item, name, meta, data, None)
            return "needs_fingerprinting"
        if self.root.key == "fx":
            match = self._fx_entry(row)
        else:
            match = self.api.lookup_catalog(CatalogLookupRequest(tracks=[track]))[0]
            # A main-library file never links to an fx entry (ADR-0009).
            if match is not None and match.library != "main":
                match = None
        if match is not None and not owner_edit_fits(row["title"], match):
            # An owner edit is never linked to the published song's entry:
            # identify takes it in as an owner-edit entry of its own.
            match = None
        if match is None:
            self._skip(
                item,
                "not_in_catalog",
                "not in the catalog",
                name,
                old_tags=tag_snapshot(data),
                detail=f"looked up {row['title']!r} by {row['artist']!r}",
            )
            self.register(item, name, meta, data, None)
            return "not_in_catalog"
        # A library file gets only the link to its catalog entry.
        tagged = write_catalog_tags(data, match, id_only=True)
        if tagged is None:
            self.record.add(
                RecordEntry(
                    file_id=file_id,
                    original_name=name,
                    action="skip",
                    state="skipped",
                    folder=str(item["folder_id"]),
                    reason="already tagged from the catalog",
                    identified_by="catalog",
                    old_tags=tag_snapshot(data),
                )
            )
            self.summary.add("already_tagged", self.label(item, name))
            self.register(item, name, meta, data, str(match.id))
            return "already_tagged"

        entries_before = len(self.record.entries)
        try:
            entry = write_in_place(
                self.drive,
                self.record,
                file_id=file_id,
                name=name,
                original=data,
                tagged=tagged,
                identified_by="catalog",
                folder=str(item["folder_id"]),
                head_revision=meta.get("headRevisionId"),
            )
        except AudioChanged:
            raise
        except Exception:
            self._check_unfinished_write(entries_before, file_id, name, data)
            raise
        outcome = "written" if entry.state == "done" else "would_write"
        label = self.label(item, name)
        if outcome == "would_write":
            changes = ", ".join(
                f"{k}: {entry.old_tags.get(k, '')!r} -> {v!r}"
                for k, v in entry.new_tags.items()
                if entry.old_tags.get(k, "") != v
            )
            label = f"{label} ({changes})"
        self.summary.add(outcome, label)
        if outcome == "written":
            # The file's contents are now the tagged bytes.
            meta = {**meta, "md5Checksum": md5(tagged)}
            data = tagged
        self.register(item, name, meta, data, str(match.id))
        return outcome

    def _fx_entry(self, row: dict[str, str]) -> CatalogMatch | None:
        """The fx entry the catalog has for an FX Music file's own title,
        artist and year, or None. Asked as a preview, so nothing is made:
        a song the catalog lacks is left to identify, which makes its fx
        entry (ADR-0009, decision 3). An answer from another library is
        never linked."""
        year = str(row.get("year") or "").strip()[:4]
        made = self.creator.create_library_entry(
            LibraryEntryCreate(
                title=row["title"].strip(),
                artist=row["artist"].strip(),
                release_year=int(year) if len(year) == 4 and year.isdigit() else None,
                preview=True,
                library="fx",
            )
        )
        if made.previewed or made.entry.library != "fx":
            return None
        return made.entry

    @property
    def creator(self) -> LibraryEntryCreator:
        """The same API client: it finds library entries too."""
        return self.api  # type: ignore[return-value]

    def _check_unfinished_write(
        self, entries_before: int, file_id: str, name: str, original: bytes
    ) -> None:
        """After a write that failed part-way: if the new bytes were already
        uploaded but not yet checked, check them now (past the deadline if
        need be), and put the original back on a mismatch."""
        if len(self.record.entries) <= entries_before:
            return
        entry = self.record.entries[-1]
        if entry.state != "pending" or not entry.written_md5:
            return
        with without_run_deadline():
            written = self.drive.download_file_bytes(file_id).data
            try:
                check_audio_unchanged(original, written)
            except AudioChanged as exc:
                update_contents(self.drive, file_id, name, original)
                entry.state = "restored"
                entry.error = f"audio changed after the write; original restored: {exc}"
                self.record.checkpoint()
                raise
        entry.state = "done"


def _process_chunk(
    work: Backfill,
    cursor: dict[str, Any],
    cursor_store: StateFile,
    end: int,
) -> None:
    """Process manifest files from the cursor up to ``end``.

    In a writing run the record is saved, and only then the cursor moved
    past the file, after every file. Raises ``RunOutOfTime`` between files
    once the deadline has passed, and :class:`AudioChanged` the moment a
    file's audio check fails, leaving the cursor on that file.
    """
    summary = work.summary
    files = work.manifest["files"]
    writes = work.mode.writes
    reserve = work.settings.reserve_seconds
    index = summary.start
    while index < end:
        item = files[index]
        if shared_deadline_passed():
            raise RunOutOfTime(f"run deadline reached before {item['name']}")
        left = remaining_seconds()
        if left is not None and left < reserve:
            summary.paused = (
                f"stopped before {item['name']} with {left:.0f}s left, to leave "
                f"{reserve}s ({RESERVE_SECONDS_VAR}) for the rest of the run; "
                "the next run carries on from here"
            )
            return
        summary.scanned += 1
        entries_before = len(work.record.entries)
        try:
            outcome = work.one(item)
        except AudioChanged as exc:
            # The cursor stays on this file: the run stops here.
            _record_failure(work, item, entries_before, f"audio check: {exc}")
            work.record.checkpoint()
            raise
        except TagEmptied as exc:
            outcome = "failed"
            _record_failure(work, item, entries_before, str(exc))
        except Exception as exc:  # noqa: BLE001 — one file must not end the run
            if shared_deadline_passed():
                raise RunOutOfTime(
                    f"run deadline reached processing {item['name']}"
                ) from exc
            log.exception("library-backfill: failed on %s", item["name"])
            outcome = "failed"
            _record_failure(work, item, entries_before, f"{type(exc).__name__}: {exc}")
        index += 1
        if writes:
            # The record first: the cursor never passes an unrecorded file.
            # Both are saved even past the run deadline: the file's outcome
            # is final, and the next run must not do it again.
            with without_run_deadline():
                _save_progress(work, item, outcome, cursor, cursor_store, index)
            summary.position = index


def _save_progress(
    work: Backfill,
    item: dict[str, Any],
    outcome: str,
    cursor: dict[str, Any],
    cursor_store: StateFile,
    index: int,
) -> None:
    """Checkpoint the run record, then move the cursor past ``item``."""
    work.record.checkpoint()
    cursor["position"] = index
    cursor["counts"][outcome] = cursor["counts"].get(outcome, 0) + 1
    if outcome == "failed":
        cursor["failed"].append(
            {
                "file_id": str(item["id"]),
                "name": str(item["name"]),
                "run_id": work.record.run_id,
            }
        )
    cursor["registry_retry"] = sorted(work.summary.registry_retry)
    cursor["updated_at"] = _now()
    cursor_store.save(cursor)


def _record_failure(
    work: Backfill, item: dict[str, Any], entries_before: int, reason: str
) -> None:
    if len(work.record.entries) > entries_before:
        entry = work.record.entries[-1]
        if entry.state not in ("done", "restored"):
            entry.state = "failed"
        entry.error = entry.error or reason
        work.summary.add("failed", f"{work.label(item)} ({reason})")
    else:
        work._skip(item, "failed", reason)


def run_backfill(
    *,
    run_id: str | None,
    mode: FlowMode,
    drive: Any | None = None,
    api: CatalogLookup | None = None,
) -> BackfillSummary:
    """One run: the mode, settings, manifest, cursor, record and report."""
    started_at = monotonic()
    run_id = run_id or local_run_id()
    summary = BackfillSummary(mode)
    with run_report(
        FLOW_NAME,
        repo=REPO,
        notable=False,
        run_id=run_id,
        version=__version__,
        list_all=True,  # every file in the chunk is named, not the first few
    ) as report:
        try:
            if mode is FlowMode.OFF:
                return summary
            settings, problems = BackfillSettings.from_env()
            if settings is None:
                summary.problems.extend(problems)
                return summary
            if drive is None:
                drive = GoogleAPI.from_env().drive
            if len(settings.roots) == 1:
                _run(drive, api, settings, settings.roots[0], run_id, summary)
                if summary.aborted:
                    _report_abort(run_id, summary)
            else:
                _run_roots(drive, api, settings, run_id, summary)
        finally:
            if summary.roots:
                record_roots(report, summary.roots, _record, monotonic() - started_at)
            else:
                _record(report, summary, monotonic() - started_at)
    return summary


def _run_roots(
    drive: Any,
    api: CatalogLookup | None,
    settings: BackfillSettings,
    run_id: str,
    summary: BackfillSummary,
) -> None:
    """Each root on its own, the main library first (ADR-0009)."""
    if api is None:
        api = api_client(base_url=api_base_url().strip() or None)

    def run(root: LibraryRoot) -> bool:
        part = BackfillSummary(summary.mode)
        summary.roots.append((root, part))
        _run(drive, api, settings, root, run_id, part)
        if part.aborted:
            _report_abort(run_id, part)
            return False
        return True

    def failed(root: LibraryRoot, exc: Exception) -> None:
        summary.roots[-1][1].error = f"{type(exc).__name__}: {exc}"

    each_root(settings.roots, run, failed)


def _run(
    drive: Any,
    api: CatalogLookup | None,
    settings: BackfillSettings,
    root: LibraryRoot,
    run_id: str,
    summary: BackfillSummary,
) -> None:
    mode = summary.mode
    summary.manifest_name = root_state_name(FLOW_NAME, MANIFEST_NAME, root)
    manifest_store = StateFile(drive, settings.state, summary.manifest_name)
    cursor_store = StateFile(
        drive, settings.state, root_state_name(FLOW_NAME, CURSOR_NAME, root)
    )
    try:
        manifest = manifest_store.load()
        cursor = cursor_store.load()
    except StateProblem as exc:
        summary.problems.append(str(exc))
        return

    if manifest is not None and manifest.get("root_folder_id") != root.root_id:
        summary.problems.append(
            f"{summary.manifest_name} lists root {manifest.get('root_folder_id')}, "
            f"not {root.folders[0].setting}={root.root_id}; trash it to list the "
            "new root"
        )
        return
    if manifest is None:
        manifest = new_root_manifest(root)
    if not manifest.get("complete"):
        before = len(manifest["files"])
        # The state folder and the other roots' folders are not this root's.
        exclude = {settings.state} | {
            folder_id
            for other in settings.roots
            if other.key != root.key
            for folder_id in other.folder_ids
        }
        try:
            build_manifest(drive, manifest_store, manifest, exclude=exclude)
        finally:
            summary.listed = len(manifest["files"]) - before
            summary.listing_pending_folders = len(manifest["pending_folders"])
        summary.listing_finished = True

    files = manifest["files"]
    summary.total = len(files)
    generation = str(manifest["generation"])
    if cursor is None or cursor.get("generation") != generation:
        summary.restarted = cursor is not None
        cursor = new_cursor(generation, len(files))
    start = int(cursor.get("position", 0))
    summary.start = summary.position = start
    if start >= len(files):
        summary.done = True
        if mode.writes and not cursor.get("done_at"):
            cursor["done_at"] = _now()
            cursor_store.save(cursor)
        return

    run = cursor.get("run") or {}
    if run.get("run_id") == run_id and int(run.get("chunk_end", 0)) > start:
        # A redelivery of a run the deadline stopped: finish its chunk.
        end = min(int(run["chunk_end"]), len(files))
    else:
        end = min(start + settings.chunk_size, len(files))
    if mode.writes:
        cursor["run"] = {"run_id": run_id, "chunk_end": end}
        cursor["total"] = len(files)
        cursor["updated_at"] = _now()
        cursor_store.save(cursor)

    record = RunRecord(
        FLOW_NAME,
        run_id,
        mode,
        stage=f"files {start + 1}-{end} of {len(files)} (manifest {generation})",
        folders={"root": root.root_id, "state": settings.state},
    ).attach(drive, settings.state)
    try:
        _resume_record(drive, settings.state, record)
    except StateProblem as exc:
        summary.problems.append(str(exc))
        return
    summary.record_name = record.file_name
    if api is None:
        api = api_client(base_url=api_base_url().strip() or None)
    work = Backfill(drive, api, settings, record, summary, manifest, root)
    status = "failed"
    try:
        retry = [str(i) for i in cursor.get("registry_retry") or []]
        if retry:
            work.retry_registrations(retry)
            if mode.writes:
                cursor["registry_retry"] = sorted(summary.registry_retry)
                cursor_store.save(cursor)
        _process_chunk(work, cursor, cursor_store, end)
        status = "complete"
    except AudioChanged as exc:
        summary.aborted = str(exc)
        status = "aborted"
    except RunOutOfTime:
        status = "stopped"
        raise
    finally:
        if record.entries:
            with without_run_deadline():
                record.finish(status)
    if mode.writes and summary.position >= len(files):
        summary.done = True
        cursor["done_at"] = _now()
        cursor_store.save(cursor)


def _resume_record(drive: Any, state_folder: str, record: RunRecord) -> None:
    """Carry on the record a redelivered run (same run id) already saved,
    so one run keeps one record, as rollback expects."""
    existing = StateFile(drive, state_folder, record.file_name)
    saved = existing.load()
    if saved is None:
        return
    names = {f.name for f in fields(RecordEntry)}
    record.entries = [
        RecordEntry(**{k: v for k, v in entry.items() if k in names})
        for entry in saved.get("entries") or []
    ]
    record.started_at = str(saved.get("started_at") or record.started_at)
    record._file_id = existing.file_id


def library_backfill(*, run_id: str | None = None) -> BackfillSummary:
    """Backfill the next chunk of the library and report the run.

    ``run_id`` is the queue message id when the Lambda worker runs this.
    """
    return run_backfill(run_id=run_id, mode=flow_mode(MODE_FLAG))


def _report_abort(run_id: str, summary: BackfillSummary) -> None:
    try:
        post_run_finding(
            FLOW_NAME,
            "ERROR",
            "library-backfill stopped: a file's audio would not stay "
            f"byte-identical ({summary.aborted}). The file is unchanged (or "
            f"restored); the cursor stays on it; see {summary.record_name}.",
            run_id=run_id,
        )
    except Exception:  # noqa: BLE001 — the notification is not the job
        log.exception("library-backfill: could not report the stop")


def _record(report: Any, summary: BackfillSummary, duration_sec: float) -> None:
    report.duration_sec = duration_sec
    if summary.mode is FlowMode.OFF:
        report.notice("off", f"{MODE_FLAG} is off: nothing was read or written")
        return
    for problem in summary.problems:
        report.issue("setting", problem)
    if summary.problems:
        return
    dry = summary.mode is FlowMode.DRY_RUN
    if dry:
        report.notice(
            "dry_run",
            f"{MODE_FLAG} is dry-run: no music was written and the cursor did "
            f"not move; would-be record {summary.record_name or '(none)'}",
        )
    if summary.listing_finished:
        report.notice(
            "manifest_built",
            f"{summary.manifest_name}: {summary.total} files listed",
        )
    elif summary.listing_pending_folders:
        report.notice(
            "manifest_in_progress",
            f"{summary.manifest_name}: {summary.listed} files listed this run, "
            f"{summary.listing_pending_folders} folders still to list",
        )
    if summary.paused:
        report.notice("paused", summary.paused)
    if summary.restarted:
        report.notice(
            "manifest_rebuilt", "a new manifest: starting from its first file"
        )
    if summary.done and summary.scanned:
        # Said once, on the run that reached the end; a finished backfill is
        # otherwise idle and quiet.
        report.notice(
            "library_done",
            f"all {summary.total} files in the manifest are processed; trash "
            f"{summary.manifest_name} in the state folder to start over",
        )
    report.ok(len(summary.outcomes["written"]))
    report.count("listed", summary.listed)
    report.count("scanned", summary.scanned)
    report.count("matched", summary.matched)
    for outcome in OUTCOMES:
        report.count(outcome, len(summary.outcomes[outcome]))
    report.count("remaining", summary.remaining)
    report.count("registered", summary.registered)
    report.count("registry_failed", len(summary.registry_failed))
    report.count("done", 1 if summary.done else 0)
    for name in summary.outcomes["written"]:
        report.created("tags written", name)
    for outcome in (
        "would_write",
        "not_in_catalog",
        "needs_fingerprinting",
        "not_mp3",
        "moved_since",
        "skipped_by_setting",
    ):
        for name in summary.outcomes[outcome]:
            report.notice(outcome, name)
    for name in summary.outcomes["failed"]:
        report.issue("failed", name)
    for name in summary.registry_failed:
        report.notice("registry_failed", name)
    if summary.aborted:
        report.issue("audio_changed_run_stopped", summary.aborted)
    if summary.error:
        report.issue("failed", f"the run over this root failed ({summary.error})")


if __name__ == "__main__":
    library_backfill()
