"""The ``library-duplicates`` step of the library job: report the
library files that look like copies of one song (ADR-0008, decision 7).

Read-only: it never writes, renames, moves or deletes a music file, and
makes no paid calls. It reads the library registry (every present file
under the root that is not already marked for the trash, with its path,
size, md5, duration, release label and catalog entry) and the catalog's
title and artist for the entries those files link to, then groups files
that are linked by any of:

- ``same_md5`` — the same Drive md5: exact copies;
- ``same_catalog_id`` — linked to the same catalog entry;
- ``same_title_artist`` — linked to entries the display rules write out
  with the same title and artist (case, spacing and punctuation ignored);
- ``same_name`` — the same file name in the same folder, ignoring a copy
  suffix such as `` (1)``.

**Kept on purpose.** Files of one song that are different edits are never
duplicates of each other: a different release label (Clean, Super Clean,
Explicit, Radio Edit, …), or durations more than
:data:`EDIT_TOLERANCE_SECS` apart (a No Intro Edit). Each linked set of
files is split into its edits; every edit with two or more files is a
group, and the files of the song's other edits are shown in it marked kept
on purpose, with why, so they can never be sent to the trash through it.

**Every song with two or more files** (ADR-0007, amended 2026-11-11: one
entry may keep several files). In the main library a song whose files are
all different edits, one file each, is still a group, so the owner sees
every file the song has: its first edit's file is offered, the others shown
beside it kept on purpose. A group the owner kept together is not reopened
by the API while its files stay the same; the cog keeps no memory of it.

**Owner edits** (ADR-0008, decided 2026-11-11): a file linked to an
owner-edit entry (a cut the owner made himself, ``Song <Intro Cut>``) is
an edit of its own, never a copy of the published song's files, even with
the same md5. Files on one owner-edit entry are grouped as copies of that
entry like any other; shown beside the published song's files, it is kept on
purpose, as an owner edit.

**Per library root** (ADR-0009, decision 4). Files are grouped only with
files of the same root, so an FX Music file and a main-library file are
never duplicates of each other. In the main library (its own folder and New
Music, one root) they are grouped within and across its folders, as above.
In FX Music, which keeps copies of one file in several role and event
folders on purpose, files are grouped only within one folder; copies linked
across FX folders are not offered as duplicates but listed in the run
report (``fx_across_folders``) for the owner to decide case by case.

Each group has a key made from its files, so an unchanged library reports
the same groups every run. With ``flags.deejay_cog.library_duplicates`` at ``write`` the
groups go to the API (``POST /v1/catalog/library/duplicates``), where the
WCS duplicates page shows them; they replace the open groups for that root,
each root reported on its own. A dry run lists the groups in the run report
only.

**Reported when they change.** ``library-duplicates-seen.json`` in
``MUSIC_TAGGING_STATE_FOLDER_ID`` keeps a digest of each root's groups (and
FX Music's sets across folders) as the last run found them. A run whose
groups are the same as then, for every root, and that the API found nothing
added to or dropped from, names no group and posts no report: the run is
still logged with its counts, and the digest saved. A group added, dropped
or changed reports as before; so does a root that failed.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from time import monotonic
from typing import Any, Protocol, cast

from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import (
    CATALOG_DISPLAY_MAX_IDS,
    DUPLICATE_REPORT_MAX_GROUPS,
    CatalogDisplay,
    CatalogDisplayRequest,
    DuplicateGroupReport,
    DuplicateMember,
    DuplicateReport,
    DuplicateReportResult,
    LibraryFile,
)
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI

from deejay_cog._pipeline_eval import REPO, run_report
from deejay_cog._version import __version__
from deejay_cog.api_client import api_client
from deejay_cog.library_backfill import BackfillSettings, LibraryRoot, StateFile
from deejay_cog.music_safety import FlowMode, flow_mode, local_run_id

log = logger_mod.get_logger()

FLOW_NAME = "library-duplicates"

#: The feature flag holding this step's mode: off | dry-run | write; a
#: missing flag or an unknown value is off. Write sends the groups to
#: the API; no mode ever writes a music file.
MODE_FLAG = "flags.deejay_cog.library_duplicates"

#: Files of one song whose durations are further apart than this are
#: different edits, not copies.
EDIT_TOLERANCE_SECS = 3.0

#: Registry rows read per page.
PAGE_SIZE = 500

#: The digest of each root's groups as the last run found them.
SEEN_NAME = f"{FLOW_NAME}-seen.json"

#: Why files were grouped, in the order a group's key names the first.
REASONS = ("same_md5", "same_catalog_id", "same_title_artist", "same_name")

_COPY_SUFFIX = re.compile(r"\s*\(\d+\)$")
_NOT_WORD = re.compile(r"[^\w]+")


class Catalog(Protocol):
    """The parts of the API client the report calls."""

    def list_library_files(self, **kwargs: Any) -> list[LibraryFile]:
        """One page of registered files."""
        ...

    def read_catalog_display(
        self, payload: CatalogDisplayRequest
    ) -> list[CatalogDisplay | None]:
        """Each entry's title and artist as the display rules write them."""
        ...

    def report_duplicate_groups(
        self, payload: DuplicateReport
    ) -> DuplicateReportResult:
        """Replace the root's open duplicate groups."""
        ...


@dataclass
class RootDuplicates:
    """The groups found under one library root, and what the API made of
    them."""

    root: LibraryRoot
    files: int = 0
    groups: list[DuplicateGroupReport] = field(default_factory=list)
    left_out: int = 0
    result: DuplicateReportResult | None = None
    #: FX Music: files linked as copies across its folders, each set listed
    #: for the owner to decide, never offered as a group to resolve.
    across_folders: list[list[str]] = field(default_factory=list)
    failed: str = ""


@dataclass
class DuplicatesSummary:
    """The groups one run found, and what the API made of them: the totals
    over every root, and each root's own."""

    mode: FlowMode
    problems: list[str] = field(default_factory=list)
    files: int = 0
    groups: list[DuplicateGroupReport] = field(default_factory=list)
    paths: dict[str, str] = field(default_factory=dict)
    left_out: int = 0
    result: DuplicateReportResult | None = None
    roots: list[RootDuplicates] = field(default_factory=list)
    #: Every root's groups are the same as at the last run: nothing to name.
    unchanged: bool = False
    #: Why the last run's groups could not be read or saved, if so.
    seen_problem: str = ""


def _normal(text: str) -> str:
    return " ".join(_NOT_WORD.sub(" ", text.casefold()).split())


def shown_path(path: str) -> str:
    """A registered path as the report shows it: from its library's root,
    with a leading slash (a file at the top of the main library is
    registered by its bare name). The registry is not changed."""
    return path if path.startswith("/") else f"/{path}"


def name_key(path: str) -> str:
    """The folder and file name with the extension and a copy suffix
    dropped: ``House/9to5 (1).mp3`` -> ``house/9to5``."""
    folder, _, name = path.rpartition("/")
    stem = name.rsplit(".", 1)[0] if "." in name else name
    stem = _COPY_SUFFIX.sub("", stem)
    return f"{folder.casefold()}/{_normal(stem)}"


def _minutes(secs: float) -> str:
    whole = round(secs)
    return f"{whole // 60}:{whole % 60:02d}"


class _Links:
    """Union-find over drive ids, remembering why each pair is linked."""

    def __init__(self, ids: list[str]) -> None:
        self.parent = {i: i for i in ids}
        self.edges: list[tuple[str, str, str]] = []

    def find(self, i: str) -> str:
        while self.parent[i] != i:
            self.parent[i] = self.parent[self.parent[i]]
            i = self.parent[i]
        return i

    def link(self, members: list[str], reason: str) -> None:
        for other in members[1:]:
            self.edges.append((members[0], other, reason))
            a, b = self.find(members[0]), self.find(other)
            if a != b:
                self.parent[max(a, b)] = min(a, b)


def _owner_edit(f: LibraryFile, owner_edits: frozenset[uuid.UUID]) -> str:
    """The owner-edit entry ``f`` is linked to, as text; "" for any other file."""
    return str(f.catalog_id) if f.catalog_id in owner_edits else ""


def _edits(
    files: list[LibraryFile], owner_edits: frozenset[uuid.UUID] = frozenset()
) -> list[list[LibraryFile]]:
    """The song's files split into edits: the files of each owner-edit entry
    apart from the rest, then by release label, then by duration, a gap of
    more than the tolerance starting a new edit. A file of unknown length
    joins the label's largest edit."""
    by_label: dict[tuple[str, str], list[LibraryFile]] = defaultdict(list)
    for f in files:
        key = (_owner_edit(f, owner_edits), (f.edit_label or "").casefold())
        by_label[key].append(f)
    out: list[list[LibraryFile]] = []
    for label in sorted(by_label):
        known = sorted(
            (f for f in by_label[label] if f.duration_secs is not None),
            key=lambda f: (f.duration_secs, f.drive_file_id),
        )
        unknown = [f for f in by_label[label] if f.duration_secs is None]
        clusters: list[list[LibraryFile]] = []
        for f in known:
            last = clusters[-1][-1].duration_secs if clusters else None
            if (
                clusters
                and last is not None
                and f.duration_secs is not None
                and f.duration_secs - last <= EDIT_TOLERANCE_SECS
            ):
                clusters[-1].append(f)
            else:
                clusters.append([f])
        if unknown:
            if clusters:
                max(clusters, key=len).extend(unknown)
            else:
                clusters.append(unknown)
        out.extend(clusters)
    return out


def _why_kept(
    f: LibraryFile,
    edit: list[LibraryFile],
    owner_edits: frozenset[uuid.UUID] = frozenset(),
) -> str:
    mine, group = _owner_edit(f, owner_edits), _owner_edit(edit[0], owner_edits)
    if mine != group:
        if mine:
            return "an owner edit (its own entry, not the published song)"
        return "the published song (this group is of an owner edit)"
    theirs = (edit[0].edit_label or "").casefold()
    if (f.edit_label or "").casefold() != theirs:
        return f"a different release label ({f.edit_label or 'none'})"
    lengths = [e.duration_secs for e in edit if e.duration_secs is not None]
    if f.duration_secs is not None and lengths:
        return (
            f"a different edit ({_minutes(f.duration_secs)} against "
            f"{_minutes(sum(lengths) / len(lengths))})"
        )
    return "a different edit"


def _songs(
    files: list[LibraryFile], shown: dict[uuid.UUID, CatalogDisplay]
) -> tuple[list[list[LibraryFile]], _Links]:
    """``files`` in sets linked as copies of one song, and the links."""
    files = sorted(files, key=lambda f: f.drive_file_id)
    links = _Links([f.drive_file_id for f in files])
    buckets: dict[tuple[str, str], list[str]] = defaultdict(list)
    for f in files:
        if f.md5:
            buckets[("same_md5", f.md5)].append(f.drive_file_id)
        if f.catalog_id:
            buckets[("same_catalog_id", str(f.catalog_id))].append(f.drive_file_id)
            display = shown.get(f.catalog_id)
            if display is not None:
                key = f"{_normal(display.title)}|{_normal(display.artist)}"
                buckets[("same_title_artist", key)].append(f.drive_file_id)
        buckets[("same_name", name_key(f.path))].append(f.drive_file_id)
    for (reason, _), members in sorted(buckets.items()):
        if len(members) > 1:
            links.link(members, reason)

    songs: dict[str, list[LibraryFile]] = defaultdict(list)
    for f in files:
        songs[links.find(f.drive_file_id)].append(f)
    return list(songs.values()), links


def find_groups(
    files: list[LibraryFile],
    shown: dict[uuid.UUID, CatalogDisplay],
    *,
    every_song: bool = True,
) -> list[DuplicateGroupReport]:
    """The duplicate groups among ``files``, sorted by key; the same files
    always give the same groups. With ``every_song`` (the main library), a
    song whose files are all different edits is a group too; FX Music
    leaves it out."""
    songs, links = _songs(files, shown)
    owner_edits = frozenset(i for i, d in shown.items() if d.owner_edit)
    groups: list[DuplicateGroupReport] = []
    for song in songs:
        if len(song) < 2:
            continue
        edits = _edits(song, owner_edits)
        offered = [edit for edit in edits if len(edit) > 1]
        if not offered and every_song:
            # One file of each edit: still one group, the first edit's file
            # offered and the others kept on purpose beside it.
            offered = edits[:1]
        for edit in offered:
            ids = {f.drive_file_id for f in edit}
            reasons = {r for a, b, r in links.edges if a in ids and b in ids}
            if not reasons:
                reasons = {r for a, b, r in links.edges if a in ids or b in ids}
            ordered = [r for r in REASONS if r in reasons]
            digest = hashlib.sha1(
                "|".join(sorted(ids)).encode(), usedforsecurity=False
            ).hexdigest()[:16]
            listed = [DuplicateMember(drive_file_id=i) for i in sorted(ids)]
            listed += [
                DuplicateMember(
                    drive_file_id=f.drive_file_id,
                    kept_on_purpose=True,
                    why_kept=_why_kept(f, edit, owner_edits),
                )
                for f in song
                if f.drive_file_id not in ids
            ]
            groups.append(
                DuplicateGroupReport(
                    key=f"{ordered[0]}:{digest}",
                    reasons=ordered,  # type: ignore[arg-type]
                    files=listed,
                )
            )
    return sorted(groups, key=lambda g: g.key)


def _folder(path: str) -> str:
    """The folder a registered path is in, under its root ("" at the top)."""
    return path.rpartition("/")[0]


def find_fx_groups(
    files: list[LibraryFile], shown: dict[uuid.UUID, CatalogDisplay]
) -> tuple[list[DuplicateGroupReport], list[list[str]]]:
    """FX Music's duplicate groups, and its copies across folders.

    Copies of one file in the same folder are grouped as anywhere else, a
    different cut (another length or edit) kept on purpose. Copies in
    different folders are kept there on purpose (one per role or event), so
    they are never grouped: each set of files linked across folders is
    returned as its paths, for the owner to decide."""
    by_folder: dict[str, list[LibraryFile]] = defaultdict(list)
    for f in files:
        by_folder[_folder(f.path)].append(f)
    groups: list[DuplicateGroupReport] = []
    for folder in sorted(by_folder):
        groups.extend(find_groups(by_folder[folder], shown, every_song=False))
    # An owner edit is never a copy of the published song, across folders
    # either: each owner-edit entry's files are a set of their own.
    owner_edits = frozenset(i for i, d in shown.items() if d.owner_edit)
    sets: list[list[LibraryFile]] = []
    for song in _songs(files, shown)[0]:
        parts: dict[str, list[LibraryFile]] = defaultdict(list)
        for f in song:
            parts[_owner_edit(f, owner_edits)].append(f)
        sets.extend(parts.values())
    across = [
        sorted(f.path for f in part)
        for part in sets
        if len({_folder(f.path) for f in part}) > 1
    ]
    return sorted(groups, key=lambda g: g.key), sorted(across)


def _files(api: Catalog, root: str) -> list[LibraryFile]:
    rows: list[LibraryFile] = []
    offset = 0
    while True:
        page = api.list_library_files(
            library_root_id=root, present=True, limit=PAGE_SIZE, offset=offset
        )
        rows.extend(r for r in page if r.present and not r.trash_requested)
        if len(page) < PAGE_SIZE:
            return rows
        offset += PAGE_SIZE


def _displays(api: Catalog, rows: list[LibraryFile]) -> dict[uuid.UUID, CatalogDisplay]:
    ids = sorted({r.catalog_id for r in rows if r.catalog_id}, key=str)
    found: dict[uuid.UUID, CatalogDisplay] = {}
    for start in range(0, len(ids), CATALOG_DISPLAY_MAX_IDS):
        batch = ids[start : start + CATALOG_DISPLAY_MAX_IDS]
        for display in api.read_catalog_display(CatalogDisplayRequest(ids=batch)):
            if display is not None:
                found[display.id] = display
    return found


def digest(found: RootDuplicates) -> str:
    """What a root's groups are, as one value: each group's key, reasons
    and files (kept on purpose or not), FX Music's sets across folders, and
    how many groups were left out."""
    shape = {
        "groups": sorted(
            [
                g.key,
                list(g.reasons),
                sorted([m.drive_file_id, m.kept_on_purpose] for m in g.files),
            ]
            for g in found.groups
        ),
        "across_folders": sorted(sorted(paths) for paths in found.across_folders),
        "left_out": found.left_out,
    }
    text = json.dumps(shape, sort_keys=True, default=str)
    return hashlib.sha256(text.encode()).hexdigest()


def _compare_with_last(
    drive: Any, settings: BackfillSettings, summary: DuplicatesSummary
) -> None:
    """Set ``summary.unchanged`` when every root's groups are the digest
    the last run saved and the API added or dropped none, then save this
    run's digests. A root that failed keeps its last digest. A state folder
    that cannot be read or written is named, and the run reports as usual."""
    store = StateFile(drive, settings.state, SEEN_NAME)
    try:
        held = store.load() or {}
        last: dict[str, str] = dict(held.get("roots") or {})
        now = dict(last)
        for found in summary.roots:
            if not found.failed:
                now[found.root.key] = digest(found)
        summary.unchanged = bool(summary.roots) and all(
            not found.failed
            and last.get(found.root.key) == now[found.root.key]
            and (
                found.result is None
                or (not found.result.added and not found.result.dropped)
            )
            for found in summary.roots
        )
        if now != last:
            store.save({"flow": FLOW_NAME, "version": 1, "roots": now})
    except Exception as exc:  # noqa: BLE001 — the memory is not the report
        log.exception("library-duplicates: last run's groups not read or saved")
        summary.unchanged = False
        summary.seen_problem = f"{type(exc).__name__}: {exc}"


def run_duplicates(
    *,
    run_id: str | None,
    mode: FlowMode,
    api: Catalog | None = None,
    drive: Any | None = None,
) -> DuplicatesSummary:
    """One run: the mode, settings, the groups, the API and the report.
    ``drive`` reaches the state folder that keeps the last run's groups;
    built from the environment when neither client is given, and without
    it every run reports its groups."""
    started_at = monotonic()
    run_id = run_id or local_run_id()
    summary = DuplicatesSummary(mode)
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
            settings, problems = BackfillSettings.from_env()
            if settings is None:
                summary.problems.extend(problems)
                return summary
            if drive is None and api is None:
                drive = GoogleAPI.from_env().drive
            client = api or cast(
                Catalog, api_client(base_url=api_base_url().strip() or None)
            )
            for root in settings.roots:
                summary.roots.append(_root_duplicates(client, root, summary))
            _totals(summary)
            if drive is not None:
                _compare_with_last(drive, settings, summary)
        finally:
            _record(report, summary, monotonic() - started_at)
    return summary


def _root_duplicates(
    api: Catalog, root: LibraryRoot, summary: DuplicatesSummary
) -> RootDuplicates:
    """Group one root's files and, in write mode, send its groups. A
    failure is the root's alone: the other roots still run."""
    found = RootDuplicates(root)
    try:
        rows = _files(api, root.root_id)
        found.files = len(rows)
        summary.paths.update({r.drive_file_id: r.path for r in rows})
        shown = _displays(api, rows)
        if root.key == "fx":
            groups, found.across_folders = find_fx_groups(rows, shown)
        else:
            groups = find_groups(rows, shown)
        found.left_out = max(0, len(groups) - DUPLICATE_REPORT_MAX_GROUPS)
        found.groups = groups[:DUPLICATE_REPORT_MAX_GROUPS]
        if summary.mode.writes:
            found.result = api.report_duplicate_groups(
                DuplicateReport(library_root_id=root.root_id, groups=found.groups)
            )
    except Exception as exc:  # noqa: BLE001 — one root must not stop the other
        log.exception("library-duplicates: %s failed", root.label)
        found.failed = f"{type(exc).__name__}: {exc}"
    return found


def _totals(summary: DuplicatesSummary) -> None:
    """The run's totals over its roots."""
    summary.files = sum(r.files for r in summary.roots)
    summary.groups = [g for r in summary.roots for g in r.groups]
    summary.left_out = sum(r.left_out for r in summary.roots)
    results = [r.result for r in summary.roots if r.result is not None]
    if results:
        summary.result = DuplicateReportResult(
            open_groups=sum(r.open_groups for r in results),
            added=sum(r.added for r in results),
            dropped=sum(r.dropped for r in results),
        )


def library_duplicates(*, run_id: str | None = None) -> DuplicatesSummary:
    """Find the library's duplicate groups, send them to the API, and report."""
    return run_duplicates(run_id=run_id, mode=flow_mode(MODE_FLAG))


def _record(report: Any, summary: DuplicatesSummary, duration_sec: float) -> None:
    report.duration_sec = duration_sec
    for problem in summary.problems:
        report.issue("setting", problem)
    if summary.problems:
        return
    if summary.seen_problem:
        report.notice(
            "last_run_not_compared",
            f"the last run's groups were not read or saved ({summary.seen_problem})",
        )
    if summary.unchanged:
        # The same groups as the last run: counted and logged, not posted.
        report.count("files", summary.files)
        report.count("groups", len(summary.groups))
        report.count("unchanged_since_last_run", len(summary.groups))
        if summary.result is not None:
            report.count("open_groups", summary.result.open_groups)
        report.ok(len(summary.groups))
        return
    if summary.mode is FlowMode.DRY_RUN:
        report.notice(
            "dry_run", f"{MODE_FLAG} is dry-run: the groups were not sent to the API"
        )
    if summary.left_out:
        report.notice(
            "too_many_groups",
            f"{summary.left_out} groups past the first "
            f"{DUPLICATE_REPORT_MAX_GROUPS} were not sent",
        )
    report.count("files", summary.files)
    report.count("groups", len(summary.groups))
    if summary.result is not None:
        report.count("open_groups", summary.result.open_groups)
        report.count("added", summary.result.added)
        report.count("dropped", summary.result.dropped)
    # With more than one root, every line names the root it is about.
    several = len(summary.roots) > 1
    for found in summary.roots:
        named = f"{found.root.label}: " if several else ""
        if found.failed:
            report.issue("failed", f"{found.root.label}: {found.failed}")
        if several:
            report.count(f"groups_{found.root.key}", len(found.groups))
        result = found.result
        unchanged = result is not None and not result.added and not result.dropped
        for group in [] if unchanged else found.groups:
            # The groups are named when they change (or in a dry run); open
            # groups that are the same as last run wait on the page, unrepeated.
            parts = []
            for m in group.files:
                registered = summary.paths.get(m.drive_file_id)
                path = (
                    shown_path(registered)
                    if registered is not None
                    else m.drive_file_id
                )
                parts.append(
                    f"{path} (kept: {m.why_kept})" if m.kept_on_purpose else path
                )
            report.notice(
                "group", f"{named}{', '.join(group.reasons)}: {'; '.join(parts)}"
            )
        if found.root.key == "fx":
            report.count("fx_across_folders", len(found.across_folders))
            for paths in found.across_folders:
                report.notice("fx_across_folders", f"{named}{'; '.join(paths)}")
    report.ok(len(summary.groups))
