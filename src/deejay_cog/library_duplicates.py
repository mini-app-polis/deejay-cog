"""The ``library-duplicates`` step of the nightly library run: report the
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

Each group has a key made from its files, so an unchanged library reports
the same groups every night. With ``flags.deejay_cog.library_duplicates`` at ``write`` the
groups go to the API (``POST /v1/catalog/library/duplicates``), where the
WCS duplicates page shows them; they replace the open groups for the root.
A dry run lists the groups in the run report only.
"""

from __future__ import annotations

import hashlib
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
from mini_app_polis.pipeline_status import run_report

from deejay_cog._pipeline_eval import REPO
from deejay_cog._version import __version__
from deejay_cog.api_client import api_client
from deejay_cog.library_backfill import BackfillSettings
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
class DuplicatesSummary:
    """The groups one run found, and what the API made of them."""

    mode: FlowMode
    problems: list[str] = field(default_factory=list)
    files: int = 0
    groups: list[DuplicateGroupReport] = field(default_factory=list)
    paths: dict[str, str] = field(default_factory=dict)
    left_out: int = 0
    result: DuplicateReportResult | None = None


def _normal(text: str) -> str:
    return " ".join(_NOT_WORD.sub(" ", text.casefold()).split())


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


def _edits(files: list[LibraryFile]) -> list[list[LibraryFile]]:
    """The song's files split into edits: by release label, then by
    duration, a gap of more than the tolerance starting a new edit. A file
    of unknown length joins the label's largest edit."""
    by_label: dict[str, list[LibraryFile]] = defaultdict(list)
    for f in files:
        by_label[(f.edit_label or "").casefold()].append(f)
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


def _why_kept(f: LibraryFile, edit: list[LibraryFile]) -> str:
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


def find_groups(
    files: list[LibraryFile], shown: dict[uuid.UUID, CatalogDisplay]
) -> list[DuplicateGroupReport]:
    """The duplicate groups among ``files``, sorted by key; the same files
    always give the same groups."""
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

    groups: list[DuplicateGroupReport] = []
    for song in songs.values():
        if len(song) < 2:
            continue
        for edit in _edits(song):
            if len(edit) < 2:
                continue
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
                    why_kept=_why_kept(f, edit),
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


def run_duplicates(
    *, run_id: str | None, mode: FlowMode, api: Catalog | None = None
) -> DuplicatesSummary:
    """One run: the mode, settings, the groups, the API and the report."""
    started_at = monotonic()
    run_id = run_id or local_run_id()
    summary = DuplicatesSummary(mode)
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
            settings, problems = BackfillSettings.from_env()
            if settings is None:
                summary.problems.extend(problems)
                return summary
            client = api or cast(
                Catalog, api_client(base_url=api_base_url().strip() or None)
            )
            rows = _files(client, settings.root)
            summary.files = len(rows)
            summary.paths = {r.drive_file_id: r.path for r in rows}
            groups = find_groups(rows, _displays(client, rows))
            summary.left_out = max(0, len(groups) - DUPLICATE_REPORT_MAX_GROUPS)
            summary.groups = groups[:DUPLICATE_REPORT_MAX_GROUPS]
            if mode.writes:
                summary.result = client.report_duplicate_groups(
                    DuplicateReport(
                        library_root_id=settings.root, groups=summary.groups
                    )
                )
        finally:
            _record(report, summary, monotonic() - started_at)
    return summary


def library_duplicates(*, run_id: str | None = None) -> DuplicatesSummary:
    """Find the library's duplicate groups, send them to the API, and report."""
    return run_duplicates(run_id=run_id, mode=flow_mode(MODE_FLAG))


def _record(report: Any, summary: DuplicatesSummary, duration_sec: float) -> None:
    report.duration_sec = duration_sec
    for problem in summary.problems:
        report.issue("setting", problem)
    if summary.problems:
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
    for group in summary.groups:
        parts = []
        for m in group.files:
            path = summary.paths.get(m.drive_file_id, m.drive_file_id)
            parts.append(f"{path} (kept: {m.why_kept})" if m.kept_on_purpose else path)
        report.notice("group", f"{', '.join(group.reasons)}: {'; '.join(parts)}")
    report.ok(len(summary.groups))
