"""Write each FX Music file's role, from its folder, at the start of its comment.

An FX Music file's role comes from the nearest role folder in its path
(ADR-0010, amended 2026-11-14) and is written as ``<FX><Role>`` at the start
of its comment, by the shared rule (:func:`role_comment`). The rest of the
comment, the owner's hand-added ``<...>`` parts included, is kept as it is.
Run locally, against production:

    doppler run --config prd -- uv run python -m deejay_cog.fx_roles
    doppler run --config prd -- uv run python -m deejay_cog.fx_roles --write

It reads every present FX Music file the registry has (``FX_LIBRARY_ROOT_FOLDER_ID``)
under a role folder, reads its comment through the shared tagger (every
format it supports), and reports:

- the files whose comment it writes (without ``--write``: would write), with
  the comment before and after;
- the files already tagged with their folder's role;
- the role conflicts, a comment that starts with ``<FX>`` and another role,
  left as they are for the owner to settle;
- the files the tagger cannot read.

Without ``--write`` nothing is changed, not even a run record. With
``--write`` only the comment is written, the safe way
(:func:`write_in_place`: revision pinned, audio checked), and the writes
are recorded as an ``fx-roles`` run, which ``deejay_cog.music_rollback``
can undo. No other tag is written, no file is renamed, moved or trashed,
and a main-library file is never read. Running it again changes only the
files that are new or have moved since.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any

from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import LibraryFile
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI
from mini_app_polis.mp3.tag import (
    AudioFormat,
    AudioTagger,
    UnsupportedFile,
    role_comment,
    role_conflict,
)

from deejay_cog import audio_formats
from deejay_cog.api_client import api_client
from deejay_cog.fx_untagged import Api, fx_rows
from deejay_cog.library_backfill import FX_ROOT_FOLDER_VAR
from deejay_cog.music_safety import (
    STATE_FOLDER_VAR,
    FlowMode,
    RunRecord,
    local_run_id,
    write_in_place,
)

log = logger_mod.get_logger()

FLOW_NAME = "fx-roles"

#: The FX Music folders that name a role, and the role each names
#: (ADR-0010, amended 2026-11-14, decision 1).
FOLDER_ROLES: dict[str, str] = {
    "_WalkOn": "Walk On",
    "_BowMusic": "Bow",
    "_MCIntro": "MC Intro",
    "_RotationMusic": "Rotation",
}


def folder_role(path: str) -> str | None:
    """The role of the nearest role folder ``path`` is under, or None when
    it is under none."""
    for folder in reversed(PurePosixPath(path).parent.parts):
        if folder in FOLDER_ROLES:
            return FOLDER_ROLES[folder]
    return None


def _format(data: bytes) -> AudioFormat:
    fmt = audio_formats.other_format(data)
    if fmt is None:
        return AudioFormat.MP3
    if isinstance(fmt, UnsupportedFile):
        raise audio_formats.NotTaggable(f"{fmt.reason.value} ({fmt.detail})")
    return fmt


def read_comment(data: bytes) -> str:
    """The file's comment as the shared tagger reads it ("" for none).
    Raises :class:`audio_formats.NotTaggable` when the tagger cannot read it."""
    fmt = _format(data)
    with audio_formats.local_copy(data, fmt) as local:
        snapshot = AudioTagger().read(local)
    if isinstance(snapshot, UnsupportedFile):
        raise audio_formats.NotTaggable(
            f"{snapshot.reason.value}: {snapshot.detail}".rstrip(": ")
        )
    return snapshot.tags.get("comment", "")


def with_comment(data: bytes, comment: str) -> bytes:
    """The file's bytes with only its comment set to ``comment``, in its own
    format; every other tag is left as it is."""
    fmt = _format(data)
    with audio_formats.local_copy(data, fmt) as local:
        AudioTagger().write(local, {"comment": comment})
        with open(local, "rb") as handle:
            return handle.read()


@dataclass(frozen=True)
class Change:
    """A comment written, or that would be."""

    path: str
    before: str
    after: str


@dataclass(frozen=True)
class Conflict:
    """A comment that carries another role than its folder's."""

    path: str
    comment: str
    role: str


@dataclass
class Report:
    """What a run wrote, or would, and what it left for the owner."""

    written: list[Change] = field(default_factory=list)
    tagged: list[str] = field(default_factory=list)
    conflicts: list[Conflict] = field(default_factory=list)
    unreadable: list[tuple[str, str]] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)
    #: The run record's name, when a writing run recorded any write.
    record: str = ""


def run(
    drive: Any,
    api: Api,
    fx_root: str,
    state_folder: str,
    *,
    write: bool,
    run_id: str | None = None,
) -> Report:
    """Tag every present FX Music file under a role folder with its role;
    without ``write``, only report what would be written."""
    mode = FlowMode.WRITE if write else FlowMode.DRY_RUN
    record = RunRecord(FLOW_NAME, run_id or local_run_id(), mode)
    if write:
        record.attach(drive, state_folder)
    report = Report()
    for row in sorted(fx_rows(api, fx_root), key=lambda r: r.path.lower()):
        role = folder_role(row.path)
        if role is None or row.trash_requested:
            continue
        try:
            _one(drive, record, row, role, report, write=write)
        except Exception as exc:  # noqa: BLE001 — one file must not stop the rest
            log.exception("fx-roles: failed on %s", row.path)
            report.failed.append((row.path, f"{type(exc).__name__}: {exc}"))
    if write and record.entries:
        record.finish("failed" if report.failed else "complete")
        report.record = record.file_name
    return report


def _one(
    drive: Any,
    record: RunRecord,
    row: LibraryFile,
    role: str,
    report: Report,
    *,
    write: bool,
) -> None:
    data: bytes = drive.download_file_bytes(row.drive_file_id).data
    try:
        comment = read_comment(data)
    except audio_formats.NotTaggable as exc:
        report.unreadable.append((row.path, str(exc)))
        return
    new = role_comment(comment, role)
    if new is None:
        if role_conflict(comment, role) is not None:
            report.conflicts.append(Conflict(row.path, comment, role))
        else:
            report.tagged.append(row.path)
        return
    entry = write_in_place(
        drive,
        record,
        file_id=row.drive_file_id,
        name=row.path,
        original=data,
        tagged=with_comment(data, new),
        identified_by=f"fx-roles (role from its folder: {role})",
    )
    if write and entry.state != "done":
        report.failed.append((row.path, f"not written: {entry.state}"))
        return
    report.written.append(Change(row.path, comment, new))


def render(report: Report, *, write: bool) -> list[str]:
    """The report as lines for the terminal."""
    verb = "Wrote" if write else "Would write"
    lines = [f"{verb} {len(report.written)} comments:"]
    lines += [f"  {c.path}: {c.before!r} -> {c.after!r}" for c in report.written]
    lines.append(f"Already tagged with their folder's role: {len(report.tagged)}")
    lines += [f"  {p}" for p in report.tagged]
    lines.append(
        f"Role conflicts, left for the owner to settle: {len(report.conflicts)}"
    )
    lines += [
        f"  {c.path}: comment {c.comment!r}, folder's role {c.role!r}"
        for c in report.conflicts
    ]
    lines.append(f"Not readable by the tagger: {len(report.unreadable)}")
    lines += [f"  {p}: {why}" for p, why in report.unreadable]
    if report.failed:
        lines.append(f"Failed: {len(report.failed)}")
        lines += [f"  {p}: {why}" for p, why in report.failed]
    return lines


def main(argv: list[str] | None = None) -> int:
    """List, or write, each FX Music file's role comment."""
    parser = argparse.ArgumentParser(
        prog="python -m deejay_cog.fx_roles",
        description="Write each FX Music file's role, from its folder, at the start of its comment.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="write the comments (without this, only print what would be written)",
    )
    args = parser.parse_args(argv)
    fx_root = os.getenv(FX_ROOT_FOLDER_VAR, "").strip()
    state_folder = os.getenv(STATE_FOLDER_VAR, "").strip()
    if not fx_root or (args.write and not state_folder):
        print(
            f"{FX_ROOT_FOLDER_VAR} (and {STATE_FOLDER_VAR} with --write) must be set; nothing done.",
            file=sys.stderr,
        )
        return 2
    drive = GoogleAPI.from_env().drive
    api = api_client(base_url=api_base_url().strip() or None)
    report = run(drive, api, fx_root, state_folder, write=args.write)
    print("\n".join(render(report, write=args.write)))
    if args.write:
        if report.record:
            print(f"Recorded as {report.record}; music_rollback can undo the writes.")
    else:
        print("Nothing was changed. Run again with --write to write the comments.")
    return 1 if report.failed else 0


if __name__ == "__main__":
    sys.exit(main())
