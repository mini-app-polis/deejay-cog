"""The ``new-music`` worker mode: take new music in, tagged and named.

Files dropped in ``NEW_MUSIC_INPUT_FOLDER_ID`` are usually new releases,
not yet in the play history or the catalog (ADR-0007, amended 2026-11-03).
For each file, at most ``NEW_MUSIC_MAX_FILES_PER_RUN`` a run:

1. **By its tags, in the catalog**, exactly as retag-music looks a file up
   (:mod:`deejay_cog.music_tags`). A match is identified: its tags are the
   catalog's.
2. **Otherwise, by its fingerprint.** The file goes to the fingerprint
   function, one message per file marked as this flow's
   (:mod:`deejay_cog.fingerprint_jobs`), which identifies it with AcoustID
   and MusicBrainz and, only when the score reaches
   ``RETAG_MIN_CONFIDENCE``, takes it the rest of the way with the same
   :class:`Intake` steps; below the score it goes to needs-review. That
   includes a file with a good title and artist the catalog does not know:
   its own tags are not taken on trust.

An identified file is **copied**, never changed: its tagged bytes are
uploaded as a new file named ``{title}_{artist}.<ext>`` into
``NEW_MUSIC_DESTINATION_FOLDER_ID``, and the original is then moved,
unchanged, to ``NEW_MUSIC_ORIGINALS_FOLDER_ID``. The name carries the main
artist only (the artist tag up to a feat./ft./featuring credit); when that
name is already taken, in the destination or by any file in the library
(the API's library registry), the featured artists are added. A file that
cannot be identified with confidence, whose name is taken even so (or has
no featured artists to add), or whose name cannot be checked because the
registry cannot be read, is moved unchanged to ``NEW_MUSIC_REVIEW_FOLDER_ID``. Nothing
is deleted and nothing is overwritten. This is the only flow that names a
file; files already in the library are never renamed.

Everything goes through :mod:`deejay_cog.music_safety`: ``NEW_MUSIC_MODE``
(``off`` unless set to ``dry-run`` or ``write``), the audio check before
and after each upload (a mismatch stops the run), no emptied tags, and the
run record in ``MUSIC_TAGGING_STATE_FOLDER_ID`` that
``python -m deejay_cog.music_rollback`` undoes a run from.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from time import monotonic
from typing import Any, Literal, Protocol, cast

from mini_app_polis import logger as logger_mod
from mini_app_polis.api.contract import CatalogLookupRequest
from mini_app_polis.deadline import without_run_deadline
from mini_app_polis.environment import api_base_url
from mini_app_polis.google import GoogleAPI
from mini_app_polis.lambda_worker import RunOutOfTime
from mini_app_polis.pipeline_status import run_report

from deejay_cog._pipeline_eval import REPO, post_run_finding
from deejay_cog._shared_deadline import shared_deadline_passed
from deejay_cog._version import __version__
from deejay_cog.api_client import api_client
from deejay_cog.fingerprint_jobs import FingerprintJobs, fingerprint_jobs_from_env
from deejay_cog.music_safety import (
    MP3_MIME_TYPE,
    SOURCE_PROPERTY,
    STATE_FOLDER_VAR,
    AudioChanged,
    FlowMode,
    RecordEntry,
    RunRecord,
    TagEmptied,
    check_audio_unchanged,
    check_no_emptied_tags,
    file_meta,
    flow_mode,
    local_run_id,
    md5,
    tag_snapshot,
)
from deejay_cog.music_tags import (
    CatalogLookup,
    ingest_track,
    read_tag_row,
    write_catalog_tags,
)

log = logger_mod.get_logger()

ModeText = Literal["off", "dry-run", "write"]

FLOW_NAME = "new-music"

#: off | dry-run | write; unset or unknown is off.
MODE_VAR = "NEW_MUSIC_MODE"

#: Files per run when ``NEW_MUSIC_MAX_FILES_PER_RUN`` is unset; 0 is no cap.
MAX_FILES_VAR = "NEW_MUSIC_MAX_FILES_PER_RUN"
DEFAULT_MAX_FILES_PER_RUN = 25

#: Every folder the flow needs, by role. All must be set, and all differ.
FOLDER_VARS = {
    "input": "NEW_MUSIC_INPUT_FOLDER_ID",
    "destination": "NEW_MUSIC_DESTINATION_FOLDER_ID",
    "review": "NEW_MUSIC_REVIEW_FOLDER_ID",
    "originals": "NEW_MUSIC_ORIGINALS_FOLDER_ID",
    "state": STATE_FOLDER_VAR,
}

_AUDIO_MIME_TYPES = (
    "audio/mpeg",
    "audio/mp4",
    "audio/x-m4a",
    "audio/wav",
    "audio/x-wav",
    "audio/flac",
    "audio/aac",
    "audio/ogg",
    "audio/x-aiff",
    "audio/aiff",
)

#: Characters a file name must not carry: path separators, Windows' reserved
#: ones and control characters (VirtualDJ's machine syncs the folder).
_UNSAFE_NAME = re.compile(r'[\x00-\x1f\x7f/\\:*?"<>|]+')
_MAX_NAME_LENGTH = 200


@dataclass(frozen=True)
class IntakeFolders:
    """The folders one run works between."""

    input: str
    destination: str
    review: str
    originals: str
    state: str

    @classmethod
    def from_env(cls) -> tuple[IntakeFolders | None, list[str]]:
        """The folders, or None and why not: every one must be set, and no
        two may be the same folder."""
        values = {role: os.getenv(var, "").strip() for role, var in FOLDER_VARS.items()}
        problems = [f"{FOLDER_VARS[r]} is not set" for r, v in values.items() if not v]
        if not problems and len(set(values.values())) < len(values):
            problems.append(
                "the new-music folders must all be different folders: "
                + ", ".join(FOLDER_VARS.values())
            )
        if problems:
            return None, problems
        return cls(**values), []

    def as_dict(self) -> dict[str, str]:
        """The folder ids by role, for the run record."""
        return {
            "input": self.input,
            "destination": self.destination,
            "review": self.review,
            "originals": self.originals,
            "state": self.state,
        }


@dataclass
class IntakeSummary:
    """What one run did, or in a dry run would do, file by file."""

    mode: FlowMode
    record_name: str = ""
    scanned: int = 0
    identified: list[str] = field(default_factory=list)
    sent_to_fingerprinting: list[str] = field(default_factory=list)
    needs_fingerprinting: list[str] = field(default_factory=list)
    needs_review: list[str] = field(default_factory=list)
    collisions: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    no_longer_in_input: list[str] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)
    aborted: str = ""


def safe_name_part(text: str) -> str:
    """``text`` fit for a file name: no separators or control characters."""
    cleaned = " ".join(_UNSAFE_NAME.sub(" ", text).split())
    return cleaned.strip(" .")


_FEAT = re.compile(r"\s*[\[(]?\s*\b(?:feat\.?|ft\.?|featuring)\s+", re.IGNORECASE)


def split_artist(artist: str) -> tuple[list[str], list[str]]:
    """The main artists and the featured artists an artist tag credits:
    split at a feat./ft./featuring credit, then on commas.

    "Wale feat. Usher, Rick Ross" -> (["Wale"], ["Usher", "Rick Ross"]).
    """
    head, *tail = _FEAT.split(artist, maxsplit=1)

    def names(text: str) -> list[str]:
        return [n.strip(" )]") for n in text.split(",") if n.strip(" )]")]

    return names(head), names(tail[0]) if tail else []


def delivery_names(tags: dict[str, str], original_name: str) -> list[str]:
    """The names a delivered file may take, in order: ``{title}_{artist}``
    with the main artist only, then, when the song has featured artists,
    with them added (``{title}_{artist} feat. {featured}``). Empty when the
    title or artist is missing. The extension is the original's."""
    title = safe_name_part(tags.get("title", ""))
    main, featured = split_artist(tags.get("artist", ""))
    artist = safe_name_part(", ".join(main))
    if not title or not artist:
        return []
    ext = os.path.splitext(original_name)[1].lower() or ".mp3"
    if not re.fullmatch(r"\.[a-z0-9]{1,5}", ext):
        ext = ".mp3"

    def named(credit: str) -> str:
        return f"{title}_{credit}"[:_MAX_NAME_LENGTH].rstrip(" .") + ext

    names = [named(artist)]
    with_featured = safe_name_part(f"{artist} feat. {', '.join(featured)}")
    if featured and named(with_featured) not in names:
        names.append(named(with_featured))
    return names


def delivery_name(tags: dict[str, str], original_name: str) -> str | None:
    """The first name a delivered file may take, or None."""
    names = delivery_names(tags, original_name)
    return names[0] if names else None


class LibraryFiles(Protocol):
    """The part of the API client that lists the library registry."""

    def list_library_files(
        self,
        *,
        present: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[Any]:
        """One page of registered library files."""
        ...


class RegistryUnavailable(Exception):  # noqa: N818 — named for what happened
    """The library registry could not be read, so a name cannot be checked."""


class LibraryNames:
    """The file names already in the library, from the API's library
    registry: read once, on the first name checked, then kept for the run."""

    PAGE = 500

    def __init__(self, api: LibraryFiles | None) -> None:
        self.api = api
        self._names: set[str] | None = None

    def taken(self, name: str) -> bool:
        """Whether a present library file already has this name (any case)."""
        if self.api is None:
            return False
        if self._names is None:
            names: set[str] = set()
            offset = 0
            try:
                while True:
                    page = self.api.list_library_files(
                        present=True, limit=self.PAGE, offset=offset
                    )
                    names.update(
                        str(row.path).rsplit("/", 1)[-1].lower() for row in page
                    )
                    if len(page) < self.PAGE:
                        break
                    offset += self.PAGE
            except Exception as exc:
                raise RegistryUnavailable(f"{type(exc).__name__}: {exc}") from exc
            self._names = names
        return name.lower() in self._names


def is_mp3(downloaded: Any, name: str) -> bool:
    """Whether a downloaded file is an MP3, by its type or name: only ID3
    tags are written."""
    mime_type = str(getattr(downloaded, "mime_type", "") or "")
    return mime_type == MP3_MIME_TYPE or name.lower().endswith(".mp3")


class NotInInput(Exception):  # noqa: N818 — named for what happened
    """The file is not in the input folder, so it is not moved: only a file
    in the input folder is ever moved by this flow."""


class Intake:
    """The steps after a file is identified, or found not to be, shared by
    this mode and the fingerprint function. Each honours the run's mode and
    records itself before and after it writes."""

    def __init__(
        self,
        drive: Any,
        folders: IntakeFolders,
        record: RunRecord,
        summary: IntakeSummary,
        library: LibraryNames | None = None,
    ) -> None:
        self.drive = drive
        self.folders = folders
        self.record = record
        self.summary = summary
        self.library = library or LibraryNames(None)

    @property
    def mode(self) -> FlowMode:
        """The mode this run was started in."""
        return self.record.mode

    def _move_from_input(self, file_id: str, to_folder: str) -> None:
        """Move a file out of the input folder, and only out of it: Drive's
        move takes a file from every folder it is in, so one that is
        anywhere but the input folder now is refused."""
        meta = file_meta(self.drive, file_id)
        if meta.get("trashed") or (meta.get("parents") or []) != [self.folders.input]:
            raise NotInInput(
                f"{file_id} is no longer only in the input folder; not moved"
            )
        self.drive.move_file(file_id, new_parent_id=to_folder)

    def deliver(
        self,
        file_id: str,
        name: str,
        original: bytes,
        tagged: bytes,
        *,
        identified_by: str,
    ) -> None:
        """Copy the tagged file into the destination under its new name, then
        move the original, unchanged, to processed-originals.

        Raises :class:`TagEmptied` before anything is written when a tag
        would be emptied, and :class:`AudioChanged` when the audio would
        differ (before the upload) or does (after it; the copy is then
        trashed and the original stays where it is).
        """
        check_no_emptied_tags(original, tagged)
        check_audio_unchanged(original, tagged)
        new_tags = tag_snapshot(tagged)
        candidates = delivery_names(new_tags, name)
        if not candidates:
            self.review(file_id, name, original, "no title or artist to name it by")
            return

        earlier = self.drive.find_files_by_app_property(
            self.folders.destination, key=SOURCE_PROPERTY, value=file_id
        )
        if earlier:
            # The copy an earlier run made keeps the name it was given.
            new_name = str(
                file_meta(self.drive, earlier[0]).get("name") or candidates[0]
            )
        else:
            try:
                free = [c for c in candidates if not self._name_taken(c)]
            except RegistryUnavailable as exc:
                self.review(
                    file_id,
                    name,
                    original,
                    f"the library registry could not be read to check the name ({exc})",
                )
                return
            if not free:
                self.summary.collisions.append(f"{name} -> {candidates[-1]}")
                tried = " or ".join(candidates)
                self.review(
                    file_id,
                    name,
                    original,
                    f"name collision: {tried} is already in the library or the "
                    "destination",
                    count=False,
                )
                return
            new_name = free[0]

        entry = self.record.add(
            RecordEntry(
                file_id=file_id,
                original_name=name,
                action="deliver",
                identified_by=identified_by,
                old_tags=tag_snapshot(original),
                new_tags=new_tags,
                new_name=new_name,
                written_md5=md5(tagged),
            )
        )
        self.summary.identified.append(f"{name} -> {new_name}")
        if not self.mode.writes:
            entry.state = "planned"
            return
        self.record.checkpoint()

        if earlier:
            # An earlier run uploaded the copy and stopped before moving the
            # original: check that copy rather than make a second one.
            copy_id = earlier[0]
            entry.copy = {
                "id": copy_id,
                "folder_id": self.folders.destination,
                "name": new_name,
                "made_by": "an earlier run",
            }
        else:
            copy_id = self.drive.upload_bytes(
                parent_id=self.folders.destination,
                filename=new_name,
                content=tagged,
                mime_type=MP3_MIME_TYPE,
                app_properties={SOURCE_PROPERTY: file_id},
                resumable=True,
            )
            entry.copy = {
                "id": copy_id,
                "folder_id": self.folders.destination,
                "name": new_name,
            }
        self.record.checkpoint()

        written = self.drive.download_file_bytes(copy_id).data
        try:
            check_audio_unchanged(original, written)
        except AudioChanged as exc:
            entry.state = "failed"
            entry.error = f"audio changed in the uploaded copy: {exc}"
            if not earlier:
                with without_run_deadline():
                    self.drive.trash_file(copy_id)
                entry.copy["trashed"] = "true"
            self.record.checkpoint()
            raise

        self._move_from_input(file_id, self.folders.originals)
        entry.move = {"from": self.folders.input, "to": self.folders.originals}
        entry.state = "done"
        self.record.checkpoint()

    def _name_taken(self, new_name: str) -> bool:
        """Whether a file of this name is in the destination or the library."""
        if self.drive.find_file_in_folder(self.folders.destination, name=new_name):
            return True
        return self.library.taken(new_name)

    def review(
        self,
        file_id: str,
        name: str,
        original: bytes | None,
        reason: str,
        *,
        count: bool = True,
    ) -> None:
        """Move the file, unchanged, to needs-review."""
        entry = self.record.add(
            RecordEntry(
                file_id=file_id,
                original_name=name,
                action="review",
                reason=reason,
                old_tags=tag_snapshot(original) if original else {},
            )
        )
        if count:
            self.summary.needs_review.append(f"{name} ({reason})")
        if not self.mode.writes:
            entry.state = "planned"
            return
        self.record.checkpoint()
        self._move_from_input(file_id, self.folders.review)
        entry.move = {"from": self.folders.input, "to": self.folders.review}
        entry.state = "done"
        self.record.checkpoint()

    def fingerprinting(self, file_id: str, name: str, reason: str) -> None:
        """Note, in the record, a file handed to the fingerprint function."""
        self.record.add(
            RecordEntry(
                file_id=file_id,
                original_name=name,
                action="fingerprint",
                state="sent",
                reason=reason,
            )
        )


# ---------------------------------------------------------------------------
# Stage 1: the worker mode
# ---------------------------------------------------------------------------


def list_audio_files(drive: Any, folder_id: str) -> list[Any]:
    """The audio files directly in a Drive folder, each once."""
    files: list[Any] = []
    seen: set[str] = set()
    for mime_type in _AUDIO_MIME_TYPES:
        for f in drive.list_files(
            parent_id=folder_id, mime_type=mime_type, trashed=False
        ):
            file_id = getattr(f, "id", None)
            if file_id and file_id not in seen:
                seen.add(file_id)
                files.append(f)
    return files


def max_files_per_run() -> int:
    """The most files one run takes in (NEW_MUSIC_MAX_FILES_PER_RUN)."""
    raw = os.getenv(MAX_FILES_VAR, "").strip()
    return int(raw) if raw else DEFAULT_MAX_FILES_PER_RUN


def intake_one(
    intake: Intake,
    api: CatalogLookup,
    file_id: str,
    fingerprint_jobs: FingerprintJobs | None,
) -> None:
    """Identify one file by its tags and take it in, or hand it on."""
    downloaded = intake.drive.download_file_bytes(file_id)
    name = str(getattr(downloaded, "name", "") or file_id)
    data: bytes = downloaded.data
    if not is_mp3(downloaded, name):
        intake.review(file_id, name, None, "not an MP3: only ID3 tags are written")
        return

    track = ingest_track(read_tag_row(data))
    match = (
        api.lookup_catalog(CatalogLookupRequest(tracks=[track]))[0]
        if track is not None
        else None
    )
    if match is not None:
        tagged = write_catalog_tags(data, match) or data
        intake.deliver(file_id, name, data, tagged, identified_by="catalog")
        return

    reason = "not in the catalog" if track is not None else "no usable title or artist"
    if fingerprint_jobs is None:
        # No queue (a local run): left in the input folder for a later run.
        intake.summary.needs_fingerprinting.append(f"{name} ({reason})")
        return
    fingerprint_jobs.send(
        file_id, name, flow="new-music", mode=cast(ModeText, intake.mode.value)
    )
    intake.fingerprinting(file_id, name, reason)
    intake.summary.sent_to_fingerprinting.append(f"{name} ({reason})")


def intake_files(
    intake: Intake,
    api: CatalogLookup,
    files: list[Any],
    *,
    max_files: int,
    fingerprint_jobs: FingerprintJobs | None,
) -> None:
    """Take in up to ``max_files`` of ``files`` (all when 0).

    Raises ``RunOutOfTime`` between files once the shared deadline has
    passed, and :class:`AudioChanged` the moment a file's audio check
    fails: the run stops there. Any other failure is that file's alone.
    """
    summary = intake.summary
    for f in files:
        if max_files > 0 and summary.scanned >= max_files:
            log.info("new-music: reached %d files for this run", max_files)
            break
        name = str(getattr(f, "name", "") or getattr(f, "id", ""))
        if shared_deadline_passed():
            raise RunOutOfTime(f"run deadline reached before {name}")
        summary.scanned += 1
        try:
            intake_one(intake, api, f.id, fingerprint_jobs)
        except AudioChanged:
            summary.failed.append(name)
            raise
        except TagEmptied as exc:
            summary.failed.append(f"{name} ({exc})")
        except Exception as exc:  # noqa: BLE001 — one file must not end the sweep
            if shared_deadline_passed():
                raise RunOutOfTime(f"run deadline reached processing {name}") from exc
            log.exception("new-music: failed on %s", name)
            summary.failed.append(f"{name} ({type(exc).__name__}: {exc})")


def run_intake(
    *,
    stage: str,
    run_id: str | None,
    mode: FlowMode,
    work: Callable[[Intake], None],
    drive: Any | None = None,
    library: LibraryFiles | None = None,
) -> IntakeSummary:
    """One run of either stage: the mode, the settings, the run record, the
    report, and the stop on an audio mismatch, around ``work``."""
    started_at = monotonic()
    run_id = run_id or local_run_id()
    summary = IntakeSummary(mode)
    with run_report(
        FLOW_NAME,
        repo=REPO,
        notable=True,
        run_id=run_id,
        version=__version__,
    ) as report:
        try:
            if mode is FlowMode.OFF:
                return summary
            folders, problems = IntakeFolders.from_env()
            if folders is None:
                summary.problems.extend(problems)
                return summary
            if drive is None:
                drive = GoogleAPI.from_env().drive
            record = RunRecord(
                FLOW_NAME, run_id, mode, stage=stage, folders=folders.as_dict()
            ).attach(drive, folders.state)
            summary.record_name = record.file_name
            intake = Intake(drive, folders, record, summary, LibraryNames(library))
            status = "failed"
            try:
                work(intake)
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
            if summary.aborted:
                _report_abort(run_id, summary)
        finally:
            _record(report, summary, monotonic() - started_at)
    return summary


def _report_abort(run_id: str, summary: IntakeSummary) -> None:
    try:
        post_run_finding(
            FLOW_NAME,
            "ERROR",
            "new-music stopped: a file's audio would not stay byte-identical "
            f"({summary.aborted}). Nothing was written for that file; see "
            f"{summary.record_name}.",
            run_id=run_id,
        )
    except Exception:  # noqa: BLE001 — the notification is not the job
        log.exception("new-music: could not report the stop")


def new_music(*, run_id: str | None = None) -> IntakeSummary:
    """Take in the input folder's files and report the run.

    ``run_id`` is the queue message id when the Lambda worker runs this.
    """
    mode = flow_mode(MODE_VAR)
    api = api_client(base_url=api_base_url().strip() or None)

    def work(intake: Intake) -> None:
        files = list_audio_files(intake.drive, intake.folders.input)
        intake_files(
            intake,
            api,
            files,
            max_files=max_files_per_run(),
            fingerprint_jobs=fingerprint_jobs_from_env(),
        )

    return run_intake(stage="intake", run_id=run_id, mode=mode, work=work, library=api)


def _record(report: Any, summary: IntakeSummary, duration_sec: float) -> None:
    report.duration_sec = duration_sec
    if summary.mode is FlowMode.OFF:
        report.notice("off", f"{MODE_VAR} is off: nothing was read or written")
        return
    for problem in summary.problems:
        report.issue("setting", problem)
    if summary.problems:
        return
    dry = summary.mode is FlowMode.DRY_RUN
    if dry:
        report.notice(
            "dry_run",
            f"{MODE_VAR} is dry-run: nothing was written; would-be record "
            f"{summary.record_name}",
        )
    report.ok(0 if dry else len(summary.identified))
    report.count("scanned", summary.scanned)
    report.count("identified", len(summary.identified))
    report.count("sent_to_fingerprinting", len(summary.sent_to_fingerprinting))
    report.count("needs_fingerprinting", len(summary.needs_fingerprinting))
    report.count("needs_review", len(summary.needs_review))
    report.count("collisions", len(summary.collisions))
    report.count("failed", len(summary.failed))
    report.count("no_longer_in_input", len(summary.no_longer_in_input))
    if dry:
        planned = (
            len(summary.identified)
            + len(summary.needs_review)
            + len(summary.collisions)
        )
        report.count("not_written_dry_run", planned)
    for name in summary.identified:
        if dry:
            report.notice("would_identify", name)
        else:
            report.created("tagged copy", name)
    for name in summary.sent_to_fingerprinting:
        report.notice("sent_to_fingerprinting", name)
    for name in summary.needs_fingerprinting:
        report.notice("needs_fingerprinting", name)
    for name in summary.needs_review:
        report.notice("would_need_review" if dry else "needs_review", name)
    for name in summary.collisions:
        report.issue("name_collision", name)
    for name in summary.failed:
        report.issue("failed", name)
    if summary.aborted:
        report.issue("audio_changed_run_stopped", summary.aborted)


if __name__ == "__main__":
    new_music()
