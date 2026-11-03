"""The safety layer every flow that writes to the Drive music library uses.

The music library is what the owner DJs from, so a flow that writes to it
goes through these rules (ADR-0007, amended 2026-11-03):

- **A mode per flow**, read from the environment at the start of each run:
  ``off`` (the default, and what an unset or unknown value means), ``dry-run``
  (read, identify and compute everything, write nothing to the library) or
  ``write``. :func:`flow_mode`.
- **The audio is never altered.** Before new tag bytes are uploaded, the
  audio they carry is compared with the original's, byte for byte, with the
  tags at either end stripped (:func:`audio_payload`); after the upload the
  file is downloaded again and compared once more. A mismatch raises
  :class:`AudioChanged`, which a flow treats as the end of its run.
- **No tag is emptied.** A field the file has a value for is never written
  empty (:func:`check_no_emptied_tags`).
- **Every write is recorded** in a run record (:class:`RunRecord`): per file
  its id, original name, old and new tags, any Drive revision kept, any copy
  made and any move, saved as JSON in ``MUSIC_TAGGING_STATE_FOLDER_ID``
  before and after each step, so a run stopped part-way still leaves its
  record. A dry run saves the record it would have written, marked as such.
  ``python -m deejay_cog.music_rollback`` undoes a run from its record.

Nothing here deletes a file. The only removal it knows is Drive's trash,
which is recoverable.
"""

from __future__ import annotations

import enum
import hashlib
import io
import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

from mini_app_polis import logger as logger_mod
from mini_app_polis.deadline import without_run_deadline
from mutagen.id3 import ID3, ID3NoHeaderError

log = logger_mod.get_logger()

#: Where every flow's run records go.
STATE_FOLDER_VAR = "MUSIC_TAGGING_STATE_FOLDER_ID"

#: The app property a copy carries naming the file it was made from, so a
#: copy made by a run that stopped before recording it can still be found.
SOURCE_PROPERTY = "deejay_source_file_id"

RECORD_MIME_TYPE = "application/json"
MP3_MIME_TYPE = "audio/mpeg"


class FlowMode(enum.Enum):
    """What a flow is allowed to do this run, least to most."""

    OFF = "off"
    DRY_RUN = "dry-run"
    WRITE = "write"

    @property
    def writes(self) -> bool:
        """Whether this mode writes music."""
        return self is FlowMode.WRITE


_ORDER = [FlowMode.OFF, FlowMode.DRY_RUN, FlowMode.WRITE]


def parse_mode(raw: str | None, *, setting: str = "mode") -> FlowMode:
    """A mode from its text. Unset or unknown is ``off``, and an unknown
    value is logged, so a typo never turns writing on."""
    text = (raw or "").strip().lower()
    if not text:
        return FlowMode.OFF
    for mode in FlowMode:
        if mode.value == text:
            return mode
    log.warning("%s=%r is not off, dry-run or write; treated as off", setting, raw)
    return FlowMode.OFF


def flow_mode(var: str) -> FlowMode:
    """The mode the environment variable ``var`` sets, read now."""
    return parse_mode(os.getenv(var), setting=var)


def stricter(*modes: FlowMode) -> FlowMode:
    """The least permissive of ``modes``."""
    return min(modes, key=_ORDER.index)


# ---------------------------------------------------------------------------
# The audio check
# ---------------------------------------------------------------------------


class AudioChanged(Exception):  # noqa: N818 — named for what happened
    """New bytes would carry, or a written file carries, different audio from
    the original. The run that meets this stops."""


class AudioCheckError(AudioChanged):
    """The audio could not be told apart from its tags, so it cannot be
    shown to be unchanged. Treated as a change."""


class TagEmptied(Exception):  # noqa: N818 — named for what happened
    """A field the file has a value for would be written empty."""


def _syncsafe(raw: bytes) -> int:
    if len(raw) != 4 or any(b & 0x80 for b in raw):
        raise AudioCheckError("an ID3v2 tag has a malformed size")
    return (raw[0] << 21) | (raw[1] << 14) | (raw[2] << 7) | raw[3]


def audio_payload(data: bytes) -> bytes:
    """``data`` without its tags: every ID3v2 tag at the start (with its
    footer), and at the end an appended ID3v2 tag, an APEv2 tag and an ID3v1
    ``TAG`` block, whichever are there. What is left is the audio, which a
    tag write must leave byte for byte as it was."""
    start = 0
    while data[start : start + 3] == b"ID3":
        if len(data) < start + 10:
            raise AudioCheckError("an ID3v2 header is cut short")
        flags = data[start + 5]
        size = _syncsafe(data[start + 6 : start + 10])
        start += 10 + size + (10 if flags & 0x10 else 0)
        if start > len(data):
            raise AudioCheckError("an ID3v2 tag runs past the end of the file")

    end = len(data)
    if end - start >= 128 and data[end - 128 : end - 125] == b"TAG":
        end -= 128
    if end - start >= 32 and data[end - 32 : end - 24] == b"APETAGEX":
        size = int.from_bytes(data[end - 20 : end - 16], "little")
        flags = int.from_bytes(data[end - 12 : end - 8], "little")
        end -= size + (32 if flags & 0x80000000 else 0)
        if end < start:
            raise AudioCheckError("an APEv2 tag runs past the start of the audio")
    if end - start >= 10 and data[end - 10 : end - 7] == b"3DI":
        end -= 20 + _syncsafe(data[end - 4 : end])
        if end < start:
            raise AudioCheckError("an appended ID3v2 tag runs past the audio")
    return data[start:end]


def check_audio_unchanged(original: bytes, candidate: bytes) -> None:
    """Raise :class:`AudioChanged` unless ``candidate`` carries exactly the
    audio ``original`` does."""
    before = audio_payload(original)
    after = audio_payload(candidate)
    if not before:
        raise AudioCheckError("the original has no audio after its tags")
    if before != after:
        raise AudioChanged(
            f"audio differs: {len(before)} bytes before, {len(after)} after"
        )


#: The fields a run record shows, by ID3 frame.
_SNAPSHOT_FRAMES = {
    "title": "TIT2",
    "artist": "TPE1",
    "year": "TDRC",
    "genre": "TCON",
    "bpm": "TBPM",
    "label": "TPUB",
    "catalog_id": "TXXX:CATALOG_ID",
}


def tag_snapshot(data: bytes) -> dict[str, str]:
    """The file's tag fields a flow writes, as text (empty when absent)."""
    try:
        tags = ID3(io.BytesIO(data))
    except ID3NoHeaderError:
        tags = ID3()
    snapshot: dict[str, str] = {}
    for name, key in _SNAPSHOT_FRAMES.items():
        frame = tags.get(key)
        texts = getattr(frame, "text", None) if frame is not None else None
        snapshot[name] = str(texts[0]).strip() if texts else ""
    return snapshot


def check_no_emptied_tags(original: bytes, candidate: bytes) -> None:
    """Raise :class:`TagEmptied` when a field ``original`` has a value for is
    empty in ``candidate``."""
    before = tag_snapshot(original)
    after = tag_snapshot(candidate)
    emptied = [name for name, value in before.items() if value and not after[name]]
    if emptied:
        raise TagEmptied(f"would empty {', '.join(emptied)}")


def md5(data: bytes) -> str:
    """What Drive reports as the file's ``md5Checksum``."""
    return hashlib.md5(data, usedforsecurity=False).hexdigest()


# ---------------------------------------------------------------------------
# Drive metadata and revisions (not on the shared facade)
# ---------------------------------------------------------------------------


def file_meta(drive: Any, file_id: str) -> dict[str, Any]:
    """The file's parents, trashed flag, md5 and head revision."""
    result: dict[str, Any] = (
        drive.service.files()
        .get(
            fileId=file_id,
            fields="id,name,parents,trashed,md5Checksum,headRevisionId",
            supportsAllDrives=True,
        )
        .execute()
    )
    return result


def keep_head_revision(drive: Any, file_id: str) -> str:
    """Pin the file's current revision so Drive does not prune it, and
    return its id: what an in-place write is undone to."""
    revision_id = str(file_meta(drive, file_id)["headRevisionId"])
    drive.service.revisions().update(
        fileId=file_id, revisionId=revision_id, body={"keepForever": True}
    ).execute()
    return revision_id


def revision_bytes(drive: Any, file_id: str, revision_id: str) -> bytes:
    """The contents of one kept revision."""
    data = (
        drive.service.revisions()
        .get_media(fileId=file_id, revisionId=revision_id)
        .execute()
    )
    return bytes(data)


def update_contents(drive: Any, file_id: str, name: str, data: bytes) -> None:
    """Write ``data`` over the file's contents; name, folder and id stay."""
    suffix = os.path.splitext(name)[1] or ".mp3"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as handle:
        handle.write(data)
        path = handle.name
    try:
        drive.update_file(file_id, path, mime_type=MP3_MIME_TYPE)
    finally:
        os.unlink(path)


# ---------------------------------------------------------------------------
# The run record
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def local_run_id() -> str:
    """A run id for a run that has no queue message id."""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"local-{stamp}-{os.urandom(2).hex()}"


@dataclass
class RecordEntry:
    """What a run did, or would do, to one file.

    ``action`` is ``deliver`` (a tagged copy into the destination, the
    original to processed-originals), ``review`` (the original to
    needs-review, unchanged), ``in_place`` (tags written over the same
    file) or ``skip`` (looked at and left as it is; ``reason`` says why).
    ``folder`` is the folder the file was in, where known. ``state`` is
    ``planned`` in a dry run, ``pending`` while the steps are under way and
    ``done`` once all are; a step's field is filled in as soon as that step
    has happened.
    """

    file_id: str
    original_name: str
    action: str
    state: str = "pending"
    folder: str = ""
    reason: str = ""
    identified_by: str = ""
    old_tags: dict[str, str] = field(default_factory=dict)
    new_tags: dict[str, str] = field(default_factory=dict)
    new_name: str = ""
    revision_kept: str | None = None
    original_md5: str | None = None
    written_md5: str | None = None
    copy: dict[str, str] | None = None
    move: dict[str, str] | None = None
    error: str = ""


@dataclass
class RunRecord:
    """Every file a run touched, saved as JSON in the state folder.

    Saved on every change in a writing run (before a step and after it) and
    once at the end of a dry run, under ``<flow>-<run id>.json`` (a dry
    run's name ends ``.dry-run.json``). A save that fails in a writing run
    raises, so no write happens that its record does not show.
    """

    flow: str
    run_id: str
    mode: FlowMode
    stage: str = ""
    folders: dict[str, str] = field(default_factory=dict)
    status: str = "running"
    started_at: str = field(default_factory=_now)
    updated_at: str = ""
    entries: list[RecordEntry] = field(default_factory=list)
    _drive: Any = field(default=None, repr=False)
    _state_folder: str = field(default="", repr=False)
    _file_id: str | None = field(default=None, repr=False)

    @property
    def file_name(self) -> str:
        """The record's file name in the state folder; a dry run's is marked."""
        suffix = ".dry-run.json" if self.mode is FlowMode.DRY_RUN else ".json"
        return f"{self.flow}-{self.run_id}{suffix}"

    def attach(self, drive: Any, state_folder: str) -> RunRecord:
        """Give the record the Drive client and state folder it saves to."""
        self._drive = drive
        self._state_folder = state_folder
        return self

    def to_json(self) -> dict[str, Any]:
        """The record as the JSON object saved to the state folder."""
        return {
            "flow": self.flow,
            "run_id": self.run_id,
            "mode": self.mode.value,
            "dry_run": self.mode is FlowMode.DRY_RUN,
            "stage": self.stage,
            "folders": self.folders,
            "status": self.status,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "entries": [asdict(entry) for entry in self.entries],
        }

    def add(self, entry: RecordEntry) -> RecordEntry:
        """Append ``entry`` to the record and return it."""
        self.entries.append(entry)
        return entry

    def save(self) -> None:
        """Write the record to the state folder (create, then update).

        Nothing is saved in ``off``; in a dry run only :meth:`finish`
        saves, since nothing is written that would need undoing.
        """
        if self._drive is None or self.mode is FlowMode.OFF:
            return
        self.updated_at = _now()
        content = json.dumps(self.to_json(), indent=2, sort_keys=False).encode()
        if self._file_id is None:
            self._file_id = self._drive.upload_bytes(
                parent_id=self._state_folder,
                filename=self.file_name,
                content=content,
                mime_type=RECORD_MIME_TYPE,
            )
            return
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as handle:
            handle.write(content)
            path = handle.name
        try:
            self._drive.update_file(self._file_id, path, mime_type=RECORD_MIME_TYPE)
        finally:
            os.unlink(path)

    def checkpoint(self) -> None:
        """Save in a writing run; nothing in a dry run."""
        if self.mode.writes:
            self.save()

    def finish(self, status: str) -> None:
        """Set the final status and save. Never raises: the record already
        holds every step of a writing run."""
        self.status = status
        try:
            self.save()
        except Exception:  # noqa: BLE001 — the incremental saves stand
            log.exception("%s: could not save the final run record", self.flow)


def write_in_place(
    drive: Any,
    record: RunRecord,
    *,
    file_id: str,
    name: str,
    original: bytes,
    tagged: bytes,
    identified_by: str,
    folder: str = "",
) -> RecordEntry:
    """Write ``tagged`` over the same Drive file, safely: for the library,
    where a file is never renamed, moved or deleted.

    Checks the tags and the audio first; in a dry run stops there, with the
    entry ``planned``. Otherwise pins the current revision (the record
    names it, so the write can be undone), writes, downloads the file again
    and checks the audio once more. A mismatch then puts the original bytes
    back and raises :class:`AudioChanged`.
    """
    check_no_emptied_tags(original, tagged)
    check_audio_unchanged(original, tagged)
    entry = record.add(
        RecordEntry(
            file_id=file_id,
            original_name=name,
            action="in_place",
            folder=folder,
            identified_by=identified_by,
            old_tags=tag_snapshot(original),
            new_tags=tag_snapshot(tagged),
            new_name=name,
            original_md5=md5(original),
        )
    )
    if not record.mode.writes:
        entry.state = "planned"
        return entry
    record.checkpoint()
    entry.revision_kept = keep_head_revision(drive, file_id)
    record.checkpoint()
    update_contents(drive, file_id, name, tagged)
    entry.written_md5 = md5(tagged)
    record.checkpoint()
    written = drive.download_file_bytes(file_id).data
    try:
        check_audio_unchanged(original, written)
    except AudioChanged as exc:
        with without_run_deadline():
            update_contents(drive, file_id, name, original)
        entry.state = "restored"
        entry.error = f"audio changed after the write; original restored: {exc}"
        record.checkpoint()
        raise
    entry.state = "done"
    record.checkpoint()
    return entry


def parse_record(raw: bytes) -> dict[str, Any]:
    """A saved run record."""
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, dict) or "entries" not in data:
        raise ValueError("not a run record")
    return data
