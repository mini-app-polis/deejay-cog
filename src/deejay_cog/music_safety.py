"""The safety layer every flow that writes to the Drive music library uses.

The music library is what the owner DJs from, so a flow that writes to it
goes through these rules (ADR-0007, amended 2026-11-03):

- **A mode per flow**, read from the flow's feature flag in the API
  (``flags.deejay_cog.STEP``) at the start of each run: ``off`` (what a
  missing, switched-off or unknown flag, or an API that cannot be read,
  means), ``dry-run`` (read, identify and compute everything, write nothing
  to the library) or ``write``. :func:`flow_mode`.
- **The audio is never altered.** Before new tag bytes are uploaded, the
  audio they carry is compared with the original's, byte for byte, with the
  tags at either end stripped (:func:`audio_payload`); after the upload the
  file is downloaded again and compared once more. A file in another format
  (FX Music: :mod:`deejay_cog.audio_formats`) is compared by its audio
  outside its tag block, taken apart per format, and by its stream info. A mismatch raises
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

import contextlib
import enum
import hashlib
import io
import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

from googleapiclient.errors import HttpError
from mini_app_polis import logger as logger_mod
from mini_app_polis.deadline import without_run_deadline
from mini_app_polis.environment import api_base_url
from mini_app_polis.google._retry import RetryConfig, execute_with_retry
from mutagen.id3 import ID3, ID3NoHeaderError

from deejay_cog import audio_formats
from deejay_cog.api_client import api_client

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


def _read_flags() -> dict[str, str | None]:
    """Every feature flag's mode text by name, read from the API now.

    A flag that is switched off reads as ``off`` whatever its value, so
    turning a flag off always stops its step.
    """
    client = api_client(base_url=api_base_url().strip() or None)
    return {
        flag.name: flag.value if flag.enabled else FlowMode.OFF.value
        for flag in client.list_flags()
    }


def flow_modes(*flags: str) -> dict[str, FlowMode]:
    """The modes the feature flags ``flags`` set, read from the API once.

    Fails closed: when the flags cannot be read, every one of them is off
    for this run, and a flag the API does not have is off.
    """
    try:
        values = _read_flags()
    except Exception as exc:  # any failure to read the flags is off
        log.warning(
            "could not read the feature flags (%s: %s); %s off for this run",
            type(exc).__name__,
            exc,
            ", ".join(flags),
        )
        return dict.fromkeys(flags, FlowMode.OFF)
    modes: dict[str, FlowMode] = {}
    for flag in flags:
        if flag not in values:
            log.warning("feature flag %s was not found; treated as off", flag)
            modes[flag] = FlowMode.OFF
        else:
            modes[flag] = parse_mode(values[flag], setting=flag)
    return modes


def flow_mode(flag: str) -> FlowMode:
    """The mode the feature flag ``flag`` sets, read from the API now."""
    return flow_modes(flag)[flag]


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
    fmt = audio_formats.other_format(original)
    if fmt is not None:
        _check_other_audio_unchanged(fmt, original, candidate)
        return
    before = audio_payload(original)
    after = audio_payload(candidate)
    if not before:
        raise AudioCheckError("the original has no audio after its tags")
    if before != after:
        raise AudioChanged(
            f"audio differs: {len(before)} bytes before, {len(after)} after"
        )


def _check_other_audio_unchanged(
    fmt: audio_formats.AudioFormat | audio_formats.UnsupportedFile,
    original: bytes,
    candidate: bytes,
) -> None:
    """:func:`check_audio_unchanged` for a format other than MP3: the same
    format, the same audio outside the tag block, the same stream info."""
    if isinstance(fmt, audio_formats.UnsupportedFile):
        raise AudioCheckError(f"no audio check for {fmt.detail or 'this file'}")
    if audio_formats.other_format(candidate) is not fmt:
        raise AudioChanged(f"the file is no longer {fmt.value}")
    try:
        before = audio_formats.audio_payload(fmt, original)
        after = audio_formats.audio_payload(fmt, candidate)
        info_before = audio_formats.stream_info(original)
        info_after = audio_formats.stream_info(candidate)
    except Exception as exc:  # noqa: BLE001 — a file not taken apart is not checked
        raise AudioCheckError(
            f"the {fmt.value} audio could not be read: {exc}"
        ) from exc
    if not before:
        raise AudioCheckError(f"the original {fmt.value} has no audio")
    if before != after:
        raise AudioChanged(
            f"{fmt.value} audio differs: {len(before)} bytes before, {len(after)} after"
        )
    if info_before != info_after:
        raise AudioChanged(
            f"{fmt.value} stream differs: {info_before} before, {info_after} after"
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
    fmt = audio_formats.other_format(data)
    if fmt is not None:
        return _other_snapshot(data, fmt)
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


#: The shared tagger's key for each field a run record shows.
_SNAPSHOT_KEYS = {
    "title": "tracktitle",
    "artist": "artist",
    "year": "year",
    "genre": "genre",
    "bpm": "bpm",
    "label": "label",
}


def _other_snapshot(
    data: bytes, fmt: audio_formats.AudioFormat | audio_formats.UnsupportedFile
) -> dict[str, str]:
    """:func:`tag_snapshot` for a format other than MP3; every field empty
    when the shared tagger cannot read it."""
    tags: dict[str, str] = {}
    link = ""
    if isinstance(fmt, audio_formats.AudioFormat):
        with contextlib.suppress(audio_formats.NotTaggable):
            tags, link = audio_formats.read(data, fmt)
    snapshot = {name: tags.get(key, "").strip() for name, key in _SNAPSHOT_KEYS.items()}
    snapshot["catalog_id"] = link
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

#: The 403 reasons Drive gives when it wants requests slowed down: the only
#: 403s worth trying again (a 429 is always one).
RATE_LIMIT_REASONS = frozenset({"userRateLimitExceeded", "rateLimitExceeded"})

#: Backoff for the Drive calls made here: 1 s doubling to at most 32 s,
#: six attempts in all (about a minute of waiting before giving up).
DRIVE_RETRY = RetryConfig(max_retries=6, base_delay_s=1.0, max_delay_s=32.0)


def _status(error: HttpError) -> int | None:
    return getattr(getattr(error, "resp", None), "status", None)


def _reasons(error: HttpError) -> set[str]:
    """The ``reason`` of each error Drive's response body lists."""
    found: set[str] = set()
    for detail in getattr(error, "error_details", None) or []:
        if isinstance(detail, dict) and detail.get("reason"):
            found.add(str(detail["reason"]))
    try:
        body = json.loads(error.content.decode("utf-8"))
    except (AttributeError, ValueError):
        return found
    errors = (body.get("error") or {}).get("errors") if isinstance(body, dict) else []
    for item in errors or []:
        if isinstance(item, dict) and item.get("reason"):
            found.add(str(item["reason"]))
    return found


def is_rate_limited(error: BaseException) -> bool:
    """Whether ``error`` is Drive asking for requests to slow down: a 429,
    or a 403 whose reason is ``userRateLimitExceeded`` or
    ``rateLimitExceeded``. Any other 403 is a refusal, not a rate limit."""
    if not isinstance(error, HttpError):
        return False
    status = _status(error)
    if status == 429:
        return True
    return status == 403 and bool(_reasons(error) & RATE_LIMIT_REASONS)


@dataclass(frozen=True)
class _Refused:
    error: HttpError


def drive_call(request: Any, *, context: str) -> Any:
    """``request.execute()``, with the shared Google retry and backoff
    (:data:`DRIVE_RETRY`): a rate limit (:func:`is_rate_limited`), a 5xx or
    a dropped connection is tried again; any other 403 is raised at once.
    An error the retries cannot get past is raised as it came, for the
    caller to handle (:func:`is_rate_limited` tells a rate limit apart).
    The shared helper knows a rate-limit 403 by Drive's message, which
    names the rate limit."""

    def attempt() -> Any:
        try:
            return request.execute()
        except HttpError as exc:
            if _status(exc) == 403 and not is_rate_limited(exc):
                # Returned, not raised: the shared helper would try a 403
                # that mentions a quota again.
                return _Refused(exc)
            raise

    result = execute_with_retry(
        attempt, context=context, retry=DRIVE_RETRY, expected_error=lambda _e: True
    )
    if isinstance(result, _Refused):
        raise result.error
    return result


def file_meta(drive: Any, file_id: str) -> dict[str, Any]:
    """The file's parents, trashed flag, md5 and head revision."""
    result: dict[str, Any] = drive_call(
        drive.service.files().get(
            fileId=file_id,
            fields="id,name,parents,trashed,md5Checksum,headRevisionId",
            supportsAllDrives=True,
        ),
        context=f"reading file {file_id}",
    )
    return result


#: How much of a file's start a tag read downloads first. A file whose
#: ID3v2 tag is longer is downloaded whole.
TAG_HEAD_BYTES = 512 * 1024

#: How much of a file's end a tag read downloads: enough for the ID3v1 block
#: (and the look before it for an APEv2 tag) that mutagen reads there.
TAG_TAIL_BYTES = 256


@dataclass(frozen=True)
class TagBytes:
    """Bytes to read a file's tags from: the whole file (``whole``), or its
    start, holding the whole ID3v2 tag, followed by its end, where an ID3v1
    block sits. Tags read from either are the same; only whole bytes may be
    written from or checked for audio (:func:`whole_bytes`)."""

    data: bytes
    whole: bool


def id3v2_length(head: bytes) -> int | None:
    """How many bytes the ID3v2 tag at the start of ``head`` takes, footer
    included: 0 when there is none, None when its header cannot be read
    (then only the whole file says what the tags are)."""
    if head[:3] != b"ID3":
        return 0
    if len(head) < 10:
        return None
    raw = head[6:10]
    if any(b & 0x80 for b in raw):
        return None
    size = (raw[0] << 21) | (raw[1] << 14) | (raw[2] << 7) | raw[3]
    return 10 + size + (10 if head[5] & 0x10 else 0)


def _ranged(drive: Any, file_id: str, span: str) -> bytes | None:
    """The file's bytes in ``span`` (an HTTP Range), or None when Drive
    will not give them (an empty file has no range to give)."""
    request = drive.service.files().get_media(fileId=file_id, supportsAllDrives=True)
    request.headers["Range"] = f"bytes={span}"
    try:
        data = drive_call(request, context=f"reading part of file {file_id}")
    except HttpError:
        return None
    return bytes(data) if data else None


def read_tag_bytes(drive: Any, file_id: str) -> TagBytes:
    """The bytes to read ``file_id``'s tags from, downloading only its start
    and end when its ID3v2 tag fits in :data:`TAG_HEAD_BYTES`; otherwise, or
    when a range cannot be read, the whole file. A file in another format
    than MP3 (:mod:`deejay_cog.audio_formats`) is read whole: its tags are
    not all at its start and end."""
    head = _ranged(drive, file_id, f"0-{TAG_HEAD_BYTES - 1}")
    if head is not None and len(head) < TAG_HEAD_BYTES:
        return TagBytes(head, whole=True)
    if head is not None and audio_formats.other_format(head) is not None:
        return TagBytes(drive.download_file_bytes(file_id).data, whole=True)
    length = id3v2_length(head) if head is not None else None
    tail = (
        _ranged(drive, file_id, f"-{TAG_TAIL_BYTES}")
        if head is not None and length is not None and length <= len(head)
        else None
    )
    if head is None or tail is None:
        return TagBytes(drive.download_file_bytes(file_id).data, whole=True)
    return TagBytes(head + tail, whole=False)


def whole_bytes(drive: Any, file_id: str, read: TagBytes) -> bytes:
    """The whole file: ``read``'s bytes when they are whole, else the file
    downloaded."""
    if read.whole:
        return read.data
    data: bytes = drive.download_file_bytes(file_id).data
    return data


def keep_head_revision(drive: Any, file_id: str, revision_id: str | None = None) -> str:
    """Pin the file's current revision so Drive does not prune it, and
    return its id: what an in-place write is undone to. ``revision_id`` is
    the head revision when the caller already read it with the file."""
    if not revision_id:
        revision_id = str(file_meta(drive, file_id)["headRevisionId"])
    drive_call(
        drive.service.revisions().update(
            fileId=file_id, revisionId=revision_id, body={"keepForever": True}
        ),
        context=f"keeping revision {revision_id} of file {file_id}",
    )
    return revision_id


def revision_bytes(drive: Any, file_id: str, revision_id: str) -> bytes:
    """The contents of one kept revision."""
    data = drive_call(
        drive.service.revisions().get_media(fileId=file_id, revisionId=revision_id),
        context=f"reading revision {revision_id} of file {file_id}",
    )
    return bytes(data)


def revision_at(drive: Any, file_id: str, when: datetime) -> str | None:
    """The id of the file's last revision Drive saved at or before ``when``:
    what the file held then. None when Drive keeps none that old."""
    found: tuple[datetime, str] | None = None
    token: str | None = None
    while True:
        page: dict[str, Any] = drive_call(
            drive.service.revisions().list(
                fileId=file_id,
                fields="nextPageToken,revisions(id,modifiedTime)",
                pageSize=1000,
                pageToken=token,
            ),
            context=f"listing revisions of file {file_id}",
        )
        for rev in page.get("revisions") or []:
            stamp = rev.get("modifiedTime")
            if not stamp:
                continue
            at = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
            if at <= when and (found is None or at >= found[0]):
                found = (at, str(rev["id"]))
        token = page.get("nextPageToken")
        if not token:
            return found[1] if found else None


def update_contents(drive: Any, file_id: str, name: str, data: bytes) -> None:
    """Write ``data`` over the file's contents; name, folder and id stay.
    It is sent as its own format's type (an MP3 unless it is another)."""
    fmt = audio_formats.other_format(data)
    mime_type = (
        audio_formats.MIME_TYPES[fmt]
        if isinstance(fmt, audio_formats.AudioFormat)
        else MP3_MIME_TYPE
    )
    suffix = os.path.splitext(name)[1] or ".mp3"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as handle:
        handle.write(data)
        path = handle.name
    try:
        drive.update_file(file_id, path, mime_type=mime_type)
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

    Saved on every change in a writing run (a save with nothing new since
    the last is skipped) and once at the end of a dry run, under ``<flow>-<run id>.json`` (a dry
    run's name ends ``.dry-run.json``). A save that fails in a writing run
    raises, so no write happens that its record does not show.

    One run id has one record. A queued run that is delivered again (after
    a failure) runs under the same id, so its first save adopts the record
    the earlier delivery left: a writing run keeps the earlier entries ahead
    of its own, so a rollback still sees every write; a dry run writes over
    it.
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
    _saved: dict[str, Any] | None = field(default=None, repr=False)

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

    def _adopt_existing(self) -> None:
        """Take over the record an earlier delivery of this run saved, if
        any: its file, and in a writing run its entries."""
        files = self._drive.list_files(
            parent_id=self._state_folder,
            name_contains=self.file_name,
            include_folders=False,
        )
        matches = [f for f in files if str(f.name) == self.file_name]
        if not matches:
            return
        self._file_id = str(matches[0].id)
        if not self.mode.writes:
            return
        earlier: list[RecordEntry] = []
        for found in matches:
            raw = self._drive.download_file_bytes(str(found.id)).data
            try:
                data = json.loads(bytes(raw).decode("utf-8"))
            except ValueError:
                log.warning("%s: an earlier record is not JSON", self.file_name)
                continue
            for item in data.get("entries") or []:
                earlier.append(RecordEntry(**item))
            self.started_at = min(
                self.started_at, str(data.get("started_at") or self.started_at)
            )
        self.entries[:0] = earlier

    def save(self) -> None:
        """Write the record to the state folder (create, then update).

        Nothing is saved in ``off``; in a dry run only :meth:`finish`
        saves, since nothing is written that would need undoing.
        """
        if self._drive is None or self.mode is FlowMode.OFF:
            return
        if self._file_id is None and self._saved is None:
            self._adopt_existing()
        unchanged = {**self.to_json(), "updated_at": None}
        if self._file_id is not None and unchanged == self._saved:
            return  # nothing new since the last save
        self._saved = unchanged
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

    @property
    def saved(self) -> bool:
        """Whether the record has been saved to the state folder yet."""
        return self._file_id is not None

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
    head_revision: str | None = None,
) -> RecordEntry:
    """Write ``tagged`` over the same Drive file, safely: for the library,
    where a file is never renamed, moved or deleted.

    Checks the tags and the audio first; in a dry run stops there, with the
    entry ``planned``. Otherwise pins the current revision (the record
    names it, so the write can be undone), writes, downloads the file again
    and checks the audio once more. A mismatch then puts the original bytes
    back and raises :class:`AudioChanged`. ``head_revision`` is the file's
    head revision as the caller read it with ``original``; without it, it is
    read again.

    The record is saved before the run's first change, then once the
    revision is pinned (the record names it before anything is written),
    once the write is made, and once it is checked.
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
    if not record.saved:
        # The run's first change: the record must save before anything does.
        record.checkpoint()
    entry.revision_kept = keep_head_revision(drive, file_id, head_revision)
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
