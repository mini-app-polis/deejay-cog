"""A Drive that keeps its files in memory, for the music-tagging tests.

It records every call that would change Drive, so a test can assert that a
dry run, a mode that is off or a refused file wrote nothing. A call it has
no fake for fails the test, rather than passing silently.
"""

from __future__ import annotations

import io
import itertools
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from mutagen.id3 import ID3

#: One MPEG-1 Layer III frame, repeated: enough audio for mutagen.
AUDIO = (b"\xff\xfb\x90\x64" + b"\x00" * 413) * 8

#: Every DriveFacade method that changes Drive.
WRITE_METHODS = frozenset(
    {
        "upload_bytes",
        "upload_file",
        "update_file",
        "copy_file",
        "move_file",
        "rename_file",
        "trash_file",
        "delete_file",
        "delete_file_with_fallback",
        "keep_revision",
    }
)


def mp3(*frames: Any, audio: bytes = AUDIO) -> bytes:
    """An MP3 whose ID3 tag holds ``frames`` (no tag at all when empty)."""
    if not frames:
        return audio
    tags = ID3()
    for frame in frames:
        tags.add(frame)
    buffer = io.BytesIO(audio)
    tags.save(buffer, v2_version=4)
    return buffer.getvalue()


@dataclass
class DriveItem:
    id: str
    name: str
    parents: list[str]
    data: bytes = b""
    mime_type: str = "audio/mpeg"
    trashed: bool = False
    app_properties: dict[str, str] = field(default_factory=dict)
    revisions: list[bytes] = field(default_factory=list)
    kept: set[str] = field(default_factory=set)


class _Request:
    def __init__(self, fn: Any) -> None:
        self._fn = fn

    def execute(self) -> Any:
        return self._fn()


class FakeDrive:
    """Files by id, each in its folders, with the facade's methods."""

    def __init__(self) -> None:
        self.items: dict[str, DriveItem] = {}
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self._ids = itertools.count(1)
        #: Set to alter what an upload stores, to fake a corrupted write.
        self.corrupt_uploads = False
        self.service = SimpleNamespace(
            files=lambda: SimpleNamespace(get=self._svc_get),
            revisions=lambda: SimpleNamespace(
                update=self._svc_rev_update, get_media=self._svc_rev_get
            ),
        )

    # -- set-up and inspection --------------------------------------------

    def add(self, name: str, folder: str, data: bytes = b"", **kw: Any) -> str:
        file_id = f"f{next(self._ids)}"
        self.items[file_id] = DriveItem(file_id, name, [folder], data, **kw)
        self.items[file_id].revisions.append(data)
        return file_id

    def in_folder(self, folder: str) -> list[DriveItem]:
        return [i for i in self.items.values() if folder in i.parents and not i.trashed]

    def writes(self, *, except_folder: str | None = None) -> list[tuple[str, Any]]:
        """The calls that changed Drive, leaving out those on files in
        ``except_folder`` (the state folder's run records)."""
        out = []
        for method, args in self.calls:
            if method not in WRITE_METHODS:
                continue
            if except_folder is not None and self._in(args, except_folder):
                continue
            out.append((method, args))
        return out

    def _in(self, args: tuple[Any, ...], folder: str) -> bool:
        for arg in args:
            if arg == folder:
                return True
            item = self.items.get(arg) if isinstance(arg, str) else None
            if item is not None and folder in item.parents:
                return True
        return False

    def _record(self, method: str, *args: Any) -> None:
        self.calls.append((method, args))

    # -- reads -------------------------------------------------------------

    def list_files(
        self,
        parent_id: str,
        *,
        mime_type: str | None = None,
        name_contains: str | None = None,
        trashed: bool = False,
        include_folders: bool = True,
    ) -> list[SimpleNamespace]:
        del include_folders
        out = []
        for i in self.items.values():
            if parent_id not in i.parents or i.trashed != trashed:
                continue
            if mime_type and i.mime_type != mime_type:
                continue
            if name_contains and name_contains not in i.name:
                continue
            out.append(SimpleNamespace(id=i.id, name=i.name, mime_type=i.mime_type))
        return out

    def download_file_bytes(self, file_id: str) -> SimpleNamespace:
        i = self.items[file_id]
        return SimpleNamespace(
            file_id=file_id, name=i.name, mime_type=i.mime_type, data=i.data
        )

    def find_file_in_folder(
        self, parent_folder_id: str, *, name: str, mime_type: str | None = None
    ) -> str | None:
        del mime_type
        for i in self.in_folder(parent_folder_id):
            if i.name == name:
                return i.id
        return None

    def find_files_by_app_property(
        self, parent_folder_id: str, *, key: str, value: str
    ) -> list[str]:
        return [
            i.id
            for i in self.in_folder(parent_folder_id)
            if i.app_properties.get(key) == value
        ]

    def _svc_get(self, fileId: str, fields: str, supportsAllDrives: bool) -> _Request:  # noqa: N803
        del fields, supportsAllDrives
        from deejay_cog.music_safety import md5

        def meta() -> dict[str, Any]:
            i = self.items[fileId]
            return {
                "id": i.id,
                "name": i.name,
                "parents": list(i.parents),
                "trashed": i.trashed,
                "md5Checksum": md5(i.data),
                "headRevisionId": f"r{len(i.revisions)}",
            }

        return _Request(meta)

    def _svc_rev_update(self, fileId: str, revisionId: str, body: dict) -> _Request:  # noqa: N803
        def run() -> dict:
            self._record("keep_revision", fileId, revisionId, body)
            self.items[fileId].kept.add(revisionId)
            return {}

        return _Request(run)

    def _svc_rev_get(self, fileId: str, revisionId: str) -> _Request:  # noqa: N803
        return _Request(
            lambda: self.items[fileId].revisions[int(revisionId.removeprefix("r")) - 1]
        )

    # -- writes ------------------------------------------------------------

    def upload_bytes(
        self,
        *,
        parent_id: str,
        filename: str,
        content: bytes,
        mime_type: str,
        app_properties: dict[str, str] | None = None,
        resumable: bool = False,
    ) -> str:
        del resumable
        self._record("upload_bytes", parent_id, filename)
        stored = content
        if self.corrupt_uploads and mime_type == "audio/mpeg":
            stored = content[:-1] + bytes([content[-1] ^ 0xFF])
        return self.add(
            filename,
            parent_id,
            stored,
            mime_type=mime_type,
            app_properties=dict(app_properties or {}),
        )

    def update_file(
        self, file_id: str, filepath: str, *, mime_type: str | None = None
    ) -> None:
        del mime_type
        self._record("update_file", file_id)
        data = Path(filepath).read_bytes()
        self.items[file_id].data = data
        self.items[file_id].revisions.append(data)

    def move_file(
        self, file_id: str, *, new_parent_id: str, remove_from_parents: bool = True
    ) -> None:
        del remove_from_parents
        self._record("move_file", file_id, new_parent_id)
        self.items[file_id].parents = [new_parent_id]

    def trash_file(self, file_id: str) -> None:
        self._record("trash_file", file_id)
        self.items[file_id].trashed = True

    def __getattr__(self, name: str) -> Any:
        # rename_file, copy_file, delete_file and anything else unfaked.
        raise AssertionError(f"unexpected Drive call: {name}")
