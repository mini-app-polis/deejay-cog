"""Fakes and event builders for the integration suite. See conftest.py."""

from __future__ import annotations

import csv
import itertools
import json
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import httpx
import respx
from mini_app_polis.google.types import DriveFile

API_BASE = "https://api.deejay.test"
API_KEY = "deejay-cog-test-key"

SOURCE_FOLDER = "folder-csv-source"
DJ_SETS_FOLDER = "folder-dj-sets"
VDJ_HISTORY_FOLDER = "folder-vdj-history"

FOLDER_MIME = "application/vnd.google-apps.folder"
SHEET_MIME = "application/vnd.google-apps.spreadsheet"


# ── Google Drive / Sheets ────────────────────────────────────────────────


@dataclass
class _Node:
    id: str
    name: str
    parents: list[str]
    mime_type: str
    content: str = ""


@dataclass
class _Fault:
    exc: BaseException
    times: int | None  # None: every call


class FakeSheets:
    """The Sheets calls the flows make, over the sheets FakeDrive creates."""

    def __init__(self) -> None:
        self.values: dict[str, list[list[str]]] = {}
        self.formatted: list[str] = []
        self.reads: list[str] = []
        self.formatter = SimpleNamespace(apply_formatting_to_sheet=self._format)

    def _format(self, spreadsheet_id: str) -> None:
        self.formatted.append(spreadsheet_id)

    def get_metadata(self, spreadsheet_id: str, fields: str | None = None) -> dict:  # noqa: ARG002
        if spreadsheet_id not in self.values:
            raise RuntimeError(f"404: spreadsheet {spreadsheet_id} not found")
        return {"sheets": [{"properties": {"title": "Sheet1"}}]}

    def read_values(self, spreadsheet_id: str, a1_range: str) -> list[list[str]]:  # noqa: ARG002
        self.reads.append(spreadsheet_id)
        return [list(row) for row in self.values.get(spreadsheet_id, [])]


class FakeDrive:
    """An in-memory Drive holding the folders the flows read and write.

    Implements the ``DriveFacade`` methods deejay-cog calls, with the
    facade's semantics where they matter: a CSV upload becomes a Google
    Sheet named after the uploaded file, ``get_all_m3u_files`` is
    newest-first by name, and listings exclude trashed and (by default)
    folder entries.

    ``fail(method, exc, times=n)`` makes the next ``n`` calls to a method
    raise, for the failure paths.
    """

    def __init__(self, sheets: FakeSheets) -> None:
        self._sheets = sheets
        self._ids = itertools.count(1)
        self.nodes: dict[str, _Node] = {}
        self._faults: dict[str, _Fault] = {}
        self._delays: dict[str, float] = {}
        self.calls: list[str] = []
        for folder_id in (SOURCE_FOLDER, DJ_SETS_FOLDER, VDJ_HISTORY_FOLDER):
            self.nodes[folder_id] = _Node(folder_id, folder_id, [], FOLDER_MIME)
        # The facade's own ``service`` escape hatch, used for a parents read.
        self.service = SimpleNamespace(files=lambda: _FilesResource(self))

    # ── test controls ────────────────────────────────────────────────────

    def add_file(
        self, folder_id: str, name: str, content: str = "", mime_type: str = "text/csv"
    ) -> str:
        file_id = f"file-{next(self._ids)}"
        self.nodes[file_id] = _Node(file_id, name, [folder_id], mime_type, content)
        return file_id

    def fail(self, method: str, exc: BaseException, *, times: int | None = 1) -> None:
        self._faults[method] = _Fault(exc, times)

    def slow(self, method: str, seconds: float) -> None:
        self._delays[method] = seconds

    def names_in(self, folder_id: str) -> list[str]:
        return sorted(n.name for n in self._children(folder_id, include_folders=False))

    def folder(self, parent_id: str, name: str) -> str | None:
        for n in self._children(parent_id, include_folders=True):
            if n.mime_type == FOLDER_MIME and n.name == name:
                return n.id
        return None

    def path(self, *names: str) -> str | None:
        """The id of a folder by its names under DJ_SETS, or None."""
        current: str | None = DJ_SETS_FOLDER
        for name in names:
            if current is None:
                return None
            current = self.folder(current, name)
        return current

    def sheets_in(self, folder_id: str) -> list[_Node]:
        return [
            n
            for n in self._children(folder_id, include_folders=False)
            if n.mime_type == SHEET_MIME
        ]

    # ── internals ────────────────────────────────────────────────────────

    def _enter(self, method: str) -> None:
        self.calls.append(method)
        delay = self._delays.get(method)
        if delay:
            time.sleep(delay)
        fault = self._faults.get(method)
        if fault is None:
            return
        if fault.times is not None:
            fault.times -= 1
            if fault.times <= 0:
                del self._faults[method]
        raise fault.exc

    def _children(self, folder_id: str, *, include_folders: bool) -> list[_Node]:
        return [
            n
            for n in self.nodes.values()
            if folder_id in n.parents
            and (include_folders or n.mime_type != FOLDER_MIME)
        ]

    def _get(self, file_id: str) -> _Node:
        try:
            return self.nodes[file_id]
        except KeyError:
            raise RuntimeError(f"404: file {file_id} not found") from None

    # ── DriveFacade surface ─────────────────────────────────────────────

    def list_files(
        self,
        folder_id: str,
        *,
        include_folders: bool = True,
        trashed: bool = False,  # noqa: ARG002 — nothing here is ever trashed
        name_contains: str | None = None,
        **_: Any,
    ) -> list[DriveFile]:
        self._enter("list_files")
        return [
            DriveFile(id=n.id, name=n.name, mime_type=n.mime_type)
            for n in self._children(folder_id, include_folders=include_folders)
            if name_contains is None or name_contains in n.name
        ]

    def ensure_folder(self, parent_id: str, name: str) -> str:
        self._enter("ensure_folder")
        existing = self.folder(parent_id, name)
        if existing:
            return existing
        folder_id = f"folder-{next(self._ids)}"
        self.nodes[folder_id] = _Node(folder_id, name, [parent_id], FOLDER_MIME)
        return folder_id

    def rename_file(self, file_id: str, new_name: str) -> None:
        self._enter("rename_file")
        self._get(file_id).name = new_name

    def move_file(
        self, file_id: str, *, new_parent_id: str, remove_from_parents: bool = True
    ) -> None:
        self._enter("move_file")
        node = self._get(file_id)
        node.parents = (
            [new_parent_id] if remove_from_parents else [*node.parents, new_parent_id]
        )

    def delete_file(self, file_id: str) -> None:
        self._enter("delete_file")
        self.nodes.pop(file_id, None)

    def download_file(self, file_id: str, destination: str) -> None:
        self._enter("download_file")
        with open(destination, "w", encoding="utf-8") as f:
            f.write(self._get(file_id).content)

    def upload_csv_as_google_sheet(
        self, filepath: str, *, parent_id: str, dest_name: str | None = None
    ) -> str:
        self._enter("upload_csv_as_google_sheet")
        with open(filepath, encoding="utf-8", newline="") as f:
            rows = [list(r) for r in csv.reader(f)]
        name = dest_name or os.path.basename(filepath)
        sheet_id = f"sheet-{next(self._ids)}"
        self.nodes[sheet_id] = _Node(sheet_id, name, [parent_id], SHEET_MIME)
        self._sheets.values[sheet_id] = rows
        return sheet_id

    def get_all_m3u_files(self) -> list[dict]:
        # The facade swallows a listing failure and answers []. Mirrored,
        # so the flow is tested against what it will actually be given.
        try:
            files = self.list_files(
                VDJ_HISTORY_FOLDER, include_folders=False, name_contains=".m3u"
            )
        except Exception:
            return []
        files.sort(key=lambda f: f.name or "", reverse=True)
        return [{"id": f.id, "name": f.name} for f in files]

    def download_m3u_file_data(
        self,
        file_id: str,
        *,
        encoding: str = "utf-8",  # noqa: ARG002
    ) -> list[str]:
        self._enter("download_m3u_file_data")
        return self._get(file_id).content.splitlines()


class _FilesResource:
    def __init__(self, drive: FakeDrive) -> None:
        self._drive = drive

    def get(self, fileId: str, fields: str = "") -> Any:  # noqa: N803, ARG002 — Drive's own spelling
        node = self._drive._get(fileId)
        return SimpleNamespace(execute=lambda: {"parents": list(node.parents)})


@dataclass
class FakeGoogle:
    drive: FakeDrive
    sheets: FakeSheets
    gspread: Any = None


# ── api-kaianolevine-com ─────────────────────────────────────────────────


@dataclass
class FakeApi:
    """The API routes this cog calls, recorded, over respx.

    ``calls[path]`` holds each request body that reached a route, in order,
    whether it was answered with success or with an injected failure.
    """

    router: respx.MockRouter
    calls: dict[str, list[dict]] = field(default_factory=dict)
    auth: list[str] = field(default_factory=list)
    hosts: list[str] = field(default_factory=list)
    _faults: dict[str, list[int]] = field(default_factory=dict)
    _delays: dict[str, float] = field(default_factory=dict)

    def fail(self, path: str, status: int, *, times: int = 1) -> None:
        self._faults.setdefault(path, []).extend([status] * times)

    def slow(self, path: str, seconds: float) -> None:
        self._delays[path] = seconds

    def bodies(self, path: str) -> list[dict]:
        return self.calls.get(path, [])

    def reports(self) -> list[dict]:
        """Each run report delivered, as its one Discord embed."""
        return [body["embeds"][0] for body in self.bodies("/v1/notify")]

    def severities(self) -> list[str]:
        return [e["footer"]["text"].split(" · ", 1)[0] for e in self.reports()]

    def _handler(self, path: str, data: Callable[[dict], Any]):
        def handle(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content or b"{}")
            self.calls.setdefault(path, []).append(body)
            self.auth.append(request.headers.get("Authorization", ""))
            self.hosts.append(request.url.host)
            delay = self._delays.get(path)
            if delay:
                time.sleep(delay)
            pending = self._faults.get(path)
            if pending:
                status = pending.pop(0)
                return httpx.Response(status, json={"detail": "injected failure"})
            return httpx.Response(
                200,
                json={
                    "data": data(body),
                    "meta": {"count": 1, "total": 1, "version": "test"},
                },
            )

        return handle


def _ingest_answer(body: dict) -> dict:
    return {
        "set_id": "00000000-0000-4000-8000-000000000001",
        "tracks_created": len(body.get("tracks", [])),
        "catalog_new": len(body.get("tracks", [])),
        "catalog_updated": 0,
        "catalog_unchanged": 0,
    }


def install_api(router: respx.MockRouter, api: FakeApi, base: str) -> None:
    routes: dict[str, Callable[[dict], Any]] = {
        "/v1/ingest": _ingest_answer,
        "/v1/live-plays": lambda b: {"inserted": len(b.get("plays", [])), "skipped": 0},
        "/v1/spotify/playlists": lambda b: {
            "upserted": len(b.get("playlists", [])),
            "unchanged": 0,
        },
        "/v1/notify": lambda _b: {
            "forwarded": True,
            "event": "notify",
            "outcome": "sent",
            "reason": "test",
        },
    }
    for path, answer in routes.items():
        router.post(f"{base}{path}").mock(side_effect=api._handler(path, answer))


# ── Spotify ──────────────────────────────────────────────────────────────


class FakeSpotify:
    """``SpotifyAPI`` as the flow uses it: search, playlists, and the raw client."""

    def __init__(self) -> None:
        self.catalog: dict[tuple[str, str], str] = {}
        self.playlists: dict[str, dict] = {}
        self.added: list[tuple[str, list[str]]] = []
        self.trimmed: list[str] = []
        self.client = SimpleNamespace(current_user_playlists=self._page)

    def search_track(self, artist: str, title: str) -> str | None:
        return self.catalog.get((artist, title))

    def find_playlist_by_name(self, name: str) -> dict | None:
        for p in self.playlists.values():
            if p["name"] == name:
                return p
        return None

    def create_playlist(self, name: str, description: str) -> str:  # noqa: ARG002
        pid = f"pl-{len(self.playlists) + 1}"
        self.playlists[pid] = {"id": pid, "name": name, "uris": []}
        return pid

    def clear_playlist(self, playlist_id: str) -> None:
        self.playlists[playlist_id]["uris"] = []

    def add_tracks_to_specific_playlist(
        self, playlist_id: str, uris: list[str]
    ) -> None:
        self.added.append((playlist_id, list(uris)))
        self.playlists.setdefault(
            playlist_id, {"id": playlist_id, "name": playlist_id, "uris": []}
        )
        self.playlists[playlist_id]["uris"].extend(uris)

    def trim_playlist_to_limit(self, *, playlist_id: str) -> None:
        self.trimmed.append(playlist_id)

    def _page(self, *, limit: int, offset: int) -> dict:
        items = [
            {
                "id": p["id"],
                "name": p["name"],
                "uri": f"spotify:playlist:{p['id']}",
                "external_urls": {
                    "spotify": f"https://open.spotify.com/playlist/{p['id']}"
                },
                "public": True,
                "collaborative": False,
                "snapshot_id": "snap",
                "tracks": {"total": len(p["uris"])},
                "owner": {"id": "kaiano", "display_name": "Kaiano"},
                "type": "playlist",
            }
            for p in self.playlists.values()
        ]
        page = items[offset : offset + limit]
        return {"items": page, "next": "more" if offset + limit < len(items) else None}


# ── SQS ──────────────────────────────────────────────────────────────────


def run_body(mode: str) -> str:
    """The message api-kaianolevine-com enqueues for ``POST /v1/deejay/runs``."""
    return json.dumps({"type": "deejay.run", "version": 1, "payload": {"mode": mode}})


def sqs_record(body: str, message_id: str, *, receive_count: int = 1) -> dict:
    """One record as the SQS event source mapping delivers it to Lambda."""
    return {
        "messageId": message_id,
        "receiptHandle": f"rh-{message_id}",
        "body": body,
        "attributes": {
            "ApproximateReceiveCount": str(receive_count),
            "SentTimestamp": "1759680000000",
            "SenderId": "AROAEXAMPLE:api-kaianolevine-com",
            "ApproximateFirstReceiveTimestamp": "1759680000001",
        },
        "messageAttributes": {},
        "md5OfBody": "ignored",
        "eventSource": "aws:sqs",
        "eventSourceARN": "arn:aws:sqs:us-east-1:000000000000:deejay-jobs",
        "awsRegion": "us-east-1",
    }


def sqs_event(*records: dict) -> dict:
    return {"Records": list(records)}


@dataclass
class LambdaContext:
    """Enough of the Lambda context for the deadline to arm."""

    remaining_ms: float = 900_000
    aws_request_id: str = "req-1"
    function_name: str = "deejay-cog"
    _started: float = field(default_factory=time.monotonic)

    def get_remaining_time_in_millis(self) -> int:
        elapsed = (time.monotonic() - self._started) * 1000
        return int(self.remaining_ms - elapsed)
