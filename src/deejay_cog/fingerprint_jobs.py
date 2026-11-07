"""The fingerprint queue's message, and the sender retag-music uses.

retag-music hands each file with no usable title or artist to the
fingerprint function, one message per file, on deejay-cog's own
``deejay-fingerprint-jobs.fifo`` queue (ADR-0007, decision 3). Both ends
are this package, so the message is defined here rather than in the shared
contract:

    {"type": "deejay.fingerprint", "version": 1,
     "payload": {"drive_file_id": "<id>", "name": "<file name>",
                 "flow": "retag-music", "mode": null}}

``flow`` says which flow sent the file and so what the function does with
it: ``retag-music`` (the default, so a message without it is read as
before) tags the file in place; ``new-music`` takes it in as the new-music
mode would (:mod:`deejay_cog.new_music`); ``library-identify`` identifies a
library file and links it to the catalog
(:mod:`deejay_cog.library_identify`), and also carries the file's
``library_root_id`` and ``path``. For ``new-music`` and ``library-identify``,
``mode`` is the sender's mode; the function uses the stricter of it and its
own, and reads a missing one as ``off``.

The deejay worker gets the queue's URL as ``DEEJAY_FINGERPRINT_QUEUE_URL``.
Unset (a local run), nothing is sent and retag-music names the files in its
report instead.
"""

from __future__ import annotations

import importlib
import os
from typing import Any, Literal

from mini_app_polis.api.contract.queue import QueueMessage
from pydantic import BaseModel, Field

#: Where the deejay worker finds the fingerprint queue.
QUEUE_URL_VAR = "DEEJAY_FINGERPRINT_QUEUE_URL"

#: Every message goes in one group, so the function takes files one at a
#: time, in the order they were sent.
MESSAGE_GROUP_ID = "deejay-fingerprint"


class FingerprintJobPayload(BaseModel):
    """One Drive file to identify by its fingerprint."""

    drive_file_id: str = Field(..., min_length=1, description="The file.")
    name: str = Field(default="", description="Its name, for the run report.")
    flow: Literal["retag-music", "new-music", "library-identify"] = Field(
        default="retag-music", description="The flow that sent the file."
    )
    mode: Literal["off", "dry-run", "write"] | None = Field(
        default=None, description="The new-music or library-identify sender's mode."
    )
    library_root_id: str | None = Field(
        default=None, description="A library file's library root (library-identify)."
    )
    path: str | None = Field(
        default=None,
        description="A library file's path under its root (library-identify).",
    )


class FingerprintJobMessage(QueueMessage[FingerprintJobPayload]):
    """``deejay.fingerprint`` on the fingerprint function's queue."""

    type: Literal["deejay.fingerprint"] = "deejay.fingerprint"


class FingerprintJobs:
    """Sends files to the fingerprint queue."""

    def __init__(self, queue_url: str, client: Any = None) -> None:
        self.queue_url = queue_url
        # boto3 ships in the Lambda runtime and is not a dependency of this
        # package, so it is imported only when a sender is built without a
        # client.
        self._client = client or importlib.import_module("boto3").client("sqs")

    def send(
        self,
        file_id: str,
        name: str,
        *,
        flow: Literal["retag-music", "new-music", "library-identify"] = "retag-music",
        mode: Literal["off", "dry-run", "write"] | None = None,
        library_root_id: str | None = None,
        path: str | None = None,
    ) -> None:
        """Queue one file. The file id (prefixed with the flow for
        new-music) is the deduplication id, so a run that is redelivered
        does not queue the same file twice within SQS's five-minute window."""
        message = FingerprintJobMessage(
            payload=FingerprintJobPayload(
                drive_file_id=file_id,
                name=name,
                flow=flow,
                mode=mode,
                library_root_id=library_root_id,
                path=path,
            )
        )
        self._client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=message.model_dump_json(),
            MessageGroupId=MESSAGE_GROUP_ID,
            MessageDeduplicationId=(
                file_id if flow == "retag-music" else f"{flow}:{file_id}"
            ),
        )


def fingerprint_jobs_from_env() -> FingerprintJobs | None:
    """A sender for the configured queue, or None when none is configured."""
    queue_url = os.getenv(QUEUE_URL_VAR, "").strip()
    return FingerprintJobs(queue_url) if queue_url else None
