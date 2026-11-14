"""The members a server calls: an agent's conversations listed, one in
particular, its history, searched, an attachment's bytes and the reminders
still to fire, read from the node's database whether or not the agent runs."""

from __future__ import annotations

lazy import asyncio
lazy import base64
lazy import os
lazy from pathlib import Path
lazy from typing import Literal

lazy from bazaar_contrib_storage import Database

lazy from .models import Detail, Found, History, Page, Reminder, Segment
lazy from .store import Store

# a segment encoded as base64 stays within the 1 MiB a call carries back
SEGMENT = 512 * 1024


def _segment(agent_id: str, relative_path: str, offset: int) -> tuple[bytes, int]:
    """A segment of a file in the agent's workspace, and the file's size."""
    workspace = (
        Path(os.environ.get("BCN_HOME") or Path.home())
        / ".bcn"
        / "workspaces"
        / agent_id
    )
    with (workspace / relative_path).open("rb") as file:
        file.seek(offset)
        return file.read(SEGMENT), os.fstat(file.fileno()).st_size


class Conversations:
    def __init__(self, database: Database) -> None:
        self._database = database

    async def conversations(
        self,
        agent_id: str,
        review: Literal["approved", "pending", "denied"],
        offset: int = 0,
        limit: int = 50,
    ) -> Page:
        return await Store(self._database, agent_id).page(review, offset, limit)

    async def conversation(self, agent_id: str, thread_id: str) -> Detail:
        return await Store(self._database, agent_id).detail(thread_id)

    async def history(
        self,
        agent_id: str,
        thread_id: str,
        around: str | None = None,
        limit: int = 50,
    ) -> History:
        return await Store(self._database, agent_id).history(thread_id, around, limit)

    async def timeline(
        self,
        agent_id: str,
        thread_id: str,
        position: tuple[int, str, str] | None = None,
        side: Literal["before", "after", "around"] = "around",
        limit: int = 50,
        around: str | None = None,
    ) -> History:
        return await Store(self._database, agent_id).history(
            thread_id, around, limit, chronological=True, position=position, side=side
        )

    async def search(
        self,
        agent_id: str,
        query: str,
        sort: Literal["relevance", "time"],
        thread_id: str | None = None,
        sender: str | None = None,
        after_ms: int | None = None,
        before_ms: int | None = None,
        offset: int = 0,
        limit: int = 20,
    ) -> Found:
        return await Store(self._database, agent_id).search(
            query, sort, thread_id, sender, after_ms, before_ms, offset, limit
        )

    async def attachment(
        self, agent_id: str, attachment_id: str, offset: int = 0
    ) -> Segment:
        stored = await Store(self._database, agent_id).attachment(attachment_id)
        if stored is None:
            raise ValueError(f"attachment is not ready: {attachment_id}")
        relative_path, media_type = stored
        data, size = await asyncio.to_thread(_segment, agent_id, relative_path, offset)
        return Segment(
            media_type=media_type,
            size_bytes=size,
            data=base64.b64encode(data).decode(),
        )

    async def reminders(
        self, agent_id: str, thread_id: str | None = None
    ) -> tuple[Reminder, ...]:
        return await Store(self._database, agent_id).reminders(thread_id)
