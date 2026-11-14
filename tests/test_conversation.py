"""An agent's conversations read through the node's sidecar as the server
reads them through starlink, from a real node whose agent talks on the
switchboard."""

from __future__ import annotations

lazy import asyncio
lazy import base64
lazy import os
lazy from collections.abc import Awaitable, Callable
lazy from pathlib import Path
lazy from time import time_ns
lazy from typing import Any

from bazaar_contrib_sidecar import Client
from node import AGENT, act, configure, node, say, sidecar, stop_agents
from pydantic import TypeAdapter

from bazaar_contrib_starlink_conversation.models import (
    Detail,
    Found,
    History,
    Page,
    Reminder,
    Segment,
)

HANCHIN = {"id": "u-hanchin", "name": "hanchin", "display_name": "Hanchin"}
AERITH = {"id": "u-aerith", "name": "aerith", "display_name": "Aerith"}
TIFA = {"id": "u-tifa", "name": "tifa", "display_name": "Tifa"}
LILEI = {"id": "u-lilei", "name": "lilei", "display_name": "李雷"}
GUEST = {"id": "u-guest", "name": "guest"}


async def settled[T](read: Callable[[], Awaitable[T]], done: Callable[[T], bool]) -> T:
    """What a read gives once the node has caught up with what was said."""
    async with asyncio.timeout(30):
        while True:
            value = await read()
            if done(value):
                return value
            await asyncio.sleep(0.1)


async def call[T](
    client: Client, kind: type[T], method: str, *args: Any, **values: Any
) -> T:
    return TypeAdapter(kind).validate_python(
        await client.call("conversation", method, AGENT["id"], *args, **values)
    )


async def picture(home: Path) -> Path:
    """An image larger than two segments."""
    path = home / "checks.png"
    content = b"\x89PNG\r\n\x1a\n" + await asyncio.to_thread(os.urandom, 1_300_000)
    await asyncio.to_thread(path.write_bytes, content)
    return path


async def attachment(client: Client, attachment_id: str) -> tuple[Segment, bytes]:
    """An attachment read a segment at a time, as the server passes it on."""
    data = b""
    while True:
        segment = await call(client, Segment, "attachment", attachment_id, len(data))
        data += base64.b64decode(segment.data)
        if len(data) >= segment.size_bytes:
            return segment, data


async def session(home: Path) -> None:
    """Wait for the agent's runtime session, which the test acts in."""
    await settled(
        lambda: asyncio.to_thread(
            (home / "switchboard" / AGENT["id"] / "session.json").exists
        ),
        bool,
    )


async def test_external_ids_and_chronological_windows(tmp_path: Path):
    config = await configure(tmp_path)
    async with node(config), sidecar() as client:
        await say(tmp_path, "hanchin", HANCHIN, "早上好")
        await session(tmp_path)
        ids = [
            await say(
                tmp_path,
                "shared-room",
                HANCHIN,
                text,
                kind="group",
                title="Shared room",
            )
            for text in ("先核对资料", "再整理安排", "最后确认结果")
        ]
        listed = await settled(
            lambda: call(client, Page, "conversations", "approved"),
            lambda page: (
                bool(page.conversations)
                and page.conversations[0].latest is not None
                and page.conversations[0].latest.text == "最后确认结果"
            ),
        )
        conversation = listed.conversations[0]
        assert conversation.provider_thread_id == "shared-room"
        assert conversation.channel_identity == "cloud-cloud-bot"
        latest = await call(
            client, History, "timeline", conversation.thread_id, limit=2
        )
        assert [message.provider_message_id for message in latest.messages] == ids[1:]
        assert latest.has_before and not latest.has_after
        anchor = latest.messages[0]
        position = (anchor.at_ms, anchor.provider_message_id, anchor.edit_version or "")
        before = await call(
            client,
            History,
            "timeline",
            conversation.thread_id,
            position=position,
            side="before",
        )
        after = await call(
            client,
            History,
            "timeline",
            conversation.thread_id,
            position=position,
            side="after",
        )
        assert [message.body for message in before.messages] == ["先核对资料"]
        assert [message.body for message in after.messages] == ["最后确认结果"]


async def test_the_server_reads_an_agents_conversations(tmp_path: Path):
    config = await configure(tmp_path)
    async with node(config), sidecar() as client:

        async def page(review: str, offset: int = 0, limit: int = 50) -> Page:
            return await call(client, Page, "conversations", review, offset, limit)

        async def history(thread_id: str, **values: Any) -> History:
            return await call(client, History, "history", thread_id, **values)

        review = await say(
            tmp_path, "hanchin", HANCHIN, "周五下午三点开评审会，记得带上排期表。"
        )
        await say(tmp_path, "hanchin", HANCHIN, "评审会就在周五下午，地点不变。")
        await say(
            tmp_path,
            "hanchin",
            HANCHIN,
            "周五下午的会开完，再对一遍周五下午的发布清单。",
        )
        design = await say(
            tmp_path,
            "design-room",
            AERITH,
            "设计稿放在共享盘里了。",
            kind="group",
            title="Design room",
        )
        await say(
            tmp_path,
            "design-room",
            TIFA,
            "周五下午之前我把反馈整理好。",
            kind="group",
            title="Design room",
        )
        # a newcomer waits to be let in; a guest group is turned away
        for text in (
            "你好，我是李雷，想请你帮忙看看下周的排期。",
            "排期表在共享盘里，标题是下周安排。",
            "周三前回复我就好，谢谢。",
        ):
            await say(tmp_path, "lilei", LILEI, text, review="pending")
        for text in (
            "大家好，这个群想请你来答疑。",
            "问题我们先整理成文档。",
            "整理好了会再通知你。",
        ):
            await say(
                tmp_path,
                "guest-room",
                GUEST,
                text,
                kind="group",
                title="Guest room",
                review="pending",
            )
        approved = await settled(
            lambda: page("approved"), lambda found: found.total == 2
        )
        pending = await settled(lambda: page("pending"), lambda found: found.total == 2)
        threads = {
            item.name: item.thread_id
            for item in (*approved.conversations, *pending.conversations)
        }
        told = {"Hanchin": 3, "Design room": 2, "李雷": 3, "Guest room": 3}

        async def heard() -> dict[str, int]:
            return {name: len((await history(threads[name])).messages) for name in told}

        await settled(heard, lambda counts: counts == told)
        await session(tmp_path)
        await act(client, tmp_path, "messages", "message", "check")
        sent = await act(
            client,
            tmp_path,
            "messages",
            "message",
            "send",
            target="dm:@hanchin",
            body="收到，周五下午见。",
        )
        assert sent["ok"], sent
        await client.call(
            "collaboration", "review_contact", threads["Guest room"], "denied"
        )
        shown = await picture(tmp_path)
        checks = await say(
            tmp_path,
            "hanchin",
            HANCHIN,
            "检查结果截图在这里，麻烦看一下。",
            attachments=[{"path": str(shown), "media_type": "image/png"}],
        )
        await settled(
            lambda: history(threads["Hanchin"]),
            lambda read: any(
                item.state == "ready"
                for message in read.messages
                if message.message_id == checks
                for item in message.attachments
            ),
        )
        later = await act(
            client,
            tmp_path,
            "reminder",
            "reminder",
            "schedule",
            title="跟进评审会的结论",
            message_id=review,
            delay_seconds=2 * 86400,
        )
        daily = await act(
            client,
            tmp_path,
            "reminder",
            "reminder",
            "schedule",
            title="每天看一眼设计稿",
            message_id=design,
            repeat_rule="daily@09:00",
            timezone="Asia/Shanghai",
        )
        assert later["ok"] and daily["ok"], (later, daily)

        approved = await page("approved")
        pending = await page("pending")
        for found, state in ((approved, "approved"), (pending, "pending")):
            assert found.total == len(found.conversations) and not found.has_more
            assert found.pending_review == pending.total == 1
            assert all(item.review == state for item in found.conversations)
            times = [item.last_activity_at_ms for item in found.conversations]
            assert times == sorted(times, reverse=True)
        assert threads["Guest room"] not in {
            item.thread_id for item in (*approved.conversations, *pending.conversations)
        }
        direct, group = sorted(approved.conversations, key=lambda item: item.kind)
        assert direct.name == "Hanchin" and direct.handle == "hanchin"
        assert direct.member_count == 2
        assert group.kind == "group" and group.name == "Design room"
        assert group.member_count == 2
        (newcomer,) = pending.conversations
        assert newcomer.name == "李雷" and newcomer.first is not None
        first = await page("approved", 0, 1)
        rest = await page("approved", 1, 1)
        assert first.has_more and not rest.has_more
        assert (*first.conversations, *rest.conversations) == approved.conversations

        detail = await call(client, Detail, "conversation", direct.thread_id)
        assert detail.conversation == direct
        whole = await history(direct.thread_id, limit=detail.message_count + 1)
        assert len(whole.messages) == detail.message_count
        assert not whole.has_before and not whole.has_after
        assert direct.first is not None and direct.latest is not None
        assert direct.first.message_id == whole.messages[0].message_id
        assert direct.latest.message_id == whole.messages[-1].message_id
        assert direct.latest.text == whole.messages[-1].body[:240]
        assert whole.messages[0].at_ms <= detail.created_at_ms <= time_ns() // 1_000_000
        assert detail.members[0].sender == whole.messages[-1].sender
        assert [member.is_self for member in detail.members].count(True) == 1
        assert detail.conversation.member_count == len(detail.members)

        for thread_id in (newcomer.thread_id, threads["Guest room"]):
            whole = await history(thread_id)
            assert len(whole.messages) == 3
            assert not whole.has_before and not whole.has_after
            latest = await history(
                thread_id, around=whole.messages[-1].message_id, limit=2
            )
            assert latest.messages == whole.messages[1:]
            assert latest.has_before and not latest.has_after
            earliest = await history(thread_id, limit=2)
            assert earliest.messages == whole.messages[:2]
            assert not earliest.has_before and earliest.has_after

        found = await call(client, Found, "search", "周五下午", "relevance", limit=2)
        assert found.has_more and found.total > len(found.messages)
        found = await call(
            client, Found, "search", "周五下午", "relevance", limit=found.total
        )
        assert found.total == len(found.messages) and not found.has_more
        scores = [hit.score for hit in found.messages]
        assert scores == sorted(scores, reverse=True) and scores[-1] > 0
        assert {hit.conversation for hit in found.messages} == {
            "Hanchin",
            "Design room",
        }
        assert all(any(matched for _, matched in hit.parts) for hit in found.messages)
        newest = await call(
            client, Found, "search", "周五下午", "time", limit=found.total
        )
        times = [hit.at_ms for hit in newest.messages]
        assert times == sorted(times, reverse=True)
        assert {hit.message_id for hit in newest.messages} == {
            hit.message_id for hit in found.messages
        }
        since = times[1]
        recent = await call(client, Found, "search", "周五下午", "time", after_ms=since)
        assert [hit.message_id for hit in recent.messages] == [
            hit.message_id for hit in newest.messages if hit.at_ms >= since
        ]
        mine = await call(client, Found, "search", "周五下午", "time", sender="self")
        assert mine.total == len(mine.messages) == 1
        assert mine.messages[0].direction == "outbound"
        theirs = await call(
            client, Found, "search", "周五下午", "time", sender="@Hanchin"
        )
        assert theirs.total == len(theirs.messages) == 3
        assert all(
            hit.sender is not None and hit.sender.id == HANCHIN["id"]
            for hit in theirs.messages
        )
        grouped = await call(
            client, Found, "search", "周五下午", "relevance", thread_id=group.thread_id
        )
        assert {hit.thread_id for hit in grouped.messages} == {group.thread_id}
        assert grouped.total == len(grouped.messages)
        scheduled = await call(client, Found, "search", "排期表", "time")
        assert {hit.conversation for hit in scheduled.messages} == {"Hanchin", "李雷"}
        assert scheduled.total == len(scheduled.messages)
        # fragments shorter than the index takes are found as they are written
        drafted = await call(client, Found, "search", "设计 盘", "relevance")
        assert [hit.thread_id for hit in drafted.messages] == [group.thread_id]

        (item,) = next(
            message
            for message in (await history(direct.thread_id)).messages
            if message.message_id == checks
        ).attachments
        assert item.attachment_id is not None and item.media_type == "image/png"
        segment, data = await attachment(client, item.attachment_id)
        (stored,) = await asyncio.to_thread(
            lambda: tuple(
                (tmp_path / ".bcn" / "workspaces" / AGENT["id"] / "attachments").glob(
                    f"{item.attachment_id}.*"
                )
            )
        )
        assert data == await asyncio.to_thread(stored.read_bytes)
        assert segment.media_type == "image/png" and segment.size_bytes == len(data)

        reminders = await call(client, tuple[Reminder, ...], "reminders")
        assert [reminder.reminder_id for reminder in reminders] == [
            daily["result"]["reminder"]["reminder_id"],
            later["result"]["reminder"]["reminder_id"],
        ]
        assert [reminder.thread_id for reminder in reminders] == [
            group.thread_id,
            direct.thread_id,
        ]
        # each named as the list names its conversation, of its kind
        assert [(reminder.conversation, reminder.kind) for reminder in reminders] == [
            (group.name, group.kind),
            (direct.name, direct.kind),
        ]
        assert reminders[0].repeat_rule == "daily@09:00"
        assert reminders[0].timezone == "Asia/Shanghai"
        assert (
            await call(client, tuple[Reminder, ...], "reminders", group.thread_id)
            == reminders[:1]
        )


async def test_the_reads_go_on_while_the_agent_is_stopped(tmp_path: Path):
    config = await configure(tmp_path)
    shown = await picture(tmp_path)

    async def read(client: Client, thread_id: str, attachment_id: str) -> list[object]:
        return [
            await call(client, Page, "conversations", "approved"),
            await call(client, Detail, "conversation", thread_id),
            await call(client, History, "history", thread_id),
            await call(client, Found, "search", "截图", "relevance"),
            await attachment(client, attachment_id),
            await call(client, tuple[Reminder, ...], "reminders"),
        ]

    async with node(config), sidecar() as client:
        checks = await say(
            tmp_path,
            "hanchin",
            HANCHIN,
            "检查结果截图在这里，麻烦看一下。",
            attachments=[{"path": str(shown), "media_type": "image/png"}],
        )
        (direct,) = (
            await settled(
                lambda: call(client, Page, "conversations", "approved"),
                lambda found: found.total == 1,
            )
        ).conversations
        await session(tmp_path)
        await act(client, tmp_path, "messages", "message", "check")
        sent = await act(
            client,
            tmp_path,
            "messages",
            "message",
            "send",
            target="dm:@hanchin",
            body="截图收到了，我看一下。",
        )
        reminded = await act(
            client,
            tmp_path,
            "reminder",
            "reminder",
            "schedule",
            title="回复截图里的问题",
            message_id=checks,
            delay_seconds=3600,
        )
        assert sent["ok"] and reminded["ok"], (sent, reminded)
        whole = await settled(
            lambda: call(client, History, "history", direct.thread_id),
            lambda read: (
                len(read.messages) == 2
                and read.messages[0].attachments[0].state == "ready"
            ),
        )
        attachment_id = whole.messages[0].attachments[0].attachment_id
        assert attachment_id is not None
        running = await read(client, direct.thread_id, attachment_id)
    await stop_agents(config)
    async with node(config), sidecar() as client:
        stopped = await act(client, tmp_path, "messages", "message", "check")
        assert stopped["code"] == "AGENT_NOT_AVAILABLE", stopped
        assert await read(client, direct.thread_id, attachment_id) == running
