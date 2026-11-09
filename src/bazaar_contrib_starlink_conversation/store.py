"""An agent's conversations read straight from the node's tables: threads
and channel_sessions, messages with their inbound_attachments and
full-text index, consumer_cursors, and reminders."""

from __future__ import annotations

from dataclasses import dataclass
lazy import json
lazy import re
lazy from collections.abc import Sequence
lazy from typing import Literal, cast

lazy from bazaar_contrib_storage import (
    All,
    AnyOf,
    Binary,
    Call,
    Case,
    Column,
    Compare,
    Contains,
    Database,
    Expression,
    In,
    IsNull,
    Join,
    Predicate,
    Record,
    Scalar,
    Search,
    Select,
    Session,
    Sort,
    Value,
)

lazy from .models import (
    Attachment,
    Conversation,
    Detail,
    Direction,
    Entry,
    Found,
    History,
    Hit,
    Kind,
    Member,
    Page,
    Preview,
    Reminder,
    Review,
    Sender,
    SenderKind,
)

# how much of a message a preview or a search snippet shows
EXCERPT = 240


@dataclass(frozen=True, slots=True)
class _Said:
    """A message as its row has it."""

    message_id: str
    seq: int
    thread_id: str
    channel: str
    kind: Kind
    direction: Direction
    sender: Sender | None
    sender_kind: SenderKind
    system_message_kind: str | None
    at_ms: int
    body: str
    attachments_json: str | None


def _equal(name: str, value: Scalar, table: str | None = None) -> Compare:
    return Compare(
        left=Column(name=name, table=table), operator="=", right=Value(value=value)
    )


def _history(agent_id: str) -> All:
    """What a conversation's history holds: the messages that came in, and
    those that went out queued or sent."""

    return All(
        conditions=(
            _equal("agent_id", agent_id, "message"),
            AnyOf(
                conditions=(
                    _equal("direction", "inbound", "message"),
                    All(
                        conditions=(
                            _equal("direction", "outbound", "message"),
                            In(
                                expression=Column(
                                    name="delivery_state", table="message"
                                ),
                                values=("queued", "sent"),
                            ),
                        )
                    ),
                )
            ),
        )
    )


def _said_at() -> Call:
    """When the provider says a message was sent, or else when it arrived or
    was written."""

    return Call(
        name="coalesce",
        arguments=tuple(
            Column(name=name, table="message")
            for name in ("provider_time_ms", "received_at_ms", "created_at_ms")
        ),
    )


def _channel(agent_id: str) -> Join:
    """A thread's channel session."""

    return Join(
        table="channel_sessions",
        alias="channel",
        on=All(
            conditions=(
                _equal("agent_id", agent_id, "channel"),
                Compare(
                    left=Column(name="id", table="channel"),
                    operator="=",
                    right=Column(name="channel_session_id", table="thread"),
                ),
            )
        ),
    )


def _said_columns() -> dict[str, Expression]:
    return {
        **{
            name: Column(name=name, table="message")
            for name in (
                "message_id",
                "seq",
                "thread_id",
                "channel",
                "target_kind",
                "direction",
                "sender",
                "sender_id",
                "sender_display_name",
                "body",
                "metadata_json",
                "attachments_json",
            )
        },
        "at_ms": _said_at(),
    }


def _said(row: Record) -> _Said:
    metadata = json.loads(cast(str | None, row["metadata_json"]) or "{}")
    direction = cast(Direction, row["direction"])
    sender_kind = (
        "agent" if direction == "outbound" else metadata.get("sender_kind", "unknown")
    )
    return _Said(
        message_id=cast(str, row["message_id"]),
        seq=cast(int, row["seq"]),
        thread_id=cast(str, row["thread_id"]),
        channel=cast(str, row["channel"]),
        kind=cast(Kind, row["target_kind"]),
        direction=direction,
        sender=None
        if row["sender"] is None and row["sender_id"] is None
        else Sender(
            id=cast(str | None, row["sender_id"]),
            name=cast(str | None, row["sender"]),
            display_name=cast(str | None, row["sender_display_name"]),
        ),
        sender_kind=sender_kind,
        system_message_kind=metadata.get("system_message_kind")
        if sender_kind == "system"
        else None,
        at_ms=cast(int, row["at_ms"]),
        body=cast(str, row["body"]),
        attachments_json=cast(str | None, row["attachments_json"]),
    )


async def _ends(
    session: Session,
    agent_id: str,
    threads: Sequence[str],
    *,
    latest: bool,
) -> dict[str, list[_Said]]:
    """By conversation, each sender's latest message, the newest first and
    whatever the agent sent being one sender's; or else the first message."""

    rows = await session.select(
        Select(
            table="messages",
            alias="message",
            ctes={
                "ends": Select(
                    table="messages",
                    alias="message",
                    columns={
                        "seq": Call(
                            name="max" if latest else "min",
                            arguments=(Column(name="seq", table="message"),),
                        )
                    },
                    where=All(
                        conditions=(
                            _history(agent_id),
                            In(
                                expression=Column(name="thread_id", table="message"),
                                values=tuple(threads),
                            ),
                        )
                    ),
                    group_by=(
                        Column(name="thread_id", table="message"),
                        *(
                            (
                                Case(
                                    when=_equal("direction", "outbound", "message"),
                                    then=Value(value=""),
                                    otherwise=Call(
                                        name="coalesce",
                                        arguments=(
                                            Column(name="sender_id", table="message"),
                                            Column(name="sender", table="message"),
                                        ),
                                    ),
                                ),
                            )
                            if latest
                            else ()
                        ),
                    ),
                )
            },
            columns=_said_columns(),
            joins=(
                Join(
                    table="ends",
                    alias="ends",
                    on=Compare(
                        left=Column(name="seq", table="ends"),
                        operator="=",
                        right=Column(name="seq", table="message"),
                    ),
                ),
            ),
            order_by=(
                Sort(expression=Column(name="seq", table="message"), descending=True),
            ),
        )
    )
    ends: dict[str, list[_Said]] = {}
    for row in rows:
        said = _said(row)
        ends.setdefault(said.thread_id, []).append(said)
    return ends


def _name(
    kind: Kind, display_name: str | None, handle: str | None, spoken: Sequence[_Said]
) -> str | None:
    """A group by its name; a DM by its peer's latest display name, or else
    their handle. `spoken` is each sender's latest message, newest first."""

    if kind == "group":
        return display_name
    peer = next(
        (
            said.sender
            for said in spoken
            if said.direction == "inbound" and said.sender_kind != "system"
        ),
        None,
    )
    return (None if peer is None else peer.display_name) or handle


def _preview(said: _Said) -> Preview:
    return Preview(
        message_id=said.message_id,
        sender=said.sender,
        sender_kind=said.sender_kind,
        at_ms=said.at_ms,
        text=said.body[:EXCERPT],
    )


def _directory(agent_id: str) -> dict[str, Select]:
    """The node's directory of an agent's conversations: each with its channel
    session, when anything last happened in it, and how many of the messages
    that wake the agent it has not read."""

    def known(table: str, *names: str) -> Call:
        return Call(
            name="coalesce",
            arguments=(
                *(Column(name=name, table=table) for name in names),
                Value(value=0),
            ),
        )

    return {
        "ends": Select(
            table="messages",
            alias="message",
            columns={
                "thread_id": Column(name="thread_id", table="message"),
                "seq": Call(
                    name="max", arguments=(Column(name="seq", table="message"),)
                ),
            },
            where=_history(agent_id),
            group_by=(Column(name="thread_id", table="message"),),
        ),
        "unread": Select(
            table="messages",
            alias="message",
            columns={
                "thread_id": Column(name="thread_id", table="message"),
                "pending_count": Call(name="count"),
            },
            joins=(
                Join(
                    table="consumer_cursors",
                    alias="cursor",
                    kind="left",
                    on=Compare(
                        left=Column(name="thread_id", table="cursor"),
                        operator="=",
                        right=Column(name="thread_id", table="message"),
                    ),
                ),
            ),
            where=All(
                conditions=(
                    _equal("agent_id", agent_id, "message"),
                    _equal("direction", "inbound", "message"),
                    _equal("notifies_runtime", 1, "message"),
                    Compare(
                        left=Column(name="seq", table="message"),
                        operator=">",
                        right=known("cursor", "delivered_through_seq"),
                    ),
                )
            ),
            group_by=(Column(name="thread_id", table="message"),),
        ),
        "directory": Select(
            table="threads",
            alias="thread",
            columns={
                "thread_id": Column(name="id", table="thread"),
                **{
                    alias: Column(name=name, table="channel")
                    for alias, name in (
                        ("kind", "target_kind"),
                        ("channel", "channel"),
                        ("review", "review"),
                        ("display_name", "target_display_name"),
                        ("handle", "target_handle"),
                        ("created_at_ms", "created_at_ms"),
                    )
                },
                "pending_count": known("unread", "pending_count"),
                "last_activity_at_ms": Call(
                    name="max",
                    arguments=(
                        known("thread", "last_activity_at_ms"),
                        known("latest", "received_at_ms", "created_at_ms"),
                        known("channel", "last_inbound_at_ms"),
                        known("channel", "last_outbound_at_ms"),
                        known("thread", "updated_at_ms"),
                        known("channel", "updated_at_ms"),
                        known("thread", "created_at_ms"),
                        known("channel", "created_at_ms"),
                    ),
                ),
            },
            joins=(
                _channel(agent_id),
                *(
                    Join(
                        table=table,
                        alias=table,
                        kind="left",
                        on=Compare(
                            left=Column(name="thread_id", table=table),
                            operator="=",
                            right=Column(name="id", table="thread"),
                        ),
                    )
                    for table in ("ends", "unread")
                ),
                Join(
                    table="messages",
                    alias="latest",
                    kind="left",
                    on=Compare(
                        left=Column(name="seq", table="latest"),
                        operator="=",
                        right=Column(name="seq", table="ends"),
                    ),
                ),
            ),
            where=_equal("agent_id", agent_id, "thread"),
        ),
    }


def _conversation(
    row: Record, spoken: Sequence[_Said], first: Sequence[_Said]
) -> Conversation:
    kind = cast(Kind, row["kind"])
    handle = cast(str | None, row["handle"])
    return Conversation(
        thread_id=cast(str, row["thread_id"]),
        name=_name(kind, cast(str | None, row["display_name"]), handle, spoken),
        handle=handle,
        kind=kind,
        channel=cast(str, row["channel"]),
        review=cast(Review, row["review"]),
        last_activity_at_ms=cast(int, row["last_activity_at_ms"]),
        pending_count=cast(int, row["pending_count"]),
        member_count=len(spoken),
        first=_preview(first[0]) if first else None,
        latest=_preview(spoken[0]) if spoken else None,
    )


class Store:
    """One agent's conversations in the node's database."""

    def __init__(self, database: Database, agent_id: str) -> None:
        self._database = database
        self.agent_id = agent_id

    async def page(
        self, review: Literal["approved", "pending"], offset: int, limit: int
    ) -> Page:
        async def read(session: Session) -> Page:
            counts = await session.select(
                Select(
                    table="threads",
                    alias="thread",
                    columns={
                        "review": Column(name="review", table="channel"),
                        "total": Call(name="count"),
                    },
                    joins=(_channel(self.agent_id),),
                    where=_equal("agent_id", self.agent_id, "thread"),
                    group_by=(Column(name="review", table="channel"),),
                )
            )
            totals = {row["review"]: cast(int, row["total"]) for row in counts}
            rows = await session.select(
                Select(
                    table="directory",
                    ctes=_directory(self.agent_id),
                    where=_equal("review", review),
                    order_by=(
                        Sort(
                            expression=Column(name="last_activity_at_ms"),
                            descending=True,
                        ),
                        Sort(expression=Column(name="thread_id")),
                    ),
                    limit=limit,
                    offset=offset,
                )
            )
            threads = tuple(cast(str, row["thread_id"]) for row in rows)
            spoken = await _ends(session, self.agent_id, threads, latest=True)
            first = await _ends(session, self.agent_id, threads, latest=False)
            total = totals.get(review, 0)
            return Page(
                conversations=tuple(
                    _conversation(
                        row,
                        spoken.get(cast(str, row["thread_id"]), ()),
                        first.get(cast(str, row["thread_id"]), ()),
                    )
                    for row in rows
                ),
                total=total,
                pending_review=totals.get("pending", 0),
                has_more=offset + len(rows) < total,
            )

        return await self._database.read(read, snapshot=True)

    async def detail(self, thread_id: str) -> Detail:
        async def read(session: Session) -> Detail:
            rows = await session.select(
                Select(
                    table="directory",
                    ctes=_directory(self.agent_id),
                    where=_equal("thread_id", thread_id),
                )
            )
            if not rows:
                raise ValueError(f"conversation not found: {thread_id}")
            counted = await session.select(
                Select(
                    table="messages",
                    alias="message",
                    columns={"total": Call(name="count")},
                    where=All(
                        conditions=(
                            _history(self.agent_id),
                            _equal("thread_id", thread_id, "message"),
                        )
                    ),
                )
            )
            spoken = (
                await _ends(session, self.agent_id, (thread_id,), latest=True)
            ).get(thread_id, [])
            first = (
                await _ends(session, self.agent_id, (thread_id,), latest=False)
            ).get(thread_id, [])
            return Detail(
                conversation=_conversation(rows[0], spoken, first),
                created_at_ms=cast(int, rows[0]["created_at_ms"]),
                message_count=cast(int, counted[0]["total"]),
                members=tuple(
                    Member(
                        sender=said.sender,
                        sender_kind=said.sender_kind,
                        is_self=said.direction == "outbound",
                    )
                    for said in spoken
                ),
            )

        return await self._database.read(read, snapshot=True)

    async def history(self, thread_id: str, around: str | None, limit: int) -> History:
        scope = All(
            conditions=(
                _history(self.agent_id),
                _equal("thread_id", thread_id, "message"),
            )
        )

        async def read(session: Session) -> History:
            (bounds,) = await session.select(
                Select(
                    table="messages",
                    alias="message",
                    columns={
                        "first": Call(
                            name="min", arguments=(Column(name="seq", table="message"),)
                        ),
                        "last": Call(
                            name="max", arguments=(Column(name="seq", table="message"),)
                        ),
                        "total": Call(name="count"),
                    },
                    where=scope,
                )
            )
            offset = 0
            if around is not None:
                anchor = await session.select(
                    Select(
                        table="messages",
                        alias="message",
                        columns={"seq": Column(name="seq", table="message")},
                        where=All(
                            conditions=(scope, _equal("message_id", around, "message"))
                        ),
                    )
                )
                if not anchor:
                    raise ValueError(
                        f"message not found in this conversation: {around}"
                    )
                (position,) = await session.select(
                    Select(
                        table="messages",
                        alias="message",
                        columns={"total": Call(name="count")},
                        where=All(
                            conditions=(
                                scope,
                                Compare(
                                    left=Column(name="seq", table="message"),
                                    operator="<=",
                                    right=Value(value=anchor[0]["seq"]),
                                ),
                            )
                        ),
                    )
                )
                # the message in the middle, or as near it as the ends allow
                start = max(cast(int, position["total"]) - limit // 2, 1)
                offset = min(start, max(cast(int, bounds["total"]) - limit + 1, 1)) - 1
            messages = tuple(
                _said(row)
                for row in await session.select(
                    Select(
                        table="messages",
                        alias="message",
                        columns=_said_columns(),
                        where=scope,
                        order_by=(
                            Sort(expression=Column(name="seq", table="message")),
                        ),
                        limit=limit,
                        offset=offset,
                    )
                )
            )
            inbound: dict[str, list[Attachment]] = {}
            for row in await session.select(
                Select(
                    table="inbound_attachments",
                    where=In(
                        expression=Column(name="message_id"),
                        values=tuple(
                            message.message_id
                            for message in messages
                            if message.direction == "inbound"
                        ),
                    ),
                    order_by=tuple(
                        Sort(expression=Column(name=name))
                        for name in ("message_id", "ordinal")
                    ),
                )
            ):
                inbound.setdefault(cast(str, row["message_id"]), []).append(
                    Attachment(
                        attachment_id=cast(str, row["attachment_id"]),
                        name=cast(str, row["name"]),
                        kind=cast(str, row["kind"]),
                        state=cast(str, row["state"]),
                        media_type=cast(str | None, row["media_type"]),
                        size_bytes=cast(int | None, row["size_bytes"]),
                    )
                )
            return History(
                messages=tuple(
                    Entry(
                        message_id=message.message_id,
                        seq=message.seq,
                        direction=message.direction,
                        sender=message.sender,
                        sender_kind=message.sender_kind,
                        at_ms=message.at_ms,
                        body=message.body,
                        system_message_kind=message.system_message_kind,
                        attachments=tuple(inbound.get(message.message_id, ()))
                        if message.direction == "inbound"
                        else tuple(
                            Attachment(
                                attachment_id=None,
                                name=item["name"],
                                kind="file",
                                state="ready",
                                media_type=item["media_type"],
                                size_bytes=item["size_bytes"],
                            )
                            for item in json.loads(message.attachments_json or "[]")
                        ),
                    )
                    for message in messages
                ),
                has_before=bool(messages)
                and messages[0].seq > cast(int, bounds["first"]),
                has_after=bool(messages)
                and messages[-1].seq < cast(int, bounds["last"]),
            )

        return await self._database.read(read, snapshot=True)

    async def search(
        self,
        query: str,
        sort: Literal["relevance", "time"],
        thread_id: str | None,
        sender: str | None,
        after_ms: int | None,
        before_ms: int | None,
        offset: int,
        limit: int,
    ) -> Found:
        query = query.strip()
        fragments = list(dict.fromkeys(query.split()))
        body = Column(name="body", table="message")
        said_at = _said_at()
        predicates: list[Predicate] = [_history(self.agent_id)]
        if thread_id is not None:
            predicates.append(_equal("thread_id", thread_id, "message"))
        if sender == "self":
            predicates.append(_equal("direction", "outbound", "message"))
        elif sender is not None:
            token = sender.removeprefix("@")
            predicates.extend(
                (
                    _equal("direction", "inbound", "message"),
                    AnyOf(
                        conditions=(
                            Compare(
                                left=Call(
                                    name="lower",
                                    arguments=(Column(name="sender", table="message"),),
                                ),
                                operator="=",
                                right=Call(
                                    name="lower", arguments=(Value(value=token),)
                                ),
                            ),
                            All(
                                conditions=(
                                    IsNull(
                                        expression=Column(
                                            name="sender", table="message"
                                        )
                                    ),
                                    _equal("sender_id", token, "message"),
                                )
                            ),
                        )
                    ),
                )
            )
        if after_ms is not None:
            predicates.append(
                Compare(left=said_at, operator=">=", right=Value(value=after_ms))
            )
        if before_ms is not None:
            predicates.append(
                Compare(left=said_at, operator="<=", right=Value(value=before_ms))
            )
        indexed = tuple(fragment for fragment in fragments if len(fragment) >= 3)
        if indexed:
            predicates.append(
                Search(
                    index="message_search",
                    table="message",
                    columns=("body",),
                    terms=indexed,
                    row_key="seq",
                )
            )
        predicates.extend(
            Contains(expression=body, text=fragment)
            for fragment in fragments
            if len(fragment) < 3
        )
        # relevance is the share of the message the query's fragments cover
        score: Expression = Value(value=0.0)
        if sort == "relevance" and fragments:
            lowered = Call(name="lower", arguments=(body,))
            coverage: list[Expression] = [
                Binary(
                    operator="-",
                    left=Call(name="length", arguments=(lowered,)),
                    right=Call(
                        name="length",
                        arguments=(
                            Call(
                                name="replace",
                                arguments=(
                                    lowered,
                                    Call(
                                        name="lower", arguments=(Value(value=fragment),)
                                    ),
                                    Value(value=""),
                                ),
                            ),
                        ),
                    ),
                )
                for fragment in fragments
            ]
            covered = coverage[0]
            for expression in coverage[1:]:
                covered = Binary(operator="+", left=covered, right=expression)
            score = Binary(
                operator="/",
                left=covered,
                right=Binary(
                    operator="*",
                    left=Value(value=1.0),
                    right=Call(name="length", arguments=(body,)),
                ),
            )
        where = All(conditions=tuple(predicates))
        pattern = (
            re.compile(
                "("
                + "|".join(map(re.escape, sorted(fragments, key=len, reverse=True)))
                + ")",
                re.IGNORECASE,
            )
            if fragments
            else None
        )

        async def read(session: Session) -> Found:
            (counted,) = await session.select(
                Select(
                    table="messages",
                    alias="message",
                    columns={"total": Call(name="count")},
                    where=where,
                )
            )
            rows = await session.select(
                Select(
                    table="messages",
                    alias="message",
                    columns={**_said_columns(), "score": score},
                    where=where,
                    order_by=(
                        *(
                            (Sort(expression=score, descending=True),)
                            if sort == "relevance"
                            else ()
                        ),
                        Sort(expression=said_at, descending=True),
                        Sort(
                            expression=Column(name="seq", table="message"),
                            descending=True,
                        ),
                    ),
                    limit=limit + 1,
                    offset=offset,
                )
            )
            hits = [(_said(row), cast(float, row["score"])) for row in rows[:limit]]
            threads = tuple(dict.fromkeys(said.thread_id for said, _ in hits))
            sessions = {
                cast(str, row["thread_id"]): row
                for row in await session.select(
                    Select(
                        table="threads",
                        alias="thread",
                        columns={
                            "thread_id": Column(name="id", table="thread"),
                            "display_name": Column(
                                name="target_display_name", table="channel"
                            ),
                            "handle": Column(name="target_handle", table="channel"),
                        },
                        joins=(_channel(self.agent_id),),
                        where=All(
                            conditions=(
                                _equal("agent_id", self.agent_id, "thread"),
                                In(
                                    expression=Column(name="id", table="thread"),
                                    values=threads,
                                ),
                            )
                        ),
                    )
                )
            }
            spoken = await _ends(
                session,
                self.agent_id,
                tuple(
                    dict.fromkeys(
                        said.thread_id for said, _ in hits if said.kind == "dm"
                    )
                ),
                latest=True,
            )
            found: list[Hit] = []
            for said, relevance in hits:
                text = " ".join(said.body.split())
                match = pattern.search(text) if pattern else None
                start = max(0, match.start() - 80) if match else 0
                end = min(len(text), start + EXCERPT - int(start > 0))
                if end < len(text):
                    end -= 1
                snippet = (
                    ("…" if start else "")
                    + text[start:end]
                    + ("…" if end < len(text) else "")
                )
                found.append(
                    Hit(
                        message_id=said.message_id,
                        thread_id=said.thread_id,
                        conversation=_name(
                            said.kind,
                            cast(str | None, sessions[said.thread_id]["display_name"]),
                            cast(str | None, sessions[said.thread_id]["handle"]),
                            spoken.get(said.thread_id, ()),
                        ),
                        kind=said.kind,
                        channel=said.channel,
                        direction=said.direction,
                        sender=said.sender,
                        sender_kind=said.sender_kind,
                        at_ms=said.at_ms,
                        snippet=snippet,
                        parts=tuple(
                            (piece, bool(pattern and index % 2))
                            for index, piece in enumerate(
                                pattern.split(snippet) if pattern else [snippet]
                            )
                        ),
                        score=relevance,
                    )
                )
            has_more = len(rows) > limit
            return Found(
                query=query,
                sort=sort,
                messages=tuple(found),
                total=cast(int, counted["total"]),
                offset=offset,
                has_more=has_more,
                next_offset=offset + len(found) if has_more else None,
            )

        return await self._database.read(read, snapshot=True)

    async def attachment(self, attachment_id: str) -> tuple[str, str | None] | None:
        """Where a ready attachment of a message that came in is kept, in its
        agent's workspace, and its media type."""

        rows = await self._database.read(
            lambda session: session.select(
                Select(
                    table="inbound_attachments",
                    alias="attachment",
                    columns={
                        name: Column(name=name, table="attachment")
                        for name in ("relative_path", "media_type")
                    },
                    joins=(
                        Join(
                            table="messages",
                            alias="message",
                            on=Compare(
                                left=Column(name="message_id", table="message"),
                                operator="=",
                                right=Column(name="message_id", table="attachment"),
                            ),
                        ),
                    ),
                    where=All(
                        conditions=(
                            _equal("agent_id", self.agent_id, "message"),
                            _equal("direction", "inbound", "message"),
                            _equal("attachment_id", attachment_id, "attachment"),
                            _equal("state", "ready", "attachment"),
                        )
                    ),
                )
            )
        )
        return (
            (
                cast(str, rows[0]["relative_path"]),
                cast(str | None, rows[0]["media_type"]),
            )
            if rows
            else None
        )

    async def reminders(self, thread_id: str | None) -> tuple[Reminder, ...]:
        rows = await self._database.read(
            lambda session: session.select(
                Select(
                    table="reminders",
                    where=All(
                        conditions=(
                            _equal("agent_id", self.agent_id),
                            _equal("state", "scheduled"),
                            *(
                                ()
                                if thread_id is None
                                else (_equal("owner_thread_id", thread_id),)
                            ),
                        )
                    ),
                    order_by=tuple(
                        Sort(expression=Column(name=name))
                        for name in ("next_fire_at_ms", "reminder_id")
                    ),
                )
            )
        )
        return tuple(
            Reminder(
                reminder_id=cast(str, row["reminder_id"]),
                thread_id=cast(str, row["owner_thread_id"]),
                title=cast(str, row["title"]),
                next_fire_at_ms=cast(int, row["next_fire_at_ms"]),
                repeat_rule=cast(str | None, row["repeat_rule"]),
                timezone=cast(str, row["timezone"]),
            )
            for row in rows
        )
