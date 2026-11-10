"""An agent's conversations as its server reads them: a page of them, one in
particular with its members, a stretch of its history, what a search finds,
a segment of an attachment's bytes and the reminders still to fire."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

type Kind = Literal["dm", "group"]
type Review = Literal["pending", "approved", "denied"]
type Direction = Literal["inbound", "outbound"]
type SenderKind = Literal["human", "agent", "system", "unknown"]


@dataclass(frozen=True, slots=True)
class Sender:
    id: str | None
    name: str | None
    display_name: str | None


@dataclass(frozen=True, slots=True)
class Preview:
    message_id: str
    sender: Sender | None
    sender_kind: SenderKind
    at_ms: int
    text: str


@dataclass(frozen=True, slots=True)
class Conversation:
    thread_id: str
    name: str | None
    # the peer's, in a DM
    handle: str | None
    kind: Kind
    channel: str
    review: Review
    last_activity_at_ms: int
    pending_count: int
    member_count: int
    first: Preview | None
    latest: Preview | None


@dataclass(frozen=True, slots=True)
class Page:
    conversations: tuple[Conversation, ...]
    total: int
    pending_review: int
    has_more: bool


@dataclass(frozen=True, slots=True)
class Member:
    sender: Sender | None
    sender_kind: SenderKind
    is_self: bool


@dataclass(frozen=True, slots=True)
class Detail:
    conversation: Conversation
    created_at_ms: int
    message_count: int
    # the latest to speak first
    members: tuple[Member, ...]


@dataclass(frozen=True, slots=True)
class Attachment:
    attachment_id: str | None
    name: str
    kind: str
    state: str
    media_type: str | None
    size_bytes: int | None


@dataclass(frozen=True, slots=True)
class Entry:
    message_id: str
    seq: int
    direction: Direction
    sender: Sender | None
    sender_kind: SenderKind
    at_ms: int
    body: str
    system_message_kind: str | None
    attachments: tuple[Attachment, ...]


@dataclass(frozen=True, slots=True)
class History:
    messages: tuple[Entry, ...]
    has_before: bool
    has_after: bool


@dataclass(frozen=True, slots=True)
class Hit:
    message_id: str
    thread_id: str
    conversation: str | None
    kind: Kind
    channel: str
    direction: Direction
    sender: Sender | None
    sender_kind: SenderKind
    at_ms: int
    snippet: str
    # the snippet in pieces, each saying whether it is a fragment of the query
    parts: tuple[tuple[str, bool], ...]
    score: float


@dataclass(frozen=True, slots=True)
class Found:
    query: str
    sort: Literal["relevance", "time"]
    messages: tuple[Hit, ...]
    total: int
    offset: int
    has_more: bool
    next_offset: int | None


@dataclass(frozen=True, slots=True)
class Segment:
    media_type: str | None
    size_bytes: int
    # base64
    data: str


@dataclass(frozen=True, slots=True)
class Reminder:
    reminder_id: str
    thread_id: str
    # what the conversation is called and what kind it is, as
    # `conversations` names them
    conversation: str | None
    kind: Kind
    title: str
    next_fire_at_ms: int
    # every:<N><m|h|d>, daily@HH:MM or weekly:<day>@HH:MM
    repeat_rule: str | None
    timezone: str
