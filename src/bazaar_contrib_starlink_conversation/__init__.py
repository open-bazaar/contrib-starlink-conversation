"""What a Bazaar Compute server reads of the conversations on a node, through
starlink: each agent's conversations, their members and history, what a
search finds, the attachments that came in and the reminders still to fire,
read from the node's tables whether or not the agent runs."""

from __future__ import annotations

from contextlib import asynccontextmanager
lazy from collections.abc import AsyncIterator

from bazaar_contrib_messages import MESSAGES
from bazaar_contrib_reminder import REMINDER
from bazaar_contrib_sidecar import RemoteEntry
from bazaar_contrib_storage import DB
from bazaar_core import Context, Contrib, ServiceKey

lazy from .service import Conversations

__all__ = ["CONVERSATION", "Conversations", "conversation", "remote"]

CONVERSATION = ServiceKey["Conversations"]("starlink.conversation")


@asynccontextmanager
async def _conversation(ctx: Context) -> AsyncIterator[None]:
    ctx.offer(CONVERSATION, Conversations(ctx.accept(DB)))
    yield


# the tables read are messages' and reminder's: both start first, and the
# entry does not start without them
conversation = Contrib(
    accepts=[MESSAGES, REMINDER, DB], offers=[CONVERSATION], lifespan=_conversation
)
remote = RemoteEntry(
    CONVERSATION,
    (
        "conversations",
        "conversation",
        "history",
        "timeline",
        "search",
        "attachment",
        "reminders",
    ),
)
