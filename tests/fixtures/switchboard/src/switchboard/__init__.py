"""A channel and a runtime enough for an agent to run on a real node while a
test talks to it: the channel takes the messages a test leaves in its inbox
and confirms whatever the agent sends, and the runtime ends each turn at once
and leaves its session's environment where the test can act as the agent."""
# ruff: noqa: ASYNC109

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from time import time_ns
from typing import Any, ClassVar

from bazaar_contrib_channel import (
    CHANNEL,
    CHANNEL_BUILDERS,
    CHANNEL_INPUT,
    ChannelBuilderConfig,
    ChannelDeliveryReceipt,
    ChannelIdentity,
    ChannelInput,
    ChannelSendAttempt,
    ChannelSendPart,
    ChannelSendRequest,
    ChannelTarget,
    ChannelTargetPresentation,
    InboundAttachment,
    InboundMessage,
    SenderIdentity,
    channel_builder_key,
)
from bazaar_contrib_harness import (
    HARNESS,
    HARNESS_BUILDERS,
    HARNESS_INPUT,
    HarnessBuilderConfig,
    HarnessInspection,
    HarnessOutput,
    HarnessSession,
    HarnessSessionReconciliation,
    HarnessTurn,
    ProviderCallResult,
    ProviderCallStatus,
    SessionInput,
    TurnCompleted,
    TurnStarted,
    harness_builder_key,
)
from bazaar_core import Context, Contrib
from pydantic import BaseModel, ConfigDict, Field


def switchboard(agent_id: str) -> Path:
    """Where a test talks to an agent: an inbox of messages, each a JSON file
    named by the message's id, and the environment of the agent's session."""
    return Path(os.environ["BCN_HOME"]) / "switchboard" / agent_id


class ChannelConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    bot: str = Field(min_length=1)


@asynccontextmanager
async def _content(path: Path) -> AsyncIterator[AsyncIterator[bytes]]:
    async def chunks() -> AsyncIterator[bytes]:
        yield await asyncio.to_thread(path.read_bytes)

    yield chunks()


class SwitchboardChannel:
    def __init__(self, input: ChannelInput, bot: str) -> None:
        self.input = input
        self.bot = bot
        self.stopped = asyncio.Event()

    def get_identity(self) -> ChannelIdentity:
        return ChannelIdentity(id=f"{self.input.agent_id}-{self.bot}", name=self.bot)

    def backfill_provider_thread_id(
        self, provider_thread_id: str, *, channel: str, channel_identity: str
    ) -> str:
        return provider_thread_id

    def dm_address(self, request: Any) -> None:
        return None

    async def receive(self) -> AsyncIterator[InboundMessage]:
        inbox = switchboard(self.input.agent_id) / "inbox"
        while not self.stopped.is_set():
            for path in await asyncio.to_thread(lambda: sorted(inbox.glob("*.json"))):
                message = json.loads(await asyncio.to_thread(path.read_text))
                await asyncio.to_thread(path.unlink)
                chat = f"switchboard:{self.input.agent_id}:{message['kind']}:{message['chat']}"
                yield InboundMessage(
                    id=path.stem,
                    provider_message_id=path.stem,
                    channel_session_id=chat,
                    thread_id=chat,
                    target=ChannelTarget(
                        kind=message["kind"], thread_id=message["chat"]
                    ),
                    target_presentation=ChannelTargetPresentation(
                        display_name=message["title"]
                    )
                    if message["kind"] == "group"
                    else ChannelTargetPresentation(handle=message["chat"]),
                    sender=SenderIdentity(**message["sender"]),
                    body=message["text"],
                    attachments=tuple(
                        InboundAttachment(
                            name=Path(item["path"]).name,
                            source=_content(Path(item["path"])),
                            media_type=item["media_type"],
                        )
                        for item in message["attachments"]
                    ),
                    occurred_at_ms=time_ns() // 1_000_000,
                    metadata={"review": message["review"]},
                )
            try:
                async with asyncio.timeout(0.1):
                    await self.stopped.wait()
            except TimeoutError:
                pass

    async def prepare_send(
        self, request: ChannelSendRequest
    ) -> tuple[ChannelSendPart, ...]:
        async def send(attempt: ChannelSendAttempt) -> ChannelDeliveryReceipt:
            return ChannelDeliveryReceipt(
                status="confirmed",
                provider_message_id=attempt.id,
                provider_thread_id=request.target.thread_id,
            )

        return (ChannelSendPart(send=send, message=request),)

    async def request_approval(self, request: Any, *, timeout: float) -> Any:
        raise NotImplementedError("the switchboard channel asks nothing")


@asynccontextmanager
async def channel_lifespan(ctx: Context, config: ChannelConfig):
    channel = SwitchboardChannel(ctx.accept(CHANNEL_INPUT), config.bot)
    ctx.offer(CHANNEL, channel)
    try:
        yield
    finally:
        channel.stopped.set()


channel = Contrib(
    config=ChannelConfig,
    accepts=[CHANNEL_INPUT],
    offers=[CHANNEL],
    lifespan=channel_lifespan,
)


class ChannelBuilderParameters(ChannelBuilderConfig):
    kind: ClassVar[str] = "switchboard-channel"
    entity_model: ClassVar[type[BaseModel]] = ChannelConfig
    entry: ClassVar[Contrib[Any]] = channel


@asynccontextmanager
async def channel_builder_lifespan(ctx: Context, config: ChannelBuilderParameters):
    with ctx.accept(CHANNEL_BUILDERS).register(config.builder()) as builder:
        ctx.offer(channel_builder_key(builder.kind), builder)
        yield


channel_builder = Contrib(
    config=ChannelBuilderParameters,
    accepts=[CHANNEL_BUILDERS],
    offers=[channel_builder_key("switchboard-channel")],
    lifespan=channel_builder_lifespan,
)


class RuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SwitchboardTurn:
    """A turn that starts and completes at once."""

    def __init__(self, turn: HarnessTurn) -> None:
        self.turn = turn
        self.said: list[TurnStarted | TurnCompleted] = [
            TurnStarted(event_name="switchboard.started"),
            TurnCompleted(event_name="switchboard.completed"),
        ]

    def __aiter__(self) -> SwitchboardTurn:
        return self

    async def __anext__(self) -> HarnessOutput:
        if not self.said:
            raise StopAsyncIteration
        return HarnessOutput(
            session_id=self.turn.session_id,
            turn_id=self.turn.id,
            payload=self.said.pop(0),
            provider="switchboard",
        )

    async def aclose(self) -> None:
        self.said.clear()


class SwitchboardRuntime:
    def __init__(self, agent_id: str) -> None:
        self.agent_id = agent_id
        self.events: asyncio.Queue[Any] = asyncio.Queue()

    async def receive_event(self) -> Any:
        return await self.events.get()

    async def start_session(
        self, request: SessionInput, *, timeout: float
    ) -> ProviderCallResult[HarnessSession]:
        directory = switchboard(self.agent_id)
        await asyncio.to_thread(directory.mkdir, parents=True, exist_ok=True)
        # written aside and moved in, so the test never reads half of it
        staged = directory / "session.part"
        await asyncio.to_thread(staged.write_text, json.dumps(request.environment))
        await asyncio.to_thread(os.replace, staged, directory / "session.json")
        return ProviderCallResult(
            status=ProviderCallStatus.CONFIRMED,
            value=HarnessSession(id=request.id, provider_thread_id=request.id),
        )

    async def reconcile_session(
        self, session: HarnessSession, *args: Any, **kwargs: Any
    ) -> ProviderCallResult[HarnessSessionReconciliation]:
        return ProviderCallResult(
            status=ProviderCallStatus.CONFIRMED,
            value=HarnessSessionReconciliation(session),
        )

    async def start_turn(
        self, session: HarnessSession, turn: HarnessTurn, *args: Any, **kwargs: Any
    ) -> SwitchboardTurn:
        return SwitchboardTurn(turn)

    async def steer_turn(self, *args: Any, **kwargs: Any) -> bool:
        return False

    async def has_background_job(self, *args: Any, **kwargs: Any) -> bool:
        return False

    async def stop_session(
        self, session: HarnessSession, *, timeout: float
    ) -> ProviderCallResult[HarnessSession]:
        return ProviderCallResult(status=ProviderCallStatus.CONFIRMED, value=session)


@asynccontextmanager
async def runtime_lifespan(ctx: Context, config: RuntimeConfig):
    del config
    ctx.offer(HARNESS, SwitchboardRuntime(ctx.accept(HARNESS_INPUT).agent_id))
    yield


runtime = Contrib(
    config=RuntimeConfig,
    accepts=[HARNESS_INPUT],
    offers=[HARNESS],
    lifespan=runtime_lifespan,
)


class RuntimeBuilderParameters(HarnessBuilderConfig):
    kind: ClassVar[str] = "switchboard-runtime"
    entity_model: ClassVar[type[BaseModel]] = RuntimeConfig
    entry: ClassVar[Contrib[Any]] = runtime

    @classmethod
    async def inspect(cls, workspace: Path, *, timeout: float) -> HarnessInspection:
        return HarnessInspection(available=True)


@asynccontextmanager
async def runtime_builder_lifespan(ctx: Context, config: RuntimeBuilderParameters):
    with ctx.accept(HARNESS_BUILDERS).register(config.builder()) as builder:
        ctx.offer(harness_builder_key(builder.kind), builder)
        yield


runtime_builder = Contrib(
    config=RuntimeBuilderParameters,
    accepts=[HARNESS_BUILDERS],
    offers=[harness_builder_key("switchboard-runtime")],
    lifespan=runtime_builder_lifespan,
)
