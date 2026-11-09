"""A real node in a process of its own, running one agent on the switchboard's
channel and runtime with starlink-conversation enabled, and the sidecar a
test calls its members through, as starlink does for the server."""

from __future__ import annotations

from contextlib import asynccontextmanager
lazy import asyncio
lazy import json
lazy import sys
lazy import tomllib
lazy from collections.abc import AsyncIterator, Sequence
lazy from pathlib import Path
lazy from typing import Any, cast
lazy from uuid import uuid7

import tomli_w
from bazaar_compute_node.boot import defaults
from bazaar_contrib_sidecar import CLIENT, Client
from bazaar_core import Context, Contrib, ContribEntry, ContribHub, ContribSpec, load

# how long a node takes to come up
WAIT_SECONDS = 120

AGENT = {
    "id": "cloud",
    "name": "Cloud",
    "mode": "dangerous_individual",
    "channel": [{"kind": "switchboard-channel", "bot": "cloud-bot"}],
    "harness": [{"kind": "switchboard-runtime"}],
}


async def configure(home: Path) -> Path:
    """The node's defaults with the agent, the switchboard and this package."""
    document = await asyncio.to_thread(defaults)
    for package in document["contribs"]:
        for entry in package["enabled"]:
            if entry["entry"] == "agent":
                entry["config"]["agents"] = [AGENT]
    document["contribs"] += [
        {
            "package": "switchboard",
            "enabled": [
                {"entry": "switchboard-channel-builder"},
                {"entry": "switchboard-runtime-builder"},
            ],
        },
        {
            "package": "bazaar-contrib-starlink-conversation",
            "enabled": [{"entry": "starlink-conversation"}],
        },
    ]
    config = home / "node.toml"
    await asyncio.to_thread(config.write_text, tomli_w.dumps(document))
    return config


async def stop_agents(config: Path) -> None:
    """Leave the agent configured but have the node run no agent."""
    document = tomllib.loads(await asyncio.to_thread(config.read_text))
    for package in document["contribs"]:
        package["enabled"] = [
            entry for entry in package["enabled"] if entry["entry"] != "agent-execution"
        ]
    await asyncio.to_thread(config.write_text, tomli_w.dumps(document))


@asynccontextmanager
async def node(config: Path) -> AsyncIterator[asyncio.subprocess.Process]:
    output = config.with_suffix(".log")
    log = await asyncio.to_thread(output.open, "wb")
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(Path(__file__).with_name("run_node.py")),
        "config-filesystem",
        str(config),
        stdin=asyncio.subprocess.PIPE,
        stdout=log,
        stderr=asyncio.subprocess.STDOUT,
    )
    await asyncio.to_thread(log.close)
    try:
        async with asyncio.timeout(WAIT_SECONDS):
            while b"bcn ready" not in await asyncio.to_thread(output.read_bytes):
                assert process.returncode is None, await asyncio.to_thread(
                    output.read_text
                )
                await asyncio.sleep(0.2)
        yield process
    finally:
        if process.returncode is None:
            assert process.stdin is not None
            process.stdin.close()
            await process.wait()


@asynccontextmanager
async def sidecar() -> AsyncIterator[Client]:
    """A client of the node's sidecar, found where the node serves it."""
    clients: list[Client] = []

    @asynccontextmanager
    async def take(ctx: Context) -> AsyncIterator[None]:
        clients.append(ctx.accept(CLIENT))
        yield

    composition = await asyncio.to_thread(
        load,
        [
            ContribSpec(contrib="timer-wheel"),
            ContribSpec(
                contrib="sidecar-windows" if sys.platform == "win32" else "sidecar-unix"
            ),
        ],
    )
    composition = composition.model_copy(
        update={
            "entries": (
                *composition.entries,
                ContribEntry(
                    name="caller", definition=Contrib(accepts=[CLIENT], lifespan=take)
                ),
            )
        }
    )
    async with ContribHub() as hub, hub.enable(composition):
        yield clients[0]


async def say(
    home: Path,
    chat: str,
    sender: dict[str, str],
    text: str,
    *,
    kind: str = "dm",
    title: str | None = None,
    review: str = "approved",
    attachments: Sequence[dict[str, str]] = (),
) -> str:
    """Leave a message in the agent's inbox; its id."""
    inbox = home / "switchboard" / AGENT["id"] / "inbox"
    await asyncio.to_thread(inbox.mkdir, parents=True, exist_ok=True)
    id = str(uuid7())
    message = {
        "chat": chat,
        "kind": kind,
        "title": title,
        "sender": sender,
        "text": text,
        "review": review,
        "attachments": list(attachments),
    }
    # written aside and moved in, so the channel never reads half of it
    staged = inbox / f"{id}.part"
    await asyncio.to_thread(staged.write_text, json.dumps(message))
    await asyncio.to_thread(staged.rename, inbox / f"{id}.json")
    return id


async def act(
    client: Client, home: Path, service: str, resource: str, command: str, **values: Any
) -> dict[str, Any]:
    """A command the agent runs, as bcc runs it in the agent's session."""
    environment = json.loads(
        await asyncio.to_thread(
            (home / "switchboard" / AGENT["id"] / "session.json").read_text
        )
    )
    return cast(
        dict[str, Any],
        await client.call(
            service,
            "request",
            {
                "kind": "command",
                "resource": resource,
                "command": command,
                "agent_id": environment["BCN_AGENT_ID"],
                "actor_id": environment["BCN_ACTOR_ID"],
                "runtime_session_id": environment["BCN_RUNTIME_SESSION_ID"],
                "session_capability": environment["BCN_COMMAND_CAPABILITY"],
                **values,
            },
        ),
    )
