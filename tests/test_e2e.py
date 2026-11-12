"""Drive an installed node from a second app and assert what Lark delivers."""

from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import struct
import sys
import tomllib
import zlib
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import uuid4

import aiohttp
import pytest
import tomli_w

from bazaar_contrib_lark.identity import LarkThreadIdentity, parse_provider_thread_id

API = "https://open.feishu.cn/open-apis/"
WAIT_SECONDS = 180


class App:
    def __init__(self, session: aiohttp.ClientSession, app_id: str, secret: str):
        self.session = session
        self.app_id = app_id
        self.secret = secret
        self.token: str | None = None

    async def headers(self) -> dict[str, str]:
        if self.token is None:
            async with self.session.post(
                API + "auth/v3/tenant_access_token/internal",
                json={"app_id": self.app_id, "app_secret": self.secret},
            ) as response:
                result = await response.json(content_type=None)
            if result.get("code") != 0:
                raise RuntimeError(
                    f"token failed ({result.get('code')}): {result.get('msg')}"
                )
            self.token = result["tenant_access_token"]
        return {"Authorization": f"Bearer {self.token}"}

    async def call(self, method: str, path: str, **options: Any) -> dict[str, Any]:
        async with self.session.request(
            method, API + path, headers=await self.headers(), **options
        ) as response:
            result = await response.json(content_type=None)
        if result.get("code") != 0:
            raise RuntimeError(
                f"{path} failed ({result.get('code')}): {result.get('msg')}"
            )
        return result.get("data") or {}

    async def send(
        self, chat_id: str, message_type: str, content: dict[str, Any]
    ) -> dict[str, Any]:
        return await self.call(
            "POST",
            "im/v1/messages",
            params={"receive_id_type": "chat_id"},
            json={
                "receive_id": chat_id,
                "msg_type": message_type,
                "content": json.dumps(content),
            },
        )

    async def reply(
        self, message_id: str, text: str, *, in_thread: bool = False
    ) -> dict[str, Any]:
        return await self.call(
            "POST",
            f"im/v1/messages/{message_id}/reply",
            json={
                "msg_type": "text",
                "content": json.dumps({"text": text}),
                "reply_in_thread": in_thread,
            },
        )

    async def messages(self, kind: str, container: str) -> list[dict[str, Any]]:
        page = await self.call(
            "GET",
            "im/v1/messages",
            params={
                "container_id_type": kind,
                "container_id": container,
                "sort_type": "ByCreateTimeDesc",
                "page_size": "50",
            },
        )
        return page.get("items", [])[::-1]

    async def upload_image(self, data: bytes) -> str:
        form = aiohttp.FormData()
        form.add_field("image_type", "message")
        form.add_field("image", data, filename="echo.png", content_type="image/png")
        return (await self.call("POST", "im/v1/images", data=form))["image_key"]

    async def download(self, message: dict[str, Any], kind: str) -> bytes:
        key = json.loads(message["body"]["content"])[f"{kind}_key"]
        async with self.session.get(
            API + f"im/v1/messages/{message['message_id']}/resources/{key}",
            params={"type": kind},
            headers=await self.headers(),
        ) as response:
            assert response.status == 200
            return await response.read()


def image(seed: bytes) -> bytes:
    """A small PNG whose pixels come from the seed."""

    size = 32
    rows = b"".join(
        b"\x00" + bytes(seed[(x + y) % len(seed)] for x in range(size * 3))
        for y in range(size)
    )

    def chunk(kind: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + kind
            + data
            + struct.pack(">I", zlib.crc32(kind + data))
        )

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(rows))
        + chunk(b"IEND", b"")
    )


def rendered(item: dict[str, Any]) -> str:
    """The text of a post as Lark renders it, without the Markdown it also returns."""

    post = json.loads(item["body"]["content"])
    return "\n".join(
        "".join(node.get("text", "") for node in row) for row in post["content"]
    )


async def journal(path: Path) -> list[dict[str, Any]]:
    if not await asyncio.to_thread(path.exists):
        return []
    text = await asyncio.to_thread(path.read_text)
    return [json.loads(line) for line in text.splitlines()]


@pytest.mark.e2e
async def test_installed_node_over_real_lark(tmp_path: Path):
    tested_id = os.environ["LARK_TEST_APP_ID"]
    driver_id = os.environ["LARK_DRIVER_APP_ID"]
    driver_secret = os.environ["LARK_DRIVER_APP_SECRET"]
    chat = os.environ["LARK_TEST_CHAT_ID"]
    source = (await asyncio.to_thread(Path(__file__).resolve)).parents[1]
    fixture = source / "tests/fixtures/harness"
    distributions = tmp_path / "distributions"
    config = tmp_path / "node.toml"
    events = tmp_path / "audit.jsonl"
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("BCN_", "BCC_", "CLAUDE_", "ANTHROPIC_", "OPENAI_"))
    } | {
        "BCN_HOME": str(tmp_path / "home"),
        "UV_TOOL_DIR": str(tmp_path / "tools"),
        "UV_TOOL_BIN_DIR": str(tmp_path / "bin"),
    }

    async def execute(label: str, *arguments: str) -> str:
        await asyncio.to_thread(print, "Run", label, flush=True)
        process = await asyncio.create_subprocess_exec(
            *arguments,
            cwd=source,
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        output, _ = await process.communicate()
        text = output.decode()
        await asyncio.to_thread((tmp_path / (label + ".log")).write_text, text)
        assert process.returncode == 0, text
        return text

    for path in (source, fixture):
        await execute(
            "build-" + path.name,
            "uv",
            "build",
            str(path),
            "--no-sources",
            "--wheel",
            "--out-dir",
            str(distributions),
        )
    (provider,) = await asyncio.to_thread(
        lambda: tuple(distributions.glob("bazaar_contrib_lark-*.whl"))
    )
    (harness,) = await asyncio.to_thread(
        lambda: tuple(distributions.glob("bcn_lark_test_harness-*.whl"))
    )
    await execute(
        "install",
        "uv",
        "tool",
        "install",
        "--python",
        "3.15",
        "--prerelease",
        "allow",
        "--refresh",
        "--default-index",
        "https://pypi.org/simple",
        "bazaar-compute-node>=0.3.0rc19,<0.4",
        "--with",
        str(provider),
        "--with",
        str(harness),
    )
    python = (
        await execute(
            "python",
            "uv",
            "python",
            "find",
            "--no-project",
            str(tmp_path / "tools/bazaar-compute-node"),
        )
    ).strip()
    node = (python, "-m", "bazaar_compute_node", "--configer-options", str(config))
    await execute("bootstrap", *node, "--help")
    document = tomllib.loads(await asyncio.to_thread(config.read_text))
    for package in document["contribs"]:
        for entry in package["enabled"]:
            if entry["entry"] == "agent":
                entry["config"]["agents"] = [
                    {
                        "id": "lark-test",
                        "name": "Lark Test",
                        "mode": "dangerous_individual",
                        "channel": [
                            {
                                "kind": "lark",
                                "app_id": tested_id,
                                "app_secret_env": "LARK_TEST_APP_SECRET",
                            }
                        ],
                        "harness": [{"kind": "lark-test"}],
                    }
                ]
            elif entry["entry"] in {"sidecar-unix", "sidecar-windows"}:
                entry["config"] = {"endpoint": f"/tmp/bcn-lark-{uuid4()}.sock"}
    document["contribs"] = [
        package
        for package in document["contribs"]
        if package["package"] not in {"bazaar-contrib-lark", "bcn-lark-test-harness"}
    ] + [
        {
            "package": "bazaar-contrib-lark @ " + provider.as_uri(),
            "enabled": [{"entry": "lark-builder"}],
        },
        {
            "package": "bcn-lark-test-harness @ " + harness.as_uri(),
            "enabled": [
                {"entry": "test-harness-builder"},
                {"entry": "audit-journal", "config": {"path": str(events)}},
            ],
        },
    ]
    await asyncio.to_thread(config.write_text, tomli_w.dumps(document))
    await execute("activation", *node, "--help")

    async with aiohttp.ClientSession() as session:
        driver = App(session, driver_id, driver_secret)
        running: list[asyncio.subprocess.Process] = []

        async def start(configer: str) -> None:
            output = tmp_path / f"node-{configer}.log"
            log = await asyncio.to_thread(output.open, "wb")
            process = await asyncio.create_subprocess_exec(
                python,
                str(source / "tests/run_node.py"),
                configer,
                str(config),
                cwd=source,
                env=environment,
                stdin=asyncio.subprocess.PIPE,
                stdout=log,
                stderr=asyncio.subprocess.STDOUT,
            )
            await asyncio.to_thread(log.close)
            running.append(process)
            connected = len(await connections())
            async with asyncio.timeout(WAIT_SECONDS):
                while len(await connections()) == connected:
                    assert process.returncode is None, "the node exited before ready"
                    await asyncio.sleep(0.5)

        async def connections() -> list[dict[str, Any]]:
            return [
                event
                for event in await journal(events)
                if event["event_name"] == "channel.connection.connected"
            ]

        async def stop() -> None:
            while running:
                process = running.pop()
                assert process.stdin is not None
                process.stdin.close()
                await process.wait()

        async def persisted(
            match: Callable[[LarkThreadIdentity], bool],
        ) -> dict[str, Any]:
            async with asyncio.timeout(WAIT_SECONDS):
                while True:
                    for event in await journal(events):
                        if event["event_name"] != "channel.inbound.persisted":
                            continue
                        thread = event["payload"]["correlation"].get(
                            "provider_thread_id"
                        )
                        if thread is not None and match(
                            parse_provider_thread_id(thread)
                        ):
                            return event
                    await asyncio.sleep(0.5)

        def answers(text: str) -> Callable[[dict[str, Any]], bool]:
            return lambda item: (
                item["sender"].get("id") == tested_id
                and text in item["body"]["content"]
            )

        def sent_after(
            asked: dict[str, Any], message_type: str
        ) -> Callable[[dict[str, Any]], bool]:
            return lambda item: (
                item["sender"].get("id") == tested_id
                and item["msg_type"] == message_type
                and int(item["create_time"]) >= int(asked["create_time"])
            )

        async def posted(
            match: Callable[[dict[str, Any]], bool], thread: str | None = None
        ) -> dict[str, Any]:
            async with asyncio.timeout(WAIT_SECONDS):
                while True:
                    for item in await driver.messages(
                        "chat" if thread is None else "thread", thread or chat
                    ):
                        if match(item):
                            return item
                    await asyncio.sleep(2)

        try:
            await start("config-filesystem")
            first_connected_at = asyncio.get_running_loop().time()
            connected = (await connections())[-1]
            assert connected["context"]["connection"] == "lark:0"
            assert connected["payload"]["identity"]["id"]

            marker = "e2e-" + secrets.token_hex(6)
            await driver.send(chat, "text", {"text": f"{marker} open"})
            opened = await persisted(
                lambda thread: thread.chat_id == chat and thread.thread_id == "0"
            )
            assert opened["payload"]["metadata"]["review"] == "pending"
            await execute(
                "review",
                *node,
                "agent",
                "review",
                "approve",
                opened["context"]["thread_id"],
            )

            marker = "e2e-" + secrets.token_hex(6)
            asked = await driver.send(chat, "text", {"text": f"{marker} reply"})
            answer = await posted(answers(f"{marker} reply"))
            assert answer.get("parent_id") == asked["message_id"]

            marker = "e2e-" + secrets.token_hex(6)
            original = await driver.send(chat, "text", {"text": f"{marker} original"})
            await driver.reply(original["message_id"], f"{marker} quote")
            await posted(answers(f"{marker} original"))

            marker = "e2e-" + secrets.token_hex(6)
            await driver.send(chat, "text", {"text": f"{marker} long"})
            end = await posted(answers(f"END {marker}"))
            parts = [
                rendered(item)
                for item in await driver.messages("chat", chat)
                if answers(marker)(item)
            ]
            sections = [
                int(number)
                for part in parts
                for number in re.findall(rf"{marker}-(\d{{4}})", part)
            ]
            total = re.search(rf"END {marker} (\d+)", rendered(end))
            assert total is not None
            assert len(parts) > 1
            assert sections == list(range(1, int(total[1]) + 1))

            marker = "e2e-" + secrets.token_hex(6)
            expected = await asyncio.to_thread(
                (fixture / "src/bcn_lark_test_harness/attachment.txt").read_bytes
            )
            asked = await driver.send(chat, "text", {"text": f"{marker} attach"})
            attached = await posted(sent_after(asked, "file"))
            assert await driver.download(attached, "file") == expected

            marker = "e2e-" + secrets.token_hex(6)
            picture = image(secrets.token_bytes(64))
            asked = await driver.send(
                chat,
                "post",
                {
                    "zh_cn": {
                        "title": "",
                        "content": [
                            [{"tag": "text", "text": f"{marker} echo"}],
                            [
                                {
                                    "tag": "img",
                                    "image_key": await driver.upload_image(picture),
                                }
                            ],
                        ],
                    }
                },
            )
            echoed = await posted(sent_after(asked, "image"))
            assert await driver.download(echoed, "image") == picture

            marker = "e2e-" + secrets.token_hex(6)
            root = await driver.send(chat, "text", {"text": f"{marker} root"})
            asked = await driver.reply(
                root["message_id"], f"{marker} reply", in_thread=True
            )
            topic = await persisted(
                lambda thread: (
                    thread.chat_id == chat and thread.thread_id == asked["thread_id"]
                )
            )
            assert topic["payload"]["metadata"]["review"] == "approved"
            await posted(answers(f"{marker} reply"), asked["thread_id"])

            # Drop only this isolated node's real TLS sockets. Observe recovery
            # through the test plugin's Audit journal, then exchange a message.
            await asyncio.sleep(
                max(0, first_connected_at + 60 - asyncio.get_running_loop().time())
            )
            connected_count = len(await connections())
            assert connected_count == 1
            sockets = await execute("sockets", "ss", "-Hntp")
            endpoints = [
                line.split()[3:5]
                for line in sockets.splitlines()
                if f"pid={running[0].pid}," in line and line.split()[4].endswith(":443")
            ]
            assert endpoints
            for number, (local, peer) in enumerate(endpoints):
                await execute(
                    f"disconnect-{number}",
                    "sudo",
                    "-n",
                    "ss",
                    "-K",
                    "src",
                    local,
                    "dst",
                    peer,
                )
            async with asyncio.timeout(WAIT_SECONDS):
                while len(await connections()) == connected_count:
                    failures = [
                        event
                        for event in await journal(events)
                        if event["event_name"] == "channel.connection.failed"
                    ]
                    assert all(
                        event["payload"]["retry_in_ms"] is not None
                        for event in failures
                    ), failures
                    await asyncio.sleep(0.5)
            marker = "e2e-" + secrets.token_hex(6)
            asked = await driver.send(chat, "text", {"text": f"{marker} reply"})
            answer = await posted(answers(f"{marker} reply"))
            assert answer.get("parent_id") == asked["message_id"]

            await stop()
            await start("config-env-token")
            marker = "e2e-" + secrets.token_hex(6)
            asked = await driver.send(chat, "text", {"text": f"{marker} reply"})
            answer = await posted(answers(f"{marker} reply"))
            assert answer.get("parent_id") == asked["message_id"]
        finally:
            await stop()
            await asyncio.to_thread(sys.stdout.flush)
