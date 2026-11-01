"""Hold the Lark long connection: frames, heartbeats, acknowledgements, reconnection."""

# Existing Lark lifecycle budgets are retained.
# ruff: noqa: ASYNC109
from __future__ import annotations

import asyncio
import importlib
import json
import math
import random
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from dataclasses import dataclass, field

import aiohttp
from bazaar_contrib_audit import AuditError
from bazaar_contrib_timer import Timer
from pydantic import JsonValue

from .api import ClientConfig, LarkApi, LarkApiError, LarkEndpoint, LarkTransportError

# protobug reads the declaring module's source when it declares a frame, so the
# frames are declared on a worker thread when the transport starts.
lazy from .frame import (
    Frame,
    FrameDecodeError,
    Header,
    decode_frame,
    encode_frame,
    header_values,
)

CONTROL_METHOD = 0
DATA_METHOD = 1
HEADER_TYPE = "type"
HEADER_MESSAGE_ID = "message_id"
HEADER_SUM = "sum"
HEADER_SEQ = "seq"
HEADER_BIZ_RT = "biz_rt"
MESSAGE_EVENT = "event"
MESSAGE_CARD = "card"
MESSAGE_PING = "ping"
MESSAGE_PONG = "pong"

_FRAGMENT_EXPIRY_MS = 5_000
_HEARTBEAT_SECONDS = 30.0
_RECONNECT_ENDPOINT_TIMEOUT_SECONDS = 30.0
_STARTUP_RETRY_MS = 250
_CARD_EVENT_TYPES = frozenset({"card.action.trigger", "p2.card.action.trigger"})


@dataclass(frozen=True, slots=True)
class CardAnswer:
    """What a card callback answers, and what runs once the answer is sent."""

    payload: bytes
    after: Callable[[], Coroutine[object, object, None]] | None = None


@dataclass(slots=True)
class _Fragments:
    total: int
    updated_ms: int
    parts: dict[int, bytes] = field(default_factory=dict)


Publish = Callable[[str, JsonValue, AuditError | None], Awaitable[None]]


class LarkTransport:
    def __init__(
        self,
        api: LarkApi,
        timer: Timer,
        group: asyncio.TaskGroup,
        *,
        on_event: Callable[[Mapping[str, object]], None],
        on_card: Callable[[Mapping[str, object]], Awaitable[CardAnswer]],
        publish: Publish,
    ) -> None:
        self._api = api
        self._timer = timer
        self._group = group
        self._on_event = on_event
        self._on_card = on_card
        self._publish = publish
        self._connection: aiohttp.ClientWebSocketResponse | None = None
        self._endpoint: LarkEndpoint | None = None
        self._stopping = asyncio.Event()
        self._connected = asyncio.Event()
        self._startup_error: Exception | None = None
        self._runner: asyncio.Task[None] | None = None
        self._fragments: dict[str, _Fragments] = {}

    @property
    def connected(self) -> bool:
        return self._connection is not None and not self._connection.closed

    async def start(self, *, timeout: float) -> None:
        """Connect once within the startup budget; later drops reconnect on their own."""

        deadline_ms = self._timer.now_ms + math.ceil(max(0.0, timeout) * 1000)
        await asyncio.to_thread(importlib.import_module, f"{__package__}.frame")
        self._stopping.clear()
        self._connected.clear()
        self._startup_error = None
        self._runner = self._group.create_task(
            self._run(deadline_ms), name="bcn-lark-transport"
        )
        await self._connected.wait()
        if self._startup_error is not None:
            await self._abort()
            raise self._startup_error

    async def stop(self) -> None:
        self._stopping.set()
        connection = self._connection
        if connection is not None:
            await connection.close()
        await self._abort()

    async def _abort(self) -> None:
        self._stopping.set()
        runner = self._runner
        self._runner = None
        if runner is not None:
            runner.cancel()
            await asyncio.gather(runner, return_exceptions=True)
        self._connection = None
        self._fragments.clear()

    async def _run(self, startup_deadline_ms: int) -> None:
        attempts = 0
        while not self._stopping.is_set():
            first = not self._connected.is_set()
            try:
                endpoint = await self._api.get_endpoint(
                    timeout=(
                        max(0, startup_deadline_ms - self._timer.now_ms) / 1000
                        if first
                        else _RECONNECT_ENDPOINT_TIMEOUT_SECONDS
                    )
                )
                self._endpoint = endpoint
                await self._serve(endpoint)
                attempts = 0
                if self._stopping.is_set():
                    return
                raise LarkTransportError("websocket", "closed")
            except (LarkApiError, LarkTransportError, FrameDecodeError, ValueError,
                    aiohttp.ClientError, OSError, TimeoutError) as error:  # fmt: skip
                self._connection = None
                if self._stopping.is_set():
                    return
                await self._publish(
                    "channel.connection.failed",
                    {"first_connection": first, "attempt": attempts + 1},
                    AuditError(kind=_error_kind(error), message=str(error)),
                )
                if first:
                    if (
                        isinstance(error, (LarkApiError, ValueError))
                        or self._timer.now_ms + _STARTUP_RETRY_MS >= startup_deadline_ms
                    ):
                        self._startup_error = error
                        self._connected.set()
                        return
                    await self._timer.create(_STARTUP_RETRY_MS).wait()
                    continue
                attempts += 1
                config = self._config()
                if _is_authentication_error(error) or (
                    config.reconnect_count >= 0 and attempts > config.reconnect_count
                ):
                    return
                await self._timer.create(
                    _delay_ms(random.uniform(0.0, config.reconnect_nonce))
                ).wait()
                await self._timer.create(_delay_ms(config.reconnect_interval)).wait()

    async def _serve(self, endpoint: LarkEndpoint) -> None:
        connection = await self._api.session.ws_connect(
            endpoint.url,
            autoping=True,
            autoclose=True,
            heartbeat=_HEARTBEAT_SECONDS,
            max_msg_size=0,
        )
        self._connection = connection
        heartbeat = self._group.create_task(
            self._heartbeat(connection, endpoint), name="bcn-lark-heartbeat"
        )
        try:
            await self._publish(
                "channel.connection.connected",
                {"service_id": endpoint.service_id},
                None,
            )
            self._connected.set()
            async for message in connection:
                if self._stopping.is_set():
                    return
                if message.type in {aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED}:
                    return
                if message.type is aiohttp.WSMsgType.ERROR:
                    raise LarkTransportError("websocket", "receive_error")
                if message.type is aiohttp.WSMsgType.BINARY:
                    await self._handle_binary(connection, message.data)
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            await connection.close()

    async def _heartbeat(
        self, connection: aiohttp.ClientWebSocketResponse, endpoint: LarkEndpoint
    ) -> None:
        while not connection.closed:
            await self._timer.create(_delay_ms(self._config().ping_interval)).wait()
            try:
                await self._send(
                    connection,
                    Frame(
                        SeqID=0,
                        LogID=0,
                        service=endpoint.service_id,
                        method=CONTROL_METHOD,
                        headers=[Header(key=HEADER_TYPE, value=MESSAGE_PING)],
                    ),
                )
            except aiohttp.ClientError, ConnectionError:
                # The reader sees the same closed connection and reconnects.
                return

    async def _handle_binary(
        self, connection: aiohttp.ClientWebSocketResponse, raw: bytes
    ) -> None:
        frame = decode_frame(raw)
        kind = _required_header(frame.headers, HEADER_TYPE)
        if frame.method == CONTROL_METHOD:
            if kind == MESSAGE_PING:
                await self._send(
                    connection,
                    Frame(
                        SeqID=frame.SeqID,
                        LogID=frame.LogID,
                        service=frame.service,
                        method=CONTROL_METHOD,
                        headers=[Header(key=HEADER_TYPE, value=MESSAGE_PONG)],
                        payload=frame.payload,
                        LogIDNew=frame.LogIDNew,
                    ),
                )
            elif kind == MESSAGE_PONG and frame.payload:
                self._update_config(frame.payload)
            return
        if frame.method != DATA_METHOD:
            raise FrameDecodeError("frame method is unsupported")
        if kind not in {MESSAGE_EVENT, MESSAGE_CARD}:
            raise FrameDecodeError("data frame type is unsupported")
        started_ms = self._timer.now_ms
        payload = self._assemble(frame)
        if payload is None:
            return
        try:
            decoded = json.loads(payload.decode("utf-8"))
        except ValueError:
            decoded = None
        if not isinstance(decoded, Mapping):
            await self._ack(connection, frame, started_ms, _status(500))
            return
        if kind == MESSAGE_CARD or _event_type(decoded) in _CARD_EVENT_TYPES:
            self._group.create_task(
                self._answer_card(connection, frame, started_ms, decoded),
                name="bcn-lark-card",
            )
            return
        await self._ack(connection, frame, started_ms, _status(200))
        self._on_event(decoded)

    async def _answer_card(
        self,
        connection: aiohttp.ClientWebSocketResponse,
        frame: Frame,
        started_ms: int,
        decoded: Mapping[str, object],
    ) -> None:
        """Answer a card callback on its own task, so reading never waits for it."""

        answer = await self._on_card(decoded)
        try:
            await self._ack(connection, frame, started_ms, answer.payload)
        except aiohttp.ClientError, ConnectionError:
            # The reader sees the same closed connection and reconnects.
            return
        if answer.after is not None:
            await answer.after()

    def _assemble(self, frame: Frame) -> bytes | None:
        """Return the whole payload, or None while fragments are still missing."""

        total = _integer_header(frame.headers, HEADER_SUM)
        sequence = _integer_header(frame.headers, HEADER_SEQ)
        if total < 1 or sequence < 0 or sequence >= total:
            raise FrameDecodeError("fragment sequence is outside its message")
        payload = frame.payload or b""
        if total == 1:
            return payload
        now_ms = self._timer.now_ms
        for expired in [
            key
            for key, fragments in self._fragments.items()
            if now_ms - fragments.updated_ms > _FRAGMENT_EXPIRY_MS
        ]:
            del self._fragments[expired]
        message_id = _required_header(frame.headers, HEADER_MESSAGE_ID)
        fragments = self._fragments.setdefault(message_id, _Fragments(total, now_ms))
        if fragments.total != total:
            raise FrameDecodeError("fragment count changed for a message")
        fragments.parts[sequence] = payload
        fragments.updated_ms = now_ms
        if len(fragments.parts) != total:
            return None
        del self._fragments[message_id]
        return b"".join(fragments.parts[index] for index in range(total))

    async def _ack(
        self,
        connection: aiohttp.ClientWebSocketResponse,
        frame: Frame,
        started_ms: int,
        payload: bytes,
    ) -> None:
        headers = [
            Header(key=header.key, value=header.value) for header in frame.headers
        ]
        headers.append(
            Header(
                key=HEADER_BIZ_RT, value=str(max(0, self._timer.now_ms - started_ms))
            )
        )
        await self._send(
            connection,
            Frame(
                SeqID=frame.SeqID,
                LogID=frame.LogID,
                service=frame.service,
                method=frame.method,
                headers=headers,
                payload_encoding=frame.payload_encoding,
                payload_type=frame.payload_type,
                payload=payload,
                LogIDNew=frame.LogIDNew,
            ),
        )

    async def _send(
        self, connection: aiohttp.ClientWebSocketResponse, frame: Frame
    ) -> None:
        await connection.send_bytes(encode_frame(frame))

    def _update_config(self, payload: bytes) -> None:
        try:
            decoded = json.loads(payload.decode("utf-8"))
        except ValueError as error:
            raise FrameDecodeError(
                "pong contains invalid client configuration"
            ) from error
        endpoint = self._endpoint
        if endpoint is not None:
            self._endpoint = LarkEndpoint(
                url=endpoint.url,
                service_id=endpoint.service_id,
                device_id=endpoint.device_id,
                client_config=ClientConfig.from_payload(decoded),
            )

    def _config(self) -> ClientConfig:
        endpoint = self._endpoint
        return endpoint.client_config if endpoint is not None else ClientConfig()


def _event_type(payload: Mapping[str, object]) -> object:
    header = payload.get("header")
    return header.get("event_type") if isinstance(header, Mapping) else None


def _status(code: int) -> bytes:
    return json.dumps({"code": code}, separators=(",", ":")).encode("utf-8")


def _delay_ms(seconds: float) -> int:
    return math.ceil(max(0.0, seconds) * 1_000)


def _required_header(headers: list[Header], key: str) -> str:
    values = header_values(headers, key)
    if len(values) != 1 or not values[0]:
        raise FrameDecodeError(f"frame is missing a unique {key} header")
    return values[0]


def _integer_header(headers: list[Header], key: str) -> int:
    try:
        return int(_required_header(headers, key))
    except ValueError:
        raise FrameDecodeError(f"frame {key} header is not an integer") from None


def _is_authentication_error(error: Exception) -> bool:
    if not isinstance(error, LarkApiError):
        return False
    return error.http_status in {401, 403} or error.provider_code in {
        403,
        514,
        1000040344,
        1000040350,
    }


def _error_kind(error: BaseException) -> str:
    if isinstance(error, LarkApiError):
        return f"lark_{error.provider_code or error.http_status}"
    if isinstance(error, LarkTransportError):
        return error.error_kind
    if isinstance(error, FrameDecodeError):
        return "frame_decode"
    return type(error).__name__


__all__ = ["CardAnswer", "LarkTransport"]
