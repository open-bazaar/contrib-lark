"""Call the Lark OpenAPI with the app's tenant token."""

# Existing Lark request budgets are retained.
# ruff: noqa: ASYNC109
from __future__ import annotations

import asyncio
import json
import math
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import BinaryIO
from urllib.parse import parse_qs, quote, urlparse

import aiohttp
from bazaar_contrib_timer import Timer, timeout

_MAX_ERROR_MESSAGE = 256
# A resource streams as long as it keeps arriving; 0.2.3 gave it 60 seconds.
_RESOURCE_TIMEOUT = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=60)
_TOKEN_REFRESH_MARGIN_MS = 600_000
_TOKEN_TIMEOUT_SECONDS = 60.0


@dataclass(frozen=True, slots=True)
class LarkSentMessage:
    """What the provider says about a message it just accepted."""

    message_id: str
    chat_id: str | None


class LarkApiError(RuntimeError):
    def __init__(
        self,
        method: str,
        *,
        http_status: int,
        provider_code: int | None,
        message: str,
    ) -> None:
        self.method = method
        self.http_status = http_status
        self.provider_code = provider_code
        self.message = message[:_MAX_ERROR_MESSAGE]
        code = provider_code if provider_code is not None else http_status
        super().__init__(f"Lark {method} failed ({code}): {self.message}")


class LarkTransportError(RuntimeError):
    def __init__(self, method: str, error_kind: str) -> None:
        self.method = method
        self.error_kind = error_kind
        super().__init__(f"Lark {method} transport failed: {error_kind}")


@dataclass(frozen=True, slots=True)
class ClientConfig:
    ping_interval: int = 120
    reconnect_count: int = -1
    reconnect_interval: int = 120
    reconnect_nonce: int = 30

    @classmethod
    def from_payload(cls, payload: object) -> ClientConfig:
        """Take the connection settings Lark sends as they are sent."""

        defaults = cls()
        if not isinstance(payload, Mapping):
            return defaults
        return cls(
            ping_interval=payload.get("PingInterval", defaults.ping_interval),
            reconnect_count=payload.get("ReconnectCount", defaults.reconnect_count),
            reconnect_interval=payload.get(
                "ReconnectInterval", defaults.reconnect_interval
            ),
            reconnect_nonce=payload.get("ReconnectNonce", defaults.reconnect_nonce),
        )


@dataclass(frozen=True, slots=True)
class LarkEndpoint:
    url: str
    service_id: int
    device_id: str
    client_config: ClientConfig


class LarkApi:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        timer: Timer,
        *,
        app_id: str,
        app_secret: str,
        base_url: str,
    ) -> None:
        self._session = session
        self._timer = timer
        self._app_id = app_id
        self._app_secret = app_secret
        self._base_url = base_url.rstrip("/")
        self._token: str | None = None
        self._refresh_at_ms = 0
        self._refreshing: asyncio.Task[str] | None = None

    @property
    def session(self) -> aiohttp.ClientSession:
        return self._session

    async def tenant_access_token(self) -> str:
        """Return the current token, refreshing it ahead of its expiry once."""

        if self._token is not None and self._timer.now_ms < self._refresh_at_ms:
            return self._token
        refreshing = self._refreshing
        if refreshing is None:
            refreshing = asyncio.create_task(
                self._refresh_token(), name="bcn-lark-token-refresh"
            )
            self._refreshing = refreshing
            refreshing.add_done_callback(self._refreshed)
        return await asyncio.shield(refreshing)

    def _refreshed(self, task: asyncio.Task[str]) -> None:
        if self._refreshing is task:
            self._refreshing = None
        if not task.cancelled():
            task.exception()

    async def _refresh_token(self) -> str:
        issued_ms = self._timer.now_ms
        body = await self._request_json(
            "tenant_access_token",
            "POST",
            "/open-apis/auth/v3/tenant_access_token/internal",
            timeout_seconds=_TOKEN_TIMEOUT_SECONDS,
            json_body={"app_id": self._app_id, "app_secret": self._app_secret},
        )
        self._check_provider_result(body, "tenant_access_token")
        token = body.get("tenant_access_token")
        expire = body.get("expire")
        if not isinstance(token, str) or not token.strip():
            raise LarkApiError(
                "tenant_access_token",
                http_status=200,
                provider_code=0,
                message="provider response is missing tenant_access_token",
            )
        if not isinstance(expire, int) or isinstance(expire, bool) or expire <= 0:
            raise LarkApiError(
                "tenant_access_token",
                http_status=200,
                provider_code=0,
                message="provider response contains an invalid expiry",
            )
        margin_ms = min(_TOKEN_REFRESH_MARGIN_MS, max(1_000, expire * 500))
        self._token = token
        self._refresh_at_ms = issued_ms + expire * 1_000 - margin_ms
        return token

    async def get_endpoint(self, *, timeout: float) -> LarkEndpoint:
        body = await self._request_json(
            "ws_endpoint",
            "POST",
            "/callback/ws/endpoint",
            timeout_seconds=timeout,
            json_body={"AppID": self._app_id, "AppSecret": self._app_secret},
        )
        self._check_provider_result(body, "ws_endpoint")
        data = body.get("data")
        if not isinstance(data, Mapping):
            raise LarkApiError(
                "ws_endpoint",
                http_status=200,
                provider_code=0,
                message="provider response is missing endpoint data",
            )
        url = data.get("URL")
        if not isinstance(url, str) or not url:
            raise LarkApiError(
                "ws_endpoint",
                http_status=200,
                provider_code=0,
                message="provider response is missing endpoint URL",
            )
        parsed = urlparse(url)
        if parsed.scheme != "wss" or not parsed.netloc:
            raise LarkApiError(
                "ws_endpoint",
                http_status=200,
                provider_code=0,
                message="provider endpoint URL is invalid",
            )
        query = parse_qs(parsed.query, keep_blank_values=True)
        return LarkEndpoint(
            url=url,
            service_id=int(_query_text(query.get("service_id"), "service_id")),
            device_id=_query_text(query.get("device_id"), "device_id"),
            client_config=ClientConfig.from_payload(data.get("ClientConfig")),
        )

    async def get_bot_info(self, *, timeout: float) -> Mapping[str, object]:
        return await self._call("bot_info", "GET", "/open-apis/bot/v3/info", timeout)

    async def get_user(self, open_id: str, *, timeout: float) -> Mapping[str, object]:
        return await self._call(
            "contact_user",
            "GET",
            f"/open-apis/contact/v3/users/{quote(open_id, safe='')}",
            timeout,
            params={"user_id_type": "open_id"},
        )

    async def get_chat(self, chat_id: str, *, timeout: float) -> Mapping[str, object]:
        return await self._call(
            "chat_get",
            "GET",
            f"/open-apis/im/v1/chats/{quote(chat_id, safe='')}",
            timeout,
        )

    async def get_message(
        self, message_id: str, *, timeout: float
    ) -> Mapping[str, object]:
        return await self._call(
            "message_get",
            "GET",
            f"/open-apis/im/v1/messages/{quote(message_id, safe='')}",
            timeout,
            params={"user_id_type": "open_id"},
        )

    async def send_message(
        self,
        *,
        receive_id: str,
        receive_id_type: str,
        message_type: str,
        content: str,
        uuid: str,
        timeout: float,
    ) -> LarkSentMessage:
        body = await self._call(
            "message_create",
            "POST",
            "/open-apis/im/v1/messages",
            timeout,
            params={"receive_id_type": receive_id_type},
            json_body={
                "receive_id": receive_id,
                "msg_type": message_type,
                "content": content,
                "uuid": uuid,
            },
        )
        return LarkSentMessage(
            message_id=_response_id(body, "message_create", "message_id"),
            chat_id=_optional_response_text(body, "chat_id"),
        )

    async def reply_message(
        self,
        *,
        message_id: str,
        message_type: str,
        content: str,
        reply_in_thread: bool,
        uuid: str,
        timeout: float,
    ) -> LarkSentMessage:
        body = await self._call(
            "message_reply",
            "POST",
            f"/open-apis/im/v1/messages/{quote(message_id, safe='')}/reply",
            timeout,
            json_body={
                "msg_type": message_type,
                "content": content,
                "reply_in_thread": reply_in_thread,
                "uuid": uuid,
            },
        )
        return LarkSentMessage(
            message_id=_response_id(body, "message_reply", "message_id"),
            chat_id=_optional_response_text(body, "chat_id"),
        )

    async def create_reaction(
        self, message_id: str, *, emoji_type: str, timeout: float
    ) -> str:
        body = await self._call(
            "message_reaction_create",
            "POST",
            f"/open-apis/im/v1/messages/{quote(message_id, safe='')}/reactions",
            timeout,
            json_body={"reaction_type": {"emoji_type": emoji_type}},
        )
        return _response_id(body, "message_reaction_create", "reaction_id")

    async def update_card(
        self, token: str, *, card: Mapping[str, object], timeout: float
    ) -> None:
        """Redraw a card with the token its button callback carried."""

        await self._call(
            "card_update",
            "POST",
            "/open-apis/interactive/v1/card/update",
            timeout,
            json_body={"token": token, "card": dict(card)},
        )

    async def patch_card(
        self, message_id: str, *, card: Mapping[str, object], timeout: float
    ) -> None:
        """Redraw a shared card the app sent, by its message ID."""

        await self._call(
            "message_patch",
            "PATCH",
            f"/open-apis/im/v1/messages/{quote(message_id, safe='')}",
            timeout,
            json_body={
                "content": json.dumps(card, ensure_ascii=False, separators=(",", ":"))
            },
        )

    async def upload_image(
        self, file: BinaryIO, *, filename: str, media_type: str, timeout: float
    ) -> str:
        form = aiohttp.FormData(quote_fields=False)
        form.add_field("image_type", "message")
        form.add_field("image", file, filename=filename, content_type=media_type)
        body = await self._call(
            "image_upload", "POST", "/open-apis/im/v1/images", timeout, form=form
        )
        return _response_id(body, "image_upload", "image_key")

    async def upload_file(
        self,
        file: BinaryIO,
        *,
        file_type: str,
        filename: str,
        media_type: str,
        timeout: float,
    ) -> str:
        form = aiohttp.FormData(quote_fields=False)
        form.add_field("file_type", file_type)
        form.add_field("file_name", filename)
        form.add_field("file", file, filename=filename, content_type=media_type)
        body = await self._call(
            "file_upload", "POST", "/open-apis/im/v1/files", timeout, form=form
        )
        return _response_id(body, "file_upload", "file_key")

    @asynccontextmanager
    async def open_message_resource(
        self, message_id: str, file_key: str, resource_type: str
    ) -> AsyncIterator[aiohttp.ClientResponse]:
        """Open a message resource; the caller reads as much of the body as it needs."""

        token = await self.tenant_access_token()
        url = (
            f"{self._base_url}/open-apis/im/v1/messages/"
            f"{quote(message_id, safe='')}/resources/{quote(file_key, safe='')}"
        )
        try:
            async with self._session.get(
                url,
                params={"type": resource_type},
                headers={"Authorization": f"Bearer {token}"},
                timeout=_RESOURCE_TIMEOUT,
            ) as response:
                if response.status < 200 or response.status >= 300:
                    raise LarkApiError(
                        "message_resource",
                        http_status=response.status,
                        provider_code=None,
                        message="provider rejected message resource",
                    )
                yield response
        except TimeoutError:
            raise LarkTransportError("message_resource", "TimeoutError") from None
        except (aiohttp.ClientError, OSError) as error:
            raise LarkTransportError("message_resource", type(error).__name__) from None

    async def _call(
        self,
        method: str,
        http_method: str,
        path: str,
        timeout_seconds: float,
        *,
        params: Mapping[str, str] | None = None,
        json_body: Mapping[str, object] | None = None,
        form: aiohttp.FormData | None = None,
    ) -> Mapping[str, object]:
        try:
            async with timeout(self._timer, math.ceil(timeout_seconds * 1000)):
                token = await self.tenant_access_token()
                body = await self._request_json(
                    method,
                    http_method,
                    path,
                    timeout_seconds=None,
                    params=params,
                    json_body=json_body,
                    form=form,
                    headers={"Authorization": f"Bearer {token}"},
                )
        except TimeoutError:
            raise LarkTransportError(method, "TimeoutError") from None
        self._check_provider_result(body, method)
        return body

    async def _request_json(
        self,
        method: str,
        http_method: str,
        path: str,
        *,
        timeout_seconds: float | None,
        params: Mapping[str, str] | None = None,
        json_body: Mapping[str, object] | None = None,
        form: aiohttp.FormData | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Mapping[str, object]:
        """Read what Lark answered, turning any failure into one of our two errors."""

        try:
            async with (
                timeout(
                    self._timer,
                    None
                    if timeout_seconds is None
                    else math.ceil(timeout_seconds * 1000),
                ),
                self._session.request(
                    http_method,
                    f"{self._base_url}{path}",
                    params=params,
                    json=json_body,
                    data=form,
                    headers=headers,
                ) as response,
            ):
                status = response.status
                try:
                    body = await response.json(content_type=None)
                except ValueError:
                    raise LarkApiError(
                        method,
                        http_status=status,
                        provider_code=None,
                        message="provider returned invalid JSON",
                    ) from None
        except TimeoutError:
            raise LarkTransportError(method, "TimeoutError") from None
        except (aiohttp.ClientError, OSError) as error:
            raise LarkTransportError(method, type(error).__name__) from None
        if not isinstance(body, dict):
            raise LarkApiError(
                method,
                http_status=status,
                provider_code=None,
                message="provider response is not an object",
            )
        if status < 200 or status >= 300:
            raise LarkApiError(
                method,
                http_status=status,
                provider_code=_provider_code(body.get("code")),
                message=self._safe_provider_message(body.get("msg")),
            )
        return body

    def _check_provider_result(self, body: Mapping[str, object], method: str) -> None:
        code = _provider_code(body.get("code"))
        if code != 0:
            raise LarkApiError(
                method,
                http_status=200,
                provider_code=code,
                message=self._safe_provider_message(body.get("msg")),
            )

    def _safe_provider_message(self, value: object) -> str:
        message = (
            value if isinstance(value, str) and value else "provider rejected request"
        )
        for secret in (self._app_secret, self._token):
            if secret:
                message = message.replace(secret, "<redacted>")
        return message[:_MAX_ERROR_MESSAGE]


def _query_text(values: list[str] | None, field_name: str) -> str:
    if values is None or len(values) != 1 or not values[0]:
        raise ValueError(f"Lark endpoint is missing {field_name}")
    return values[0]


def _provider_code(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _optional_response_text(body: Mapping[str, object], field_name: str) -> str | None:
    data = body.get("data")
    value = data.get(field_name) if isinstance(data, Mapping) else None
    return value if isinstance(value, str) and value else None


def _response_id(body: Mapping[str, object], method: str, field_name: str) -> str:
    value = _optional_response_text(body, field_name)
    if value is None:
        raise LarkApiError(
            method,
            http_status=200,
            provider_code=0,
            message=f"provider response is missing {field_name}",
        )
    return value


__all__ = [
    "ClientConfig",
    "LarkApi",
    "LarkApiError",
    "LarkEndpoint",
    "LarkSentMessage",
    "LarkTransportError",
]
