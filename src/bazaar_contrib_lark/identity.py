"""Name the Lark bot, its conversations and their messages."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import quote, unquote_to_bytes
from uuid import NAMESPACE_URL, uuid5

from bazaar_contrib_channel import ChannelIdentity


@dataclass(frozen=True, slots=True)
class LarkBotIdentity:
    open_id: str
    app_name: str | None = None

    def __post_init__(self) -> None:
        _validate_identity_text(self.open_id, "bot open_id")
        if self.app_name is not None:
            _validate_identity_text(self.app_name, "bot app_name")

    def as_channel_identity(self) -> ChannelIdentity:
        return ChannelIdentity(id=self.open_id, name=self.app_name)


@dataclass(frozen=True, slots=True)
class LarkThreadIdentity:
    bot_open_id: str
    chat_id: str
    thread_id: str = "0"

    def __post_init__(self) -> None:
        _validate_identity_text(self.bot_open_id, "Lark bot open_id")
        _validate_identity_text(self.chat_id, "Lark chat_id")
        _validate_identity_text(self.thread_id, "Lark thread_id")

    @property
    def provider_thread_id(self) -> str:
        return (
            f"lark:bot:{_encode_segment(self.bot_open_id)}:"
            f"chat:{_encode_segment(self.chat_id)}:"
            f"thread:{_encode_segment(self.thread_id)}"
        )

    @property
    def channel_session_id(self) -> str:
        return str(uuid5(NAMESPACE_URL, self.provider_thread_id))

    @property
    def session_id(self) -> str:
        return str(uuid5(NAMESPACE_URL, f"bcn:{self.provider_thread_id}"))

    @property
    def main(self) -> LarkThreadIdentity:
        """The conversation of the chat itself, outside any topic."""

        return LarkThreadIdentity(bot_open_id=self.bot_open_id, chat_id=self.chat_id)

    def message_id(self, provider_message_id: str) -> str:
        _validate_identity_text(provider_message_id, "Lark message_id")
        return str(
            uuid5(
                NAMESPACE_URL,
                "lark:bot:"
                f"{_encode_segment(self.bot_open_id)}:message:"
                f"{_encode_segment(provider_message_id)}",
            )
        )


def parse_provider_thread_id(
    value: str,
    *,
    bot_open_id: str | None = None,
) -> LarkThreadIdentity:
    _validate_identity_text(value, "Lark provider_thread_id")
    parts = value.split(":")
    if len(parts) != 7 or parts[0] != "lark" or parts[1] != "bot":
        raise ValueError("Lark provider_thread_id has invalid format")
    if parts[3] != "chat" or parts[5] != "thread":
        raise ValueError("Lark provider_thread_id has invalid format")
    decoded = tuple(_decode_segment(part) for part in (parts[2], parts[4], parts[6]))
    if bot_open_id is not None and decoded[0] != bot_open_id:
        raise ValueError("Lark provider_thread_id belongs to another bot")
    return LarkThreadIdentity(
        bot_open_id=decoded[0],
        chat_id=decoded[1],
        thread_id=decoded[2],
    )


def parse_bot_info(payload: object) -> LarkBotIdentity:
    if not isinstance(payload, Mapping):
        raise TypeError("Lark bot info must be an object")
    bot = payload.get("bot")
    if not isinstance(bot, Mapping):
        data = payload.get("data")
        bot = data.get("bot") if isinstance(data, Mapping) else None
    if not isinstance(bot, Mapping):
        if "open_id" in payload:
            bot = payload
        else:
            raise ValueError("Lark bot info is missing bot data")
    open_id = bot.get("open_id")
    if not isinstance(open_id, str) or not open_id:
        raise ValueError("Lark bot info is missing open_id")
    app_name = bot.get("app_name")
    if app_name is None:
        app_name = bot.get("name")
    if app_name is not None and (not isinstance(app_name, str) or not app_name):
        raise ValueError("Lark bot info contains an invalid display name")
    return LarkBotIdentity(open_id=open_id, app_name=app_name)


def _validate_identity_text(value: object, field_name: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be non-empty text")
    if "\r" in value or "\n" in value:
        raise ValueError(f"{field_name} must not contain line breaks")


def _encode_segment(value: str) -> str:
    return quote(value, safe="")


def _decode_segment(value: str) -> str:
    try:
        decoded = unquote_to_bytes(value).decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise ValueError("Lark provider_thread_id contains invalid UTF-8") from error
    if not decoded or _encode_segment(decoded) != value:
        raise ValueError("Lark provider_thread_id contains a non-canonical segment")
    return decoded


__all__ = [
    "LarkBotIdentity",
    "LarkThreadIdentity",
    "parse_bot_info",
    "parse_provider_thread_id",
]
