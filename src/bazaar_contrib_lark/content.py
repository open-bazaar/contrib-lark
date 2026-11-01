"""Read the body, mentions and resources of a Lark message."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import partial
from typing import TypeIs

RESOURCE_TYPES = frozenset({"image", "file", "audio", "media", "sticker"})


@dataclass(frozen=True, slots=True)
class LarkMention:
    key: str
    open_id: str | None
    display_name: str


@dataclass(frozen=True, slots=True)
class LarkResource:
    """A resource a message carries, as the event names it."""

    file_key: str | None
    resource_type: str
    name: str
    media_type: str | None = None

    @property
    def placeholder(self) -> str:
        return f"[{self.resource_type}: {self.name}]"


@dataclass(frozen=True, slots=True)
class LarkContent:
    message_type: str
    body: str
    resources: tuple[LarkResource, ...] = ()
    content_error: bool = False


def project_content(
    message_type: str,
    content: object,
    *,
    mentions: Mapping[str, LarkMention],
    bot_open_id: str,
) -> LarkContent:
    value, content_error = _decode_content(content)
    if message_type == "text":
        text = value.get("text") if isinstance(value, Mapping) else None
        if not isinstance(text, str):
            return LarkContent(
                message_type="text",
                body="[invalid lark content: text]",
                content_error=True,
            )
        return LarkContent(
            message_type="text",
            body=_replace_mentions(text, mentions, bot_open_id).strip(),
            content_error=content_error,
        )
    if message_type == "post":
        resources: list[LarkResource] = []
        body = _render_post(
            _select_post_locale(value),
            mentions=mentions,
            bot_open_id=bot_open_id,
            resources=resources,
        ).strip()
        return LarkContent(
            message_type="post",
            body=body or "[lark post]",
            resources=tuple(_unique_resources(resources)),
            content_error=content_error,
        )
    if message_type in RESOURCE_TYPES or message_type == "video":
        resource = _resource(
            value, "media" if message_type == "video" else message_type
        )
        return LarkContent(
            message_type=message_type,
            body=resource.placeholder,
            resources=(resource,),
            content_error=content_error,
        )
    return LarkContent(
        message_type=f"unsupported:{message_type}",
        body=f"[unsupported lark message type: {message_type}]",
        content_error=content_error,
    )


def parse_mentions(value: object) -> dict[str, LarkMention]:
    if not isinstance(value, list):
        return {}
    mentions: dict[str, LarkMention] = {}
    for item in value:
        if not isinstance(item, Mapping):
            continue
        key = provider_text(item.get("key"))
        if key is None:
            continue
        mention_id = item.get("id")
        open_id = (
            provider_text(mention_id.get("open_id"))
            if isinstance(mention_id, Mapping)
            else None
        )
        mentions[key] = LarkMention(
            key=key,
            open_id=open_id,
            display_name=provider_text(item.get("name")) or open_id or key,
        )
    return mentions


def provider_text(value: object) -> str | None:
    if not isinstance(value, str) or not value or "\r" in value or "\n" in value:
        return None
    return value


def provider_time_ms(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def safe_filename(value: object, *, fallback: str) -> str:
    """Keep a provider name only when it is one plain file name."""

    return value if is_safe_filename(value) else fallback


def is_safe_filename(value: object) -> TypeIs[str]:
    return (
        isinstance(value, str)
        and bool(value)
        and len(value) <= 255
        and "/" not in value
        and "\\" not in value
        and not any(ord(character) < 32 or ord(character) == 127 for character in value)
    )


def _decode_content(content: object) -> tuple[object, bool]:
    if isinstance(content, Mapping):
        return content, False
    if not isinstance(content, str):
        return {}, True
    try:
        decoded = json.loads(content)
    except ValueError:
        return {}, True
    return decoded, not isinstance(decoded, Mapping)


def _select_post_locale(content: object) -> object:
    if not isinstance(content, Mapping):
        return content
    if "title" in content or "content" in content:
        return content
    for locale in ("zh_cn", "en_us", "ja_jp", "ko_kr"):
        value = content.get(locale)
        if isinstance(value, (Mapping, list, tuple)):
            return value
    for value in content.values():
        if isinstance(value, (Mapping, list, tuple)):
            return value
    return content


def _render_post(
    value: object,
    *,
    mentions: Mapping[str, LarkMention],
    bot_open_id: str,
    resources: list[LarkResource],
) -> str:
    render = partial(
        _render_post, mentions=mentions, bot_open_id=bot_open_id, resources=resources
    )
    if isinstance(value, str):
        return _replace_mentions(value, mentions, bot_open_id)
    if isinstance(value, (list, tuple)):
        return "".join(render(child) for child in value)
    if not isinstance(value, Mapping):
        return ""
    tag = value.get("tag")
    if isinstance(tag, str):
        return _render_tag(
            value,
            tag,
            render,
            mentions=mentions,
            bot_open_id=bot_open_id,
            resources=resources,
        )
    parts: list[str] = []
    title = value.get("title")
    if title is not None:
        parts.append(render(title))
    content = value.get("content")
    if isinstance(content, (list, tuple)):
        # A post's content is its rows; each row is a line of inline nodes.
        parts.append("\n".join(render(row) for row in content))
    elif content is not None:
        parts.append(render(content))
    if parts:
        return "\n".join(parts)
    text = value.get("text")
    if isinstance(text, str):
        return _replace_mentions(text, mentions, bot_open_id)
    return ""


def _render_tag(
    value: Mapping[str, object],
    tag: str,
    render: Callable[[object], str],
    *,
    mentions: Mapping[str, LarkMention],
    bot_open_id: str,
    resources: list[LarkResource],
) -> str:
    """Render one tagged post node, by what the tag says it is."""

    if tag in {"text", "code", "pre", "del", "bold", "underline", "italic"}:
        text = value.get("text")
        return _replace_mentions(
            text if isinstance(text, str) else "", mentions, bot_open_id
        )
    if tag in {"a", "link"}:
        label = render(value.get("text") or value.get("content") or "")
        href = value.get("href") or value.get("url")
        if isinstance(href, str) and href:
            return f"{label} ({href})" if label else href
        return label
    if tag == "at":
        return _render_mention_node(value, mentions, bot_open_id)
    resource_type = _resource_type_for_tag(tag)
    if resource_type is not None:
        resource = _resource(value, resource_type)
        resources.append(resource)
        return resource.placeholder
    children = value.get("content")
    if children is None:
        children = value.get("children")
    if children is None:
        children = value.get("text")
    rendered = render(children)
    if tag in {"title", "paragraph", "p", "li", "list", "blockquote", "quote"}:
        return f"{rendered}\n"
    return rendered


def _render_mention_node(
    node: Mapping[str, object],
    mentions: Mapping[str, LarkMention],
    bot_open_id: str,
) -> str:
    key = node.get("key")
    mention = mentions.get(key) if isinstance(key, str) else None
    if mention is None:
        open_id = node.get("user_id") or node.get("open_id")
        if isinstance(open_id, str):
            mention = next(
                (item for item in mentions.values() if item.open_id == open_id), None
            )
            if mention is None:
                return "" if open_id == bot_open_id else f"@{open_id}"
    if mention is not None:
        return "" if mention.open_id == bot_open_id else f"@{mention.display_name}"
    text = node.get("text")
    return text if isinstance(text, str) else "@user"


def _resource(value: object, resource_type: str) -> LarkResource:
    mapping = value if isinstance(value, Mapping) else {}
    key = next(
        (
            candidate
            for field_name in (
                "file_key",
                "image_key",
                "audio_key",
                "media_key",
                "sticker_key",
                "resource_key",
                "key",
            )
            if isinstance(candidate := mapping.get(field_name), str) and candidate
        ),
        None,
    )
    raw_name = next(
        (
            candidate
            for field_name in ("file_name", "name", "filename", "alt", "text")
            if isinstance(candidate := mapping.get(field_name), str) and candidate
        ),
        None,
    )
    media_type = mapping.get("mime_type")
    return LarkResource(
        file_key=key,
        resource_type=resource_type,
        name=safe_filename(raw_name, fallback=resource_type),
        media_type=provider_text(media_type),
    )


def _resource_type_for_tag(tag: str) -> str | None:
    if tag in {"img", "image"}:
        return "image"
    if tag == "video":
        return "media"
    return tag if tag in RESOURCE_TYPES else None


def _unique_resources(resources: list[LarkResource]) -> list[LarkResource]:
    unique: list[LarkResource] = []
    seen: set[tuple[str | None, str]] = set()
    for resource in resources:
        key = (resource.file_key, resource.resource_type)
        if key not in seen:
            seen.add(key)
            unique.append(resource)
    return unique


def _replace_mentions(
    value: str, mentions: Mapping[str, LarkMention], bot_open_id: str
) -> str:
    rendered = value
    for key, mention in sorted(mentions.items(), key=lambda item: -len(item[0])):
        rendered = rendered.replace(
            key, "" if mention.open_id == bot_open_id else f"@{mention.display_name}"
        )
    return rendered


__all__ = [
    "LarkContent",
    "LarkMention",
    "LarkResource",
    "is_safe_filename",
    "parse_mentions",
    "project_content",
    "provider_text",
    "provider_time_ms",
    "safe_filename",
]
