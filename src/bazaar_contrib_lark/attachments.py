"""Name and type a received resource, and stream it once the node asks."""

from __future__ import annotations

import asyncio
import mimetypes
from collections.abc import AsyncGenerator
from contextlib import aclosing
from email.message import Message

from bazaar_contrib_channel import InboundAttachment

from .api import LarkApi, LarkApiError, LarkTransportError
from .content import LarkResource, is_safe_filename

_DOWNLOAD_TYPES = {"image": "image", "file": "file", "audio": "file", "media": "file"}


async def inbound_attachment(
    api: LarkApi, provider_message_id: str, resource: LarkResource
) -> InboundAttachment:
    """Read the resource's name and type on receipt, leaving its body unread."""

    if resource.file_key is None:
        return InboundAttachment(
            name=resource.name,
            kind=resource.resource_type,
            state="failed",
            media_type=resource.media_type,
            error="missing_resource_key",
        )
    download_type = _DOWNLOAD_TYPES.get(resource.resource_type)
    if download_type is None:
        return InboundAttachment(
            name=resource.name,
            kind=resource.resource_type,
            state="failed",
            media_type=resource.media_type,
            error="resource_download_unsupported",
        )
    response_name: str | None = None
    media_type = resource.media_type
    try:
        async with api.open_message_resource(
            provider_message_id, resource.file_key, download_type
        ) as response:
            response_name = _disposition_filename(
                response.headers.get("Content-Disposition")
            )
            media_type = response.headers.get("Content-Type") or media_type
    except LarkApiError, LarkTransportError:
        # The download reports the same failure once the node asks for it.
        pass
    name = await asyncio.to_thread(
        resource_name,
        event_name=resource.name,
        response_name=response_name,
        media_type=media_type,
        resource_type=resource.resource_type,
    )
    return InboundAttachment(
        name=name,
        kind=resource.resource_type,
        media_type=media_type,
        source=aclosing(
            _stream(api, provider_message_id, resource.file_key, download_type)
        ),
    )


async def _stream(
    api: LarkApi, provider_message_id: str, file_key: str, download_type: str
) -> AsyncGenerator[bytes]:
    async with api.open_message_resource(
        provider_message_id, file_key, download_type
    ) as response:
        async for chunk in response.content.iter_any():
            yield chunk


def resource_name(
    *,
    event_name: str,
    response_name: str | None,
    media_type: str | None,
    resource_type: str,
) -> str:
    """Prefer a name with a suffix, then give a bare name its type's suffix."""

    if _suffix(event_name):
        name = event_name
    elif response_name is not None and _suffix(response_name):
        name = response_name
    elif event_name != resource_type or response_name is None:
        name = event_name
    else:
        name = response_name
    if _suffix(name):
        return name
    declared = (media_type or "").partition(";")[0].strip().lower()
    suffix = (
        mimetypes.guess_extension(declared) or ""
        if declared and not declared.endswith("/octet-stream")
        else ""
    )
    if not _is_suffix(suffix) and resource_type == "audio":
        suffix = ".mp3"
    return f"{name}{suffix}" if _is_suffix(suffix) else name


def _suffix(name: str) -> str:
    dot = name.rfind(".")
    return name[dot:] if dot > 0 and _is_suffix(name[dot:]) else ""


def _is_suffix(suffix: str) -> bool:
    return 1 < len(suffix) <= 11 and suffix[1:].isascii() and suffix[1:].isalnum()


def _disposition_filename(value: str | None) -> str | None:
    if not value or "\r" in value or "\n" in value:
        return None
    header = Message()
    header["Content-Disposition"] = value
    filename = header.get_filename()
    return filename if is_safe_filename(filename) else None


__all__ = ["inbound_attachment", "resource_name"]
