"""Prepare what the agent sends to Lark, and translate how Lark refuses it."""

from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from bazaar_contrib_channel import (
    ChannelAuthenticationError,
    ChannelConnectionError,
    ChannelError,
    ChannelRateLimitError,
    ChannelRejectedError,
    ChannelSendPhase,
    ChannelTimeoutError,
    OutboundAttachment,
)

from .api import LarkApiError, LarkTransportError

MAX_MARKDOWN_CODEPOINTS = 3_500
MAX_FILENAME_BYTES = 256
# Lark's own upload limits.
MAX_IMAGE_BYTES = 10 * 1024 * 1024
MAX_FILE_BYTES = 30 * 1024 * 1024

_IMAGE_SUFFIXES = frozenset(
    {".bmp", ".gif", ".ico", ".jpeg", ".jpg", ".png", ".tif", ".tiff", ".webp"}
)
_AUDIO_SUFFIXES = frozenset({".opus"})
_VIDEO_SUFFIXES = frozenset({".mp4"})
_DOCUMENT_FILE_TYPES = {
    ".doc": "doc",
    ".docx": "doc",
    ".pdf": "pdf",
    ".ppt": "ppt",
    ".pptx": "ppt",
    ".xls": "xls",
    ".xlsx": "xls",
}
# 0.2.3 read these codes as rejected credentials, and 99991400 is the rate limit.
_AUTHENTICATION_CODES = frozenset({403, 514, 1000040344, 1000040350})
_RATE_LIMIT_CODES = frozenset({99991400})


@dataclass(frozen=True, slots=True)
class PreparedAttachment:
    descriptor: OutboundAttachment
    workspace: Path
    media_type: str
    message_type: str
    file_type: str
    size_bytes: int
    device: int
    inode: int

    def open(self) -> BinaryIO:
        descriptor, file_stat = _open_regular_file(self.workspace, self.descriptor)
        if (
            file_stat.st_dev != self.device
            or file_stat.st_ino != self.inode
            or file_stat.st_size != self.size_bytes
        ):
            os.close(descriptor)
            raise ValueError(
                f"Lark attachment changed after preflight: {self.descriptor.name}"
            )
        return os.fdopen(descriptor, "rb")


def markdown_post_content(markdown: str) -> str:
    return json_string(
        {"zh_cn": {"title": "", "content": [[{"tag": "md", "text": markdown}]]}}
    )


def json_string(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def prepare_attachments(
    workspace: Path, attachments: tuple[OutboundAttachment, ...]
) -> tuple[PreparedAttachment, ...]:
    root = workspace.resolve(strict=True)
    prepared: list[PreparedAttachment] = []
    for attachment in attachments:
        if len(attachment.name.encode("utf-8")) > MAX_FILENAME_BYTES:
            raise ValueError(
                f"Lark attachment filename exceeds 256 bytes: {attachment.name}"
            )
        media_type = (
            attachment.media_type
            or mimetypes.guess_type(attachment.name)[0]
            or "application/octet-stream"
        )
        message_type = _message_type(attachment.name)
        maximum_size = MAX_IMAGE_BYTES if message_type == "image" else MAX_FILE_BYTES
        descriptor, file_stat = _open_regular_file(root, attachment)
        try:
            if file_stat.st_size == 0:
                raise ValueError(
                    f"Lark attachment must not be empty: {attachment.name}"
                )
            if file_stat.st_size > maximum_size:
                raise ValueError(
                    f"Lark {message_type} attachment exceeds its size limit: "
                    f"{attachment.name}"
                )
            if (
                attachment.size_bytes is not None
                and attachment.size_bytes != file_stat.st_size
            ):
                raise ValueError(f"Lark attachment size changed: {attachment.name}")
            if attachment.sha256 is not None:
                with os.fdopen(os.dup(descriptor), "rb") as file:
                    digest = hashlib.file_digest(file, "sha256").hexdigest()
                if digest != attachment.sha256:
                    raise ValueError(
                        f"Lark attachment digest mismatch: {attachment.name}"
                    )
        finally:
            os.close(descriptor)
        prepared.append(
            PreparedAttachment(
                descriptor=attachment,
                workspace=root,
                media_type=media_type,
                message_type=message_type,
                file_type=_file_type(attachment.name),
                size_bytes=file_stat.st_size,
                device=file_stat.st_dev,
                inode=file_stat.st_ino,
            )
        )
    return tuple(prepared)


def channel_error(error: Exception, *, phase: ChannelSendPhase) -> ChannelError:
    """Say what a failed Lark call means for its delivery."""

    if isinstance(error, ChannelError):
        return error
    if isinstance(error, LarkTransportError):
        if error.error_kind == "TimeoutError":
            return ChannelTimeoutError(str(error), phase=phase)
        return ChannelConnectionError(str(error), phase=phase)
    if isinstance(error, TimeoutError):
        return ChannelTimeoutError(str(error) or "Lark call timed out", phase=phase)
    if isinstance(error, LarkApiError):
        code = str(error.provider_code or error.http_status)
        if (
            error.http_status in {401, 403}
            or error.provider_code in _AUTHENTICATION_CODES
        ):
            return ChannelAuthenticationError(str(error), phase=phase, code=code)
        if error.http_status == 429 or error.provider_code in _RATE_LIMIT_CODES:
            return ChannelRateLimitError(str(error), phase=phase, code=code)
        if error.provider_code == 0 or error.http_status >= 500:
            # Lark failed on its side, or accepted the call with an unreadable answer.
            return ChannelError(str(error), phase=phase, retryable=True, code=code)
        return ChannelRejectedError(str(error), phase=phase, code=code)
    return ChannelError(str(error) or type(error).__name__, phase=phase)


def _message_type(name: str) -> str:
    suffix = PurePosixPath(name).suffix.lower()
    if suffix in _IMAGE_SUFFIXES:
        return "image"
    if suffix in _AUDIO_SUFFIXES:
        return "audio"
    if suffix in _VIDEO_SUFFIXES:
        return "media"
    return "file"


def _file_type(name: str) -> str:
    suffix = PurePosixPath(name).suffix.lower()
    if suffix in _AUDIO_SUFFIXES:
        return "opus"
    if suffix in _VIDEO_SUFFIXES:
        return "mp4"
    return _DOCUMENT_FILE_TYPES.get(suffix, "stream")


def _open_regular_file(
    workspace: Path, attachment: OutboundAttachment
) -> tuple[int, os.stat_result]:
    relative = (
        attachment.path.relative_to(workspace)
        if attachment.path.is_absolute()
        else attachment.path
    )
    current = workspace
    for part in relative.parts:
        current /= part
        if current.is_symlink():
            raise ValueError(
                f"Lark attachment path must not contain symlinks: {attachment.name}"
            )
    resolved = current.resolve(strict=True)
    try:
        resolved.relative_to(workspace)
    except ValueError as error:
        raise ValueError(
            f"Lark attachment path leaves the workspace: {attachment.name}"
        ) from error
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_BINARY", 0)
    descriptor = os.open(resolved, flags)
    try:
        file_stat = os.fstat(descriptor)
        opened_stat = os.stat(resolved, follow_symlinks=False)
        if (
            file_stat.st_dev != opened_stat.st_dev
            or file_stat.st_ino != opened_stat.st_ino
        ):
            raise ValueError(
                f"Lark attachment changed while it was opened: {attachment.name}"
            )
        if not stat.S_ISREG(file_stat.st_mode):
            raise ValueError(
                f"Lark attachment must be a regular file: {attachment.name}"
            )
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor, file_stat


__all__ = [
    "MAX_MARKDOWN_CODEPOINTS",
    "PreparedAttachment",
    "channel_error",
    "json_string",
    "markdown_post_content",
    "prepare_attachments",
]
