"""Encode and decode the protobuf frames of the Lark long connection."""

from __future__ import annotations

from collections.abc import Iterable

import protobug


class FrameDecodeError(ValueError):
    """Raised when a provider frame violates the wire contract."""


@protobug.message
class Header:
    key: protobug.String = protobug.field(1)
    value: protobug.String = protobug.field(2)


@protobug.message
class Frame:
    SeqID: protobug.UInt64 = protobug.field(1)
    LogID: protobug.UInt64 = protobug.field(2)
    service: protobug.Int32 = protobug.field(3)
    method: protobug.Int32 = protobug.field(4)
    headers: list[Header] = protobug.field(5, default_factory=list)
    payload_encoding: protobug.String | None = protobug.field(6, default=None)
    payload_type: protobug.String | None = protobug.field(7, default=None)
    payload: protobug.Bytes | None = protobug.field(8, default=None)
    LogIDNew: protobug.String | None = protobug.field(9, default=None)


def encode_frame(frame: Frame) -> bytes:
    validate_frame(frame)
    return protobug.dumps(frame)


def decode_frame(raw: object) -> Frame:
    if not isinstance(raw, bytes):
        raise FrameDecodeError("frame payload must be bytes")
    if not raw:
        raise FrameDecodeError("frame payload is empty")
    try:
        frame = protobug.loads(raw, Frame)
    except (ValueError, TypeError, EOFError) as error:
        raise FrameDecodeError("frame decoding failed") from error
    validate_frame(frame)
    return frame


def validate_frame(frame: object) -> None:
    if not isinstance(frame, Frame):
        raise FrameDecodeError("frame has an invalid type")
    if not isinstance(frame.headers, list):
        raise FrameDecodeError("frame headers have an invalid type")
    for header in frame.headers:
        if not isinstance(header, Header):
            raise FrameDecodeError("frame contains an invalid header")
        if not isinstance(header.key, str) or not header.key:
            raise FrameDecodeError("header key must be non-empty text")
        if not isinstance(header.value, str):
            raise FrameDecodeError("header value must be text")


def header_values(headers: Iterable[Header], key: str) -> tuple[str, ...]:
    return tuple(header.value for header in headers if header.key == key)


__all__ = [
    "Frame",
    "FrameDecodeError",
    "Header",
    "decode_frame",
    "encode_frame",
    "header_values",
]
