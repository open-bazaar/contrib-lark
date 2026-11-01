"""Connect a Lark bot over its long connection."""

# Existing Lark request and lifecycle budgets are retained.
# ruff: noqa: ASYNC109
from __future__ import annotations

import asyncio
import math
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from functools import partial
from time import time_ns
from typing import Literal
from uuid import NAMESPACE_URL, uuid5

import aiohttp
from bazaar_contrib_audit import Audit, AuditContext, AuditError
from bazaar_contrib_channel import (
    ChannelApprovalRequest,
    ChannelDeliveryReceipt,
    ChannelIdentity,
    ChannelInput,
    ChannelRejectedError,
    ChannelSendAttempt,
    ChannelSendPart,
    ChannelSendRequest,
    ChannelTarget,
    ChannelTargetPresentation,
    ChannelUnavailableError,
    DmAddress,
    DmRequest,
    InboundAttachment,
    InboundMessage,
    SenderIdentity,
)
from bazaar_contrib_collaboration import ContactReviewed
from bazaar_contrib_harness import (
    ApprovalResult,
    HarnessOutput,
    TurnCancelled,
    TurnCompleted,
    TurnFailed,
)
from bazaar_contrib_messages import Agent, Messages
from bazaar_contrib_messages.states import Review
from bazaar_contrib_timer import Timer, wait_for
from pydantic import JsonValue

from .api import LarkApi, LarkApiError, LarkTransportError
from .approval import LarkApprovals
from .attachments import inbound_attachment
from .content import parse_mentions, project_content, provider_text, provider_time_ms
from .identity import (
    LarkBotIdentity,
    LarkThreadIdentity,
    parse_bot_info,
    parse_provider_thread_id,
)
from .markdown import code_points, split_markdown
from .outbound import (
    MAX_MARKDOWN_CODEPOINTS,
    PreparedAttachment,
    channel_error,
    json_string,
    markdown_post_content,
    prepare_attachments,
)
from .transport import CardAnswer, LarkTransport

_STOP = object()
_MESSAGE_EVENT = "im.message.receive_v1"
# The message every group's main conversation holds, under one fixed identity.
_GROUP_MESSAGE_ID = "group"
# 0.2.3 gave fetching a parent message ten seconds; a new connection can take five.
_CALL_TIMEOUT_SECONDS = 10.0
# 0.2.3 kept a name found for a day and retried a failed lookup after five minutes.
_NAME_EXPIRY_MS = 24 * 60 * 60 * 1000
_FAILED_NAME_EXPIRY_MS = 5 * 60 * 1000
_READ_MARKER = "Get"
_SESSION_TIMEOUT = aiohttp.ClientTimeout(total=None, sock_connect=10)


@dataclass(frozen=True, slots=True)
class _Event:
    header: Mapping[str, object]
    message: Mapping[str, object]
    sender: Mapping[str, object]
    provider_message_id: str
    chat_id: str
    chat_type: str
    sender_open_id: str
    sender_type: str


@dataclass(slots=True)
class _Delivery:
    """What one request's operations leave for the next: the anchor and upload keys."""

    anchor: str | None
    in_thread: bool
    keys: dict[int, str] = field(default_factory=dict)


class LarkChannel:
    def __init__(
        self,
        input: ChannelInput,
        timer: Timer,
        audit: Audit,
        messages: Messages,
        group: asyncio.TaskGroup,
        *,
        app_id: str,
        app_secret: str,
        base_url: str,
    ) -> None:
        self._input = input
        self._timer = timer
        self._audit = audit
        self._messages = messages
        self._group = group
        self._app_id = app_id
        self._app_secret = app_secret
        self._base_url = base_url
        self._inbound: asyncio.Queue[InboundMessage | object] = asyncio.Queue()
        self._events: asyncio.Queue[Mapping[str, object]] = asyncio.Queue()
        self._session: aiohttp.ClientSession | None = None
        self._api: LarkApi | None = None
        self._identity: LarkBotIdentity | None = None
        self._transport: LarkTransport | None = None
        self._approvals: LarkApprovals | None = None
        self._mapper: asyncio.Task[None] | None = None
        self._sender_names: dict[str, tuple[int, str | None]] = {}
        self._group_names: dict[str, tuple[int, str | None]] = {}
        self._reviews: dict[str, Review] = {}
        self._group_threads: dict[str, str] = {}
        self._group_messages: set[str] = set()
        self._topic_roots: dict[str, str] = {}
        self._reading: set[tuple[str, str, str]] = set()

    @property
    def name(self) -> str:
        return "lark"

    async def _publish(
        self, event_name: str, payload: JsonValue, error: AuditError | None = None
    ) -> None:
        await self._audit.publish(
            event_name,
            context=AuditContext(agent_id=self._input.agent_id, channel=self.name),
            payload=payload,
            error=error,
        )

    def get_identity(self) -> ChannelIdentity:
        if self._identity is None:
            raise RuntimeError(
                "Lark bot identity is not known before the Channel starts"
            )
        return self._identity.as_channel_identity()

    def backfill_provider_thread_id(
        self, provider_thread_id: str, *, channel: str, channel_identity: str
    ) -> str:
        return provider_thread_id

    def dm_address(self, request: DmRequest) -> DmAddress | None:
        if self._identity is None or request.sender_id is None:
            return None
        thread = LarkThreadIdentity(
            bot_open_id=self._identity.open_id, chat_id=request.sender_id
        )
        return DmAddress(
            channel_session_id=thread.channel_session_id,
            thread_id=thread.session_id,
            provider_thread_id=thread.provider_thread_id,
        )

    def forget_review(self, event: ContactReviewed) -> None:
        if event.agent.id != self._input.agent_id:
            return
        chat_id = self._group_threads.pop(event.thread.id, None)
        if chat_id is not None:
            self._reviews.pop(chat_id, None)

    async def start(self, *, timeout: float) -> None:
        deadline_ms = self._timer.now_ms + math.ceil(max(0.0, timeout) * 1000)
        self._session = aiohttp.ClientSession(timeout=_SESSION_TIMEOUT)
        api = LarkApi(
            self._session,
            self._timer,
            app_id=self._app_id,
            app_secret=self._app_secret,
            base_url=self._base_url,
        )
        self._api = api
        try:
            identity = parse_bot_info(await api.get_bot_info(timeout=timeout))
            self._identity = identity
            self._approvals = LarkApprovals(
                api,
                self._timer,
                self._group,
                bot_open_id=identity.open_id,
                publish=self._publish,
            )
            self._mapper = self._group.create_task(
                self._map_events(), name="bcn-lark-mapper"
            )
            self._transport = LarkTransport(
                api,
                self._timer,
                self._group,
                on_event=self._events.put_nowait,
                on_card=self._answer_card,
                publish=self._publish,
            )
            await wait_for(
                self._timer,
                self._transport.start(
                    timeout=max(0, deadline_ms - self._timer.now_ms) / 1000
                ),
                max(1, deadline_ms - self._timer.now_ms),
            )
        except BaseException:
            await self._close()
            raise

    async def stop(self, *, timeout: float) -> None:
        try:
            if self._approvals is not None:
                await self._approvals.stop(timeout=timeout)
        finally:
            await self._close()
            await self._inbound.put(_STOP)

    async def _close(self) -> None:
        transport = self._transport
        self._transport = None
        if transport is not None:
            await transport.stop()
        mapper = self._mapper
        self._mapper = None
        if mapper is not None:
            mapper.cancel()
            await asyncio.gather(mapper, return_exceptions=True)
        session = self._session
        self._session = None
        if session is not None:
            await session.close()

    async def receive(self) -> AsyncIterator[InboundMessage]:
        while True:
            item = await self._inbound.get()
            if item is _STOP:
                return
            if not isinstance(item, InboundMessage):
                raise TypeError("Lark inbound queue contained an invalid message")
            yield item

    def mark_read(self, item: HarnessOutput, *, target: ChannelTarget) -> None:
        """Add the read marker to the message a turn answers, on the turn's first event."""

        key = (item.session_id, item.turn_id, target.thread_id)
        if isinstance(item.payload, (TurnCompleted, TurnFailed, TurnCancelled)):
            self._reading.discard(key)
            return
        message_id = _sent_message_id(target.reply_to_message_id)
        if key in self._reading or message_id is None:
            return
        self._reading.add(key)
        self._group.create_task(
            self._place_read_marker(message_id), name="bcn-lark-read-marker"
        )

    async def _place_read_marker(self, message_id: str) -> None:
        api = self._api
        if api is None:
            return
        try:
            await api.create_reaction(
                message_id,
                emoji_type=_READ_MARKER,
                timeout=_CALL_TIMEOUT_SECONDS,
            )
        except (LarkApiError, LarkTransportError) as error:
            await self._publish(
                "channel.reaction.failed",
                {"emoji_type": _READ_MARKER},
                AuditError(kind=type(error).__name__, message=str(error)),
            )

    async def prepare_send(
        self, request: ChannelSendRequest
    ) -> tuple[ChannelSendPart, ...]:
        identity = self._identity
        if identity is None or self._api is None:
            raise ChannelUnavailableError(
                "Lark channel is not connected", phase="prepare", delivery="not_sent"
            )
        try:
            thread = parse_provider_thread_id(
                request.target.thread_id, bot_open_id=identity.open_id
            )
        except ValueError as error:
            raise ChannelRejectedError(
                str(error), phase="prepare", code="invalid_target"
            ) from error
        anchor = self._anchor(request.target)
        if thread.thread_id != "0" and anchor is None:
            raise ChannelRejectedError(
                "Lark topic delivery requires a reply anchor",
                phase="prepare",
                code="missing_thread_anchor",
            )
        try:
            batches = (
                split_markdown(
                    request.body, limit=MAX_MARKDOWN_CODEPOINTS, measure=code_points
                )
                if request.body.strip()
                else ()
            )
        except ValueError as error:
            raise ChannelRejectedError(
                str(error), phase="prepare", code="invalid_markdown"
            ) from error
        try:
            attachments = await asyncio.to_thread(
                prepare_attachments, self._input.workspace, request.attachments
            )
        except (OSError, ValueError) as error:
            raise ChannelRejectedError(
                str(error), phase="prepare", code="invalid_attachment"
            ) from error
        if not batches and not attachments:
            raise ChannelRejectedError(
                "Lark outbound message must not be empty",
                phase="prepare",
                code="empty_message",
            )
        delivery = _Delivery(anchor=anchor, in_thread=thread.thread_id != "0")
        operations = [
            ChannelSendPart(
                send=partial(
                    self._send_message,
                    delivery,
                    thread,
                    "post",
                    markdown_post_content(batch),
                ),
                message=request.model_copy(update={"body": batch, "attachments": ()}),
                idempotent=True,
            )
            for batch in batches
        ]
        for index, attachment in enumerate(attachments):
            operations.append(
                ChannelSendPart(
                    send=partial(self._upload, delivery, index, attachment),
                    message=None,
                )
            )
            operations.append(
                ChannelSendPart(
                    send=partial(
                        self._send_attachment, delivery, thread, index, attachment
                    ),
                    message=request.model_copy(
                        update={
                            "body": "",
                            "attachments": (request.attachments[index],),
                        }
                    ),
                    idempotent=True,
                )
            )
        return tuple(operations)

    async def _send_message(
        self,
        delivery: _Delivery,
        thread: LarkThreadIdentity,
        message_type: str,
        content: str,
        attempt: ChannelSendAttempt,
    ) -> ChannelDeliveryReceipt:
        api = self._require_api("send")
        try:
            if delivery.anchor is not None:
                sent = await api.reply_message(
                    message_id=delivery.anchor,
                    message_type=message_type,
                    content=content,
                    reply_in_thread=delivery.in_thread,
                    uuid=attempt.id,
                    timeout=attempt.timeout,
                )
            else:
                sent = await api.send_message(
                    receive_id=thread.chat_id,
                    # Lark ids carry their kind in the prefix: `ou_` a user, `oc_` a chat.
                    receive_id_type="open_id"
                    if thread.chat_id.startswith("ou_")
                    else "chat_id",
                    message_type=message_type,
                    content=content,
                    uuid=attempt.id,
                    timeout=attempt.timeout,
                )
        except (LarkApiError, LarkTransportError) as error:
            raise channel_error(error, phase="send") from error
        # A topic chains each part on the last; the main chat replies once.
        delivery.anchor = sent.message_id if delivery.in_thread else None
        return ChannelDeliveryReceipt(
            status="confirmed",
            provider_message_id=sent.message_id,
            receipt={"message_type": message_type},
        )

    async def _upload(
        self,
        delivery: _Delivery,
        index: int,
        attachment: PreparedAttachment,
        attempt: ChannelSendAttempt,
    ) -> None:
        api = self._require_api("upload")
        file = await asyncio.to_thread(attachment.open)
        try:
            if attachment.message_type == "image":
                key = await api.upload_image(
                    file,
                    filename=attachment.descriptor.name,
                    media_type=attachment.media_type,
                    timeout=attempt.timeout,
                )
            else:
                key = await api.upload_file(
                    file,
                    file_type=attachment.file_type,
                    filename=attachment.descriptor.name,
                    media_type=attachment.media_type,
                    timeout=attempt.timeout,
                )
        except (LarkApiError, LarkTransportError) as error:
            raise channel_error(error, phase="upload") from error
        finally:
            await asyncio.to_thread(file.close)
        delivery.keys[index] = key

    async def _send_attachment(
        self,
        delivery: _Delivery,
        thread: LarkThreadIdentity,
        index: int,
        attachment: PreparedAttachment,
        attempt: ChannelSendAttempt,
    ) -> ChannelDeliveryReceipt:
        key_field = "image_key" if attachment.message_type == "image" else "file_key"
        return await self._send_message(
            delivery,
            thread,
            attachment.message_type,
            json_string({key_field: delivery.keys[index]}),
            attempt,
        )

    def _require_api(self, phase: Literal["send", "upload"]) -> LarkApi:
        if self._api is None:
            raise ChannelUnavailableError(
                "Lark channel is not connected", phase=phase, delivery="not_sent"
            )
        return self._api

    async def request_approval(
        self, request: ChannelApprovalRequest, *, timeout: float
    ) -> ApprovalResult:
        if self._approvals is None:
            raise RuntimeError("Lark channel is not ready for approvals")
        return await self._approvals.request(
            request, anchor=self._anchor(request.target), timeout=timeout
        )

    def _anchor(self, target: ChannelTarget) -> str | None:
        """The Lark message a send to the target answers, if any."""

        return _sent_message_id(target.reply_to_message_id) or self._topic_roots.get(
            target.thread_id
        )

    async def _answer_card(self, payload: Mapping[str, object]) -> CardAnswer:
        approvals = self._approvals
        if approvals is None:
            raise RuntimeError("Lark card callback arrived before the Channel started")
        return await approvals.answer(payload)

    async def _map_events(self) -> None:
        while True:
            payload = await self._events.get()
            try:
                await self._map_event(payload)
            except Exception as error:  # noqa: BLE001
                # One failed event must not stop ingress or reveal provider credentials.
                await self._publish(
                    "channel.message.filtered",
                    {"reason": "mapping_failed"},
                    AuditError(kind=type(error).__name__, message=type(error).__name__),
                )

    async def _map_event(self, payload: Mapping[str, object]) -> None:
        header = payload.get("header")
        event = payload.get("event")
        if (
            payload.get("schema") != "2.0"
            or not isinstance(header, Mapping)
            or not isinstance(event, Mapping)
        ):
            await self._filter("invalid_envelope")
            return
        if header.get("event_type") != _MESSAGE_EVENT:
            await self._filter("unsupported_event_type")
            return
        identity = self._identity
        assert identity is not None
        fields = _read_event(header, event, identity)
        if isinstance(fields, str):
            await self._filter(fields, event.get("message"))
            return
        thread_id, readable = _optional_reference(fields.message.get("thread_id"))
        if not readable:
            await self._filter("invalid_thread_id", fields.message)
            return
        parent_id, readable = _optional_reference(fields.message.get("parent_id"))
        if not readable or parent_id == fields.provider_message_id:
            await self._filter("invalid_parent_id", fields.message)
            return
        root_id, readable = _optional_reference(fields.message.get("root_id"))
        if not readable:
            await self._filter("invalid_root_id", fields.message)
            return
        target_kind: Literal["dm", "group"] = (
            "dm" if fields.chat_type == "p2p" else "group"
        )
        thread = LarkThreadIdentity(
            bot_open_id=identity.open_id,
            # A one-to-one chat is named by the peer, not by the `oc_` id the
            # provider gives that chat: the peer is the only part of it we can
            # know before the chat exists, so a conversation opened from
            # `dm:@name` and one the peer starts are the same conversation.
            chat_id=fields.sender_open_id if target_kind == "dm" else fields.chat_id,
            thread_id=thread_id or "0",
        )
        presentation = None
        metadata: dict[str, JsonValue] = {
            "lark_event_type": _MESSAGE_EVENT,
            "lark_chat_type": fields.chat_type,
            "lark_sender_type": fields.sender_type,
            "lark_threaded": thread.thread_id != "0",
            "threaded": thread.thread_id != "0",
        }
        event_id = provider_text(header.get("event_id"))
        if event_id is not None:
            metadata["lark_event_id"] = event_id
        if root_id is not None:
            metadata["lark_root_id"] = root_id
        if thread.thread_id != "0":
            # Lark posts into a topic only as a reply to a message already in it.
            self._topic_roots[thread.provider_thread_id] = (
                root_id or fields.provider_message_id
            )
        if target_kind == "group":
            name = await self._group_name(fields.chat_id)
            if name is not None:
                presentation = ChannelTargetPresentation(display_name=name)
            if thread.thread_id != "0":
                review = await self._group_review(thread.main)
                if review is None:
                    await self._group_message(
                        thread.main, name or fields.chat_id, fields.sender
                    )
                elif review is Review.APPROVED:
                    metadata["review"] = "approved"
        received_at_ms = time_ns() // 1_000_000
        if parent_id is not None:
            metadata["provider_reply_to_message_id"] = parent_id
            quoted = await self._quoted(
                parent_id,
                approved=metadata.get("review") == "approved",
                chat_id=fields.chat_id,
                chat_type=fields.chat_type,
                thread=thread,
                target_kind=target_kind,
                presentation=presentation,
                received_at_ms=received_at_ms,
            )
            if quoted is None:
                metadata["reply_fetch_failed"] = True
            else:
                await self._inbound.put(quoted)
        await self._inbound.put(
            await self._inbound_message(
                fields.message,
                thread=thread,
                target_kind=target_kind,
                presentation=presentation,
                sender=fields.sender,
                reply_to=parent_id,
                mentions_agent=target_kind == "group",
                notifies_runtime=True,
                received_at_ms=received_at_ms,
                metadata=metadata,
            )
        )

    async def _group_message(
        self,
        main: LarkThreadIdentity,
        name: str,
        sender: Mapping[str, object],
    ) -> None:
        """Give a group with no conversation its main one, to review, once a run."""

        if main.chat_id in self._group_messages:
            return
        self._group_messages.add(main.chat_id)
        sender_identity = await self._sender(sender)
        await self._inbound.put(
            InboundMessage(
                id=main.message_id(f"{_GROUP_MESSAGE_ID}:{main.chat_id}"),
                provider_message_id=f"{_GROUP_MESSAGE_ID}:{main.chat_id}",
                channel_session_id=main.channel_session_id,
                thread_id=main.session_id,
                target=ChannelTarget(kind="group", thread_id=main.provider_thread_id),
                sender=sender_identity,
                body=name,
                target_presentation=ChannelTargetPresentation(display_name=name),
                provider_chat_id=main.chat_id,
                received_at_ms=time_ns() // 1_000_000,
                mentions_agent=True,
                notifies_runtime=True,
                metadata={
                    "lark_event_type": "group",
                    "sender_kind": sender_identity.kind,
                },
            )
        )

    async def _group_review(self, main: LarkThreadIdentity) -> Review | None:
        """Read the review of a group's main conversation, or None when it has none."""

        review = self._reviews.get(main.chat_id)
        if review is not None:
            return review
        session = await self._messages.storage(
            Agent(self._input.agent_id)
        ).get_channel_session(
            self._stored_id("channel-session", main.channel_session_id)
        )
        if session is None:
            return None
        self._reviews[main.chat_id] = session.review
        self._group_threads[self._stored_id("bcn-session", main.session_id)] = (
            main.chat_id
        )
        return session.review

    def _stored_id(self, kind: str, channel_id: str) -> str:
        """Name an id the way the agent stores the ids this Channel gives it."""

        return str(
            uuid5(NAMESPACE_URL, f"bcn:{self._input.agent_id}:{kind}:{channel_id}")
        )

    async def _quoted(
        self,
        parent_id: str,
        *,
        approved: bool,
        chat_id: str,
        chat_type: str,
        thread: LarkThreadIdentity,
        target_kind: Literal["dm", "group"],
        presentation: ChannelTargetPresentation | None,
        received_at_ms: int,
    ) -> InboundMessage | None:
        """Fetch the message a reply quotes, so the reply has something to point at."""

        failure: str
        api = self._api
        assert api is not None
        try:
            response = await api.get_message(parent_id, timeout=_CALL_TIMEOUT_SECONDS)
        except (LarkApiError, LarkTransportError) as error:
            failure = f"request_failed:{type(error).__name__}"
        else:
            data = response.get("data")
            items = data.get("items") if isinstance(data, Mapping) else None
            parent = (
                _parent_message(items[0])
                if isinstance(items, list) and items and isinstance(items[0], Mapping)
                else None
            )
            if parent is None or provider_text(parent.get("message_id")) is None:
                failure = "malformed_response"
            elif (
                parent.get("chat_id") is not None
                and provider_text(parent.get("chat_id")) != chat_id
            ):
                failure = "parent_chat_mismatch"
            else:
                return await self._inbound_message(
                    parent,
                    thread=thread,
                    target_kind=target_kind,
                    presentation=presentation,
                    sender=parent.get("sender"),
                    reply_to=None,
                    mentions_agent=False,
                    notifies_runtime=False,
                    received_at_ms=received_at_ms,
                    metadata={
                        "quoted_backfill": True,
                        "lark_chat_type": chat_type,
                        "threaded": thread.thread_id != "0",
                        # It may open the topic, which then follows the group too.
                        **({"review": "approved"} if approved else {}),
                    },
                )
        await self._publish(
            "channel.message.reference_unresolved",
            {"provider_message_id": parent_id, "reason": failure},
        )
        return None

    async def _inbound_message(
        self,
        message: Mapping[str, object],
        *,
        thread: LarkThreadIdentity,
        target_kind: Literal["dm", "group"],
        presentation: ChannelTargetPresentation | None,
        sender: object,
        reply_to: str | None,
        mentions_agent: bool,
        notifies_runtime: bool,
        received_at_ms: int,
        metadata: dict[str, JsonValue],
    ) -> InboundMessage:
        provider_message_id = provider_text(message.get("message_id"))
        message_type = provider_text(message.get("message_type"))
        identity = self._identity
        api = self._api
        if provider_message_id is None or message_type is None:
            raise ValueError("Lark message is missing provider identity")
        assert identity is not None and api is not None
        content = project_content(
            message_type,
            message.get("content"),
            mentions=parse_mentions(message.get("mentions")),
            bot_open_id=identity.open_id,
        )
        attachments: list[InboundAttachment] = [
            await inbound_attachment(api, provider_message_id, resource)
            for resource in content.resources
        ]
        sender_identity = await self._sender(sender)
        metadata.setdefault("sender_kind", sender_identity.kind)
        if content.content_error:
            metadata["content_parse_failed"] = True
        occurred_at_ms = provider_time_ms(message.get("create_time"))
        return InboundMessage(
            id=thread.message_id(provider_message_id),
            provider_message_id=provider_message_id,
            channel_session_id=thread.channel_session_id,
            thread_id=thread.session_id,
            target=ChannelTarget(
                kind=target_kind,
                thread_id=thread.provider_thread_id,
                reply_to_message_id=reply_to,
            ),
            sender=sender_identity,
            body=content.body,
            message_type=content.message_type,
            target_presentation=presentation,
            received_at_ms=received_at_ms,
            attachments=tuple(attachments),
            occurred_at_ms=occurred_at_ms,
            provider_chat_id=thread.chat_id,
            notifies_runtime=notifies_runtime,
            mentions_agent=mentions_agent,
            metadata=metadata,
        )

    async def _sender(self, sender: object) -> SenderIdentity:
        """Name the sender by open_id; only a person is looked up for a display name."""

        open_id = _sender_open_id(sender)
        if open_id is None or not isinstance(sender, Mapping):
            return SenderIdentity(name="unknown", kind="unknown")
        sender_type = provider_text(sender.get("sender_type"))
        if sender_type in {"app", "bot", "system"}:
            return SenderIdentity(id=open_id, kind="agent")
        if sender_type != "user":
            return SenderIdentity(id=open_id, kind="unknown")
        cached = self._sender_names.get(open_id)
        if cached is None or cached[0] <= self._timer.now_ms:
            name = await self._lookup_sender(open_id)
            cached = (self._name_expiry(name), name)
            self._sender_names[open_id] = cached
        return SenderIdentity(id=open_id, kind="human", display_name=cached[1])

    async def _lookup_sender(self, open_id: str) -> str | None:
        api = self._api
        assert api is not None
        try:
            response = await api.get_user(open_id, timeout=_CALL_TIMEOUT_SECONDS)
        except (LarkApiError, LarkTransportError) as error:
            await self._directory_failed("contact_user", error)
            return None
        data = response.get("data")
        user = data.get("user") if isinstance(data, Mapping) else None
        if not isinstance(user, Mapping):
            return None
        return next(
            (
                name
                for field_name in ("name", "en_name", "nickname")
                if (name := provider_text(user.get(field_name))) is not None
            ),
            None,
        )

    async def _group_name(self, chat_id: str) -> str | None:
        cached = self._group_names.get(chat_id)
        if cached is None or cached[0] <= self._timer.now_ms:
            api = self._api
            assert api is not None
            name: str | None = None
            try:
                response = await api.get_chat(chat_id, timeout=_CALL_TIMEOUT_SECONDS)
            except (LarkApiError, LarkTransportError) as error:
                await self._directory_failed("chat_get", error)
            else:
                data = response.get("data")
                name = (
                    provider_text(data.get("name"))
                    if isinstance(data, Mapping)
                    else None
                )
            cached = (self._name_expiry(name), name)
            self._group_names[chat_id] = cached
        return cached[1]

    def _name_expiry(self, name: str | None) -> int:
        return self._timer.now_ms + (
            _NAME_EXPIRY_MS if name is not None else _FAILED_NAME_EXPIRY_MS
        )

    async def _directory_failed(
        self, method: str, error: LarkApiError | LarkTransportError
    ) -> None:
        await self._publish(
            "channel.directory.failed",
            {"method": method},
            AuditError(kind=type(error).__name__, message=str(error)),
        )

    async def _filter(self, reason: str, message: object = None) -> None:
        payload: dict[str, JsonValue] = {"reason": reason}
        if isinstance(message, Mapping):
            provider_message_id = provider_text(message.get("message_id"))
            if provider_message_id is not None:
                payload["provider_message_id"] = provider_message_id
        await self._publish("channel.message.filtered", payload)


def _read_event(
    header: Mapping[str, object],
    event: Mapping[str, object],
    identity: LarkBotIdentity,
) -> _Event | str:
    """Read a message event, or name the reason it cannot be taken."""

    message = event.get("message")
    sender = event.get("sender")
    if not isinstance(message, Mapping) or not isinstance(sender, Mapping):
        return "invalid_message_event"
    provider_message_id = provider_text(message.get("message_id"))
    chat_id = provider_text(message.get("chat_id"))
    chat_type = provider_text(message.get("chat_type"))
    open_id = _sender_open_id(sender)
    if provider_message_id is None:
        return "invalid_message_id"
    if chat_id is None:
        return "invalid_chat_id"
    if chat_type not in {"p2p", "group", "topic"}:
        return "unsupported_chat_type"
    if provider_text(message.get("message_type")) is None:
        return "invalid_message_type"
    if open_id is None:
        return "invalid_sender"
    if open_id == identity.open_id:
        return "current_bot_message"
    assert chat_type is not None
    return _Event(
        header=header,
        message=message,
        sender=sender,
        provider_message_id=provider_message_id,
        chat_id=chat_id,
        chat_type=chat_type,
        sender_open_id=open_id,
        sender_type=provider_text(sender.get("sender_type")) or "unknown",
    )


def _sender_open_id(sender: object) -> str | None:
    if not isinstance(sender, Mapping):
        return None
    sender_id = sender.get("sender_id")
    return (
        provider_text(sender_id.get("open_id"))
        if isinstance(sender_id, Mapping)
        else None
    )


def _optional_reference(value: object) -> tuple[str | None, bool]:
    """Read an id the provider may omit; the flag says whether it was readable."""

    if value is None or value == "":
        return None, True
    text = provider_text(value)
    return text, text is not None


def _sent_message_id(provider_message_id: str | None) -> str | None:
    """Pass a message Lark sent; the group message stands for the chat itself."""

    if provider_message_id is None or provider_message_id.startswith(
        f"{_GROUP_MESSAGE_ID}:"
    ):
        return None
    return provider_message_id


def _parent_message(message: Mapping[str, object]) -> dict[str, object]:
    """Adapt the message REST response to the event message shape."""

    parent = dict(message)
    if provider_text(parent.get("message_type")) is None:
        message_type = provider_text(parent.get("msg_type"))
        if message_type is not None:
            parent["message_type"] = message_type
    if parent.get("content") is None:
        body = parent.get("body")
        if isinstance(body, Mapping) and "content" in body:
            parent["content"] = body["content"]
    sender = parent.get("sender")
    if isinstance(sender, Mapping) and not isinstance(sender.get("sender_id"), Mapping):
        sender_id = provider_text(sender.get("id"))
        if (
            sender_id is not None
            and (provider_text(sender.get("id_type")) or "open_id") == "open_id"
        ):
            parent["sender"] = {**sender, "sender_id": {"open_id": sender_id}}
    return parent


__all__ = ["LarkChannel"]
