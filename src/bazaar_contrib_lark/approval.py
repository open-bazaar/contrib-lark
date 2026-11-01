"""Ask the original sender to approve an action with an interactive card."""

# The provider owns the approval deadline as part of the public call contract.
# ruff: noqa: ASYNC109
from __future__ import annotations

import asyncio
import base64
import json
import math
import secrets
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from dataclasses import dataclass
from typing import Literal

from bazaar_contrib_audit import AuditError
from bazaar_contrib_channel import ChannelApprovalRequest
from bazaar_contrib_harness import ApprovalResult
from bazaar_contrib_timer import Timer, wait_for
from pydantic import JsonValue

from .api import LarkApi, LarkApiError, LarkTransportError
from .content import provider_text
from .identity import LarkThreadIdentity, parse_provider_thread_id
from .outbound import json_string
from .transport import CardAnswer

# 0.2.3 gave a card update five seconds; a new connection alone can take five.
_CARD_UPDATE_TIMEOUT_SECONDS = 10.0

TEXT = {
    "approval.action.command_execution": "command execution",
    "approval.action.file_change": "file change",
    "approval.action.permissions": "permissions",
    "approval.button.approve": "✅ Approve",
    "approval.button.reject": "❎ Reject",
    "approval.callback.approved": "Approved",
    "approval.callback.invalid": "Approval is no longer valid",
    "approval.callback.rejected": "Rejected",
    "approval.callback.sender_mismatch": "This approval belongs to another user",
    "approval.callback.unknown_action": "Unknown approval action",
    "approval.card.status.approved": "Approved",
    "approval.card.status.rejected": "Rejected",
    "approval.feedback.cancelled": "Action rejected: the approval request was cancelled",
    "approval.feedback.channel_stopped": "Action rejected: the Lark channel stopped",
    "approval.prompt.title": "Approval required",
}

Publish = Callable[[str, JsonValue, AuditError | None], Awaitable[None]]


@dataclass(slots=True)
class _Pending:
    request: ChannelApprovalRequest
    token: str
    thread: LarkThreadIdentity
    chat_id: str
    future: asyncio.Future[ApprovalResult]
    message_id: str | None = None


class LarkApprovals:
    def __init__(
        self,
        api: LarkApi,
        timer: Timer,
        group: asyncio.TaskGroup,
        *,
        bot_open_id: str,
        publish: Publish,
    ) -> None:
        self._api = api
        self._timer = timer
        self._group = group
        self._bot_open_id = bot_open_id
        self._publish = publish
        self._pending: dict[str, _Pending] = {}
        self._tasks: set[asyncio.Task[None]] = set()

    async def request(
        self, request: ChannelApprovalRequest, *, anchor: str | None, timeout: float
    ) -> ApprovalResult:
        """Send the card within the budget, then wait for the sender's decision."""

        if timeout <= 0:
            raise TimeoutError("Lark approval card delivery timed out")
        if any(
            item.request.approval.request_id == request.approval.request_id
            for item in self._pending.values()
        ):
            raise ValueError("Lark approval request is already pending")
        thread = parse_provider_thread_id(
            request.target.thread_id, bot_open_id=self._bot_open_id
        )
        if thread.thread_id != "0" and anchor is None:
            raise ValueError("Lark approval requires a topic reply anchor")
        pending = _Pending(
            request=request,
            token=secrets.token_urlsafe(18),
            thread=thread,
            chat_id=thread.chat_id,
            future=asyncio.get_running_loop().create_future(),
        )
        self._pending[pending.token] = pending
        try:
            await wait_for(
                self._timer,
                self._send(pending, anchor, timeout),
                math.ceil(timeout * 1000),
            )
            return await pending.future
        except asyncio.CancelledError:
            # Cancelling the wait cancels the future it awaits; the card still closes.
            if not pending.future.done() or pending.future.cancelled():
                pending.future.cancel()
                self._close(pending, TEXT["approval.feedback.cancelled"])
            raise
        finally:
            self._pending.pop(pending.token, None)

    async def _send(
        self, pending: _Pending, anchor: str | None, timeout: float
    ) -> None:
        content = json_string(_card(pending.request, pending.token))
        if anchor is not None:
            sent = await self._api.reply_message(
                message_id=anchor,
                message_type="interactive",
                content=content,
                reply_in_thread=pending.thread.thread_id != "0",
                uuid=pending.token,
                timeout=timeout,
            )
        else:
            sent = await self._api.send_message(
                receive_id=pending.chat_id,
                # A one-to-one chat is named by the peer, an `ou_` id; card callbacks
                # report the `oc_` id Lark gave that chat, which only the send tells.
                receive_id_type="open_id"
                if pending.chat_id.startswith("ou_")
                else "chat_id",
                message_type="interactive",
                content=content,
                uuid=pending.token,
                timeout=timeout,
            )
        if sent.chat_id is not None:
            pending.chat_id = sent.chat_id
        pending.message_id = sent.message_id

    async def answer(self, payload: Mapping[str, object]) -> CardAnswer:
        """Settle a pending approval from a button press, within Lark's three seconds."""

        callback = _parse_callback(payload)
        if callback is None:
            return await self._reject("invalid_callback", "approval.callback.invalid")
        operator_id, chat_id, message_id, action, token, update_token = callback
        pending = self._pending.get(token)
        if pending is None or pending.future.done():
            return await self._reject("not_pending", "approval.callback.invalid")
        if operator_id != pending.request.sender_id:
            return await self._reject(
                "sender_mismatch", "approval.callback.sender_mismatch"
            )
        if chat_id != pending.chat_id or (
            pending.message_id is not None and message_id != pending.message_id
        ):
            return await self._reject("route_mismatch", "approval.callback.invalid")
        if action not in {"approve", "reject"}:
            return await self._reject(
                "unknown_action", "approval.callback.unknown_action"
            )
        pending.message_id = message_id
        decision: Literal["approved", "rejected"] = (
            "approved" if action == "approve" else "rejected"
        )
        pending.future.set_result(
            ApprovalResult(
                request_id=pending.request.approval.request_id, decision=decision
            )
        )
        card = _card(
            pending.request,
            pending.token,
            status=TEXT[f"approval.card.status.{decision}"],
        )
        return CardAnswer(
            payload=_toast(TEXT[f"approval.callback.{decision}"], "success"),
            after=lambda: self._redraw(update_token, card),
        )

    async def stop(self, *, timeout: float) -> None:
        """Reject every pending card and close it, waiting within the budget."""

        for pending in tuple(self._pending.values()):
            if not pending.future.done():
                pending.future.set_result(
                    ApprovalResult(
                        request_id=pending.request.approval.request_id,
                        decision="rejected",
                        reason="channel_stopped",
                    )
                )
                self._close(pending, TEXT["approval.feedback.channel_stopped"])
        self._pending.clear()
        tasks = tuple(self._tasks)
        if tasks:
            try:
                await wait_for(
                    self._timer,
                    asyncio.gather(*tasks, return_exceptions=True),
                    math.ceil(max(0.0, timeout) * 1000),
                )
            except TimeoutError:
                pass

    async def _reject(self, reason: str, text_key: str) -> CardAnswer:
        await self._publish(
            "channel.approval.callback_rejected", {"reason": reason}, None
        )
        return CardAnswer(payload=_toast(TEXT[text_key], "warning"))

    async def _redraw(self, update_token: str, card: Mapping[str, object]) -> None:
        try:
            await self._api.update_card(
                update_token, card=card, timeout=_CARD_UPDATE_TIMEOUT_SECONDS
            )
        except (LarkApiError, LarkTransportError) as error:
            await self._edit_failed(error)

    def _close(self, pending: _Pending, feedback: str) -> None:
        if pending.message_id is None:
            return
        card = _card(pending.request, pending.token, status=feedback)
        self._spawn(self._close_card(pending.message_id, card))

    async def _close_card(self, message_id: str, card: Mapping[str, object]) -> None:
        try:
            await self._api.patch_card(
                message_id, card=card, timeout=_CARD_UPDATE_TIMEOUT_SECONDS
            )
        except (LarkApiError, LarkTransportError) as error:
            await self._edit_failed(error)

    async def _edit_failed(self, error: LarkApiError | LarkTransportError) -> None:
        await self._publish(
            "channel.approval.edit_failed",
            {"method": error.method},
            AuditError(kind=type(error).__name__, message=str(error)),
        )

    def _spawn(self, coroutine: Coroutine[object, object, None]) -> None:
        task = self._group.create_task(coroutine, name="bcn-lark-approval-card")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)


def _card(
    request: ChannelApprovalRequest, token: str, *, status: str | None = None
) -> dict[str, object]:
    """Draw the card: its buttons while pending, its end once decided."""

    approval = request.approval
    action = TEXT.get(
        f"approval.action.{approval.action}", approval.action.replace("_", " ")
    )
    details = approval.details
    description = "\n".join(
        f"{label}{details[key]}"
        for key, label in (
            ("reason", ""),
            ("command", "Command: "),
            ("grant_root", "Grant root: "),
            ("cwd", "Working directory: "),
            ("tool_name", "Tool: "),
            ("tool_input", "Input: "),
            ("permissions", "Permissions: "),
        )
        if details.get(key)
    ).strip()
    if len(description) > 4000:
        description = description[:3999] + "…"
    markdown = f"**Action:** {action}" + (f"\n\n{description}" if description else "")
    elements: list[dict[str, object]] = [{"tag": "markdown", "content": markdown}]
    if status is None:
        elements.append(
            {
                "tag": "column_set",
                "columns": [
                    _button(
                        "primary", TEXT["approval.button.approve"], "approve", token
                    ),
                    _button("danger", TEXT["approval.button.reject"], "reject", token),
                ],
            }
        )
    else:
        elements.append({"tag": "markdown", "content": status})
    return {
        "schema": "2.0",
        "config": {"update_multi": True},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": TEXT["approval.prompt.title"]},
        },
        "body": {"elements": elements},
    }


def _button(kind: str, label: str, action: str, token: str) -> dict[str, object]:
    return {
        "tag": "column",
        "elements": [
            {
                "tag": "button",
                "type": kind,
                "text": {"tag": "plain_text", "content": label},
                "behaviors": [
                    {"type": "callback", "value": {"action": action, "token": token}}
                ],
            }
        ],
    }


def _toast(text: str, kind: str) -> bytes:
    toast = json.dumps(
        {"toast": {"type": kind, "content": text}},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return json.dumps(
        {"code": 200, "data": base64.b64encode(toast).decode("ascii")},
        separators=(",", ":"),
    ).encode("utf-8")


def _parse_callback(
    payload: Mapping[str, object],
) -> tuple[str, str, str, str, str, str] | None:
    event = payload.get("event")
    if not isinstance(event, Mapping):
        return None
    operator = event.get("operator")
    context = event.get("context")
    action = event.get("action")
    if not isinstance(operator, Mapping) or not isinstance(context, Mapping):
        return None
    if not isinstance(action, Mapping) or action.get("tag") != "button":
        return None
    value = action.get("value")
    if not isinstance(value, Mapping):
        return None
    fields = (
        provider_text(operator.get("open_id")),
        provider_text(context.get("open_chat_id")),
        provider_text(context.get("open_message_id")),
        provider_text(value.get("action")),
        provider_text(value.get("token")),
        provider_text(event.get("token")),
    )
    if any(item is None for item in fields):
        return None
    operator_id, chat_id, message_id, action_name, token, update_token = fields
    assert operator_id and chat_id and message_id and action_name and token
    assert update_token
    return operator_id, chat_id, message_id, action_name, token, update_token


__all__ = ["TEXT", "LarkApprovals"]
