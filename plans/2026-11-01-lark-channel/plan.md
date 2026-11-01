# Lark Channel provider

Migrate the Lark implementation from BCN `v0.2.3`, commit
`6f5c9927865b9f945bb1502b711068244ef5af03`, under
`src/bazaar_compute_node/contrib/lark/`, into this independent provider. Use the
published core, audit, channel, harness, timer, messages and collaboration
contracts from `0.3.0rc13`. All implementation, tests, metadata and
documentation belong to this repository.

## Task 1: Provider implementation and package validation

Register `lark-builder` through `ChannelBuilderConfig`, `CHANNEL_BUILDERS` and
`channel_builder_key("lark")`. Accept `app_id`, an optional `region` of
`feishu`, the default, or `lark`, and either `app_secret_env` or a direct
`app_secret` from the configer; a supplied `app_secret` takes precedence. The
selected Channel owns its aiohttp session, long connection and tasks in its
contrib lifespan, accepts `CHANNEL_INPUT`, `TIMER`, `AUDIT` and `MESSAGES`,
listens to `CONTACT_REVIEWED`, and offers `CHANNEL` after connecting. Retain the
existing request and lifecycle budgets through the public Timer contract.

Preserve the long connection: the endpoint request, protobuf frames declared
with `protobug`, heartbeats and the client configuration a pong carries,
fragment reassembly, acknowledgements, and reconnection with its nonce,
interval and count. Preserve tenant token refresh ahead of expiry and the bot
identity from its `open_id` and app name. In conversations, preserve direct
identities keyed by the peer, group identities keyed by chat and topic thread,
stable message IDs, text, rich posts, images, files, audio, video and stickers
with their placeholders, mention rendering, every group message mentioning the
agent, sender names from the contact directory and group names from chat
information, quote backfill through the parent message, Markdown sent as post
`md` nodes and split at 3,500 code points with fenced blocks closed and
reopened, replies inside a topic through `reply_in_thread`, image and file
uploads, proactive DM addressing, receipts, and interactive card approvals
bound to the original sender, chat and card, answered within three seconds
with a toast and redrawn with their decision and no buttons. Conversations
stored by 0.2.3 keep their identities.
Convert addressing and attachments at the existing public Channel boundary.
`prepare_send` returns individual message operations; the existing shared
sender owns retry policy and receipts. Translate Lark error codes and transport
information into public Channel errors. Keep the original English approval
text.

Changes from 0.2.3:

- A topic follows its group's review. A topic opens approved when its group's
  main conversation is approved and pending otherwise, whichever message opens
  it, a quoted one included; a topic opened while the group is pending is
  reviewed on its own. The Channel reads the group conversation's review
  through `MESSAGES` and keeps it per group in memory. `CONTACT_REVIEWED` for
  a group conversation of this agent drops that entry, so the next topic
  reads it again.
- Review rests on received messages alone: any message in a group's main
  conversation opens it for review. When a topic's first event arrives in a
  group that has no conversation yet, as in a group whose messages all live in
  topics, the group's main conversation first receives one message with a
  fixed provider identity, distinct per group, and the group's name. That
  message notifies the runtime, so a configured `review.reply` is posted into
  the group; after approval the agent reads it once. Lark never sent it, so a
  send answering it posts into the group and the read marker passes it by.
- The rows of a rich post read as separate lines; 0.2.3 joined them with
  nothing between.
- A message sent into a topic without one to answer, such as a review reply,
  answers the topic's root message in the topic, since Lark posts into a topic
  only as a reply; 0.2.3 refused such a send.
- A received image, file, audio or video is downloaded by the node once the
  conversation is approved, as BCN 0.3 does for every Channel. Its name and
  type still come from the event and from the download's `Content-Disposition`
  and `Content-Type` under the 0.2.3 rules, read on receipt by a request that
  leaves the body unread. The provider no longer caches materialized resources
  and no longer caps them at 25 MB.
- A read marker replaces the moving Typing reaction, since every reaction
  notifies the app. The `channel` contrib listens to `CHANNEL_TURN` instead of
  receiving `accept_turn_event`; the first event of a turn adds one `Get`
  reaction to the message the turn answers, the target's
  `reply_to_message_id`, and the reaction stays.
- The read marker, card updates and the sender and group name lookups get the
  ten seconds 0.2.3 gave a parent fetch, instead of its three and five, since
  a new connection alone can take five seconds.
- A message operation passes the shared sender's attempt id, which stays the
  same across retries, as Lark's `uuid`, and is marked idempotent. Lark sends
  at most one message per `uuid` within an hour, so the shared sender retries a
  transport failure or a server error even when the request may have reached
  Lark, without posting twice.
- Bounds with no platform basis are removed: the 256-event queue in front of
  the mapper is unbounded and each card callback is answered on its own task,
  so acknowledgements and heartbeats never wait for mapping or approvals, as
  in the WeCom provider; fragment reassembly keeps its five-second expiry
  without the 128-message, 64-part and 8 MiB limits, frame headers and
  WebSocket messages are not size-limited, the post renderer has no depth
  limit, and the client configuration Lark sends is used as sent. A press on
  a card that is no longer pending answers `Approval is no longer valid`, the
  0.2.3 answer for an unknown card, instead of the latest 256 decisions being
  remembered to name each one, since a decided card loses its buttons. Sender
  and group names keep 0.2.3's expiry, a day for a name found and five
  minutes for a failed lookup, without its limit of 256 entries.
- Close an approval card that can no longer be answered, as the Telegram
  provider does. 0.2.3 updates a card only with the token its button callback
  carries, so a card pending at shutdown kept its buttons. Lark updates a card
  the app sent by its message ID, the card already being shared through
  `update_multi`. When the call waiting for the decision is cancelled, as on
  node shutdown, the card loses its buttons and ends with
  `Action rejected: the approval request was cancelled`. When the Channel
  stops with a card pending, the request is still rejected with
  `channel_stopped`, and the card ends with
  `Action rejected: the Lark channel stopped`. Stopping waits for these
  updates within its existing budget.
- Publish provider-internal failures as Audit events instead of the 0.2 health
  counters and logs, which nothing in 0.3 reads:
  `channel.connection.failed`, `channel.message.filtered`,
  `channel.message.reference_unresolved`, `channel.directory.failed`,
  `channel.reaction.failed`, `channel.approval.callback_rejected` and
  `channel.approval.edit_failed`. Each established long connection publishes
  `channel.connection.connected`. Received frames, heartbeats and reactions
  are not published. Inbound, outbound, attachment and approval outcomes come
  from the existing framework events.

Keep README as short as the Telegram provider's setup and configuration
reference. Verify content, addressing, frame encoding and real filesystem
attachments, then independently install both wheel and sdist and enable the
actual registered builder through a real ContribHub with both credential
configuration forms. Enable blockbuster in pytest and the isolated installation
subprocesses. Run Ruff, format, Pyright and three-platform CI.

## Task 2: Real Lark behavior tests

Test the Lark Channel through a real installed node against the real platform,
in the manner of the Telegram and WeCom providers. A scripted TestHarness
stands in for the model and a second app's bot drives the conversation from
the Lark side. Tests are marked `e2e`, run in their own CI job, and read their
configuration from the environment; a missing value fails the run.

- `LARK_TEST_APP_ID` (variable) and `LARK_TEST_APP_SECRET` (secret): the app
  under test.
- `LARK_DRIVER_APP_ID` (variable) and `LARK_DRIVER_APP_SECRET` (secret): the
  driver app.
- `LARK_TEST_CHAT_ID` (variable): the dedicated test group, where both bots
  are members.

The app under test receives `im.message.receive_v1` over the long connection,
where card presses also arrive, and holds
`im:message.group_msg.include_bot:read`, without which Lark withholds other
bots' group messages. The driver app holds `im:message.group_msg`, without
which Lark refuses to list the group's messages.

The fixture package `bcn-lark-test-harness` under `tests/fixtures/harness`
registers `test-harness-builder` with the Telegram provider's sending-only
Harness and worker. For every new message carrying the run marker the worker
sends what the instruction after the marker names: `reply`, `long`, `attach`,
`quote`, `echo` and `approve`, as in the WeCom provider. It reads each message
whole, since a Lark post puts its text and images on separate lines. The
package also offers `audit-journal` and `config-env-token`, which turns each
Lark channel's `app_secret_env` into a direct `app_secret` held only in memory.
The test reads the journal only for what the channel interfaces cannot tell it:
which pending conversation is the test group, the review a topic conversation
opened with, from `channel.inbound.persisted`, and when the Channel has
connected. Each start waits for `channel.connection.connected` before the
driver posts. The node is prepared like the Telegram acceptance and first runs
with `config-filesystem`.

The test script is the driver: it posts and replies in the test group through
the driver app, then reads the group's messages and checks that what arrived
is what the instruction asked for.

1. Review: `<marker> open` from the driver leaves the group conversation
   waiting for review. The test approves it with `bcn agent review approve`.
2. Reply: `<marker> reply` is answered by a reply to it.
3. Quote backfill: `<marker> quote`, a driver reply to an earlier driver
   message, is answered with that earlier message's text.
4. Long rich text: `<marker> long` is answered by several messages whose
   numbered sections, in order, are the whole document.
5. Outbound attachment: `<marker> attach` is answered by a message carrying a
   file whose bytes are the fixture file.
6. Inbound attachment: a driver post carrying an image and `<marker> echo` is
   answered by a message carrying that image.
7. Topic: `<marker> reply`, a driver reply opened as a topic under an earlier
   driver message, is answered inside that topic, and its conversation opened
   approved with the group.
8. Direct secret: after a restart with `config-env-token`, `<marker> reply` in
   the group is answered again.

Checked locally by hand rather than in CI: direct conversations, which need a
person to message the bot, with quote backfill and media there; card approvals
approved, rejected and left pending at shutdown; the read marker; and a group
whose messages all live in topics, with its review. A card pressed by another
member and sender names keep 0.2.3's handling and are not checked by hand.
Record the CI run and the local check as acceptance evidence.
