from __future__ import annotations

import json
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from bazaar_contrib_channel import OutboundAttachment

from bazaar_contrib_lark.attachments import resource_name
from bazaar_contrib_lark.content import parse_mentions, project_content
from bazaar_contrib_lark.frame import Frame, Header, decode_frame, encode_frame
from bazaar_contrib_lark.identity import LarkThreadIdentity, parse_provider_thread_id
from bazaar_contrib_lark.markdown import code_points, split_markdown
from bazaar_contrib_lark.outbound import markdown_post_content, prepare_attachments


def test_long_editorial_message_fits_the_lark_limit():
    paragraphs = [
        f"第 {index} 期编辑记录：请保留每份稿件的标题、作者和发布时间。"
        for index in range(1, 400)
    ]
    body = "\n\n".join(paragraphs) + '\n\n```python\nprint("Friday edition")\n```'
    parts = split_markdown(body, limit=3_500, measure=code_points)
    delivered = "\n\n".join(parts)
    for paragraph in paragraphs:
        assert paragraph in delivered
    assert 'print("Friday edition")' in delivered
    assert all(code_points(part) <= 3_500 for part in parts)
    post = json.loads(markdown_post_content(parts[0]))
    assert post["zh_cn"]["content"][0][0] == {"tag": "md", "text": parts[0]}


def test_mentions_read_as_names_and_the_bot_drops_out():
    mentions = parse_mentions(
        [
            {"key": "@_user_1", "id": {"open_id": "ou_editor_bot"}, "name": "Editor"},
            {"key": "@_user_2", "id": {"open_id": "ou_zhangsan"}, "name": "Zhang San"},
        ]
    )
    content = project_content(
        "text",
        json.dumps({"text": "@_user_1 请让 @_user_2 审阅周五的稿件"}),
        mentions=mentions,
        bot_open_id="ou_editor_bot",
    )
    assert "@Zhang San" in content.body
    assert "Editor" not in content.body
    assert "@_user_1" not in content.body


def test_rich_post_keeps_its_title_link_and_image():
    content = project_content(
        "post",
        json.dumps(
            {
                "zh_cn": {
                    "title": "周五刊",
                    "content": [
                        [
                            {"tag": "text", "text": "终稿见 "},
                            {
                                "tag": "a",
                                "text": "排版页",
                                "href": "https://example.com/friday",
                            },
                        ],
                        [{"tag": "text", "text": "请周五前确认"}],
                        [{"tag": "img", "image_key": "img_v3_cover"}],
                    ],
                }
            }
        ),
        mentions={},
        bot_open_id="ou_editor_bot",
    )
    assert "周五刊" in content.body
    assert "排版页 (https://example.com/friday)\n请周五前确认" in content.body
    assert [resource.file_key for resource in content.resources] == ["img_v3_cover"]
    assert content.resources[0].placeholder in content.body


def test_received_file_keeps_its_name():
    content = project_content(
        "file",
        json.dumps({"file_key": "file_v3_draft", "file_name": "friday-draft.pdf"}),
        mentions={},
        bot_open_id="ou_editor_bot",
    )
    (resource,) = content.resources
    assert resource.name == "friday-draft.pdf"
    assert content.body == "[file: friday-draft.pdf]"


def test_resource_names_keep_a_real_suffix():
    assert (
        resource_name(
            event_name="friday-draft.pdf",
            response_name="download.bin",
            media_type="application/octet-stream",
            resource_type="file",
        )
        == "friday-draft.pdf"
    )
    assert (
        resource_name(
            event_name="image",
            response_name=None,
            media_type="image/png",
            resource_type="image",
        )
        == "image.png"
    )
    assert (
        resource_name(
            event_name="audio",
            response_name=None,
            media_type="application/octet-stream",
            resource_type="audio",
        )
        == "audio.mp3"
    )


def test_recorded_frame_decodes_and_round_trips():
    fixture = Path(__file__).with_name("fixtures") / "lark_frame.hex"
    frame = decode_frame(bytes.fromhex(fixture.read_text().strip()))
    assert (frame.SeqID, frame.LogID, frame.service, frame.method) == (1, 2, 3, 0)
    assert frame.headers == [Header(key="type", value="ping")]
    event = Frame(
        SeqID=7,
        LogID=8,
        service=3,
        method=1,
        headers=[Header(key="type", value="event"), Header(key="is_ack", value="")],
        payload=json.dumps({"schema": "2.0"}).encode(),
    )
    assert decode_frame(encode_frame(event)) == event


def test_conversation_identity_matches_what_0_2_3_stored():
    topic = LarkThreadIdentity(
        bot_open_id="ou_editor_bot", chat_id="oc_friday", thread_id="omt_cover"
    )
    assert parse_provider_thread_id(topic.provider_thread_id) == topic
    assert topic.channel_session_id == str(
        uuid5(NAMESPACE_URL, "lark:bot:ou_editor_bot:chat:oc_friday:thread:omt_cover")
    )
    assert topic.main.provider_thread_id.endswith(":thread:0")
    assert topic.message_id("om_1") == topic.main.message_id("om_1")


def test_workspace_attachments_prepare_as_lark_uploads(tmp_path: Path):
    cover = tmp_path / "cover.png"
    cover.write_bytes(b"\x89PNG\r\n\x1a\n" + b"Friday cover" * 64)
    draft = tmp_path / "friday-draft.pdf"
    draft.write_bytes(b"%PDF-1.7 Friday edition draft")
    prepared = prepare_attachments(
        tmp_path,
        (
            OutboundAttachment(name=cover.name, path=cover, media_type="image/png"),
            OutboundAttachment(name=draft.name, path=Path(draft.name)),
        ),
    )
    assert [(item.message_type, item.file_type) for item in prepared] == [
        ("image", "stream"),
        ("file", "pdf"),
    ]
    with prepared[1].open() as file:
        assert b"Friday edition draft" in file.read()
