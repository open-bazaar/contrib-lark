from bazaar_contrib_channel import ChannelTarget

from bazaar_contrib_lark import BuilderConfig
from bazaar_contrib_lark.identity import LarkThreadIdentity


def test_groups_and_topics_require_an_unambiguous_region():
    builder = BuilderConfig.builder()
    first, second, topic = (
        ChannelTarget(
            kind="group",
            thread_id=LarkThreadIdentity(bot, "oc_room", thread).provider_thread_id,
        )
        for bot, thread in (("ou_one", "0"), ("ou_two", "0"), ("ou_two", "omt_thread"))
    )
    domestic = ({"app_id": "app-one"}, {"app_id": "app-two"})
    overseas = ({"app_id": "app-three", "region": "lark"},)
    key = builder.conversation_key(first, domestic)
    assert key is not None
    assert key == builder.conversation_key(second, domestic)
    assert key != builder.conversation_key(topic, domestic)
    assert key != builder.conversation_key(second, overseas)
    assert builder.conversation_key(first, domestic + overseas) is None
    assert builder.conversation_key(first, overseas + domestic) is None
    assert builder.conversation_key(first) is None
    assert (
        builder.conversation_key(first.model_copy(update={"kind": "dm"}), domestic)
        is None
    )
