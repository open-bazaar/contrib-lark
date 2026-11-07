"""The configuration form the server draws from the schema."""

from bazaar_contrib_lark import Config


def test_every_field_and_region_is_worded_in_both_languages():
    schema = Config.model_json_schema()
    assert {"en", "zh-CN"} <= schema["x-locales"].keys()
    for texts in schema["x-locales"].values():
        for name in schema["properties"]:
            assert texts[f"{name}.title"] and texts[f"{name}.description"]
        for region in schema["properties"]["region"]["enum"]:
            assert texts[f"region.{region}"]
        assert texts["app_secret.empty"]
    assert schema["properties"]["app_secret"]["writeOnly"] is True
