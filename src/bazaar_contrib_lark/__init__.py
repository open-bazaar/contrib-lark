"""Register Lark and connect each selected agent's Channel."""

from __future__ import annotations

import asyncio
import os
import tomllib
from contextlib import asynccontextmanager
from importlib.resources import files
from pathlib import Path
from typing import Any, ClassVar, Literal, cast

from bazaar_contrib_audit import AUDIT
from bazaar_contrib_channel import (
    CHANNEL,
    CHANNEL_BUILDERS,
    CHANNEL_INPUT,
    CHANNEL_TURN,
    ChannelBuilderConfig,
    ChannelTurn,
    channel_builder_key,
)
from bazaar_contrib_collaboration import CONTACT_REVIEWED, ContactReviewed
from bazaar_contrib_messages import MESSAGES
from bazaar_contrib_timer import TIMER
from bazaar_core import Context, Contrib
from pydantic import BaseModel, ConfigDict, Field

from .channel import LarkChannel

_REGION_BASE_URLS = {
    "feishu": "https://open.feishu.cn",
    "lark": "https://open.larksuite.com",
}


_LOCALES = Path(str(files(__name__))) / "resources" / "locales"


class Config(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        json_schema_extra={
            "x-locales": {
                path.stem: tomllib.loads(path.read_text(encoding="utf-8"))
                for path in sorted(_LOCALES.glob("*.toml"))
            }
        },
    )

    app_id: str = Field(min_length=1)
    app_secret_env: str | None = Field(default=None, min_length=1)
    app_secret: str | None = Field(default=None, json_schema_extra={"writeOnly": True})
    region: Literal["feishu", "lark"] = "feishu"


@asynccontextmanager
async def lifespan(ctx: Context, config: Config):
    secret = config.app_secret
    if secret is None and config.app_secret_env is not None:
        secret = os.environ.get(config.app_secret_env)
    if secret is None or not secret.strip():
        raise ValueError(
            "lark credential is missing: configure app_secret or app_secret_env"
        )
    async with asyncio.TaskGroup() as group:
        instance = LarkChannel(
            ctx.accept(CHANNEL_INPUT),
            ctx.accept(TIMER),
            ctx.accept(AUDIT),
            ctx.accept(MESSAGES),
            group,
            app_id=config.app_id,
            app_secret=secret,
            base_url=_REGION_BASE_URLS[config.region],
        )
        await instance.start(timeout=60)
        ctx.offer(CHANNEL, instance)
        try:
            yield
        finally:
            await instance.stop(timeout=5)


async def reviewed(ctx: Context, event: ContactReviewed) -> None:
    cast(LarkChannel, ctx.accept(CHANNEL)).forget_review(event)


def turn(ctx: Context, value: ChannelTurn) -> None:
    cast(LarkChannel, ctx.accept(CHANNEL)).mark_read(value.event, target=value.target)


channel = Contrib(
    config=Config,
    accepts=[CHANNEL_INPUT, TIMER, AUDIT, MESSAGES],
    offers=[CHANNEL],
    lifespan=lifespan,
    listen={CONTACT_REVIEWED: reviewed, CHANNEL_TURN: turn},
)


class BuilderConfig(ChannelBuilderConfig):
    kind: ClassVar[str] = "lark"
    entity_model: ClassVar[type[BaseModel]] = Config
    entry: ClassVar[Contrib[Any]] = channel


@asynccontextmanager
async def builder_lifespan(ctx: Context, config: BuilderConfig):
    with ctx.accept(CHANNEL_BUILDERS).register(config.builder()) as builder:
        ctx.offer(channel_builder_key(builder.kind), builder)
        yield


builder = Contrib(
    config=BuilderConfig,
    accepts=[CHANNEL_BUILDERS],
    offers=[channel_builder_key("lark")],
    lifespan=builder_lifespan,
)
