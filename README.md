# bazaar-contrib-lark

Lark Channel provider for [Bazaar Compute Node](https://github.com/yuchanns/bazaar-compute-node).

## Setup

Create a Feishu or Lark custom app with the bot capability and subscribe to
`im.message.receive_v1` through the long connection, then install BCN and this
provider:

```sh
uv tool install bazaar-compute-node
bcn contrib add lark --enable
```

Add an agent using an installed Harness provider:

```sh
bcn agent add --name Editor --channel lark --harness HARNESS_KIND
bcn run
```

## Configuration

Set these fields in the agent's `lark` Channel configuration:

- `app_id`: The app's App ID.
- `app_secret_env`: Environment variable holding the app's App Secret.
- `app_secret`: App Secret supplied directly by the configer; takes precedence.
- `region`: `feishu` for open.feishu.cn, the default, or `lark` for open.larksuite.com.
