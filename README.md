# hermes-openwebui-platform

A [Hermes Agent](https://hermes-agent.com) platform plugin that connects Hermes to [Open WebUI](https://openwebui.com) channels via REST API and Socket.IO.

> As far as I know, this is the only Open WebUI platform plugin for Hermes. If you know of another, please open an issue!

## Features

- **Multi-account support** — run multiple bot accounts from a single adapter instance (A bot → A channel, B bot → B channel)
- **Real-time messaging** — receives and sends messages via Open WebUI's Socket.IO event system
- **Image support** — downloads file attachments from Open WebUI and passes them to the agent as native vision input
- **Typing indicator** — shows "typing..." while the agent is processing, clears when done
- **Edit + delete** — supports `edit_message()` and `delete_message()` for streaming and progress cleanup
- **Thread-aware** — isolates sessions per thread (`channel_id:parent_id`)
- **Auto-reconnect** — reconnects individual accounts on WebSocket drop without restarting the gateway
- **Three config modes** — `config.yaml` multi-account, env var multi-account, single-account env fallback

## Requirements

- [Hermes Agent](https://hermes-agent.com) with gateway enabled
- [Open WebUI](https://openwebui.com) instance (self-hosted)
- `aiohttp` (already bundled in Hermes' venv — no extra install needed)

## Installation

Copy the plugin folder into your Hermes plugins directory:

```bash
cp -r hermes-openwebui-platform ~/.hermes/hermes-agent/plugins/platforms/open-webui
```

Then restart the gateway:

```bash
hermes gateway restart
```

## Configuration

### Option 1 — `config.yaml` (multi-account, recommended)

```yaml
# ~/.hermes/config.yaml
platforms:
  - kind: open-webui-platform
    extra:
      base_url: http://192.168.1.100:8082
      accounts:
        main:
          email: mainbot@local.com
          password: yourpassword
          channel_ids:
            - xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
          require_mention: false
        assistant:
          email: assistant@local.com
          password: yourpassword
          channel_ids:
            - a1b2c3d4-0000-0000-0000-000000000000
          require_mention: false
```

### Option 2 — Environment variables (multi-account)

```bash
# ~/.hermes/.env
OPENWEBUI_URL=http://192.168.1.100:8082
OPENWEBUI_ACCOUNTS=main,assistant

OPENWEBUI_MAIN_EMAIL=mainbot@local.com
OPENWEBUI_MAIN_PASSWORD=yourpassword
OPENWEBUI_MAIN_CHANNEL_IDS=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

OPENWEBUI_ASSISTANT_EMAIL=assistant@local.com
OPENWEBUI_ASSISTANT_PASSWORD=yourpassword
OPENWEBUI_ASSISTANT_CHANNEL_IDS=a1b2c3d4-0000-0000-0000-000000000000
```

### Option 3 — Environment variables (single account)

```bash
OPENWEBUI_URL=http://192.168.1.100:8082
OPENWEBUI_EMAIL=mainbot@local.com
OPENWEBUI_PASSWORD=yourpassword
OPENWEBUI_CHANNEL_IDS=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
OPENWEBUI_REQUIRE_MENTION=false
```

## Finding your Channel ID

In Open WebUI, navigate to your channel and copy the UUID from the URL:

```
http://your-openwebui/channels/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
                                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                                This is your channel_id
```

## Creating a bot account in Open WebUI

1. Go to **Admin Panel → Users → Add User**
2. Set role to **User** (not Admin)
3. Use the email/password in your Hermes config

To allow all users to chat with the bot without pairing, set in `.env`:

```bash
GATEWAY_ALLOW_ALL_USERS=true
```

## Recommended Hermes config

```yaml
# ~/.hermes/config.yaml
streaming:
  enabled: false          # Open WebUI renders complete messages better

display:
  tool_progress: all      # Show tool progress as separate messages
  cleanup_progress: false # Keep progress messages visible (openclaw-style)
  interim_assistant_messages: true

agent:
  image_input_mode: native  # Required for local LLMs (lmstudio, ollama, etc.)
                            # Cloud providers (Anthropic, OpenAI, OpenRouter) work
                            # with the default "auto" and don't need this line.
```

> **Image routing note:** Hermes detects vision capability via [models.dev](https://models.dev).
> Cloud providers are listed there, so `auto` mode works out of the box.
> Local model servers (LM Studio, Ollama, custom OpenAI-compatible endpoints) are not
> in that database, so `auto` falls back to a text-based vision pipeline.
> Set `image_input_mode: native` to skip the lookup and send images directly to your model.

## How it works

```
User message (Open WebUI channel)
        ↓
Socket.IO  events:channel  event
        ↓
adapter._handle_channel_event()
        ↓
Hermes agent loop (tools, reasoning...)
  ├─ send_typing()  → typing indicator via Socket.IO
  ├─ send()         → POST /api/v1/channels/{id}/messages/post
  ├─ edit_message() → PUT  /api/v1/channels/{id}/messages/{msg_id}
  └─ delete_message()→ DELETE /api/v1/channels/{id}/messages/{msg_id}
        ↓
Final response posted to channel
```

The adapter hand-rolls the Engine.IO v4 + Socket.IO v4 WebSocket protocol directly with `aiohttp` — no `python-socketio` dependency required.

## Protocol details

Open WebUI uses Socket.IO v4 over Engine.IO v4. The adapter implements the handshake manually:

| Step | Frame |
|------|-------|
| Server → Client | `0{"sid":"...","pingInterval":25000,...}` (EIO OPEN) |
| Client → Server | `40{"token":"<jwt>"}` (SIO CONNECT with auth) |
| Server → Client | `40{"sid":"..."}` (SIO CONNECT ack) |
| Client → Server | `42["user-join",{"auth":{"token":"..."}}]` |
| Client → Server | `42["join-channels",{"auth":{"token":"..."}}]` |
| Server → Client | `42["events:channel",{...}]` (inbound messages) |
| Client → Server | `42["heartbeat",{}]` every 30s |
| Client → Server | `2` + `3` (EIO PING/PONG) |

## License

MIT
