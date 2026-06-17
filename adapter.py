"""
Open WebUI Platform Adapter for Hermes Agent — multi-account edition.

One adapter instance manages N bot accounts, each with its own email/password,
Socket.IO connection, and channel list.  Mirrors the multi-account design of
openclaw's open-webui-multi plugin.

Configuration (config.yaml platforms.open-webui.extra):

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
            - yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy

Environment variable fallback (single-account, backward-compatible):

    OPENWEBUI_URL=http://192.168.1.100:8082
    OPENWEBUI_EMAIL=mainbot@local.com
    OPENWEBUI_PASSWORD=yourpassword
    OPENWEBUI_CHANNEL_IDS=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    OPENWEBUI_REQUIRE_MENTION=false

Multi-account via env vars:

    OPENWEBUI_URL=http://192.168.1.100:8082
    OPENWEBUI_ACCOUNTS=main,assistant
    OPENWEBUI_MAIN_EMAIL=mainbot@local.com
    OPENWEBUI_MAIN_PASSWORD=yourpassword
    OPENWEBUI_MAIN_CHANNEL_IDS=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    OPENWEBUI_ASSISTANT_EMAIL=assistant@local.com
    OPENWEBUI_ASSISTANT_PASSWORD=yourpassword
    OPENWEBUI_ASSISTANT_CHANNEL_IDS=yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode, urlparse

import aiohttp

logger = logging.getLogger(__name__)

from gateway.platforms.base import (
    BasePlatformAdapter,
    SendResult,
    MessageEvent,
    MessageType,
    resolve_channel_prompt,
    cache_image_from_bytes,
)
from gateway.config import PlatformConfig, Platform


# ── Engine.IO / Socket.IO packet-type constants ───────────────────────────────
_EIO_OPEN    = "0"
_EIO_CLOSE   = "1"
_EIO_PING    = "2"
_EIO_PONG    = "3"
_EIO_MESSAGE = "4"
_SIO_CONNECT       = "0"
_SIO_DISCONNECT    = "1"
_SIO_EVENT         = "2"
_SIO_CONNECT_ERROR = "4"


# ── Per-account connection state ──────────────────────────────────────────────

@dataclass
class _AccountConn:
    """Holds all runtime state for a single Open WebUI bot account."""

    account_id: str
    base_url: str          # shared with other accounts
    email: str
    password: str
    channel_ids: List[str]
    require_mention: bool

    # Auth (populated after login)
    token: Optional[str] = field(default=None, repr=False)
    user_id: Optional[str] = field(default=None)

    # WebSocket
    ws: Optional[aiohttp.ClientWebSocketResponse] = field(default=None, repr=False)
    sid: Optional[str] = field(default=None)
    ping_interval: float = field(default=25.0)

    # Background tasks
    recv_task: Optional[asyncio.Task] = field(default=None, repr=False)
    heartbeat_task: Optional[asyncio.Task] = field(default=None, repr=False)

    # (no per-account keepalive tasks needed — base._keep_typing handles periodic refresh)

    @property
    def label(self) -> str:
        return f"open-webui[{self.account_id}]"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_channel_ids(raw) -> List[str]:
    if isinstance(raw, list):
        return [str(c).strip() for c in raw if str(c).strip()]
    return [c.strip() for c in str(raw or "").split(",") if c.strip()]


def _extract_channel_id(chat_id: str) -> str:
    """Strip optional ':parent_id' suffix from a peer_id to get the channel UUID."""
    return chat_id.split(":")[0]


def _content_type_to_ext(content_type: str) -> str:
    ct = content_type.split(";")[0].strip().lower()
    return {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/gif": ".gif",
        "image/webp": ".webp",
        "image/bmp": ".bmp",
        "image/tiff": ".tiff",
    }.get(ct, ".jpg")


# ── Main adapter ──────────────────────────────────────────────────────────────

class OpenWebUIAdapter(BasePlatformAdapter):
    """
    Multi-account Open WebUI adapter.

    Each named account in ``extra.accounts`` (or via env vars) gets its own
    login token and WebSocket connection.  All accounts share one aiohttp
    ClientSession for HTTP calls.
    """

    def __init__(self, config, **kwargs):
        platform = Platform("open-webui")
        super().__init__(config=config, platform=platform)

        extra: Dict[str, Any] = getattr(config, "extra", {}) or {}

        self.base_url: str = (
            os.getenv("OPENWEBUI_URL") or extra.get("base_url") or extra.get("url", "")
        ).rstrip("/")

        # Build account list from config
        self._accounts: Dict[str, _AccountConn] = {}
        self._build_accounts(extra)

        # channel_id → _AccountConn for send/edit/delete routing
        self._channel_to_conn: Dict[str, _AccountConn] = {}

        # Shared HTTP session (created in connect())
        self._session: Optional[aiohttp.ClientSession] = None

    # ── Account config parsing ────────────────────────────────────────────────

    def _build_accounts(self, extra: Dict[str, Any]) -> None:
        """Populate self._accounts from config.yaml extra: or env vars."""

        accounts_cfg: Dict[str, Any] = extra.get("accounts") or {}

        if accounts_cfg:
            # ── Multi-account via config.yaml extra.accounts ──────────────────
            for acct_id, acct in accounts_cfg.items():
                if not isinstance(acct, dict):
                    continue
                email = acct.get("email", "")
                password = acct.get("password", "")
                if not (email and password):
                    logger.warning("open-webui: account '%s' missing email/password, skipped", acct_id)
                    continue
                self._accounts[acct_id] = _AccountConn(
                    account_id=acct_id,
                    base_url=self.base_url,
                    email=email,
                    password=password,
                    channel_ids=_parse_channel_ids(acct.get("channel_ids") or acct.get("channelIds", [])),
                    require_mention=bool(acct.get("require_mention", False)),
                )
            return

        # ── Multi-account via env vars ────────────────────────────────────────
        accts_env = os.getenv("OPENWEBUI_ACCOUNTS", "").strip()
        if accts_env:
            for acct_id in [a.strip() for a in accts_env.split(",") if a.strip()]:
                prefix = f"OPENWEBUI_{acct_id.upper()}_"
                email = os.getenv(f"{prefix}EMAIL", "")
                password = os.getenv(f"{prefix}PASSWORD", "")
                if not (email and password):
                    logger.warning("open-webui: env account '%s' missing email/password, skipped", acct_id)
                    continue
                self._accounts[acct_id] = _AccountConn(
                    account_id=acct_id,
                    base_url=self.base_url,
                    email=email,
                    password=password,
                    channel_ids=_parse_channel_ids(os.getenv(f"{prefix}CHANNEL_IDS", "")),
                    require_mention=(
                        os.getenv(f"{prefix}REQUIRE_MENTION", "").lower() in ("1", "true", "yes")
                    ),
                )
            return

        # ── Single-account fallback (env vars / top-level extra keys) ─────────
        email = os.getenv("OPENWEBUI_EMAIL") or extra.get("email", "")
        password = os.getenv("OPENWEBUI_PASSWORD") or extra.get("password", "")
        if email and password:
            self._accounts["default"] = _AccountConn(
                account_id="default",
                base_url=self.base_url,
                email=email,
                password=password,
                channel_ids=_parse_channel_ids(
                    os.getenv("OPENWEBUI_CHANNEL_IDS") or extra.get("channel_ids") or extra.get("channelIds", "")
                ),
                require_mention=(
                    os.getenv("OPENWEBUI_REQUIRE_MENTION", "").lower() in ("1", "true", "yes")
                    or bool(extra.get("require_mention", False))
                ),
            )

    @property
    def name(self) -> str:
        return "Open WebUI"

    # ── Connection lifecycle ──────────────────────────────────────────────────

    async def connect(self) -> bool:
        if not self.base_url:
            logger.error("open-webui: OPENWEBUI_URL / extra.base_url must be set")
            self._set_fatal_error("config_missing", "OPENWEBUI_URL not set", retryable=False)
            return False

        if not self._accounts:
            logger.error("open-webui: no accounts configured")
            self._set_fatal_error("config_missing", "No accounts configured", retryable=False)
            return False

        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()

        connected = 0
        for conn in self._accounts.values():
            if await self._connect_account(conn):
                connected += 1

        if connected == 0:
            self._set_fatal_error("all_accounts_failed", "No accounts connected", retryable=True)
            return False

        # Build channel→conn routing map
        self._channel_to_conn.clear()
        for conn in self._accounts.values():
            if conn.token:  # only connected accounts
                for ch_id in conn.channel_ids:
                    self._channel_to_conn[ch_id] = conn

        self._mark_connected()
        logger.info(
            "open-webui: %d/%d account(s) connected, routing %d channel(s)",
            connected, len(self._accounts), len(self._channel_to_conn),
        )
        return True

    async def _connect_account(self, conn: _AccountConn) -> bool:
        """Login + open WebSocket for a single account."""
        # 1. REST login
        if not await self._login(conn):
            return False

        # 2. Open WebSocket
        parsed = urlparse(self.base_url)
        ws_scheme = "wss" if parsed.scheme == "https" else "ws"
        ws_url = f"{ws_scheme}://{parsed.netloc}/ws/socket.io/?{urlencode({'EIO': 4, 'transport': 'websocket'})}"

        try:
            conn.ws = await self._session.ws_connect(
                ws_url,
                heartbeat=None,
                timeout=aiohttp.ClientWSTimeout(ws_close=30.0),
            )
        except Exception as exc:
            logger.error("%s: WebSocket connect failed: %s", conn.label, exc)
            return False

        # 3. Engine.IO OPEN handshake
        try:
            msg = await asyncio.wait_for(conn.ws.receive(), timeout=15.0)
            if msg.type != aiohttp.WSMsgType.TEXT or not msg.data.startswith(_EIO_OPEN):
                raise ValueError(f"Unexpected frame: {msg.data[:60]}")
            hs = json.loads(msg.data[1:])
            conn.sid = hs.get("sid")
            conn.ping_interval = hs.get("pingInterval", 25000) / 1000
        except Exception as exc:
            logger.error("%s: EIO handshake failed: %s", conn.label, exc)
            await conn.ws.close()
            conn.ws = None
            return False

        # 4. Socket.IO CONNECT with auth
        await self._ws_send(conn, f'{_EIO_MESSAGE}{_SIO_CONNECT}{{"token":"{conn.token}"}}')

        # 5. Wait for SIO CONNECT ack
        try:
            msg = await asyncio.wait_for(conn.ws.receive(), timeout=15.0)
            raw = msg.data if msg.type == aiohttp.WSMsgType.TEXT else ""
            if not (raw.startswith(_EIO_MESSAGE) and len(raw) > 1 and raw[1] == _SIO_CONNECT):
                raise ValueError(f"Expected SIO CONNECT ack, got: {raw[:60]}")
        except Exception as exc:
            logger.error("%s: SIO CONNECT ack failed: %s", conn.label, exc)
            await conn.ws.close()
            conn.ws = None
            return False

        # 6. Join user + channels
        auth_blob = json.dumps({"auth": {"token": conn.token}})
        await self._ws_send(conn, f'42["user-join",{auth_blob}]')
        await self._ws_send(conn, f'42["join-channels",{auth_blob}]')

        # 7. Start background tasks
        conn.recv_task = asyncio.create_task(self._receive_loop(conn))
        conn.heartbeat_task = asyncio.create_task(self._heartbeat_loop(conn))

        logger.info(
            "%s: connected (user_id=%s, channels=%s)",
            conn.label, conn.user_id,
            conn.channel_ids if conn.channel_ids else "all",
        )
        return True

    async def disconnect(self) -> None:
        self._mark_disconnected()
        for conn in self._accounts.values():
            await self._disconnect_account(conn)
        self._channel_to_conn.clear()

        if self._session and not self._session.closed:
            try:
                await self._session.close()
            except Exception:
                pass
        self._session = None

    async def _disconnect_account(self, conn: _AccountConn) -> None:
        # Cancel background tasks
        for task in [conn.recv_task, conn.heartbeat_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        conn.recv_task = None
        conn.heartbeat_task = None

        if conn.ws and not conn.ws.closed:
            try:
                await conn.ws.close()
            except Exception:
                pass
        conn.ws = None
        conn.token = None
        conn.user_id = None
        conn.sid = None

    # ── Auth ──────────────────────────────────────────────────────────────────

    async def _login(self, conn: _AccountConn) -> bool:
        try:
            async with self._session.post(
                f"{self.base_url}/api/v1/auths/signin",
                json={"email": conn.email, "password": conn.password},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if not resp.ok:
                    body = await resp.text()
                    logger.error("%s: login failed (%s): %s", conn.label, resp.status, body[:200])
                    return False
                data = await resp.json()
                conn.token = data["token"]
                conn.user_id = data["id"]
                logger.info("%s: logged in as %s (%s)", conn.label, data.get("name", conn.email), conn.user_id)
                return True
        except Exception as exc:
            logger.error("%s: login error: %s", conn.label, exc)
            return False

    # ── Routing helpers ───────────────────────────────────────────────────────

    def _conn_for_channel(self, chat_id: str) -> Optional[_AccountConn]:
        """Return the account connection responsible for a given chat_id / channel_id."""
        ch_id = _extract_channel_id(chat_id)
        return self._channel_to_conn.get(ch_id)

    # ── Sending ───────────────────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        conn = self._conn_for_channel(chat_id)
        if conn is None:
            logger.warning("open-webui: send() — no account for chat_id=%s", chat_id)
            return SendResult(success=False, error=f"No account owns channel {chat_id}")

        if not self._session or self._session.closed or not conn.token:
            return SendResult(success=False, error="Not connected")

        ch_id = _extract_channel_id(chat_id)
        body: Dict[str, Any] = {"content": content}
        if reply_to:
            body["reply_to_id"] = reply_to
        if metadata and metadata.get("parent_id"):
            body["parent_id"] = metadata["parent_id"]

        try:
            async with self._session.post(
                f"{self.base_url}/api/v1/channels/{ch_id}/messages/post",
                json=body,
                headers={"Authorization": f"Bearer {conn.token}"},
                timeout=aiohttp.ClientTimeout(total=60),
            ) as resp:
                if not resp.ok:
                    text = await resp.text()
                    logger.error("%s: send failed (%s): %s", conn.label, resp.status, text[:200])
                    return SendResult(success=False, error=f"HTTP {resp.status}")
                data = await resp.json()
                return SendResult(success=True, message_id=data.get("id", ""))
        except Exception as exc:
            logger.error("%s: send error: %s", conn.label, exc)
            return SendResult(success=False, error=str(exc))

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """Edit an existing message (used for streaming token-by-token updates)."""
        conn = self._conn_for_channel(chat_id)
        if conn is None or not conn.token:
            return SendResult(success=False, error="No account / not connected")

        ch_id = _extract_channel_id(chat_id)
        try:
            async with self._session.put(
                f"{self.base_url}/api/v1/channels/{ch_id}/messages/{message_id}",
                json={"content": content},
                headers={"Authorization": f"Bearer {conn.token}"},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if not resp.ok:
                    return SendResult(success=False, error=f"HTTP {resp.status}")
                return SendResult(success=True, message_id=message_id)
        except Exception as exc:
            logger.debug("%s: edit_message error: %s", conn.label, exc)
            return SendResult(success=False, error=str(exc))

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        """Delete a message (cleans up interim status messages)."""
        conn = self._conn_for_channel(chat_id)
        if conn is None or not conn.token:
            return False

        ch_id = _extract_channel_id(chat_id)
        try:
            async with self._session.delete(
                f"{self.base_url}/api/v1/channels/{ch_id}/messages/{message_id}",
                headers={"Authorization": f"Bearer {conn.token}"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                return resp.ok
        except Exception as exc:
            logger.debug("%s: delete_message error: %s", conn.label if conn else "?", exc)
            return False

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Called by base._keep_typing every ~2s while processing."""
        conn = self._conn_for_channel(chat_id)
        if conn is None:
            return
        await self._emit_typing(conn, chat_id, True)

    async def stop_typing(self, chat_id: str) -> None:
        """Called by base._keep_typing finally block when processing ends."""
        conn = self._conn_for_channel(chat_id)
        if conn is None:
            return
        await self._emit_typing(conn, chat_id, False)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": _extract_channel_id(chat_id), "type": "channel"}

    # ── Typing helpers ────────────────────────────────────────────────────────

    async def _emit_typing(self, conn: _AccountConn, chat_id: str, typing: bool) -> None:
        if not conn.ws or conn.ws.closed:
            return
        ch_id = _extract_channel_id(chat_id)
        payload = json.dumps([
            "events:channel",
            {
                "channel_id": ch_id,
                "message_id": None,
                "data": {"type": "typing", "data": {"typing": typing}},
            },
        ])
        await self._ws_send(conn, f"42{payload}")

    # ── Background tasks (per account) ───────────────────────────────────────

    async def _heartbeat_loop(self, conn: _AccountConn) -> None:
        try:
            while True:
                await asyncio.sleep(30.0)
                await self._ws_send(conn, '42["heartbeat",{}]')
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("%s: heartbeat error: %s", conn.label, exc)

    async def _receive_loop(self, conn: _AccountConn) -> None:
        try:
            async for msg in conn.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_raw(conn, msg.data)
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                    logger.warning("%s: WebSocket closed/error (%s)", conn.label, msg.data)
                    break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("%s: receive loop error: %s", conn.label, exc)
        finally:
            if self.is_connected:
                logger.warning("%s: connection lost", conn.label)
                # Attempt reconnect for this account only
                asyncio.create_task(self._reconnect_account(conn))

    async def _reconnect_account(self, conn: _AccountConn) -> None:
        """Re-establish a single account's WebSocket after an unexpected drop.

        Retries with exponential backoff (5s → 10s → 20s → … capped at 60s)
        until the connection succeeds, so a temporary Open WebUI restart does
        not leave the account permanently offline.
        """
        # Cancel the orphaned heartbeat from the old connection before we start
        # creating new tasks, otherwise it leaks as a zombie.
        old_hb = conn.heartbeat_task
        if old_hb and not old_hb.done():
            old_hb.cancel()
            try:
                await old_hb
            except (asyncio.CancelledError, Exception):
                pass
        conn.heartbeat_task = None

        delay = 5.0
        max_delay = 60.0
        attempt = 0

        while self.is_connected:
            await asyncio.sleep(delay)
            attempt += 1
            logger.info("%s: reconnect attempt %d (delay was %.0fs)...", conn.label, attempt, delay)

            if await self._connect_account(conn):
                for ch_id in conn.channel_ids:
                    self._channel_to_conn[ch_id] = conn
                logger.info("%s: reconnected after %d attempt(s)", conn.label, attempt)
                return

            delay = min(delay * 2, max_delay)
            logger.warning(
                "%s: reconnect attempt %d failed, retrying in %.0fs",
                conn.label, attempt, delay,
            )

    # ── Frame parsing (per account) ───────────────────────────────────────────

    async def _handle_raw(self, conn: _AccountConn, raw: str) -> None:
        if not raw:
            return

        eio = raw[0]

        if eio == _EIO_PING:
            await self._ws_send(conn, _EIO_PONG + raw[1:])
            return
        if eio == _EIO_CLOSE:
            return
        if eio != _EIO_MESSAGE:
            return

        rest = raw[1:]
        if not rest:
            return
        sio = rest[0]

        if sio == _SIO_CONNECT:
            # Reconnected at SIO level — re-join rooms
            auth_blob = json.dumps({"auth": {"token": conn.token}})
            await self._ws_send(conn, f'42["user-join",{auth_blob}]')
            await self._ws_send(conn, f'42["join-channels",{auth_blob}]')
            return

        if sio == _SIO_CONNECT_ERROR:
            logger.error("%s: SIO CONNECT_ERROR: %s", conn.label, rest[1:100])
            return

        if sio == _SIO_EVENT:
            payload_str = rest[1:]
            while payload_str and payload_str[0].isdigit():
                payload_str = payload_str[1:]
            try:
                packet = json.loads(payload_str)
            except json.JSONDecodeError:
                return
            if isinstance(packet, list) and len(packet) >= 2 and packet[0] == "events:channel":
                await self._handle_channel_event(conn, packet[1])

    async def _handle_channel_event(self, conn: _AccountConn, event: Any) -> None:
        if not isinstance(event, dict):
            return

        data_block = event.get("data") or {}
        if data_block.get("type") != "message":
            return

        message = data_block.get("data")
        if not message or not isinstance(message, dict):
            return

        # Ignore own messages
        if message.get("user_id") == conn.user_id:
            return

        channel_id: str = event.get("channel_id", "")
        channel_meta = event.get("channel") or {}
        channel_type = channel_meta.get("type")
        is_dm = channel_type == "dm"

        # Channel filter (DMs bypass the filter)
        if not is_dm and conn.channel_ids and channel_id not in conn.channel_ids:
            return

        # Register this channel in the routing map (covers DMs and unfiltered channels)
        self._channel_to_conn.setdefault(channel_id, conn)

        text: str = (message.get("content") or "").strip()
        media_urls, media_types = await self._fetch_media(conn, message)

        # Mention check
        if conn.require_mention and not is_dm and conn.user_id:
            if f"<@U:{conn.user_id}" not in text:
                return

        user_block = event.get("user") or {}
        user_id: str = user_block.get("id") or message.get("user_id", "")
        sender_name: str = user_block.get("name") or user_id or "unknown"
        message_id: str = message.get("id") or str(int(time.time() * 1000))
        parent_id: Optional[str] = message.get("parent_id")

        logger.info(
            "%s: message from %s in channel %s",
            conn.label, sender_name, channel_id,
        )

        if is_dm:
            chat_type = "dm"
        elif channel_type == "standard":
            chat_type = "channel"
        else:
            chat_type = "group"

        # Thread-scoped peer_id (same logic as open-webui-multi)
        peer_id = f"{channel_id}:{parent_id}" if parent_id else channel_id

        source = self.build_source(
            chat_id=peer_id,
            chat_name=channel_id,
            chat_type=chat_type,
            user_id=user_id,
            user_name=sender_name,
        )

        channel_prompt = resolve_channel_prompt(
            getattr(self.config, "extra", {}) or {},
            channel_id,
            parent_id or None,
        )

        evt = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=message_id,
            timestamp=datetime.datetime.now(),
            reply_to_message_id=message_id,
            channel_prompt=channel_prompt,
            media_urls=media_urls,
            media_types=media_types,
            raw_message={
                "account_id": conn.account_id,
                "channel_id": channel_id,
                "message_id": message_id,
                "parent_id": parent_id,
            },
        )

        await self.handle_message(evt)

    # ── Media helpers ─────────────────────────────────────────────────────────

    async def _fetch_media(self, conn: _AccountConn, message: dict):
        """Download image attachments from Open WebUI.

        Returns (paths, mime_types) — parallel lists for MessageEvent.media_urls
        and MessageEvent.media_types.
        """
        files = (message.get("data") or {}).get("files") or []
        if not files:
            return [], []

        media_paths: List[str] = []
        media_types: List[str] = []
        for entry in files:
            if not isinstance(entry, dict):
                continue
            file_info = entry.get("file") or {}
            file_id = file_info.get("id")
            if not file_id:
                continue

            # Prefer meta.content_type from the message data (no extra request)
            meta_ct = (file_info.get("meta") or {}).get("content_type", "")

            url = f"{self.base_url}/api/v1/files/{file_id}/content"
            try:
                async with self._session.get(
                    url,
                    headers={"Authorization": f"Bearer {conn.token}"},
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if not resp.ok:
                        logger.warning(
                            "%s: file download failed (%s) for id=%s",
                            conn.label, resp.status, file_id,
                        )
                        continue
                    content_type = resp.headers.get("Content-Type", "") or meta_ct
                    if not content_type.startswith("image/"):
                        continue
                    ext = _content_type_to_ext(content_type)
                    data = await resp.read()
                    path = cache_image_from_bytes(data, ext)
                    media_paths.append(path)
                    media_types.append(content_type.split(";")[0].strip())
                    logger.info("%s: cached image %s → %s", conn.label, file_id, path)
            except Exception as exc:
                logger.warning("%s: error downloading file %s: %s", conn.label, file_id, exc)

        return media_paths, media_types

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _ws_send(self, conn: _AccountConn, text: str) -> None:
        try:
            if conn.ws and not conn.ws.closed:
                await conn.ws.send_str(text)
        except Exception as exc:
            logger.debug("%s: ws_send error: %s", conn.label, exc)


# ── Plugin registration helpers ───────────────────────────────────────────────

def _load_extra(config=None) -> Dict[str, Any]:
    return getattr(config, "extra", {}) or {}


def check_requirements() -> bool:
    extra = {}
    url = os.getenv("OPENWEBUI_URL") or extra.get("base_url", "")
    # Minimal: URL + at least one account credential path
    if not url:
        return False
    return bool(
        os.getenv("OPENWEBUI_EMAIL")
        or os.getenv("OPENWEBUI_ACCOUNTS")
    )


def validate_config(config) -> bool:
    extra = _load_extra(config)
    url = os.getenv("OPENWEBUI_URL") or extra.get("base_url") or extra.get("url", "")
    if not url:
        return False
    accounts_cfg = extra.get("accounts") or {}
    if accounts_cfg:
        return any(
            a.get("email") and a.get("password")
            for a in accounts_cfg.values()
            if isinstance(a, dict)
        )
    return bool(os.getenv("OPENWEBUI_EMAIL") or os.getenv("OPENWEBUI_ACCOUNTS"))


def is_connected(config) -> bool:
    return validate_config(config)


def _env_enablement(config=None) -> dict | None:
    """Seed PlatformConfig.extra from env vars so gateway status reflects env-only config."""
    url = os.getenv("OPENWEBUI_URL", "").strip()
    if not url:
        return None

    seed: Dict[str, Any] = {"base_url": url}

    # Do NOT inject accounts into the seed — gateway does extra.update(seed),
    # which would overwrite any accounts already in config.yaml.
    # _build_accounts() reads OPENWEBUI_ACCOUNTS / OPENWEBUI_EMAIL directly.
    #
    # Only set home_channel for cron delivery routing.
    accts_env = os.getenv("OPENWEBUI_ACCOUNTS", "").strip()
    if accts_env:
        first_acct = accts_env.split(",")[0].strip()
        prefix = f"OPENWEBUI_{first_acct.upper()}_"
        first_channels = _parse_channel_ids(os.getenv(f"{prefix}CHANNEL_IDS", ""))
        if first_channels:
            seed["home_channel"] = {"chat_id": first_channels[0], "name": first_channels[0]}
        return seed

    channel_ids = _parse_channel_ids(os.getenv("OPENWEBUI_CHANNEL_IDS", ""))
    if channel_ids:
        seed["home_channel"] = {"chat_id": channel_ids[0], "name": channel_ids[0]}
    return seed


def register(ctx):
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="open-webui",
        label="Open WebUI",
        adapter_factory=lambda cfg: OpenWebUIAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["OPENWEBUI_URL"],
        install_hint="Requires aiohttp (already in Hermes venv). No extra packages needed.",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="OPENWEBUI_CHANNEL_IDS",
        emoji="🌐",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=(
            "You are chatting via Open WebUI channels. "
            "Markdown formatting is supported and will be rendered. "
            "Keep responses helpful and conversational."
        ),
    )
