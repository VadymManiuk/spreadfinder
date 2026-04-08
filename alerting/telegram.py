"""
Telegram Bot API sender with rate limiting and interactive filter controls.

Inputs: Bot token, chat ID, formatted message string.
Outputs: Sends messages via Telegram Bot API with inline keyboard buttons.
Assumptions:
  - Bot token and chat ID come from environment variables.
  - Rate limited to avoid Telegram API throttling.
  - Supports interactive filter buttons AND custom /setmin command.
  - User types spread thresholds in % (e.g., /setmin 2.5 = 2.5% minimum).
"""

import asyncio
import json
import os
import time

import aiohttp
import structlog

from models.snapshot import SpreadOpportunity
from alerting.formatter import format_alert, format_grouped_alert

logger = structlog.get_logger(__name__)

MIN_SEND_INTERVAL_SECONDS = 1.0
TELEGRAM_API_BASE = "https://api.telegram.org"

# Quick-select buttons (in %, not bps)
SPREAD_FILTER_OPTIONS = [
    {"label": "> 1%", "pct": 1.0},
    {"label": "> 2%", "pct": 2.0},
    {"label": "> 3%", "pct": 3.0},
    {"label": "> 5%", "pct": 5.0},
    {"label": "> 10%", "pct": 10.0},
    {"label": "All", "pct": 0.0},
]

# Persistent bottom keyboard buttons
BOTTOM_MENU_KEYBOARD = {
    "keyboard": [
        [{"text": "⚙️ Settings"}, {"text": "📊 Status"}],
        [{"text": "❓ Help"}],
    ],
    "resize_keyboard": True,
    "is_persistent": True,
}


class TelegramSender:
    """
    Sends spread alerts to a Telegram chat via Bot API.
    Supports inline keyboard buttons and /setmin command for custom % filter.

    Usage:
        sender = TelegramSender(bot_token="...", chat_id="...")
        await sender.start_polling()
        await sender.send_alert(opportunity)
        await sender.close()
    """

    def __init__(
        self,
        bot_token: str | None = None,
        chat_id: str | None = None,
        session: aiohttp.ClientSession | None = None,
    ):
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID", "")
        self._session = session
        self._own_session = session is None
        self._last_send_time: float = 0
        self._lock = asyncio.Lock()
        self._polling_task: asyncio.Task | None = None
        self._last_update_id: int = 0

        # Per-chat filter: chat_id -> min net spread in % (e.g. 1.0 = 1%)
        # Default: 1% minimum
        self._chat_filters: dict[str, float] = {}
        self._default_min_pct: float = 1.0
        self._filter_file = os.path.join(os.path.dirname(__file__), "..", ".chat_filters.json")
        self._load_filters()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    def _is_configured(self) -> bool:
        return bool(self.bot_token) and bool(self.chat_id)

    # ------------------------------------------------------------------
    # Filter settings (all in % now, not bps)
    # ------------------------------------------------------------------

    def get_min_spread_pct(self, chat_id: str | None = None) -> float:
        """Get the current minimum spread filter in % for a chat."""
        cid = chat_id or self.chat_id
        return self._chat_filters.get(cid, self._default_min_pct)

    def set_min_spread_pct(self, min_pct: float, chat_id: str | None = None) -> None:
        """Set the minimum spread filter in % for a chat. Persists to disk."""
        cid = chat_id or self.chat_id
        self._chat_filters[cid] = min_pct
        self._save_filters()
        logger.info("filter_updated", chat_id=cid, min_net_spread_pct=min_pct)

    def _load_filters(self) -> None:
        """Load saved filters from disk."""
        try:
            if os.path.exists(self._filter_file):
                with open(self._filter_file, "r") as f:
                    self._chat_filters = json.load(f)
                logger.info("filters_loaded", filters=self._chat_filters)
        except Exception:
            logger.exception("filter_load_error")

    def _save_filters(self) -> None:
        """Persist filters to disk so they survive restarts."""
        try:
            with open(self._filter_file, "w") as f:
                json.dump(self._chat_filters, f)
        except Exception:
            logger.exception("filter_save_error")

    def passes_filter(self, opp: SpreadOpportunity, chat_id: str | None = None) -> bool:
        """Check if an opportunity passes the chat's spread filter."""
        min_pct = self.get_min_spread_pct(chat_id)
        # Convert bps to %: 100 bps = 1%
        net_pct = float(opp.net_spread_bps) / 100.0
        passed = net_pct >= min_pct
        if not passed:
            logger.debug(
                "telegram_filter_rejected",
                symbol=opp.canonical_symbol,
                net_pct=round(net_pct, 2),
                min_pct=min_pct,
                chat_id=chat_id or self.chat_id,
                filters=dict(self._chat_filters),
            )
        return passed

    # ------------------------------------------------------------------
    # Inline keyboard
    # ------------------------------------------------------------------

    def _build_filter_keyboard(self, chat_id: str | None = None) -> list[list[dict]]:
        """Build inline keyboard for spread filter selection."""
        cid = chat_id or self.chat_id
        current = self.get_min_spread_pct(cid)

        rows: list[list[dict]] = []
        row: list[dict] = []

        for opt in SPREAD_FILTER_OPTIONS:
            is_active = abs(opt["pct"] - current) < 0.01
            label = f"{'✅ ' if is_active else ''}{opt['label']}"
            btn = {
                "text": label,
                "callback_data": f"filter:{opt['pct']}",
            }
            row.append(btn)
            if len(row) == 3:
                rows.append(row)
                row = []

        if row:
            rows.append(row)

        return rows

    # ------------------------------------------------------------------
    # Sending alerts
    # ------------------------------------------------------------------

    async def send_alert(self, opp: SpreadOpportunity) -> bool:
        """Send a spread alert if it passes the filter. No buttons on alerts."""
        if not self._is_configured():
            logger.warning("telegram_not_configured", hint="Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")
            return False

        if not self.passes_filter(opp):
            return False

        message = format_alert(opp)
        # No inline keyboard on alerts — settings are in /filter panel only
        return await self._send_message(message)

    async def send_grouped_alert(
        self,
        opps: list[SpreadOpportunity],
        deposit_status: dict | None = None,
    ) -> bool:
        """
        Send a grouped alert with multiple routes for the same base token.
        Filters each route individually; sends only those that pass.
        Includes funding info and deposit/withdrawal status.
        """
        if not self._is_configured():
            return False

        # Filter: only routes that pass the chat's min spread filter
        passing = [o for o in opps if self.passes_filter(o)]
        if not passing:
            return False

        # Sort by net spread descending (best first)
        passing.sort(key=lambda o: float(o.net_spread_bps), reverse=True)

        message = format_grouped_alert(passing, deposit_status=deposit_status)
        return await self._send_message(message)

    async def _send_message(
        self,
        text: str,
        keyboard: list[list[dict]] | None = None,
    ) -> bool:
        """Send a MarkdownV2 message with optional inline keyboard."""
        async with self._lock:
            await self._rate_limit()

            url = f"{TELEGRAM_API_BASE}/bot{self.bot_token}/sendMessage"
            payload: dict = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "MarkdownV2",
                "disable_web_page_preview": True,
            }
            if keyboard:
                payload["reply_markup"] = {"inline_keyboard": keyboard}

            try:
                session = await self._get_session()
                async with session.post(url, json=payload) as resp:
                    self._last_send_time = time.monotonic()
                    if resp.status == 200:
                        logger.info("telegram_sent", chat_id=self.chat_id)
                        return True
                    else:
                        body = await resp.text()
                        logger.error("telegram_send_failed", status=resp.status, body=body[:200])
                        return False
            except Exception:
                logger.exception("telegram_send_error")
                return False

    async def _send_plain(self, chat_id: str, text: str, with_menu: bool = True) -> None:
        """Send a plain text message with persistent bottom keyboard."""
        url = f"{TELEGRAM_API_BASE}/bot{self.bot_token}/sendMessage"
        payload: dict = {"chat_id": chat_id, "text": text}
        if with_menu:
            payload["reply_markup"] = BOTTOM_MENU_KEYBOARD
        try:
            session = await self._get_session()
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning("plain_send_failed", status=resp.status, body=body[:200])
        except Exception:
            logger.exception("plain_send_error")

    async def _send_bottom_menu(self) -> None:
        """Send a greeting with the persistent bottom keyboard on startup."""
        current = self.get_min_spread_pct()
        await self._send_plain(
            self.chat_id,
            f"🟢 Bot started\n"
            f"Filter: > {current:g}% net spread\n\n"
            f"Use the buttons below to manage settings."
        )

    async def _send_status(self, chat_id: str | None = None) -> None:
        """Send current bot status: uptime, filter, snapshot counts."""
        cid = chat_id or self.chat_id
        current = self.get_min_spread_pct(cid)
        snap_count = len(self._alert_count) if hasattr(self, '_alert_count') else 0

        await self._send_plain(
            cid,
            f"📊 Bot Status\n\n"
            f"Filter: > {current:g}% net spread\n"
            f"Exchanges: Binance, Hyperliquid, Gate.io\n"
            f"Status: Running"
        )

    # ------------------------------------------------------------------
    # Polling for updates (button presses + commands)
    # ------------------------------------------------------------------

    async def start_polling(self) -> None:
        """Start polling for Telegram updates."""
        if not self._is_configured():
            return

        # Clear any stale webhook/polling sessions to avoid 409 Conflict errors
        await self._clear_webhook()

        # Register persistent bot menu commands (bottom menu in Telegram)
        await self._set_bot_commands()

        self._polling_task = asyncio.create_task(self._poll_loop())
        logger.info("telegram_polling_started")

        # Send initial message with persistent bottom keyboard
        await self._send_bottom_menu()

    async def _set_bot_commands(self) -> None:
        """Register bot commands so they appear in Telegram's bottom menu."""
        url = f"{TELEGRAM_API_BASE}/bot{self.bot_token}/setMyCommands"
        commands = [
            {"command": "filter", "description": "Open filter settings panel"},
            {"command": "setmin", "description": "Set minimum spread % (e.g. /setmin 2.5)"},
            {"command": "help", "description": "Show help and commands"},
        ]
        try:
            session = await self._get_session()
            async with session.post(url, json={"commands": commands}) as resp:
                if resp.status == 200:
                    logger.info("bot_commands_registered")
                else:
                    body = await resp.text()
                    logger.warning("bot_commands_failed", status=resp.status, body=body[:200])
        except Exception:
            logger.exception("bot_commands_error")

    async def _clear_webhook(self) -> None:
        """
        Delete any existing webhook, drop pending updates, and flush stale
        getUpdates sessions to avoid 409 Conflict errors.
        """
        session = await self._get_session()

        # 1. Delete webhook
        try:
            url = f"{TELEGRAM_API_BASE}/bot{self.bot_token}/deleteWebhook"
            async with session.post(url, json={"drop_pending_updates": True}) as resp:
                logger.info("webhook_cleared", status=resp.status)
        except Exception:
            logger.exception("webhook_clear_error")

        # 2. Flush any pending getUpdates by doing a quick non-blocking poll
        #    with offset=-1 to skip all old updates
        try:
            url = f"{TELEGRAM_API_BASE}/bot{self.bot_token}/getUpdates"
            async with session.get(url, params={"offset": -1, "timeout": 0},
                                   timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    results = data.get("result", [])
                    if results:
                        # Set offset past the last update to clear the queue
                        self._last_update_id = results[-1]["update_id"]
                    logger.info("updates_flushed", cleared=len(results))
        except Exception:
            logger.exception("updates_flush_error")

        # 3. Wait for Telegram to release the old polling session
        await asyncio.sleep(2)

    async def _poll_loop(self) -> None:
        """Long-poll for Telegram updates. Handles 409 conflicts gracefully."""
        conflict_backoff = 1
        while True:
            try:
                session = await self._get_session()
                url = f"{TELEGRAM_API_BASE}/bot{self.bot_token}/getUpdates"
                payload = {
                    "offset": self._last_update_id + 1,
                    "timeout": 30,
                    "allowed_updates": ["callback_query", "message"],
                }

                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=40)) as resp:
                    if resp.status == 409:
                        # 409 = another getUpdates session is active.
                        # Wait with increasing backoff until the stale session expires.
                        logger.debug("poll_conflict_waiting", backoff=conflict_backoff)
                        await asyncio.sleep(conflict_backoff)
                        conflict_backoff = min(conflict_backoff * 2, 60)
                        continue

                    if resp.status != 200:
                        logger.warning("poll_error", status=resp.status)
                        await asyncio.sleep(5)
                        continue

                    # Success — reset conflict backoff
                    conflict_backoff = 1

                    data = await resp.json()
                    updates = data.get("result", [])
                    if updates:
                        logger.info("updates_received", count=len(updates))
                    for update in updates:
                        self._last_update_id = update["update_id"]

                        callback = update.get("callback_query")
                        if callback:
                            await self._handle_callback(callback)

                        message = update.get("message")
                        if message:
                            text = (message.get("text") or "")
                            logger.info("message_received", text=text[:50])
                            await self._handle_message(message)

            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                continue
            except Exception:
                logger.exception("poll_error")
                await asyncio.sleep(5)

    async def _handle_callback(self, callback: dict) -> None:
        """Handle inline keyboard button press."""
        callback_id = callback.get("id", "")
        data = callback.get("data", "")
        from_user = callback.get("from", {})
        chat_id = str(from_user.get("id", self.chat_id))

        if data.startswith("filter:"):
            try:
                min_pct = float(data.split(":")[1])
                self.set_min_spread_pct(min_pct, chat_id)

                if min_pct == 0:
                    label = "All alerts"
                else:
                    label = f"> {min_pct:g}%"

                await self._answer_callback(callback_id, f"Filter: {label}")

                msg = callback.get("message", {})
                msg_id = msg.get("message_id")
                msg_chat_id = str(msg.get("chat", {}).get("id", chat_id))
                if msg_id:
                    await self._update_keyboard(msg_chat_id, msg_id, chat_id)

            except (ValueError, IndexError):
                await self._answer_callback(callback_id, "Invalid filter")

    async def _handle_message(self, message: dict) -> None:
        """Handle text messages, commands, and bottom menu button presses."""
        text = (message.get("text") or "").strip()
        chat_id = str(message.get("chat", {}).get("id", ""))

        # Bottom menu button presses
        if text == "⚙️ Settings":
            await self._send_filter_status(chat_id)
        elif text == "📊 Status":
            await self._send_status(chat_id)
        elif text == "❓ Help":
            await self._send_welcome(chat_id)
        # Slash commands
        elif text.startswith("/setmin"):
            await self._handle_setmin(text, chat_id)
        elif text.startswith("/filter") or text.startswith("/settings"):
            await self._send_filter_status(chat_id)
        elif text.startswith("/start") or text.startswith("/help"):
            await self._send_welcome(chat_id)

    async def _handle_setmin(self, text: str, chat_id: str) -> None:
        """
        Handle /setmin command — user types custom minimum spread %.

        Examples:
          /setmin 2.5   → set minimum to 2.5%
          /setmin 0.5   → set minimum to 0.5%
          /setmin 0     → show all alerts
          /setmin       → show usage help
        """
        parts = text.split()

        if len(parts) < 2:
            await self._send_plain(
                chat_id,
                "📐 Usage: /setmin <percent>\n\n"
                "Examples:\n"
                "  /setmin 2.5  → only show spreads > 2.5%\n"
                "  /setmin 1    → only show spreads > 1%\n"
                "  /setmin 0    → show all alerts\n\n"
                f"Current: > {self.get_min_spread_pct(chat_id):g}%"
            )
            return

        try:
            value = float(parts[1])
            if value < 0 or value > 100:
                await self._send_plain(chat_id, "❌ Value must be between 0 and 100")
                return

            self.set_min_spread_pct(value, chat_id)

            if value == 0:
                await self._send_plain(chat_id, "✅ Filter removed — showing all alerts")
            else:
                await self._send_plain(
                    chat_id,
                    f"✅ Minimum spread set to {value:g}%\n"
                    f"Only alerts with net spread > {value:g}% will be shown."
                )

        except ValueError:
            await self._send_plain(
                chat_id,
                f"❌ Invalid number: {parts[1]}\n"
                "Usage: /setmin 2.5"
            )

    async def _send_welcome(self, chat_id: str | None = None) -> None:
        """Send welcome message with command list."""
        cid = chat_id or self.chat_id
        current = self.get_min_spread_pct(cid)

        await self._send_plain(
            cid,
            "🔔 Spread Scanner Bot\n\n"
            "Monitors cross-exchange spread opportunities on "
            "Binance, Hyperliquid, and Gate.io perpetual futures.\n\n"
            "Commands:\n"
            f"  /setmin <percent>  — Set minimum spread (current: {current:g}%)\n"
            "  /filter  — Show filter panel with buttons\n"
            "  /help    — Show this message\n\n"
            "Examples:\n"
            "  /setmin 3    — only alert if spread > 3%\n"
            "  /setmin 0.5  — alert if spread > 0.5%\n"
            "  /setmin 0    — show all alerts"
        )

    async def _send_filter_status(self, chat_id: str | None = None) -> None:
        """Send current filter status with buttons."""
        cid = chat_id or self.chat_id
        current = self.get_min_spread_pct(cid)

        if current == 0:
            status = "Showing all alerts"
        else:
            status = f"Showing alerts with net spread > {current:g}%"

        text = (
            f"⚙️ Filter Settings\n\n"
            f"{status}\n\n"
            f"Quick select or type /setmin <percent> for custom value:"
        )

        keyboard = self._build_filter_keyboard(cid)

        url = f"{TELEGRAM_API_BASE}/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": cid,
            "text": text,
            "reply_markup": {"inline_keyboard": keyboard},
        }

        try:
            session = await self._get_session()
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning("filter_status_failed", status=resp.status, body=body[:200])
        except Exception:
            logger.exception("filter_status_error")

    async def _answer_callback(self, callback_id: str, text: str) -> None:
        """Answer a callback query."""
        url = f"{TELEGRAM_API_BASE}/bot{self.bot_token}/answerCallbackQuery"
        payload = {"callback_query_id": callback_id, "text": text, "show_alert": False}
        try:
            session = await self._get_session()
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    logger.warning("callback_answer_failed", status=resp.status)
        except Exception:
            logger.exception("callback_answer_error")

    async def _update_keyboard(self, chat_id: str, message_id: int, user_chat_id: str) -> None:
        """Update inline keyboard on existing message."""
        keyboard = self._build_filter_keyboard(user_chat_id)
        url = f"{TELEGRAM_API_BASE}/bot{self.bot_token}/editMessageReplyMarkup"
        payload = {
            "chat_id": chat_id,
            "message_id": message_id,
            "reply_markup": {"inline_keyboard": keyboard},
        }
        try:
            session = await self._get_session()
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    logger.debug("keyboard_update_failed", status=resp.status)
        except Exception:
            logger.exception("keyboard_update_error")

    # ------------------------------------------------------------------
    # Rate limiting & lifecycle
    # ------------------------------------------------------------------

    async def _rate_limit(self) -> None:
        """Wait if needed to respect send rate limit."""
        if self._last_send_time > 0:
            elapsed = time.monotonic() - self._last_send_time
            if elapsed < MIN_SEND_INTERVAL_SECONDS:
                await asyncio.sleep(MIN_SEND_INTERVAL_SECONDS - elapsed)

    async def close(self) -> None:
        """Close the aiohttp session and stop polling."""
        if self._polling_task:
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass
            self._polling_task = None

        if self._own_session and self._session:
            await self._session.close()
            self._session = None
