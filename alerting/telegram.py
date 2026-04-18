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
from pump_detector.models import PumpAlert
from alerting.formatter import format_alert, format_grouped_alert, format_pump_alert

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
        [{"text": "⚙️ Spread"}, {"text": "🚀 Pump"}],
        [{"text": "📊 Status"}, {"text": "❓ Help"}],
    ],
    "resize_keyboard": True,
    "is_persistent": True,
}

# Inline quick-select options for the pump panel.
# Threshold in %, window in minutes, volume floor in USD.
PUMP_PCT_OPTIONS = [
    {"label": "> 3%", "value": 3.0},
    {"label": "> 5%", "value": 5.0},
    {"label": "> 10%", "value": 10.0},
    {"label": "> 20%", "value": 20.0},
]
PUMP_TIME_OPTIONS = [
    {"label": "5 min", "value": 5},
    {"label": "15 min", "value": 15},
    {"label": "1 h", "value": 60},
    {"label": "4 h", "value": 240},
]
PUMP_VOL_OPTIONS = [
    {"label": "$10K", "value": 10_000},
    {"label": "$100K", "value": 100_000},
    {"label": "$500K", "value": 500_000},
    {"label": "$1M", "value": 1_000_000},
]


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
        all_snapshots: dict | None = None,
    ) -> bool:
        """
        Send a grouped alert with multiple routes for the same base token.
        Filters each route individually; sends only those that pass.
        Includes funding info, deposit/withdrawal status, and all exchange prices.
        """
        if not self._is_configured():
            return False

        # Filter: only routes that pass the chat's min spread filter
        passing = [o for o in opps if self.passes_filter(o)]
        if not passing:
            return False

        # Sort by net spread descending (best first)
        passing.sort(key=lambda o: float(o.net_spread_bps), reverse=True)

        message = format_grouped_alert(
            passing,
            deposit_status=deposit_status,
            all_snapshots=all_snapshots,
        )
        return await self._send_message(message)

    async def send_pump_alert(self, alert: PumpAlert) -> bool:
        """Send a pump/dump price alert. Bypasses spread % filter."""
        if not self._is_configured():
            return False
        message = format_pump_alert(alert)
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
        """Send comprehensive bot status with diagnostics."""
        cid = chat_id or self.chat_id
        current = self.get_min_spread_pct(cid)

        diag = getattr(self, "_scanner_diag", None)
        scanner = getattr(self, "_scanner_ref", None)

        if diag is None or scanner is None:
            await self._send_plain(cid, "📊 Bot Status\nNo diagnostics attached")
            return

        import time as _t

        uptime_s = int(_t.monotonic() - scanner._start_time)
        uptime_h = uptime_s // 3600
        uptime_m = (uptime_s % 3600) // 60

        # Count live snapshots per exchange (= exchanges whose WS is feeding us)
        live_exchanges: dict[str, int] = {}
        for (ex, _sym), _snap in scanner._snapshots.items():
            live_exchanges[ex] = live_exchanges.get(ex, 0) + 1

        # Enumerate ALL exchanges in config — including ones that failed bootstrap
        # or whose WS never connected. Surfacing those silently-dead exchanges
        # was the whole point of the diagnostic: previously they were invisible
        # because this section only listed exchanges with snapshots.
        mapper = getattr(scanner, "_mapper", None)
        enabled = list(getattr(scanner.settings, "enabled_exchanges", []))

        exchange_lines: list[str] = []
        for ex in sorted(enabled):
            mapped = len(mapper.get_exchange_symbols(ex)) if mapper else 0
            live = live_exchanges.get(ex, 0)
            err = mapper.get_bootstrap_error(ex) if mapper else "no_mapper"

            if err:
                # Bootstrap failed — show the error explicitly.
                exchange_lines.append(f"  ❌ {ex}: bootstrap failed ({err})")
            elif mapped == 0:
                exchange_lines.append(f"  ⚠️ {ex}: ok but 0 symbols mapped")
            elif live == 0:
                # Bootstrap ok, symbols available, but no live WS snapshots.
                # Likely a WS connect loop; supervisor will keep retrying.
                exchange_lines.append(f"  🔌 {ex}: {mapped} mapped, 0 live (ws down?)")
            else:
                exchange_lines.append(f"  ✅ {ex}: {live}/{mapped} live")

        exchanges_str = "\n".join(exchange_lines) or "  (no exchanges configured)"

        last_spread = diag.get("last_spread_alert_ts")
        last_spread_str = last_spread.strftime("%H:%M:%S UTC") if last_spread else "never"
        last_spread_sym = diag.get("last_spread_symbol", "")

        last_pump = diag.get("last_pump_ts")
        last_pump_str = last_pump.strftime("%H:%M:%S UTC") if last_pump else "never"

        matchable_count = sum(len(v) for v in scanner._match_lookup.values()) // 2

        await self._send_plain(
            cid,
            f"📊 Bot Status\n\n"
            f"⏱ Uptime: {uptime_h}h {uptime_m}m\n"
            f"🔗 Exchanges:\n{exchanges_str}\n\n"
            f"📸 Snapshots: {diag['snapshots_total']:,}\n"
            f"🔀 Matchable pairs: {matchable_count:,}\n\n"
            f"── Spread Alerts ──\n"
            f"Calculations:   {diag['spreads_calculated']:,}\n"
            f"Passed filters: {diag['spreads_passed_filters']:,}\n"
            f"Sent:           {diag['spreads_sent']:,}\n"
            f"Rejected <1%:   {diag['spreads_rejected_hard']:,}\n"
            f"Rejected filter:{diag['spreads_rejected_filter']:,}\n"
            f"Flush errors:   {diag['flush_errors']:,}\n"
            f"Last alert:     {last_spread_str} {last_spread_sym}\n"
            f"TG filter:      > {current:g}%\n\n"
            f"── Pump Alerts ──\n"
            f"Sent:           {diag['pumps_sent']:,}\n"
            f"Last alert:     {last_pump_str}\n"
            f"Enabled:        {'yes' if scanner._pump_enabled else 'no'}\n"
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
            {"command": "filter", "description": "Open spread filter panel"},
            {"command": "setmin", "description": "Set minimum spread % (e.g. /setmin 2.5)"},
            {"command": "pump", "description": "Open pump alert panel"},
            {"command": "setpump", "description": "Set min pump %  (e.g. /setpump 5)"},
            {"command": "setpumptime", "description": "Set pump window minutes (e.g. /setpumptime 60)"},
            {"command": "setpumpvol", "description": "Set min 24h volume (e.g. /setpumpvol 100000)"},
            {"command": "pumpon", "description": "Enable pump alerts"},
            {"command": "pumpoff", "description": "Disable pump alerts"},
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

        elif data.startswith("pump_"):
            await self._handle_pump_callback(data, callback, callback_id, chat_id)

    async def _handle_message(self, message: dict) -> None:
        """Handle text messages, commands, and bottom menu button presses."""
        text = (message.get("text") or "").strip()
        chat_id = str(message.get("chat", {}).get("id", ""))

        # Bottom menu button presses
        if text in ("⚙️ Spread", "⚙️ Settings"):
            await self._send_filter_status(chat_id)
        elif text == "🚀 Pump":
            await self._send_pump_panel(chat_id)
        elif text == "📊 Status":
            await self._send_status(chat_id)
        elif text == "❓ Help":
            await self._send_welcome(chat_id)
        # Slash commands
        elif text.startswith("/setmin"):
            await self._handle_setmin(text, chat_id)
        elif text.startswith("/filter") or text.startswith("/settings"):
            await self._send_filter_status(chat_id)
        elif text.startswith("/setpumptime"):
            await self._handle_setpumptime(text, chat_id)
        elif text.startswith("/setpumpvol"):
            await self._handle_setpumpvol(text, chat_id)
        elif text.startswith("/setpump"):
            await self._handle_setpump(text, chat_id)
        elif text.startswith("/pumpon"):
            await self._handle_pump_toggle(chat_id, True)
        elif text.startswith("/pumpoff"):
            await self._handle_pump_toggle(chat_id, False)
        elif text.startswith("/pumpsettings") or text.startswith("/pump"):
            await self._send_pump_panel(chat_id)
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

    # ------------------------------------------------------------------
    # /pump commands
    # ------------------------------------------------------------------

    def _get_pump_detector(self):
        """Return the attached PumpDetector, or None if not wired."""
        return getattr(self, "pump_detector", None)

    def _pump_is_enabled(self) -> bool:
        """Best-effort read of the runtime pump-enabled flag from main.py."""
        getter = getattr(self, "_pump_enabled_getter", None)
        if callable(getter):
            try:
                return bool(getter())
            except Exception:
                return True
        return True

    # ------------------------------------------------------------------
    # /pump interactive panel
    # ------------------------------------------------------------------

    def _build_pump_keyboard(self) -> list[list[dict]]:
        """
        Inline keyboard for the pump settings panel.
        Rows: threshold, window, volume, enable toggle.
        Active option is marked with ✅.
        """
        det = self._get_pump_detector()
        if det is None:
            return []

        cur_pct = float(det.min_change_pct)
        cur_time = det.window_seconds // 60
        cur_vol = float(det.min_volume_24h)
        enabled = self._pump_is_enabled()

        def row(prefix: str, options: list[dict], current) -> list[dict]:
            out: list[dict] = []
            for opt in options:
                is_active = abs(float(opt["value"]) - float(current)) < 1e-6
                mark = "✅ " if is_active else ""
                out.append({
                    "text": f"{mark}{opt['label']}",
                    "callback_data": f"{prefix}:{opt['value']}",
                })
            return out

        return [
            row("pump_pct", PUMP_PCT_OPTIONS, cur_pct),
            row("pump_time", PUMP_TIME_OPTIONS, cur_time),
            row("pump_vol", PUMP_VOL_OPTIONS, cur_vol),
            [
                {
                    "text": ("🛑 Turn off" if enabled else "✅ Turn on"),
                    "callback_data": f"pump_toggle:{'off' if enabled else 'on'}",
                },
                {"text": "🔄 Refresh", "callback_data": "pump_refresh:1"},
            ],
        ]

    def _pump_panel_text(self) -> str:
        det = self._get_pump_detector()
        if det is None:
            return "❌ Pump detector not attached"
        enabled = self._pump_is_enabled()
        status_line = "🟢 ENABLED" if enabled else "🛑 DISABLED"
        min_change = float(det.min_change_pct)
        window_min = det.window_seconds // 60
        min_vol = float(det.min_volume_24h)
        max_mcap = det.max_market_cap
        cooldown_min = det.cooldown_seconds // 60
        return (
            "🚀 Pump Alert Settings\n\n"
            f"Status:        {status_line}\n"
            f"Min change:    {min_change:g}%\n"
            f"Window:        {window_min} min\n"
            f"Min vol 24h:   ${min_vol:,.0f}\n"
            f"Max mcap:      ${max_mcap:,}\n"
            f"Cooldown:      {cooldown_min} min\n\n"
            "Tap a button below, or use:\n"
            "  /setpump <pct>\n"
            "  /setpumptime <min>\n"
            "  /setpumpvol <usd>"
        )

    async def _send_pump_panel(self, chat_id: str | None = None) -> None:
        """Send the interactive pump settings panel with inline buttons."""
        cid = chat_id or self.chat_id
        det = self._get_pump_detector()
        if det is None:
            await self._send_plain(cid, "❌ Pump detector not attached")
            return

        text = self._pump_panel_text()
        keyboard = self._build_pump_keyboard()

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
                    logger.warning("pump_panel_failed", status=resp.status, body=body[:200])
        except Exception:
            logger.exception("pump_panel_error")

    async def _handle_pump_callback(
        self, data: str, callback: dict, callback_id: str, chat_id: str
    ) -> None:
        """Handle inline button presses from the pump settings panel."""
        det = self._get_pump_detector()
        if det is None:
            await self._answer_callback(callback_id, "Pump detector not attached")
            return

        from decimal import Decimal as _D

        try:
            key, raw = data.split(":", 1)
        except ValueError:
            await self._answer_callback(callback_id, "Bad data")
            return

        toast = ""
        try:
            if key == "pump_pct":
                value = _D(raw)
                det.update(min_change_pct=value)
                toast = f"Min change: {float(value):g}%"
            elif key == "pump_time":
                mins = int(raw)
                det.update(window_minutes=mins)
                toast = f"Window: {mins} min"
            elif key == "pump_vol":
                value = _D(raw)
                det.update(min_volume_24h=value)
                toast = f"Min vol: ${float(value):,.0f}"
            elif key == "pump_toggle":
                setter = getattr(self, "_pump_enabled_setter", None)
                if not callable(setter):
                    await self._answer_callback(callback_id, "Toggle not wired")
                    return
                enable = raw == "on"
                setter(enable)
                toast = "Pump alerts ON" if enable else "Pump alerts OFF"
            elif key == "pump_refresh":
                toast = "Refreshed"
            else:
                await self._answer_callback(callback_id, "Unknown action")
                return
        except Exception:
            logger.exception("pump_callback_error")
            await self._answer_callback(callback_id, "Failed")
            return

        await self._answer_callback(callback_id, toast)

        # Re-render the panel text + keyboard in place
        msg = callback.get("message", {})
        msg_id = msg.get("message_id")
        msg_chat_id = str(msg.get("chat", {}).get("id", chat_id))
        if msg_id:
            await self._edit_pump_panel(msg_chat_id, msg_id)

    async def _edit_pump_panel(self, chat_id: str, message_id: int) -> None:
        """Edit an existing pump panel message in place with fresh state."""
        url = f"{TELEGRAM_API_BASE}/bot{self.bot_token}/editMessageText"
        payload = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": self._pump_panel_text(),
            "reply_markup": {"inline_keyboard": self._build_pump_keyboard()},
        }
        try:
            session = await self._get_session()
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.debug("pump_panel_edit_failed", status=resp.status, body=body[:200])
        except Exception:
            logger.exception("pump_panel_edit_error")

    async def _handle_setpump(self, text: str, chat_id: str) -> None:
        det = self._get_pump_detector()
        if det is None:
            await self._send_plain(chat_id, "❌ Pump detector not attached")
            return
        parts = text.split()
        if len(parts) < 2:
            await self._send_plain(chat_id, "Usage: /setpump 5  → 5% threshold")
            return
        try:
            from decimal import Decimal as _D
            value = _D(parts[1])
            if value <= 0 or value > 1000:
                await self._send_plain(chat_id, "❌ Must be between 0 and 1000")
                return
            det.update(min_change_pct=value)
            await self._send_plain(chat_id, f"✅ Pump min change set to {float(value):g}%")
        except Exception:
            await self._send_plain(chat_id, f"❌ Invalid number: {parts[1]}")

    async def _handle_setpumptime(self, text: str, chat_id: str) -> None:
        det = self._get_pump_detector()
        if det is None:
            await self._send_plain(chat_id, "❌ Pump detector not attached")
            return
        parts = text.split()
        if len(parts) < 2:
            await self._send_plain(chat_id, "Usage: /setpumptime 60  → 60 minute window")
            return
        try:
            mins = int(parts[1])
            if mins < 1 or mins > 1440:
                await self._send_plain(chat_id, "❌ Must be between 1 and 1440 minutes")
                return
            det.update(window_minutes=mins)
            await self._send_plain(chat_id, f"✅ Pump window set to {mins} min")
        except ValueError:
            await self._send_plain(chat_id, f"❌ Invalid number: {parts[1]}")

    async def _handle_setpumpvol(self, text: str, chat_id: str) -> None:
        det = self._get_pump_detector()
        if det is None:
            await self._send_plain(chat_id, "❌ Pump detector not attached")
            return
        parts = text.split()
        if len(parts) < 2:
            await self._send_plain(chat_id, "Usage: /setpumpvol 100000  → $100K min 24h vol")
            return
        try:
            from decimal import Decimal as _D
            value = _D(parts[1])
            if value < 0:
                await self._send_plain(chat_id, "❌ Must be >= 0")
                return
            det.update(min_volume_24h=value)
            await self._send_plain(chat_id, f"✅ Pump min volume set to ${float(value):,.0f}")
        except Exception:
            await self._send_plain(chat_id, f"❌ Invalid number: {parts[1]}")

    async def _handle_pump_toggle(self, chat_id: str, enable: bool) -> None:
        # Toggle is owned by the scanner — set a flag the scanner reads.
        # We piggy-back on a `pump_enabled` attr if the scanner exposes one.
        scanner_flag = getattr(self, "_pump_enabled_setter", None)
        if scanner_flag is None:
            await self._send_plain(
                chat_id,
                "⚠️ Pump toggle isn't wired in this build. "
                "Restart with PUMP_ENABLED in .env to control it."
            )
            return
        scanner_flag(enable)
        await self._send_plain(
            chat_id,
            "✅ Pump alerts ENABLED" if enable else "🛑 Pump alerts DISABLED"
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
