"""
Telegram Bot API sender with rate limiting.

Inputs: Bot token, chat ID, formatted message string.
Outputs: Sends message via Telegram Bot API.
Assumptions:
  - Bot token and chat ID come from environment variables.
  - Rate limited to avoid Telegram API throttling (max 30 msg/sec to same chat).
  - Uses aiohttp for async HTTP requests.
"""

import asyncio
import os
import time

import aiohttp
import structlog

from models.snapshot import SpreadOpportunity
from alerting.formatter import format_alert

logger = structlog.get_logger(__name__)

# Telegram rate limit: max 30 messages per second to same chat
# We use a conservative limit of 1 message per second
MIN_SEND_INTERVAL_SECONDS = 1.0

TELEGRAM_API_BASE = "https://api.telegram.org"


class TelegramSender:
    """
    Sends spread alerts to a Telegram chat via Bot API.

    Usage:
        sender = TelegramSender(bot_token="...", chat_id="...")
        await sender.send_alert(opportunity)
        await sender.close()
    """

    def __init__(
        self,
        bot_token: str | None = None,
        chat_id: str | None = None,
        session: aiohttp.ClientSession | None = None,
    ):
        """
        Args:
            bot_token: Telegram bot token. Falls back to TELEGRAM_BOT_TOKEN env var.
            chat_id: Telegram chat ID. Falls back to TELEGRAM_CHAT_ID env var.
            session: Optional shared aiohttp session.
        """
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID", "")
        self._session = session
        self._own_session = session is None
        self._last_send_time: float = 0
        self._lock = asyncio.Lock()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def send_alert(self, opp: SpreadOpportunity) -> bool:
        """
        Format and send a spread alert to Telegram.

        Returns True if sent successfully, False otherwise.
        """
        if not self.bot_token or not self.chat_id:
            logger.warning("telegram_not_configured", hint="Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")
            return False

        message = format_alert(opp)
        return await self._send_message(message)

    async def _send_message(self, text: str) -> bool:
        """Send a MarkdownV2 message to the configured chat, respecting rate limits."""
        async with self._lock:
            await self._rate_limit()

            url = f"{TELEGRAM_API_BASE}/bot{self.bot_token}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "MarkdownV2",
                "disable_web_page_preview": True,
            }

            try:
                session = await self._get_session()
                async with session.post(url, json=payload) as resp:
                    self._last_send_time = time.monotonic()
                    if resp.status == 200:
                        logger.info("telegram_sent", chat_id=self.chat_id)
                        return True
                    else:
                        body = await resp.text()
                        logger.error(
                            "telegram_send_failed",
                            status=resp.status,
                            body=body[:200],
                        )
                        return False
            except Exception:
                logger.exception("telegram_send_error")
                return False

    async def _rate_limit(self) -> None:
        """Wait if needed to respect send rate limit."""
        if self._last_send_time > 0:
            elapsed = time.monotonic() - self._last_send_time
            if elapsed < MIN_SEND_INTERVAL_SECONDS:
                await asyncio.sleep(MIN_SEND_INTERVAL_SECONDS - elapsed)

    async def close(self) -> None:
        """Close the aiohttp session if we own it."""
        if self._own_session and self._session:
            await self._session.close()
            self._session = None
