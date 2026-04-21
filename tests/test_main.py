"""
Tests for scanner startup health checks.
"""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from config.settings import PumpTelegramSettings, Settings, TelegramSettings
from main import SpreadScanner
from pump_detector.models import PumpAlert


def _make_scanner(exchanges: list[str]) -> SpreadScanner:
    return SpreadScanner(
        Settings(
            enabled_exchanges=exchanges,
            pump_telegram=PumpTelegramSettings(bot_token="", chat_id=""),
        )
    )


def test_bootstrap_health_rejects_heavily_degraded_startup():
    exchanges = [
        "binance",
        "hyperliquid",
        "gate",
        "bybit",
        "okx",
        "bitget",
        "aster",
        "lighter",
    ]
    scanner = _make_scanner(exchanges)

    scanner._mapper.load_static("aster", {"MEGA": "MEGA-USDT-PERP"})
    scanner._mapper.load_static("lighter", {"MEGA": "MEGA-USDC-PERP"})
    scanner._mapper._bootstrap_errors["aster"] = None
    scanner._mapper._bootstrap_errors["lighter"] = None

    for exchange in exchanges[:-2]:
        scanner._mapper._native_to_canonical[exchange] = {}
        scanner._mapper._canonical_to_native[exchange] = {}
        scanner._mapper._bootstrap_errors[exchange] = "dns failure"

    with pytest.raises(RuntimeError, match="bootstrap too degraded"):
        scanner._validate_bootstrap_health()


def test_bootstrap_health_allows_majority_of_exchanges_ready():
    exchanges = [
        "binance",
        "hyperliquid",
        "gate",
        "bybit",
        "okx",
        "bitget",
        "aster",
        "lighter",
    ]
    scanner = _make_scanner(exchanges)

    for exchange in exchanges[:5]:
        scanner._mapper.load_static(exchange, {"MEGA": "MEGA-USDT-PERP"})
        scanner._mapper._bootstrap_errors[exchange] = None

    for exchange in exchanges[5:]:
        scanner._mapper._native_to_canonical[exchange] = {}
        scanner._mapper._canonical_to_native[exchange] = {}
        scanner._mapper._bootstrap_errors[exchange] = "dns failure"

    scanner._validate_bootstrap_health()


def test_pump_alerts_use_main_sender_by_default():
    scanner = SpreadScanner(
        Settings(
            enabled_exchanges=["binance"],
            telegram=TelegramSettings(bot_token="main-token", chat_id="main-chat"),
            pump_telegram=PumpTelegramSettings(bot_token="", chat_id=""),
        )
    )

    assert scanner._pump_sender() is scanner._telegram


def test_pump_alerts_use_secondary_bot_when_configured():
    scanner = SpreadScanner(
        Settings(
            enabled_exchanges=["binance"],
            telegram=TelegramSettings(bot_token="main-token", chat_id="main-chat"),
            pump_telegram=PumpTelegramSettings(
                bot_token="pump-token",
                chat_id="pump-chat",
            ),
        )
    )

    assert scanner._pump_telegram is not None
    assert scanner._pump_sender() is scanner._pump_telegram
    assert scanner._pump_telegram.bot_token == "pump-token"
    assert scanner._pump_telegram.chat_id == "pump-chat"


def test_pump_alerts_fall_back_to_main_chat_id_for_secondary_bot():
    scanner = SpreadScanner(
        Settings(
            enabled_exchanges=["binance"],
            telegram=TelegramSettings(bot_token="main-token", chat_id="main-chat"),
            pump_telegram=PumpTelegramSettings(
                bot_token="pump-token",
                chat_id="",
            ),
        )
    )

    assert scanner._pump_telegram is not None
    assert scanner._pump_telegram.chat_id == "main-chat"


@pytest.mark.asyncio
async def test_pump_alert_send_falls_back_to_main_sender_on_secondary_failure():
    scanner = SpreadScanner(Settings(enabled_exchanges=["binance"]))

    class StubSender:
        def __init__(self, result: bool):
            self.result = result
            self.calls = 0

        async def send_pump_alert(self, alert: PumpAlert) -> bool:
            self.calls += 1
            return self.result

    scanner._telegram = StubSender(True)
    scanner._pump_telegram = StubSender(False)

    alert = PumpAlert(
        base="ARIA",
        direction="pump",
        start_price=Decimal("1.0"),
        current_price=Decimal("1.2"),
        change_pct=Decimal("20.0"),
        window_seconds=300,
        start_ts=datetime.now(timezone.utc),
        current_ts=datetime.now(timezone.utc),
        triggered_on="binance",
    )

    sent = await scanner._send_pump_alert(alert)

    assert sent is True
    assert scanner._pump_telegram.calls == 1
    assert scanner._telegram.calls == 1
