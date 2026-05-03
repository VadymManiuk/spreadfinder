"""
Tests for TelegramSender startup-message behavior.

Inputs: Configured sender instance with a temporary UI-state file.
Outputs: Verifies the startup menu is not resent on every restart.
Assumptions:
  - Startup menu should be delivered once per chat and persisted to disk.
  - Failed sends must not mark the startup message as delivered.
"""

import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from alerting.telegram import TelegramSender
from models.snapshot import SpreadOpportunity


NOW = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def make_opp(**overrides) -> SpreadOpportunity:
    defaults = {
        "canonical_symbol": "VANRY-USDT-SPOT",
        "buy_exchange": "gate_spot",
        "sell_exchange": "binance",
        "buy_ask": Decimal("0.0100"),
        "sell_bid": Decimal("0.0110"),
        "gross_spread": Decimal("0.0010"),
        "gross_spread_bps": Decimal("1000"),
        "net_spread": Decimal("0.0009"),
        "net_spread_bps": Decimal("900"),
        "estimated_fees": Decimal("0.00005"),
        "estimated_slippage": Decimal("0.00005"),
        "buy_funding_rate": None,
        "sell_funding_rate": Decimal("0.0001"),
        "buy_ask_size": Decimal("1000"),
        "sell_bid_size": Decimal("900"),
        "buy_volume_24h": Decimal("2500000"),
        "sell_volume_24h": Decimal("1800000"),
        "data_age_ms": 150,
        "confidence": Decimal("0.85"),
        "timestamp": NOW,
    }
    defaults.update(overrides)
    return SpreadOpportunity(**defaults)


@pytest.mark.asyncio
async def test_startup_message_sent_only_once_per_chat(tmp_path, monkeypatch):
    state_file = tmp_path / "telegram_ui_state.json"
    sender = TelegramSender(
        bot_token="token",
        chat_id="12345",
        allow_default_env=False,
        ui_state_file=str(state_file),
    )

    send_plain = AsyncMock(return_value=True)
    monkeypatch.setattr(sender, "_send_plain", send_plain)

    await sender._send_bottom_menu()
    await sender._send_bottom_menu()

    send_plain.assert_awaited_once()
    assert json.loads(state_file.read_text()) == {
        "startup_message_sent_chat_ids": ["12345"]
    }


@pytest.mark.asyncio
async def test_startup_message_state_loaded_from_disk(tmp_path, monkeypatch):
    state_file = tmp_path / "telegram_ui_state.json"
    state_file.write_text(
        json.dumps({"startup_message_sent_chat_ids": ["999"]})
    )
    sender = TelegramSender(
        bot_token="token",
        chat_id="999",
        allow_default_env=False,
        ui_state_file=str(state_file),
    )

    send_plain = AsyncMock(return_value=True)
    monkeypatch.setattr(sender, "_send_plain", send_plain)

    await sender._send_bottom_menu()

    send_plain.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_startup_message_does_not_persist_state(tmp_path, monkeypatch):
    state_file = tmp_path / "telegram_ui_state.json"
    sender = TelegramSender(
        bot_token="token",
        chat_id="777",
        allow_default_env=False,
        ui_state_file=str(state_file),
    )

    send_plain = AsyncMock(return_value=False)
    monkeypatch.setattr(sender, "_send_plain", send_plain)

    await sender._send_bottom_menu()

    assert not state_file.exists()
    assert sender._has_sent_startup_message("777") is False


def test_excluded_ticker_persists_and_filters(tmp_path):
    exclusions_file = tmp_path / "excluded_tickers.json"
    sender = TelegramSender(
        bot_token="token",
        chat_id="12345",
        allow_default_env=False,
        excluded_tickers_file=str(exclusions_file),
    )

    assert sender.exclude_ticker("$vanry") == "VANRY"
    assert sender.is_ticker_excluded("vanry") is True
    assert sender.passes_ticker_exclusion(make_opp()) is False

    loaded = TelegramSender(
        bot_token="token",
        chat_id="12345",
        allow_default_env=False,
        excluded_tickers_file=str(exclusions_file),
    )
    assert loaded.get_excluded_tickers() == ["VANRY"]


@pytest.mark.asyncio
async def test_grouped_alert_has_exclude_button_and_respects_exclusions(tmp_path, monkeypatch):
    sender = TelegramSender(
        bot_token="token",
        chat_id="12345",
        allow_default_env=False,
        excluded_tickers_file=str(tmp_path / "excluded_tickers.json"),
    )
    send_message = AsyncMock(return_value=True)
    monkeypatch.setattr(sender, "_send_message", send_message)

    assert await sender.send_grouped_alert([make_opp()]) is True
    keyboard = send_message.await_args.args[1]
    assert keyboard == [[{"text": "🚫 Exclude VANRY", "callback_data": "exclude:VANRY"}]]

    sender.exclude_ticker("VANRY")
    assert await sender.send_grouped_alert([make_opp()]) is False
    assert send_message.await_count == 1


@pytest.mark.asyncio
async def test_exclude_callback_adds_ticker_and_replaces_button(tmp_path, monkeypatch):
    sender = TelegramSender(
        bot_token="token",
        chat_id="12345",
        allow_default_env=False,
        excluded_tickers_file=str(tmp_path / "excluded_tickers.json"),
    )
    answer = AsyncMock()
    edit_markup = AsyncMock()
    monkeypatch.setattr(sender, "_answer_callback", answer)
    monkeypatch.setattr(sender, "_edit_message_reply_markup", edit_markup)

    await sender._handle_callback({
        "id": "callback-1",
        "data": "exclude:VANRY",
        "from": {"id": 12345},
        "message": {"message_id": 99, "chat": {"id": "12345"}},
    })

    assert sender.is_ticker_excluded("VANRY", "12345") is True
    answer.assert_awaited_once_with("callback-1", "VANRY excluded")
    edit_markup.assert_awaited_once_with(
        "12345",
        99,
        [[{"text": "↩️ Undo exclude VANRY", "callback_data": "include:VANRY"}]],
    )
