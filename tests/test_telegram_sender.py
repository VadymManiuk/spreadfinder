"""
Tests for TelegramSender startup-message behavior.

Inputs: Configured sender instance with a temporary UI-state file.
Outputs: Verifies the startup menu is not resent on every restart.
Assumptions:
  - Startup menu should be delivered once per chat and persisted to disk.
  - Failed sends must not mark the startup message as delivered.
"""

import json
from unittest.mock import AsyncMock

import pytest

from alerting.telegram import TelegramSender


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
