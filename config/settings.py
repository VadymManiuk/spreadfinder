"""
Centralized configuration for the spread scanner.

Inputs: Environment variables and .env file.
Outputs: Validated Settings object accessible throughout the application.
Assumptions:
  - All thresholds, fees, timeouts, and secrets are configured here.
  - Defaults are tuned for small-cap tokens (<$200M market cap).
  - pydantic-settings loads from .env automatically.
  - ALL sub-settings classes must include env_file=".env" to read from .env.
"""

from decimal import Decimal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Shared env file config — every sub-settings class needs this
_ENV_FILE_CONFIG = {
    "env_file": ".env",
    "env_file_encoding": "utf-8",
    "extra": "ignore",
}


class TelegramSettings(BaseSettings):
    """Telegram bot configuration."""

    model_config = SettingsConfigDict(env_prefix="TELEGRAM_", **_ENV_FILE_CONFIG)

    bot_token: str = ""
    chat_id: str = ""


class FilterSettings(BaseSettings):
    """Spread opportunity filter thresholds."""

    model_config = SettingsConfigDict(**_ENV_FILE_CONFIG)

    # Spread thresholds — default 1% net minimum (100 bps)
    min_gross_spread_bps: Decimal = Decimal("50.0")
    max_gross_spread_bps: Decimal = Decimal("50000.0")  # 500% — effectively disabled; small-cap spreads can be huge
    min_net_spread_bps: Decimal = Decimal("100.0")

    # Liquidity minimums
    min_bid_size: Decimal = Decimal("100.0")
    min_ask_size: Decimal = Decimal("100.0")

    # Volume filter (None = disabled)
    min_volume_24h: Decimal | None = None

    # Data freshness
    max_data_age_ms: int = 2000

    # Alert spam prevention
    cooldown_seconds: int = 1800
    persistence_ms: int = 1000

    # Confidence threshold
    min_confidence: Decimal = Decimal("0.3")


class ExchangeFees(BaseSettings):
    """
    Fee rates per exchange as decimal fractions.
    ESTIMATE — actual rates depend on VIP tier.
    """

    model_config = SettingsConfigDict(**_ENV_FILE_CONFIG)

    # Binance: 0.02% maker, 0.04% taker
    binance_maker: Decimal = Decimal("0.0002")
    binance_taker: Decimal = Decimal("0.0004")

    # Hyperliquid: 0.02% maker, 0.05% taker
    hyperliquid_maker: Decimal = Decimal("0.0002")
    hyperliquid_taker: Decimal = Decimal("0.0005")

    # Gate: 0.015% maker, 0.05% taker
    gate_maker: Decimal = Decimal("0.00015")
    gate_taker: Decimal = Decimal("0.0005")

    # Bybit: 0.02% maker, 0.055% taker  # ESTIMATE
    bybit_maker: Decimal = Decimal("0.0002")
    bybit_taker: Decimal = Decimal("0.00055")

    # OKX: 0.02% maker, 0.05% taker  # ESTIMATE
    okx_maker: Decimal = Decimal("0.0002")
    okx_taker: Decimal = Decimal("0.0005")

    # Bitget: 0.02% maker, 0.06% taker  # ESTIMATE
    bitget_maker: Decimal = Decimal("0.0002")
    bitget_taker: Decimal = Decimal("0.0006")

    # Aster: 0.02% maker, 0.05% taker  # ESTIMATE
    aster_maker: Decimal = Decimal("0.0002")
    aster_taker: Decimal = Decimal("0.0005")

    # Lighter: 0.02% maker, 0.05% taker  # ESTIMATE
    lighter_maker: Decimal = Decimal("0.0002")
    lighter_taker: Decimal = Decimal("0.0005")

    # MEXC: 0.02% maker, 0.06% taker  # ESTIMATE
    mexc_maker: Decimal = Decimal("0.0002")
    mexc_taker: Decimal = Decimal("0.0006")

    # Slippage factor (fraction of mid price)
    # ESTIMATE — small caps will have higher slippage
    slippage_factor: Decimal = Decimal("0.0001")


class AdapterSettings(BaseSettings):
    """Exchange adapter connection settings."""

    model_config = SettingsConfigDict(**_ENV_FILE_CONFIG)

    # Stale feed detection threshold (seconds)
    stale_threshold_seconds: float = 10.0

    # Heartbeat check interval (seconds)
    heartbeat_interval_seconds: float = 30.0

    # Meta/REST polling interval for funding/mark prices (seconds)
    meta_poll_interval_seconds: float = 30.0


class Settings(BaseSettings):
    """
    Root settings object. Loads all config from environment variables and .env file.

    Usage:
        settings = Settings()
        print(settings.telegram.bot_token)
        print(settings.filters.min_gross_spread_bps)
    """

    model_config = SettingsConfigDict(**_ENV_FILE_CONFIG)

    # Sub-settings
    telegram: TelegramSettings = Field(default_factory=TelegramSettings)
    filters: FilterSettings = Field(default_factory=FilterSettings)
    fees: ExchangeFees = Field(default_factory=ExchangeFees)
    adapter: AdapterSettings = Field(default_factory=AdapterSettings)

    # Logging
    log_level: str = "INFO"

    # Exchanges to enable (subset of: binance, hyperliquid, gate)
    enabled_exchanges: list[str] = Field(
        default_factory=lambda: ["binance", "hyperliquid", "gate", "mexc"]
    )

    # Market cap filter — targets small-cap tokens
    max_market_cap: int = 200_000_000  # $200M — above this, skip token
    mcap_refresh_interval: int = 1800  # 30 minutes
