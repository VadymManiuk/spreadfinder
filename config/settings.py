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


class PumpSettings(BaseSettings):
    """
    Price pump/dump alert configuration.

    Detects when a token's mid price changes by more than `min_change_pct`
    over the last `window_minutes`. Filtered by 24h volume and market cap.
    """

    model_config = SettingsConfigDict(env_prefix="PUMP_", **_ENV_FILE_CONFIG)

    # Master switch
    enabled: bool = True

    # Detection thresholds
    min_change_pct: Decimal = Decimal("5.0")        # 5% move triggers alert
    window_minutes: int = 60                          # measured over the last 60 minutes
    check_interval_seconds: int = 30                  # how often the detection loop runs

    # Liquidity / market filters
    min_volume_24h: Decimal = Decimal("100000")       # at least $100K 24h volume
    min_market_cap: int = 0                           # no floor by default
    max_market_cap: int = 500_000_000                 # ignore tokens above $500M

    # Spam prevention
    cooldown_seconds: int = 1800                      # 30 min between alerts per token+direction

    # History retention — keep at least window + buffer
    history_retention_minutes: int = 180


class AdapterSettings(BaseSettings):
    """Exchange adapter connection settings."""

    model_config = SettingsConfigDict(**_ENV_FILE_CONFIG)

    # Stale feed detection threshold (seconds)
    stale_threshold_seconds: float = 10.0

    # Heartbeat check interval (seconds)
    heartbeat_interval_seconds: float = 30.0

    # Meta/REST polling interval for funding/mark prices (seconds)
    meta_poll_interval_seconds: float = 30.0


class DexSettings(BaseSettings):
    """
    DEX-to-futures alert configuration.

    DEX sources are polled via REST and compared against live futures quotes.
    """

    model_config = SettingsConfigDict(env_prefix="DEX_", **_ENV_FILE_CONFIG)

    # Master switch for DEX spread alert delivery
    enabled: bool = True

    # Source toggles
    okx_enabled: bool = True
    binance_alpha_enabled: bool = True

    # Polling cadence
    poll_interval_seconds: int = 30

    # DEX route quality thresholds
    min_net_spread_pct: Decimal = Decimal("10.0")
    min_volume_24h: Decimal = Decimal("2000000")

    # Comma-separated OKX chain indices for the top-volume scan
    # Base default matches the user-requested example.
    okx_chain_indices: str = "8453"


class OkxAuthSettings(BaseSettings):
    """
    Shared OKX API credentials.

    Used for both wallet/asset endpoints and authenticated DEX Market API calls.
    """

    model_config = SettingsConfigDict(env_prefix="OKX_", **_ENV_FILE_CONFIG)

    api_key: str = ""
    api_secret: str = ""
    passphrase: str = ""
    project_id: str = ""


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
    pump: PumpSettings = Field(default_factory=PumpSettings)
    dex: DexSettings = Field(default_factory=DexSettings)
    okx_auth: OkxAuthSettings = Field(default_factory=OkxAuthSettings)

    # Logging
    log_level: str = "INFO"

    # Exchanges to enable (subset of: binance, hyperliquid, gate)
    enabled_exchanges: list[str] = Field(
        default_factory=lambda: ["binance", "hyperliquid", "gate", "mexc"]
    )

    # Market cap filter — targets small-cap tokens
    max_market_cap: int = 200_000_000  # $200M — above this, skip token
    mcap_refresh_interval: int = 1800  # 30 minutes
