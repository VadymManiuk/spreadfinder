# Spread Scanner Bot

Real-time crypto futures spread scanner. Monitors perpetual markets on **Binance**, **Hyperliquid**, and **Gate**. Detects cross-exchange spread opportunities and sends **Telegram alerts**.

**Target:** Small-cap tokens (<$200M market cap). Alerting only — no execution.

## Quick Start

### 1. Install

```bash
pip install -e ".[dev]"
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env — set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID
```

### 3. Run

```bash
python -m main
```

### 4. Run Tests

```bash
pytest
```

## Docker

```bash
docker compose up -d
```

## systemd

```bash
sudo cp spread-scanner.service /etc/systemd/system/
sudo systemctl enable --now spread-scanner
```

## Architecture

```
Exchange Adapters (WS) → Symbol Mapper → MarketSnapshot → Spread Engine → Filters → Telegram
```

| Component | Directory | Description |
|-----------|-----------|-------------|
| Models | `models/` | `MarketSnapshot`, `SpreadOpportunity` (Pydantic, frozen, Decimal) |
| Symbol Mapper | `symbol_mapper/` | Cross-exchange normalization to `{BASE}-{QUOTE}-PERP` |
| Exchange Adapters | `exchange_adapters/` | Binance, Hyperliquid, Gate — WebSocket + REST |
| Spread Engine | `spread_engine/` | Gross/net spread calculation, confidence scoring |
| Filters | `filters/` | Configurable quality gates, cooldowns, persistence |
| Alerting | `alerting/` | Telegram MarkdownV2 alerts |
| Config | `config/` | pydantic-settings, loads from `.env` |
| Utils | `utils/` | structlog setup, exponential backoff |

## Spread Formulas

```
gross_spread       = sell_bid - buy_ask
gross_spread_bps   = (gross_spread / buy_ask) * 10000
net_spread         = gross_spread - estimated_fees - estimated_slippage
estimated_fees     = (taker_fee_buy + maker_fee_sell) * mid_price * 2
estimated_slippage = slippage_factor * mid_price
```

Both directions (A→B and B→A) are checked for every symbol pair.

## Configuration

All settings are in `.env`. See `.env.example` for the full list with defaults.

| Setting | Default | Description |
|---------|---------|-------------|
| `MIN_GROSS_SPREAD_BPS` | 10.0 | Minimum gross spread in basis points |
| `MAX_GROSS_SPREAD_BPS` | 500.0 | Maximum gross spread allowed before treating it as bad/stale data |
| `MIN_NET_SPREAD_BPS` | 5.0 | Minimum net spread after costs |
| `COOLDOWN_SECONDS` | 300 | Alert cooldown per symbol+direction |
| `PERSISTENCE_MS` | 1000 | Spread must persist this long before alert |
| `MAX_DATA_AGE_MS` | 2000 | Maximum acceptable data staleness |
| `MIN_CONFIDENCE` | 0.3 | Minimum confidence score (0.0–1.0) |

## License

Private — not for redistribution.
