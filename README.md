# Spread Scanner Bot

Real-time crypto spread scanner. Monitors perpetual markets on **Binance**, **Hyperliquid**, and **Gate**, and can also compare **DEX spot/aggregator prices** against futures. Detects spread opportunities and sends **Telegram alerts**.

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

## Auto Deploy

After you push `main` to GitHub, the VPS can pull and restart automatically with a systemd timer:

```bash
sudo cp deploy/spread-scanner-autodeploy.service /etc/systemd/system/
sudo cp deploy/spread-scanner-autodeploy.timer /etc/systemd/system/
sudo cp deploy/auto-deploy.sh /root/spreadfinder/deploy/auto-deploy.sh
sudo chmod +x /root/spreadfinder/deploy/auto-deploy.sh
sudo systemctl daemon-reload
sudo systemctl enable --now spread-scanner-autodeploy.timer
```

The timer checks `origin/main` every 5 minutes. On a new revision it runs `pip install -e .`, `pytest`, then restarts `spread-scanner.service`. If deploy validation fails, it rolls back to the previous revision.

## Manual VPS Deploy

To force the VPS to catch up to every commit missing from `origin/main`, run:

```bash
./deploy/pull-latest-to-vps.sh
```

The script prints the commits missing on the VPS, fast-forwards the remote checkout with `git pull --ff-only`, runs `pytest`, restarts `spread-scanner.service`, and prints the final deployed revision. It uses normal `ssh`, so it works with either SSH keys or an interactive password prompt.

## Architecture

```
Exchange Adapters / DEX Pollers → Symbol Mapper → MarketSnapshot → Spread Engine → Filters → Telegram
```

| Component | Directory | Description |
|-----------|-----------|-------------|
| Models | `models/` | `MarketSnapshot`, `SpreadOpportunity` (Pydantic, frozen, Decimal) |
| Symbol Mapper | `symbol_mapper/` | Cross-exchange normalization to `{BASE}-{QUOTE}-PERP` |
| Exchange Adapters | `exchange_adapters/` | Futures WebSocket adapters plus DEX REST pollers |
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

For `DEX -> futures` routes, the bot only alerts on the actionable direction:
buy on DEX, sell/short on futures.

## Configuration

All settings are in `.env`. See `.env.example` for the full list with defaults.

| Setting | Default | Description |
|---------|---------|-------------|
| `MIN_GROSS_SPREAD_BPS` | 10.0 | Minimum gross spread in basis points |
| `MAX_GROSS_SPREAD_BPS` | 500.0 | Maximum gross spread allowed before treating it as bad/stale data |
| `MIN_NET_SPREAD_BPS` | 5.0 | Minimum net spread after costs |
| `COOLDOWN_SECONDS` | 300 | Alert cooldown per symbol+direction |
| `PERSISTENCE_MS` | 20000 | Spread must persist this long before alert |
| `MAX_DATA_AGE_MS` | 2000 | Maximum acceptable data staleness |
| `MIN_CONFIDENCE` | 0.3 | Minimum confidence score (0.0–1.0) |

### Dedicated Pump/Dump Bot

Pump and dump alerts can be sent through a separate Telegram bot while keeping
spread/DEX controls on the main bot.

```bash
PUMP_TELEGRAM_BOT_TOKEN=...
PUMP_TELEGRAM_CHAT_ID=...
```

Both dedicated bot values are required. Pump alerts never fall back to the
spread bot. The secondary bot is send-only: `/pump` controls and the
interactive panels stay on the main bot.

Pump/dump detection uses futures reference prices (`mark_price`, fallback
`index_price`) instead of raw order-book mid, and ignores DEX aggregator
sources like Binance Alpha / OKX DEX as standalone triggers. This avoids false
alerts from thin books or noisy spot aggregator quotes.

Pump history stores at most one sample every 10 seconds and at most 600
samples per token+exchange series. The cadence automatically becomes coarser
for long windows such as 4 hours, keeping memory bounded. Live spread
snapshots are still updated on every tick. Disabling pump alerts clears pump
history immediately; after re-enabling, detection needs time to warm up.

| Setting | Default | Description |
|---------|---------|-------------|
| `PUMP_HISTORY_RETENTION_MINUTES` | `90` | Minimum retained history |
| `PUMP_HISTORY_SAMPLE_INTERVAL_SECONDS` | `10` | Minimum sample cadence |
| `PUMP_HISTORY_MAX_SAMPLES_PER_SERIES` | `600` | Hard cap per token+exchange |
| `PUMP_HISTORY_WINDOW_BUFFER_MINUTES` | `30` | Extra retention beyond the active window |

### DEX Alert Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `DEX_ENABLED` | `true` | Master switch for DEX -> futures alerts |
| `DEX_OKX_ENABLED` | `true` | Enable authenticated OKX DEX volume scan |
| `DEX_BINANCE_ALPHA_ENABLED` | `true` | Enable Binance Alpha token scan |
| `DEX_POLL_INTERVAL_SECONDS` | `30` | REST polling cadence for DEX sources |
| `DEX_MIN_NET_SPREAD_PCT` | `10.0` | Minimum DEX route net spread in percent |
| `DEX_MIN_VOLUME_24H` | `2000000` | Minimum DEX-side 24h volume in USD |
| `DEX_OKX_CHAIN_INDICES` | `8453` | Comma-separated OKX chain indices to scan |

### OKX Credentials

OKX DEX Market API is authenticated. Add the following to `.env` to enable the OKX DEX source:

```bash
OKX_API_KEY=...
OKX_API_SECRET=...
OKX_PASSPHRASE=...
# Optional for some projects/endpoints:
OKX_PROJECT_ID=...
```

These are OKX Web3 Developer Portal credentials. The optional regular-exchange
asset status check uses separate read-only variables:

```bash
OKX_EXCHANGE_API_KEY=...
OKX_EXCHANGE_API_SECRET=...
OKX_EXCHANGE_PASSPHRASE=...
```

Binance Alpha uses public endpoints and does not need credentials.

## License

Private — not for redistribution.
