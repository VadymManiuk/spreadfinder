# Spread Scanner Bot

## Purpose
Real-time crypto futures spread scanner. Monitors perpetual markets on Binance, Hyperliquid, and Gate. Detects cross-exchange spread opportunities and sends Telegram alerts. Alerting only — no execution.

**Target: small-cap tokens (<$200M market cap).** Large-cap tokens ($1B+) are out of scope — their spreads are too tight. The bot focuses on long-tail assets where cross-exchange pricing inefficiency exists.

## Architecture
Modular, event-driven async pipeline:

```
Exchange Adapters (WS+REST) → Symbol Mapper → MarketSnapshot → Spread Engine → Filters → Telegram Alerts
```

### Directory Structure
- `models/` — Pydantic data models (MarketSnapshot, SpreadOpportunity)
- `symbol_mapper/` — Cross-exchange symbol normalization to `{BASE}-{QUOTE}-PERP`
- `exchange_adapters/` — One file per exchange (base.py, binance.py, hyperliquid.py, gate.py)
- `spread_engine/` — Spread calculation and confidence scoring
- `filters/` — Configurable opportunity quality filters
- `alerting/` — Telegram sender and message formatter
- `config/` — Settings and thresholds (pydantic-settings)
- `utils/` — Logging setup, reconnect helpers
- `tests/` — pytest tests

## Stack
- Python 3.12+
- asyncio, websockets, aiohttp, httpx
- pydantic v2 (frozen models, Decimal for prices)
- structlog for structured logging
- python-dotenv for secrets

## Coding Conventions
- One concern per file. No monoliths.
- Every file: top comment with what it does, inputs, outputs, assumptions.
- Every formula: commented with the formula itself.
- Mark rough estimates with `# ESTIMATE — refine later`
- Mark exchange-specific uncertainties with `# TODO`
- Prefer explicit over clever. Beginner-readable.
- Use `Decimal` for all price/size fields (financial accuracy).
- Frozen pydantic models for immutability.
- Type hints on all functions.

## Key Commands
```bash
# Install dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run the scanner
python -m main
```

## Build Order
1. models/ → 2. symbol_mapper/ → 3. Binance adapter + utils/ → 4. spread_engine/ → 5. filters/ → 6. alerting/ → 7. Hyperliquid adapter → 8. Gate adapter → 9. config/ → 10. main.py + Docker

## Secrets
All secrets via `.env` (see `.env.example`). Never hardcode tokens or keys.
