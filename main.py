"""
Spread Scanner Bot — Main orchestrator.

Wires together all components: exchange adapters, symbol mapper,
spread engine, filters, and Telegram alerting.

Inputs: Configuration from .env / environment variables.
Outputs: Telegram alerts for detected spread opportunities.
Assumptions:
  - Runs as a long-lived async process.
  - Handles SIGINT/SIGTERM for graceful shutdown.
  - Targets small-cap tokens (<$200M market cap).
"""

import asyncio
import signal
import sys
from datetime import datetime, timezone
from decimal import Decimal

import structlog

from config.settings import Settings
from utils.logging import setup_logging
from symbol_mapper.mapper import SymbolMapper
from exchange_adapters.binance import BinanceAdapter
from exchange_adapters.hyperliquid import HyperliquidAdapter
from exchange_adapters.gate import GateAdapter
from spread_engine.calculator import calculate_spread
from filters.filter_chain import FilterChain
from alerting.telegram import TelegramSender
from models.snapshot import MarketSnapshot

logger = structlog.get_logger(__name__)


class SpreadScanner:
    """
    Main application class that orchestrates all components.

    Flow:
      1. Bootstrap symbol mapper (fetch symbol lists from exchanges)
      2. Start exchange adapters (WebSocket connections)
      3. On each snapshot: check for spread opportunities across exchanges
      4. Filter opportunities and send Telegram alerts
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self._running = False

        # Latest snapshot per (exchange, canonical_symbol)
        self._snapshots: dict[tuple[str, str], MarketSnapshot] = {}

        # Components
        self._mapper = SymbolMapper(exchanges=settings.enabled_exchanges)
        self._filter_chain = FilterChain(
            min_gross_spread_bps=settings.filters.min_gross_spread_bps,
            min_net_spread_bps=settings.filters.min_net_spread_bps,
            min_bid_size=settings.filters.min_bid_size,
            min_ask_size=settings.filters.min_ask_size,
            min_volume_24h=settings.filters.min_volume_24h,
            max_data_age_ms=settings.filters.max_data_age_ms,
            min_confidence=settings.filters.min_confidence,
            cooldown_seconds=settings.filters.cooldown_seconds,
            persistence_ms=settings.filters.persistence_ms,
        )
        self._telegram = TelegramSender(
            bot_token=settings.telegram.bot_token,
            chat_id=settings.telegram.chat_id,
        )
        self._adapters: list = []

    async def _on_snapshot(self, snapshot: MarketSnapshot) -> None:
        """
        Callback invoked by exchange adapters on each new market snapshot.

        Stores the snapshot and checks for spread opportunities against
        all other exchanges that have data for the same symbol.
        """
        key = (snapshot.exchange, snapshot.canonical_symbol)
        self._snapshots[key] = snapshot

        # Find snapshots from other exchanges for the same symbol
        for (other_exchange, other_symbol), other_snap in self._snapshots.items():
            if other_symbol != snapshot.canonical_symbol:
                continue
            if other_exchange == snapshot.exchange:
                continue

            # Calculate spreads in both directions
            opportunities = calculate_spread(snapshot, other_snap)

            for opp in opportunities:
                passed, results = self._filter_chain.evaluate(opp)
                if passed:
                    self._filter_chain.record_alert(opp)
                    logger.info(
                        "spread_alert",
                        symbol=opp.canonical_symbol,
                        buy=opp.buy_exchange,
                        sell=opp.sell_exchange,
                        gross_bps=float(opp.gross_spread_bps),
                        net_bps=float(opp.net_spread_bps),
                        confidence=float(opp.confidence),
                    )
                    await self._telegram.send_alert(opp)

    def _build_adapters(self) -> None:
        """Create exchange adapters based on symbol mapper results."""
        for exchange in self.settings.enabled_exchanges:
            symbols_canonical = self._mapper.get_exchange_symbols(exchange)
            if not symbols_canonical:
                logger.warning("no_symbols_for_exchange", exchange=exchange)
                continue

            # Convert canonical symbols to native format for the adapter
            native_symbols = []
            canonical_map = {}
            for canonical in symbols_canonical:
                native = self._mapper.to_native(exchange, canonical)
                if native:
                    native_symbols.append(native)
                    canonical_map[native] = canonical

            if not native_symbols:
                continue

            stale_threshold = self.settings.adapter.stale_threshold_seconds

            if exchange == "binance":
                adapter = BinanceAdapter(
                    symbols=native_symbols,
                    on_snapshot=self._on_snapshot,
                    canonical_map=canonical_map,
                    stale_threshold_seconds=stale_threshold,
                )
            elif exchange == "hyperliquid":
                adapter = HyperliquidAdapter(
                    symbols=native_symbols,
                    on_snapshot=self._on_snapshot,
                    canonical_map=canonical_map,
                    stale_threshold_seconds=stale_threshold,
                )
            elif exchange == "gate":
                adapter = GateAdapter(
                    symbols=native_symbols,
                    on_snapshot=self._on_snapshot,
                    canonical_map=canonical_map,
                    stale_threshold_seconds=stale_threshold,
                )
            else:
                logger.warning("unknown_exchange_adapter", exchange=exchange)
                continue

            self._adapters.append(adapter)
            logger.info(
                "adapter_created",
                exchange=exchange,
                symbol_count=len(native_symbols),
            )

    async def start(self) -> None:
        """Bootstrap and run the scanner."""
        self._running = True
        logger.info("scanner_starting", exchanges=self.settings.enabled_exchanges)

        # Step 1: Bootstrap symbol mapper
        logger.info("bootstrapping_symbol_mapper")
        await self._mapper.bootstrap()

        # Log pairwise common symbols
        pairwise = self._mapper.get_pairwise_common()
        for (ex_a, ex_b), symbols in pairwise.items():
            logger.info(
                "common_symbols",
                exchange_a=ex_a,
                exchange_b=ex_b,
                count=len(symbols),
                sample=sorted(symbols)[:5],
            )

        # Step 2: Build adapters
        self._build_adapters()

        if not self._adapters:
            logger.error("no_adapters_created", hint="Check exchange configs and symbol availability")
            return

        # Step 3: Run all adapters concurrently
        logger.info("starting_adapters", count=len(self._adapters))
        tasks = [asyncio.create_task(adapter.start()) for adapter in self._adapters]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("scanner_cancelled")

    async def stop(self) -> None:
        """Gracefully stop all adapters and clean up."""
        logger.info("scanner_stopping")
        self._running = False

        for adapter in self._adapters:
            await adapter.stop()

        await self._telegram.close()
        logger.info("scanner_stopped")


async def async_main() -> None:
    """Entry point for the async application."""
    settings = Settings()
    setup_logging(settings.log_level)

    scanner = SpreadScanner(settings)

    # Set up signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def handle_signal() -> None:
        logger.info("shutdown_signal_received")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    # Run scanner in background, wait for shutdown signal
    scanner_task = asyncio.create_task(scanner.start())

    # Wait for either scanner to finish or shutdown signal
    shutdown_waiter = asyncio.create_task(shutdown_event.wait())
    done, pending = await asyncio.wait(
        [scanner_task, shutdown_waiter],
        return_when=asyncio.FIRST_COMPLETED,
    )

    # Clean up
    for task in pending:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    await scanner.stop()


def main() -> None:
    """Synchronous entry point."""
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
