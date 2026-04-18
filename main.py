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

# Clear bytecode cache on startup to ensure fresh code always runs.
# This prevents stale .pyc files from overriding source changes.
import importlib
import os
import pathlib
import shutil

_project_root = pathlib.Path(__file__).parent
for _cache_dir in _project_root.rglob("__pycache__"):
    shutil.rmtree(_cache_dir, ignore_errors=True)

# ---- Single-instance lock ----
# Kill any other running instances of this bot before starting.
# This prevents duplicate alerts from multiple processes.
_pidfile = _project_root / ".bot.pid"
_my_pid = os.getpid()

def _kill_old_instances() -> None:
    """Kill any previously running bot instances using the PID file."""
    if _pidfile.exists():
        try:
            old_pid = int(_pidfile.read_text().strip())
            if old_pid != _my_pid:
                os.kill(old_pid, 9)
        except (ValueError, ProcessLookupError, PermissionError):
            pass
    # Also kill any other python -m main processes (belt + suspenders)
    import subprocess
    try:
        result = subprocess.run(
            ["pgrep", "-f", "python.*-m main"],
            capture_output=True, text=True
        )
        for line in result.stdout.strip().split("\n"):
            if line.strip():
                pid = int(line.strip())
                if pid != _my_pid:
                    try:
                        os.kill(pid, 9)
                    except (ProcessLookupError, PermissionError):
                        pass
    except Exception:
        pass
    _pidfile.write_text(str(_my_pid))

_kill_old_instances()
# ---- End single-instance lock ----

import asyncio
import math
import signal
import sys
import time as _time
from datetime import datetime, timezone
from decimal import Decimal

import structlog

from config.settings import Settings
from utils.logging import setup_logging
from utils.deposit_checker import DepositChecker
from symbol_mapper.mapper import SymbolMapper
from exchange_adapters.binance import BinanceAdapter
from exchange_adapters.hyperliquid import HyperliquidAdapter
from exchange_adapters.gate import GateAdapter
from exchange_adapters.bybit import BybitAdapter
from exchange_adapters.okx import OkxAdapter
from exchange_adapters.bitget import BitgetAdapter
from exchange_adapters.aster import AsterAdapter
from exchange_adapters.lighter import LighterAdapter
from exchange_adapters.mexc import MexcAdapter
from symbol_mapper.exchange_symbols import LIGHTER_MARKET_INDEX_MAP
from spread_engine.calculator import calculate_spread
from filters.filter_chain import FilterChain
from filters.market_cap_filter import MarketCapFilter
from alerting.telegram import TelegramSender
from models.snapshot import MarketSnapshot
from pump_detector import PriceHistory, PumpDetector

logger = structlog.get_logger(__name__)

# If bootstrap succeeds for too few enabled exchanges, the bot should fail
# fast and let systemd retry. Running for hours on 2/8 exchanges is worse
# than restarting until DNS / REST connectivity recovers.
_MIN_BOOTSTRAP_SUCCESS_COUNT = 2
_MIN_BOOTSTRAP_SUCCESS_RATIO = 0.6


class SpreadScanner:
    """
    Main application class that orchestrates all components.

    Flow:
      1. Bootstrap symbol mapper (fetch symbol lists from exchanges)
      2. Start exchange adapters (WebSocket connections)
      3. On each snapshot: check for spread opportunities across exchanges
      4. Filter opportunities and send Telegram alerts
    """

    # How long to batch routes for the same base token before sending
    # one combined alert. Allows multiple exchange pairs to accumulate.
    BATCH_WINDOW_SECONDS: float = 3.0

    def __init__(self, settings: Settings):
        self.settings = settings
        self._running = False

        # Latest snapshot per (exchange, canonical_symbol)
        self._snapshots: dict[tuple[str, str], MarketSnapshot] = {}

        # Snapshot throttle: don't recalculate spreads for the same key
        # more than once per second. Reduces CPU on high-frequency feeds.
        self._last_calc_time: dict[tuple[str, str], float] = {}
        self._min_calc_interval: float = 1.0  # seconds

        # ── Diagnostic counters (exposed via /status) ──────────────────
        self._start_time: float = _time.monotonic()
        self._diag = {
            "snapshots_total": 0,          # total snapshots received
            "spreads_calculated": 0,       # spread calculations run
            "spreads_passed_filters": 0,   # passed filter chain
            "spreads_sent": 0,             # sent via telegram
            "spreads_rejected_hard": 0,    # rejected by <1% hard check
            "spreads_rejected_filter": 0,  # rejected by filter chain
            "last_spread_alert_ts": None,  # datetime of last spread alert
            "last_spread_symbol": "",      # symbol of last spread alert
            "pumps_sent": 0,               # pump alerts sent
            "last_pump_ts": None,          # datetime of last pump alert
            "flush_errors": 0,             # errors in _flush_after_delay
        }

        # Cross-quote matchable pairs built after bootstrap.
        # Maps (exchange, canonical) -> list of (other_exchange, other_canonical)
        # to know which snapshots to compare for spreads.
        self._match_lookup: dict[tuple[str, str], list[tuple[str, str]]] = {}

        # Alert batching: accumulate routes per base token, flush after window.
        # base_token -> list of SpreadOpportunity
        self._alert_buffer: dict[str, list] = {}
        # base_token -> asyncio.Task that will flush after BATCH_WINDOW_SECONDS
        self._flush_tasks: dict[str, asyncio.Task] = {}

        # Deposit/withdrawal checker for alert enrichment
        self._deposit_checker = DepositChecker()

        # Components
        self._mapper = SymbolMapper(exchanges=settings.enabled_exchanges)
        self._filter_chain = FilterChain(
            min_gross_spread_bps=settings.filters.min_gross_spread_bps,
            max_gross_spread_bps=settings.filters.max_gross_spread_bps,
            min_net_spread_bps=settings.filters.min_net_spread_bps,
            min_bid_size=settings.filters.min_bid_size,
            min_ask_size=settings.filters.min_ask_size,
            min_volume_24h=settings.filters.min_volume_24h,
            max_data_age_ms=settings.filters.max_data_age_ms,
            min_confidence=settings.filters.min_confidence,
            cooldown_seconds=settings.filters.cooldown_seconds,
            persistence_ms=settings.filters.persistence_ms,
        )
        logger.info(
            "filter_chain_config",
            min_gross_spread_bps=float(settings.filters.min_gross_spread_bps),
            max_gross_spread_bps=float(settings.filters.max_gross_spread_bps),
            min_net_spread_bps=float(settings.filters.min_net_spread_bps),
        )
        self._telegram = TelegramSender(
            bot_token=settings.telegram.bot_token,
            chat_id=settings.telegram.chat_id,
        )
        self._adapters: list = []

        # Pump/dump detector
        self._mcap_filter = MarketCapFilter(
            max_mcap=settings.pump.max_market_cap,
            min_mcap=settings.pump.min_market_cap,
            refresh_interval=settings.mcap_refresh_interval,
        )
        self._price_history = PriceHistory(
            retention_minutes=settings.pump.history_retention_minutes,
        )
        self._pump_detector = PumpDetector(
            history=self._price_history,
            min_change_pct=settings.pump.min_change_pct,
            window_minutes=settings.pump.window_minutes,
            min_volume_24h=settings.pump.min_volume_24h,
            cooldown_seconds=settings.pump.cooldown_seconds,
            mcap_filter=self._mcap_filter,
            min_market_cap=settings.pump.min_market_cap,
            max_market_cap=settings.pump.max_market_cap,
        )
        self._pump_check_interval = settings.pump.check_interval_seconds
        self._pump_enabled = settings.pump.enabled
        self._pump_task: asyncio.Task | None = None
        # Expose detector + telegram sender so /pump commands can mutate
        self._telegram.pump_detector = self._pump_detector  # type: ignore[attr-defined]
        self._telegram._pump_enabled_setter = self._set_pump_enabled  # type: ignore[attr-defined]
        self._telegram._pump_enabled_getter = lambda: self._pump_enabled  # type: ignore[attr-defined]
        self._telegram._scanner_diag = self._diag  # type: ignore[attr-defined]
        self._telegram._scanner_ref = self  # type: ignore[attr-defined]

    def _set_pump_enabled(self, enabled: bool) -> None:
        """Toggle pump alerts at runtime (called from /pumpon, /pumpoff)."""
        was = self._pump_enabled
        self._pump_enabled = enabled
        if enabled and not was and self._running:
            if self._pump_task is None or self._pump_task.done():
                self._pump_task = asyncio.create_task(self._pump_check_loop())
        logger.info("pump_toggled", enabled=enabled)

    def _extract_base(self, canonical: str) -> str:
        """Extract base token from canonical symbol. 'POLYX-USDT-PERP' → 'POLYX'."""
        return canonical.split("-")[0] if "-" in canonical else canonical

    async def _on_snapshot(self, snapshot: MarketSnapshot) -> None:
        """
        Callback invoked by exchange adapters on each new market snapshot.

        Stores the snapshot and checks for spread opportunities against
        all matchable counterparts (including cross-quote like USDT↔USDC).
        Batches routes by base token and sends one grouped alert per token.
        """
        key = (snapshot.exchange, snapshot.canonical_symbol)
        self._snapshots[key] = snapshot
        self._diag["snapshots_total"] += 1

        # Record into pump-detector price history
        base_for_history = self._extract_base(snapshot.canonical_symbol)
        if base_for_history:
            self._price_history.record(base_for_history, snapshot)

        # Throttle: skip spread calculation if we just did it for this key
        now = _time.monotonic()
        last = self._last_calc_time.get(key, 0)
        if now - last < self._min_calc_interval:
            return
        self._last_calc_time[key] = now

        # Look up which (exchange, canonical) pairs we should compare against
        counterparts = self._match_lookup.get(key, [])

        for other_key in counterparts:
            other_snap = self._snapshots.get(other_key)
            if other_snap is None:
                continue

            # Calculate spreads in both directions
            # Note: snapshots may have different canonical_symbols (USDT vs USDC)
            # so we use a cross-quote aware calculation
            opportunities = calculate_spread(snapshot, other_snap)
            self._diag["spreads_calculated"] += len(opportunities)

            for opp in opportunities:
                # HARD SAFETY CHECK — never send alerts below 1% net spread
                # regardless of any other filter settings
                net_pct = float(opp.net_spread_bps) / 100.0
                if net_pct < 1.0:
                    self._diag["spreads_rejected_hard"] += 1
                    continue

                passed, results = self._filter_chain.evaluate(opp)
                if not passed:
                    self._diag["spreads_rejected_filter"] += 1
                    continue

                self._diag["spreads_passed_filters"] += 1

                # Record cooldown immediately so duplicate routes don't pass
                self._filter_chain.record_alert(opp)

                # Buffer the route — will be flushed as a grouped alert
                base = self._extract_base(opp.canonical_symbol)
                self._alert_buffer.setdefault(base, []).append(opp)

                logger.info(
                    "route_buffered",
                    base=base,
                    symbol=opp.canonical_symbol,
                    buy=opp.buy_exchange,
                    sell=opp.sell_exchange,
                    net_pct=round(net_pct, 2),
                    buffered_routes=len(self._alert_buffer[base]),
                )

                # Start a flush timer for this base token if not already running
                if base not in self._flush_tasks or self._flush_tasks[base].done():
                    self._flush_tasks[base] = asyncio.create_task(
                        self._flush_after_delay(base)
                    )

    async def _flush_after_delay(self, base: str) -> None:
        """
        Wait for the batch window, then send all buffered routes
        for this base token as one grouped Telegram alert.
        Enriches with deposit/withdrawal status and all exchange snapshots.
        """
        await asyncio.sleep(self.BATCH_WINDOW_SECONDS)

        routes = self._alert_buffer.pop(base, [])
        self._flush_tasks.pop(base, None)

        if not routes:
            return

        try:
            # Sort by net spread descending (best route first)
            routes.sort(key=lambda o: float(o.net_spread_bps), reverse=True)

            # Gather ALL exchange snapshots for this base token
            # Used by the formatter to build the "All exchanges" table
            all_snapshots: dict[str, MarketSnapshot] = {}
            for (ex, canonical), snap in list(self._snapshots.items()):
                snap_base = self._extract_base(canonical)
                if snap_base == base:
                    # If multiple canonicals per exchange (USDT vs USDC),
                    # keep the most recent one
                    existing = all_snapshots.get(ex)
                    if existing is None or snap.local_ts > existing.local_ts:
                        all_snapshots[ex] = snap

            # Build deposit status dict for all exchanges with snapshots
            deposit_status = {}
            all_exchanges = set(all_snapshots.keys())
            for opp in routes:
                all_exchanges.add(opp.buy_exchange)
                all_exchanges.add(opp.sell_exchange)
            for ex in all_exchanges:
                key = (ex, base)
                if key not in deposit_status:
                    deposit_status[key] = self._deposit_checker.get_status(ex, base)

            logger.info(
                "flushing_grouped_alert",
                base=base,
                route_count=len(routes),
                exchanges_with_snapshots=len(all_snapshots),
                best_net_pct=round(float(routes[0].net_spread_bps) / 100, 2),
            )

            sent = await self._telegram.send_grouped_alert(
                routes,
                deposit_status=deposit_status,
                all_snapshots=all_snapshots,
            )
            if sent:
                self._diag["spreads_sent"] += 1
                self._diag["last_spread_alert_ts"] = datetime.now(timezone.utc)
                self._diag["last_spread_symbol"] = base
        except Exception:
            self._diag["flush_errors"] += 1
            logger.exception("flush_alert_error", base=base, route_count=len(routes))

    async def _pump_check_loop(self) -> None:
        """Periodically scan price history and dispatch pump/dump alerts."""
        logger.info(
            "pump_loop_started",
            interval_s=self._pump_check_interval,
            min_change_pct=float(self._pump_detector.min_change_pct),
            window_s=self._pump_detector.window_seconds,
        )
        while self._running:
            try:
                await asyncio.sleep(self._pump_check_interval)
                if not self._pump_enabled:
                    continue
                alerts = self._pump_detector.scan()
                for alert in alerts:
                    logger.info(
                        "pump_alert",
                        base=alert.base,
                        direction=alert.direction,
                        change_pct=round(float(alert.change_pct), 2),
                        window_s=alert.window_seconds,
                        triggered_on=alert.triggered_on,
                    )
                    sent = await self._telegram.send_pump_alert(alert)
                    if sent:
                        self._diag["pumps_sent"] += 1
                        self._diag["last_pump_ts"] = datetime.now(timezone.utc)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("pump_loop_error")

    async def _supervise_adapter(self, adapter) -> None:
        """
        Wrapper that runs adapter.start() in a loop.
        If the adapter crashes for any reason, logs the error, waits,
        and restarts it. One flaky exchange can never kill the whole bot.
        """
        restart_delay = 5  # seconds between restarts
        while self._running:
            try:
                await adapter.start()
                # start() returned normally (shouldn't happen unless stopped)
                if not self._running:
                    break
                logger.warning(
                    "adapter_exited_unexpectedly",
                    exchange=adapter.exchange_name,
                    hint="restarting in 5s",
                )
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception(
                    "adapter_crashed",
                    exchange=adapter.exchange_name,
                    hint=f"restarting in {restart_delay}s",
                )
            if self._running:
                await asyncio.sleep(restart_delay)

    def _validate_bootstrap_health(self) -> None:
        """Abort startup if symbol bootstrap is too degraded to trade on."""
        enabled = list(self.settings.enabled_exchanges)
        ready = [
            exchange
            for exchange in enabled
            if self._mapper.get_exchange_symbols(exchange)
        ]
        failed = {
            exchange: self._mapper.get_bootstrap_error(exchange) or "0 symbols mapped"
            for exchange in enabled
            if not self._mapper.get_exchange_symbols(exchange)
        }
        min_required = min(
            len(enabled),
            max(
                _MIN_BOOTSTRAP_SUCCESS_COUNT,
                math.ceil(len(enabled) * _MIN_BOOTSTRAP_SUCCESS_RATIO),
            ),
        )

        logger.info(
            "bootstrap_health",
            enabled_count=len(enabled),
            ready_count=len(ready),
            min_required=min_required,
            ready=ready,
            failed=failed,
        )

        if len(ready) < min_required:
            raise RuntimeError(
                f"bootstrap too degraded: {len(ready)}/{len(enabled)} exchanges ready "
                f"(need at least {min_required})"
            )

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
            elif exchange == "bybit":
                adapter = BybitAdapter(
                    symbols=native_symbols,
                    on_snapshot=self._on_snapshot,
                    canonical_map=canonical_map,
                    stale_threshold_seconds=stale_threshold,
                )
            elif exchange == "okx":
                adapter = OkxAdapter(
                    symbols=native_symbols,
                    on_snapshot=self._on_snapshot,
                    canonical_map=canonical_map,
                    stale_threshold_seconds=stale_threshold,
                )
            elif exchange == "bitget":
                adapter = BitgetAdapter(
                    symbols=native_symbols,
                    on_snapshot=self._on_snapshot,
                    canonical_map=canonical_map,
                    stale_threshold_seconds=stale_threshold,
                )
            elif exchange == "aster":
                adapter = AsterAdapter(
                    symbols=native_symbols,
                    on_snapshot=self._on_snapshot,
                    canonical_map=canonical_map,
                    stale_threshold_seconds=stale_threshold,
                )
            elif exchange == "lighter":
                adapter = LighterAdapter(
                    symbols=native_symbols,
                    on_snapshot=self._on_snapshot,
                    canonical_map=canonical_map,
                    market_index_map=LIGHTER_MARKET_INDEX_MAP,
                    stale_threshold_seconds=stale_threshold,
                )
            elif exchange == "mexc":
                adapter = MexcAdapter(
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
        self._validate_bootstrap_health()

        # Step 1b: Build cross-quote matchable pairs (all tokens, no market cap filter)
        matchable = self._mapper.get_matchable_pairs()

        for pair in matchable:
            key_a = (pair["exchange_a"], pair["canonical_a"])
            key_b = (pair["exchange_b"], pair["canonical_b"])
            self._match_lookup.setdefault(key_a, []).append(key_b)
            self._match_lookup.setdefault(key_b, []).append(key_a)

        logger.info(
            "matchable_symbols",
            total=len(matchable),
            sample=[f"{p['base']} ({p['exchange_a']}↔{p['exchange_b']})" for p in matchable[:5]],
        )

        # Step 2: Build adapters (only subscribe to symbols in matchable pairs)
        self._build_adapters()

        if not self._adapters:
            raise RuntimeError("no adapters created from bootstrap results")

        # Step 3: Start deposit/withdrawal checker
        await self._deposit_checker.start()

        # Step 3b: Start market cap filter (used by pump detector)
        # Timeout: don't let CoinGecko block adapter startup
        try:
            await asyncio.wait_for(self._mcap_filter.start(), timeout=30)
        except asyncio.TimeoutError:
            logger.warning("mcap_filter_start_timeout", hint="CoinGecko unreachable, continuing without mcap data")
        except Exception:
            logger.exception("mcap_filter_start_error")

        # Step 3c: Start pump detector loop
        if self._pump_enabled:
            self._pump_task = asyncio.create_task(self._pump_check_loop())

        # Step 4: Start Telegram polling (for inline button callbacks)
        await self._telegram.start_polling()

        # Step 5: Run all adapters concurrently.
        # Each adapter is wrapped in a supervisor that catches any crash
        # and restarts that adapter independently, so one flaky exchange
        # can never kill the entire bot.
        logger.info("starting_adapters", count=len(self._adapters))
        tasks = [
            asyncio.create_task(self._supervise_adapter(adapter))
            for adapter in self._adapters
        ]

        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            logger.info("scanner_cancelled")

    async def stop(self) -> None:
        """Gracefully stop all adapters and clean up."""
        logger.info("scanner_stopping")
        self._running = False

        if self._pump_task:
            self._pump_task.cancel()
            try:
                await self._pump_task
            except asyncio.CancelledError:
                pass
            self._pump_task = None

        for adapter in self._adapters:
            await adapter.stop()

        try:
            await self._mcap_filter.stop()
        except Exception:
            logger.exception("mcap_filter_stop_error")

        await self._deposit_checker.stop()
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
    """Synchronous entry point. Auto-restarts on crash."""
    while True:
        try:
            asyncio.run(async_main())
            break  # clean shutdown — don't restart
        except KeyboardInterrupt:
            break
        except Exception:
            logger.exception("fatal_crash_restarting_in_10s")
            import time
            time.sleep(10)


if __name__ == "__main__":
    main()
