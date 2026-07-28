"""
Tests for pump/dump detector history inputs and false-alert protections.

Inputs: Synthetic MarketSnapshot sequences and PumpDetector scans.
Outputs: Confidence that pump detection ignores DEX aggregators and uses
         futures reference prices instead of distorted book mid prices.
Assumptions:
  - Pump alerts should be driven by perp mark/index prices.
  - DEX aggregators are excluded from pump/dump triggers.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from models.snapshot import MarketSnapshot
from pump_detector import PriceHistory, PumpDetector


def _snapshot(
    *,
    exchange: str,
    bid: str,
    ask: str,
    ts: datetime,
    mark: str | None = None,
    index: str | None = None,
    volume: str = "1000000",
) -> MarketSnapshot:
    return MarketSnapshot(
        canonical_symbol="AVNT-USDT-PERP",
        exchange=exchange,
        bid=Decimal(bid),
        ask=Decimal(ask),
        bid_size=Decimal("1000"),
        ask_size=Decimal("1000"),
        exchange_ts=ts,
        local_ts=ts,
        mark_price=Decimal(mark) if mark is not None else None,
        index_price=Decimal(index) if index is not None else None,
        volume_24h=Decimal(volume),
        is_stale=False,
    )


def test_price_history_uses_mark_price_not_book_mid() -> None:
    history = PriceHistory(retention_minutes=180)
    base = "AVNT"
    start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    history.record(
        base,
        _snapshot(
            exchange="aster",
            bid="0.10",
            ask="0.30",
            mark="0.15",
            index="0.149",
            ts=start,
        ),
    )
    history.record(
        base,
        _snapshot(
            exchange="aster",
            bid="0.11",
            ask="0.39",
            mark="0.151",
            index="0.1505",
            ts=start + timedelta(minutes=30),
        ),
    )

    result = history.get_window_change(
        base,
        "aster",
        window_seconds=31 * 60,
        now=start + timedelta(minutes=30),
    )

    assert result is not None
    start_price, current_price, *_ = result
    assert start_price == Decimal("0.15")
    assert current_price == Decimal("0.151")


def test_price_history_throttles_samples_but_keeps_latest_snapshot() -> None:
    history = PriceHistory(
        retention_minutes=90,
        sample_interval_seconds=10,
        max_samples_per_series=600,
    )
    base = "AVNT"
    start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    history.record(
        base,
        _snapshot(
            exchange="aster",
            bid="0.14",
            ask="0.16",
            mark="0.15",
            ts=start,
        ),
    )
    latest = _snapshot(
        exchange="aster",
        bid="0.15",
        ask="0.17",
        mark="0.16",
        ts=start + timedelta(seconds=5),
    )
    history.record(base, latest)

    stats = history.stats()
    assert stats.sample_count == 1
    assert stats.throttled_samples == 1
    assert history.latest_snapshots_for_base(base)["aster"] is latest


def test_price_history_enforces_hard_cap_per_series() -> None:
    history = PriceHistory(
        retention_minutes=90,
        sample_interval_seconds=1,
        max_samples_per_series=5,
    )
    start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    for offset in range(10):
        history.record(
            "AVNT",
            _snapshot(
                exchange="aster",
                bid="0.14",
                ask="0.16",
                mark=str(Decimal("0.15") + Decimal(offset) / 1000),
                ts=start + timedelta(seconds=offset),
            ),
        )

    stats = history.stats()
    assert stats.series_count == 1
    assert stats.sample_count == 5
    assert stats.max_samples_per_series == 5


def test_price_history_trims_expired_samples() -> None:
    history = PriceHistory(
        retention_minutes=1,
        sample_interval_seconds=1,
        max_samples_per_series=100,
        window_buffer_minutes=0,
    )
    start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    for offset in (0, 30, 61):
        history.record(
            "AVNT",
            _snapshot(
                exchange="aster",
                bid="0.14",
                ask="0.16",
                mark="0.15",
                ts=start + timedelta(seconds=offset),
            ),
        )

    assert history.stats().sample_count == 2


def test_price_history_prunes_inactive_series() -> None:
    history = PriceHistory(
        retention_minutes=1,
        sample_interval_seconds=1,
        max_samples_per_series=100,
        window_buffer_minutes=0,
    )
    start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    history.record(
        "AVNT",
        _snapshot(
            exchange="aster",
            bid="0.14",
            ask="0.16",
            mark="0.15",
            ts=start,
        ),
    )

    history.prune(start + timedelta(seconds=61))

    assert history.stats().series_count == 0
    assert history.stats().sample_count == 0
    assert history.known_bases() == []


def test_price_history_adapts_cadence_for_four_hour_window() -> None:
    history = PriceHistory(
        retention_minutes=90,
        sample_interval_seconds=10,
        max_samples_per_series=600,
        window_buffer_minutes=30,
    )

    history.configure_window(240)

    stats = history.stats()
    assert stats.retention_minutes == 270
    assert stats.sample_interval_seconds == 28
    assert stats.max_samples_per_series == 600


def test_price_history_ignores_dex_aggregator_snapshots() -> None:
    history = PriceHistory(retention_minutes=180)
    base = "BOB"
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    history.record(
        base,
        _snapshot(
            exchange="binance_alpha:56",
            bid="0.00697222",
            ask="0.00697222",
            ts=now,
            volume="157200",
        ),
    )

    assert history.known_bases() == []
    assert history.latest_snapshots_for_base(base) == {}


def test_pump_detector_does_not_alert_on_dex_only_dump() -> None:
    history = PriceHistory(retention_minutes=180)
    detector = PumpDetector(
        history=history,
        min_change_pct=Decimal("5"),
        window_minutes=30,
        min_volume_24h=Decimal("100000"),
        cooldown_seconds=1800,
    )

    base = "BROCCOLI"
    start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    history.record(
        base,
        _snapshot(
            exchange="binance_alpha:56",
            bid="0.00430645",
            ask="0.00430645",
            ts=start,
            volume="272600",
        ),
    )
    history.record(
        base,
        _snapshot(
            exchange="binance_alpha:56",
            bid="0.00159088",
            ask="0.00159088",
            ts=start + timedelta(minutes=30),
            volume="272600",
        ),
    )

    assert detector.scan(now=start + timedelta(minutes=30)) == []


def test_pump_detector_alert_table_uses_reference_prices() -> None:
    history = PriceHistory(retention_minutes=180)
    detector = PumpDetector(
        history=history,
        min_change_pct=Decimal("5"),
        window_minutes=30,
        min_volume_24h=Decimal("100000"),
        cooldown_seconds=1800,
    )

    base = "AVNT"
    start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    history.record(
        base,
        _snapshot(
            exchange="aster",
            bid="0.10",
            ask="0.30",
            mark="0.145",
            index="0.145",
            ts=start,
            volume="200000",
        ),
    )
    history.record(
        base,
        _snapshot(
            exchange="aster",
            bid="0.11",
            ask="0.39",
            mark="0.160",
            index="0.160",
            ts=start + timedelta(minutes=30),
            volume="200000",
        ),
    )
    history.record(
        base,
        _snapshot(
            exchange="bybit",
            bid="0.159",
            ask="0.161",
            mark="0.160",
            index="0.160",
            ts=start + timedelta(minutes=30),
            volume="5000000",
        ),
    )

    alerts = detector.scan(now=start + timedelta(minutes=30))

    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.triggered_on == "aster"
    assert alert.current_price == Decimal("0.160")
    assert alert.exchange_prices["aster"] == Decimal("0.160")
    assert alert.exchange_prices["bybit"] == Decimal("0.160")


def test_pump_detector_detects_sixty_minute_move_with_throttled_history() -> None:
    history = PriceHistory(
        retention_minutes=90,
        sample_interval_seconds=10,
        max_samples_per_series=600,
    )
    detector = PumpDetector(
        history=history,
        min_change_pct=Decimal("5"),
        window_minutes=60,
        min_volume_24h=Decimal("100000"),
        cooldown_seconds=1800,
    )
    start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    for offset in range(0, 3601):
        mark = Decimal("1.0") + Decimal(offset) / Decimal("36000")
        history.record(
            "AVNT",
            _snapshot(
                exchange="aster",
                bid=str(mark - Decimal("0.001")),
                ask=str(mark + Decimal("0.001")),
                mark=str(mark),
                ts=start + timedelta(seconds=offset),
            ),
        )

    alerts = detector.scan(now=start + timedelta(minutes=60))

    assert len(alerts) == 1
    assert alerts[0].change_pct == Decimal("10.0")
    assert history.stats().sample_count == 361
    assert history.stats().throttled_samples == 3240
