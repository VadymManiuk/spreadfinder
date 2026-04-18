"""
Tests for scanner startup health checks.
"""

import pytest

from config.settings import Settings
from main import SpreadScanner


def _make_scanner(exchanges: list[str]) -> SpreadScanner:
    return SpreadScanner(Settings(enabled_exchanges=exchanges))


def test_bootstrap_health_rejects_heavily_degraded_startup():
    exchanges = [
        "binance",
        "hyperliquid",
        "gate",
        "bybit",
        "okx",
        "bitget",
        "aster",
        "lighter",
    ]
    scanner = _make_scanner(exchanges)

    scanner._mapper.load_static("aster", {"MEGA": "MEGA-USDT-PERP"})
    scanner._mapper.load_static("lighter", {"MEGA": "MEGA-USDC-PERP"})
    scanner._mapper._bootstrap_errors["aster"] = None
    scanner._mapper._bootstrap_errors["lighter"] = None

    for exchange in exchanges[:-2]:
        scanner._mapper._native_to_canonical[exchange] = {}
        scanner._mapper._canonical_to_native[exchange] = {}
        scanner._mapper._bootstrap_errors[exchange] = "dns failure"

    with pytest.raises(RuntimeError, match="bootstrap too degraded"):
        scanner._validate_bootstrap_health()


def test_bootstrap_health_allows_majority_of_exchanges_ready():
    exchanges = [
        "binance",
        "hyperliquid",
        "gate",
        "bybit",
        "okx",
        "bitget",
        "aster",
        "lighter",
    ]
    scanner = _make_scanner(exchanges)

    for exchange in exchanges[:5]:
        scanner._mapper.load_static(exchange, {"MEGA": "MEGA-USDT-PERP"})
        scanner._mapper._bootstrap_errors[exchange] = None

    for exchange in exchanges[5:]:
        scanner._mapper._native_to_canonical[exchange] = {}
        scanner._mapper._canonical_to_native[exchange] = {}
        scanner._mapper._bootstrap_errors[exchange] = "dns failure"

    scanner._validate_bootstrap_health()
