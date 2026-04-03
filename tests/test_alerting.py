"""
Tests for Telegram alerting: MarkdownV2 formatting and escaping.

Covers: escape_md2, format_alert output structure, field presence,
special character handling, edge cases.
"""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from alerting.formatter import escape_md2, format_alert, _fmt_volume, _fmt_funding
from models.snapshot import SpreadOpportunity


NOW = datetime(2026, 1, 1, 12, 30, 45, tzinfo=timezone.utc)


def make_opp(**overrides) -> SpreadOpportunity:
    defaults = {
        "canonical_symbol": "APE-USDT-PERP",
        "buy_exchange": "binance",
        "sell_exchange": "gate",
        "buy_ask": Decimal("1.2350"),
        "sell_bid": Decimal("1.2480"),
        "gross_spread": Decimal("0.0130"),
        "gross_spread_bps": Decimal("105.3"),
        "net_spread": Decimal("0.0090"),
        "net_spread_bps": Decimal("72.9"),
        "estimated_fees": Decimal("0.0030"),
        "estimated_slippage": Decimal("0.0010"),
        "buy_funding_rate": Decimal("0.0001"),
        "sell_funding_rate": Decimal("-0.0002"),
        "buy_ask_size": Decimal("500.50"),
        "sell_bid_size": Decimal("320.75"),
        "buy_volume_24h": Decimal("2500000"),
        "sell_volume_24h": Decimal("1800000"),
        "data_age_ms": 150,
        "confidence": Decimal("0.85"),
        "timestamp": NOW,
    }
    defaults.update(overrides)
    return SpreadOpportunity(**defaults)


# ---------------------------------------------------------------------------
# MarkdownV2 escaping
# ---------------------------------------------------------------------------

class TestEscapeMd2:

    def test_no_special_chars(self):
        assert escape_md2("hello") == "hello"

    def test_escapes_dots(self):
        assert escape_md2("1.23") == "1\\.23"

    def test_escapes_dashes(self):
        assert escape_md2("BTC-USDT-PERP") == "BTC\\-USDT\\-PERP"

    def test_escapes_parens(self):
        assert escape_md2("(test)") == "\\(test\\)"

    def test_escapes_pipes(self):
        assert escape_md2("a|b") == "a\\|b"

    def test_escapes_plus(self):
        assert escape_md2("+5.0") == "\\+5\\.0"

    def test_escapes_equals(self):
        assert escape_md2("a=b") == "a\\=b"

    def test_escapes_exclamation(self):
        assert escape_md2("alert!") == "alert\\!"

    def test_escapes_hash(self):
        assert escape_md2("#tag") == "\\#tag"

    def test_empty_string(self):
        assert escape_md2("") == ""

    def test_all_special_chars(self):
        # Every char in the escape set should get a backslash
        result = escape_md2("_*[]()~`>#+-=|{}.!")
        for ch in "_*[]()~`>#+-=|{}.!":
            assert f"\\{ch}" in result


# ---------------------------------------------------------------------------
# Volume and funding formatting
# ---------------------------------------------------------------------------

class TestFormatHelpers:

    def test_volume_millions(self):
        assert _fmt_volume(Decimal("2500000")) == "$2.5M"

    def test_volume_thousands(self):
        assert _fmt_volume(Decimal("750000")) == "$750.0K"

    def test_volume_small(self):
        assert _fmt_volume(Decimal("500")) == "$500"

    def test_volume_none(self):
        assert _fmt_volume(None) == "—"

    def test_funding_positive(self):
        assert _fmt_funding(Decimal("0.0001")) == "0.0100%"

    def test_funding_negative(self):
        assert _fmt_funding(Decimal("-0.0002")) == "-0.0200%"

    def test_funding_none(self):
        assert _fmt_funding(None) == "—"


# ---------------------------------------------------------------------------
# Full alert formatting
# ---------------------------------------------------------------------------

class TestFormatAlert:

    def test_contains_symbol(self):
        msg = format_alert(make_opp())
        assert "APE" in msg
        assert "USDT" in msg
        assert "PERP" in msg

    def test_contains_exchanges(self):
        msg = format_alert(make_opp())
        assert "BINANCE" in msg
        assert "GATE" in msg

    def test_contains_prices(self):
        msg = format_alert(make_opp())
        assert "1.2350" in msg.replace("\\", "")
        assert "1.2480" in msg.replace("\\", "")

    def test_contains_spread_pct(self):
        msg = format_alert(make_opp())
        unescaped = msg.replace("\\", "")
        assert "1.05%" in unescaped  # gross (105.3 bps = 1.05%)
        assert "0.73%" in unescaped  # net (72.9 bps = 0.73%)

    def test_contains_funding_rates(self):
        msg = format_alert(make_opp())
        unescaped = msg.replace("\\", "")
        assert "0.0100%" in unescaped
        assert "-0.0200%" in unescaped

    def test_contains_volumes(self):
        msg = format_alert(make_opp())
        unescaped = msg.replace("\\", "")
        assert "$2.5M" in unescaped
        assert "$1.8M" in unescaped

    def test_contains_data_age(self):
        msg = format_alert(make_opp())
        assert "150" in msg

    def test_contains_confidence(self):
        msg = format_alert(make_opp())
        unescaped = msg.replace("\\", "")
        assert "0.85" in unescaped

    def test_contains_timestamp(self):
        msg = format_alert(make_opp())
        assert "12:30:45" in msg.replace("\\", "")

    def test_unknown_funding_shows_dash(self):
        msg = format_alert(make_opp(buy_funding_rate=None, sell_funding_rate=None))
        assert "—" in msg

    def test_unknown_volume_shows_dash(self):
        msg = format_alert(make_opp(buy_volume_24h=None, sell_volume_24h=None))
        assert "—" in msg

    def test_buy_sell_labels(self):
        msg = format_alert(make_opp())
        assert "*Buy*" in msg
        assert "*Sell*" in msg

    def test_alert_header(self):
        msg = format_alert(make_opp())
        assert "SPREAD ALERT" in msg

    def test_is_valid_markdown_structure(self):
        """Check that bold markers are balanced."""
        msg = format_alert(make_opp())
        # Count unescaped * chars (not preceded by \)
        import re
        unescaped_stars = re.findall(r'(?<!\\)\*', msg)
        assert len(unescaped_stars) % 2 == 0, "Unbalanced bold markers"
