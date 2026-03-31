"""
Telegram MarkdownV2 message formatter for spread alerts.

Inputs: SpreadOpportunity object.
Outputs: Formatted MarkdownV2 string ready to send via Bot API.
Assumptions:
  - All special characters must be escaped for MarkdownV2.
  - Messages should be compact but include all required fields.
"""

from decimal import Decimal

from models.snapshot import SpreadOpportunity

# Characters that must be escaped in MarkdownV2
# https://core.telegram.org/bots/api#markdownv2-style
_MD2_ESCAPE_CHARS = r"_*[]()~`>#+-=|{}.!"


def escape_md2(text: str) -> str:
    """Escape special characters for Telegram MarkdownV2."""
    result = []
    for ch in text:
        if ch in _MD2_ESCAPE_CHARS:
            result.append("\\")
        result.append(ch)
    return "".join(result)


def _fmt_decimal(value: Decimal, precision: int = 4) -> str:
    """Format a Decimal to a fixed number of decimal places."""
    return f"{value:.{precision}f}"


def _fmt_bps(value: Decimal) -> str:
    """Format basis points with 1 decimal place."""
    return f"{value:.1f}"


def _fmt_funding(rate: Decimal | None) -> str:
    """Format funding rate as percentage, or '—' if unknown."""
    if rate is None:
        return "—"
    # Display as percentage with 4 decimals (e.g. 0.0100%)
    pct = rate * 100
    return f"{pct:.4f}%"


def _fmt_volume(volume: Decimal | None) -> str:
    """Format 24h volume in human-readable form, or '—' if unknown."""
    if volume is None:
        return "—"
    if volume >= 1_000_000:
        return f"${volume / 1_000_000:.1f}M"
    if volume >= 1_000:
        return f"${volume / 1_000:.1f}K"
    return f"${volume:.0f}"


def format_alert(opp: SpreadOpportunity) -> str:
    """
    Format a SpreadOpportunity into a Telegram MarkdownV2 message.

    Layout:
      🔔 SPREAD ALERT: {symbol}
      Buy {exchange} @ {ask} | Sell {exchange} @ {bid}
      Gross: {bps} bps | Net: {bps} bps
      Fees: {est} | Slip: {est}
      Funding: {buy} / {sell}
      Sizes: {ask_size} / {bid_size}
      Volume: {buy} / {sell}
      Age: {ms}ms | Conf: {score}
      ⏱ {timestamp}
    """
    symbol = escape_md2(opp.canonical_symbol)
    buy_ex = escape_md2(opp.buy_exchange.upper())
    sell_ex = escape_md2(opp.sell_exchange.upper())

    buy_ask = escape_md2(_fmt_decimal(opp.buy_ask))
    sell_bid = escape_md2(_fmt_decimal(opp.sell_bid))

    gross_bps = escape_md2(_fmt_bps(opp.gross_spread_bps))
    net_bps = escape_md2(_fmt_bps(opp.net_spread_bps))

    fees = escape_md2(_fmt_decimal(opp.estimated_fees))
    slippage = escape_md2(_fmt_decimal(opp.estimated_slippage))

    buy_funding = escape_md2(_fmt_funding(opp.buy_funding_rate))
    sell_funding = escape_md2(_fmt_funding(opp.sell_funding_rate))

    ask_size = escape_md2(_fmt_decimal(opp.buy_ask_size, 2))
    bid_size = escape_md2(_fmt_decimal(opp.sell_bid_size, 2))

    buy_vol = escape_md2(_fmt_volume(opp.buy_volume_24h))
    sell_vol = escape_md2(_fmt_volume(opp.sell_volume_24h))

    age = escape_md2(str(opp.data_age_ms))
    conf = escape_md2(f"{opp.confidence:.2f}")

    ts = escape_md2(opp.timestamp.strftime("%H:%M:%S UTC"))

    lines = [
        f"🔔 *SPREAD ALERT: {symbol}*",
        "",
        f"*Buy* {buy_ex} @ `{buy_ask}`",
        f"*Sell* {sell_ex} @ `{sell_bid}`",
        "",
        f"*Gross:* {gross_bps} bps \\| *Net:* {net_bps} bps",
        f"*Fees:* {fees} \\| *Slip:* {slippage}",
        "",
        f"*Funding:* {buy_funding} / {sell_funding}",
        f"*Sizes:* {ask_size} / {bid_size}",
        f"*Volume:* {buy_vol} / {sell_vol}",
        "",
        f"*Age:* {age}ms \\| *Conf:* {conf}",
        f"⏱ {ts}",
    ]

    return "\n".join(lines)
