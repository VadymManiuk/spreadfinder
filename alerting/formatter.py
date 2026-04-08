"""
Telegram MarkdownV2 message formatter for spread alerts.

Inputs: SpreadOpportunity object, optional deposit/withdrawal status.
Outputs: Formatted MarkdownV2 string ready to send via Bot API.
Assumptions:
  - All special characters must be escaped for MarkdownV2.
  - Messages should be compact but include all required fields.
  - Funding settlement times: 8h for CEXes, 1h for Hyperliquid.
"""

from datetime import datetime, timedelta, timezone
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


def _fmt_pct(bps: Decimal) -> str:
    """Format basis points as percentage (100 bps = 1.00%)."""
    pct = bps / 100
    return f"{pct:.2f}%"


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

    gross_pct = escape_md2(_fmt_pct(opp.gross_spread_bps))
    net_pct = escape_md2(_fmt_pct(opp.net_spread_bps))

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
        f"*Gross:* {gross_pct} \\| *Net:* {net_pct}",
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


def _next_funding_minutes(exchange: str) -> int:
    """
    Minutes until next funding settlement for an exchange.

    Standard CEXes settle every 8h at 00:00, 08:00, 16:00 UTC.
    Hyperliquid settles every 1h on the hour.
    """
    now = datetime.now(timezone.utc)

    if exchange == "hyperliquid":
        # Hourly funding — next settlement at the top of the next hour
        next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        return max(1, int((next_hour - now).total_seconds() / 60))

    # Standard 8h cycle: 00:00, 08:00, 16:00 UTC
    current_hour = now.hour
    next_funding_hour = ((current_hour // 8) + 1) * 8
    if next_funding_hour >= 24:
        next_time = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    else:
        next_time = now.replace(
            hour=next_funding_hour, minute=0, second=0, microsecond=0
        )
    return max(1, int((next_time - now).total_seconds() / 60))


def _fmt_minutes(minutes: int) -> str:
    """Format minutes into 'Xh Ym' or 'Ym'."""
    if minutes >= 60:
        h = minutes // 60
        m = minutes % 60
        return f"{h}h {m}m" if m else f"{h}h"
    return f"{minutes}m"


def _format_route(
    opp: SpreadOpportunity,
    idx: int,
    deposit_status: dict | None = None,
) -> str:
    """
    Format one route for a grouped alert, including funding and deposit info.

    deposit_status: optional dict from DepositChecker, keyed by (exchange, base).
    Each value has .format_short() method.
    """
    buy_ex = escape_md2(opp.buy_exchange.upper())
    sell_ex = escape_md2(opp.sell_exchange.upper())
    buy_ask = escape_md2(_fmt_decimal(opp.buy_ask))
    sell_bid = escape_md2(_fmt_decimal(opp.sell_bid))
    gross_pct = escape_md2(_fmt_pct(opp.gross_spread_bps))
    net_pct = escape_md2(_fmt_pct(opp.net_spread_bps))
    ask_size = escape_md2(_fmt_decimal(opp.buy_ask_size, 2))
    bid_size = escape_md2(_fmt_decimal(opp.sell_bid_size, 2))
    conf = escape_md2(f"{opp.confidence:.2f}")
    num = escape_md2(f"#{idx}")

    # Funding rates
    buy_fund = escape_md2(_fmt_funding(opp.buy_funding_rate))
    sell_fund = escape_md2(_fmt_funding(opp.sell_funding_rate))

    lines = [
        f"  {num}  *{buy_ex}* → *{sell_ex}*",
        f"        Net: *{net_pct}* \\| Gross: {gross_pct}",
        f"        Buy @ `{buy_ask}` \\| Sell @ `{sell_bid}`",
        f"        Sizes: {ask_size} / {bid_size} \\| Conf: {conf}",
        f"        Funding: {buy_fund} / {sell_fund}",
    ]

    # Deposit/withdrawal status if available
    if deposit_status is not None:
        base = opp.canonical_symbol.split("-")[0] if "-" in opp.canonical_symbol else opp.canonical_symbol
        buy_st = deposit_status.get((opp.buy_exchange, base))
        sell_st = deposit_status.get((opp.sell_exchange, base))
        if buy_st or sell_st:
            buy_dw = escape_md2(buy_st.format_short() if buy_st else "⚪")
            sell_dw = escape_md2(sell_st.format_short() if sell_st else "⚪")
            lines.append(
                f"        💰 {buy_ex} {buy_dw} \\| {sell_ex} {sell_dw}"
            )

    return "\n".join(lines)


def format_grouped_alert(
    opps: list[SpreadOpportunity],
    deposit_status: dict | None = None,
) -> str:
    """
    Format multiple routes for the same base token into ONE message.
    Includes funding info per route, time to next funding, and deposit/withdrawal status.

    Args:
        opps: Pre-sorted by net_spread_bps descending (best first).
        deposit_status: {(exchange, base): CoinStatus} from DepositChecker.
    """
    if not opps:
        return ""

    best = opps[0]

    # Extract base token name
    base = best.canonical_symbol.split("-")[0] if "-" in best.canonical_symbol else best.canonical_symbol
    symbol = escape_md2(base)

    count = escape_md2(str(len(opps)))
    ts = escape_md2(best.timestamp.strftime("%H:%M:%S UTC"))
    best_net = escape_md2(_fmt_pct(best.net_spread_bps))

    # Time to next funding — use the minimum across all exchanges in the routes
    exchanges_in_routes = set()
    for opp in opps:
        exchanges_in_routes.add(opp.buy_exchange)
        exchanges_in_routes.add(opp.sell_exchange)

    min_funding_mins = min(
        _next_funding_minutes(ex) for ex in exchanges_in_routes
    )
    funding_time = escape_md2(_fmt_minutes(min_funding_mins))

    # Check if any route involves Hyperliquid (1h funding cycle)
    has_hourly = "hyperliquid" in exchanges_in_routes
    funding_label = f"⏰ Funding in: {funding_time}"
    if has_hourly:
        funding_label += escape_md2(" (HL: 1h cycle)")

    lines = [
        f"🔔 *SPREAD ALERT: {symbol}*",
        f"📊 {count} routes \\| Best: *{best_net}* net",
        funding_label,
        "",
    ]

    for i, opp in enumerate(opps, 1):
        lines.append(_format_route(opp, i, deposit_status))
        lines.append("")

    lines.append(f"⏱ {ts}")

    return "\n".join(lines)
