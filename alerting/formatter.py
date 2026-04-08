"""
Telegram MarkdownV2 message formatter for spread alerts.

Inputs: SpreadOpportunity objects, deposit/withdrawal status, exchange snapshots.
Outputs: Formatted MarkdownV2 string ready to send via Bot API.
Assumptions:
  - All special characters must be escaped for MarkdownV2 (except inside code blocks).
  - Uses code blocks (```) for tabular data to get monospace alignment.
  - DW emoji symbols go outside code blocks for proper rendering.
  - Funding settlement times: 8h for CEXes, 1h for Hyperliquid.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from models.snapshot import MarketSnapshot, SpreadOpportunity

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


# Short alias
_e = escape_md2


# ---------------------------------------------------------------------------
# Helper formatters
# ---------------------------------------------------------------------------

def _auto_precision(price: Decimal) -> int:
    """Determine decimal places based on price magnitude."""
    p = abs(float(price))
    if p == 0:
        return 4
    if p >= 1000:
        return 2
    if p >= 10:
        return 3
    if p >= 1:
        return 4
    if p >= 0.01:
        return 6
    return 8


def _fmt_vol(volume: Decimal | None) -> str:
    """Format 24h volume in short form: $1.2M, $500K, $0."""
    if volume is None:
        return "—"
    v = float(volume)
    if v >= 1_000_000:
        return f"${v / 1_000_000:.1f}M"
    if v >= 1_000:
        return f"${v / 1_000:.1f}K"
    return f"${v:.0f}"


def _fmt_funding_short(rate: Decimal | None) -> str:
    """Format funding rate as short percentage for table display."""
    if rate is None:
        return "—"
    pct = float(rate) * 100
    # Show sign explicitly
    return f"{pct:+.3f}%"


def _fmt_pct(bps: Decimal) -> str:
    """Format basis points as percentage (100 bps = 1.00%)."""
    pct = bps / 100
    return f"{pct:.2f}%"


def _fmt_decimal(value: Decimal, precision: int = 4) -> str:
    """Format a Decimal to a fixed number of decimal places."""
    return f"{value:.{precision}f}"


def _fmt_funding(rate: Decimal | None) -> str:
    """Format funding rate as percentage, or '—' if unknown."""
    if rate is None:
        return "—"
    pct = rate * 100
    return f"{pct:.4f}%"


# Backward-compatible helper names kept for legacy tests/imports.
_fmt_volume = _fmt_vol


def _dw_symbols(deposit_status: dict | None, exchange: str, base: str) -> str:
    """
    Get DW status as colored circle emojis.
    🟢 = enabled, 🔴 = disabled, ⚫ = unknown.
    First circle = deposit, second = withdraw.
    """
    if deposit_status is None:
        return "⚫⚫"
    status = deposit_status.get((exchange, base))
    if status is None:
        return "⚫⚫"
    d = "🟢" if status.deposit is True else ("🔴" if status.deposit is False else "⚫")
    w = "🟢" if status.withdraw is True else ("🔴" if status.withdraw is False else "⚫")
    return d + w


def _next_funding_minutes(exchange: str) -> int:
    """
    Minutes until next funding settlement for an exchange.
    Standard CEXes settle every 8h at 00:00, 08:00, 16:00 UTC.
    Hyperliquid settles every 1h on the hour.
    """
    now = datetime.now(timezone.utc)

    if exchange == "hyperliquid":
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
        return f"{h}h{m}m" if m else f"{h}h"
    return f"{minutes}m"


# ---------------------------------------------------------------------------
# Single alert format (legacy, kept for backward compat)
# ---------------------------------------------------------------------------

def format_alert(opp: SpreadOpportunity) -> str:
    """
    Format a single SpreadOpportunity into a Telegram MarkdownV2 message.
    """
    symbol = _e(opp.canonical_symbol)
    buy_ex = _e(opp.buy_exchange.upper())
    sell_ex = _e(opp.sell_exchange.upper())

    buy_ask = _e(_fmt_decimal(opp.buy_ask))
    sell_bid = _e(_fmt_decimal(opp.sell_bid))

    gross_pct = _e(_fmt_pct(opp.gross_spread_bps))
    net_pct = _e(_fmt_pct(opp.net_spread_bps))

    fees = _e(_fmt_decimal(opp.estimated_fees))
    slippage = _e(_fmt_decimal(opp.estimated_slippage))

    buy_funding = _e(_fmt_funding(opp.buy_funding_rate))
    sell_funding = _e(_fmt_funding(opp.sell_funding_rate))

    ask_size = _e(_fmt_decimal(opp.buy_ask_size, 2))
    bid_size = _e(_fmt_decimal(opp.sell_bid_size, 2))

    buy_vol = _e(_fmt_vol(opp.buy_volume_24h))
    sell_vol = _e(_fmt_vol(opp.sell_volume_24h))

    age = _e(str(opp.data_age_ms))
    conf = _e(f"{opp.confidence:.2f}")
    ts = _e(opp.timestamp.strftime("%H:%M:%S UTC"))

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


# ---------------------------------------------------------------------------
# Grouped alert — new table-style format
# ---------------------------------------------------------------------------

def format_grouped_alert(
    opps: list[SpreadOpportunity],
    deposit_status: dict | None = None,
    all_snapshots: dict[str, MarketSnapshot] | None = None,
) -> str:
    """
    Format multiple routes for the same base token into a table-style alert.

    Layout:
      🔔 TOKEN  X.XX%  buy_ex → sell_ex
      ⏰ exchange:Xh Ym | exchange:Xh Ym

      📍 Best route:
      ```
      LONG  buy_ex   0.0549  $1.2M  +0.010%
      SHORT sell_ex  0.0563  $500K  -0.005%
      ```
      DW: buy_ex 🟢🟢 | sell_ex 🟢🟢

      📊 All exchanges:
      ```
      Ex        Price   Vol    Sprd   Fund      Tfund
      gate      0.0549  $1.2M  buy   -0.005%   5h23m
      binance   0.0563  $500K  sell  +0.010%   5h23m
      ```
      DW: gate 🟢🟢 | binance 🟢🟢

      🔀 3 routes:
        #1 gate→binance 8.32%
        #2 okx→binance  3.21%

      ⏱ 08:26:59 UTC

    Args:
        opps: Routes pre-sorted by net_spread_bps descending (best first).
        deposit_status: {(exchange, base): CoinStatus} from DepositChecker.
        all_snapshots: {exchange_name: MarketSnapshot} for ALL exchanges
                       that have a live snapshot for this base token.
    """
    if not opps:
        return ""

    best = opps[0]
    base = best.canonical_symbol.split("-")[0] if "-" in best.canonical_symbol else best.canonical_symbol
    best_net_pct = float(best.net_spread_bps) / 100
    prec = _auto_precision(best.buy_ask)

    # ── HEADER ──────────────────────────────────────────────────────────
    lines = [
        f"🔔 *{_e(base)}  {_e(f'{best_net_pct:.2f}%')}  "
        f"{_e(best.buy_exchange)} → {_e(best.sell_exchange)}*",
    ]

    # ── FUNDING COUNTDOWN PER EXCHANGE ──────────────────────────────────
    all_ex_set: set[str] = set()
    for o in opps:
        all_ex_set.add(o.buy_exchange)
        all_ex_set.add(o.sell_exchange)
    if all_snapshots:
        all_ex_set.update(all_snapshots.keys())

    funding_parts = []
    for ex in sorted(all_ex_set):
        mins = _next_funding_minutes(ex)
        funding_parts.append(f"{ex}:{_fmt_minutes(mins)}")
    lines.append(f"⏰ {_e(' | '.join(funding_parts))}")
    lines.append("")

    # ── BEST ROUTE (code block) ─────────────────────────────────────────
    buy_price = f"{float(best.buy_ask):.{prec}f}"
    sell_price = f"{float(best.sell_bid):.{prec}f}"
    buy_vol = _fmt_vol(best.buy_volume_24h)
    sell_vol = _fmt_vol(best.sell_volume_24h)
    buy_fund = _fmt_funding_short(best.buy_funding_rate)
    sell_fund = _fmt_funding_short(best.sell_funding_rate)

    ex_w = max(len(best.buy_exchange), len(best.sell_exchange))

    best_table = (
        f"LONG  {best.buy_exchange:<{ex_w}}  {buy_price}  {buy_vol:<7} {buy_fund}\n"
        f"SHORT {best.sell_exchange:<{ex_w}}  {sell_price}  {sell_vol:<7} {sell_fund}"
    )

    buy_dw = _dw_symbols(deposit_status, best.buy_exchange, base)
    sell_dw = _dw_symbols(deposit_status, best.sell_exchange, base)

    lines.append(f"📍 *Best route:*")
    lines.append(f"```\n{best_table}\n```")
    lines.append(
        f"DW: {_e(best.buy_exchange)} {buy_dw} \\| {_e(best.sell_exchange)} {sell_dw}"
    )
    lines.append("")

    # ── ALL EXCHANGES TABLE (code block) ────────────────────────────────
    # Build from all_snapshots if available, else from opp data
    exchange_rows = _build_exchange_rows(
        best, base, prec, all_snapshots, opps
    )

    if exchange_rows:
        # Calculate column widths for alignment
        col_ex = max(max(len(r["ex"]) for r in exchange_rows), 2)
        col_price = max(max(len(r["price"]) for r in exchange_rows), 5)
        col_vol = max(max(len(r["vol"]) for r in exchange_rows), 3)
        col_sprd = max(max(len(r["spread"]) for r in exchange_rows), 4)
        col_fund = max(max(len(r["fund"]) for r in exchange_rows), 4)
        col_tf = max(max(len(r["tfund"]) for r in exchange_rows), 5)

        # Header row
        hdr = (
            f"{'Ex':<{col_ex}}  {'Price':>{col_price}}  "
            f"{'Vol':>{col_vol}}  {'Sprd':>{col_sprd}}  "
            f"{'Fund':>{col_fund}}  {'Tfund':>{col_tf}}"
        )
        tbl_lines = [hdr]

        for r in exchange_rows:
            row = (
                f"{r['ex']:<{col_ex}}  {r['price']:>{col_price}}  "
                f"{r['vol']:>{col_vol}}  {r['spread']:>{col_sprd}}  "
                f"{r['fund']:>{col_fund}}  {r['tfund']:>{col_tf}}"
            )
            tbl_lines.append(row)

        table_str = "\n".join(tbl_lines)

        # DW line outside code block (emojis render properly)
        dw_parts = []
        for r in exchange_rows:
            dw = _dw_symbols(deposit_status, r["ex"], base)
            dw_parts.append(f"{_e(r['ex'])} {dw}")

        lines.append(f"📊 *All exchanges:*")
        lines.append(f"```\n{table_str}\n```")
        lines.append("DW: " + " \\| ".join(dw_parts))
        lines.append("")

    # ── ROUTES LIST (only if >1 route) ──────────────────────────────────
    if len(opps) > 1:
        lines.append(f"🔀 *{_e(str(len(opps)))} routes:*")
        for i, opp in enumerate(opps, 1):
            net = float(opp.net_spread_bps) / 100
            lines.append(
                f"  \\#{_e(str(i))} "
                f"{_e(opp.buy_exchange)}→{_e(opp.sell_exchange)} "
                f"*{_e(f'{net:.2f}%')}*"
            )
        lines.append("")

    # ── TIMESTAMP ───────────────────────────────────────────────────────
    ts = _e(best.timestamp.strftime("%H:%M:%S UTC"))
    lines.append(f"⏱ {ts}")

    return "\n".join(lines)


def _build_exchange_rows(
    best: SpreadOpportunity,
    base: str,
    prec: int,
    all_snapshots: dict[str, MarketSnapshot] | None,
    opps: list[SpreadOpportunity],
) -> list[dict[str, str]]:
    """
    Build row dicts for the All Exchanges table.

    Each row: {ex, price, vol, spread, fund, tfund}
    Sorted by ask price ascending (cheapest/buy-side first).

    spread column:
      - "buy"  for the best route's buy exchange
      - "sell" for the best route's sell exchange
      - "+X.X%" for all others (spread vs sell exchange's bid)
    """
    ref_bid = float(best.sell_bid)  # sell side reference price
    sell_ex = best.sell_exchange
    buy_ex = best.buy_exchange
    rows: list[dict[str, str]] = []

    if all_snapshots and len(all_snapshots) > 0:
        # Use live snapshots — sorted by ask price ascending
        for ex in sorted(all_snapshots.keys(), key=lambda e: float(all_snapshots[e].ask)):
            snap = all_snapshots[ex]
            # Sell exchange shows bid (the price you sell at),
            # all others show ask (the price you buy at)
            if ex == sell_ex:
                price_f = float(snap.bid)
            else:
                price_f = float(snap.ask)
            price_str = f"{price_f:.{prec}f}"
            vol_str = _fmt_vol(snap.volume_24h)
            fund_str = _fmt_funding_short(snap.funding_rate)
            tfund_str = _fmt_minutes(_next_funding_minutes(ex))

            if ex == sell_ex:
                spread_str = "sell"
            elif ex == buy_ex:
                spread_str = "buy"
            else:
                if price_f > 0:
                    sprd = (ref_bid - price_f) / price_f * 100
                    spread_str = f"{sprd:+.1f}%"
                else:
                    spread_str = "—"

            rows.append({
                "ex": ex,
                "price": price_str,
                "vol": vol_str,
                "spread": spread_str,
                "fund": fund_str,
                "tfund": tfund_str,
            })
    else:
        # Fallback: build from opportunity data only
        seen: set[str] = set()
        for opp in opps:
            for side in ("buy", "sell"):
                if side == "buy":
                    ex = opp.buy_exchange
                    price_f = float(opp.buy_ask)
                    vol = opp.buy_volume_24h
                    fund = opp.buy_funding_rate
                else:
                    ex = opp.sell_exchange
                    price_f = float(opp.sell_bid)
                    vol = opp.sell_volume_24h
                    fund = opp.sell_funding_rate

                if ex in seen:
                    continue
                seen.add(ex)

                price_str = f"{price_f:.{prec}f}"
                vol_str = _fmt_vol(vol)
                fund_str = _fmt_funding_short(fund)
                tfund_str = _fmt_minutes(_next_funding_minutes(ex))

                if ex == sell_ex:
                    spread_str = "sell"
                elif ex == buy_ex:
                    spread_str = "buy"
                else:
                    if price_f > 0:
                        sprd = (ref_bid - price_f) / price_f * 100
                        spread_str = f"{sprd:+.1f}%"
                    else:
                        spread_str = "—"

                rows.append({
                    "ex": ex,
                    "price": price_str,
                    "vol": vol_str,
                    "spread": spread_str,
                    "fund": fund_str,
                    "tfund": tfund_str,
                })

        # Sort by price ascending
        rows.sort(key=lambda r: float(r["price"]))

    return rows
