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
from pump_detector.models import PumpAlert
from utils.exchange_links import futures_url, supported_exchanges
from utils.venues import display_exchange, is_dex_exchange

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


def _next_funding_minutes(exchange: str, snap: MarketSnapshot | None = None) -> int:
    """
    Minutes until next funding settlement.

    Uses snap.next_funding_time from exchange API when available (most accurate).
    Falls back to calculated estimate:
      - Hyperliquid: 1h cycle on the hour
      - CEXes without API data: 8h cycle at 00:00, 08:00, 16:00 UTC
    """
    now = datetime.now(timezone.utc)

    # Prefer actual next_funding_time from exchange API (Binance, Bybit, Gate provide this)
    if snap is not None and snap.next_funding_time is not None:
        delta = (snap.next_funding_time - now).total_seconds() / 60
        return max(1, int(delta))

    # Fallback: calculated estimate
    if exchange == "hyperliquid":
        next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        return max(1, int((next_hour - now).total_seconds() / 60))

    # Standard 8h cycle: 00:00, 08:00, 16:00 UTC (fallback for exchanges without API data)
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
    buy_ex = _e(display_exchange(opp.buy_exchange).upper())
    sell_ex = _e(display_exchange(opp.sell_exchange).upper())

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
        f"{_e(display_exchange(best.buy_exchange))} → {_e(display_exchange(best.sell_exchange))}*",
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
        if is_dex_exchange(ex):
            continue
        snap = all_snapshots.get(ex) if all_snapshots else None
        mins = _next_funding_minutes(ex, snap)
        funding_parts.append(f"{display_exchange(ex)}:{_fmt_minutes(mins)}")
    if funding_parts:
        lines.append(f"⏰ {_e(' | '.join(funding_parts))}")
        lines.append("")

    # ── BEST ROUTE (code block) ─────────────────────────────────────────
    buy_price = f"{float(best.buy_ask):.{prec}f}"
    sell_price = f"{float(best.sell_bid):.{prec}f}"
    buy_vol = _fmt_vol(best.buy_volume_24h)
    sell_vol = _fmt_vol(best.sell_volume_24h)
    buy_fund = "spot" if is_dex_exchange(best.buy_exchange) else _fmt_funding_short(best.buy_funding_rate)
    sell_fund = "spot" if is_dex_exchange(best.sell_exchange) else _fmt_funding_short(best.sell_funding_rate)
    buy_label = display_exchange(best.buy_exchange)
    sell_label = display_exchange(best.sell_exchange)

    ex_w = max(len(buy_label), len(sell_label))

    best_table = (
        f"LONG  {buy_label:<{ex_w}}  {buy_price}  {buy_vol:<7} {buy_fund}\n"
        f"SHORT {sell_label:<{ex_w}}  {sell_price}  {sell_vol:<7} {sell_fund}"
    )

    buy_dw = _dw_symbols(deposit_status, best.buy_exchange, base)
    sell_dw = _dw_symbols(deposit_status, best.sell_exchange, base)

    lines.append(f"📍 *Best route:*")
    lines.append(f"```\n{best_table}\n```")
    lines.append(
        f"DW: {_e(buy_label)} {buy_dw} \\| {_e(sell_label)} {sell_dw}"
    )
    lines.append("")

    # ── ALL EXCHANGES TABLE (code block) ────────────────────────────────
    # Build from all_snapshots if available, else from opp data
    exchange_rows = _build_exchange_rows(
        best, base, prec, all_snapshots, opps
    )

    if exchange_rows:
        # Calculate column widths for alignment
        col_ex = max(max(len(r["label"]) for r in exchange_rows), 2)
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
                f"{r['label']:<{col_ex}}  {r['price']:>{col_price}}  "
                f"{r['vol']:>{col_vol}}  {r['spread']:>{col_sprd}}  "
                f"{r['fund']:>{col_fund}}  {r['tfund']:>{col_tf}}"
            )
            tbl_lines.append(row)

        table_str = "\n".join(tbl_lines)

        # DW line outside code block (emojis render properly)
        dw_parts = []
        for r in exchange_rows:
            dw = _dw_symbols(deposit_status, r["ex"], base)
            dw_parts.append(f"{_e(r['label'])} {dw}")

        lines.append(f"📊 *All exchanges:*")
        lines.append(f"```\n{table_str}\n```")
        lines.append("DW: " + " \\| ".join(dw_parts))

        # Clickable futures links — one per exchange in the table.
        # Order matches the table so users can scan top-to-bottom.
        links_line = _build_links_line(base, [r["ex"] for r in exchange_rows])
        if links_line:
            lines.append(f"🔗 {links_line}")
        lines.append("")

    # ── ROUTES LIST (only if >1 route) ──────────────────────────────────
    if len(opps) > 1:
        lines.append(f"🔀 *{_e(str(len(opps)))} routes:*")
        for i, opp in enumerate(opps, 1):
            net = float(opp.net_spread_bps) / 100
            lines.append(
                f"  \\#{_e(str(i))} "
                f"{_e(display_exchange(opp.buy_exchange))}→{_e(display_exchange(opp.sell_exchange))} "
                f"*{_e(f'{net:.2f}%')}*"
            )
        lines.append("")

    # ── TIMESTAMP ───────────────────────────────────────────────────────
    ts = _e(best.timestamp.strftime("%H:%M:%S UTC"))
    lines.append(f"⏱ {ts}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Pump / dump alert format
# ---------------------------------------------------------------------------

def _build_links_line(base: str, priority: list[str] | None = None) -> str | None:
    """
    Build a MarkdownV2 line with clickable futures links for ALL supported
    exchanges, so the user can jump to any venue to verify/trade — even if
    the bot doesn't have a live snapshot from that exchange yet.

    `priority` is an optional ordering hint: exchanges in this list are
    rendered first (in the given order), then all remaining supported
    exchanges follow alphabetically. Priority typically comes from the
    alert's per-exchange table so the most relevant links stay on top.

    Returns None only if no link could be built at all (should never happen).
    """
    priority = priority or []

    ordered: list[str] = []
    seen: set[str] = set()

    # 1. Exchanges explicitly in priority order
    for ex in priority:
        key = ex.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(key)

    # 2. Every remaining supported exchange, alphabetically
    for ex in supported_exchanges():
        if ex in seen:
            continue
        seen.add(ex)
        ordered.append(ex)

    parts: list[str] = []
    has_link = False
    for ex in ordered:
        url = futures_url(ex, base)
        name_escaped = _e(display_exchange(ex))
        if url:
            # MarkdownV2 link: [text](url). The URL must also have its
            # special chars escaped per Telegram docs (only `)` and `\`).
            safe_url = url.replace("\\", "\\\\").replace(")", "\\)")
            parts.append(f"[{name_escaped}]({safe_url})")
            has_link = True
        else:
            parts.append(name_escaped)

    if not parts or not has_link:
        return None
    return " \\| ".join(parts)


def _fmt_mcap(mcap: float | None) -> str:
    """Format a market cap value as $X.XM/$X.XB or '?'."""
    if mcap is None:
        return "?"
    if mcap >= 1_000_000_000:
        return f"${mcap / 1_000_000_000:.2f}B"
    if mcap >= 1_000_000:
        return f"${mcap / 1_000_000:.1f}M"
    if mcap >= 1_000:
        return f"${mcap / 1_000:.0f}K"
    return f"${mcap:.0f}"


def _fmt_window(seconds: int) -> str:
    """Format a window length in seconds as 'Xh Ym' or 'Ym' or 'Xs'."""
    if seconds >= 3600:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}h{m}m" if m else f"{h}h"
    if seconds >= 60:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def format_pump_alert(alert: PumpAlert) -> str:
    """
    Format a PumpAlert into a Telegram MarkdownV2 message.

    Layout:
      🚀 PUMP  ARIA  +12.34%  in 1h
      💰 MCap: $45M  |  Vol24h: $2.3M
      📈 0.0500 → 0.0561  (trigger: bitget)

      ```
      Ex        Price     Vol
      bitget    0.0561    $2.3M
      aster     0.0558    $1.1M
      gate      0.0560    $0.5M
      ```

      ⏱ 12:34:56 UTC
    """
    base = _e(alert.base)
    arrow_emoji = "🚀" if alert.direction == "pump" else "📉"
    label = "PUMP" if alert.direction == "pump" else "DUMP"
    change_pct = float(alert.change_pct)
    change_str = _e(f"{change_pct:+.2f}%")
    window_str = _e(_fmt_window(alert.window_seconds))

    prec = _auto_precision(alert.current_price)
    start_str = _e(f"{float(alert.start_price):.{prec}f}")
    current_str = _e(f"{float(alert.current_price):.{prec}f}")

    mcap_str = _e(_fmt_mcap(alert.market_cap))
    vol_str = _e(_fmt_vol(alert.max_volume_24h))
    trigger_ex = _e(alert.triggered_on)

    lines = [
        f"{arrow_emoji} *{label}  {base}  {change_str}  in {window_str}*",
        f"💰 MCap: {mcap_str}  \\|  Vol24h: {vol_str}",
        f"📈 `{start_str}` → `{current_str}`  \\(trigger: {trigger_ex}\\)",
        "",
    ]

    # Per-exchange table sorted by price descending so the highest price
    # (most likely the pump leader) shows up first.
    if alert.exchange_prices:
        exchanges = sorted(
            alert.exchange_prices.keys(),
            key=lambda e: float(alert.exchange_prices[e]),
            reverse=(alert.direction == "pump"),
        )
        rows: list[tuple[str, str, str]] = []
        for ex in exchanges:
            price = alert.exchange_prices[ex]
            vol = alert.exchange_volumes.get(ex)
            rows.append((ex, f"{float(price):.{prec}f}", _fmt_vol(vol)))

        col_ex = max(max(len(r[0]) for r in rows), 2)
        col_price = max(max(len(r[1]) for r in rows), 5)
        col_vol = max(max(len(r[2]) for r in rows), 3)

        hdr = (
            f"{'Ex':<{col_ex}}  {'Price':>{col_price}}  {'Vol':>{col_vol}}"
        )
        tbl_lines = [hdr]
        for ex, price, vol in rows:
            tbl_lines.append(
                f"{ex:<{col_ex}}  {price:>{col_price}}  {vol:>{col_vol}}"
            )
        table_str = "\n".join(tbl_lines)
        lines.append(f"```\n{table_str}\n```")

        # Clickable futures links per exchange (Telegram markdown links
        # only work OUTSIDE code blocks, which is why this is a separate row)
        links_line = _build_links_line(alert.base, exchanges)
        if links_line:
            lines.append(f"🔗 {links_line}")
        lines.append("")

    ts = _e(alert.timestamp.strftime("%H:%M:%S UTC"))
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
            fund_str = "spot" if is_dex_exchange(ex) else _fmt_funding_short(snap.funding_rate)
            tfund_str = "—" if is_dex_exchange(ex) else _fmt_minutes(_next_funding_minutes(ex, snap))

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
                "label": display_exchange(ex),
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
                fund_str = "spot" if is_dex_exchange(ex) else _fmt_funding_short(fund)
                tfund_str = "—" if is_dex_exchange(ex) else _fmt_minutes(_next_funding_minutes(ex))

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
                    "label": display_exchange(ex),
                    "price": price_str,
                    "vol": vol_str,
                    "spread": spread_str,
                    "fund": fund_str,
                    "tfund": tfund_str,
                })

        # Sort by price ascending
        rows.sort(key=lambda r: float(r["price"]))

    return rows
