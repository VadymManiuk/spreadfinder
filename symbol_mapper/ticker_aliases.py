"""
Cross-exchange ticker alias map.

Inputs: None (static data).
Outputs: Mapping of alternate tickers to their canonical base asset name.
Assumptions:
  - The same token can have different tickers on different exchanges.
  - This map normalizes all known aliases to one canonical name.
  - The canonical name is the most commonly used ticker.
  - Updated periodically as exchanges rebrand tokens.

Examples:
  - Polygon: MATIC (old) → POL (new) — some exchanges still use MATIC
  - Terra Classic: LUNA → LUNC after the fork
  - 1000x tokens: Binance lists 1000PEPE, others list PEPE
"""

# Map: alternate_ticker → canonical_ticker
# The canonical ticker is the one most commonly used across exchanges.
# Both directions are handled — the mapper normalizes to whichever name
# appears in this map as the VALUE.
TICKER_ALIASES: dict[str, str] = {
    # --- Rebrands ---
    "MATIC": "POL",          # Polygon rebranded MATIC → POL
    "LUNA": "LUNC",          # Terra Classic (post-fork)
    "LUNA2": "LUNA",         # Terra 2.0 — some exchanges use LUNA2
    "RNDR": "RENDER",        # Render Network rebranded RNDR → RENDER (Nov 2023)
    "BETH": "ETH",           # Beacon ETH on some exchanges
    "WBTC": "BTC",           # Wrapped BTC sometimes listed as WBTC
    "WETH": "ETH",           # Wrapped ETH
    "BETH": "ETH",           # Binance staked ETH
    "STETH": "ETH",          # Lido staked ETH
    "RETH": "ETH",           # Rocket Pool ETH

    # --- Binance 1000x tokens ---
    # Binance lists some low-price tokens with a 1000x multiplier
    # while other exchanges list the raw token.
    "1000PEPE": "PEPE",
    "1000FLOKI": "FLOKI",
    "1000SHIB": "SHIB",
    "1000LUNC": "LUNC",
    "1000XEC": "XEC",
    "1000SATS": "SATS",
    "1000RATS": "RATS",
    "1000BONK": "BONK",
    "1000CAT": "CAT",
    "1000CHEEMS": "CHEEMS",
    "1000WHIP": "WHIP",
    "1000X": "X",
    "1000APU": "APU",
    "1000MOGCOIN": "MOG",
    "1000BTT": "BTT",
    "1000TURBO": "TURBO",
    "1000AIDOGE": "AIDOGE",
    "1000TOKEN": "TOKEN",
    "1000WHY": "WHY",
    "1000BEER": "BEER",
    "1000IQ50": "IQ50",
    "1000DOGS": "DOGS",
    "1000NEIRO": "NEIRO",
    "1000X2": "X2",

    # --- Name differences across exchanges ---
    "MOBX": "MOB",           # MobileCoin
    "BEAMX": "BEAM",         # Beam — Binance uses BEAMX
    "GALFT": "GAL",          # Galxe — name varies
    "SDAO": "AGIX",          # SingularityDAO → AGIX on some
    "AGIX": "FET",           # AGIX merged into FET (ASI alliance)
    "OCEAN": "FET",          # OCEAN merged into FET (ASI alliance)
    "REPV2": "REP",          # Augur v2
    "YFIDOWN": "YFI",        # Leveraged token aliases (skip these)
    "SUSHISWAP": "SUSHI",    # SushiSwap
    "COMPOUND": "COMP",      # Compound
    "CHAINLINK": "LINK",     # Chainlink
    "UNISWAP": "UNI",        # Uniswap
    "POLYGONECOSYSTEMTOKEN": "POL",
    "MANTAUSD": "MANTA",     # Gate sometimes appends quote

    # --- Hyperliquid-specific ---
    # Hyperliquid uses certain tickers that differ from Binance/Gate
    "KPEPE": "PEPE",         # Hyperliquid k-prefix = 1000x notation
    "KFLOKI": "FLOKI",
    "KSHIB": "SHIB",
    "KLUNC": "LUNC",
    "KBONK": "BONK",
    "KSATS": "SATS",
    "KNEIRO": "NEIRO",

    # --- Stablecoin proxies (should not be traded as spreads) ---
    # These are filtered out, not aliased — but listed for completeness.
}

# -----------------------------------------------------------------------
# Ticker collisions: same ticker, DIFFERENT tokens on different exchanges.
# These must be EXCLUDED from cross-exchange matching — comparing their
# prices is meaningless because they are fundamentally different assets.
#
# Format: {ticker: "reason why it's ambiguous"}
# -----------------------------------------------------------------------
TICKER_COLLISIONS: dict[str, str] = {
    # --- Index / composite tokens (different underlyings per exchange) ---
    "ALL": "Composite index token — different underlying baskets on Gate vs Binance",

    # --- Confirmed different projects using same ticker ---
    "BEAM": "Beam privacy coin (Mimblewimble, 2019) vs Beam gaming (Merit Circle rebrand, 2023)",
    "NEIRO": "Two competing Neiro meme tokens (different ETH contracts) listed on different exchanges in 2024",
    "MERL": "Merlin Chain vs Merlin Lab — different projects across exchanges",
    "ACE": "Fusionist (Binance) vs ACE Token / Acent on other exchanges",
    "PORTAL": "Portal gaming (Binance-launched) vs Portal cross-chain DEX",
    "SUN": "Sun Token (Tron) vs Sun New — fork/migration issues across exchanges",
    "MIR": "Mirror Protocol — delisted on some, different token on others",
    "CORE": "Core DAO vs CoreDAO — may differ across exchanges",

    # --- Generic / short tickers with high collision risk ---
    "AI": "Multiple AI-themed tokens: Sleepless AI (Binance) vs others on Gate/Hyperliquid",
    "X": "Single-letter ticker — multiple projects use it across exchanges",
    "X2": "Generic ticker — collision risk across exchanges",
    "C": "Single-letter ticker — high collision risk",
    "D": "Single-letter ticker — high collision risk",
    "4": "Single-character ticker — high collision risk",
    "ID": "SPACE ID — very short ticker, collision risk with other ID tokens",
    "TOKEN": "TokenFi on some exchanges, different projects on others — extremely generic",
    "CAT": "Simon's Cat (Binance) vs other CAT meme tokens on different exchanges",
    "RARE": "SuperRare (NFT) vs other RARE-ticker tokens",
    "COMBO": "COMBO gaming (Binance) vs other COMBO tokens",
    "OG": "OG Fan Token vs OG protocol — different projects",

    # --- Meme tokens with same ticker, different chains/contracts ---
    "MOG": "Mog Coin (Ethereum) vs MOG tokens on other chains — different contracts",
    "WHY": "Multiple WHY tokens on BNB Chain vs Solana — different projects",
    "BEER": "Multiple BEER meme tokens on different chains",
    "DOGS": "Multiple dog-themed tokens using DOGS ticker",
    "APU": "Apu Apustaja — multiple versions on different chains",
    "IQ50": "Meme token with different versions across chains",

    # --- Fan tokens (often chain-specific, may not match across exchanges) ---
    "ALPINE": "Alpine F1 Team Fan Token — not always same contract",
    "LAZIO": "Lazio Fan Token — not always same contract",
    "SANTOS": "Santos FC Fan Token — not always same contract",
    "PORTO": "Porto Fan Token — not always same contract",
    "ATM": "Atletico Madrid Fan Token — may conflict",
    "ASR": "AS Roma Fan Token — may conflict",
    "CITY": "Manchester City Fan Token — may conflict",
    "BAR": "FC Barcelona Fan Token — may conflict",
    "JUV": "Juventus Fan Token — may conflict",
    "PSG": "Paris Saint-Germain Fan Token — may conflict",

    # --- Rebrand/migration confusion (may resolve over time) ---
    "GAL": "Galxe — was Project Galaxy, ticker varies (GAL vs GALFT)",
    "EDU": "Open Campus — may conflict across exchanges",
    "ACH": "Alchemy Pay — ticker sometimes confused with other projects",
    "MDT": "Measurable Data Token — may conflict on some exchanges",
    "WING": "Wing Finance — different versions on exchanges",
    "FOR": "ForTube — ticker collision risk",
    "LEVER": "LeverFi — may differ across exchanges",
}

# Build reverse lookup: canonical → set of all aliases
_CANONICAL_TO_ALIASES: dict[str, set[str]] = {}
for _alias, _canonical in TICKER_ALIASES.items():
    _CANONICAL_TO_ALIASES.setdefault(_canonical, set()).add(_alias)


def normalize_base(base: str) -> str:
    """
    Normalize a base asset ticker to its canonical form.

    Uses static alias map first, then falls back to dynamic detection
    of 1000x and k-prefix tokens so new ones are caught automatically.

    Examples:
      normalize_base("1000PEPE") → "PEPE"
      normalize_base("MATIC") → "POL"
      normalize_base("kBONK") → "BONK"
      normalize_base("BTC") → "BTC"  (no alias, returned as-is)
    """
    upper = base.upper()

    # 1. Check static alias map first
    if upper in TICKER_ALIASES:
        return TICKER_ALIASES[upper]

    # 2. Dynamic detection: "1000" prefix → strip it
    #    Catches new 1000x tokens not yet in the static map
    if upper.startswith("1000") and len(upper) > 4:
        return upper[4:]

    # 3. Dynamic detection: "K" prefix (Hyperliquid notation)
    #    Only if remaining part is all uppercase letters (avoid false matches)
    if upper.startswith("K") and len(upper) > 1 and upper[1:].isalpha():
        # Be conservative: only strip K if it looks like a k-prefix token
        # (short base name, all alpha). Avoids stripping from "KAVA", "KDA", etc.
        # Known k-prefix tokens are already in the static map above,
        # so this is just a safety net for new ones.
        stripped = upper[1:]
        # If the stripped version exists as a known token in the alias map
        # values, it's likely a k-prefix token
        if stripped in _CANONICAL_TO_ALIASES or stripped in TICKER_ALIASES.values():
            return stripped

    return upper


def get_aliases(canonical_base: str) -> set[str]:
    """
    Get all known aliases for a canonical base asset.

    Returns set including the canonical name itself.
    """
    aliases = _CANONICAL_TO_ALIASES.get(canonical_base.upper(), set())
    return aliases | {canonical_base.upper()}


def are_same_asset(base_a: str, base_b: str) -> bool:
    """
    Check if two base asset tickers refer to the same underlying asset.

    Examples:
      are_same_asset("MATIC", "POL") → True
      are_same_asset("1000PEPE", "PEPE") → True
      are_same_asset("BTC", "ETH") → False
    """
    return normalize_base(base_a) == normalize_base(base_b)
