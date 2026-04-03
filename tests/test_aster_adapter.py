"""
Tests for Aster adapter chunking and URL construction.

Inputs: Synthetic Aster symbol lists.
Outputs: Assertions over chunk sizing and combined stream URLs.
Assumptions:
  - Aster mirrors Binance Futures stream naming.
  - Adapter uses two streams per symbol: @bookTicker and @markPrice.
"""

from exchange_adapters.aster import AsterAdapter, MAX_STREAMS_PER_CONN


class TestAsterAdapter:

    def test_url_contains_expected_streams(self):
        url = AsterAdapter._build_ws_url_for_symbols(["APEUSDT", "XRPUSDT"])

        assert "fstream.asterdex.com/stream?streams=" in url
        assert "apeusdt@bookTicker" in url
        assert "xrpusdt@markPrice" in url

    def test_large_symbol_list_is_split_by_stream_limit(self):
        max_symbols_per_conn = MAX_STREAMS_PER_CONN // 2
        symbols = [f"SYM{i}USDT" for i in range(max_symbols_per_conn * 2 + 5)]

        adapter = AsterAdapter(
            symbols=symbols,
            on_snapshot=lambda s: None,
        )

        assert [len(chunk) for chunk in adapter._symbol_chunks] == [
            max_symbols_per_conn,
            max_symbols_per_conn,
            5,
        ]
