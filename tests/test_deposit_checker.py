"""
Tests for deposit/withdrawal credential isolation.

Inputs: Web3 environment credentials and a request-forbidden session.
Outputs: Confidence that Web3 credentials are not reused for OKX Exchange.
Assumptions:
  - OKX Exchange asset access is optional and uses a separate API key.
"""

import pytest

from utils.deposit_checker import DepositChecker


class _RequestForbiddenSession:
    """Fail if an unauthenticated checker attempts an HTTP request."""

    def get(self, *_args, **_kwargs):
        raise AssertionError("OKX Exchange request should have been skipped")


@pytest.mark.asyncio
async def test_okx_exchange_fetch_skips_without_explicit_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKX_API_KEY", "web3-key")
    monkeypatch.setenv("OKX_API_SECRET", "web3-secret")
    monkeypatch.setenv("OKX_PASSPHRASE", "web3-passphrase")
    checker = DepositChecker()

    await checker._fetch_okx(_RequestForbiddenSession())
