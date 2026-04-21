"""
Helpers for signing authenticated OKX API requests.

Inputs: Timestamp, method, request path, and secret key.
Outputs: Ready-to-send OKX authentication headers.
Assumptions:
  - Signatures use HMAC-SHA256 over timestamp + method + requestPath + body.
  - GET query parameters must be included in requestPath.
  - Some DEX endpoints may additionally accept OK-ACCESS-PROJECT.
"""

import base64
import hashlib
import hmac
from datetime import datetime, timezone


def okx_timestamp() -> str:
    """Current UTC timestamp in the ISO format expected by OKX."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def okx_sign(
    timestamp: str,
    method: str,
    request_path: str,
    secret: str,
    body: str = "",
) -> str:
    """Build the base64-encoded OKX API signature."""
    payload = timestamp + method.upper() + request_path + body
    mac = hmac.new(
        secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    )
    return base64.b64encode(mac.digest()).decode("utf-8")


def okx_headers(
    api_key: str,
    api_secret: str,
    passphrase: str,
    method: str,
    request_path: str,
    body: str = "",
    project_id: str = "",
) -> dict[str, str]:
    """Build authenticated headers for an OKX API request."""
    timestamp = okx_timestamp()
    headers = {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": okx_sign(timestamp, method, request_path, api_secret, body),
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
        "User-Agent": "python-aiohttp/3.9",
    }
    if project_id:
        headers["OK-ACCESS-PROJECT"] = project_id
    return headers
