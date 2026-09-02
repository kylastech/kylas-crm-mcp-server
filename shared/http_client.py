"""
HTTP client to the Kylas API: auth header resolution, request/response
logging, the throttled client itself (see shared/throttle.py), and uniform
error handling via KylasAPIError.
"""

import json
from typing import Any, Dict, Optional

import httpx

from shared.app import mcp
from shared.config import logger, API_KEY, BASE_URL, SERVER_VERSION
from shared.throttle import _ThrottledClient

# ---------------------------------------------------------------------------
# Logging Helpers
# ---------------------------------------------------------------------------

def _mask_api_key(api_key: Optional[str]) -> str:
    """Mask API key but keep last 12 characters for context."""
    if not api_key:
        return "None"
    if len(api_key) <= 12:
        return "***"
    return "*" * (len(api_key) - 12) + api_key[-12:]


async def _log_request(request: httpx.Request) -> None:
    """Log request details in a readable format."""
    api_key = request.headers.get("api-key")
    auth_header = request.headers.get("Authorization")

    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        masked_auth = f"Bearer {_mask_api_key(token)}"
    else:
        masked_auth = f"api-key: {_mask_api_key(api_key)}" if api_key else "None"

    payload = "None"
    if request.content:
        try:
            payload = json.dumps(json.loads(request.content), indent=2)
        except Exception:
            payload = request.content.decode("utf-8", errors="replace")

    logger.info(
        f"\n{'='*60}\n"
        f"🚀 API REQUEST\n"
        f"{'-'*60}\n"
        f"Method:   {request.method}\n"
        f"URL:      {request.url}\n"
        f"Auth:     {masked_auth}\n"
        f"Payload:\n{payload}\n"
        f"{'='*60}"
    )


async def _log_response(response: httpx.Response) -> None:
    """Log response details in a readable format."""
    await response.aread()

    body = "None"
    if response.content:
        try:
            body = json.dumps(response.json(), indent=2)
        except Exception:
            body = response.text

    error_info = ""
    if response.status_code >= 400:
        try:
            data = response.json()
            error_code = data.get("errorCode", "N/A")
            details = data.get("details") or data.get("message") or "N/A"
            error_info = f"\nError Code: {error_code}\nDetails:    {details}"
        except Exception:
            pass

    logger.info(
        f"\n{'='*60}\n"
        f"✅ API RESPONSE\n"
        f"{'-'*60}\n"
        f"Status:   {response.status_code}{error_info}\n"
        f"Body:\n{body}\n"
        f"{'='*60}"
    )


# ---------------------------------------------------------------------------
# HTTP Client & Errors
# ---------------------------------------------------------------------------

class KylasAPIError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None, response_body: Optional[str] = None):
        self.message = message
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(self.message)


def _get_mcp_client_name() -> str:
    """
    Resolve the MCP client identifier from the MCP initialize handshake.
    Returns '{name}({version})' (e.g. 'Claude Desktop(1.2.3)') or 'unknown'.
    Used for the outbound User-Agent to Kylas: kylas_mcp_server({version}) on {client}.
    """
    try:
        ctx = mcp.get_context()
        client_params = ctx.session.client_params
        if client_params and client_params.clientInfo:
            name = client_params.clientInfo.name or ""
            version = client_params.clientInfo.version or ""
            if name:
                result = f"{name}({version})" if version else name
                logger.debug("MCP client identified: %s", result)
                return result
            logger.warning("MCP clientInfo present but name is empty: %r", client_params.clientInfo)
        else:
            logger.warning("MCP client_params missing or has no clientInfo (proxy/unknown client)")
    except Exception as exc:
        logger.warning("Could not resolve MCP client name: %s", exc)
    return "unknown"


def _resolve_auth_headers() -> Dict[str, str]:
    """Resolve authentication headers:
    1. FastMCP OAuth Bearer token (if user is authenticated via OAuth)
    2. Per-request x-api-key header (if passed in the HTTP request)
    3. Environment variable fallback (KYLAS_API_KEY)
    """
    # 1. Try FastMCP OAuth access token
    try:
        from fastmcp.server.dependencies import get_access_token
        access_token = get_access_token()
        if access_token and access_token.token:
            logger.debug("Resolved OAuth Bearer token authentication")
            return {"Authorization": f"Bearer {access_token.token}"}
    except Exception as e:
        logger.debug("Could not resolve OAuth token: %s", e)

    # 2. Try per-request x-api-key HTTP header
    try:
        from fastmcp.server.dependencies import get_http_request
        request = get_http_request()
        header_key = request.headers.get("x-api-key")
        if header_key:
            logger.debug("Resolved x-api-key header authentication")
            return {"api-key": header_key}
    except Exception:
        pass

    # 3. Fall back to env var
    if API_KEY:
        logger.debug("Resolved environment KYLAS_API_KEY authentication")
        return {"api-key": API_KEY}

    raise KylasAPIError(
        "No authentication provided. Pass x-api-key header, set KYLAS_API_KEY, or configure Kylas OAuth."
    )


class _ThrottledClientContext:
    """Async context manager: wraps httpx.AsyncClient and yields _ThrottledClient for 100–500 ms delay between calls."""

    def __init__(self) -> None:
        auth_headers = _resolve_auth_headers()
        client_name = _get_mcp_client_name()
        user_agent = f"kylas_mcp_server({SERVER_VERSION}) on {client_name}"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": user_agent,
        }
        headers.update(auth_headers)
        self._raw = httpx.AsyncClient(
            base_url=BASE_URL,
            headers=headers,
            timeout=30.0,
            event_hooks={
                "request": [_log_request],
                "response": [_log_response],
            },
        )

    async def __aenter__(self) -> "_ThrottledClient":
        entered = await self._raw.__aenter__()
        return _ThrottledClient(entered)

    async def __aexit__(self, *args: Any) -> Any:
        return await self._raw.__aexit__(*args)


def get_client() -> _ThrottledClientContext:
    """Return a context manager that yields a throttled HTTP client (100–500 ms delay between subsequent API calls)."""
    return _ThrottledClientContext()


async def handle_api_response(response: httpx.Response, operation: str) -> Dict[str, Any]:
    try:
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        error_body = e.response.text
        logger.error(f"❌ {operation} failed: {e.response.status_code}")
        if e.response.status_code == 429:
            retry_after = e.response.headers.get("Retry-After", "30")
            raise KylasAPIError(
                f"Rate limit hit (429). Wait {retry_after} seconds before retrying. "
                "If you were paginating, stop and ask the user before fetching more pages.",
                status_code=429,
                response_body=error_body,
            )
        raise KylasAPIError(
            f"{operation} failed: {e.response.status_code}",
            status_code=e.response.status_code,
            response_body=error_body
        )
    except Exception as e:
        logger.error(f"{operation} failed: {str(e)}")
        raise KylasAPIError(f"{operation} failed: {str(e)}")
