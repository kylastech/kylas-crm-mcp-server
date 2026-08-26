"""OAuth wiring for the Kylas MCP server.

Builds a FastMCP ``OAuthProxy`` tailored to Kylas:
  * derives the Kylas OAuth2 endpoints from the API base URL's origin — one
    ``KYLAS_BASE_URL`` drives both the API and OAuth (staging, prod, etc.);
  * strips the OpenID Connect scopes Kylas' OAuth server doesn't recognise; and
  * validates the opaque Kylas token on every request via :class:`KylasTokenVerifier`.

Tokens are kept in FastMCP's built-in encrypted on-disk store (an encrypted
FileTreeStore persisted to disk) — fine for a single instance.

``create_kylas_auth(...)`` is the single entry point. It returns the auth provider and
the verifier (a closeable resource the app lifespan shuts down), or ``(None, None)``
when OAuth isn't configured.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlsplit, quote

import httpx
from fastmcp.server.auth import AccessToken, OAuthProxy, TokenVerifier
from mcp.server.auth.middleware.auth_context import AuthContextMiddleware
from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser, BearerAuthBackend
from starlette.authentication import AuthCredentials
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware

logger = logging.getLogger("kylas-mcp.oauth")

_API_KEY_HEADER = "x-api-key"

_OAUTH_AUTHORIZE_PATH = "/oauth/authorize"
_OAUTH_TOKEN_PATH = "/oauth/token"
_USERS_ME_PATH = "/users/me"

_KYLAS_OIDC_SCOPES = ["openid", "profile", "email", "offline_access"]


def _origin(url: str) -> str:
    """Return the ``scheme://host[:port]`` origin of *url*, dropping any path/query."""
    parts = urlsplit(url)
    if not parts.scheme or not parts.netloc:
        raise ValueError(f"Invalid Kylas base URL (need scheme and host): {url!r}")
    return f"{parts.scheme}://{parts.netloc}"


class KylasTokenVerifier(TokenVerifier):
    """Validate Kylas opaque access tokens against ``GET /v1/users/me``.

    FastMCP calls ``verify_token`` on *every* MCP request, so this:
      * reuses one pooled ``httpx.AsyncClient`` instead of opening one per call;
      * keeps a small, bounded, short-lived positive cache so a burst of requests
        from the same client doesn't hammer the Kylas API;
      * treats every failure (timeout, network, non-200, bad body) as "token
        invalid" and never raises — an exception here would surface as a 500.
    """

    def __init__(
        self,
        base_url: str,
        required_scopes: Optional[list[str]] = None,
        *,
        request_timeout: float = 10.0,
        cache_ttl_seconds: float = 30.0,
        cache_max_entries: int = 1024,
    ) -> None:
        super().__init__(base_url=base_url, required_scopes=required_scopes)
        self._verify_url = f"{base_url.rstrip('/')}{_USERS_ME_PATH}"
        self._request_timeout = request_timeout
        self._cache_ttl = cache_ttl_seconds
        self._cache_max = cache_max_entries
        # token -> (monotonic_expiry, AccessToken)
        self._cache: Dict[str, Tuple[float, AccessToken]] = {}
        self._http: Optional[httpx.AsyncClient] = None

    async def verify_token(self, token: str) -> Optional[AccessToken]:
        if not token:
            return None

        cached = self._cache_get(token)
        if cached is not None:
            return cached

        try:
            resp = await self._client().get(
                self._verify_url,
                headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            )
        except httpx.HTTPError as exc:
            # Network/timeout problems: fail closed, do not cache.
            logger.warning("Token verification request to %s failed: %s", self._verify_url, exc)
            return None

        if resp.status_code != 200:
            logger.info("Kylas rejected token (status=%s): %s", resp.status_code, resp.text[:200])
            self._cache.pop(token, None)
            return None

        try:
            data = resp.json()
        except ValueError:
            logger.warning("Token verification: /users/me returned a non-JSON body")
            return None

        # The Kylas token is opaque; use the Kylas user id as the client identity.
        access = AccessToken(
            token=token,
            client_id=str(data.get("id", "unknown-user")),
            scopes=self.required_scopes or [],
        )
        self._cache_put(token, access)
        return access

    async def aclose(self) -> None:
        """Close the pooled HTTP client (called on server shutdown)."""
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    def _client(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=self._request_timeout)
        return self._http

    def _cache_get(self, token: str) -> Optional[AccessToken]:
        if self._cache_ttl <= 0:
            return None
        entry = self._cache.get(token)
        if entry is None:
            return None
        expiry, value = entry
        if expiry <= time.monotonic():
            self._cache.pop(token, None)
            return None
        return value

    def _cache_put(self, token: str, value: AccessToken) -> None:
        if self._cache_ttl <= 0:
            return
        if len(self._cache) >= self._cache_max:
            self._evict()
        self._cache[token] = (time.monotonic() + self._cache_ttl, value)

    def _evict(self) -> None:
        """Drop expired entries; if still at capacity, drop the oldest inserted."""
        now = time.monotonic()
        for key in [k for k, (expiry, _) in self._cache.items() if expiry <= now]:
            self._cache.pop(key, None)
        while len(self._cache) >= self._cache_max and self._cache:
            self._cache.pop(next(iter(self._cache)), None)


class _ApiKeyAwareBearerBackend(BearerAuthBackend):
    """Bearer-token auth, plus two escape hatches: raw Kylas tokens and ``x-api-key``.

    OAuth clients still authenticate exactly as before (proxy-issued JWT → verifier).
    Two additional paths are tried, in order, when that lookup misses:

    1. **Raw Kylas access token.** A request whose ``Authorization: Bearer`` carries a
       token Kylas itself issued (e.g. one lifted from an embedded host-app session)
       won't match the FastMCP JWTs the proxy issued, so the base backend rejects it.
       We then verify the raw token directly against Kylas ``/users/me`` via the shared
       verifier; if Kylas accepts it, it is forwarded as a real Bearer token on the API
       call. There is no refresh — an expired token 401s at this gate, and the host app
       supplies a fresh token on its next login.
    2. **``x-api-key``.** A request carrying a non-empty ``x-api-key`` header gets a
       synthetic authenticated user whose access token is empty, so
       ``main._resolve_auth_headers`` skips the OAuth branch and falls through to the
       ``x-api-key`` header (which Kylas validates on the real API call).

    A request that satisfies none of these still gets 401 and must log in.
    """

    def __init__(self, provider: "KylasOAuthProxy") -> None:
        super().__init__(provider)
        self._provider = provider

    async def authenticate(self, conn):
        # Proxy-issued OAuth Bearer token wins whenever present and valid.
        result = await super().authenticate(conn)
        if result is not None:
            return result

        # A raw Kylas access token presented as a Bearer token. Verify it directly
        # against Kylas /users/me; if valid, forward it as a real Bearer token.
        raw_token = self._bearer_token(conn)
        if raw_token:
            access = await self._provider._token_validator.verify_token(raw_token)
            if access is not None:
                scopes = list(access.scopes or [])
                logger.debug("Request authenticated via raw Kylas access token")
                return AuthCredentials(scopes), AuthenticatedUser(access)

        api_key = next(
            (conn.headers.get(k) for k in conn.headers if k.lower() == _API_KEY_HEADER), None
        )
        if not api_key:
            return None
        scopes = list(getattr(self._provider, "required_scopes", None) or [])
        # token="" so the tool layer uses the x-api-key path, not a Bearer header.
        synthetic = AccessToken(token="", client_id="kylas-x-api-key", scopes=scopes)
        logger.debug("Request authenticated via x-api-key header (OAuth gate bypassed)")
        return AuthCredentials(scopes), AuthenticatedUser(synthetic)

    @staticmethod
    def _bearer_token(conn) -> Optional[str]:
        """Return the raw token from an ``Authorization: Bearer <token>`` header, if any."""
        header = next(
            (conn.headers.get(k) for k in conn.headers if k.lower() == "authorization"), None
        )
        if not header:
            return None
        scheme, _, value = header.partition(" ")
        if scheme.lower() != "bearer":
            return None
        value = value.strip()
        return value or None


class KylasOAuthProxy(OAuthProxy):
    """OAuthProxy tailored for Kylas.

    Customizations:
      * scope handling — MCP clients request the standard OIDC scopes, but Kylas' OAuth
        server doesn't recognise them, so we drop them at each point the base proxy sends
        scopes upstream (authorize / token / refresh); with no scope sent, Kylas grants
        the app's own registered scopes.
      * ``get_middleware`` swaps in :class:`_ApiKeyAwareBearerBackend` so existing
        ``x-api-key`` clients keep working with OAuth enabled.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._client_names: Dict[str, str] = {}

    def get_middleware(self) -> list:
        return [
            Middleware(AuthenticationMiddleware, backend=_ApiKeyAwareBearerBackend(self)),
            Middleware(AuthContextMiddleware),
        ]

    def _drop_oidc_scopes(self, scopes: list[str]) -> list[str]:
        return [s for s in scopes if s not in _KYLAS_OIDC_SCOPES]

    async def register_client(self, client_info: Any) -> None:
        await super().register_client(client_info)
        name = getattr(client_info, "client_name", None)
        if name:
            self._client_names[client_info.client_id] = name

    def _build_upstream_authorize_url(self, txn_id: str, transaction: dict[str, Any]) -> str:
        scopes = self._drop_oidc_scopes(transaction.get("scopes") or [])
        url = super()._build_upstream_authorize_url(txn_id, {**transaction, "scopes": scopes})

        client_name = self._client_names.get(transaction.get("client_id"))
        if not client_name:
            logger.info(
                "No client_name for client_id=%s (not sent at DCR, or server restarted "
                "since); omitting from upstream authorize URL",
                transaction.get("client_id"),
            )
            return url

        separator = "&" if "?" in url else "?"
        return f"{url}{separator}client_name={quote(client_name)}"

    def _prepare_scopes_for_token_exchange(self, scopes: list[str]) -> list[str]:
        return super()._prepare_scopes_for_token_exchange(self._drop_oidc_scopes(scopes))

    def _prepare_scopes_for_upstream_refresh(self, scopes: list[str]) -> list[str]:
        return super()._prepare_scopes_for_upstream_refresh(self._drop_oidc_scopes(scopes))


def create_kylas_auth(
    *,
    base_url: str,
    client_id: Optional[str],
    client_secret: Optional[str],
    mcp_server_base_url: str,
    token_cache_ttl_seconds: float = 30.0,
    authorize_url: Optional[str] = None,
    token_url: Optional[str] = None,
) -> Tuple[Optional[KylasOAuthProxy], Optional[KylasTokenVerifier]]:
    """Build the Kylas OAuth provider.

    Returns ``(auth_provider, token_verifier)``, or ``(None, None)`` when
    client_id/secret aren't set (OAuth disabled). The verifier is returned so the
    caller can close it on shutdown.

    Token storage is FastMCP's built-in encrypted on-disk store (FileTreeStore),
    persisted to disk — fine for a single instance.
    """
    if not (client_id and client_secret):
        logger.info("Kylas OAuth disabled (client_id/secret not configured)")
        return None, None

    origin = _origin(base_url)
    authorize_url = authorize_url or f"{origin}{_OAUTH_AUTHORIZE_PATH}"
    token_url = token_url or f"{origin}{_OAUTH_TOKEN_PATH}"
    logger.info("Kylas OAuth: authorize=%s token=%s", authorize_url, token_url)
    logger.info("OAuth token storage: on-disk (FastMCP's encrypted FileTreeStore)")

    verifier = KylasTokenVerifier(base_url=base_url, cache_ttl_seconds=token_cache_ttl_seconds)

    provider = KylasOAuthProxy(
        upstream_authorization_endpoint=authorize_url,
        upstream_token_endpoint=token_url,
        upstream_client_id=client_id,
        upstream_client_secret=client_secret,
        token_verifier=verifier,
        base_url=mcp_server_base_url,
        valid_scopes=list(_KYLAS_OIDC_SCOPES),
        require_authorization_consent="external",
    )
    return provider, verifier
