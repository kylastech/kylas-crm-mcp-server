"""Tests for the hybrid auth backend in ``kylas_oauth``.

Focus: ``_ApiKeyAwareBearerBackend.authenticate`` — the raw Kylas access-token
fallback that sits between the proxy-issued OAuth JWT lookup and the ``x-api-key``
escape hatch.

Run: python test_oauth.py   (or: pytest test_oauth.py -v)
"""

import asyncio
from unittest.mock import AsyncMock

try:
    import pytest
except ImportError:  # allow running without pytest installed
    class pytest:
        class mark:
            asyncio = staticmethod(lambda f: f)

from fastmcp.server.auth import AccessToken
from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser

from kylas_oauth import _ApiKeyAwareBearerBackend


class _FakeConn:
    """Minimal stand-in for a Starlette HTTPConnection: just headers."""

    def __init__(self, headers=None):
        self.headers = dict(headers or {})


class _FakeProvider:
    """Stand-in for KylasOAuthProxy exposing only what the backend touches.

    ``_token_validator`` is the KylasTokenVerifier; ``verify_token`` is the raw-token
    verifier the fallback calls. ``load_access_token``/``verify_token`` here back the
    base BearerAuthBackend's proxy-JWT lookup (``super().authenticate``).
    """

    def __init__(self, *, proxy_token=None, raw_valid_for=None, required_scopes=None):
        self.required_scopes = required_scopes or []
        self._proxy_token = proxy_token  # token string the proxy recognizes as its own
        self._raw_valid_for = raw_valid_for  # token string Kylas /users/me accepts

        self._token_validator = AsyncMock()
        self._token_validator.verify_token = AsyncMock(side_effect=self._verify_raw)

    async def _verify_raw(self, token):
        """Mimic KylasTokenVerifier: valid Kylas token -> AccessToken, else None."""
        if token and token == self._raw_valid_for:
            return AccessToken(token=token, client_id="kylas-user-42", scopes=self.required_scopes)
        return None

    async def verify_token(self, token):
        """Back the base BearerAuthBackend: only the proxy-issued JWT is recognized."""
        if token and token == self._proxy_token:
            return AccessToken(token=token, client_id="proxy-client", scopes=self.required_scopes)
        return None


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


@pytest.mark.asyncio
async def test_proxy_issued_token_wins():
    """A proxy-issued Bearer JWT authenticates via the base backend; fallback untouched."""
    provider = _FakeProvider(proxy_token="proxy-jwt", raw_valid_for="raw-kylas")
    backend = _ApiKeyAwareBearerBackend(provider)

    conn = _FakeConn({"authorization": "Bearer proxy-jwt"})
    result = await backend.authenticate(conn)

    assert result is not None
    _creds, user = result
    assert isinstance(user, AuthenticatedUser)
    assert user.access_token.client_id == "proxy-client"
    # Raw-token verifier must not have been consulted for a valid proxy token.
    provider._token_validator.verify_token.assert_not_awaited()


@pytest.mark.asyncio
async def test_raw_kylas_token_authenticates_and_forwards_bearer():
    """A raw Kylas token (not proxy-issued) is verified via /users/me and forwarded."""
    provider = _FakeProvider(proxy_token="proxy-jwt", raw_valid_for="raw-kylas")
    backend = _ApiKeyAwareBearerBackend(provider)

    conn = _FakeConn({"authorization": "Bearer raw-kylas"})
    result = await backend.authenticate(conn)

    assert result is not None
    _creds, user = result
    assert isinstance(user, AuthenticatedUser)
    assert user.access_token.client_id == "kylas-user-42"
    # The real token is carried so _resolve_auth_headers forwards it as Bearer.
    assert user.access_token.token == "raw-kylas"
    provider._token_validator.verify_token.assert_awaited_once_with("raw-kylas")


@pytest.mark.asyncio
async def test_expired_or_invalid_raw_token_is_rejected():
    """A raw token Kylas rejects (expired/invalid) yields 401 when no api-key is present."""
    provider = _FakeProvider(proxy_token="proxy-jwt", raw_valid_for="raw-kylas")
    backend = _ApiKeyAwareBearerBackend(provider)

    conn = _FakeConn({"authorization": "Bearer expired-token"})
    result = await backend.authenticate(conn)

    assert result is None  # -> RequireAuthMiddleware returns 401
    provider._token_validator.verify_token.assert_awaited_once_with("expired-token")


@pytest.mark.asyncio
async def test_invalid_raw_token_falls_through_to_api_key():
    """An unrecognized Bearer token still lets a valid x-api-key through."""
    provider = _FakeProvider(proxy_token="proxy-jwt", raw_valid_for="raw-kylas")
    backend = _ApiKeyAwareBearerBackend(provider)

    conn = _FakeConn({"authorization": "Bearer nope", "x-api-key": "secret-key"})
    result = await backend.authenticate(conn)

    assert result is not None
    _creds, user = result
    # x-api-key path: empty token so the tool layer uses the api-key header.
    assert user.access_token.token == ""
    assert user.access_token.client_id == "kylas-x-api-key"


@pytest.mark.asyncio
async def test_api_key_only_still_works():
    """No Bearer header at all: x-api-key alone authenticates (verifier untouched)."""
    provider = _FakeProvider(proxy_token="proxy-jwt", raw_valid_for="raw-kylas")
    backend = _ApiKeyAwareBearerBackend(provider)

    conn = _FakeConn({"x-api-key": "secret-key"})
    result = await backend.authenticate(conn)

    assert result is not None
    _creds, user = result
    assert user.access_token.client_id == "kylas-x-api-key"
    provider._token_validator.verify_token.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_credentials_returns_none():
    """Neither Bearer nor api-key: unauthenticated -> 401."""
    provider = _FakeProvider(proxy_token="proxy-jwt", raw_valid_for="raw-kylas")
    backend = _ApiKeyAwareBearerBackend(provider)

    result = await backend.authenticate(_FakeConn({}))
    assert result is None


@pytest.mark.asyncio
async def test_bearer_token_parsing_variants():
    """Header parsing: scheme is case-insensitive; a schemeless/empty value is ignored."""
    parse = _ApiKeyAwareBearerBackend._bearer_token
    assert parse(_FakeConn({"authorization": "Bearer abc"})) == "abc"
    assert parse(_FakeConn({"authorization": "bearer abc"})) == "abc"
    assert parse(_FakeConn({"authorization": "Bearer   abc  "})) == "abc"
    assert parse(_FakeConn({"authorization": "Bearer"})) is None
    assert parse(_FakeConn({"authorization": "Bearer   "})) is None
    assert parse(_FakeConn({"authorization": "Basic abc"})) is None
    assert parse(_FakeConn({})) is None


if __name__ == "__main__":
    _tests = [
        test_proxy_issued_token_wins,
        test_raw_kylas_token_authenticates_and_forwards_bearer,
        test_expired_or_invalid_raw_token_is_rejected,
        test_invalid_raw_token_falls_through_to_api_key,
        test_api_key_only_still_works,
        test_no_credentials_returns_none,
        test_bearer_token_parsing_variants,
    ]
    for _t in _tests:
        _run(_t())
        print(f"PASS {_t.__name__}")
    print(f"\n{len(_tests)} passed")
