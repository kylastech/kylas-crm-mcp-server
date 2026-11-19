"""Auth: a signed-in user's own Kylas token in the x-kylas-token header, beside
the OAuth login and the x-api-key header."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastmcp.server.auth import AccessToken

from app import client
from app.oauth import _ApiKeyAwareBearerBackend, kylas_user_token


def _conn(headers):
    # Starlette headers are case-insensitive; a lower-cased dict stands in for them.
    return SimpleNamespace(headers={k.lower(): v for k, v in headers.items()})


def _authenticate(headers, own_result=None):
    """The gate's answer for a request with `headers`, where this server's own OAuth
    check answers `own_result` (None: no valid OAuth token of ours)."""
    provider = MagicMock()
    provider.required_scopes = ["openid"]
    backend = _ApiKeyAwareBearerBackend.__new__(_ApiKeyAwareBearerBackend)
    backend._provider = provider

    async def run():
        with patch("mcp.server.auth.middleware.bearer_auth.BearerAuthBackend.authenticate",
                   new=AsyncMock(return_value=own_result)):
            return await backend.authenticate(_conn(headers))
    return asyncio.run(run())


# --- auth gate (OAuth on) -------------------------------------------------------

def test_user_token_header_is_let_through():
    creds, user = _authenticate({"x-kylas-token": "62f3:3691:8112"})
    assert user.access_token.client_id == "kylas-user-token"
    assert user.access_token.token == ""        # the tool layer reads the header itself
    assert creds.scopes == ["openid"]


def test_user_token_header_name_is_case_insensitive():
    _, user = _authenticate({"X-Kylas-Token": "tok"})
    assert user.access_token.client_id == "kylas-user-token"


def test_own_oauth_token_wins():
    own = ("own-creds", "own-user")
    assert _authenticate({"authorization": "Bearer mcp-jwt", "x-kylas-token": "tok"},
                         own_result=own) == own


def test_api_key_wins_over_user_token():
    _, user = _authenticate({"x-api-key": "key", "x-kylas-token": "tok"})
    assert user.access_token.client_id == "kylas-x-api-key"


def test_api_key_alone_unchanged():
    _, user = _authenticate({"x-api-key": "key"})
    assert user.access_token.token == ""
    assert user.access_token.client_id == "kylas-x-api-key"


@pytest.mark.parametrize("headers", [
    {},
    {"authorization": "Bearer some-kylas-token"},   # Authorization is OAuth only
    {"authorization": "Bearer expired-mcp-jwt"},    # -> 401, its client logs in again
    {"x-kylas-token": ""},
    {"x-kylas-token": "   "},
    {"x-kylas-token": "Bearer "},
    {"x-api-key": ""},
])
def test_no_usable_credential_is_refused(headers):
    assert _authenticate(headers) is None


@pytest.mark.parametrize("value,expected", [
    ("tok", "tok"),
    ("  tok  ", "tok"),
    ("Bearer tok", "tok"),
    ("bearer   tok", "tok"),
    ("Bearer", None),
    ("Bearer a b", "a b"),
    ("", None),
    ("   ", None),
    ("Bearer   ", None),
    (None, None),
])
def test_kylas_user_token(value, expected):
    assert kylas_user_token(value) == expected


# --- headers sent to Kylas --------------------------------------------------------

def _headers(request_headers, access_token=None, env_key=None):
    request = SimpleNamespace(headers=request_headers)
    with patch("fastmcp.server.dependencies.get_access_token", return_value=access_token), \
         patch("fastmcp.server.dependencies.get_http_request", return_value=request), \
         patch.object(client, "API_KEY", env_key):
        return client._resolve_auth_headers()


SYNTHETIC = AccessToken(token="", client_id="kylas-user-token", scopes=[])


def test_user_token_is_sent_to_kylas_as_bearer_oauth_on():
    assert _headers({"x-kylas-token": "user-tok"}, access_token=SYNTHETIC) == {
        "Authorization": "Bearer user-tok"}


def test_user_token_is_sent_to_kylas_as_bearer_oauth_off():
    assert _headers({"x-kylas-token": "Bearer user-tok"}) == {
        "Authorization": "Bearer user-tok"}


def test_user_token_never_falls_back_to_env_key():
    assert _headers({"x-kylas-token": "user-tok"}, env_key="env-key") == {
        "Authorization": "Bearer user-tok"}


def test_api_key_wins_over_user_token_at_kylas_too():
    assert _headers({"x-api-key": "key", "x-kylas-token": "user-tok"}) == {"api-key": "key"}


def test_authorization_header_alone_is_not_forwarded():
    # OAuth off: a bare Authorization header is not a Kylas credential here.
    assert _headers({"authorization": "Bearer tok"}, env_key="env-key") == {"api-key": "env-key"}


def test_oauth_token_still_forwarded_as_before():
    token = AccessToken(token="kylas-oauth-tok", client_id="8112", scopes=[])
    assert _headers({}, access_token=token) == {"Authorization": "Bearer kylas-oauth-tok"}


def test_nothing_falls_back_to_env_key():
    assert _headers({}, env_key="env-key") == {"api-key": "env-key"}


def test_nothing_and_no_env_key_errors():
    with pytest.raises(Exception, match="x-kylas-token"):
        _headers({})
