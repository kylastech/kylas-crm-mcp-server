"""
Configuration & Logging

Environment variables, logging setup, and the server's own version string.
Loaded once at import time; every other module reads these as constants —
nothing here is reassigned after import.
"""

import logging
import os
from importlib.metadata import version as _pkg_version, PackageNotFoundError

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("kylas-mcp")

BASE_URL = os.getenv("KYLAS_BASE_URL", "https://api.kylas.io/v1")
API_KEY = os.getenv("KYLAS_API_KEY")
KYLAS_CLIENT_ID = os.getenv("KYLAS_CLIENT_ID")
KYLAS_CLIENT_SECRET = os.getenv("KYLAS_CLIENT_SECRET")
MCP_SERVER_BASE_URL = os.getenv("MCP_SERVER_BASE_URL", "http://localhost:8000")
# Positive-verification cache TTL (seconds). FastMCP verifies the token on every request;
# a short cache avoids calling Kylas /users/me for every call. 0 disables the cache.
KYLAS_TOKEN_CACHE_TTL_SECONDS = float(os.getenv("KYLAS_TOKEN_CACHE_TTL", "30"))

try:
    SERVER_VERSION = _pkg_version("kylas-crm-mcp-server")
except PackageNotFoundError:
    SERVER_VERSION = "unknown"
