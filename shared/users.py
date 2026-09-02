"""
Current-user profile (timezone, permissions — needed before any date/datetime
conversion) and user lookup (resolving a name to an ID for createdBy/
updatedBy/ownerId/importedBy/convertedBy filters).
"""

from typing import Any, Dict, List

from shared.app import mcp
from shared.config import logger
from shared.http_client import KylasAPIError, get_client, handle_api_response
from shared.throttle import _reset_api_call_count

# ---------------------------------------------------------------------------
# Tool 1b: Get current user (timezone, recordActions, etc.) – for date/datetime handling
# ---------------------------------------------------------------------------

async def _fetch_current_user() -> Dict[str, Any]:
    """Fetch current user from GET /users/me. Returns full user object (timezone, recordActions, name, etc.)."""
    async with get_client() as client:
        response = await client.get("/users/me")
        return await handle_api_response(response, "Fetch current user")


@mcp.tool()
async def get_current_user() -> str:
    """
    Get the current authenticated user's profile from Kylas (GET /users/me).
    Call this whenever a date or datetime-related query is involved, or whenever
    you need the current user's own ID (e.g. to set ownerId/createdBy to "me").
    Returns id, timezone (IANA, e.g. Asia/Calcutta), recordActions (call, email, sms, etc.), name, and other profile fields.
    - For filtering (search_leads, search_idle_leads): use the returned timezone as the timeZone in date/datetime filters; keep the user's date/datetime as-is (do not convert to UTC).
    - For create_lead: when the user provides a datetime in their own words (e.g. "11th Feb 2026 at 7:30 AM"), interpret it in this timezone, convert to UTC using parse_datetime_to_utc_iso, and send the UTC ISO string in field_values.
    - For ownerId/createdBy referring to the current user (e.g. "assign to me", "create a lead owned by me"): use the returned id directly — do NOT call user.lookup/lookup_users to resolve yourself by name.
    """
    try:
        _reset_api_call_count()
        logger.info("Fetching current user (users/me)")
        user = await _fetch_current_user()
        user_id = user.get("id")
        tz = user.get("timezone") or "UTC"
        name = user.get("name") or f"{user.get('firstName', '')} {user.get('lastName', '')}".strip() or "—"
        lines = [
            "=" * 50,
            "CURRENT USER (GET /users/me)",
            "=" * 50,
            f"ID: {user_id}",
            f"Name: {name}",
            f"Timezone: {tz}",
            "",
            "recordActions (permissions):",
        ]
        ra = user.get("recordActions") or {}
        for k, v in sorted(ra.items()):
            lines.append(f"  • {k}: {v}")
        lines.extend([
            "",
            "Use this timezone for:",
            "  - Date/datetime filters in search_leads: pass timeZone in each date filter; do not convert filter values to UTC.",
            "  - create_lead with datetime fields: convert user's local datetime to UTC with parse_datetime_to_utc_iso, then send UTC ISO in field_values.",
            f"Use this ID ({user_id}) directly for ownerId/createdBy when the action refers to the current user — no need for a separate user.lookup call.",
            "=" * 50,
        ])
        return "\n".join(lines)
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("get_current_user")
        return f"Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Tool 2: Lookup Users (for createdBy, updatedBy, ownerId, importedBy, convertedBy filters)
# ---------------------------------------------------------------------------

async def lookup_users_logic(
    query: str, page: int = 0, size: int = 50, fetch_all_pages: bool = False
) -> str:
    """
    Call GET /users/lookup?q=<query> and return a formatted list of users (id, name).
    Use this when the user asks for leads by "created by X", "owner is Y", etc., to resolve X/Y to a user ID.
    If fetch_all_pages is True, request all pages and return all users in one response (cap at 500).
    """
    if not query or not str(query).strip():
        return "Error: query cannot be empty. Provide a name or search term (e.g. 'last' or 'firstName:last'), or use query 'name:' with return_all=True to list all users."
    q = str(query).strip()
    page_size = min(size, 50)
    content: List[Dict[str, Any]] = []
    total = 0
    total_pages = 1
    current_page = page
    max_users = 500 if fetch_all_pages else page_size

    async with get_client() as client:
        while True:
            response = await client.get(
                "/users/lookup",
                params={"q": q, "page": current_page, "size": page_size},
            )
            data = await handle_api_response(response, "User lookup")
            chunk = data.get("content", data.get("data", []))
            total = data.get("totalElements", data.get("total", len(chunk) + len(content)))
            total_pages = data.get("totalPages", 1)
            content.extend(chunk)
            if not fetch_all_pages or current_page >= total_pages - 1 or len(content) >= max_users or len(chunk) < page_size:
                break
            current_page += 1

    if not content:
        return f"No users found matching '{q}'."
    if fetch_all_pages:
        header = f"Found {len(content)} user(s)" + (f" matching '{q}'" if q != "name:" else "") + f" (total {total}, all returned in one list)"
    else:
        header = f"Found {len(content)} user(s) matching '{q}' (total {total}, page {page + 1} of {total_pages})"
    lines = [header, "-" * 50]
    for u in content:
        uid = u.get("id", "?")
        name = u.get("name", "—")
        lines.append(f"  • ID: {uid}  |  Name: {name}")
    lines.append("-" * 50)
    if len(content) > 1 and not fetch_all_pages:
        lines.append("More than one user matched. Ask the user which one they mean, then use that ID in search_leads (e.g. filter createdBy / ownerId equal to that ID).")
    elif len(content) == 1:
        lines.append(f"Use user ID {content[0].get('id')} in search_leads when filtering by created by / owner / etc.")
    return "\n".join(lines)


@mcp.tool()
async def lookup_users(
    query: str = "name:",
    page: int = 0,
    size: int = 50,
    return_all: bool = False,
) -> str:
    """
    Look up users by name, or list all users in the system.
    - Use return_all=True (with query "name:" or empty) to fetch all users in one response (all pages combined).
    - For name search: query in field:value form (e.g. "firstName:last", "name:Last"). If one user is found, use that ID in search_leads; if multiple, ask which one.
    query: Search string (e.g. "firstName:last", "name:Last"). Use "name:" or leave default to list all when return_all=True.
    page: 0-based page (default 0). Ignored when return_all=True.
    size: Page size, max 50 (default 50). Used per page when return_all=True.
    return_all: If True, fetch all pages and return every user in one response (cap 500).
    """
    try:
        _reset_api_call_count()
        q = (query or "name:").strip() or "name:"
        logger.info("User lookup: q=%s return_all=%s", q, return_all)
        return await lookup_users_logic(q, page, size, fetch_all_pages=return_all)
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("lookup_users")
        return f"Unexpected error: {str(e)}"
