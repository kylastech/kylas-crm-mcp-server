"""
Entity label mapping (tenant-specific display names).

Deliberately NOT cached process-wide. This server runs stateless_http=True
(see main.run()) — every request is its own fresh transport with no session
continuity (mcp/server/streamable_http_manager.py sets mcp_session_id=None
in stateless mode) — so there is no per-tenant or per-session slot to cache
this in safely. A module-level dict here previously caused tenant A's
labels to leak to tenant B (and, via a background refresh loop with no
request context, to whatever KYLAS_API_KEY the env fell back to). Always
fetch live, scoped to the resolved auth of the current request. The extra
/entities/label round-trip is the accepted cost of correctness.
"""

from typing import Dict

from shared.config import logger, BASE_URL
from shared.http_client import get_client


def _format_entity_labels_for_instructions(labels: Dict[str, Dict[str, str]]) -> str:
    """
    Format entity labels as action-oriented routing rules for system instructions.
    Returns empty string if no labels.
    """
    if not labels:
        return ""

    lines = [
        "## ENTITY NAME ROUTING — CALL get_entity_labels() FIRST, THEN USE THESE RULES",
        "",
        "This tenant has renamed CRM entities. When the user mentions any of the names below,",
        "call get_entity_labels() to confirm, then use the standard type for all tool calls:",
        "",
    ]

    for entity_type in sorted(labels.keys()):
        label_data = labels[entity_type]
        display_name = label_data.get("displayName", entity_type)
        display_plural = label_data.get("displayNamePlural", entity_type)
        std_type = entity_type.lower()
        lines.append(
            f'- If user says "{display_name}" or "{display_plural}" → call get_entity_labels(), '
            f'then use standard type "{std_type}" in all tool calls'
        )

    lines.append("")
    lines.append('DO NOT tell the user an entity "doesn\'t exist" before calling get_entity_labels().')

    return "\n".join(lines)


async def _fetch_entity_labels() -> Dict[str, Dict[str, str]]:
    """
    Fetch entity labels from /v1/entities/label endpoint, live, for the caller's
    own resolved auth (get_client() reads x-api-key/OAuth off the current request).
    Returns mapping like: {"LEAD": {"displayName": "Lid", "displayNamePlural": "Lids"}, ...}
    Returns empty dict if fetch fails — callers fall back to standard names.

    Not cached anywhere — this server runs stateless_http (see main.run()), so there's no
    session or tenant-scoped slot to cache this in without either leaking across
    tenants (the old bug) or reintroducing per-tenant credential storage for a
    background refresh. Call this fresh wherever the labels are needed.
    """
    try:
        async with get_client() as client:
            resp = await client.get(f"{BASE_URL}/entities/label")
            labels = resp.json()
            summary = "\n".join(
                f"  • {etype:8} => {data.get('displayName', etype)} / {data.get('displayNamePlural', etype)}"
                for etype, data in sorted(labels.items())
            )
            logger.debug(f"\n📦 Fetched entity labels:\n{summary}")
            return labels
    except Exception as e:
        logger.warning(f"Failed to fetch entity labels: {e}")
        return {}
