"""
The two MCP-registered surfaces for entity labels: the mandatory
get_entity_labels() tool (call first, every session) and the
kylas://entity-labels resource. Both just format whatever
shared.labels._fetch_entity_labels returns live — see that module's
docstring for why this is never cached.
"""

from shared.app import mcp
from shared.labels import _fetch_entity_labels


@mcp.tool()
async def get_entity_labels() -> str:
    """
    Returns the mapping of this tenant's custom entity display names to standard CRM entity types.
    CALL THIS ONCE at the start of every session, before any other tool.
    This tenant uses custom names (e.g. "animals" instead of contacts, "cars" instead of deals).
    Without this, you will fail to recognize entity requests from users.
    After calling this, when the user mentions a custom name, map it to the standard type for all tool calls.
    """
    labels = await _fetch_entity_labels()
    if not labels:
        return "No custom entity labels configured for this tenant. Standard names apply: lead, contact, deal, task, company, meeting, call_log."
    lines = ["# Entity Label Mapping — Custom Names for This Tenant\n"]
    lines.append("When the user says one of the custom names below, use the corresponding STANDARD TYPE in all tool calls.\n")
    for entity_type in sorted(labels.keys()):
        label_data = labels[entity_type]
        display_name = label_data.get("displayName", entity_type)
        display_plural = label_data.get("displayNamePlural", entity_type)
        std_type = entity_type.lower()
        lines.append(f'- User says "{display_name}" or "{display_plural}" → use standard type: "{std_type}"')
    lines.append('\n**Example:** User says "get animals" → map "animals" to "contact" → use "contact.search" (list_tool -> build_payload -> execute_request)')
    lines.append('**Example:** User says "show cars" → map "cars" to "deal" → use "deal.search" (list_tool -> build_payload -> execute_request)')
    return "\n".join(lines)


@mcp.resource(
    "kylas://entity-labels",
    name="Entity Label Mapping",
    description=(
        "Tenant-specific entity name mapping. Read this to resolve custom entity names to standard CRM types. "
        "Example: 'animals' may map to 'contact', 'cars' may map to 'deal'. "
        "Always read this resource when the user refers to an entity by an unfamiliar name."
    ),
    mime_type="text/plain",
)
async def entity_labels_resource() -> str:
    """Serve current entity label mapping as a readable resource."""
    labels = await _fetch_entity_labels()
    if not labels:
        return "No custom entity labels. Standard names apply: lead, contact, deal, task, company, meeting, call_log."
    lines = ["# Entity Label Mapping — Tenant-Specific Custom Names", ""]
    lines.append("When the user says a custom name, use the STANDARD TYPE (right of →) in all tool calls.")
    lines.append("")
    for entity_type in sorted(labels.keys()):
        label_data = labels[entity_type]
        display_name = label_data.get("displayName", entity_type)
        display_plural = label_data.get("displayNamePlural", entity_type)
        std_type = entity_type.lower()
        lines.append(f'- "{display_name}" / "{display_plural}" → "{std_type}"')
    lines.extend([
        "",
        "Examples:",
        '  User says "get animals" → standard type is "contact" → use "contact.search" (list_tool -> build_payload -> execute_request)',
        '  User says "show cars"   → standard type is "deal"    → use "deal.search" (list_tool -> build_payload -> execute_request)',
    ])
    return "\n".join(lines)
