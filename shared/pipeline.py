"""
Pipeline lookup/stages/details — shared across lead and deal (both have a
pipeline + pipelineStage). _get_pipeline_details_raw is also reused by
entities/deal.py's sequential-stage-flow handling (advancing a deal through
stages one at a time when sequentialStageFlow=true on the pipeline).
"""

from typing import Any, Dict

from shared.app import mcp
from shared.config import logger
from shared.http_client import KylasAPIError, get_client, handle_api_response
from shared.throttle import _reset_api_call_count

# ---------------------------------------------------------------------------
# Tool 3c: Lookup Pipelines (for pipeline + stage filters on leads)
# ---------------------------------------------------------------------------

async def lookup_pipelines_logic(
    query: str = "",
    entity_type: str = "LEAD",
    page: int = 0,
    size: int = 50,
) -> str:
    """
    Call GET /pipelines/lookup?entityType=<entity_type>&q=<query> and return a formatted list of pipelines (id, name).
    Use when the user asks for leads by stage (e.g. open/closed/won) but pipeline is not specified; then ask user to select a pipeline.
    """
    q = str(query).strip() if query else ""
    if ":" not in q and q:
        q = f"name:{q}"
    # Empty q: some APIs return all pipelines when q=name:
    if not q:
        q = "name:"
    async with get_client() as client:
        response = await client.get(
            "/pipelines/lookup",
            params={"entityType": entity_type, "q": q, "page": page, "size": min(size, 50)},
        )
        data = await handle_api_response(response, "Pipeline lookup")
    content = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(content)))
    total_pages = data.get("totalPages", 1)
    if not content:
        return f"No pipelines found for entity {entity_type}" + (f" matching '{q}'." if q else ".")
    lines = [
        f"Found {len(content)} pipeline(s) (entityType={entity_type}, total {total}, page {page + 1} of {total_pages})",
        "-" * 50,
    ]
    for p in content:
        pid = p.get("id", "?")
        name = p.get("name", p.get("displayName", "—"))
        lines.append(f"  • ID: {pid}  |  Name: {name}")
    lines.append("-" * 50)
    lines.append("Ask the user to confirm which pipeline to use (list id and name). Do NOT resolve pipeline.details until the user has confirmed. After confirmation, resolve pipeline.details (list_tool -> build_payload -> execute_request; not a standalone tool) with that pipeline ID only, then search or update with pipeline + pipelineStage filters.")
    return "\n".join(lines)


@mcp.tool()
async def lookup_pipelines(
    query: str = "",
    entity_type: str = "LEAD",
    page: int = 0,
    size: int = 50,
) -> str:
    """
    Look up pipelines by name for leads or deals. Use when the user asks for items by stage but does not specify which pipeline.

    **For Leads:** lookup_pipelines(query="", entity_type="LEAD")
    **For Deals:** lookup_pipelines(query="", entity_type="DEAL")

    Workflow:
    - Call this first; do NOT call get_pipeline_stages until after the user confirms the pipeline.
    - Present the pipeline(s) (id and name) and ask the user which pipeline they mean. If only one pipeline is found, still ask for confirmation.
    - Only after the user confirms, call get_pipeline_stages with that pipeline ID to get stages, then search_leads or update_deal/update_lead.

    query: Search string. Use "name:<pipeline_name>" or just the pipeline name; empty string returns all pipelines for the entity.
    entity_type: Entity type - "LEAD" (default) or "DEAL".
    page: 0-based page (default 0).
    size: Max 50 (default 50).
    """
    try:
        _reset_api_call_count()
        logger.info("Pipeline lookup: entityType=%s q=%s", entity_type, query)
        return await lookup_pipelines_logic(query, entity_type, page, size)
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("lookup_pipelines")
        return f"Unexpected error: {str(e)}"


async def get_pipeline_stages_logic(pipeline_id: int) -> str:
    """
    Call POST /pipelines/summary with jsonRule filtering by pipeline id(s). Returns pipeline name and list of stages (id, name, forecastingType).
    Use after the user has selected a pipeline; then map user intent (open/closed/won/lost) to stage id(s) and call search_leads.
    """
    payload = {
        "jsonRule": {
            "condition": "AND",
            "rules": [{"operator": "in", "id": "id", "field": "id", "type": "long", "value": [pipeline_id]}],
            "valid": True,
        }
    }
    async with get_client() as client:
        response = await client.post("/pipelines/summary", json=payload)
        data = await handle_api_response(response, "Pipeline summary")
    # Response is array of {id, name, stages: [{id, name, position, forecastingType}]}
    pipelines = data if isinstance(data, list) else data.get("content", data.get("data", []))
    if not pipelines:
        return f"No pipeline found with ID {pipeline_id}."
    lines = []
    for pl in pipelines:
        pl_id = pl.get("id", "?")
        pl_name = pl.get("name", "—")
        lines.append(f"Pipeline: {pl_name} (ID: {pl_id})")
        stages = pl.get("stages", [])
        if not stages:
            lines.append("  (no stages)")
        else:
            for s in stages:
                sid = s.get("id", "?")
                sname = s.get("name", "—")
                ftype = s.get("forecastingType", "")
                lines.append(f"  • Stage ID: {sid}  |  Name: {sname}  |  forecastingType: {ftype}")
        lines.append("")
    lines.append("Map user intent to stage: 'open' → OPEN; 'won' → CLOSED_WON; 'lost' → CLOSED_LOST; 'closed unqualified' → CLOSED_UNQUALIFIED. If multiple stages match (e.g. several OPEN stages), ask the user which stage they mean, then use that stage ID in search_leads with pipeline and pipelineStage filters.")
    return "\n".join(lines).strip()


@mcp.tool()
async def get_pipeline_stages(pipeline_id: int) -> str:
    """
    Get stages for a pipeline. Call this only after the user has confirmed which pipeline to use (from lookup_pipelines). Do not call before pipeline confirmation.
    Returns pipeline name and list of stages for that pipeline only, with id, name, and forecastingType (OPEN, CLOSED_WON, CLOSED_LOST, CLOSED_UNQUALIFIED).
    Use the stage IDs in search_leads: filters [{"field": "pipeline", "operator": "equal", "value": pipeline_id}, {"field": "pipelineStage", "operator": "equal", "value": stage_id}].
    If the user said "open leads" or "closed leads" and more than one stage has the same forecastingType, ask which stage they mean.
    pipeline_id: The pipeline ID (from lookup_pipelines).
    """
    try:
        pipeline_id = int(pipeline_id)
    except (TypeError, ValueError):
        return "Error: pipeline_id must be a number."
    try:
        _reset_api_call_count()
        logger.info("Pipeline stages: pipeline_id=%s", pipeline_id)
        return await get_pipeline_stages_logic(pipeline_id)
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("get_pipeline_stages")
        return f"Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Tool 3d: Get pipeline details (GET /pipelines/{id}) – stages + lost/unqualified reasons
# ---------------------------------------------------------------------------

async def _get_pipeline_details_raw(pipeline_id: int) -> Dict[str, Any]:
    """Fetch raw pipeline details dict from GET /pipelines/{id}."""
    async with get_client() as client:
        response = await client.get(f"/pipelines/{int(pipeline_id)}")
        return await handle_api_response(response, "Get pipeline details")


async def get_pipeline_details_logic(pipeline_id: int) -> str:
    """
    Call GET /pipelines/{id}. Returns pipeline name, stages (id, name, forecastingType),
    sequentialStageFlow flag, unqualifiedReasons (for Closed Unqualified), and lostReasons (for Closed Lost).
    Use when moving a lead to Closed Lost or Closed Unqualified: get reasons, ask the user to pick one,
    then update_lead with pipelineStageReason set to that exact string.
    """
    pipeline_id = int(pipeline_id)
    data = await _get_pipeline_details_raw(pipeline_id)
    name = data.get("name", "—")
    sequential = data.get("sequentialStageFlow", False)
    lines = [
        f"Pipeline: {name} (ID: {pipeline_id})",
        f"Sequential Stage Flow: {'YES — stages must be moved one at a time in order' if sequential else 'NO — any stage can be targeted directly'}",
        "",
        "Stages (ordered by position):",
    ]
    for s in sorted(data.get("stages", []), key=lambda x: x.get("position", 0)):
        sid = s.get("id", "?")
        sname = s.get("name", "—")
        ftype = s.get("forecastingType", "")
        pos = s.get("position", "?")
        lines.append(f"  • Position {pos} | Stage ID: {sid}  |  Name: {sname}  |  forecastingType: {ftype}")
    unq = data.get("unqualifiedReasons") or []
    lost = data.get("lostReasons") or []
    lines.extend([
        "",
        "Closed Unqualified reasons (use exact string as pipelineStageReason when moving to Closed Unqualified):",
    ])
    if unq:
        for r in unq:
            lines.append(f"  • \"{r}\"")
    else:
        lines.append("  (none configured)")
    lines.extend([
        "",
        "Closed Lost reasons (use exact string as pipelineStageReason when moving to Closed Lost):",
    ])
    if lost:
        for r in lost:
            lines.append(f"  • \"{r}\"")
    else:
        lines.append("  (none configured)")
    lines.append("")
    lines.append("When updating a lead or deal to Closed Lost or Closed Unqualified, ask the user to pick one reason from the list above, then resolve lead.update or deal.update (list_tool -> build_payload -> execute_request; not a standalone tool) with pipelineStageReason set to that exact string.")
    return "\n".join(lines)


@mcp.tool()
async def get_pipeline_details(pipeline_id: int) -> str:
    """
    Get full pipeline details by ID (GET /pipelines/{id}): stages plus unqualifiedReasons and lostReasons.
    Call this when moving a lead to Closed Lost or Closed Unqualified. Present the relevant reasons list to the user,
    ask them to pick one, then call update_lead with pipelineStageReason set to that exact string (e.g. "No followup", "Booked with competitor").
    pipeline_id: The pipeline ID (from the lead's current pipeline or from lookup_pipelines).
    """
    try:
        pipeline_id = int(pipeline_id)
    except (TypeError, ValueError):
        return "Error: pipeline_id must be a number."
    try:
        _reset_api_call_count()
        logger.info("Pipeline details: pipeline_id=%s", pipeline_id)
        return await get_pipeline_details_logic(pipeline_id)
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("get_pipeline_details")
        return f"Unexpected error: {str(e)}"
