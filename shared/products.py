"""
Product lookup — resolves a product name to its ID for filtering leads or
deals by product (search_entity's "products" field).
"""

from shared.app import mcp
from shared.config import logger
from shared.http_client import KylasAPIError, get_client, handle_api_response
from shared.throttle import _reset_api_call_count

# ---------------------------------------------------------------------------
# Tool 3b: Lookup Products (for products filter on leads)
# ---------------------------------------------------------------------------

async def lookup_products_logic(query: str, page: int = 0, size: int = 50) -> str:
    """
    Call GET /products/lookup?q=<query> and return a formatted list of products (id, name).
    Use this when the user asks for leads by product name (e.g. "leads with product X") to resolve X to a product ID.
    """
    if not query or not str(query).strip():
        return "Error: query cannot be empty. Provide a product name or search term (e.g. 'name:Widget' or 'Widget')."
    q = str(query).strip()
    # If user passed plain text, treat as product name for API (name:value form)
    if ":" not in q:
        q = f"name:{q}"
    async with get_client() as client:
        response = await client.get(
            "/products/lookup",
            params={"q": q, "page": page, "size": min(size, 50)},
        )
        data = await handle_api_response(response, "Product lookup")
    content = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(content)))
    total_pages = data.get("totalPages", 1)
    if not content:
        return f"No products found matching '{q}'."
    lines = [f"Found {len(content)} product(s) matching '{q}' (total {total}, page {page + 1} of {total_pages})", "-" * 50]
    for p in content:
        pid = p.get("id", "?")
        name = p.get("name", p.get("displayName", "—"))
        lines.append(f"  • ID: {pid}  |  Name: {name}")
    lines.append("-" * 50)
    if total > 1:
        lines.append("More than one product matched. Ask the user which one they mean, then use that ID in search_entity (e.g. filter products equal to that ID).")
    else:
        lines.append(f"Use product ID {content[0].get('id')} in search_entity when filtering by product (e.g. {{\"field\": \"products\", \"operator\": \"equal\", \"value\": <id>}}). Works for both leads and deals.")
    return "\n".join(lines)


@mcp.tool()
async def lookup_products(query: str, page: int = 0, size: int = 50) -> str:
    """
    Look up products by name. Use this BEFORE filtering leads or deals by product when the user gives a product name.
    - If one product is found, use that product's ID in search_entity for leads or deals (e.g. {"field": "products", "operator": "equal", "value": <id>}).
    - If multiple products are found, ask the user which product they mean (list the options), then use the chosen product's ID.
    query: Search string. Use "name:<product_name>" (e.g. "name:Widget") or just the product name (e.g. "Widget"); the server will send name:value to the API.
    page: 0-based page (default 0).
    size: Max 50 (default 50).
    """
    try:
        _reset_api_call_count()
        logger.info("Product lookup: q=%s", query)
        return await lookup_products_logic(query, page, size)
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("lookup_products")
        return f"Unexpected error: {str(e)}"
