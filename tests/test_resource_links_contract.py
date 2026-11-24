"""The resource link contract (tests/contract/resource_links.json, copied
verbatim from sd-ai-agent), MCP side: every case answered through
execute_request must give exactly its `link`. Native-node cases are
sd-ai-agent's own and are skipped here."""

import json
from pathlib import Path

import pytest

from app import resource_links as rl
from app.tools import _REGISTRY

CONTRACT_FILE = Path(__file__).parent / "contract" / "resource_links.json"
# Copied verbatim from sd-ai-agent tests/contract/resource_links.json. Missing,
# the contract test fails and says why, rather than stopping the whole run.
CONTRACT = json.loads(CONTRACT_FILE.read_text()) if CONTRACT_FILE.exists() else {"cases": []}
CASES = [c for c in CONTRACT["cases"] if "mcp" in c]


def test_the_contract_file_is_here():
    assert CONTRACT_FILE.exists(), (
        f"{CONTRACT_FILE} is missing: copy it from sd-ai-agent tests/contract/resource_links.json")
    assert CASES, "the contract has no MCP cases"


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_each_execute_request_case_produces_exactly_its_link(case):
    arguments, answer = case["mcp"]["arguments"], case["mcp"]["answer"]
    executed = case["mcp"].get("executedRule")
    bucket = _REGISTRY[arguments["id"]]["bucket"]
    sent = [(rl.SEARCH_ENDPOINTS[bucket], executed)] if executed else None
    links = rl.links_for(arguments["id"], _REGISTRY.get(arguments["id"]), arguments["payload"],
                         json.dumps(answer), sent)
    assert [link.model_dump(mode="json", by_alias=True, exclude_none=True) for link in links] == [case["link"]]
