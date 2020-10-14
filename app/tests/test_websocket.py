from __future__ import annotations

import pytest
import json

import websockets

async def send_request(request: Dict[str, Any]) -> Dict[str, Any]:
    uri = "ws://localhost:8080/data"
    async with websockets.connect(uri) as websocket:
        await websocket.send(json.dumps(request))
        print(f"> {json.dumps(request)}")

        response = await websocket.recv()
        print(f"< {response}")

test_requests = [
    {
        "action": "get",
        "experiment": "concreteness",
        "get": "utopia",
    },
    #{
    #    "action": "get",
    #    "experiment": "top-100-wikipedia",
    #    "get": "People of Praise",
    #    "single_value": False,
    #},
    {
        "action": "list_experiments",
    },
    {
        "action": "list_image_urls",
        "filter": [{"term": {"image_id": "000dc9fa41fb56b826345029012ca249" }}]
    }
]

@pytest.mark.asyncio
async def test_websockets() -> None:
    for request in test_requests:
        await send_request(request)
